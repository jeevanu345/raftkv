//! The runtime drives `raft_core::RaftNode`. It owns:
//!
//! * The pure state machine (locked behind a Mutex).
//! * The durable log + meta + snapshot stores.
//! * The KV state machine.
//! * Outbound peer clients and an inbox for incoming peer responses.
//! * Channels for client proposals and read-index requests.
//!
//! Side-effect mapping: every `Action` produced by the core is interpreted
//! here.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use kv_state_machine::{Command, KvStateMachine, Response};
use parking_lot::Mutex;
use raft_core::{
    Action, ConfigChange, Entry, EntryKind, Message, MetricEvent, NodeId, ProposeError, RaftConfig,
    RaftLog, RaftNode, Role,
};
use raft_net::{client::PeerClient, server::MessageProcessor, Inbox, InboxTx};
use raft_storage::{MetaStore, SegmentedLog, SnapshotMeta, SnapshotStore};
use resp_server::codec::RespFrame;
use resp_server::handler::{CommandHandler, ProposeFailure};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, Notify};

use crate::config::ServerConfig;

/// Per-proposal waiter info.
struct PendingProposal {
    tx: oneshot::Sender<Result<Response, ProposeFailure>>,
}

/// Read-index waiter (linearizable read).
struct PendingRead {
    ctx: bytes::Bytes,
    quorum_confirmed: bool,
    term: u64,
    commit_target: u64,
    tx: oneshot::Sender<RespFrame>,
    f: Box<dyn FnOnce(&KvStateMachine) -> RespFrame + Send>,
}

/// Proposal request enqueued from the client task.
enum ProposalKind {
    Command(Command),
    #[allow(dead_code)] // Reserved until durable live membership is supported.
    ConfigChange(ConfigChange),
}

struct ProposalRequest {
    kind: ProposalKind,
    tx: oneshot::Sender<Result<Response, ProposeFailure>>,
}

struct ReadRequest {
    f: Box<dyn FnOnce(&KvStateMachine) -> RespFrame + Send>,
    tx: oneshot::Sender<RespFrame>,
}

/// The runtime, shared with client tasks via Arc.
pub struct Runtime {
    cfg: ServerConfig,
    driver: tokio::sync::Mutex<()>,
    node: Mutex<RaftNode>,
    log: Arc<SegmentedLog>,
    meta: Arc<MetaStore>,
    snaps: Arc<SnapshotStore>,
    sm: Arc<KvStateMachine>,
    peers: HashMap<NodeId, PeerClient>,
    proposal_tx: mpsc::Sender<ProposalRequest>,
    read_tx: mpsc::Sender<ReadRequest>,
    notify: Arc<Notify>,
    soft: Mutex<SoftCache>,
    addr_book: HashMap<NodeId, String>,
    pending: Mutex<HashMap<u64, PendingProposal>>,
    pending_reads: Mutex<Vec<PendingRead>>,
    started: Instant,
    read_sequence: AtomicU64,
    counters: RuntimeCounters,
    healthy: AtomicBool,
    failure: Mutex<Option<String>>,
}

#[derive(Default)]
struct RuntimeCounters {
    proposals_received: AtomicU64,
    proposals_committed: AtomicU64,
    proposal_failures: AtomicU64,
    reads_received: AtomicU64,
    raft_messages_received: AtomicU64,
    raft_messages_sent: AtomicU64,
    elections_started: AtomicU64,
    elections_won: AtomicU64,
    append_rejections: AtomicU64,
    storage_errors: AtomicU64,
}

impl RuntimeCounters {
    fn json(&self) -> Value {
        json!({
            "proposals_received": self.proposals_received.load(Ordering::Relaxed),
            "proposals_committed": self.proposals_committed.load(Ordering::Relaxed),
            "proposal_failures": self.proposal_failures.load(Ordering::Relaxed),
            "reads_received": self.reads_received.load(Ordering::Relaxed),
            "raft_messages_received": self.raft_messages_received.load(Ordering::Relaxed),
            "raft_messages_sent": self.raft_messages_sent.load(Ordering::Relaxed),
            "elections_started": self.elections_started.load(Ordering::Relaxed),
            "elections_won": self.elections_won.load(Ordering::Relaxed),
            "append_rejections": self.append_rejections.load(Ordering::Relaxed),
            "storage_errors": self.storage_errors.load(Ordering::Relaxed),
        })
    }
}

#[derive(Default)]
struct SoftCache {
    role: String,
    term: u64,
    leader: Option<NodeId>,
    commit_index: u64,
    applied_index: u64,
    last_log_index: u64,
}

impl Runtime {
    /// Create the runtime, opening durable stores and constructing the core.
    pub async fn new(cfg: ServerConfig) -> anyhow::Result<(Arc<Self>, RuntimeHandles)> {
        cfg.validate()?;
        std::fs::create_dir_all(&cfg.data_dir)?;
        let log = Arc::new(SegmentedLog::open(
            raft_storage::segmented_log::SegmentedLogConfig {
                dir: cfg.data_dir.join("log"),
                max_segment_bytes: 64 * 1024 * 1024,
                sync_each_append: false,
            },
        )?);
        let meta = Arc::new(MetaStore::open(cfg.data_dir.join("meta"))?);
        let snaps = Arc::new(SnapshotStore::open(cfg.data_dir.join("snapshots"))?);
        let sm = Arc::new(KvStateMachine::open(cfg.data_dir.join("kv"))?);

        let hs = meta.load_hard_state()?;
        let snap_ptr = meta.load_snapshot_pointer()?.unwrap_or((0, 0));
        let mut raft_log = RaftLog::from_snapshot(snap_ptr.0, snap_ptr.1);
        let mut buf = Vec::new();
        let last = log.last_index();
        let first = log.first_index().max(snap_ptr.0 + 1);
        if last >= first {
            log.read_range(first, last + 1, &mut buf)?;
            raft_log.append(buf);
        }

        let node = RaftNode::new(
            RaftConfig {
                id: cfg.id,
                initial_voters: cfg.peers.iter().map(|p| p.id).collect(),
                election_timeout_ticks: (cfg.election_timeout_ms / cfg.tick_ms.max(1)).max(1),
                heartbeat_ticks: (cfg.heartbeat_ms / cfg.tick_ms.max(1)).max(1),
                max_append_entries: 256,
                max_inflight_msgs: 1024,
                rng_seed: 0x600D_F00D ^ cfg.id,
                pre_vote: cfg.pre_vote,
            },
            hs,
            raft_log,
        );

        let (proposal_tx, proposal_rx) = mpsc::channel(1024);
        let (read_tx, read_rx) = mpsc::channel(1024);
        let (inbox_tx, inbox_rx): (InboxTx, Inbox) = mpsc::unbounded_channel();
        let notify = Arc::new(Notify::new());

        let mut peers: HashMap<NodeId, PeerClient> = HashMap::new();
        let mut addr_book: HashMap<NodeId, String> = HashMap::new();
        for p in &cfg.peers {
            addr_book.insert(p.id, p.raft_addr.clone());
            if p.id != cfg.id {
                peers.insert(
                    p.id,
                    PeerClient::new(p.id, p.raft_addr.clone(), inbox_tx.clone()),
                );
            }
        }

        let rt = Arc::new(Self {
            cfg,
            driver: tokio::sync::Mutex::new(()),
            node: Mutex::new(node),
            log,
            meta,
            snaps,
            sm,
            peers,
            proposal_tx,
            read_tx,
            notify,
            soft: Mutex::new(SoftCache::default()),
            addr_book,
            pending: Mutex::new(HashMap::new()),
            pending_reads: Mutex::new(Vec::new()),
            started: Instant::now(),
            read_sequence: AtomicU64::new(1),
            counters: RuntimeCounters::default(),
            healthy: AtomicBool::new(true),
            failure: Mutex::new(None),
        });

        Ok((
            rt.clone(),
            RuntimeHandles {
                inbox_tx,
                inbox_rx,
                proposal_rx,
                read_rx,
            },
        ))
    }

    /// Spawn the runtime loop.
    pub fn spawn(self: Arc<Self>, handles: RuntimeHandles) {
        let RuntimeHandles {
            inbox_tx: _,
            mut inbox_rx,
            mut proposal_rx,
            mut read_rx,
        } = handles;
        let me = self.clone();
        tokio::spawn(async move {
            let tick = Duration::from_millis(me.cfg.tick_ms);
            let mut tick_iv = tokio::time::interval(tick);
            tick_iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    _ = tick_iv.tick() => {
                        let _driver = me.driver.lock().await;
                        if !me.healthy.load(Ordering::Acquire) { continue; }
                        let actions = me.node.lock().tick();
                        me.execute_actions(actions).await;
                    }
                    Some(msg) = inbox_rx.recv() => {
                        let _driver = me.driver.lock().await;
                        me.counters.raft_messages_received.fetch_add(1, Ordering::Relaxed);
                        if !me.healthy.load(Ordering::Acquire) { continue; }
                        let actions = me.node.lock().step(msg);
                        me.execute_actions(actions).await;
                    }
                    Some(req) = proposal_rx.recv() => {
                        let _driver = me.driver.lock().await;
                        me.handle_proposal(req).await;
                    }
                    Some(req) = read_rx.recv() => {
                        let _driver = me.driver.lock().await;
                        me.handle_read(req).await;
                    }
                }
                me.fail_stale_proposals();
            }
        });
    }

    fn fail_stale_proposals(&self) {
        let role = self.node.lock().role();
        self.pending
            .lock()
            .retain(|_, proposal| !proposal.tx.is_closed());
        self.drain_ready_reads();
        if !matches!(role, Role::Leader) {
            let mut pending = self.pending.lock();
            for (_, p) in pending.drain() {
                self.counters
                    .proposal_failures
                    .fetch_add(1, Ordering::Relaxed);
                let _ = p.tx.send(Err(ProposeFailure::NotLeader { hint: None }));
            }
        }
    }

    /// Latch failed storage state. An unsafe node must stop acknowledging work.
    fn fail_storage(&self, error: impl std::fmt::Display) {
        self.counters.storage_errors.fetch_add(1, Ordering::Relaxed);
        self.healthy.store(false, Ordering::Release);
        let message = format!("storage failure: {error}");
        self.failure.lock().get_or_insert_with(|| message.clone());
        tracing::error!(node = self.cfg.id, %message, "runtime stopped accepting Raft work");
        for (_, proposal) in self.pending.lock().drain() {
            self.counters
                .proposal_failures
                .fetch_add(1, Ordering::Relaxed);
            let _ = proposal
                .tx
                .send(Err(ProposeFailure::Other(message.clone())));
        }
        let cancelled: Vec<_> = self
            .pending_reads
            .lock()
            .drain(..)
            .map(|read| {
                let _ = read.tx.send(RespFrame::err(message.clone()));
                read.ctx
            })
            .collect();
        let mut node = self.node.lock();
        for ctx in cancelled {
            node.cancel_read(&ctx);
        }
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    async fn handle_proposal(self: &Arc<Self>, req: ProposalRequest) {
        if req.tx.is_closed() {
            return;
        }
        if !self.healthy.load(Ordering::Acquire) {
            let _ = req
                .tx
                .send(Err(ProposeFailure::Other("storage unavailable".into())));
            return;
        }
        if self.pending.lock().len() >= 1024 {
            self.counters
                .proposal_failures
                .fetch_add(1, Ordering::Relaxed);
            let _ = req.tx.send(Err(ProposeFailure::Other(
                "too many pending proposals".into(),
            )));
            return;
        }
        let acts_res = match req.kind {
            ProposalKind::Command(cmd) => {
                let bytes = bincode::serialize(&cmd).unwrap_or_default();
                let mut node = self.node.lock();
                let prev_last = node.last_log_index();
                match node.propose(bytes) {
                    Ok(acts) => Ok((prev_last + 1, acts)),
                    Err(e) => Err(e),
                }
            }
            ProposalKind::ConfigChange(cc) => {
                let mut node = self.node.lock();
                let prev_last = node.last_log_index();
                match node.propose_config_change(cc) {
                    Ok(acts) => Ok((prev_last + 1, acts)),
                    Err(e) => Err(e),
                }
            }
        };
        match acts_res {
            Ok((idx, acts)) => {
                self.pending
                    .lock()
                    .insert(idx, PendingProposal { tx: req.tx });
                self.execute_actions(acts).await;
            }
            Err(e) => {
                self.counters
                    .proposal_failures
                    .fetch_add(1, Ordering::Relaxed);
                let f = match e {
                    ProposeError::NotLeader { hint } => ProposeFailure::NotLeader {
                        hint: hint.and_then(|h| self.addr_book.get(&h).cloned()),
                    },
                    other => ProposeFailure::Other(other.to_string()),
                };
                let _ = req.tx.send(Err(f));
            }
        }
    }

    async fn handle_read(self: &Arc<Self>, req: ReadRequest) {
        if req.tx.is_closed() {
            return;
        }
        if !self.healthy.load(Ordering::Acquire) {
            let _ = req
                .tx
                .send(RespFrame::err("storage unavailable".to_owned()));
            return;
        }
        if self.pending_reads.lock().len() >= 1024 {
            let _ = req
                .tx
                .send(RespFrame::err("too many pending reads".to_owned()));
            return;
        }
        let ctx = bytes::Bytes::copy_from_slice(
            &self
                .read_sequence
                .fetch_add(1, Ordering::Relaxed)
                .to_le_bytes(),
        );
        let actions;
        {
            let mut node = self.node.lock();
            if !matches!(node.role(), Role::Leader) {
                let _ = req.tx.send(RespFrame::err("MOVED 0 ".to_string()));
                return;
            }
            self.pending_reads.lock().push(PendingRead {
                ctx: ctx.clone(),
                quorum_confirmed: false,
                term: node.hard_state().current_term,
                commit_target: node.commit_index(),
                tx: req.tx,
                f: req.f,
            });
            actions = node.read_index(ctx);
        }
        self.execute_actions(actions).await;
    }

    fn confirm_read(&self, ctx: &bytes::Bytes, commit_index: u64) {
        for read in self
            .pending_reads
            .lock()
            .iter_mut()
            .filter(|read| read.ctx == *ctx)
        {
            read.commit_target = commit_index;
            read.quorum_confirmed = true;
        }
    }

    fn drain_ready_reads(&self) {
        let (term, role) = {
            let node = self.node.lock();
            (node.hard_state().current_term, node.role())
        };
        let applied = self.sm.applied_index();
        let mut reads = self.pending_reads.lock();
        let mut still = Vec::with_capacity(reads.len());
        let mut cancelled = Vec::new();
        for read in reads.drain(..) {
            if read.tx.is_closed() {
                cancelled.push(read.ctx);
                continue;
            }
            if read.term != term || !matches!(role, Role::Leader) {
                cancelled.push(read.ctx.clone());
                let _ = read.tx.send(RespFrame::err("MOVED 0 ".to_string()));
            } else if read.quorum_confirmed && applied >= read.commit_target {
                let response = (read.f)(&self.sm);
                let _ = read.tx.send(response);
            } else {
                still.push(read);
            }
        }
        *reads = still;
        drop(reads);
        if !cancelled.is_empty() {
            let mut node = self.node.lock();
            for ctx in cancelled {
                node.cancel_read(&ctx);
            }
        }
    }

    async fn execute_actions(&self, actions: smallvec::SmallVec<[Action; 8]>) {
        for act in actions {
            if !self.healthy.load(Ordering::Acquire) {
                return;
            }
            match act {
                Action::PersistHardState(hs) => {
                    if let Err(e) = self.meta.save_hard_state(&hs) {
                        self.fail_storage(e);
                        return;
                    }
                }
                Action::AppendEntries(entries) => {
                    if let Err(e) = self.log.append(&entries) {
                        self.fail_storage(e);
                        return;
                    }
                }
                Action::TruncateLog { from } => {
                    if let Err(e) = self.log.truncate_from(from) {
                        self.fail_storage(e);
                        return;
                    }
                }
                Action::SendMessage { to, msg } => {
                    if let Some(client) = self.peers.get(&to).cloned() {
                        self.counters
                            .raft_messages_sent
                            .fetch_add(1, Ordering::Relaxed);
                        tokio::spawn(async move { client.send(msg).await });
                    }
                }
                Action::ApplyCommitted { entries } => {
                    self.apply_entries(entries).await;
                }
                Action::TakeSnapshot {
                    last_included_index,
                    last_included_term,
                } => {
                    if let Err(e) = self.take_snapshot(last_included_index, last_included_term) {
                        self.fail_storage(e);
                        return;
                    }
                }
                Action::InstallSnapshot {
                    last_included_index,
                    last_included_term,
                    ..
                } => {
                    if let Err(error) = self
                        .meta
                        .save_snapshot_pointer(last_included_index, last_included_term)
                    {
                        self.fail_storage(error);
                        return;
                    }
                    self.log.set_snapshot_tip(last_included_index);
                    self.node
                        .lock()
                        .on_snapshot_taken(last_included_index, last_included_term);
                }
                Action::ResetElectionTimer | Action::ResetHeartbeatTimer => {}
                Action::NotifyReadIndex {
                    ctx, commit_index, ..
                } => {
                    self.confirm_read(&ctx, commit_index);
                    self.drain_ready_reads();
                }
                Action::BecameLeader { term } => {
                    tracing::info!(term, node = self.cfg.id, "became leader");
                    self.refresh_soft();
                }
                Action::BecameFollower { term, leader } => {
                    tracing::info!(term, leader = ?leader, node = self.cfg.id, "became follower");
                    self.refresh_soft();
                }
                Action::Metric(ev) => self.observe(ev),
            }
        }
        self.drain_ready_reads();
        self.refresh_soft();
        self.notify.notify_waiters();
    }

    async fn apply_entries(&self, entries: Vec<Entry>) {
        for e in entries {
            if !self.healthy.load(Ordering::Acquire) {
                return;
            }
            let idx = e.index;
            if idx <= self.sm.applied_index() {
                continue;
            }
            let resp = match e.kind {
                EntryKind::Noop => self.advance_metadata_entry(idx),
                EntryKind::Normal => match bincode::deserialize::<Command>(&e.data) {
                    Ok(cmd) => match self.sm.apply(idx, &cmd) {
                        Ok(r) => r,
                        Err(err) => {
                            self.fail_storage(err);
                            return;
                        }
                    },
                    Err(error) => {
                        self.fail_storage(format!("invalid committed command: {error}"));
                        return;
                    }
                },
                EntryKind::ConfigJoint | EntryKind::ConfigNew => self.advance_metadata_entry(idx),
            };
            if !self.healthy.load(Ordering::Acquire) {
                return;
            }
            if let Some(p) = self.pending.lock().remove(&idx) {
                self.counters
                    .proposals_committed
                    .fetch_add(1, Ordering::Relaxed);
                let _ = p.tx.send(Ok(resp));
            }
        }
    }

    fn advance_metadata_entry(&self, index: u64) -> Response {
        match self.sm.advance_applied_index(index) {
            Ok(()) => Response::Ok,
            Err(error) => {
                self.fail_storage(&error);
                Response::Error(error.to_string())
            }
        }
    }

    fn take_snapshot(
        &self,
        last_included_index: u64,
        last_included_term: u64,
    ) -> anyhow::Result<()> {
        let meta = SnapshotMeta {
            last_included_index,
            last_included_term,
            config: vec![],
        };
        let writer = self.snaps.begin_write(&meta)?;
        writer.finish()?;
        self.meta
            .save_snapshot_pointer(last_included_index, last_included_term)?;
        self.log.compact(last_included_index)?;
        self.snaps.keep_last(3).ok();
        Ok(())
    }

    fn refresh_soft(&self) {
        let n = self.node.lock();
        let mut s = self.soft.lock();
        s.role = n.role().as_str().into();
        s.term = n.hard_state().current_term;
        s.leader = n.soft_state().leader_id;
        s.commit_index = n.commit_index();
        s.applied_index = self.sm.applied_index();
        s.last_log_index = n.last_log_index();
    }

    fn observe(&self, ev: MetricEvent) {
        match ev {
            MetricEvent::TermBumped { new_term } => tracing::info!(term = new_term, "term bumped"),
            MetricEvent::ElectionStarted { term, pre_vote } => {
                self.counters
                    .elections_started
                    .fetch_add(1, Ordering::Relaxed);
                tracing::info!(term, pre_vote, "election started")
            }
            MetricEvent::ElectionWon { term } => {
                self.counters.elections_won.fetch_add(1, Ordering::Relaxed);
                tracing::info!(term, "election won");
            }
            MetricEvent::CommitAdvanced { index } => tracing::debug!(index, "commit advanced"),
            MetricEvent::AppendRejected { from } => {
                self.counters
                    .append_rejections
                    .fetch_add(1, Ordering::Relaxed);
                tracing::debug!(from, "append rejected");
            }
            MetricEvent::LeadershipTransferStarted { target } => {
                tracing::info!(target, "leadership transfer")
            }
        }
    }

    /// Local node id.
    pub fn local_id(&self) -> NodeId {
        self.cfg.id
    }
}

/// gRPC handlers route inbound Raft RPCs through here. Each Raft RPC has
/// exactly one synchronous reply, so we step the core, harvest the response
/// `Action::SendMessage { to: <caller> }`, and return the body to the gRPC
/// handler — which sends it back over the same gRPC call.
#[async_trait]
impl MessageProcessor for Runtime {
    async fn process(&self, from: u64, msg: Message) -> Option<Message> {
        // Serialize transition + persistence + application with the driver loop.
        let _driver = self.driver.lock().await;
        self.counters
            .raft_messages_received
            .fetch_add(1, Ordering::Relaxed);
        if !self.healthy.load(Ordering::Acquire) {
            return None;
        }
        let actions = self.node.lock().step(msg);

        let mut response: Option<Message> = None;
        let mut other_actions: smallvec::SmallVec<[Action; 8]> = smallvec::SmallVec::new();
        for act in actions {
            match act {
                Action::SendMessage { to, msg }
                    if to == from && response.is_none() && is_response_kind(&msg) =>
                {
                    response = Some(msg);
                }
                a => other_actions.push(a),
            }
        }

        self.execute_actions(other_actions).await;
        if !self.healthy.load(Ordering::Acquire) {
            return None;
        }
        if response.is_some() {
            self.counters
                .raft_messages_sent
                .fetch_add(1, Ordering::Relaxed);
        }

        response
    }
}

fn is_response_kind(m: &Message) -> bool {
    matches!(
        m,
        Message::AppendEntriesResponse { .. }
            | Message::RequestVoteResponse { .. }
            | Message::InstallSnapshotResponse { .. }
    )
}

/// Channels owned by the run loop.
pub struct RuntimeHandles {
    /// Network -> runtime inbox sender.
    #[allow(dead_code)]
    pub inbox_tx: InboxTx,
    /// Network -> runtime inbox receiver.
    pub inbox_rx: Inbox,
    /// Proposals receiver.
    proposal_rx: mpsc::Receiver<ProposalRequest>,
    /// Reads receiver.
    read_rx: mpsc::Receiver<ReadRequest>,
}

/// Implements the resp-server CommandHandler trait via the runtime channels.
pub struct ClientHandler {
    rt: Arc<Runtime>,
}

impl ClientHandler {
    /// Construct.
    pub fn new(rt: Arc<Runtime>) -> Self {
        Self { rt }
    }

    pub fn status_json(&self) -> Value {
        let node = self.rt.node.lock();
        let leader = node.soft_state().leader_id;
        let voters = node.config().state.voters();
        let (old_voters, new_voters, joint) = match &node.config().state {
            raft_core::ConfigState::Stable(_) => (Vec::new(), Vec::new(), false),
            raft_core::ConfigState::Joint { old, new } => (
                old.iter().copied().collect::<Vec<_>>(),
                new.iter().copied().collect::<Vec<_>>(),
                true,
            ),
        };
        let mut ids: std::collections::BTreeSet<u64> = self.rt.addr_book.keys().copied().collect();
        ids.extend(voters.iter().copied());
        let nodes: Vec<Value> = ids.into_iter().map(|id| {
            let replication = if matches!(node.role(), Role::Leader) {
                node.replication_progress().get(id).map(|p| json!({
                    "match_index": p.match_index, "next_index": p.next_index,
                    "lag": node.last_log_index().saturating_sub(p.match_index),
                    "state": format!("{:?}", p.state).to_lowercase(), "inflight": p.inflight,
                    "recent_active": p.recent_active, "pending_snapshot": p.pending_snapshot,
                    "transport_inflight": self.rt.peers.get(&id).map(|peer| peer.active_requests()),
                    "transport_limit": self.rt.peers.get(&id).map(|peer| peer.max_concurrent_requests()),
                }))
            } else { None };
            json!({"id": id, "addr": self.rt.addr_book.get(&id), "local": id == self.rt.cfg.id,
                "role": if id == self.rt.cfg.id { node.role().as_str() } else if Some(id) == leader { "leader" } else { "unknown" },
                "voter": voters.contains(&id), "learner": false, "replication": replication})
        }).collect();
        let applied = self.rt.sm.applied_index();
        json!({
            "node_id": self.rt.cfg.id, "role": node.role().as_str(), "term": node.hard_state().current_term,
            "leader_id": leader, "commit_index": node.commit_index(), "applied_index": applied,
            "last_log_index": node.last_log_index(), "keys": self.rt.sm.len(),
            "state_hash": self.rt.sm.state_hash().iter().map(|b| format!("{b:02x}")).collect::<String>(), "nodes": nodes,
            "uptime_ms": self.rt.started.elapsed().as_millis() as u64, "observed_at_ms": Runtime::now_ms(),
            "version": env!("CARGO_PKG_VERSION"),
            "health": {"state": if self.rt.healthy.load(Ordering::Acquire) { "healthy" } else { "failed" },
                "storage_healthy": self.rt.healthy.load(Ordering::Acquire), "error": *self.rt.failure.lock()},
            "membership": {"voters": voters, "learners": [], "joint": joint,
                "quorum_size": if joint { None } else { Some(voters.len() / 2 + 1) },
                "old_voters": old_voters, "new_voters": new_voters},
            "endpoints": {"raft": self.rt.cfg.raft_listen, "client": self.rt.cfg.client_listen,
                "metrics": self.rt.cfg.metrics_listen, "api": self.rt.cfg.ui_listen},
            "config": {"election_timeout_ms": self.rt.cfg.election_timeout_ms, "heartbeat_ms": self.rt.cfg.heartbeat_ms,
                "tick_ms": self.rt.cfg.tick_ms, "pre_vote": self.rt.cfg.pre_vote,
                "snapshot_entries_threshold": self.rt.cfg.snapshot_entries_threshold,
                "max_append_entries": 256, "max_inflight_msgs": 1024, "transport_concurrency_limit": 32},
            "log": {"first_index": self.rt.log.first_index(), "last_index": node.last_log_index(),
                "last_term": node.last_log_term(), "snapshot_index": node.snapshot_index(), "snapshot_term": node.snapshot_term(),
                "uncommitted_entries": node.last_log_index().saturating_sub(node.commit_index()),
                "unapplied_entries": node.commit_index().saturating_sub(applied), "core_applied_index": node.last_applied()},
            "queues": {"pending_proposals": self.rt.pending.lock().len(), "pending_reads": self.rt.pending_reads.lock().len(),
                "proposal_capacity": 1024, "read_capacity": 1024,
                "queued_proposals": 1024 - self.rt.proposal_tx.capacity(), "queued_reads": 1024 - self.rt.read_tx.capacity()},
            "counters": self.rt.counters.json(),
            "capabilities": {"read_consistency": "local", "resp_read_consistency": "quorum_read_index",
                "live_membership": false, "leadership_transfer": false, "snapshot_state_transfer": false},
            "limitations": ["Dashboard reads inspect local applied state and may be stale.",
                "Remote roles and reachability require polling each node; replication activity is not a health probe.",
                "Live membership changes and snapshot state transfer are not exposed by this API.",
                "Runtime counters reset when the process restarts.",
                "State hash describes command history and is not a checksum of the current keyspace."]
        })
    }

    /// Prometheus exposition generated from the same observed state as the API.
    pub fn metrics_text(&self) -> String {
        use std::fmt::Write;
        let status = self.status_json();
        let mut text = String::new();
        for (name, value) in [
            ("term", status["term"].as_u64().unwrap_or(0) as f64),
            (
                "commit_index",
                status["commit_index"].as_u64().unwrap_or(0) as f64,
            ),
            (
                "applied_index",
                status["applied_index"].as_u64().unwrap_or(0) as f64,
            ),
            (
                "last_log_index",
                status["last_log_index"].as_u64().unwrap_or(0) as f64,
            ),
            ("keys", status["keys"].as_u64().unwrap_or(0) as f64),
            ("uptime_seconds", self.rt.started.elapsed().as_secs_f64()),
            (
                "storage_healthy",
                if self.rt.healthy.load(Ordering::Acquire) {
                    1.0
                } else {
                    0.0
                },
            ),
            (
                "is_leader",
                if status["role"] == "leader" { 1.0 } else { 0.0 },
            ),
        ] {
            let _ = writeln!(
                text,
                "# TYPE raftkv_{name} gauge\nraftkv_{name}{{node_id=\"{}\"}} {value}",
                self.rt.cfg.id
            );
        }
        if let Some(counters) = status["counters"].as_object() {
            for (name, value) in counters {
                let _ = writeln!(
                    text,
                    "# TYPE raftkv_{name}_total counter\nraftkv_{name}_total{{node_id=\"{}\"}} {}",
                    self.rt.cfg.id,
                    value.as_u64().unwrap_or(0)
                );
            }
        }
        text
    }

    /// Bounded tail of retained local log metadata; values are never returned.
    pub fn log_json(&self) -> Result<Value, (u16, String)> {
        if !self.rt.healthy.load(Ordering::Acquire) {
            return Err((503, "storage unavailable".into()));
        }
        let (commit, applied) = (
            self.rt.node.lock().commit_index(),
            self.rt.sm.applied_index(),
        );
        let (first, last) = (self.rt.log.first_index(), self.rt.log.last_index());
        let start = first.max(last.saturating_sub(99));
        let mut entries = Vec::new();
        for index in start..=last {
            if let Some(entry) = self
                .rt
                .log
                .read(index)
                .map_err(|error| (500, error.to_string()))?
            {
                entries.push(json!({"index": entry.index, "term": entry.term,
                    "kind": format!("{:?}", entry.kind).to_lowercase(), "payload_bytes": entry.data.len(),
                    "committed": entry.index <= commit, "applied": entry.index <= applied}));
            }
        }
        Ok(
            json!({"entries": entries, "limit": 100, "first_index": first, "last_index": last,
            "consistency": "local", "node_id": self.rt.cfg.id}),
        )
    }

    /// Dashboard inspection reads explicitly return local applied state.
    pub async fn keys_json(&self) -> Result<Value, (u16, String)> {
        if !self.rt.healthy.load(Ordering::Acquire) {
            return Err((503, "storage unavailable".into()));
        }
        let keys = self
            .rt
            .sm
            .keys()
            .map_err(|error| (500, error.to_string()))?;
        Ok(
            json!({"keys": keys, "consistency": "local", "node_id": self.rt.cfg.id, "applied_index": self.rt.sm.applied_index()}),
        )
    }

    pub async fn get_json(&self, key: &str) -> Result<Value, (u16, String)> {
        if !self.rt.healthy.load(Ordering::Acquire) {
            return Err((503, "storage unavailable".into()));
        }
        let value = self
            .rt
            .sm
            .get(key.as_bytes())
            .map_err(|error| (500, error.to_string()))?
            .map(|value| String::from_utf8_lossy(&value).into_owned());
        Ok(
            json!({"value": value, "consistency": "local", "node_id": self.rt.cfg.id, "applied_index": self.rt.sm.applied_index()}),
        )
    }

    pub async fn set_json(&self, key: &str, body: &str) -> Result<Value, (u16, String)> {
        let payload: Value = serde_json::from_str(body)
            .map_err(|_| (400, "expected JSON object with string value".into()))?;
        let value = payload
            .get("value")
            .and_then(Value::as_str)
            .ok_or_else(|| (400, "value must be a string".into()))?
            .to_owned();
        match self
            .propose(Command::Set {
                key: key.as_bytes().to_vec(),
                value: value.as_bytes().to_vec(),
                expire_at_ms: None,
            })
            .await
        {
            Ok(Response::Error(error)) => Err((500, error)),
            Ok(_) => Ok(json!({"ok": true, "key": key, "value": value})),
            Err(ProposeFailure::NotLeader { .. }) => Err((
                503,
                format!(
                    "not leader; observed leader: {:?}",
                    self.rt.node.lock().soft_state().leader_id
                ),
            )),
            Err(e) => Err((503, e.to_string())),
        }
    }

    pub async fn delete_json(&self, key: &str) -> Result<Value, (u16, String)> {
        match self
            .propose(Command::Del {
                keys: vec![key.as_bytes().to_vec()],
            })
            .await
        {
            Ok(Response::Error(error)) => Err((500, error)),
            Ok(Response::Int(removed)) => Ok(json!({"ok": true, "removed": removed > 0})),
            Ok(_) => Ok(json!({"ok": true})),
            Err(ProposeFailure::NotLeader { .. }) => Err((
                503,
                format!(
                    "not leader; observed leader: {:?}",
                    self.rt.node.lock().soft_state().leader_id
                ),
            )),
            Err(e) => Err((503, e.to_string())),
        }
    }
}

#[async_trait]
impl CommandHandler for ClientHandler {
    async fn propose(&self, cmd: Command) -> Result<Response, ProposeFailure> {
        if !self.rt.healthy.load(Ordering::Acquire) {
            return Err(ProposeFailure::Other("storage unavailable".into()));
        }
        let (tx, rx) = oneshot::channel();
        let req = ProposalRequest {
            kind: ProposalKind::Command(cmd),
            tx,
        };
        self.rt
            .counters
            .proposals_received
            .fetch_add(1, Ordering::Relaxed);
        if self.rt.proposal_tx.try_send(req).is_err() {
            self.rt
                .counters
                .proposal_failures
                .fetch_add(1, Ordering::Relaxed);
            return Err(ProposeFailure::Other(
                "proposal queue full or runtime stopped".into(),
            ));
        }
        match tokio::time::timeout(Duration::from_secs(5), rx).await {
            Ok(Ok(res)) => res,
            Ok(Err(_)) => Err(ProposeFailure::Other("dropped".into())),
            Err(_) => Err(ProposeFailure::Timeout),
        }
    }

    async fn linearizable_read<F>(&self, f: F) -> Result<RespFrame, ProposeFailure>
    where
        F: FnOnce(&KvStateMachine) -> RespFrame + Send + 'static,
    {
        if !self.rt.healthy.load(Ordering::Acquire) {
            return Err(ProposeFailure::Other("storage unavailable".into()));
        }
        let (tx, rx) = oneshot::channel();
        let req = ReadRequest { f: Box::new(f), tx };
        self.rt
            .counters
            .reads_received
            .fetch_add(1, Ordering::Relaxed);
        if self.rt.read_tx.try_send(req).is_err() {
            return Err(ProposeFailure::Other(
                "read queue full or runtime stopped".into(),
            ));
        }
        match tokio::time::timeout(Duration::from_secs(5), rx).await {
            Ok(Ok(frame)) => Ok(frame),
            Ok(Err(_)) => Err(ProposeFailure::Other("dropped".into())),
            Err(_) => Err(ProposeFailure::Timeout),
        }
    }

    fn is_leader(&self) -> bool {
        self.rt.healthy.load(Ordering::Acquire)
            && matches!(self.rt.node.lock().role(), Role::Leader)
    }

    fn info(&self, _section: Option<&str>) -> String {
        let s = self.rt.soft.lock();
        format!(
            "# Server\nraftkv_version:0.1.0\n# Raft\nrole:{}\nterm:{}\nleader_id:{}\ncommit_index:{}\napplied_index:{}\nlast_log_index:{}\n",
            s.role, s.term, s.leader.unwrap_or(0), s.commit_index, s.applied_index, s.last_log_index,
        )
    }

    fn cluster_nodes(&self) -> String {
        self.rt
            .addr_book
            .iter()
            .map(|(id, addr)| format!("{} {}", id, addr))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static TEST_DIRECTORY_ID: AtomicU64 = AtomicU64::new(0);

    async fn test_runtime() -> (Arc<Runtime>, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "raftkv-runtime-test-{}-{}-{}",
            std::process::id(),
            TEST_DIRECTORY_ID.fetch_add(1, Ordering::Relaxed),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let cfg = ServerConfig {
            data_dir: path.clone(),
            ..ServerConfig::default()
        };
        let (runtime, _handles) = Runtime::new(cfg).await.unwrap();
        (runtime, path)
    }

    async fn elect_single_node(runtime: &Arc<Runtime>) {
        for _ in 0..100 {
            let actions = runtime.node.lock().tick();
            runtime.execute_actions(actions).await;
            if matches!(runtime.node.lock().role(), Role::Leader) {
                return;
            }
        }
        panic!("single node did not elect itself");
    }

    #[tokio::test]
    async fn confirmed_read_requires_quorum_and_applied_index() {
        let (runtime, path) = test_runtime().await;
        elect_single_node(&runtime).await;
        assert_eq!(
            runtime.sm.applied_index(),
            runtime.node.lock().commit_index()
        );
        let (tx, mut rx) = oneshot::channel();
        let term = runtime.node.lock().hard_state().current_term;
        let target = runtime.sm.applied_index();
        runtime.pending_reads.lock().push(PendingRead {
            ctx: bytes::Bytes::from_static(b"round"),
            quorum_confirmed: false,
            term,
            commit_target: target,
            tx,
            f: Box::new(|_| RespFrame::Integer(42)),
        });
        runtime.drain_ready_reads();
        assert!(matches!(
            rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        runtime.confirm_read(&bytes::Bytes::from_static(b"other-round"), target);
        runtime.drain_ready_reads();
        assert!(matches!(
            rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        runtime.confirm_read(&bytes::Bytes::from_static(b"round"), target);
        runtime.drain_ready_reads();
        assert!(matches!(rx.await.unwrap(), RespFrame::Integer(42)));
        drop(runtime);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn nonleader_read_rejected_but_local_inspection_labelled() {
        let (runtime, path) = test_runtime().await;
        let handler = ClientHandler::new(runtime.clone());
        let (tx, rx) = oneshot::channel();
        runtime
            .handle_read(ReadRequest {
                tx,
                f: Box::new(|_| RespFrame::Integer(1)),
            })
            .await;
        assert!(matches!(rx.await.unwrap(), RespFrame::Error(_)));
        let response = handler.keys_json().await.unwrap();
        assert_eq!(response["consistency"], "local");
        let status = handler.status_json();
        assert_eq!(status["membership"]["quorum_size"], 1);
        assert_eq!(status["capabilities"]["live_membership"], false);
        assert_eq!(status["nodes"][0]["replication"], Value::Null);
        drop(handler);
        drop(runtime);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn committed_command_replay_is_idempotent() {
        let (runtime, path) = test_runtime().await;
        let command = Command::Incr {
            key: b"count".to_vec(),
            delta: 1,
        };
        let entry = Entry::normal(1, 1, bincode::serialize(&command).unwrap());
        runtime.apply_entries(vec![entry.clone()]).await;
        runtime.apply_entries(vec![entry]).await;
        assert_eq!(runtime.sm.get(b"count").unwrap(), Some(b"1".to_vec()));
        drop(runtime);
        std::fs::remove_dir_all(path).unwrap();
    }
    #[tokio::test]
    async fn failed_storage_aborts_batch_and_rejects_clients() {
        let (runtime, path) = test_runtime().await;
        let (proposal_tx, proposal_rx) = oneshot::channel();
        runtime
            .pending
            .lock()
            .insert(1, PendingProposal { tx: proposal_tx });
        let (read_tx, read_rx) = oneshot::channel();
        runtime.pending_reads.lock().push(PendingRead {
            ctx: bytes::Bytes::from_static(b"failed-round"),
            quorum_confirmed: false,
            term: 0,
            commit_target: 1,
            tx: read_tx,
            f: Box::new(|_| RespFrame::Integer(0)),
        });
        runtime
            .execute_actions(smallvec::smallvec![
                Action::ApplyCommitted {
                    entries: vec![Entry::normal(1, 1, vec![255])]
                },
                Action::AppendEntries(vec![Entry::noop(1, 2)]),
            ])
            .await;
        assert!(!runtime.healthy.load(Ordering::Acquire));
        assert!(proposal_rx.await.unwrap().is_err());
        assert!(matches!(read_rx.await.unwrap(), RespFrame::Error(_)));
        assert_eq!(runtime.log.last_index(), 0);
        assert_eq!(runtime.sm.applied_index(), 0);
        let handler = ClientHandler::new(runtime.clone());
        assert_eq!(handler.keys_json().await.unwrap_err().0, 503);
        assert!(handler
            .propose(Command::Incr {
                key: b"x".to_vec(),
                delta: 1
            })
            .await
            .is_err());
        assert_eq!(handler.status_json()["health"]["state"], "failed");
        assert!(handler
            .metrics_text()
            .contains("raftkv_storage_healthy{node_id=\"1\"} 0"));
        drop(handler);
        drop(runtime);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn same_term_demotion_fails_pending_proposal() {
        let (runtime, path) = test_runtime().await;
        elect_single_node(&runtime).await;
        let (tx, rx) = oneshot::channel();
        runtime.pending.lock().insert(99, PendingProposal { tx });
        let (term, prev_log_index, prev_log_term, leader_commit) = {
            let node = runtime.node.lock();
            (
                node.hard_state().current_term,
                node.last_log_index(),
                node.last_log_term(),
                node.commit_index(),
            )
        };
        let actions = runtime.node.lock().step(Message::AppendEntries {
            from: 2,
            term,
            prev_log_index,
            prev_log_term,
            entries: Vec::new(),
            leader_commit,
            request_id: 1,
        });
        runtime.execute_actions(actions).await;
        runtime.fail_stale_proposals();
        assert!(matches!(
            rx.await.unwrap(),
            Err(ProposeFailure::NotLeader { .. })
        ));
        drop(runtime);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn log_tail_is_bounded_and_excludes_payloads() {
        let (runtime, path) = test_runtime().await;
        let entries: Vec<_> = (1..=120)
            .map(|index| Entry::normal(1, index, b"private-value".to_vec()))
            .collect();
        runtime.log.append(&entries).unwrap();
        let handler = ClientHandler::new(runtime.clone());
        let tail = handler.log_json().unwrap();
        assert_eq!(tail["entries"].as_array().unwrap().len(), 100);
        assert_eq!(tail["entries"][0]["index"], 21);
        assert_eq!(tail["entries"][99]["index"], 120);
        assert!(!tail.to_string().contains("private-value"));
        assert!(handler
            .metrics_text()
            .contains("# TYPE raftkv_proposals_received_total counter"));
        drop(handler);
        drop(runtime);
        std::fs::remove_dir_all(path).unwrap();
    }
}
