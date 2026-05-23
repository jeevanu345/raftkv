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
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use kv_state_machine::{Command, KvStateMachine, Response};
use parking_lot::Mutex;
use raft_core::{
    Action, ConfigChange, Entry, EntryKind, Message, MetricEvent, NodeId, ProposeError,
    RaftConfig, RaftLog, RaftNode, Role,
};
use raft_net::{client::PeerClient, server::MessageProcessor, Inbox, InboxTx};
use raft_storage::{MetaStore, SegmentedLog, SnapshotMeta, SnapshotStore};
use resp_server::handler::{CommandHandler, ProposeFailure};
use resp_server::codec::RespFrame;
use tokio::sync::{mpsc, oneshot, Notify};

use crate::config::ServerConfig;

/// Per-proposal waiter info.
struct PendingProposal {
    expected_index: Option<u64>,
    tx: oneshot::Sender<Result<Response, ProposeFailure>>,
}

/// Read-index waiter (linearizable read).
struct PendingRead {
    commit_target: u64,
    tx: oneshot::Sender<RespFrame>,
    f: Box<dyn FnOnce(&KvStateMachine) -> RespFrame + Send>,
}

/// Proposal request enqueued from the client task.
enum ProposalKind {
    Command(Command),
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
    node: Mutex<RaftNode>,
    log: Arc<SegmentedLog>,
    meta: Arc<MetaStore>,
    snaps: Arc<SnapshotStore>,
    sm: Arc<KvStateMachine>,
    peers: HashMap<NodeId, PeerClient>,
    proposal_tx: mpsc::UnboundedSender<ProposalRequest>,
    read_tx: mpsc::UnboundedSender<ReadRequest>,
    notify: Arc<Notify>,
    soft: Mutex<SoftCache>,
    addr_book: HashMap<NodeId, String>,
    pending: Mutex<HashMap<u64, PendingProposal>>,
    pending_reads: Mutex<Vec<PendingRead>>,
    last_term: Mutex<u64>,
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
        std::fs::create_dir_all(&cfg.data_dir)?;
        let log = Arc::new(SegmentedLog::open(raft_storage::segmented_log::SegmentedLogConfig {
            dir: cfg.data_dir.join("log"),
            max_segment_bytes: 64 * 1024 * 1024,
            sync_each_append: false,
        })?);
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

        let (proposal_tx, proposal_rx) = mpsc::unbounded_channel();
        let (read_tx, read_rx) = mpsc::unbounded_channel();
        let (inbox_tx, inbox_rx): (InboxTx, Inbox) = mpsc::unbounded_channel();
        let notify = Arc::new(Notify::new());

        let mut peers: HashMap<NodeId, PeerClient> = HashMap::new();
        let mut addr_book: HashMap<NodeId, String> = HashMap::new();
        for p in &cfg.peers {
            addr_book.insert(p.id, p.raft_addr.clone());
            if p.id != cfg.id {
                peers.insert(p.id, PeerClient::new(p.id, p.raft_addr.clone(), inbox_tx.clone()));
            }
        }

        let rt = Arc::new(Self {
            cfg,
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
            last_term: Mutex::new(0),
        });

        Ok((rt.clone(), RuntimeHandles { inbox_tx, inbox_rx, proposal_rx, read_rx }))
    }

    /// Spawn the runtime loop.
    pub fn spawn(self: Arc<Self>, handles: RuntimeHandles) {
        let RuntimeHandles { inbox_tx: _, mut inbox_rx, mut proposal_rx, mut read_rx } = handles;
        let me = self.clone();
        tokio::spawn(async move {
            let tick = Duration::from_millis(me.cfg.tick_ms);
            let mut tick_iv = tokio::time::interval(tick);
            tick_iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    _ = tick_iv.tick() => {
                        let actions = me.node.lock().tick();
                        me.execute_actions(actions).await;
                    }
                    Some(msg) = inbox_rx.recv() => {
                        let actions = me.node.lock().step(msg);
                        me.execute_actions(actions).await;
                    }
                    Some(req) = proposal_rx.recv() => {
                        me.handle_proposal(req).await;
                    }
                    Some(req) = read_rx.recv() => {
                        me.handle_read(req).await;
                    }
                }
                me.fail_stale_proposals();
            }
        });
    }

    fn fail_stale_proposals(&self) {
        let (term, role) = {
            let n = self.node.lock();
            (n.hard_state().current_term, n.role())
        };
        let mut last = self.last_term.lock();
        if term != *last && !matches!(role, Role::Leader) {
            let mut pending = self.pending.lock();
            for (_, p) in pending.drain() {
                let _ = p.tx.send(Err(ProposeFailure::NotLeader { hint: None }));
            }
            *last = term;
        }
    }

    fn now_ms() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
    }

    async fn handle_proposal(self: &Arc<Self>, req: ProposalRequest) {
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
                self.pending.lock().insert(idx, PendingProposal { expected_index: Some(idx), tx: req.tx });
                self.execute_actions(acts).await;
            }
            Err(e) => {
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
        let acts;
        let commit_target;
        let is_leader;
        {
            let mut node = self.node.lock();
            is_leader = matches!(node.role(), Role::Leader);
            commit_target = node.commit_index();
            acts = if is_leader { node.read_index(bytes::Bytes::new()) } else { Default::default() };
        }
        if !is_leader {
            let voters_only_self = self.cfg.peers.len() == 1;
            if voters_only_self {
                let resp = (req.f)(&self.sm);
                let _ = req.tx.send(resp);
                return;
            }
            let _ = req.tx.send(RespFrame::err("MOVED 0 ".to_string()));
            return;
        }
        self.pending_reads.lock().push(PendingRead { commit_target, tx: req.tx, f: req.f });
        self.execute_actions(acts).await;
        self.drain_ready_reads();
    }

    fn drain_ready_reads(&self) {
        let applied = self.sm.applied_index();
        let mut reads = self.pending_reads.lock();
        let mut still: Vec<PendingRead> = Vec::with_capacity(reads.len());
        for r in reads.drain(..) {
            if applied >= r.commit_target {
                let resp = (r.f)(&self.sm);
                let _ = r.tx.send(resp);
            } else {
                still.push(r);
            }
        }
        *reads = still;
    }

    async fn execute_actions(self: &Arc<Self>, actions: smallvec::SmallVec<[Action; 8]>) {
        for act in actions {
            match act {
                Action::PersistHardState(hs) => {
                    if let Err(e) = self.meta.save_hard_state(&hs) {
                        tracing::error!(error = %e, "failed to persist hard state");
                    }
                }
                Action::AppendEntries(entries) => {
                    if let Err(e) = self.log.append(&entries) {
                        tracing::error!(error = %e, "log append failed");
                    }
                }
                Action::TruncateLog { from } => {
                    if let Err(e) = self.log.truncate_from(from) {
                        tracing::error!(error = %e, "log truncate failed");
                    }
                }
                Action::SendMessage { to, msg } => {
                    if let Some(client) = self.peers.get(&to).cloned() {
                        tokio::spawn(async move { client.send(msg).await });
                    }
                }
                Action::ApplyCommitted { entries } => {
                    self.apply_entries(entries).await;
                }
                Action::TakeSnapshot { last_included_index, last_included_term } => {
                    if let Err(e) = self.take_snapshot(last_included_index, last_included_term) {
                        tracing::error!(error = %e, "snapshot failed");
                    }
                }
                Action::InstallSnapshot { last_included_index, last_included_term, .. } => {
                    let _ = self.meta.save_snapshot_pointer(last_included_index, last_included_term);
                    self.log.set_snapshot_tip(last_included_index);
                    self.node.lock().on_snapshot_taken(last_included_index, last_included_term);
                }
                Action::ResetElectionTimer | Action::ResetHeartbeatTimer => {}
                Action::NotifyReadIndex { .. } => {
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

    async fn apply_entries(self: &Arc<Self>, entries: Vec<Entry>) {
        for e in entries {
            let idx = e.index;
            let resp = match e.kind {
                EntryKind::Noop => Response::Ok,
                EntryKind::Normal => match bincode::deserialize::<Command>(&e.data) {
                    Ok(cmd) => match self.sm.apply(idx, &cmd) {
                        Ok(r) => r,
                        Err(err) => Response::Error(err.to_string()),
                    },
                    Err(_) => Response::Error("bad command encoding".into()),
                },
                EntryKind::ConfigJoint | EntryKind::ConfigNew => Response::Ok,
            };
            if let Some(p) = self.pending.lock().remove(&idx) {
                let _ = p.tx.send(Ok(resp));
            }
        }
    }

    fn take_snapshot(&self, last_included_index: u64, last_included_term: u64) -> anyhow::Result<()> {
        let meta = SnapshotMeta {
            last_included_index, last_included_term, config: vec![],
        };
        let writer = self.snaps.begin_write(&meta)?;
        writer.finish()?;
        self.meta.save_snapshot_pointer(last_included_index, last_included_term)?;
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
            MetricEvent::ElectionStarted { term, pre_vote } => tracing::info!(term, pre_vote, "election started"),
            MetricEvent::ElectionWon { term } => tracing::info!(term, "election won"),
            MetricEvent::CommitAdvanced { index } => tracing::debug!(index, "commit advanced"),
            MetricEvent::AppendRejected { from } => tracing::debug!(from, "append rejected"),
            MetricEvent::LeadershipTransferStarted { target } => tracing::info!(target, "leadership transfer"),
        }
    }

    /// Local node id.
    pub fn local_id(&self) -> NodeId { self.cfg.id }
}

/// gRPC handlers route inbound Raft RPCs through here. Each Raft RPC has
/// exactly one synchronous reply, so we step the core, harvest the response
/// `Action::SendMessage { to: <caller> }`, and return the body to the gRPC
/// handler — which sends it back over the same gRPC call.
#[async_trait]
impl MessageProcessor for Runtime {
    async fn process(&self, from: u64, msg: Message) -> Option<Message> {
        let actions = self.node.lock().step(msg);

        let mut response: Option<Message> = None;
        let mut other_actions: smallvec::SmallVec<[Action; 8]> = smallvec::SmallVec::new();
        for act in actions {
            match act {
                Action::SendMessage { to, msg } if to == from && response.is_none() && is_response_kind(&msg) => {
                    response = Some(msg);
                }
                a => other_actions.push(a),
            }
        }

        // Build an Arc-self for execute_actions reuse via a ref-counted shim.
        // We can't trivially get an `Arc<Self>` from `&self`, so we inline the
        // effects we care about here.
        for act in other_actions {
            match act {
                Action::PersistHardState(hs) => {
                    if let Err(e) = self.meta.save_hard_state(&hs) {
                        tracing::error!(error = %e, "failed to persist hard state");
                    }
                }
                Action::AppendEntries(entries) => {
                    if let Err(e) = self.log.append(&entries) {
                        tracing::error!(error = %e, "log append failed");
                    }
                }
                Action::TruncateLog { from } => {
                    if let Err(e) = self.log.truncate_from(from) {
                        tracing::error!(error = %e, "log truncate failed");
                    }
                }
                Action::SendMessage { to, msg } => {
                    if let Some(client) = self.peers.get(&to).cloned() {
                        tokio::spawn(async move { client.send(msg).await });
                    }
                }
                Action::ApplyCommitted { entries } => {
                    for e in entries {
                        let idx = e.index;
                        let resp = match e.kind {
                            EntryKind::Noop => Response::Ok,
                            EntryKind::Normal => match bincode::deserialize::<Command>(&e.data) {
                                Ok(cmd) => match self.sm.apply(idx, &cmd) {
                                    Ok(r) => r,
                                    Err(err) => Response::Error(err.to_string()),
                                },
                                Err(_) => Response::Error("bad command encoding".into()),
                            },
                            EntryKind::ConfigJoint | EntryKind::ConfigNew => Response::Ok,
                        };
                        if let Some(p) = self.pending.lock().remove(&idx) {
                            let _ = p.tx.send(Ok(resp));
                        }
                    }
                }
                Action::TakeSnapshot { last_included_index, last_included_term } => {
                    if let Err(e) = self.take_snapshot(last_included_index, last_included_term) {
                        tracing::error!(error = %e, "snapshot failed");
                    }
                }
                Action::InstallSnapshot { last_included_index, last_included_term, .. } => {
                    let _ = self.meta.save_snapshot_pointer(last_included_index, last_included_term);
                    self.log.set_snapshot_tip(last_included_index);
                    self.node.lock().on_snapshot_taken(last_included_index, last_included_term);
                }
                Action::ResetElectionTimer | Action::ResetHeartbeatTimer => {}
                Action::NotifyReadIndex { .. } => {
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
    pub inbox_tx: InboxTx,
    /// Network -> runtime inbox receiver.
    pub inbox_rx: Inbox,
    /// Proposals receiver.
    pub(crate) proposal_rx: mpsc::UnboundedReceiver<ProposalRequest>,
    /// Reads receiver.
    pub(crate) read_rx: mpsc::UnboundedReceiver<ReadRequest>,
}

/// Implements the resp-server CommandHandler trait via the runtime channels.
pub struct ClientHandler {
    rt: Arc<Runtime>,
}

impl ClientHandler {
    /// Construct.
    pub fn new(rt: Arc<Runtime>) -> Self { Self { rt } }
}

#[async_trait]
impl CommandHandler for ClientHandler {
    async fn propose(&self, cmd: Command) -> Result<Response, ProposeFailure> {
        let (tx, rx) = oneshot::channel();
        let req = ProposalRequest { kind: ProposalKind::Command(cmd), tx };
        if self.rt.proposal_tx.send(req).is_err() {
            return Err(ProposeFailure::Other("runtime gone".into()));
        }
        match tokio::time::timeout(Duration::from_secs(5), rx).await {
            Ok(Ok(res)) => res,
            Ok(Err(_)) => Err(ProposeFailure::Other("dropped".into())),
            Err(_) => Err(ProposeFailure::Timeout),
        }
    }

    async fn linearizable_read<F>(&self, f: F) -> Result<RespFrame, ProposeFailure>
    where F: FnOnce(&KvStateMachine) -> RespFrame + Send + 'static {
        let (tx, rx) = oneshot::channel();
        let req = ReadRequest { f: Box::new(f), tx };
        if self.rt.read_tx.send(req).is_err() {
            return Err(ProposeFailure::Other("runtime gone".into()));
        }
        match tokio::time::timeout(Duration::from_secs(5), rx).await {
            Ok(Ok(frame)) => Ok(frame),
            Ok(Err(_)) => Err(ProposeFailure::Other("dropped".into())),
            Err(_) => Err(ProposeFailure::Timeout),
        }
    }

    fn is_leader(&self) -> bool {
        matches!(self.rt.node.lock().role(), Role::Leader)
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
