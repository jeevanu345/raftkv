//! Serialized core transitions and one durable action executor for all transports.
use crate::{config::ServerConfig, metrics::Metrics};
use async_trait::async_trait;
use bytes::Bytes;
use kv_state_machine::{Command, KvStateMachine, Response};
use parking_lot::Mutex;
use raft_core::{
    config::Member, Action, ConfigChange, Entry, EntryKind, Message, MetricEvent, NodeId,
    ProposeError, RaftConfig, RaftLog, RaftNode, Role,
};
use raft_net::{client::PeerClient, server::MessageProcessor, Inbox, InboxTx};
use raft_storage::{MetaStore, SegmentedLog, SnapshotMeta, SnapshotStore};
use resp_server::{
    codec::RespFrame,
    handler::{CommandHandler, ProposeFailure},
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{broadcast, mpsc, oneshot, Semaphore};

struct PendingRead {
    index: Option<u64>,
    term: u64,
    tx: oneshot::Sender<RespFrame>,
    f: Box<dyn FnOnce(&KvStateMachine) -> RespFrame + Send>,
}
enum ProposalKind {
    Command(Command),
    Config(ConfigChange),
    Member(Member),
    RemoveLearner(u64),
}
struct ProposalRequest {
    kind: ProposalKind,
    tx: oneshot::Sender<Result<(Response, Value), ProposeFailure>>,
}
struct ReadRequest {
    f: Box<dyn FnOnce(&KvStateMachine) -> RespFrame + Send>,
    tx: oneshot::Sender<RespFrame>,
}
use raft_storage::backup::{Backup, SnapshotPackage};

pub struct Runtime {
    pub(crate) cfg: ServerConfig,
    node: Mutex<RaftNode>,
    // Covers step/propose plus every action. No RPC may ACK before persistence.
    gate: Mutex<()>,
    membership_gate: tokio::sync::Mutex<()>,
    contacts: Mutex<BTreeMap<NodeId, (u64, Instant)>>,
    log: Arc<SegmentedLog>,
    meta: Arc<MetaStore>,
    snaps: Arc<SnapshotStore>,
    pub(crate) sm: Arc<KvStateMachine>,
    peers: Mutex<HashMap<NodeId, PeerClient>>,
    pub(crate) members: Mutex<BTreeMap<NodeId, Member>>,
    inbox_tx: InboxTx,
    peer_tls: Option<tonic::transport::ClientTlsConfig>,
    proposal_tx: mpsc::Sender<ProposalRequest>,
    read_tx: mpsc::Sender<ReadRequest>,
    pending: Mutex<
        HashMap<
            u64,
            (
                u64,
                Instant,
                oneshot::Sender<Result<(Response, Value), ProposeFailure>>,
            ),
        >,
    >,
    reads: Mutex<HashMap<u64, PendingRead>>,
    next_read: AtomicU64,
    clock: AtomicU64,
    failed: AtomicBool,
    started: Instant,
    outbound: Arc<Semaphore>,
    pub(crate) metrics: Metrics,
    audit_file: Mutex<std::fs::File>,
    pub(crate) events: broadcast::Sender<Value>,
    pub(crate) event_history: Mutex<VecDeque<Value>>,
    event_seq: AtomicU64,
    last_size_scan: AtomicU64,
}
pub struct RuntimeHandles {
    inbox_rx: Inbox,
    proposal_rx: mpsc::Receiver<ProposalRequest>,
    read_rx: mpsc::Receiver<ReadRequest>,
}
impl Runtime {
    pub async fn new(cfg: ServerConfig) -> anyhow::Result<(Arc<Self>, RuntimeHandles)> {
        anyhow::ensure!(cfg.tick_ms > 0, "tick_ms must be positive");
        std::fs::create_dir_all(&cfg.data_dir)?;
        let log = Arc::new(SegmentedLog::open(
            raft_storage::segmented_log::SegmentedLogConfig {
                dir: cfg.data_dir.join("log"),
                max_segment_bytes: 64 * 1024 * 1024,
                sync_each_append: true,
            },
        )?);
        let meta = Arc::new(MetaStore::open(cfg.data_dir.join("meta"))?);
        let snaps = Arc::new(SnapshotStore::open(cfg.data_dir.join("snapshots"))?);
        let sm = Arc::new(KvStateMachine::open(cfg.data_dir.join("kv"))?);
        let mut hs = meta.load_hard_state()?;
        let mut snap = meta.load_snapshot_pointer()?.unwrap_or((0, 0));
        let mut snapshot_config = None;
        if let Some((snapshot_meta, path)) = snaps.latest() {
            if snapshot_meta.last_included_index >= snap.0 {
                let bytes = snaps.read_checked(&path)?;
                let package: SnapshotPackage = bincode::deserialize(&bytes)?;
                anyhow::ensure!(
                    package.version == 1 && package.index == snapshot_meta.last_included_index,
                    "invalid snapshot package"
                );
                KvStateMachine::validate_snapshot(&package.state, package.index)?;
                anyhow::ensure!(
                    package.term == snapshot_meta.last_included_term
                        && bincode::serialize(&package.config)? == snapshot_meta.config,
                    "snapshot metadata mismatch"
                );
                // A crash between state install and metadata publication is repaired
                // from the fully fsynced snapshot package before serving requests.
                if package.index > sm.applied_index() {
                    sm.restore(&package.state, package.index)?;
                }
                snap = (package.index, package.term);
                snapshot_config = Some(package.config);
                hs.commit_index = hs.commit_index.max(snap.0);
                meta.save_hard_state(&hs)?;
                meta.save_snapshot_pointer(snap.0, snap.1)?;
            }
        }
        anyhow::ensure!(
            snap.0 == 0 || snaps.latest().is_some(),
            "snapshot pointer has no complete checkpoint"
        );
        log.set_snapshot_tip(snap.0);
        let mut raft_log = RaftLog::from_snapshot(snap.0, snap.1);
        let mut entries = vec![];
        log.read_range(snap.0 + 1, log.last_index() + 1, &mut entries)?;
        raft_log.append(entries);
        let core_cfg = RaftConfig {
            id: cfg.id,
            initial_voters: cfg
                .peers
                .iter()
                .filter(|p| !p.learner)
                .map(|p| p.id)
                .collect(),
            election_timeout_ticks: (cfg.election_timeout_ms / cfg.tick_ms).max(1),
            heartbeat_ticks: (cfg.heartbeat_ms / cfg.tick_ms).max(1),
            max_append_entries: 256,
            max_inflight_msgs: 64,
            rng_seed: 0x600D_F00D ^ cfg.id,
            pre_vote: cfg.pre_vote,
        };
        let mut node = RaftNode::restore(core_cfg, hs, raft_log, sm.applied_index())
            .map_err(anyhow::Error::msg)?;
        if let Some(config) = snapshot_config {
            // The snapshot configuration is the base; replay later config entries.
            node.restore_config(config);
            let mut entries = vec![];
            log.read_range(snap.0 + 1, log.last_index() + 1, &mut entries)?;
            node.recover_config_entries(&entries);
        }
        node.bootstrap_members(
            cfg.peers
                .iter()
                .map(|p| Member {
                    id: p.id,
                    raft_addr: p.raft_addr.clone(),
                    client_addr: p.client_addr.clone(),
                    admin_addr: p.admin_addr.clone(),
                    learner: p.learner,
                    certificate_sha256: p.certificate_sha256.clone(),
                })
                .collect(),
        );
        let members = node.config().members.clone();
        let (proposal_tx, proposal_rx) = mpsc::channel(4096);
        let (read_tx, read_rx) = mpsc::channel(8192);
        let (inbox_tx, inbox_rx) = mpsc::channel(16384);
        let (events, _) = broadcast::channel(2048);
        let peer_tls = cfg
            .peer_tls
            .as_ref()
            .map(|tls| -> anyhow::Result<_> {
                Ok(tonic::transport::ClientTlsConfig::new()
                    .identity(tonic::transport::Identity::from_pem(
                        std::fs::read(&tls.cert)?,
                        std::fs::read(&tls.key)?,
                    ))
                    .ca_certificate(tonic::transport::Certificate::from_pem(std::fs::read(
                        &tls.ca,
                    )?)))
            })
            .transpose()?;
        let peers = members
            .values()
            .filter(|m| m.id != cfg.id)
            .map(|m| {
                (m.id, {
                    let peer = PeerClient::new(m.id, m.raft_addr.clone(), inbox_tx.clone());
                    if let Some(tls) = &peer_tls {
                        peer.with_tls(tls.clone())
                    } else {
                        peer
                    }
                })
            })
            .collect();
        let audit_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(cfg.data_dir.join("audit.jsonl"))?;
        let rt = Arc::new(Self {
            clock: AtomicU64::new(sm.logical_time()),
            cfg,
            node: Mutex::new(node),
            gate: Mutex::new(()),
            membership_gate: tokio::sync::Mutex::new(()),
            contacts: Mutex::new(BTreeMap::new()),
            log,
            meta,
            snaps,
            sm,
            peers: Mutex::new(peers),
            members: Mutex::new(members),
            inbox_tx: inbox_tx.clone(),
            peer_tls,
            proposal_tx,
            read_tx,
            pending: Mutex::new(HashMap::new()),
            reads: Mutex::new(HashMap::new()),
            next_read: AtomicU64::new(1),
            failed: AtomicBool::new(false),
            started: Instant::now(),
            outbound: Arc::new(Semaphore::new(1024)),
            metrics: Metrics::new()?,
            audit_file: Mutex::new(audit_file),
            events,
            event_history: Mutex::new(VecDeque::new()),
            last_size_scan: AtomicU64::new(0),
            event_seq: AtomicU64::new(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_micros() as u64,
            ),
        });
        {
            let _gate = rt.gate.lock();
            let acts = rt.node.lock().replay_committed();
            rt.execute(acts, None)?;
        }
        Ok((
            rt,
            RuntimeHandles {
                inbox_rx,
                proposal_rx,
                read_rx,
            },
        ))
    }
    pub fn spawn(self: Arc<Self>, handles: RuntimeHandles) {
        tokio::spawn(async move {
            let RuntimeHandles {
                mut inbox_rx,
                mut proposal_rx,
                mut read_rx,
                ..
            } = handles;
            let mut tick = tokio::time::interval(Duration::from_millis(self.cfg.tick_ms));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut time_tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _=tick.tick()=> {let _gate=self.gate.lock();if !self.failed.load(Ordering::Relaxed) {let acts=self.node.lock().tick();self.run(acts,None);}}
                    _=time_tick.tick()=> {let _gate=self.gate.lock();if self.is_leader() && !self.failed.load(Ordering::Relaxed) {let cmd=Command::Tick{now_ms:self.logical_now()};let acts=self.node.lock().propose(bincode::serialize(&cmd).expect("tick encoding"));if let Ok(acts)=acts {self.run(acts,None);}}}
                    Some(msg)=inbox_rx.recv()=> {let _gate=self.gate.lock();if !self.failed.load(Ordering::Relaxed) {self.message_event("MessageReceived",&msg,None);let acts=self.node.lock().step(msg);self.run(acts,None);}}
                    Some(req)=proposal_rx.recv()=>self.handle_proposal(req),
                    Some(req)=read_rx.recv()=>self.handle_read(req),
                }
                self.clean_waiters();
            }
        });
    }
    pub(crate) fn audit(
        &self,
        identity: &str,
        action: &str,
        target: &str,
        result: &str,
        request_id: u64,
    ) -> std::io::Result<()> {
        use std::io::Write;
        let record = json!({"timestamp":crate::control::timestamp(),"identity":identity,"action":action,"target":target,"node":self.cfg.id,"term":self.node.lock().hard_state().current_term,"result":result,"requestId":request_id});
        let mut file = self.audit_file.lock();
        writeln!(file, "{record}")?;
        file.sync_data()
    }
    pub(crate) fn trace_id(&self) -> u64 {
        self.event_seq.fetch_add(1, Ordering::Relaxed)
    }
    pub fn local_id(&self) -> NodeId {
        self.cfg.id
    }
    pub(crate) fn is_leader(&self) -> bool {
        self.node.lock().role() == Role::Leader
    }
    pub(crate) fn ready(&self) -> bool {
        if self.failed.load(Ordering::Relaxed) {
            return false;
        }
        let node = self.node.lock();
        let term = node.hard_state().current_term;
        let contacts = self.contacts.lock();
        let fresh = |id: &u64| {
            contacts.get(id).is_some_and(|(t, at)| {
                *t == term && at.elapsed() < Duration::from_millis(self.cfg.election_timeout_ms)
            })
        };
        if node.role() == Role::Leader {
            let mut available = std::collections::BTreeSet::from([self.cfg.id]);
            available.extend(contacts.keys().filter(|id| fresh(id)).copied());
            node.config().state.has_quorum(&available)
        } else {
            node.soft_state().leader_id.is_some_and(|id| fresh(&id))
        }
    }
    pub(crate) fn leader_id(&self) -> Option<u64> {
        self.node.lock().soft_state().leader_id
    }
    pub(crate) fn logical_now(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let prior = self
            .clock
            .fetch_max(now.max(self.sm.logical_time()), Ordering::Relaxed);
        self.clock
            .fetch_max(prior.saturating_add(1), Ordering::Relaxed)
            .max(prior.saturating_add(1))
    }
    fn not_leader(&self) -> ProposeFailure {
        ProposeFailure::NotLeader {
            hint: self
                .leader_id()
                .and_then(|id| self.members.lock().get(&id).map(|m| m.client_addr.clone()))
                .filter(|a| !a.is_empty()),
        }
    }
    fn handle_proposal(&self, req: ProposalRequest) {
        let _gate = self.gate.lock();
        if self.failed.load(Ordering::Relaxed) {
            let _ = req
                .tx
                .send(Err(ProposeFailure::Other("storage unavailable".into())));
            return;
        }
        if self.pending.lock().len() >= 4096 {
            let _ = req
                .tx
                .send(Err(ProposeFailure::Other("BUSY proposal waiters".into())));
            return;
        }
        let (index, term, acts) = {
            let mut node = self.node.lock();
            let index = node.last_log_index() + 1;
            let term = node.hard_state().current_term;
            let acts = match req.kind {
                ProposalKind::Command(cmd) => node.propose(
                    bincode::serialize(&Command::AtTime {
                        now_ms: self.logical_now(),
                        command: Box::new(cmd),
                    })
                    .expect("command encoding"),
                ),
                ProposalKind::Config(config) => node.propose_config_change(config),
                ProposalKind::Member(member) => node.propose_member(member),
                ProposalKind::RemoveLearner(id) => node.propose_learner_removal(id),
            };
            (index, term, acts)
        };
        match acts {
            Ok(acts) => {
                self.pending
                    .lock()
                    .insert(index, (term, Instant::now(), req.tx));
                self.run(acts, None);
            }
            Err(error) => {
                let failure = match error {
                    ProposeError::NotLeader { .. } => self.not_leader(),
                    error => ProposeFailure::Other(error.to_string()),
                };
                let _ = req.tx.send(Err(failure));
            }
        }
    }
    fn handle_read(&self, req: ReadRequest) {
        let _gate = self.gate.lock();
        if !self.is_leader() || self.failed.load(Ordering::Relaxed) {
            let hint = self
                .leader_id()
                .and_then(|id| self.members.lock().get(&id).map(|m| m.client_addr.clone()))
                .unwrap_or_default();
            let _ = req.tx.send(RespFrame::err(format!("MOVED 0 {hint}")));
            return;
        }
        if self.reads.lock().len() >= 8192 {
            let _ = req.tx.send(RespFrame::err("BUSY read waiters"));
            return;
        }
        let id = self.next_read.fetch_add(1, Ordering::Relaxed);
        let term = self.node.lock().hard_state().current_term;
        self.reads.lock().insert(
            id,
            PendingRead {
                index: None,
                term,
                tx: req.tx,
                f: req.f,
            },
        );
        self.emit("ReadIndexStarted", json!({"requestId":id}));
        let acts = self
            .node
            .lock()
            .read_index(Bytes::copy_from_slice(&id.to_le_bytes()));
        self.run(acts, None);
    }
    fn clean_waiters(&self) {
        let _gate = self.gate.lock();
        let node = self.node.lock();
        let term = node.hard_state().current_term;
        let leader = node.role() == Role::Leader;
        drop(node);
        self.pending
            .lock()
            .retain(|_, (t, _, tx)| !tx.is_closed() && *t == term && leader);
        let mut reads = self.reads.lock();
        let stale: Vec<_> = reads
            .iter()
            .filter_map(|(id, r)| (r.tx.is_closed() || r.term != term || !leader).then_some(*id))
            .collect();
        for id in stale {
            reads.remove(&id);
            self.node.lock().cancel_read(&id.to_le_bytes());
        }
    }
    fn drain_reads(&self) {
        let applied = self.sm.applied_index();
        let mut reads = self.reads.lock();
        let ready: Vec<_> = reads
            .iter()
            .filter_map(|(id, r)| r.index.filter(|i| applied >= *i).map(|_| *id))
            .collect();
        for id in ready {
            let read = reads.remove(&id).expect("pending read");
            let _ = read.tx.send((read.f)(&self.sm));
        }
    }
    fn run<I: IntoIterator<Item = Action>>(
        &self,
        acts: I,
        response_to: Option<u64>,
    ) -> Option<Message> {
        match self.execute(acts, response_to) {
            Ok(response) => response,
            Err(error) => {
                self.failed.store(true, Ordering::Relaxed);
                self.pending.lock().clear();
                self.reads.lock().clear();
                tracing::error!(%error,"durable action failed; node is fenced until restart");
                None
            }
        }
    }
    fn execute<I: IntoIterator<Item = Action>>(
        &self,
        acts: I,
        response_to: Option<u64>,
    ) -> anyhow::Result<Option<Message>> {
        let mut response = None;
        for act in acts {
            let _storage_timer = matches!(
                &act,
                Action::PersistHardState(_)
                    | Action::AppendEntries(_)
                    | Action::TruncateLog { .. }
                    | Action::ApplyCommitted { .. }
                    | Action::InstallSnapshot { .. }
            )
            .then(|| self.metrics.storage_duration.start_timer());
            match act {
                Action::PersistHardState(hs) => self.meta.save_hard_state(&hs)?,
                Action::AppendEntries(entries) => {
                    self.log.append(&entries)?;
                    self.emit(
                        "LogAppended",
                        json!({"logIndex":entries.last().map(|e|e.index),"entries":entries.len()}),
                    );
                }
                Action::TruncateLog { from } => {
                    self.log.truncate_from(from)?;
                    self.emit("LogTruncated", json!({"logIndex":from}));
                }
                Action::SendMessage { to, mut msg } => {
                    if response_to == Some(to)
                        && matches!(
                            msg,
                            Message::AppendEntriesResponse { .. }
                                | Message::RequestVoteResponse { .. }
                                | Message::InstallSnapshotResponse { .. }
                        )
                    {
                        response = Some(msg);
                        continue;
                    }
                    if let Message::InstallSnapshot {
                        data,
                        last_included_index,
                        ..
                    } = &mut msg
                    {
                        let (meta, path) = self
                            .snaps
                            .latest()
                            .ok_or_else(|| anyhow::anyhow!("snapshot missing"))?;
                        anyhow::ensure!(
                            meta.last_included_index == *last_included_index,
                            "snapshot boundary mismatch"
                        );
                        *data = Bytes::from(self.snaps.read_checked(&path)?);
                    }
                    self.sync_peers();
                    if let Some(peer) = self.peers.lock().get(&to).cloned() {
                        if let Ok(permit) = self.outbound.clone().try_acquire_owned() {
                            self.metrics.sent.inc();
                            self.message_event("MessageSent", &msg, Some(to));
                            tokio::spawn(async move {
                                let _permit = permit;
                                peer.send(msg).await;
                            });
                        }
                    }
                }
                Action::ApplyCommitted { entries } => self.apply_entries(entries)?,
                Action::TakeSnapshot {
                    last_included_index,
                    last_included_term,
                } => self.take_snapshot(last_included_index, last_included_term)?,
                Action::InstallSnapshot {
                    last_included_index,
                    last_included_term,
                    data,
                } => self.install_snapshot(last_included_index, last_included_term, &data)?,
                Action::NotifyReadIndex {
                    ctx, commit_index, ..
                } => {
                    if let Ok(bytes) = <&[u8; 8]>::try_from(ctx.as_ref()) {
                        let id = u64::from_le_bytes(*bytes);
                        if let Some(read) = self.reads.lock().get_mut(&id) {
                            read.index = Some(commit_index);
                        }
                        self.emit(
                            "ReadIndexQuorumReached",
                            json!({"requestId":id,"commitIndex":commit_index}),
                        );
                    }
                }
                Action::BecameLeader { .. } | Action::BecameFollower { .. } => {
                    self.metrics.leadership.inc();
                    let role = self.node.lock().role().as_str();
                    self.emit("RoleChanged", json!({"detail":role}));
                }
                Action::Metric(event) => self.observe(event),
                Action::ResetElectionTimer | Action::ResetHeartbeatTimer => {}
            }
        }
        self.drain_reads();
        self.sync_peers();
        self.refresh_metrics();
        Ok(response)
    }
    fn apply_entries(&self, entries: Vec<Entry>) -> anyhow::Result<()> {
        for entry in entries {
            let committed = Instant::now();
            let response = match entry.kind {
                EntryKind::Normal => {
                    let command: Command = bincode::deserialize(&entry.data)?;
                    self.sm.apply(entry.index, &command)?
                }
                _ => self.sm.advance(entry.index)?,
            };
            self.emit("EntryApplied", json!({"logIndex":entry.index}));
            if let Some((term, started, tx)) = self.pending.lock().remove(&entry.index) {
                let node = self.node.lock();
                let replicated = 1 + node
                    .peer_progress()
                    .iter()
                    .filter(|(_, p)| p.match_index >= entry.index)
                    .count();
                let execution = json!({"receivedByNodeId":self.cfg.id,"leaderId":self.cfg.id,"term":term,"proposedIndex":entry.index,"committedMs":committed.duration_since(started).as_secs_f64()*1000.0,"appliedMs":started.elapsed().as_secs_f64()*1000.0,"replicatedTo":replicated,"voterCount":node.config().state.voters().len()});
                let _ = tx.send(Ok((response, execution)));
            }
        }
        let (snapshot_index, index, term) = {
            let node = self.node.lock();
            let index = self.sm.applied_index();
            (
                node.snapshot_index(),
                index,
                node.term_at(index).unwrap_or(0),
            )
        };
        if self.cfg.snapshot_entries_threshold > 0
            && index.saturating_sub(snapshot_index) >= self.cfg.snapshot_entries_threshold
        {
            self.take_snapshot(index, term)?;
        }
        Ok(())
    }
    fn take_snapshot(&self, index: u64, term: u64) -> anyhow::Result<()> {
        anyhow::ensure!(
            index == self.sm.applied_index(),
            "snapshot must cover durable applied state"
        );
        self.emit("SnapshotStarted", json!({"logIndex":index}));
        let package = SnapshotPackage {
            version: 1,
            index,
            term,
            config: self.node.lock().config_at(index),
            state: self.sm.snapshot()?,
        };
        let meta = SnapshotMeta {
            last_included_index: index,
            last_included_term: term,
            config: bincode::serialize(&package.config)?,
        };
        let mut writer = self.snaps.begin_write(&meta)?;
        writer.write_chunk(&bincode::serialize(&package)?)?;
        writer.finish()?;
        self.meta.save_snapshot_pointer(index, term)?;
        self.log.compact(index)?;
        self.node.lock().on_snapshot_taken(index, term);
        self.snaps.keep_last(3)?;
        self.metrics.snapshots.inc();
        Ok(())
    }
    fn install_snapshot(&self, index: u64, term: u64, data: &[u8]) -> anyhow::Result<()> {
        let package: SnapshotPackage = bincode::deserialize(data)?;
        anyhow::ensure!(
            package.version == 1 && package.index == index && package.term == term,
            "snapshot package mismatch"
        );
        KvStateMachine::validate_snapshot(&package.state, index)?;
        let meta = SnapshotMeta {
            last_included_index: index,
            last_included_term: term,
            config: bincode::serialize(&package.config)?,
        };
        let mut writer = self.snaps.begin_write(&meta)?;
        writer.write_chunk(data)?;
        writer.finish()?;
        self.sm.restore(&package.state, index)?;
        if !self.log.read(index)?.is_some_and(|e| e.term == term) {
            self.log.truncate_from(1)?;
        }
        self.meta.save_snapshot_pointer(index, term)?;
        self.log.set_snapshot_tip(index);
        self.log.compact(index)?;
        self.node.lock().restore_config(package.config);
        self.metrics.installs.inc();
        self.emit("SnapshotInstalled", json!({"logIndex":index}));
        Ok(())
    }
    pub(crate) fn backup(&self) -> anyhow::Result<Vec<u8>> {
        let _gate = self.gate.lock();
        anyhow::ensure!(!self.failed.load(Ordering::Relaxed), "storage unavailable");
        let index = self.sm.applied_index();
        let term = self.node.lock().term_at(index).unwrap_or(0);
        let snapshot = bincode::serialize(&SnapshotPackage {
            version: 1,
            index,
            term,
            config: self.node.lock().config_at(index),
            state: self.sm.snapshot()?,
        })?;
        Ok(Backup::encode(
            snapshot,
            "raftkv".into(),
            crate::control::timestamp(),
            hex::encode(self.sm.state_hash()),
            self.sm.len() as u64,
        )?)
    }
    pub(crate) fn trigger_snapshot(&self) -> anyhow::Result<()> {
        let _gate = self.gate.lock();
        anyhow::ensure!(!self.failed.load(Ordering::Relaxed), "storage unavailable");
        let index = self.sm.applied_index();
        let term = self.node.lock().term_at(index).unwrap_or(0);
        self.take_snapshot(index, term)
    }
    fn sync_peers(&self) {
        let members = self.node.lock().config().members.clone();
        let mut peers = self.peers.lock();
        for m in members.values().filter(|m| m.id != self.cfg.id) {
            if peers.get(&m.id).map_or(true, |p| p.endpoint != m.raft_addr) {
                let peer = PeerClient::new(m.id, m.raft_addr.clone(), self.inbox_tx.clone());
                peers.insert(
                    m.id,
                    if let Some(tls) = &self.peer_tls {
                        peer.with_tls(tls.clone())
                    } else {
                        peer
                    },
                );
            }
        }
        peers.retain(|id, _| members.contains_key(id));
        *self.members.lock() = members;
    }
    fn observe(&self, event: MetricEvent) {
        let (kind, fields) = match event {
            MetricEvent::TermBumped { new_term } => ("TermChanged", json!({"term":new_term})),
            MetricEvent::ElectionStarted { term, pre_vote } => {
                self.metrics.elections.inc();
                (
                    "ElectionStarted",
                    json!({"term":term,"detail":if pre_vote {"PreVote"}else{"RequestVote"}}),
                )
            }
            MetricEvent::ElectionWon { term } => ("ElectionWon", json!({"term":term})),
            MetricEvent::CommitAdvanced { index } => {
                ("CommitAdvanced", json!({"commitIndex":index}))
            }
            MetricEvent::AppendRejected { from } => {
                self.metrics.rejections.inc();
                ("AppendRejected", json!({"peerId":from}))
            }
            MetricEvent::LeadershipTransferStarted { target } => {
                ("LeadershipTransferStarted", json!({"peerId":target}))
            }
        };
        self.emit(kind, fields);
    }
    pub(crate) fn emit(&self, kind: &str, fields: Value) {
        let mut event = json!({"seq":self.event_seq.fetch_add(1,Ordering::Relaxed),"nodeId":self.cfg.id,"term":self.node.lock().hard_state().current_term,"type":kind,"timestamp":crate::control::timestamp()});
        if let (Some(event), Some(fields)) = (event.as_object_mut(), fields.as_object()) {
            event.extend(fields.clone());
        }
        let mut history = self.event_history.lock();
        if history.len() >= 10_000 {
            history.pop_front();
        }
        history.push_back(event.clone());
        let _ = self.events.send(event);
    }
    fn message_event(&self, kind: &str, msg: &Message, peer: Option<u64>) {
        if kind == "MessageReceived" {
            match msg {
                Message::AppendEntries { from, term, .. }
                | Message::InstallSnapshot { from, term, .. }
                | Message::AppendEntriesResponse {
                    from,
                    term,
                    success: true,
                    ..
                }
                | Message::InstallSnapshotResponse { from, term, .. } => {
                    self.contacts.lock().insert(*from, (*term, Instant::now()));
                }
                _ => {}
            }
        }
        let (detail, from, rid) = match msg {
            Message::AppendEntries {
                from, request_id, ..
            } => ("AppendEntries", *from, *request_id),
            Message::AppendEntriesResponse {
                from, request_id, ..
            } => ("AppendResponse", *from, *request_id),
            Message::RequestVote {
                from, request_id, ..
            } => ("RequestVote", *from, *request_id),
            Message::RequestVoteResponse {
                from, request_id, ..
            } => ("VoteResponse", *from, *request_id),
            Message::InstallSnapshot {
                from, request_id, ..
            } => ("InstallSnapshot", *from, *request_id),
            Message::InstallSnapshotResponse {
                from, request_id, ..
            } => ("SnapshotResponse", *from, *request_id),
            Message::TimeoutNow { from, .. } => ("TimeoutNow", *from, 0),
        };
        self.emit(
            kind,
            json!({"peerId":peer.unwrap_or(from),"requestId":rid,"detail":detail}),
        );
    }
    fn refresh_metrics(&self) {
        let elapsed = self.started.elapsed().as_millis() as u64;
        if elapsed.saturating_sub(self.last_size_scan.load(Ordering::Relaxed)) >= 1000 {
            self.last_size_scan.store(elapsed, Ordering::Relaxed);
            self.metrics
                .log_bytes
                .set(directory_size(&self.cfg.data_dir.join("log")) as f64);
            self.metrics
                .snapshot_bytes
                .set(directory_size(&self.cfg.data_dir.join("snapshots")) as f64);
        }
        let node = self.node.lock();
        self.metrics.term.set(node.hard_state().current_term as f64);
        self.metrics.role.set(if node.role() == Role::Leader {
            2.0
        } else if node.role() == Role::Candidate {
            1.0
        } else {
            0.0
        });
        self.metrics.commit.set(node.commit_index() as f64);
        self.metrics.applied.set(self.sm.applied_index() as f64);
        self.metrics.last_log.set(node.last_log_index() as f64);
        self.metrics.keys.set(self.sm.len() as f64);
        self.metrics.expiring.set(self.sm.expiring_len() as f64);
        for (id, p) in node.peer_progress().iter() {
            self.metrics
                .lag
                .with_label_values(&[&id.to_string()])
                .set(node.last_log_index().saturating_sub(p.match_index) as f64);
        }
    }
    pub(crate) fn status(&self) -> Value {
        let _gate = self.gate.lock();
        let node = self.node.lock();
        let members = self.members.lock();
        use crate::diagnostics::{LogEntryDiagnostics, NodeDiagnostics, PeerDiagnostics};
        let contacts = self.contacts.lock();
        let peers = node
            .peer_progress()
            .iter()
            .filter(|(id, _)| **id != self.cfg.id)
            .map(|(id, p)| PeerDiagnostics {
                id: *id,
                match_index: p.match_index,
                next_index: p.next_index,
                replication_lag: node.last_log_index().saturating_sub(p.match_index),
                inflight: p.inflight,
                recently_active: contacts.get(id).is_some_and(|(term, at)| {
                    *term == node.hard_state().current_term
                        && at.elapsed() < Duration::from_millis(self.cfg.election_timeout_ms)
                }),
                snapshot_in_progress: p.pending_snapshot.is_some(),
            })
            .collect();
        let term = node.hard_state().current_term;
        let fresh = |id: &u64| {
            contacts.get(id).is_some_and(|(t, at)| {
                *t == term && at.elapsed() < Duration::from_millis(self.cfg.election_timeout_ms)
            })
        };
        let serving = if node.role() == Role::Leader {
            let mut available = std::collections::BTreeSet::from([self.cfg.id]);
            available.extend(contacts.keys().filter(|id| fresh(id)).copied());
            node.config().state.has_quorum(&available)
        } else {
            node.soft_state().leader_id.is_some_and(|id| fresh(&id))
        };
        let local = members.get(&self.cfg.id);
        let log_entries = node
            .recent_log(12)
            .iter()
            .map(|e| LogEntryDiagnostics {
                index: e.index,
                term: e.term,
                kind: format!("{:?}", e.kind),
            })
            .collect();
        serde_json::to_value(NodeDiagnostics {
            node_id: self.cfg.id,
            role: node.role().as_str(),
            health: if self.failed.load(Ordering::Relaxed) {
                "unreachable"
            } else if serving {
                "healthy"
            } else {
                "degraded"
            },
            term: node.hard_state().current_term,
            leader_id: node.soft_state().leader_id,
            voted_for: node.hard_state().voted_for,
            commit_index: node.commit_index(),
            applied_index: self.sm.applied_index(),
            last_log_index: node.last_log_index(),
            snapshot_index: node.snapshot_index(),
            state_hash: hex::encode(self.sm.state_hash()),
            uptime_seconds: self.started.elapsed().as_secs(),
            key_count: self.sm.len(),
            raft_address: local.map(|m| m.raft_addr.clone()),
            client_address: local.map(|m| m.client_addr.clone()),
            admin_address: local.map(|m| m.admin_addr.clone()),
            storage_bytes: directory_size(&self.cfg.data_dir),
            peers,
            voters: node.config().state.voters(),
            configuration_state: if matches!(
                node.config().state,
                raft_core::ConfigState::Joint { .. }
            ) {
                "joint"
            } else {
                "stable"
            },
            configuration: node.config().state.clone(),
            log_entries,
            snapshot_threshold: self.cfg.snapshot_entries_threshold,
        })
        .expect("diagnostic serialization")
    }
    pub(crate) fn snapshots(&self) -> Value {
        json!(self.snaps.list().iter().filter_map(|path| {
        let meta:SnapshotMeta=bincode::deserialize(&std::fs::read(path.with_extension("meta")).ok()?).ok()?;Some(json!({"id":path.file_stem()?.to_str()?,"lastIncludedIndex":meta.last_included_index,"lastIncludedTerm":meta.last_included_term,"sizeBytes":std::fs::metadata(path).ok()?.len()}))
    }).collect::<Vec<_>>())
    }
    pub(crate) fn transfer(&self, target: u64) -> Result<(), String> {
        let _gate = self.gate.lock();
        let mut node = self.node.lock();
        if node.role() != Role::Leader {
            return Err("not leader".into());
        }
        if !node.config().state.is_voter(target) || target == self.cfg.id {
            return Err("target must be another voter".into());
        }
        let acts = node.transfer_leadership(target);
        drop(node);
        self.execute(acts, None).map_err(|e| e.to_string())?;
        Ok(())
    }
    pub(crate) async fn member_change(&self, mut member: Member) -> Result<(), String> {
        let _membership = self.membership_gate.lock().await;
        if let Some(hash) = &mut member.certificate_sha256 {
            hash.make_ascii_lowercase();
        }
        if member.id == 0
            || member.raft_addr.is_empty()
            || member.client_addr.is_empty()
            || member.admin_addr.is_empty()
        {
            return Err("member id and advertised endpoints are required".into());
        }
        if self.cfg.peer_tls.is_some()
            && member
                .certificate_sha256
                .as_ref()
                .is_none_or(|hash| hash.len() != 64 || hex::decode(hash).is_err())
        {
            return Err("mTLS membership requires certificate_sha256".into());
        }
        let exists = self.members.lock().get(&member.id).cloned();
        if exists.is_none() && !member.learner {
            return Err("register as a learner, catch up, then promote to voter".into());
        }
        if exists.as_ref().is_some_and(|m| !m.learner) && member.learner {
            return Err("voter downgrade requires a configuration change".into());
        }
        if !member.learner {
            let node = self.node.lock();
            let caught_up = node
                .peer_progress()
                .get(member.id)
                .is_some_and(|p| p.match_index >= node.commit_index());
            if !caught_up {
                return Err("learner has not caught up".into());
            }
        }
        self.submit(ProposalKind::Member(member.clone()))
            .await
            .map_err(|e| e.to_string())?;
        if !member.learner {
            self.submit(ProposalKind::Config(ConfigChange::AddServer(member.id)))
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
    pub(crate) async fn remove_member(&self, id: u64) -> Result<(), String> {
        let _membership = self.membership_gate.lock().await;
        if self.members.lock().get(&id).is_some_and(|m| m.learner) {
            return self
                .submit(ProposalKind::RemoveLearner(id))
                .await
                .map(|_| ())
                .map_err(|e| e.to_string());
        }
        {
            let node = self.node.lock();
            if !node.config().state.is_voter(id) {
                return Err("member is not a voter".into());
            }
            if node.config().state.voters().len() <= 1 {
                return Err("cannot remove the last voter".into());
            }
        }
        self.submit(ProposalKind::Config(ConfigChange::RemoveServer(id)))
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
    async fn submit(&self, kind: ProposalKind) -> Result<Response, ProposeFailure> {
        self.submit_with_execution(kind).await.map(|(r, _)| r)
    }
    async fn submit_with_execution(
        &self,
        kind: ProposalKind,
    ) -> Result<(Response, Value), ProposeFailure> {
        let (tx, rx) = oneshot::channel();
        self.proposal_tx
            .try_send(ProposalRequest { kind, tx })
            .map_err(|_| ProposeFailure::Other("BUSY proposal queue".into()))?;
        tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .map_err(|_| ProposeFailure::Timeout)?
            .map_err(|_| ProposeFailure::Other("proposal cancelled".into()))?
    }
}
pub(crate) fn directory_size(path: &std::path::Path) -> u64 {
    std::fs::read_dir(path)
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .map(|e| {
            if e.path().is_dir() {
                directory_size(&e.path())
            } else {
                e.metadata().map(|m| m.len()).unwrap_or(0)
            }
        })
        .sum()
}
#[async_trait]
impl MessageProcessor for Runtime {
    fn validate_certificate(&self, node: u64, hash: &str) -> bool {
        self.members
            .lock()
            .get(&node)
            .is_some_and(|m| m.certificate_sha256.as_deref() == Some(hash))
    }
    async fn process(&self, from: u64, msg: Message) -> Option<Message> {
        let _rpc_timer = self.metrics.rpc_duration.start_timer();
        let _gate = self.gate.lock();
        if self.failed.load(Ordering::Relaxed) {
            return None;
        }
        // Unknown peers cannot make this node vote or acknowledge replication.
        if !self.members.lock().contains_key(&from) {
            return None;
        }
        if matches!(
            msg,
            Message::AppendEntries { .. }
                | Message::RequestVote { .. }
                | Message::InstallSnapshot { .. }
                | Message::TimeoutNow { .. }
        ) && !self.node.lock().config().state.is_voter(from)
        {
            return None;
        }
        self.metrics.received.inc();
        self.message_event("MessageReceived", &msg, Some(from));
        let acts = self.node.lock().step(msg);
        self.run(acts, Some(from))
    }
}
pub struct ClientHandler {
    pub(crate) rt: Arc<Runtime>,
    pub(crate) execution: Mutex<Option<Value>>,
}
impl ClientHandler {
    pub fn new(rt: Arc<Runtime>) -> Self {
        Self {
            rt,
            execution: Mutex::new(None),
        }
    }
}
#[async_trait]
impl CommandHandler for ClientHandler {
    async fn propose(&self, cmd: Command) -> Result<Response, ProposeFailure> {
        let start = Instant::now();
        let result = self
            .rt
            .submit_with_execution(ProposalKind::Command(cmd))
            .await
            .map(|(response, execution)| {
                *self.execution.lock() = Some(execution);
                response
            });
        self.rt.metrics.requests.with_label_values(&["write"]).inc();
        self.rt
            .metrics
            .duration
            .observe(start.elapsed().as_secs_f64());
        if result.is_err() {
            self.rt.metrics.errors.inc();
        }
        result
    }
    async fn linearizable_read<F>(&self, f: F) -> Result<RespFrame, ProposeFailure>
    where
        F: FnOnce(&KvStateMachine) -> RespFrame + Send + 'static,
    {
        let start = Instant::now();
        let result = async {
            if !self.rt.is_leader() {
                return Err(self.rt.not_leader());
            }
            let (tx, rx) = oneshot::channel();
            self.rt
                .read_tx
                .try_send(ReadRequest { f: Box::new(f), tx })
                .map_err(|_| ProposeFailure::Other("BUSY read queue".into()))?;
            tokio::time::timeout(Duration::from_secs(5), rx)
                .await
                .map_err(|_| ProposeFailure::Timeout)?
                .map_err(|_| ProposeFailure::Other("read cancelled".into()))
        }
        .await;
        self.rt.metrics.requests.with_label_values(&["read"]).inc();
        self.rt
            .metrics
            .duration
            .observe(start.elapsed().as_secs_f64());
        if result.is_err()
            || result
                .as_ref()
                .is_ok_and(|frame| matches!(frame, RespFrame::Error(_)))
        {
            self.rt.metrics.errors.inc();
        }
        result
    }
    fn is_leader(&self) -> bool {
        self.rt.is_leader()
    }
    fn clock_ms(&self) -> u64 {
        self.rt.logical_now()
    }
    fn info(&self, _section: Option<&str>) -> String {
        let status = self.rt.status();
        format!("# Server\r\nraftkv_version:0.1.0-development\r\n# Raft\r\nrole:{}\r\nterm:{}\r\nleader_id:{}\r\ncommit_index:{}\r\napplied_index:{}\r\nlast_log_index:{}\r\n",status["role"].as_str().unwrap_or("unknown"),status["term"],status["leaderId"],status["commitIndex"],status["appliedIndex"],status["lastLogIndex"])
    }
    fn cluster_nodes(&self) -> String {
        self.rt
            .members
            .lock()
            .values()
            .map(|m| format!("{} {}", m.id, m.client_addr))
            .collect::<Vec<_>>()
            .join("\r\n")
    }
}
