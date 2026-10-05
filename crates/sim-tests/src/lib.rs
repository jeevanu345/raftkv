#![deny(unsafe_code)]
//! Deterministic simulation harness around `raft_core::RaftNode`.
//!
//! The harness is fully synchronous — it advances a logical clock by ticks and
//! delivers messages from a network that the test can perturb (drop, delay,
//! reorder, partition). Tests built on top of this are 100% reproducible from
//! a single PRNG seed.
//!
//! See `tests/` for concrete invariants (election liveness, log matching,
//! commit safety under partitions).

use std::collections::{BTreeMap, VecDeque};

use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;

use raft_core::{Action, HardState, Message, NodeId, RaftConfig, RaftLog, RaftNode, Role};

/// One simulated cluster node: a `RaftNode` plus an inbox.
pub struct SimNode {
    /// The node id.
    pub id: NodeId,
    /// The pure raft state machine.
    pub node: RaftNode,
    /// Pending in-bound messages (delivery queue).
    pub inbox: VecDeque<Message>,
    /// Whether this node is currently partitioned (drops all in/out).
    pub partitioned: bool,
    /// Crash stops ticks and deliveries; durable state survives.
    pub crashed: bool,
    durable_hs: HardState,
    durable_log: RaftLog,
    cfg: RaftConfig,
    applied: BTreeMap<u64, raft_core::Entry>,
    snapshot: Option<(u64, u64, Vec<u8>)>,
    stalled_until: u64,
}

impl SimNode {
    /// Construct a simulated node from a config.
    pub fn new(cfg: RaftConfig) -> Self {
        let id = cfg.id;
        Self {
            id,
            cfg: cfg.clone(),
            crashed: false,
            durable_hs: HardState::default(),
            durable_log: RaftLog::new(),
            applied: BTreeMap::new(),
            snapshot: None,
            stalled_until: 0,
            node: RaftNode::new(cfg, HardState::default(), RaftLog::new()),
            inbox: VecDeque::new(),
            partitioned: false,
        }
    }
}

/// In-flight network message (with a delivery time).
#[derive(Debug)]
pub struct InFlight {
    /// Source node.
    pub from: NodeId,
    /// Destination node.
    pub to: NodeId,
    /// Delivery time in absolute ticks.
    pub deliver_at: u64,
    /// Message body.
    pub msg: Message,
}

/// A deterministic storage cut point. These model durable prefixes, not OS syscalls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageFault {
    /// Fail before executing a persistence action.
    BeforePersistence,
    /// Persist the action, then lose the ACK and crash (uncertain fsync outcome).
    AfterPersistence,
    /// Keep only complete entries before a torn final append record.
    TornAppend,
    /// Fail snapshot publication, preserving the old checkpoint.
    SnapshotPublication,
}

/// The simulation cluster.
pub struct Sim {
    /// Per-node state.
    pub nodes: BTreeMap<NodeId, SimNode>,
    /// In-flight messages (will be delivered when `now >= deliver_at`).
    pub network: Vec<InFlight>,
    /// Current tick.
    pub now: u64,
    /// Network delay range (in ticks).
    pub delay_min: u64,
    /// Upper bound (exclusive).
    pub delay_max: u64,
    /// Per-step drop probability (0..=1).
    pub drop_p: f64,
    /// Deterministic PRNG.
    pub rng: ChaCha20Rng,
    /// Original replay seed.
    pub seed: u64,
    /// Asymmetric blocked links.
    pub blocked: std::collections::BTreeSet<(NodeId, NodeId)>,
    /// Duplicate probability.
    pub duplicate_p: f64,
    /// Captured observable actions (bounded).
    pub events: VecDeque<(u64, NodeId, Action)>,
    committed: BTreeMap<u64, raft_core::Entry>,
    previous_indices: BTreeMap<NodeId, (u64, u64)>,
    /// Fail the next persistence action on this node, then crash it.
    pub disk_failure: Option<NodeId>,
    /// Selected modeled storage fault.
    pub storage_fault: Option<(NodeId, StorageFault)>,
    /// Delay inbound traffic as well as clocks on selected nodes.
    pub slow_until: BTreeMap<NodeId, u64>,
    /// Monotonic total number of captured actions, including evicted events.
    pub event_count: u64,
}

impl Sim {
    /// Build a simulation cluster.
    pub fn new(ids: &[NodeId], seed: u64) -> Self {
        let mut nodes = BTreeMap::new();
        for &id in ids {
            let cfg = RaftConfig {
                id,
                initial_voters: ids.to_vec(),
                election_timeout_ticks: 10,
                heartbeat_ticks: 1,
                max_append_entries: 16,
                max_inflight_msgs: 64,
                rng_seed: seed.wrapping_add(id),
                pre_vote: true,
            };
            nodes.insert(id, SimNode::new(cfg));
        }
        Self {
            nodes,
            network: Vec::new(),
            now: 0,
            delay_min: 1,
            delay_max: 3,
            drop_p: 0.0,
            rng: ChaCha20Rng::seed_from_u64(seed),
            seed,
            blocked: Default::default(),
            duplicate_p: 0.0,
            events: VecDeque::new(),
            committed: BTreeMap::new(),
            previous_indices: BTreeMap::new(),
            disk_failure: None,
            storage_fault: None,
            slow_until: BTreeMap::new(),
            event_count: 0,
        }
    }

    /// Advance time by one tick on every node and deliver due messages.
    pub fn step(&mut self) {
        self.now += 1;
        // Tick every node.
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for id in &ids {
            if self.nodes[id].crashed || self.nodes[id].stalled_until > self.now {
                continue;
            }
            let acts = self.nodes.get_mut(id).unwrap().node.tick();
            self.dispatch(*id, acts);
        }
        // Deliver due messages.
        let mut keep = Vec::with_capacity(self.network.len());
        let due: Vec<InFlight> = std::mem::take(&mut self.network)
            .into_iter()
            .filter_map(|m| {
                if m.deliver_at <= self.now {
                    Some(m)
                } else {
                    keep.push(m);
                    None
                }
            })
            .collect();
        self.network = keep;
        for mut m in due {
            if self
                .slow_until
                .get(&m.to)
                .is_some_and(|until| *until > self.now)
            {
                m.deliver_at = self.now + 1;
                self.network.push(m);
                continue;
            }
            if self.blocked.contains(&(m.from, m.to))
                || self
                    .nodes
                    .get(&m.from)
                    .is_some_and(|n| n.partitioned || n.crashed)
            {
                continue;
            }
            if let Some(node) = self.nodes.get_mut(&m.to) {
                if node.partitioned || node.crashed {
                    continue;
                }
                let acts = node.node.step(m.msg);
                self.dispatch(m.to, acts);
            }
        }
        self.check_invariants()
            .unwrap_or_else(|error| panic!("{error}\n{}", self.diagnostics()));
    }

    /// Full bounded replay diagnostics for a failing invariant.
    pub fn diagnostics(&self) -> String {
        let mut text = format!(
            "seed={} tick={} network={:?} blocked={:?}\n",
            self.seed, self.now, self.network, self.blocked
        );
        for (id, node) in &self.nodes {
            text.push_str(&format!("node={id} role={:?} crashed={} hard={:?} config={:?} log={:?} applied={:?} snapshot={:?}\n",node.node.role(),node.crashed,node.durable_hs,node.node.config(),node.durable_log,node.applied,node.snapshot.as_ref().map(|(i,t,b)|(*i,*t,b.len()))));
        }
        text.push_str(&format!(
            "recent actions={:?}",
            self.events.iter().rev().take(100).collect::<Vec<_>>()
        ));
        text
    }
    /// Run until predicate is satisfied or `max_ticks` elapses.
    pub fn run_until<F: Fn(&Self) -> bool>(&mut self, pred: F, max_ticks: u64) -> bool {
        for _ in 0..max_ticks {
            if pred(self) {
                return true;
            }
            self.step();
        }
        pred(self)
    }

    /// Find the current leader, if any.
    pub fn leader(&self) -> Option<NodeId> {
        self.nodes
            .values()
            .find(|n| !n.crashed && matches!(n.node.role(), Role::Leader))
            .map(|n| n.id)
    }

    /// Crash a node after any already completed durable actions.
    pub fn crash(&mut self, id: NodeId) {
        if let Some(node) = self.nodes.get_mut(&id) {
            node.crashed = true;
        }
    }
    /// Restart from durable hard state, log and applied pointer.
    pub fn restart(&mut self, id: NodeId) {
        if let Some(node) = self.nodes.get_mut(&id) {
            let applied = node
                .applied
                .keys()
                .next_back()
                .copied()
                .unwrap_or(node.durable_log.snapshot_index());
            node.node = RaftNode::restore(
                node.cfg.clone(),
                node.durable_hs.clone(),
                node.durable_log.clone(),
                applied,
            )
            .expect("valid simulated recovery");
            if let Some((_, _, data)) = &node.snapshot {
                let (_, config): (
                    BTreeMap<u64, raft_core::Entry>,
                    raft_core::config::ClusterConfig,
                ) = bincode::deserialize(data).unwrap();
                node.node.restore_config(config);
                let entries = node
                    .durable_log
                    .slice(
                        node.durable_log.first_index(),
                        node.durable_log.last_index() + 1,
                    )
                    .to_vec();
                node.node.recover_config_entries(&entries);
            }
            node.crashed = false;
            let acts = node.node.replay_committed();
            self.dispatch(id, acts);
        }
    }
    /// Propose a command on the indicated node.
    pub fn propose(&mut self, id: NodeId, data: Vec<u8>) -> Result<(), String> {
        let node = self.nodes.get_mut(&id).ok_or("unknown node")?;
        if node.crashed {
            return Err("node crashed".into());
        }
        let acts = node.node.propose(data).map_err(|e| e.to_string())?;
        self.dispatch(id, acts);
        Ok(())
    }
    /// Stall one node's logical clock without changing durable state.
    pub fn stall(&mut self, id: NodeId, ticks: u64) {
        if let Some(node) = self.nodes.get_mut(&id) {
            node.stalled_until = self.now + ticks;
        }
    }
    /// Checkpoint the simulated applied state and compact its log.
    pub fn snapshot(&mut self, id: NodeId) -> Result<(), String> {
        if self.storage_fault == Some((id, StorageFault::SnapshotPublication)) {
            self.storage_fault = None;
            self.crash(id);
            return Err("injected snapshot publication failure".into());
        }
        let node = self.nodes.get_mut(&id).ok_or("unknown node")?;
        let index = node.applied.keys().next_back().copied().unwrap_or(0);
        let term = node.node.term_at(index).ok_or("snapshot term missing")?;
        let data = bincode::serialize(&(node.applied.clone(), node.node.config_at(index)))
            .map_err(|e| e.to_string())?;
        node.snapshot = Some((index, term, data));
        node.node.on_snapshot_taken(index, term);
        node.durable_log.compact_through(index, term);
        Ok(())
    }
    /// Drive a configuration change through the same durable action path.
    pub fn change_membership(
        &mut self,
        id: NodeId,
        change: raft_core::ConfigChange,
    ) -> Result<(), String> {
        let acts = self
            .nodes
            .get_mut(&id)
            .ok_or("unknown node")?
            .node
            .propose_config_change(change)
            .map_err(|e| e.to_string())?;
        self.dispatch(id, acts);
        Ok(())
    }
    /// Check safety after every logical step, including crash/restart history.
    pub fn check_invariants(&mut self) -> Result<(), String> {
        let mut leaders = BTreeMap::new();
        for (id, node) in &self.nodes {
            if !node.crashed && node.node.role() == Role::Leader {
                let term = node.node.hard_state().current_term;
                if let Some(other) = leaders.insert(term, *id) {
                    return Err(format!("two leaders in term {term}: {other}, {id}"));
                }
            }
            let commit = node.durable_hs.commit_index;
            let applied = node
                .applied
                .keys()
                .next_back()
                .copied()
                .unwrap_or(node.durable_log.snapshot_index());
            if let Some((old_commit, old_applied)) =
                self.previous_indices.insert(*id, (commit, applied))
            {
                if commit < old_commit || applied < old_applied {
                    return Err(format!("indices decreased on node {id}"));
                }
            }
            for (index, entry) in &node.applied {
                if let Some(old) = self.committed.get(index) {
                    if old != entry {
                        return Err(format!("state machine safety at {index}"));
                    }
                } else {
                    self.committed.insert(*index, entry.clone());
                }
            }
            for (index, entry) in &self.committed {
                if node.node.role() == Role::Leader
                    && entry.term <= node.node.hard_state().current_term
                    && *index > node.durable_log.snapshot_index()
                    && node.durable_log.get(*index) != Some(entry)
                {
                    return Err(format!("leader completeness: node {id} missing {index}"));
                }
                if let Some(local) = node.durable_log.get(*index) {
                    if *index <= commit && local != entry {
                        return Err(format!("committed entry changed at {index}"));
                    }
                }
            }
        }
        let nodes: Vec<_> = self.nodes.values().collect();
        for (a_pos, a) in nodes.iter().enumerate() {
            for b in nodes.iter().skip(a_pos + 1) {
                let low = a.durable_log.first_index().max(b.durable_log.first_index());
                let high = a.durable_log.last_index().min(b.durable_log.last_index());
                for index in low..=high {
                    let (Some(ae), Some(be)) = (a.durable_log.get(index), b.durable_log.get(index))
                    else {
                        continue;
                    };
                    if ae.term == be.term {
                        for prefix in low..=index {
                            if a.durable_log.get(prefix) != b.durable_log.get(prefix) {
                                return Err(format!(
                                    "log matching: {} vs {} at {prefix}",
                                    a.id, b.id
                                ));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
    fn dispatch<I: IntoIterator<Item = Action>>(&mut self, from: NodeId, acts: I) {
        for action in acts {
            if self.events.len() >= 10_000 {
                self.events.pop_front();
            }
            self.events.push_back((self.now, from, action.clone()));
            self.event_count += 1;
            let persistent = matches!(
                &action,
                Action::PersistHardState(_)
                    | Action::AppendEntries(_)
                    | Action::TruncateLog { .. }
                    | Action::InstallSnapshot { .. }
            );
            let selected = self
                .storage_fault
                .filter(|(id, _)| *id == from)
                .map(|(_, fault)| fault);
            if (selected == Some(StorageFault::BeforePersistence) && persistent)
                || (selected == Some(StorageFault::SnapshotPublication)
                    && matches!(&action, Action::InstallSnapshot { .. }))
            {
                self.storage_fault = None;
                self.crash(from);
                break;
            }
            if selected == Some(StorageFault::TornAppend) {
                if let Action::AppendEntries(entries) = &action {
                    let prefix = entries
                        .iter()
                        .take(entries.len().saturating_sub(1))
                        .cloned()
                        .collect::<Vec<_>>();
                    self.nodes
                        .get_mut(&from)
                        .unwrap()
                        .durable_log
                        .append(prefix);
                    self.storage_fault = None;
                    self.crash(from);
                    break;
                }
            }
            if self.disk_failure == Some(from)
                && matches!(
                    action,
                    Action::PersistHardState(_)
                        | Action::AppendEntries(_)
                        | Action::InstallSnapshot { .. }
                )
            {
                self.disk_failure = None;
                self.crash(from);
                break;
            }
            match action {
                Action::PersistHardState(hs) => self.nodes.get_mut(&from).unwrap().durable_hs = hs,
                Action::AppendEntries(entries) => self
                    .nodes
                    .get_mut(&from)
                    .unwrap()
                    .durable_log
                    .append(entries),
                Action::TruncateLog { from: index } => self
                    .nodes
                    .get_mut(&from)
                    .unwrap()
                    .durable_log
                    .truncate_from(index),
                Action::ApplyCommitted { entries } => {
                    let node = self.nodes.get_mut(&from).unwrap();
                    for entry in entries {
                        node.applied.entry(entry.index).or_insert(entry);
                    }
                }
                Action::InstallSnapshot {
                    last_included_index,
                    last_included_term,
                    data,
                } => {
                    let (applied, config): (
                        BTreeMap<u64, raft_core::Entry>,
                        raft_core::config::ClusterConfig,
                    ) = bincode::deserialize(&data).expect("valid simulated snapshot");
                    let node = self.nodes.get_mut(&from).unwrap();
                    node.applied = applied;
                    if node.durable_log.term_at(last_included_index) != Some(last_included_term) {
                        node.durable_log =
                            RaftLog::from_snapshot(last_included_index, last_included_term);
                    } else {
                        node.durable_log
                            .compact_through(last_included_index, last_included_term);
                    }
                    node.snapshot = Some((last_included_index, last_included_term, data.to_vec()));
                    node.node.restore_config(config);
                }
                Action::TakeSnapshot { .. } => {
                    self.snapshot(from).expect("snapshot");
                }
                Action::SendMessage { to, mut msg } => {
                    if let Message::InstallSnapshot { data, .. } = &mut msg {
                        if let Some((_, _, snapshot)) = &self.nodes[&from].snapshot {
                            *data = snapshot.clone().into();
                        }
                    }
                    if self
                        .nodes
                        .get(&from)
                        .is_some_and(|n| n.partitioned || n.crashed)
                        || self.blocked.contains(&(from, to))
                        || self.rng.gen_bool(self.drop_p)
                    {
                        continue;
                    }
                    let delay = self
                        .rng
                        .gen_range(self.delay_min..self.delay_max.max(self.delay_min + 1));
                    let message = InFlight {
                        from,
                        to,
                        deliver_at: self.now + delay,
                        msg: msg.clone(),
                    };
                    self.network.push(message);
                    if self.rng.gen_bool(self.duplicate_p) {
                        self.network.push(InFlight {
                            from,
                            to,
                            deliver_at: self.now + delay + 1,
                            msg,
                        });
                    }
                }
                _ => {}
            }
            if selected == Some(StorageFault::AfterPersistence) && persistent {
                self.storage_fault = None;
                self.crash(from);
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_sweep_with_partition_crash_and_duplicates() {
        let seeds = std::env::var("RAFTKV_SIM_SEEDS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        for seed in 0..seeds {
            let mut sim = Sim::new(&[1, 2, 3, 4, 5], seed);
            sim.duplicate_p = 0.1;
            sim.drop_p = 0.05;
            assert!(sim.run_until(|s| s.leader().is_some(), 500), "seed={seed}");
            for _ in 0..50 {
                sim.step();
            }
            if let Some(leader) = sim.leader() {
                sim.propose(leader, vec![1, 2, 3]).unwrap();
                sim.nodes.get_mut(&leader).unwrap().partitioned = true;
            }
            for _ in 0..100 {
                sim.step();
            }
            sim.crash(3);
            for _ in 0..20 {
                sim.step();
            }
            sim.restart(3);
            for node in sim.nodes.values_mut() {
                node.partitioned = false;
            }
            for _ in 0..100 {
                sim.step();
            }
        }
    }
    #[test]
    fn snapshot_install_and_recovery() {
        let mut s = Sim::new(&[1, 2, 3], 123);
        assert!(s.run_until(|s| s.leader().is_some(), 500));
        for _ in 0..50 {
            s.step();
        }
        let leader = s.leader().unwrap();
        let target = (1..=3).find(|id| *id != leader).unwrap();
        s.crash(target);
        for i in 0..20 {
            s.propose(leader, vec![i]).unwrap();
            for _ in 0..10 {
                s.step();
            }
        }
        s.snapshot(leader).unwrap();
        s.restart(target);
        for _ in 0..500 {
            s.step();
        }
        assert_eq!(
            s.nodes[&target].node.commit_index(),
            s.nodes[&leader].node.commit_index()
        );
    }
    #[test]
    fn three_node_cluster_elects_a_leader() {
        let mut sim = Sim::new(&[1, 2, 3], 42);
        let elected = sim.run_until(|s| s.leader().is_some(), 500);
        assert!(elected, "expected a leader within 500 ticks");
    }

    #[test]
    fn deterministic_replay() {
        let mut a = Sim::new(&[1, 2, 3, 4, 5], 0xCAFE);
        let mut b = Sim::new(&[1, 2, 3, 4, 5], 0xCAFE);
        for _ in 0..1000 {
            a.step();
            b.step();
        }
        assert_eq!(a.leader(), b.leader());
    }
    #[test]
    fn storage_cut_points_recover_without_acknowledging_lost_entries() {
        for fault in [
            StorageFault::BeforePersistence,
            StorageFault::AfterPersistence,
            StorageFault::TornAppend,
        ] {
            let mut sim = Sim::new(&[1, 2, 3], 72);
            assert!(sim.run_until(|s| s.leader().is_some(), 300));
            for _ in 0..30 {
                sim.step();
            }
            let leader = sim.leader().unwrap();
            sim.storage_fault = Some((leader, fault));
            sim.propose(leader, vec![9]).unwrap();
            assert!(sim.nodes[&leader].crashed);
            sim.restart(leader);
            for _ in 0..100 {
                sim.step();
            }
            sim.check_invariants().unwrap();
        }
        let mut sim = Sim::new(&[1, 2, 3], 3);
        assert!(sim.run_until(|s| s.leader().is_some(), 300));
        for _ in 0..30 {
            sim.step();
        }
        let leader = sim.leader().unwrap();
        sim.snapshot(leader).unwrap();
        let old = sim.nodes[&leader].snapshot.clone();
        sim.storage_fault = Some((leader, StorageFault::SnapshotPublication));
        assert!(sim.snapshot(leader).is_err());
        assert_eq!(sim.nodes[&leader].snapshot, old);
        sim.restart(leader);
        for _ in 0..50 {
            sim.step();
        }
        sim.check_invariants().unwrap();
    }
    #[test]
    fn snapshot_and_membership_transition_survive_partition_and_restart() {
        let mut sim = Sim::new(&[1, 2, 3, 4, 5], 811);
        assert!(sim.run_until(|s| s.leader().is_some(), 300));
        for _ in 0..30 {
            sim.step();
        }
        let leader = sim.leader().unwrap();
        let removed = if leader == 5 { 4 } else { 5 };
        sim.nodes.get_mut(&removed).unwrap().partitioned = true;
        sim.change_membership(leader, raft_core::ConfigChange::RemoveServer(removed))
            .unwrap();
        sim.snapshot(leader).unwrap();
        for _ in 0..100 {
            sim.step();
        }
        assert!(!sim.nodes[&leader].node.config().state.is_voter(removed));
        sim.snapshot(leader).unwrap();
        sim.crash(leader);
        sim.restart(leader);
        sim.nodes.get_mut(&removed).unwrap().partitioned = false;
        for _ in 0..100 {
            sim.step();
        }
        sim.check_invariants().unwrap();
    }
}
