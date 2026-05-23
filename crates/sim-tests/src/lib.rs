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

use raft_core::{
    Action, HardState, Message, NodeId, RaftConfig, RaftLog, RaftNode, Role,
};

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
}

impl SimNode {
    /// Construct a simulated node from a config.
    pub fn new(cfg: RaftConfig) -> Self {
        let id = cfg.id;
        Self {
            id,
            node: RaftNode::new(cfg, HardState::default(), RaftLog::new()),
            inbox: VecDeque::new(),
            partitioned: false,
        }
    }
}

/// In-flight network message (with a delivery time).
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
        }
    }

    /// Advance time by one tick on every node and deliver due messages.
    pub fn step(&mut self) {
        self.now += 1;
        // Tick every node.
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for id in &ids {
            let acts = self.nodes.get_mut(id).unwrap().node.tick();
            self.dispatch(*id, acts);
        }
        // Deliver due messages.
        let mut keep = Vec::with_capacity(self.network.len());
        let due: Vec<InFlight> = std::mem::take(&mut self.network)
            .into_iter()
            .filter_map(|m| if m.deliver_at <= self.now { Some(m) } else { keep.push(m); None })
            .collect();
        self.network = keep;
        for m in due {
            if let Some(node) = self.nodes.get_mut(&m.to) {
                if node.partitioned { continue; }
                let acts = node.node.step(m.msg);
                self.dispatch(m.to, acts);
            }
        }
    }

    /// Run until predicate is satisfied or `max_ticks` elapses.
    pub fn run_until<F: Fn(&Self) -> bool>(&mut self, pred: F, max_ticks: u64) -> bool {
        for _ in 0..max_ticks {
            if pred(self) { return true; }
            self.step();
        }
        pred(self)
    }

    /// Find the current leader, if any.
    pub fn leader(&self) -> Option<NodeId> {
        self.nodes.values().find(|n| matches!(n.node.role(), Role::Leader)).map(|n| n.id)
    }

    fn dispatch<I: IntoIterator<Item = Action>>(&mut self, from: NodeId, acts: I) {
        let from_partitioned = self.nodes.get(&from).map(|n| n.partitioned).unwrap_or(false);
        for a in acts {
            if let Action::SendMessage { to, msg } = a {
                if from_partitioned { continue; }
                if self.rng.gen_bool(self.drop_p) { continue; }
                let lo = self.delay_min;
                let hi = self.delay_max.max(lo + 1);
                let d = self.rng.gen_range(lo..hi);
                self.network.push(InFlight { from, to, deliver_at: self.now + d, msg });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        for _ in 0..1000 { a.step(); b.step(); }
        assert_eq!(a.leader(), b.leader());
    }
}
