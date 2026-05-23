//! Per-peer replication progress tracker (leader-side).
//!
//! Mirrors the etcd/raft `Progress` design. Each peer is in exactly one of
//! three states:
//!
//! * `Probe`     — slow, one-at-a-time AppendEntries until a match is found.
//! * `Replicate` — fast pipelined AppendEntries.
//! * `Snapshot`  — awaiting completion of an InstallSnapshot.

use crate::log::LogIndex;
use crate::node::NodeId;
use std::collections::BTreeMap;

/// Replication state for a single follower.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProgressState {
    /// Probing for the matching log point with one outstanding AE.
    Probe,
    /// Pipelining AEs.
    Replicate,
    /// Waiting for an InstallSnapshot to complete.
    Snapshot,
}

/// Per-peer progress. The leader maintains one of these per voter / learner.
#[derive(Debug, Clone)]
pub struct Progress {
    /// Index of the next log entry to send to this follower.
    pub next_index: LogIndex,
    /// Highest log entry known to be replicated on this follower.
    pub match_index: LogIndex,
    /// Current state.
    pub state: ProgressState,
    /// Whether the follower has responded since we became leader (used for
    /// read-index quorum confirmation).
    pub recent_active: bool,
    /// Number of in-flight AEs (very small bounded value used for backpressure).
    pub inflight: u32,
    /// Pending snapshot index, if any.
    pub pending_snapshot: Option<LogIndex>,
    /// Whether this peer is a non-voting learner.
    pub is_learner: bool,
}

impl Progress {
    /// Create a fresh progress with `next_index = last_log_index + 1`.
    pub fn new(last_log_index: LogIndex) -> Self {
        Self {
            next_index: last_log_index + 1,
            match_index: 0,
            state: ProgressState::Probe,
            recent_active: false,
            inflight: 0,
            pending_snapshot: None,
            is_learner: false,
        }
    }

    /// Note: use this when (re)becoming leader to start cautiously.
    pub fn reset_to_probe(&mut self, last_log_index: LogIndex) {
        self.next_index = last_log_index + 1;
        self.match_index = 0;
        self.state = ProgressState::Probe;
        self.inflight = 0;
        self.pending_snapshot = None;
    }

    /// Apply a successful AppendEntries response.
    pub fn maybe_update(&mut self, last_log_index_on_peer: LogIndex) -> bool {
        let updated = if last_log_index_on_peer > self.match_index {
            self.match_index = last_log_index_on_peer;
            true
        } else { false };
        if last_log_index_on_peer + 1 > self.next_index {
            self.next_index = last_log_index_on_peer + 1;
        }
        if matches!(self.state, ProgressState::Probe) {
            self.state = ProgressState::Replicate;
        }
        updated
    }

    /// Apply a rejected AppendEntries response with conflict hints.
    /// Returns true if `next_index` was decreased.
    pub fn maybe_decr_to(&mut self, conflict_index: LogIndex) -> bool {
        let target = conflict_index.max(1);
        if target < self.next_index {
            self.next_index = target;
            self.state = ProgressState::Probe;
            self.inflight = 0;
            true
        } else {
            false
        }
    }

    /// Mark a snapshot as in flight.
    pub fn become_snapshot(&mut self, snap_index: LogIndex) {
        self.state = ProgressState::Snapshot;
        self.pending_snapshot = Some(snap_index);
        self.inflight = 0;
    }

    /// Snapshot finished installing — resume probing from snap_index + 1.
    pub fn snapshot_finished(&mut self, snap_index: LogIndex) {
        self.match_index = self.match_index.max(snap_index);
        self.next_index = snap_index + 1;
        self.state = ProgressState::Probe;
        self.pending_snapshot = None;
    }

    /// Whether we may send an AppendEntries to this peer right now.
    pub fn can_send(&self, max_inflight: u32) -> bool {
        match self.state {
            ProgressState::Snapshot => false,
            ProgressState::Probe => self.inflight == 0,
            ProgressState::Replicate => self.inflight < max_inflight,
        }
    }
}

/// Map of peer-id -> Progress.
#[derive(Debug, Default)]
pub struct ProgressSet {
    /// Underlying storage.
    pub peers: BTreeMap<NodeId, Progress>,
}

impl ProgressSet {
    /// Insert or refresh entries for a list of voters.
    pub fn ensure(&mut self, ids: &[NodeId], last_log_index: LogIndex) {
        for &id in ids {
            self.peers.entry(id).or_insert_with(|| Progress::new(last_log_index));
        }
    }

    /// Borrow a peer's progress mutably.
    pub fn get_mut(&mut self, id: NodeId) -> Option<&mut Progress> { self.peers.get_mut(&id) }

    /// Borrow a peer's progress.
    pub fn get(&self, id: NodeId) -> Option<&Progress> { self.peers.get(&id) }

    /// Iterate all peers.
    pub fn iter(&self) -> impl Iterator<Item = (&NodeId, &Progress)> { self.peers.iter() }

    /// Drop peers no longer in the cluster.
    pub fn retain_in(&mut self, ids: &[NodeId]) {
        let keep: std::collections::BTreeSet<_> = ids.iter().copied().collect();
        self.peers.retain(|k, _| keep.contains(k));
    }
}
