//! The Raft state machine.
//!
//! Pure: no I/O, no async, no system clock, no OS RNG. The runtime drives it
//! with messages and an external monotonic time value (`u64` ticks).

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;
use smallvec::SmallVec;

use crate::action::{Action, MetricEvent};
use crate::config::{ClusterConfig, ConfigChange, ConfigState};
use crate::error::ProposeError;
use crate::log::{Entry, EntryKind, LogIndex, RaftLog, Term};
use crate::message::{Message, RequestId};
use crate::progress::{Progress, ProgressSet, ProgressState};
use crate::role::Role;
use crate::state::{HardState, SoftState};

/// Cluster-unique node identifier.
pub type NodeId = u64;

/// A read barrier is valid only after responses to this particular round.
struct ReadRound {
    ctx: Bytes,
    commit_index: LogIndex,
    acks: BTreeSet<NodeId>,
}

/// Configuration for a `RaftNode`.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// This node's id.
    pub id: NodeId,
    /// Initial cluster membership.
    pub initial_voters: Vec<NodeId>,
    /// Election timeout in ticks (the runtime decides what a tick is; typically
    /// 1ms). The actual timeout is randomized in
    /// `[election_timeout_ticks, 2 * election_timeout_ticks)`.
    pub election_timeout_ticks: u64,
    /// Heartbeat interval in ticks.
    pub heartbeat_ticks: u64,
    /// Maximum number of entries in a single `AppendEntries`.
    pub max_append_entries: usize,
    /// Maximum number of in-flight pipelined AEs per peer.
    pub max_inflight_msgs: u32,
    /// PRNG seed (must be deterministic for simulation tests).
    pub rng_seed: u64,
    /// Enable pre-vote (Ongaro thesis §9.6).
    pub pre_vote: bool,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            id: 1,
            initial_voters: vec![1, 2, 3],
            election_timeout_ticks: 150,
            heartbeat_ticks: 15,
            max_append_entries: 64,
            max_inflight_msgs: 256,
            rng_seed: 0xDEAD_BEEF,
            pre_vote: true,
        }
    }
}

/// The Raft state machine.
pub struct RaftNode {
    /// Local configuration.
    cfg: RaftConfig,
    /// Persisted state.
    hs: HardState,
    /// In-memory log.
    log: RaftLog,
    /// Volatile commit/apply pointers (`commit_index` lives in `HardState`).
    last_applied: LogIndex,
    /// Current role.
    role: Role,
    /// Best-known leader for the current term.
    leader_id: Option<NodeId>,
    /// Cluster configuration (latest seen, possibly uncommitted).
    config: ClusterConfig,
    /// Per-peer progress (only meaningful as Leader).
    progress: ProgressSet,
    /// Votes received in the current election (`RequestVote` / `PreVote`).
    votes: BTreeMap<NodeId, bool>,
    /// Tick counter (logical time).
    elapsed_election_ticks: u64,
    elapsed_heartbeat_ticks: u64,
    /// Randomized election timeout for the *current* term.
    randomized_election_timeout: u64,
    /// Deterministic PRNG.
    rng: ChaCha20Rng,
    /// Counter for outgoing request ids.
    next_request_id: u64,
    /// Pending leadership transfer target.
    leader_transfer_target: Option<NodeId>,
    /// Whether `C_old,new` has been observed but not yet superseded by `C_new`.
    in_joint: bool,
    read_rounds: BTreeMap<RequestId, ReadRound>,
}

impl RaftNode {
    /// Construct a fresh node from durable state. If `hs` is default and `log`
    /// is empty, this is a brand-new cluster member.
    pub fn new(cfg: RaftConfig, hs: HardState, log: RaftLog) -> Self {
        let rng = ChaCha20Rng::seed_from_u64(cfg.rng_seed.wrapping_add(cfg.id));
        let mut me = Self {
            config: ClusterConfig::stable(cfg.initial_voters.iter().copied()),
            cfg,
            hs,
            log,
            last_applied: 0,
            role: Role::Follower,
            leader_id: None,
            progress: ProgressSet::default(),
            votes: BTreeMap::new(),
            elapsed_election_ticks: 0,
            elapsed_heartbeat_ticks: 0,
            randomized_election_timeout: 0,
            rng,
            next_request_id: 1,
            leader_transfer_target: None,
            in_joint: false,
            read_rounds: BTreeMap::new(),
        };
        me.reset_randomized_election_timeout();
        // last_applied >= snapshot index (we never re-apply through a snapshot)
        me.last_applied = me.log.snapshot_index();
        me
    }

    /// Soft (volatile) state snapshot.
    pub const fn soft_state(&self) -> SoftState {
        SoftState {
            role: self.role,
            leader_id: self.leader_id,
        }
    }

    /// Borrow the hard (persistent) state.
    pub const fn hard_state(&self) -> &HardState {
        &self.hs
    }

    /// Borrow the cluster config.
    pub const fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// This node's id.
    pub const fn id(&self) -> NodeId {
        self.cfg.id
    }

    /// Current role.
    pub const fn role(&self) -> Role {
        self.role
    }

    /// Last log index.
    pub fn last_log_index(&self) -> LogIndex {
        self.log.last_index()
    }

    /// Term of the log tip (or snapshot tip).
    pub fn last_log_term(&self) -> Term {
        self.log.last_term()
    }

    /// Most recent compacted index.
    pub const fn snapshot_index(&self) -> LogIndex {
        self.log.snapshot_index()
    }

    /// Term of the most recent compacted entry.
    pub const fn snapshot_term(&self) -> Term {
        self.log.snapshot_term()
    }

    /// Leader-side progress; followers must not interpret it as peer health.
    pub const fn replication_progress(&self) -> &ProgressSet {
        &self.progress
    }

    /// Commit index.
    pub const fn commit_index(&self) -> LogIndex {
        self.hs.commit_index
    }

    /// Last applied index.
    pub const fn last_applied(&self) -> LogIndex {
        self.last_applied
    }

    const fn next_rid(&mut self) -> RequestId {
        let r = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);
        r
    }

    fn reset_randomized_election_timeout(&mut self) {
        let lo = self.cfg.election_timeout_ticks;
        let hi = lo * 2;
        self.randomized_election_timeout = self.rng.gen_range(lo..hi);
    }

    fn voters(&self) -> Vec<NodeId> {
        self.config.state.voters()
    }

    fn is_voter_self(&self) -> bool {
        self.config.state.is_voter(self.cfg.id)
    }

    // ===================================================================
    // Public API
    // ===================================================================

    /// Drive one tick of logical time. Runtime calls this on a fixed cadence.
    pub fn tick(&mut self) -> SmallVec<[Action; 8]> {
        let mut acts: SmallVec<[Action; 8]> = SmallVec::new();
        match self.role {
            Role::Leader => {
                self.elapsed_heartbeat_ticks += 1;
                if self.elapsed_heartbeat_ticks >= self.cfg.heartbeat_ticks {
                    self.elapsed_heartbeat_ticks = 0;
                    self.broadcast_heartbeat(&mut acts);
                    self.broadcast_read_rounds(&mut acts);
                }
            }
            Role::Follower | Role::Candidate | Role::PreCandidate => {
                self.elapsed_election_ticks += 1;
                if self.elapsed_election_ticks >= self.randomized_election_timeout {
                    self.elapsed_election_ticks = 0;
                    self.start_election(&mut acts);
                }
            }
        }
        acts
    }

    /// Process a Raft message from a peer.
    #[allow(clippy::too_many_lines)] // Keep term handling and RPC dispatch in one protocol boundary.
    pub fn step(&mut self, msg: Message) -> SmallVec<[Action; 8]> {
        let mut acts: SmallVec<[Action; 8]> = SmallVec::new();
        // Universal term-bump rule (Raft §5.1): any RPC with a higher term
        // immediately reverts us to follower.
        let msg_term = match &msg {
            Message::AppendEntries { term, .. }
            | Message::AppendEntriesResponse { term, .. }
            | Message::RequestVote {
                term,
                pre_vote: false,
                ..
            }
            | Message::RequestVoteResponse {
                term,
                pre_vote: false,
                ..
            }
            | Message::InstallSnapshot { term, .. }
            | Message::InstallSnapshotResponse { term, .. }
            | Message::TimeoutNow { term, .. } => *term,
            // PreVote messages do NOT bump terms.
            Message::RequestVote {
                term: _,
                pre_vote: true,
                ..
            }
            | Message::RequestVoteResponse {
                term: _,
                pre_vote: true,
                ..
            } => 0,
        };
        if msg_term > self.hs.current_term {
            let leader_hint = match &msg {
                Message::AppendEntries { from, .. } | Message::InstallSnapshot { from, .. } => {
                    Some(*from)
                }
                _ => None,
            };
            self.become_follower(msg_term, leader_hint, &mut acts);
        }

        match msg {
            Message::AppendEntries {
                from,
                term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                request_id,
            } => {
                self.handle_append_entries(
                    from,
                    term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                    request_id,
                    &mut acts,
                );
            }
            Message::AppendEntriesResponse {
                from,
                term,
                success,
                conflict_index,
                conflict_term: _,
                last_log_index,
                request_id,
            } => {
                // Read probes are deliberately separate from replication: an
                // empty probe must never advance match_index to a follower's
                // possibly divergent suffix.
                if request_id & (1 << 63) != 0 {
                    if matches!(self.role, Role::Leader) && term == self.hs.current_term && success
                    {
                        if let Some(round) = self.read_rounds.get_mut(&request_id) {
                            round.acks.insert(from);
                        }
                        self.complete_read_rounds(&mut acts);
                    }
                    return acts;
                }
                self.handle_append_entries_response(
                    from,
                    term,
                    success,
                    conflict_index,
                    last_log_index,
                    &mut acts,
                );
            }
            Message::RequestVote {
                from,
                term,
                last_log_index,
                last_log_term,
                pre_vote,
                request_id,
            } => {
                self.handle_request_vote(
                    from,
                    term,
                    last_log_index,
                    last_log_term,
                    pre_vote,
                    request_id,
                    &mut acts,
                );
            }
            Message::RequestVoteResponse {
                from,
                term,
                vote_granted,
                pre_vote,
                request_id: _,
            } => {
                self.handle_vote_response(from, term, vote_granted, pre_vote, &mut acts);
            }
            Message::InstallSnapshot {
                from,
                term,
                last_included_index,
                last_included_term,
                data,
                request_id,
            } => {
                self.handle_install_snapshot(
                    from,
                    term,
                    last_included_index,
                    last_included_term,
                    data,
                    request_id,
                    &mut acts,
                );
            }
            Message::InstallSnapshotResponse {
                from,
                term: _,
                request_id: _,
            } => {
                if let Some(p) = self.progress.get_mut(from) {
                    if let Some(idx) = p.pending_snapshot {
                        p.snapshot_finished(idx);
                    }
                }
            }
            Message::TimeoutNow { from: _, term: _ } => {
                if matches!(self.role, Role::Follower) && self.is_voter_self() {
                    self.start_election(&mut acts);
                }
            }
        }
        acts
    }

    /// Propose a normal entry. Only valid as leader.
    pub fn propose(&mut self, data: Vec<u8>) -> Result<SmallVec<[Action; 8]>, ProposeError> {
        if !matches!(self.role, Role::Leader) {
            return Err(ProposeError::NotLeader {
                hint: self.leader_id,
            });
        }
        if self.leader_transfer_target.is_some() {
            return Err(ProposeError::LeadershipTransferInProgress);
        }
        let mut acts = SmallVec::new();
        let idx = self.log.last_index() + 1;
        let entry = Entry::normal(self.hs.current_term, idx, data);
        self.log.append(vec![entry.clone()]);
        acts.push(Action::AppendEntries(vec![entry]));
        // Leader trivially has match_index = last_log_index for self.
        if let Some(p) = self.progress.get_mut(self.cfg.id) {
            p.match_index = self.log.last_index();
            p.next_index = self.log.last_index() + 1;
        }
        self.maybe_advance_commit(&mut acts);
        self.broadcast_replication(&mut acts);
        Ok(acts)
    }

    /// Propose a configuration change. Goes through joint consensus.
    pub fn propose_config_change(
        &mut self,
        change: ConfigChange,
    ) -> Result<SmallVec<[Action; 8]>, ProposeError> {
        if !matches!(self.role, Role::Leader) {
            return Err(ProposeError::NotLeader {
                hint: self.leader_id,
            });
        }
        if self.in_joint {
            return Err(ProposeError::ConfigChangeInProgress);
        }
        let mut new_voters: BTreeSet<NodeId> = match &self.config.state {
            ConfigState::Stable(s) => s.clone(),
            ConfigState::Joint { new, .. } => new.clone(),
        };
        match change {
            ConfigChange::AddServer(id) => {
                new_voters.insert(id);
            }
            ConfigChange::RemoveServer(id) => {
                new_voters.remove(&id);
            }
        }
        let old_voters: BTreeSet<NodeId> = match &self.config.state {
            ConfigState::Stable(s) => s.clone(),
            ConfigState::Joint { old, .. } => old.clone(),
        };
        let joint_state = ConfigState::Joint {
            old: old_voters,
            new: new_voters,
        };
        let payload = bincode::serialize(&joint_state).expect("config encode");
        let idx = self.log.last_index() + 1;
        let entry = Entry {
            term: self.hs.current_term,
            index: idx,
            kind: EntryKind::ConfigJoint,
            data: payload,
        };
        self.log.append(vec![entry.clone()]);
        self.apply_config_entry(&entry);
        let mut acts = SmallVec::new();
        acts.push(Action::AppendEntries(vec![entry]));
        if let Some(p) = self.progress.get_mut(self.cfg.id) {
            p.match_index = self.log.last_index();
            p.next_index = self.log.last_index() + 1;
        }
        self.broadcast_replication(&mut acts);
        Ok(acts)
    }

    /// Begin a linearizable read-index round. Returns actions which include
    /// heartbeats whose acks the runtime correlates with the read.
    pub fn read_index(&mut self, ctx: Bytes) -> SmallVec<[Action; 8]> {
        let mut acts = SmallVec::new();
        if !matches!(self.role, Role::Leader) {
            return acts;
        }
        let rid = self.next_rid() | (1 << 63);
        self.read_rounds.insert(
            rid,
            ReadRound {
                ctx,
                commit_index: self.hs.commit_index,
                acks: BTreeSet::from([self.cfg.id]),
            },
        );
        self.broadcast_read_rounds(&mut acts);
        self.complete_read_rounds(&mut acts);
        acts
    }

    /// Send a uniquely correlated empty `AppendEntries` after a current-term
    /// entry is committed. Retransmit on heartbeats so dropped probes recover.
    fn broadcast_read_rounds(&mut self, acts: &mut SmallVec<[Action; 8]>) {
        if self.log.term_at(self.hs.commit_index) != Some(self.hs.current_term) {
            return;
        }
        let voters = self.voters();
        for (&rid, round) in &mut self.read_rounds {
            round.commit_index = round.commit_index.max(self.hs.commit_index);
            for &peer in &voters {
                if peer != self.cfg.id && !round.acks.contains(&peer) {
                    acts.push(Action::SendMessage {
                        to: peer,
                        msg: Message::AppendEntries {
                            from: self.cfg.id,
                            term: self.hs.current_term,
                            prev_log_index: self.hs.commit_index,
                            prev_log_term: self.log.term_at(self.hs.commit_index).unwrap_or(0),
                            entries: vec![],
                            leader_commit: self.hs.commit_index,
                            request_id: rid,
                        },
                    });
                }
            }
        }
    }

    fn complete_read_rounds(&mut self, acts: &mut SmallVec<[Action; 8]>) {
        if self.log.term_at(self.hs.commit_index) != Some(self.hs.current_term) {
            return;
        }
        let ready: Vec<_> = self
            .read_rounds
            .iter()
            .filter(|(_, round)| self.config.state.has_quorum(&round.acks))
            .map(|(&rid, _)| rid)
            .collect();
        for rid in ready {
            if let Some(round) = self.read_rounds.remove(&rid) {
                acts.push(Action::NotifyReadIndex {
                    ctx: round.ctx,
                    commit_index: round.commit_index,
                    request_id: rid,
                });
            }
        }
    }

    /// Cancel a client barrier after timeout without retaining probe state.
    pub fn cancel_read(&mut self, ctx: &Bytes) {
        self.read_rounds.retain(|_, round| round.ctx != ctx);
    }

    /// Initiate leadership transfer to `target` (Ongaro thesis §3.10).
    pub fn transfer_leadership(&mut self, target: NodeId) -> SmallVec<[Action; 8]> {
        let mut acts = SmallVec::new();
        if !matches!(self.role, Role::Leader) || target == self.cfg.id {
            return acts;
        }
        if !self.config.state.is_voter(target) {
            return acts;
        }
        self.leader_transfer_target = Some(target);
        acts.push(Action::Metric(MetricEvent::LeadershipTransferStarted {
            target,
        }));
        // Send TimeoutNow only if the target is caught up; otherwise wait.
        let caught_up = self
            .progress
            .get(target)
            .is_some_and(|p| p.match_index == self.log.last_index());
        if caught_up {
            acts.push(Action::SendMessage {
                to: target,
                msg: Message::TimeoutNow {
                    from: self.cfg.id,
                    term: self.hs.current_term,
                },
            });
        }
        acts
    }

    // ===================================================================
    // Election logic
    // ===================================================================

    fn become_follower(
        &mut self,
        term: Term,
        leader: Option<NodeId>,
        acts: &mut SmallVec<[Action; 8]>,
    ) {
        let term_changed = term > self.hs.current_term;
        if term_changed {
            self.hs.current_term = term;
            self.hs.voted_for = None;
            acts.push(Action::Metric(MetricEvent::TermBumped { new_term: term }));
        }
        self.role = Role::Follower;
        self.leader_id = leader;
        self.votes.clear();
        self.elapsed_election_ticks = 0;
        self.elapsed_heartbeat_ticks = 0;
        self.leader_transfer_target = None;
        self.read_rounds.clear();
        self.reset_randomized_election_timeout();
        acts.push(Action::PersistHardState(self.hs.clone()));
        acts.push(Action::ResetElectionTimer);
        acts.push(Action::BecameFollower {
            term: self.hs.current_term,
            leader,
        });
    }

    fn become_pre_candidate(&mut self, acts: &mut SmallVec<[Action; 8]>) {
        self.read_rounds.clear();
        self.role = Role::PreCandidate;
        self.leader_id = None;
        self.votes.clear();
        // Pre-candidate votes count against `current_term + 1` but no state is
        // persisted yet.
        self.votes.insert(self.cfg.id, true);
        acts.push(Action::Metric(MetricEvent::ElectionStarted {
            term: self.hs.current_term + 1,
            pre_vote: true,
        }));
    }

    fn become_candidate(&mut self, acts: &mut SmallVec<[Action; 8]>) {
        self.read_rounds.clear();
        self.hs.current_term += 1;
        self.hs.voted_for = Some(self.cfg.id);
        self.role = Role::Candidate;
        self.leader_id = None;
        self.votes.clear();
        self.votes.insert(self.cfg.id, true);
        self.elapsed_election_ticks = 0;
        self.reset_randomized_election_timeout();
        acts.push(Action::Metric(MetricEvent::TermBumped {
            new_term: self.hs.current_term,
        }));
        acts.push(Action::Metric(MetricEvent::ElectionStarted {
            term: self.hs.current_term,
            pre_vote: false,
        }));
        acts.push(Action::PersistHardState(self.hs.clone()));
    }

    fn become_leader(&mut self, acts: &mut SmallVec<[Action; 8]>) {
        self.read_rounds.clear();
        self.role = Role::Leader;
        self.leader_id = Some(self.cfg.id);
        self.elapsed_heartbeat_ticks = 0;
        self.leader_transfer_target = None;

        let last_idx = self.log.last_index();
        // Reset progress for all current voters.
        let voters = self.voters();
        self.progress.peers.clear();
        for &id in &voters {
            let mut p = Progress::new(last_idx);
            if id == self.cfg.id {
                p.match_index = last_idx;
                p.next_index = last_idx + 1;
                p.state = ProgressState::Replicate;
            }
            self.progress.peers.insert(id, p);
        }

        // Append a no-op entry to commit any prior-term entries (Figure 8).
        let idx = last_idx + 1;
        let noop = Entry::noop(self.hs.current_term, idx);
        self.log.append(vec![noop.clone()]);
        if let Some(p) = self.progress.get_mut(self.cfg.id) {
            p.match_index = self.log.last_index();
            p.next_index = self.log.last_index() + 1;
        }
        acts.push(Action::AppendEntries(vec![noop]));
        acts.push(Action::BecameLeader {
            term: self.hs.current_term,
        });
        acts.push(Action::Metric(MetricEvent::ElectionWon {
            term: self.hs.current_term,
        }));
        acts.push(Action::ResetHeartbeatTimer);
        self.maybe_advance_commit(acts);
        self.broadcast_replication(acts);
    }

    fn start_election(&mut self, acts: &mut SmallVec<[Action; 8]>) {
        if !self.is_voter_self() {
            return;
        }
        if self.cfg.pre_vote {
            self.become_pre_candidate(acts);
        } else {
            self.become_candidate(acts);
        }
        let term = if matches!(self.role, Role::PreCandidate) {
            self.hs.current_term + 1
        } else {
            self.hs.current_term
        };
        let last_log_index = self.log.last_index();
        let last_log_term = self.log.last_term();
        for peer in self.voters() {
            if peer == self.cfg.id {
                continue;
            }
            let rid = self.next_rid();
            acts.push(Action::SendMessage {
                to: peer,
                msg: Message::RequestVote {
                    from: self.cfg.id,
                    term,
                    last_log_index,
                    last_log_term,
                    pre_vote: matches!(self.role, Role::PreCandidate),
                    request_id: rid,
                },
            });
        }
        // If the cluster is a single voter (this node), trivially win.
        if self.voters().len() == 1 && self.config.state.is_voter(self.cfg.id) {
            if matches!(self.role, Role::PreCandidate) {
                self.become_candidate(acts);
            }
            self.become_leader(acts);
        }
    }

    #[allow(clippy::too_many_arguments)] // Parameters correspond directly to the wire message.
    fn handle_request_vote(
        &mut self,
        from: NodeId,
        term: Term,
        last_log_index: LogIndex,
        last_log_term: Term,
        pre_vote: bool,
        request_id: RequestId,
        acts: &mut SmallVec<[Action; 8]>,
    ) {
        let our_term = self.hs.current_term;
        // Pre-vote request term is the candidate's *proposed* term, not bumped.
        let effective_term = if pre_vote {
            term.saturating_sub(1)
        } else {
            term
        };
        let mut grant = false;

        if pre_vote {
            // PreVote: do not change state; only grant if we'd vote in `term`
            // and our election timer is at risk of firing.
            let leader_lease_active = self.elapsed_election_ticks < self.cfg.election_timeout_ticks
                && self.leader_id.is_some();
            if !leader_lease_active
                && term > our_term
                && self.log.is_up_to_date(last_log_index, last_log_term)
            {
                grant = true;
            }
            // Use the candidate's term verbatim in the response so the
            // candidate can detect competitive races.
            let _ = effective_term;
        } else {
            // Standard RequestVote (term already handled by universal bump).
            if term < our_term {
                grant = false;
            } else {
                let can_vote = self.hs.voted_for.is_none_or(|v| v == from);
                if can_vote && self.log.is_up_to_date(last_log_index, last_log_term) {
                    self.hs.voted_for = Some(from);
                    self.elapsed_election_ticks = 0;
                    grant = true;
                    acts.push(Action::PersistHardState(self.hs.clone()));
                }
            }
        }

        let resp_term = if pre_vote { term } else { self.hs.current_term };
        acts.push(Action::SendMessage {
            to: from,
            msg: Message::RequestVoteResponse {
                from: self.cfg.id,
                term: resp_term,
                vote_granted: grant,
                pre_vote,
                request_id,
            },
        });
    }

    fn handle_vote_response(
        &mut self,
        from: NodeId,
        term: Term,
        vote_granted: bool,
        pre_vote: bool,
        acts: &mut SmallVec<[Action; 8]>,
    ) {
        let expected_role = if pre_vote {
            Role::PreCandidate
        } else {
            Role::Candidate
        };
        if self.role != expected_role {
            return;
        }
        let expected_term = if pre_vote {
            self.hs.current_term + 1
        } else {
            self.hs.current_term
        };
        if term != expected_term {
            return;
        }

        self.votes.insert(from, vote_granted);
        let granted_set: BTreeSet<NodeId> = self
            .votes
            .iter()
            .filter_map(|(k, v)| v.then_some(*k))
            .collect();
        let denied = self.votes.values().filter(|v| !**v).count();

        if self.config.state.has_quorum(&granted_set) {
            // Won.
            if pre_vote {
                self.become_candidate(acts);
                let last_log_index = self.log.last_index();
                let last_log_term = self.log.last_term();
                for peer in self.voters() {
                    if peer == self.cfg.id {
                        continue;
                    }
                    let rid = self.next_rid();
                    acts.push(Action::SendMessage {
                        to: peer,
                        msg: Message::RequestVote {
                            from: self.cfg.id,
                            term: self.hs.current_term,
                            last_log_index,
                            last_log_term,
                            pre_vote: false,
                            request_id: rid,
                        },
                    });
                }
            } else {
                self.become_leader(acts);
            }
        } else if denied > self.voters().len() / 2 {
            // Cannot win — revert to follower.
            self.become_follower(self.hs.current_term, None, acts);
        }
    }

    // ===================================================================
    // Replication logic
    // ===================================================================

    fn broadcast_heartbeat(&mut self, acts: &mut SmallVec<[Action; 8]>) {
        for peer in self.voters() {
            if peer == self.cfg.id {
                continue;
            }
            self.send_append(peer, true, acts);
        }
    }

    fn broadcast_replication(&mut self, acts: &mut SmallVec<[Action; 8]>) {
        let voters = self.voters();
        for peer in voters {
            if peer == self.cfg.id {
                continue;
            }
            self.send_append(peer, false, acts);
        }
    }

    fn send_append(
        &mut self,
        peer: NodeId,
        force_heartbeat: bool,
        acts: &mut SmallVec<[Action; 8]>,
    ) {
        let max_inflight = self.cfg.max_inflight_msgs;
        let last_idx = self.log.last_index();
        let max_batch = self.cfg.max_append_entries;
        let snapshot_first = self.log.first_index();
        let snapshot_index = self.log.snapshot_index();
        let snapshot_term = self.log.snapshot_term();

        let Some(progress) = self.progress.get_mut(peer) else {
            return;
        };
        if force_heartbeat {
            // A timed-out RPC produces no response in the pure core. Reclaim
            // its accounting at the next heartbeat so a recovered peer can
            // resume data replication rather than remaining saturated forever.
            // The transport separately bounds actually outstanding RPC tasks.
            progress.inflight = 0;
        }
        if !progress.can_send(max_inflight) && !force_heartbeat {
            return;
        }
        // A successful response must not trigger another empty append forever.
        // Idle followers receive traffic only at the heartbeat cadence.
        if progress.next_index > last_idx && !force_heartbeat {
            return;
        }

        // Need snapshot?
        if progress.next_index < snapshot_first {
            progress.become_snapshot(snapshot_index);
            acts.push(Action::SendMessage {
                to: peer,
                msg: Message::InstallSnapshot {
                    from: self.cfg.id,
                    term: self.hs.current_term,
                    last_included_index: snapshot_index,
                    last_included_term: snapshot_term,
                    // The runtime fills the actual snapshot bytes; here we send a marker.
                    data: Bytes::new(),
                    request_id: 0,
                },
            });
            return;
        }

        let next = progress.next_index;
        let prev_index = next.saturating_sub(1);
        let prev_term = if prev_index == 0 {
            0
        } else if prev_index == snapshot_index {
            snapshot_term
        } else if let Some(t) = self.log.term_at(prev_index) {
            t
        } else {
            // Edge case: we no longer have prev_index — fall back to snapshot.
            progress.become_snapshot(snapshot_index);
            acts.push(Action::SendMessage {
                to: peer,
                msg: Message::InstallSnapshot {
                    from: self.cfg.id,
                    term: self.hs.current_term,
                    last_included_index: snapshot_index,
                    last_included_term: snapshot_term,
                    data: Bytes::new(),
                    request_id: 0,
                },
            });
            return;
        };

        let entries = if force_heartbeat || next > last_idx {
            Vec::new()
        } else {
            let to = (next + max_batch as u64).min(last_idx + 1);
            self.log.slice(next, to).to_vec()
        };
        progress.inflight = progress.inflight.saturating_add(1);

        let term = self.hs.current_term;
        let leader_commit = self.hs.commit_index;
        let id = self.cfg.id;
        let rid = self.next_rid();
        acts.push(Action::SendMessage {
            to: peer,
            msg: Message::AppendEntries {
                from: id,
                term,
                prev_log_index: prev_index,
                prev_log_term: prev_term,
                entries,
                leader_commit,
                request_id: rid,
            },
        });
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)] // One ordered follower transition.
    fn handle_append_entries(
        &mut self,
        from: NodeId,
        term: Term,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<Entry>,
        leader_commit: LogIndex,
        request_id: RequestId,
        acts: &mut SmallVec<[Action; 8]>,
    ) {
        // Term older than ours: reject.
        if term < self.hs.current_term {
            acts.push(Action::SendMessage {
                to: from,
                msg: Message::AppendEntriesResponse {
                    from: self.cfg.id,
                    term: self.hs.current_term,
                    success: false,
                    conflict_index: 0,
                    conflict_term: 0,
                    last_log_index: self.log.last_index(),
                    request_id,
                },
            });
            return;
        }

        // Same/higher term -> recognize sender as leader for this term.
        self.leader_id = Some(from);
        if !matches!(self.role, Role::Follower) {
            self.role = Role::Follower;
            self.read_rounds.clear();
        }
        self.elapsed_election_ticks = 0;

        // Log matching: prev_log_index must exist with matching term.
        let our_prev_term = if prev_log_index == 0 {
            Some(0)
        } else {
            self.log.term_at(prev_log_index)
        };
        let matches = our_prev_term == Some(prev_log_term);
        if !matches {
            // Construct fast back-off hint.
            let (conflict_index, conflict_term) = if prev_log_index > self.log.last_index() {
                (self.log.last_index() + 1, 0)
            } else {
                let t = self.log.term_at(prev_log_index).unwrap_or(0);
                // Find earliest index in `t`
                let mut ci = prev_log_index;
                while ci > self.log.first_index() {
                    if self.log.term_at(ci - 1) != Some(t) {
                        break;
                    }
                    ci -= 1;
                }
                (ci, t)
            };
            acts.push(Action::Metric(MetricEvent::AppendRejected {
                from: self.cfg.id,
            }));
            acts.push(Action::SendMessage {
                to: from,
                msg: Message::AppendEntriesResponse {
                    from: self.cfg.id,
                    term: self.hs.current_term,
                    success: false,
                    conflict_index,
                    conflict_term,
                    last_log_index: self.log.last_index(),
                    request_id,
                },
            });
            return;
        }

        let matched_through = entries.last().map_or(prev_log_index, |e| e.index);
        // Append / overwrite: walk new entries, find the first conflict, then
        // truncate and append.
        if !entries.is_empty() {
            let mut conflict_at: Option<LogIndex> = None;
            for e in &entries {
                match self.log.term_at(e.index) {
                    Some(existing) if existing == e.term => {}
                    Some(_) | None => {
                        conflict_at = Some(e.index);
                        break;
                    }
                }
            }
            if let Some(ci) = conflict_at {
                if ci <= self.log.last_index() {
                    self.log.truncate_from(ci);
                    acts.push(Action::TruncateLog { from: ci });
                }
                let to_append: Vec<Entry> = entries.into_iter().filter(|e| e.index >= ci).collect();
                if !to_append.is_empty() {
                    // Apply config entries observed by follower.
                    for e in &to_append {
                        if matches!(e.kind, EntryKind::ConfigJoint | EntryKind::ConfigNew) {
                            self.apply_config_entry(e);
                        }
                    }
                    self.log.append(to_append.clone());
                    acts.push(Action::AppendEntries(to_append));
                }
            }
        }

        // Advance commit index.
        // An unmatched local suffix is not known to belong to this leader.
        let new_commit = leader_commit.min(matched_through);
        if new_commit > self.hs.commit_index {
            self.hs.commit_index = new_commit;
            acts.push(Action::Metric(MetricEvent::CommitAdvanced {
                index: new_commit,
            }));
            acts.push(Action::PersistHardState(self.hs.clone()));
            self.flush_apply(acts);
        }

        acts.push(Action::SendMessage {
            to: from,
            msg: Message::AppendEntriesResponse {
                from: self.cfg.id,
                term: self.hs.current_term,
                success: true,
                conflict_index: 0,
                conflict_term: 0,
                last_log_index: matched_through,
                request_id,
            },
        });
    }

    fn handle_append_entries_response(
        &mut self,
        from: NodeId,
        term: Term,
        success: bool,
        conflict_index: LogIndex,
        last_log_index: LogIndex,
        acts: &mut SmallVec<[Action; 8]>,
    ) {
        if !matches!(self.role, Role::Leader) || term != self.hs.current_term {
            return;
        }
        let Some(progress) = self.progress.get_mut(from) else {
            return;
        };
        progress.recent_active = true;
        progress.inflight = progress.inflight.saturating_sub(1);

        if success {
            progress.maybe_update(last_log_index);
            // Possibly advance commit.
            self.maybe_advance_commit(acts);
            // Continue replicating in case there is more.
            self.send_append(from, false, acts);
            // Leadership transfer hand-off.
            if let Some(target) = self.leader_transfer_target {
                if target == from {
                    if let Some(p) = self.progress.get(target) {
                        if p.match_index == self.log.last_index() {
                            acts.push(Action::SendMessage {
                                to: target,
                                msg: Message::TimeoutNow {
                                    from: self.cfg.id,
                                    term: self.hs.current_term,
                                },
                            });
                        }
                    }
                }
            }
        } else {
            progress.maybe_decr_to(conflict_index);
            self.send_append(from, false, acts);
        }
    }

    fn maybe_advance_commit(&mut self, acts: &mut SmallVec<[Action; 8]>) {
        // Only commit entries from current term (Figure 8 caveat).
        // For joint consensus, require quorum across BOTH sets independently.
        let new_commit = match &self.config.state {
            ConfigState::Stable(s) => {
                if s.is_empty() {
                    return;
                }
                let mut sorted: Vec<LogIndex> = s
                    .iter()
                    .map(|id| self.progress.get(*id).map_or(0, |p| p.match_index))
                    .collect();
                sorted.sort_unstable();
                sorted[sorted.len() - (s.len() / 2 + 1)]
            }
            ConfigState::Joint { old, new } => {
                let mut so: Vec<LogIndex> = old
                    .iter()
                    .map(|id| self.progress.get(*id).map_or(0, |p| p.match_index))
                    .collect();
                let mut sn: Vec<LogIndex> = new
                    .iter()
                    .map(|id| self.progress.get(*id).map_or(0, |p| p.match_index))
                    .collect();
                if so.is_empty() || sn.is_empty() {
                    return;
                }
                so.sort_unstable();
                sn.sort_unstable();
                let mo = so[so.len() - (old.len() / 2 + 1)];
                let mn = sn[sn.len() - (new.len() / 2 + 1)];
                mo.min(mn)
            }
        };

        if new_commit > self.hs.commit_index {
            // Only commit if entry@new_commit is from current term.
            if self.log.term_at(new_commit) == Some(self.hs.current_term) {
                self.hs.commit_index = new_commit;
                acts.push(Action::Metric(MetricEvent::CommitAdvanced {
                    index: new_commit,
                }));
                acts.push(Action::PersistHardState(self.hs.clone()));
                self.flush_apply(acts);
                self.maybe_finish_joint(acts);
                self.broadcast_read_rounds(acts);
                self.complete_read_rounds(acts);
            }
        }
    }

    fn flush_apply(&mut self, acts: &mut SmallVec<[Action; 8]>) {
        if self.hs.commit_index > self.last_applied {
            let from = self.last_applied + 1;
            let to = self.hs.commit_index + 1;
            let entries = self.log.slice(from, to).to_vec();
            if !entries.is_empty() {
                self.last_applied = self.hs.commit_index;
                acts.push(Action::ApplyCommitted { entries });
            }
        }
    }

    // ===================================================================
    // Snapshot handling
    // ===================================================================

    #[allow(clippy::too_many_arguments)]
    fn handle_install_snapshot(
        &mut self,
        from: NodeId,
        term: Term,
        last_included_index: LogIndex,
        last_included_term: Term,
        data: Bytes,
        request_id: RequestId,
        acts: &mut SmallVec<[Action; 8]>,
    ) {
        if term < self.hs.current_term {
            acts.push(Action::SendMessage {
                to: from,
                msg: Message::InstallSnapshotResponse {
                    from: self.cfg.id,
                    term: self.hs.current_term,
                    request_id,
                },
            });
            return;
        }
        self.leader_id = Some(from);
        self.role = Role::Follower;
        self.elapsed_election_ticks = 0;

        // If our log already covers the snapshot, ignore.
        if last_included_index <= self.log.snapshot_index() {
            acts.push(Action::SendMessage {
                to: from,
                msg: Message::InstallSnapshotResponse {
                    from: self.cfg.id,
                    term: self.hs.current_term,
                    request_id,
                },
            });
            return;
        }

        self.log
            .compact_through(last_included_index, last_included_term);
        self.last_applied = last_included_index;
        if self.hs.commit_index < last_included_index {
            self.hs.commit_index = last_included_index;
            acts.push(Action::PersistHardState(self.hs.clone()));
        }
        acts.push(Action::InstallSnapshot {
            last_included_index,
            last_included_term,
            data,
        });
        acts.push(Action::SendMessage {
            to: from,
            msg: Message::InstallSnapshotResponse {
                from: self.cfg.id,
                term: self.hs.current_term,
                request_id,
            },
        });
    }

    /// Inform the core that a local snapshot has been taken at `index/term`.
    /// The core will compact its in-memory log accordingly.
    pub fn on_snapshot_taken(&mut self, last_included_index: LogIndex, last_included_term: Term) {
        self.log
            .compact_through(last_included_index, last_included_term);
    }

    // ===================================================================
    // Configuration handling
    // ===================================================================

    fn apply_config_entry(&mut self, entry: &Entry) {
        match entry.kind {
            EntryKind::ConfigJoint => {
                if let Ok(state) = bincode::deserialize::<ConfigState>(&entry.data) {
                    self.config.state = state;
                    self.in_joint = true;
                    let voters = self.voters();
                    if matches!(self.role, Role::Leader) {
                        self.progress.ensure(&voters, self.log.last_index());
                        self.progress.retain_in(&voters);
                    }
                }
            }
            EntryKind::ConfigNew => {
                if let Ok(state) = bincode::deserialize::<ConfigState>(&entry.data) {
                    self.config.state = state;
                    self.in_joint = false;
                    let voters = self.voters();
                    if matches!(self.role, Role::Leader) {
                        self.progress.ensure(&voters, self.log.last_index());
                        self.progress.retain_in(&voters);
                    }
                }
            }
            _ => {}
        }
    }

    fn maybe_finish_joint(&mut self, acts: &mut SmallVec<[Action; 8]>) {
        if !self.in_joint || !matches!(self.role, Role::Leader) {
            return;
        }
        // Once `C_old,new` is committed, append `C_new`.
        let new_state = match &self.config.state {
            ConfigState::Joint { new, .. } => ConfigState::Stable(new.clone()),
            ConfigState::Stable(_) => return,
        };
        let payload = bincode::serialize(&new_state).expect("config encode");
        let idx = self.log.last_index() + 1;
        let entry = Entry {
            term: self.hs.current_term,
            index: idx,
            kind: EntryKind::ConfigNew,
            data: payload,
        };
        self.log.append(vec![entry.clone()]);
        self.apply_config_entry(&entry);
        if let Some(p) = self.progress.get_mut(self.cfg.id) {
            p.match_index = self.log.last_index();
            p.next_index = self.log.last_index() + 1;
        }
        acts.push(Action::AppendEntries(vec![entry]));
        self.broadcast_replication(acts);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leader(voters: Vec<NodeId>) -> RaftNode {
        let mut n = RaftNode::new(
            cfg(1, voters),
            HardState {
                current_term: 1,
                ..HardState::default()
            },
            RaftLog::new(),
        );
        n.become_leader(&mut SmallVec::new());
        n
    }

    fn ack(n: &mut RaftNode, from: NodeId, index: u64, request_id: u64) -> SmallVec<[Action; 8]> {
        n.step(Message::AppendEntriesResponse {
            from,
            term: n.hs.current_term,
            success: true,
            conflict_index: 0,
            conflict_term: 0,
            last_log_index: index,
            request_id,
        })
    }

    #[test]
    fn even_cluster_requires_strict_majority() {
        for count in [2, 4, 6] {
            let mut n = leader((1..=count).collect());
            for peer in 2..=count / 2 {
                ack(&mut n, peer, 1, 0);
            }
            assert_eq!(n.commit_index(), 0, "half of {count} must not commit");
            ack(&mut n, count / 2 + 1, 1, 0);
            assert_eq!(n.commit_index(), 1);
        }
    }

    #[test]
    fn missing_progress_is_not_a_smaller_voter_set() {
        let mut n = leader(vec![1, 2, 3, 4, 5]);
        n.progress.peers.remove(&3);
        n.progress.peers.remove(&4);
        n.progress.peers.remove(&5);
        ack(&mut n, 2, 1, 0);
        assert_eq!(n.commit_index(), 0);
    }

    #[test]
    fn joint_commit_requires_strict_majority_of_each_set() {
        let mut n = leader(vec![1, 2, 3, 4, 5, 6]);
        n.config.state = ConfigState::Joint {
            old: BTreeSet::from([1, 2, 3, 4]),
            new: BTreeSet::from([1, 2, 5, 6]),
        };
        ack(&mut n, 2, 1, 0);
        assert_eq!(n.commit_index(), 0);
        ack(&mut n, 3, 1, 0);
        assert_eq!(n.commit_index(), 0, "new set lacks a majority");
        ack(&mut n, 5, 1, 0);
        assert_eq!(n.commit_index(), 1);
    }

    #[test]
    fn idle_ack_does_not_start_an_append_response_loop() {
        let mut n = leader(vec![1, 2, 3]);
        let actions = ack(&mut n, 2, 1, 0);
        assert!(!actions
            .iter()
            .any(|a| matches!(a, Action::SendMessage { to: 2, .. })));
    }

    #[test]
    fn heartbeat_reclaims_timed_out_inflight_for_recovered_peer() {
        let mut n = leader(vec![1, 2, 3]);
        n.progress.get_mut(2).unwrap().inflight = u32::MAX;
        let mut actions = SmallVec::new();
        n.send_append(2, true, &mut actions);
        assert_eq!(n.progress.get(2).unwrap().inflight, 1);
        let actions = ack(&mut n, 2, 0, 0);
        assert!(actions.iter().any(|action| matches!(action,
            Action::SendMessage { to: 2, msg: Message::AppendEntries { entries, .. } } if !entries.is_empty())));
    }

    #[test]
    fn read_waits_for_current_term_commit_and_its_own_quorum() {
        let mut n = leader(vec![1, 2, 3]);
        let ctx = Bytes::from_static(b"read-1");
        let actions = n.read_index(ctx.clone());
        assert!(
            actions.is_empty(),
            "no read before the leader's no-op commits"
        );
        let actions = ack(&mut n, 2, 1, 10);
        assert!(!actions
            .iter()
            .any(|a| matches!(a, Action::NotifyReadIndex { .. })));
        let rid = *n.read_rounds.keys().next().unwrap();
        let stale_ack = ack(&mut n, 3, 1, 11);
        assert!(!stale_ack
            .iter()
            .any(|a| matches!(a, Action::NotifyReadIndex { .. })));
        let actions = ack(&mut n, 2, 1, rid);
        assert!(actions.iter().any(
            |a| matches!(a, Action::NotifyReadIndex { ctx: c, commit_index: 1, .. } if *c == ctx)
        ));
        // Late duplicates must not be mistaken for replication confirmations.
        ack(&mut n, 3, 999, rid);
        assert_ne!(n.progress.get(3).unwrap().match_index, 999);
    }

    #[test]
    fn duplicate_or_nonvoter_read_acks_cannot_form_quorum() {
        let mut n = leader(vec![1, 2, 3, 4, 5]);
        ack(&mut n, 2, 1, 0);
        ack(&mut n, 3, 1, 0);
        n.read_index(Bytes::from_static(b"read"));
        let rid = *n.read_rounds.keys().next().unwrap();
        for peer in [2, 2, 99] {
            assert!(!ack(&mut n, peer, 1, rid)
                .iter()
                .any(|a| matches!(a, Action::NotifyReadIndex { .. })));
        }
        assert!(ack(&mut n, 3, 1, rid)
            .iter()
            .any(|a| matches!(a, Action::NotifyReadIndex { .. })));
    }

    #[test]
    fn stepping_down_discards_read_rounds() {
        let mut n = leader(vec![1, 2, 3]);
        ack(&mut n, 2, 1, 0);
        n.read_index(Bytes::from_static(b"read"));
        n.step(Message::TimeoutNow { from: 2, term: 2 });
        assert!(n.read_rounds.is_empty());
    }

    #[test]
    fn single_node_commits_election_noop_and_can_read_immediately() {
        let mut n = leader(vec![1]);
        assert_eq!(n.commit_index(), 1);
        assert!(n
            .read_index(Bytes::new())
            .iter()
            .any(|a| matches!(a, Action::NotifyReadIndex { .. })));
    }

    #[test]
    fn heartbeat_does_not_acknowledge_or_commit_divergent_suffix() {
        let mut log = RaftLog::new();
        log.append(vec![
            Entry::noop(1, 1),
            Entry::normal(2, 2, b"divergent".to_vec()),
        ]);
        let mut n = RaftNode::new(
            cfg(2, vec![1, 2, 3]),
            HardState {
                current_term: 3,
                ..HardState::default()
            },
            log,
        );
        let actions = n.step(Message::AppendEntries {
            from: 1,
            term: 3,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![],
            leader_commit: 2,
            request_id: 9,
        });
        assert_eq!(n.commit_index(), 1);
        assert!(actions.iter().any(|a| matches!(
            a,
            Action::SendMessage {
                msg: Message::AppendEntriesResponse {
                    last_log_index: 1,
                    success: true,
                    ..
                },
                ..
            }
        )));
    }

    fn cfg(id: NodeId, voters: Vec<NodeId>) -> RaftConfig {
        RaftConfig {
            id,
            initial_voters: voters,
            election_timeout_ticks: 10,
            heartbeat_ticks: 1,
            max_append_entries: 8,
            max_inflight_msgs: 16,
            rng_seed: 42,
            pre_vote: true,
        }
    }

    #[test]
    fn single_node_elects_self() {
        let mut n = RaftNode::new(cfg(1, vec![1]), HardState::default(), RaftLog::new());
        // tick until election fires
        for _ in 0..50 {
            let _ = n.tick();
        }
        assert!(matches!(n.role(), Role::Leader));
    }

    #[test]
    fn three_node_election_with_majority() {
        let mut a = RaftNode::new(cfg(1, vec![1, 2, 3]), HardState::default(), RaftLog::new());
        // Force election.
        for _ in 0..50 {
            let _ = a.tick();
        }
        // a should be Candidate (or PreCandidate) waiting for votes.
        assert!(matches!(a.role(), Role::Candidate | Role::PreCandidate));
        // Simulate granted vote from peer 2.
        let term = a.hard_state().current_term;
        let _ = a.step(Message::RequestVoteResponse {
            from: 2,
            term: if matches!(a.role(), Role::PreCandidate) {
                term + 1
            } else {
                term
            },
            vote_granted: true,
            pre_vote: matches!(a.role(), Role::PreCandidate),
            request_id: 0,
        });
        // If pre-vote, that promotes to candidate; need another grant.
        if matches!(a.role(), Role::Candidate) {
            let term = a.hard_state().current_term;
            let _ = a.step(Message::RequestVoteResponse {
                from: 2,
                term,
                vote_granted: true,
                pre_vote: false,
                request_id: 0,
            });
        }
        assert!(
            matches!(a.role(), Role::Leader),
            "expected leader after majority votes"
        );
    }

    #[test]
    fn append_entries_truncates_conflicting_suffix() {
        let mut n = RaftNode::new(cfg(2, vec![1, 2, 3]), HardState::default(), RaftLog::new());
        // Pretend leader 1 sends entries with prev=0.
        let entries = vec![
            Entry::normal(1, 1, b"a".to_vec()),
            Entry::normal(1, 2, b"b".to_vec()),
        ];
        let _ = n.step(Message::AppendEntries {
            from: 1,
            term: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries,
            leader_commit: 0,
            request_id: 1,
        });
        assert_eq!(n.last_log_index(), 2);
        // New leader (term 2) overwrites index 2.
        let _ = n.step(Message::AppendEntries {
            from: 1,
            term: 2,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![Entry::normal(2, 2, b"B".to_vec())],
            leader_commit: 0,
            request_id: 2,
        });
        assert_eq!(n.last_log_index(), 2);
    }
}
