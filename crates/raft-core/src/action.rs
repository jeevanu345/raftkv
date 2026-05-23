//! Actions the state machine asks the runtime to perform.
//!
//! The pure core never performs I/O. Every side effect goes through this
//! enum, which the runtime interprets.

use bytes::Bytes;

use crate::log::{Entry, LogIndex, Term};
use crate::message::{Message, RequestId};
use crate::node::NodeId;
use crate::state::HardState;

/// Side-effect requests emitted by the core.
#[derive(Debug, Clone)]
pub enum Action {
    /// Persist `(currentTerm, votedFor, commitIndex)` before any network reply
    /// that depends on them (Raft §5.2).
    PersistHardState(HardState),
    /// Append entries to durable log storage.
    AppendEntries(Vec<Entry>),
    /// Truncate the log so no entries with `index >= from` remain.
    TruncateLog {
        /// Truncation start (inclusive).
        from: LogIndex,
    },
    /// Send a Raft message to a peer.
    SendMessage {
        /// Destination peer.
        to: NodeId,
        /// Message body.
        msg: Message,
    },
    /// Apply a slice of committed entries to the state machine.
    ApplyCommitted {
        /// Newly applicable entries (already in the durable log).
        entries: Vec<Entry>,
    },
    /// Leader requests a snapshot be taken at or before the given index.
    TakeSnapshot {
        /// Last included index of the snapshot.
        last_included_index: LogIndex,
        /// Term at `last_included_index`.
        last_included_term: Term,
    },
    /// Install a received snapshot (replace state machine + truncate log).
    InstallSnapshot {
        /// Snapshot meta.
        last_included_index: LogIndex,
        /// Snapshot meta.
        last_included_term: Term,
        /// Snapshot bytes.
        data: Bytes,
    },
    /// Reset the election timer (election timeout starts now).
    ResetElectionTimer,
    /// Reset the heartbeat timer (only meaningful as leader).
    ResetHeartbeatTimer,
    /// Notify any read-index waiter that index `commit_index` is observed by a
    /// quorum and reads up to that point may proceed once applied.
    NotifyReadIndex {
        /// Opaque caller-supplied context.
        ctx: Bytes,
        /// Confirmed commit index at the time of issuance.
        commit_index: LogIndex,
        /// Trace correlation.
        request_id: RequestId,
    },
    /// Promotion to leader has just occurred — used by the runtime for
    /// metrics/triggers.
    BecameLeader {
        /// Term in which leadership was won.
        term: Term,
    },
    /// Demotion to follower for a given (term, leader-hint).
    BecameFollower {
        /// New current term.
        term: Term,
        /// Best-known leader, if any.
        leader: Option<NodeId>,
    },
    /// Free-form metric event for observability.
    Metric(MetricEvent),
}

/// Observable events for metrics + tracing.
#[derive(Debug, Clone)]
pub enum MetricEvent {
    /// Term was advanced.
    TermBumped {
        /// New term.
        new_term: Term,
    },
    /// An election started.
    ElectionStarted {
        /// Election term.
        term: Term,
        /// Whether this was a pre-vote.
        pre_vote: bool,
    },
    /// An election succeeded.
    ElectionWon {
        /// Won-in term.
        term: Term,
    },
    /// AppendEntries was rejected by a follower.
    AppendRejected {
        /// Peer rejecting.
        from: NodeId,
    },
    /// Commit index advanced.
    CommitAdvanced {
        /// New commit index.
        index: LogIndex,
    },
    /// Leadership transfer initiated.
    LeadershipTransferStarted {
        /// Target node id.
        target: NodeId,
    },
}
