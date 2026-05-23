//! Internal message types — the alphabet the state machine consumes.
//!
//! These mirror but are independent from the wire `proto/raft.proto` types so
//! the core doesn't depend on prost/tonic.

use bytes::Bytes;

use crate::log::{Entry, LogIndex, Term};
use crate::node::NodeId;

/// Identifier for correlating a request with its response in tracing/metrics.
pub type RequestId = u64;

/// Internal Raft messages.
#[derive(Debug, Clone)]
pub enum Message {
    /// AppendEntries (with optional heartbeat = empty entries).
    AppendEntries {
        /// Sender (leader).
        from: NodeId,
        /// Leader's term.
        term: Term,
        /// Index immediately preceding the new entries.
        prev_log_index: LogIndex,
        /// Term of `prev_log_index`.
        prev_log_term: Term,
        /// New entries (may be empty for heartbeat).
        entries: Vec<Entry>,
        /// Leader's commit index.
        leader_commit: LogIndex,
        /// Trace correlation id.
        request_id: RequestId,
    },
    /// AppendEntries response.
    AppendEntriesResponse {
        /// Responder.
        from: NodeId,
        /// Responder's current term.
        term: Term,
        /// Whether the entries were accepted.
        success: bool,
        /// Hint for fast back-off (Ongaro thesis §3.5).
        conflict_index: LogIndex,
        /// Conflict term hint.
        conflict_term: Term,
        /// Responder's last log index — used to advance matchIndex on success.
        last_log_index: LogIndex,
        /// Trace correlation id.
        request_id: RequestId,
    },
    /// RequestVote (or PreVote when `pre_vote` is true).
    RequestVote {
        /// Sender (candidate).
        from: NodeId,
        /// Candidate's term (or proposed term for pre-vote).
        term: Term,
        /// Candidate's last log index.
        last_log_index: LogIndex,
        /// Candidate's last log term.
        last_log_term: Term,
        /// True if this is a PreVote (does not bump receiver's term).
        pre_vote: bool,
        /// Trace correlation id.
        request_id: RequestId,
    },
    /// RequestVote response.
    RequestVoteResponse {
        /// Responder.
        from: NodeId,
        /// Responder's term.
        term: Term,
        /// Whether the vote was granted.
        vote_granted: bool,
        /// Whether this corresponds to a pre-vote.
        pre_vote: bool,
        /// Trace correlation id.
        request_id: RequestId,
    },
    /// InstallSnapshot RPC (chunked transfer is the runtime's responsibility;
    /// the core sees a single logical message per snapshot).
    InstallSnapshot {
        /// Sender (leader).
        from: NodeId,
        /// Leader's term.
        term: Term,
        /// Last index covered by the snapshot.
        last_included_index: LogIndex,
        /// Term at `last_included_index`.
        last_included_term: Term,
        /// Snapshot bytes (state machine + config).
        data: Bytes,
        /// Trace correlation id.
        request_id: RequestId,
    },
    /// InstallSnapshot response.
    InstallSnapshotResponse {
        /// Responder.
        from: NodeId,
        /// Responder's term.
        term: Term,
        /// Trace correlation id.
        request_id: RequestId,
    },
    /// Leadership transfer trigger (Ongaro thesis §3.10).
    TimeoutNow {
        /// Sender (current leader).
        from: NodeId,
        /// Sender's term.
        term: Term,
    },
}
