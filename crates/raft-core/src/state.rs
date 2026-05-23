//! Persistent and volatile state of a Raft node.

use serde::{Deserialize, Serialize};

use crate::log::{LogIndex, Term};
use crate::node::NodeId;
use crate::role::Role;

/// State that **must** be persisted before responding to election/replication
/// RPCs (Raft §5.2, §5.3).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardState {
    /// Latest term server has seen (initialized to 0, increases monotonically).
    pub current_term: Term,
    /// Candidate id that received vote in the current term, or `None`.
    pub voted_for: Option<NodeId>,
    /// Highest log index known to be committed. Persisting this is optional for
    /// safety (Raft) but useful for fast restart.
    pub commit_index: LogIndex,
}

/// Volatile state observable by the runtime / metrics layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SoftState {
    /// Current role.
    pub role: Role,
    /// Best-known leader for the current term.
    pub leader_id: Option<NodeId>,
}

impl Default for SoftState {
    fn default() -> Self {
        Self { role: Role::Follower, leader_id: None }
    }
}
