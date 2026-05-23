//! Roles a Raft server may occupy.

use serde::{Deserialize, Serialize};

/// Raft roles. `PreCandidate` is the pre-vote extension (Ongaro thesis §9.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    /// Default role; redirects clients to the leader.
    Follower,
    /// Probing peers to determine election viability without bumping the term.
    PreCandidate,
    /// Has bumped term and is collecting votes.
    Candidate,
    /// Replicating entries.
    Leader,
}

impl Role {
    /// Short string for logs/metrics.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Follower => "follower",
            Self::PreCandidate => "precandidate",
            Self::Candidate => "candidate",
            Self::Leader => "leader",
        }
    }
}
