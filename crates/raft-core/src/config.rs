//! Cluster configuration and joint-consensus state machine.
//!
//! Implements §6 of the Raft paper and §4 of the Ongaro thesis.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::node::NodeId;

/// Configuration phase.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConfigState {
    /// Stable configuration `C`.
    Stable(BTreeSet<NodeId>),
    /// Joint configuration `C_old,new`.
    Joint {
        /// Old voter set.
        old: BTreeSet<NodeId>,
        /// New voter set.
        new: BTreeSet<NodeId>,
    },
}

impl Default for ConfigState {
    fn default() -> Self { Self::Stable(BTreeSet::new()) }
}

impl ConfigState {
    /// All voters relevant for any quorum check.
    pub fn voters(&self) -> Vec<NodeId> {
        match self {
            Self::Stable(s) => s.iter().copied().collect(),
            Self::Joint { old, new } => old.union(new).copied().collect(),
        }
    }

    /// Returns true if `id` is a voter in any current configuration.
    pub fn is_voter(&self, id: NodeId) -> bool {
        match self {
            Self::Stable(s) => s.contains(&id),
            Self::Joint { old, new } => old.contains(&id) || new.contains(&id),
        }
    }

    /// Quorum size for a stable configuration.
    fn quorum(set: &BTreeSet<NodeId>) -> usize { set.len() / 2 + 1 }

    /// Determine whether `acks` represents a quorum, accounting for joint
    /// configs (which require quorum in BOTH old and new).
    pub fn has_quorum(&self, acks: &BTreeSet<NodeId>) -> bool {
        match self {
            Self::Stable(s) => {
                if s.is_empty() { return false; }
                let need = Self::quorum(s);
                s.iter().filter(|id| acks.contains(id)).count() >= need
            }
            Self::Joint { old, new } => {
                let need_old = Self::quorum(old);
                let need_new = Self::quorum(new);
                let got_old = old.iter().filter(|id| acks.contains(id)).count();
                let got_new = new.iter().filter(|id| acks.contains(id)).count();
                got_old >= need_old && got_new >= need_new
            }
        }
    }
}

/// Top-level cluster configuration. Has both the committed state and any
/// pending in-flight transition.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Currently active config state.
    pub state: ConfigState,
}

impl ClusterConfig {
    /// Construct a stable config from a list of voters.
    pub fn stable<I: IntoIterator<Item = NodeId>>(voters: I) -> Self {
        Self { state: ConfigState::Stable(voters.into_iter().collect()) }
    }
}

/// A configuration-change request from a client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConfigChange {
    /// Add a voting server.
    AddServer(NodeId),
    /// Remove a server.
    RemoveServer(NodeId),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(ids: &[NodeId]) -> BTreeSet<NodeId> { ids.iter().copied().collect() }

    #[test]
    fn quorum_stable() {
        let cfg = ConfigState::Stable(set(&[1, 2, 3]));
        assert!(!cfg.has_quorum(&set(&[1])));
        assert!(cfg.has_quorum(&set(&[1, 2])));
        assert!(cfg.has_quorum(&set(&[1, 2, 3])));
    }

    #[test]
    fn quorum_joint_requires_both() {
        let cfg = ConfigState::Joint { old: set(&[1, 2, 3]), new: set(&[3, 4, 5]) };
        // Quorum in old (1,2) but not new (only 3)
        assert!(!cfg.has_quorum(&set(&[1, 2, 3])));
        // Quorum in both
        assert!(cfg.has_quorum(&set(&[1, 2, 3, 4])));
    }
}
