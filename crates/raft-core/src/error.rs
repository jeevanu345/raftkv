//! Error types for the consensus core.

use thiserror::Error;

/// Errors a client proposal may produce.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ProposeError {
    /// This node is not the leader; the runtime should redirect to the hint.
    #[error("not leader (hint: {hint:?})")]
    NotLeader {
        /// Best-known leader id, if any.
        hint: Option<u64>,
    },
    /// A configuration change is already in flight.
    #[error("configuration change already in progress")]
    ConfigChangeInProgress,
    /// The proposal was rejected because leadership transfer is in progress.
    #[error("leadership transfer in progress")]
    LeadershipTransferInProgress,
    /// Proposal payload exceeds limits.
    #[error("proposal too large: {0} bytes")]
    ProposalTooLarge(usize),
}

/// Top-level core errors (rare; most invariant violations are debug-asserted).
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RaftError {
    /// Invariant violated (should never occur in correct code).
    #[error("raft invariant violated: {0}")]
    InvariantViolation(String),
    /// Bad input from the runtime.
    #[error("invalid input: {0}")]
    InvalidInput(String),
}
