#![deny(unsafe_code)]
//! Deterministic KV state machine applied from the committed Raft log.
//!
//! NOTE: The original spec calls for RocksDB. This implementation uses sled
//! to avoid the RocksDB build toolchain dependency. Both are LSM-based and
//! provide WAL durability + atomic flush semantics; the trait-based
//! abstraction means swapping the backend later is mechanical. See
//! ARCHITECTURE.md → Design Decisions.

pub mod command;
pub mod state;

pub use command::{Command, Response};
pub use state::{KvStateMachine, StateMachineError};
