#![deny(unsafe_code)]
#![warn(missing_docs)]
//! Pure, deterministic Raft consensus state machine.
//!
//! This crate is intentionally free of I/O, async, clocks, and OS-level
//! randomness. It exposes a `RaftNode` that consumes [`Message`]s and produces
//! [`Action`]s for a runtime to interpret. This separation is what makes the
//! consensus logic deterministically testable (see TigerBeetle VOPR,
//! FoundationDB simulation, and the etcd/raft pure state machine).
//!
//! Reference: Ongaro & Ousterhout, "In Search of an Understandable Consensus
//! Algorithm", USENIX ATC 2014; Ongaro PhD thesis 2014.

pub mod action;
pub mod config;
pub mod error;
pub mod log;
pub mod message;
pub mod node;
pub mod progress;
pub mod role;
pub mod state;

pub use action::{Action, MetricEvent};
pub use config::{ClusterConfig, ConfigChange, ConfigState};
pub use error::{ProposeError, RaftError};
pub use log::{Entry, EntryKind, LogIndex, RaftLog, Term};
pub use message::Message;
pub use node::{NodeId, RaftConfig, RaftNode};
pub use role::Role;
pub use state::{HardState, SoftState};
