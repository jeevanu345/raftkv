#![deny(unsafe_code)]
//! Durable storage for the Raft consensus log + metadata.
//!
//! Contains:
//! * [`segmented_log::SegmentedLog`] — append-only log with CRC32C-checked
//!   records, segment rotation, configurable fsync, and torn-write recovery.
//! * [`meta::MetaStore`] — sled-backed `(currentTerm, votedFor, commitIndex)`.
//! * [`snapshot_store::SnapshotStore`] — atomic snapshot file management.

pub mod error;
pub mod meta;
pub mod segment;
pub mod segmented_log;
pub mod snapshot_store;

pub use error::StorageError;
pub use meta::MetaStore;
pub use segmented_log::SegmentedLog;
pub use snapshot_store::{SnapshotMeta, SnapshotStore};
