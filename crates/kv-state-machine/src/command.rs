//! Command and response types. Encoded with bincode into Raft log entries.

use serde::{Deserialize, Serialize};

/// Logical command type. The leader records `commit_ts_millis` (taken from the
/// proposing leader's monotonic clock) so that TTL evaluation is deterministic
/// across replicas (every replica sees the same timestamp in the same entry).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    /// SET key value [EX seconds]
    Set {
        /// Key.
        key: Vec<u8>,
        /// Value.
        value: Vec<u8>,
        /// Optional absolute expiry, in milliseconds since the proposing
        /// leader's epoch (filled in by the proposer).
        expire_at_ms: Option<u64>,
    },
    /// DEL key1 key2 ...
    Del {
        /// Keys to delete.
        keys: Vec<Vec<u8>>,
    },
    /// INCR key
    Incr {
        /// Key.
        key: Vec<u8>,
        /// Delta (can be negative for DECR).
        delta: i64,
    },
    /// EXPIRE key seconds  (absolute time computed at proposal time).
    Expire {
        /// Key.
        key: Vec<u8>,
        /// Absolute expiry ms (deterministic).
        expire_at_ms: u64,
    },
    /// PERSIST key
    Persist {
        /// Key.
        key: Vec<u8>,
    },
    /// MSET k1 v1 k2 v2 ...
    MSet {
        /// Pairs.
        pairs: Vec<(Vec<u8>, Vec<u8>)>,
    },
    /// FLUSHDB
    FlushDb,
    /// Tick: a no-op command carrying the current time so the sweeper can
    /// evict expired keys deterministically.
    Tick {
        /// Now in ms.
        now_ms: u64,
    },
}

/// Response to a command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    /// "OK" string.
    Ok,
    /// Simple integer reply.
    Int(i64),
    /// Bulk reply (`Some` value or nil).
    Bulk(Option<Vec<u8>>),
    /// Multi-bulk reply.
    Array(Vec<Option<Vec<u8>>>),
    /// Error reply.
    Error(String),
}
