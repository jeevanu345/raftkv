//! Sled-backed Raft persistent metadata: `(currentTerm, votedFor, commitIndex)`
//! plus snapshot bookmarks.

use std::path::Path;

use raft_core::log::{LogIndex, Term};
use raft_core::state::HardState;

use crate::error::StorageError;

const KEY_HARD_STATE: &[u8] = b"hs";
const KEY_SNAP_INDEX: &[u8] = b"snap_index";
const KEY_SNAP_TERM: &[u8] = b"snap_term";

/// Sled-backed metadata store.
pub struct MetaStore {
    db: sled::Db,
}

impl MetaStore {
    /// Open or create the metadata db at `path`.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        let db = sled::Config::new()
            .path(path)
            .flush_every_ms(Some(50))
            .open()?;
        Ok(Self { db })
    }

    /// Persist `HardState` and fsync. Must complete before responding to RPCs
    /// that depend on the state.
    pub fn save_hard_state(&self, hs: &HardState) -> Result<(), StorageError> {
        let bytes = bincode::serialize(hs).map_err(|e| StorageError::Codec(e.to_string()))?;
        self.db.insert(KEY_HARD_STATE, bytes)?;
        {
            let _timer = crate::durability::fsync_histogram().start_timer();
            self.db.flush()?;
        }
        Ok(())
    }

    /// Read `HardState`, or return default.
    pub fn load_hard_state(&self) -> Result<HardState, StorageError> {
        match self.db.get(KEY_HARD_STATE)? {
            Some(b) => bincode::deserialize(&b).map_err(|e| StorageError::Codec(e.to_string())),
            None => Ok(HardState::default()),
        }
    }

    /// Persist snapshot meta `(last_included_index, last_included_term)`.
    pub fn save_snapshot_pointer(&self, idx: LogIndex, term: Term) -> Result<(), StorageError> {
        let mut batch = sled::Batch::default();
        batch.insert(KEY_SNAP_INDEX, &idx.to_le_bytes());
        batch.insert(KEY_SNAP_TERM, &term.to_le_bytes());
        self.db.apply_batch(batch)?;
        {
            let _timer = crate::durability::fsync_histogram().start_timer();
            self.db.flush()?;
        }
        Ok(())
    }

    /// Read snapshot meta if present.
    pub fn load_snapshot_pointer(&self) -> Result<Option<(LogIndex, Term)>, StorageError> {
        let i = self.db.get(KEY_SNAP_INDEX)?;
        let t = self.db.get(KEY_SNAP_TERM)?;
        match (i, t) {
            (Some(i), Some(t)) if i.len() == 8 && t.len() == 8 => Ok(Some((
                u64::from_le_bytes(i.as_ref().try_into().unwrap()),
                u64::from_le_bytes(t.as_ref().try_into().unwrap()),
            ))),
            (None, None) => Ok(None),
            _ => Err(StorageError::Invariant("invalid snapshot pointer")),
        }
    }
}
