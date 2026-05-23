//! Snapshot storage: write-to-temp + atomic rename. Crash-safe.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use raft_core::log::{LogIndex, Term};
use serde::{Deserialize, Serialize};

use crate::error::StorageError;

/// Metadata for a snapshot file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMeta {
    /// Last log index covered.
    pub last_included_index: LogIndex,
    /// Term at `last_included_index`.
    pub last_included_term: Term,
    /// Optional cluster config bytes.
    pub config: Vec<u8>,
}

/// Snapshot file management with atomic rename.
pub struct SnapshotStore {
    dir: PathBuf,
}

impl SnapshotStore {
    /// Open or create the snapshot directory.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        fs::create_dir_all(&path)?;
        Ok(Self { dir: path.as_ref().to_path_buf() })
    }

    /// Path of the latest snapshot, if any.
    pub fn latest(&self) -> Option<(SnapshotMeta, PathBuf)> {
        let mut entries: Vec<_> = fs::read_dir(&self.dir).ok()?.filter_map(|e| e.ok()).collect();
        entries.sort_by_key(|e| e.file_name());
        let last = entries.into_iter().rev().find(|e| {
            e.path().extension().and_then(|s| s.to_str()) == Some("snap")
        })?;
        let meta_path = last.path().with_extension("meta");
        let mut buf = Vec::new();
        File::open(&meta_path).ok()?.read_to_end(&mut buf).ok()?;
        let meta: SnapshotMeta = bincode::deserialize(&buf).ok()?;
        Some((meta, last.path()))
    }

    /// Begin writing a snapshot to a temp file. Returns a handle.
    pub fn begin_write(&self, meta: &SnapshotMeta) -> Result<SnapshotWriter, StorageError> {
        let stem = format!("{:020}-{}", meta.last_included_index, meta.last_included_term);
        let snap_path = self.dir.join(format!("{stem}.snap"));
        let tmp_path = self.dir.join(format!("{stem}.snap.tmp"));
        let meta_path = self.dir.join(format!("{stem}.meta"));
        let file = OpenOptions::new().create(true).truncate(true).write(true).open(&tmp_path)?;
        Ok(SnapshotWriter { tmp_path, snap_path, meta_path, meta: meta.clone(), file })
    }

    /// List snapshots (oldest first).
    pub fn list(&self) -> Vec<PathBuf> {
        let mut out: Vec<_> = fs::read_dir(&self.dir)
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("snap"))
            .collect();
        out.sort();
        out
    }

    /// Remove all but the most recent N snapshots.
    pub fn keep_last(&self, n: usize) -> Result<(), StorageError> {
        let snaps = self.list();
        if snaps.len() <= n { return Ok(()); }
        let drop_n = snaps.len() - n;
        for p in &snaps[..drop_n] {
            let _ = fs::remove_file(p);
            let _ = fs::remove_file(p.with_extension("meta"));
        }
        Ok(())
    }
}

/// A snapshot in the process of being written.
pub struct SnapshotWriter {
    tmp_path: PathBuf,
    snap_path: PathBuf,
    meta_path: PathBuf,
    meta: SnapshotMeta,
    file: File,
}

impl SnapshotWriter {
    /// Write a chunk.
    pub fn write_chunk(&mut self, data: &[u8]) -> Result<(), StorageError> {
        self.file.write_all(data)?;
        Ok(())
    }

    /// fsync and atomically install the snapshot.
    pub fn finish(self) -> Result<(), StorageError> {
        self.file.sync_all()?;
        // Write meta first (also via temp+rename for atomicity).
        let meta_tmp = self.meta_path.with_extension("meta.tmp");
        let bytes = bincode::serialize(&self.meta).map_err(|e| StorageError::Codec(e.to_string()))?;
        std::fs::write(&meta_tmp, bytes)?;
        std::fs::rename(&meta_tmp, &self.meta_path)?;
        std::fs::rename(&self.tmp_path, &self.snap_path)?;
        Ok(())
    }
}
