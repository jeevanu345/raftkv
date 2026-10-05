//! Snapshot storage: write-to-temp + atomic rename. Crash-safe.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use raft_core::log::{LogIndex, Term};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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
        Ok(Self {
            dir: path.as_ref().to_path_buf(),
        })
    }

    /// Path of the latest snapshot, if any.
    pub fn latest(&self) -> Option<(SnapshotMeta, PathBuf)> {
        let mut entries: Vec<_> = fs::read_dir(&self.dir)
            .ok()?
            .filter_map(|e| e.ok())
            .collect();
        entries.sort_by_key(|e| e.file_name());
        let last = entries
            .into_iter()
            .rev()
            .find(|e| e.path().extension().and_then(|s| s.to_str()) == Some("snap"))?;
        let meta_path = last.path().with_extension("meta");
        let mut buf = Vec::new();
        File::open(&meta_path).ok()?.read_to_end(&mut buf).ok()?;
        let meta: SnapshotMeta = bincode::deserialize(&buf).ok()?;
        Some((meta, last.path()))
    }

    /// Read a complete checkpoint and verify data AND membership metadata.
    pub fn read_checked(&self, path: &Path) -> Result<Vec<u8>, StorageError> {
        let data = fs::read(path)?;
        let metadata = fs::read(path.with_extension("meta"))?;
        let expected = fs::read_to_string(path.with_extension("sha256"))?;
        let mut hash = Sha256::new();
        hash.update(&metadata);
        hash.update(&data);
        if hex::encode(hash.finalize()) != expected.trim() {
            return Err(StorageError::Invariant("snapshot file checksum mismatch"));
        }
        Ok(data)
    }
    /// Begin writing a snapshot to a temp file. Returns a handle.
    pub fn begin_write(&self, meta: &SnapshotMeta) -> Result<SnapshotWriter, StorageError> {
        let stem = format!(
            "{:020}-{}",
            meta.last_included_index, meta.last_included_term
        );
        let snap_path = self.dir.join(format!("{stem}.snap"));
        let tmp_path = self.dir.join(format!("{stem}.snap.tmp"));
        let meta_path = self.dir.join(format!("{stem}.meta"));
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)?;
        Ok(SnapshotWriter {
            tmp_path,
            snap_path,
            meta_path,
            meta: meta.clone(),
            file,
            hasher: {
                let mut hash = Sha256::new();
                hash.update(
                    bincode::serialize(meta).map_err(|e| StorageError::Codec(e.to_string()))?,
                );
                hash
            },
        })
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
        if snaps.len() <= n {
            return Ok(());
        }
        let drop_n = snaps.len() - n;
        for p in &snaps[..drop_n] {
            let _ = fs::remove_file(p);
            let _ = fs::remove_file(p.with_extension("meta"));
            let _ = fs::remove_file(p.with_extension("sha256"));
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
    hasher: Sha256,
}

impl SnapshotWriter {
    /// Write a chunk.
    pub fn write_chunk(&mut self, data: &[u8]) -> Result<(), StorageError> {
        self.file.write_all(data)?;
        self.hasher.update(data);
        Ok(())
    }

    /// fsync and atomically install the snapshot.
    pub fn finish(self) -> Result<(), StorageError> {
        crate::durability::sync_all(&self.file)?;
        let checksum = hex::encode(self.hasher.finalize());
        // Never replace one published identity through separate renames: a crash
        // between them could otherwise invalidate the previous checkpoint.
        if self.snap_path.exists() {
            let old = fs::read_to_string(self.snap_path.with_extension("sha256"))?;
            let mut prior = Sha256::new();
            prior.update(fs::read(&self.meta_path)?);
            prior.update(fs::read(&self.snap_path)?);
            if old.trim() != checksum || hex::encode(prior.finalize()) != checksum {
                return Err(StorageError::Invariant(
                    "snapshot identity reused with different state",
                ));
            }
            let _ = fs::remove_file(&self.tmp_path);
            return Ok(());
        }
        // Write meta first (also via temp+rename for atomicity).
        let meta_tmp = self.meta_path.with_extension("meta.tmp");
        let bytes =
            bincode::serialize(&self.meta).map_err(|e| StorageError::Codec(e.to_string()))?;
        let mut meta_file = File::create(&meta_tmp)?;
        meta_file.write_all(&bytes)?;
        crate::durability::sync_all(&meta_file)?;
        let sum = self.snap_path.with_extension("sha256");
        let sum_tmp = sum.with_extension("sha256.tmp");
        let mut file = File::create(&sum_tmp)?;
        file.write_all(checksum.as_bytes())?;
        crate::durability::sync_all(&file)?;
        fs::rename(sum_tmp, sum)?;
        std::fs::rename(&meta_tmp, &self.meta_path)?;
        std::fs::rename(&self.tmp_path, &self.snap_path)?;
        if let Some(dir) = self.snap_path.parent() {
            crate::durability::sync_all(&File::open(dir)?)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn detects_state_and_membership_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        let meta = SnapshotMeta {
            last_included_index: 2,
            last_included_term: 1,
            config: vec![1],
        };
        let mut writer = store.begin_write(&meta).unwrap();
        writer.write_chunk(b"state").unwrap();
        writer.finish().unwrap();
        let (_, path) = store.latest().unwrap();
        assert_eq!(store.read_checked(&path).unwrap(), b"state");
        fs::write(path.with_extension("meta"), b"corrupt").unwrap();
        assert!(store.read_checked(&path).is_err());
    }
    #[test]
    fn incomplete_checkpoint_never_replaces_published_state() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        let meta = SnapshotMeta {
            last_included_index: 2,
            last_included_term: 1,
            config: vec![1],
        };
        let mut writer = store.begin_write(&meta).unwrap();
        writer.write_chunk(b"old").unwrap();
        writer.finish().unwrap();
        let newer = SnapshotMeta {
            last_included_index: 3,
            ..meta.clone()
        };
        let mut unfinished = store.begin_write(&newer).unwrap();
        unfinished.write_chunk(b"partial").unwrap();
        drop(unfinished);
        let (saved, path) = store.latest().unwrap();
        assert_eq!(saved.last_included_index, 2);
        assert_eq!(store.read_checked(&path).unwrap(), b"old");
        let mut duplicate = store.begin_write(&meta).unwrap();
        duplicate.write_chunk(b"different").unwrap();
        assert!(duplicate.finish().is_err());
        assert_eq!(store.read_checked(&path).unwrap(), b"old");
    }
}
