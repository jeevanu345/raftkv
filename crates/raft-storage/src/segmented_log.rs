//! Append-only segmented log with crash-safe recovery.
//!
//! Disk layout: `<dir>/00000000000000000001.log`, where the number is the
//! 1-based index of the first entry stored in that segment.

use std::fs;
use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use raft_core::log::{Entry, LogIndex};

use crate::error::StorageError;
use crate::segment::Segment;

/// Configuration for a segmented log.
#[derive(Debug, Clone)]
pub struct SegmentedLogConfig {
    /// Directory containing segment files.
    pub dir: PathBuf,
    /// Roll segments at this size (default 64 MiB).
    pub max_segment_bytes: u64,
    /// fsync after every append (slow, safest) or only on `flush()` (fast).
    pub sync_each_append: bool,
}

impl Default for SegmentedLogConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("./data/raft-log"),
            max_segment_bytes: 64 * 1024 * 1024,
            sync_each_append: false,
        }
    }
}

/// Append-only segmented log.
pub struct SegmentedLog {
    cfg: SegmentedLogConfig,
    inner: Mutex<Inner>,
}

struct Inner {
    segments: Vec<Segment>,
    last_index: LogIndex,
    /// Snapshot tip below which entries are not retained.
    snapshot_index: LogIndex,
}

fn segment_filename(base_index: LogIndex) -> String { format!("{:020}.log", base_index) }

fn parse_segment_index(p: &Path) -> Option<LogIndex> {
    let stem = p.file_stem()?.to_str()?;
    stem.parse::<u64>().ok()
}

impl SegmentedLog {
    /// Open or create the log directory.
    pub fn open(cfg: SegmentedLogConfig) -> Result<Self, StorageError> {
        fs::create_dir_all(&cfg.dir)?;
        let mut entries: Vec<PathBuf> = fs::read_dir(&cfg.dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("log"))
            .collect();
        entries.sort();

        let mut segments: Vec<Segment> = Vec::new();
        let mut last_index: LogIndex = 0;
        for path in entries {
            let base = parse_segment_index(&path).ok_or(StorageError::Invariant("bad segment filename"))?;
            let (seg, hi) = Segment::open(&path, base, cfg.max_segment_bytes, cfg.sync_each_append)?;
            if let Some(h) = hi { last_index = last_index.max(h); }
            segments.push(seg);
        }
        if segments.is_empty() {
            let path = cfg.dir.join(segment_filename(1));
            let seg = Segment::create(&path, 1, cfg.max_segment_bytes, cfg.sync_each_append)?;
            segments.push(seg);
        }
        Ok(Self {
            cfg,
            inner: Mutex::new(Inner { segments, last_index, snapshot_index: 0 }),
        })
    }

    /// Last index stored in the log.
    pub fn last_index(&self) -> LogIndex { self.inner.lock().last_index }

    /// First index that may still be served from the log (snapshot+1).
    pub fn first_index(&self) -> LogIndex { self.inner.lock().snapshot_index + 1 }

    /// Append a batch of entries (single fsync at end).
    pub fn append(&self, entries: &[Entry]) -> Result<(), StorageError> {
        if entries.is_empty() { return Ok(()); }
        let mut inner = self.inner.lock();
        let expected = inner.last_index + 1;
        if entries[0].index != expected {
            return Err(StorageError::Invariant("non-contiguous append"));
        }
        for e in entries {
            // Roll segment if full.
            let needs_roll = inner
                .segments
                .last()
                .map(Segment::is_full)
                .unwrap_or(true);
            if needs_roll {
                let path = self.cfg.dir.join(segment_filename(e.index));
                let seg = Segment::create(&path, e.index, self.cfg.max_segment_bytes, self.cfg.sync_each_append)?;
                inner.segments.push(seg);
            }
            let seg = inner.segments.last_mut().expect("segment");
            seg.append(e)?;
            inner.last_index = e.index;
        }
        if let Some(seg) = inner.segments.last_mut() { seg.sync()?; }
        Ok(())
    }

    /// Force an fsync.
    pub fn flush(&self) -> Result<(), StorageError> {
        let mut inner = self.inner.lock();
        if let Some(seg) = inner.segments.last_mut() { seg.sync()?; }
        Ok(())
    }

    /// Read a single entry by index.
    pub fn read(&self, index: LogIndex) -> Result<Option<Entry>, StorageError> {
        let mut inner = self.inner.lock();
        if index <= inner.snapshot_index || index > inner.last_index { return Ok(None); }
        // Find the right segment (largest base_index <= index).
        let pos = match inner.segments.iter().rposition(|s| s.base_index <= index) {
            Some(p) => p,
            None => return Ok(None),
        };
        inner.segments[pos].read(index)
    }

    /// Read entries with indices in `[from, to)`. Caller-supplied buffer to
    /// avoid allocation churn in hot paths.
    pub fn read_range(&self, from: LogIndex, to: LogIndex, out: &mut Vec<Entry>) -> Result<(), StorageError> {
        out.clear();
        if to <= from { return Ok(()); }
        for i in from..to {
            if let Some(e) = self.read(i)? { out.push(e); } else { break; }
        }
        Ok(())
    }

    /// Truncate so no entry with `index >= from` remains.
    pub fn truncate_from(&self, from: LogIndex) -> Result<(), StorageError> {
        let mut inner = self.inner.lock();
        if from > inner.last_index { return Ok(()); }
        // Drop whole segments whose base_index >= from
        while let Some(seg) = inner.segments.last() {
            if seg.base_index >= from {
                let p = seg.path.clone();
                inner.segments.pop();
                drop(fs::remove_file(p));
            } else { break; }
        }
        if let Some(seg) = inner.segments.last_mut() {
            seg.truncate_from(from)?;
            inner.last_index = seg.last_index().unwrap_or(seg.base_index.saturating_sub(1));
        } else {
            // recreate empty initial segment
            let path = self.cfg.dir.join(segment_filename(from.max(1)));
            let seg = Segment::create(&path, from.max(1), self.cfg.max_segment_bytes, self.cfg.sync_each_append)?;
            inner.segments.push(seg);
            inner.last_index = from.saturating_sub(1);
        }
        Ok(())
    }

    /// Drop entries up to `last_included_index` (inclusive). Used after a
    /// snapshot.
    pub fn compact(&self, last_included_index: LogIndex) -> Result<(), StorageError> {
        let mut inner = self.inner.lock();
        // Drop full segments whose final index <= last_included_index, but keep
        // at least one segment around.
        let mut to_drop: Vec<PathBuf> = Vec::new();
        while inner.segments.len() > 1 {
            let drop_first = match inner.segments.first().and_then(|s| s.last_index()) {
                Some(li) => li <= last_included_index,
                None => false,
            };
            if drop_first {
                let s = inner.segments.remove(0);
                to_drop.push(s.path.clone());
            } else { break; }
        }
        for p in to_drop { let _ = fs::remove_file(p); }
        inner.snapshot_index = last_included_index;
        Ok(())
    }

    /// Inform the log of the current snapshot tip without compacting (used on
    /// restart after a snapshot install).
    pub fn set_snapshot_tip(&self, idx: LogIndex) {
        let mut inner = self.inner.lock();
        inner.snapshot_index = idx;
        inner.last_index = inner.last_index.max(idx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft_core::log::Entry;
    use tempfile::tempdir;

    #[test]
    fn append_and_read_roundtrip() {
        let dir = tempdir().unwrap();
        let log = SegmentedLog::open(SegmentedLogConfig {
            dir: dir.path().to_path_buf(),
            max_segment_bytes: 1024,
            sync_each_append: false,
        }).unwrap();
        let entries = (1u64..=20).map(|i| Entry::normal(1, i, vec![i as u8; 50])).collect::<Vec<_>>();
        log.append(&entries).unwrap();
        assert_eq!(log.last_index(), 20);
        for i in 1u64..=20 {
            assert_eq!(log.read(i).unwrap().unwrap().index, i);
        }
    }

    #[test]
    fn truncate_drops_tail() {
        let dir = tempdir().unwrap();
        let log = SegmentedLog::open(SegmentedLogConfig { dir: dir.path().into(), max_segment_bytes: 64*1024, sync_each_append: false }).unwrap();
        let entries: Vec<_> = (1..=10).map(|i| Entry::normal(1, i, vec![1])).collect();
        log.append(&entries).unwrap();
        log.truncate_from(6).unwrap();
        assert_eq!(log.last_index(), 5);
        assert!(log.read(6).unwrap().is_none());
    }

    #[test]
    fn reopen_recovers_entries() {
        let dir = tempdir().unwrap();
        {
            let log = SegmentedLog::open(SegmentedLogConfig { dir: dir.path().into(), max_segment_bytes: 64*1024, sync_each_append: true }).unwrap();
            let entries: Vec<_> = (1..=5).map(|i| Entry::normal(1, i, vec![1])).collect();
            log.append(&entries).unwrap();
        }
        let log = SegmentedLog::open(SegmentedLogConfig { dir: dir.path().into(), max_segment_bytes: 64*1024, sync_each_append: false }).unwrap();
        assert_eq!(log.last_index(), 5);
    }
}
