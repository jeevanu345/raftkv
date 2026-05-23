//! In-memory representation of the Raft log.
//!
//! The pure core keeps the log abstractly. Persistence is the runtime's
//! responsibility (via [`crate::Action::AppendEntries`] and
//! [`crate::Action::TruncateLog`]).

use serde::{Deserialize, Serialize};

/// Raft term number (monotonic).
pub type Term = u64;

/// Raft log index (1-based; 0 is the sentinel "empty log" value).
pub type LogIndex = u64;

/// Kind of log entry — discriminator for both wire and disk encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryKind {
    /// A no-op entry leaders append on election (see Raft §8 commit safety).
    Noop,
    /// A user/state-machine command.
    Normal,
    /// Joint-consensus configuration entry (`C_old,new`).
    ConfigJoint,
    /// Final new configuration (`C_new`).
    ConfigNew,
}

/// A single Raft log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    /// Term in which the entry was created by the leader.
    pub term: Term,
    /// Position in the log (1-based).
    pub index: LogIndex,
    /// Entry kind.
    pub kind: EntryKind,
    /// Opaque payload — the state machine interprets this for `Normal`,
    /// the config module for `ConfigJoint`/`ConfigNew`.
    pub data: Vec<u8>,
}

impl Entry {
    /// Build a normal command entry.
    pub fn normal(term: Term, index: LogIndex, data: Vec<u8>) -> Self {
        Self { term, index, kind: EntryKind::Normal, data }
    }

    /// Build a no-op entry.
    pub fn noop(term: Term, index: LogIndex) -> Self {
        Self { term, index, kind: EntryKind::Noop, data: Vec::new() }
    }

    /// Approximate serialized size for batching decisions.
    pub fn approx_size(&self) -> usize {
        24 + self.data.len()
    }
}

/// Pure in-memory log. Persistence is handled by the runtime via Actions.
///
/// Invariants:
/// * `entries[i].index == base_index + i + 1`
/// * Indices are strictly monotonic.
/// * Terms are non-decreasing along the log.
#[derive(Debug, Clone, Default)]
pub struct RaftLog {
    /// Entries strictly *after* the snapshot point.
    entries: Vec<Entry>,
    /// Index of the last entry covered by the snapshot. Entries[0].index =
    /// snapshot_last_index + 1.
    snapshot_last_index: LogIndex,
    /// Term of the entry at `snapshot_last_index`.
    snapshot_last_term: Term,
}

impl RaftLog {
    /// Construct an empty log.
    pub fn new() -> Self { Self::default() }

    /// Restore from a snapshot point with no live entries.
    pub fn from_snapshot(last_index: LogIndex, last_term: Term) -> Self {
        Self { entries: Vec::new(), snapshot_last_index: last_index, snapshot_last_term: last_term }
    }

    /// Index of the last entry (or snapshot tip if log is empty).
    pub fn last_index(&self) -> LogIndex {
        self.entries.last().map_or(self.snapshot_last_index, |e| e.index)
    }

    /// Term of the last entry (or snapshot tip).
    pub fn last_term(&self) -> Term {
        self.entries.last().map_or(self.snapshot_last_term, |e| e.term)
    }

    /// First live index (i.e. snapshot_last_index + 1).
    pub fn first_index(&self) -> LogIndex {
        self.snapshot_last_index + 1
    }

    /// Snapshot tip index.
    pub fn snapshot_index(&self) -> LogIndex { self.snapshot_last_index }
    /// Snapshot tip term.
    pub fn snapshot_term(&self) -> Term { self.snapshot_last_term }

    /// Term at a given index, if known.
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == 0 {
            return Some(0);
        }
        if index == self.snapshot_last_index {
            return Some(self.snapshot_last_term);
        }
        if index < self.snapshot_last_index || index > self.last_index() {
            return None;
        }
        let off = (index - self.snapshot_last_index - 1) as usize;
        self.entries.get(off).map(|e| e.term)
    }

    /// Borrow entry at `index` (live entries only).
    pub fn get(&self, index: LogIndex) -> Option<&Entry> {
        if index <= self.snapshot_last_index { return None; }
        let off = (index - self.snapshot_last_index - 1) as usize;
        self.entries.get(off)
    }

    /// Slice of entries with indices in `[from, to)` (clamped to live range).
    pub fn slice(&self, from: LogIndex, to: LogIndex) -> &[Entry] {
        if to <= from || from > self.last_index() { return &[]; }
        let lo_idx = from.max(self.snapshot_last_index + 1);
        let hi_idx = to.min(self.last_index() + 1);
        if hi_idx <= lo_idx { return &[]; }
        let lo = (lo_idx - self.snapshot_last_index - 1) as usize;
        let hi = (hi_idx - self.snapshot_last_index - 1) as usize;
        &self.entries[lo..hi]
    }

    /// Append entries in monotonic order. Caller guarantees indices line up.
    pub fn append(&mut self, mut new_entries: Vec<Entry>) {
        if new_entries.is_empty() { return; }
        let expected = self.last_index() + 1;
        debug_assert_eq!(
            new_entries[0].index, expected,
            "append must be contiguous: expected {} got {}", expected, new_entries[0].index
        );
        // Monotonic terms invariant
        if let Some(last) = self.entries.last() {
            debug_assert!(new_entries[0].term >= last.term, "terms must be non-decreasing");
        }
        self.entries.append(&mut new_entries);
    }

    /// Truncate the log so that no entries with index >= `from` remain.
    /// Used when a follower discovers a conflict.
    pub fn truncate_from(&mut self, from: LogIndex) {
        if from <= self.snapshot_last_index {
            // Must not truncate into the snapshot.
            self.entries.clear();
            return;
        }
        let off = (from - self.snapshot_last_index - 1) as usize;
        if off < self.entries.len() {
            self.entries.truncate(off);
        }
    }

    /// Discard entries up to and including `last_included_index`, then set the
    /// snapshot pointer.
    pub fn compact_through(&mut self, last_included_index: LogIndex, last_included_term: Term) {
        if last_included_index <= self.snapshot_last_index {
            return;
        }
        if last_included_index >= self.last_index() {
            self.entries.clear();
        } else {
            let off = (last_included_index - self.snapshot_last_index) as usize;
            self.entries.drain(0..off);
        }
        self.snapshot_last_index = last_included_index;
        self.snapshot_last_term = last_included_term;
    }

    /// Number of live entries.
    pub fn len(&self) -> usize { self.entries.len() }
    /// True if no live entries.
    pub fn is_empty(&self) -> bool { self.entries.is_empty() }

    /// Determine whether a candidate's log is at least as up-to-date as ours
    /// (Raft §5.4.1).
    pub fn is_up_to_date(&self, last_log_index: LogIndex, last_log_term: Term) -> bool {
        let our_term = self.last_term();
        let our_index = self.last_index();
        if last_log_term != our_term {
            last_log_term > our_term
        } else {
            last_log_index >= our_index
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_and_query() {
        let mut log = RaftLog::new();
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);

        log.append(vec![
            Entry::noop(1, 1),
            Entry::normal(1, 2, b"a".to_vec()),
            Entry::normal(2, 3, b"b".to_vec()),
        ]);
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
        assert_eq!(log.get(2).unwrap().data, b"a");
        assert_eq!(log.term_at(2), Some(1));
        assert_eq!(log.term_at(3), Some(2));
        assert_eq!(log.term_at(0), Some(0));
        assert_eq!(log.term_at(99), None);
    }

    #[test]
    fn truncate_from() {
        let mut log = RaftLog::new();
        log.append(vec![
            Entry::normal(1, 1, vec![1]),
            Entry::normal(1, 2, vec![2]),
            Entry::normal(1, 3, vec![3]),
        ]);
        log.truncate_from(2);
        assert_eq!(log.last_index(), 1);
        assert!(log.get(2).is_none());
    }

    #[test]
    fn up_to_date_check() {
        let mut log = RaftLog::new();
        log.append(vec![Entry::normal(2, 1, vec![1])]);
        // Higher term wins regardless of index.
        assert!(log.is_up_to_date(0, 3));
        // Same term, lower index loses.
        assert!(!log.is_up_to_date(0, 2));
        // Same (term, index) is a tie -> up to date.
        assert!(log.is_up_to_date(1, 2));
        // Lower term loses.
        assert!(!log.is_up_to_date(99, 1));
    }

    #[test]
    fn compaction_preserves_tail() {
        let mut log = RaftLog::new();
        log.append(vec![
            Entry::normal(1, 1, vec![1]),
            Entry::normal(1, 2, vec![2]),
            Entry::normal(1, 3, vec![3]),
            Entry::normal(1, 4, vec![4]),
        ]);
        log.compact_through(2, 1);
        assert_eq!(log.first_index(), 3);
        assert_eq!(log.last_index(), 4);
        assert_eq!(log.snapshot_index(), 2);
        assert_eq!(log.term_at(2), Some(1));
        assert_eq!(log.get(3).unwrap().data, vec![3]);
    }
}
