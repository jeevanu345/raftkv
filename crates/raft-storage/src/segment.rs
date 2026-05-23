//! Single segment file for the segmented Raft log.
//!
//! On-disk record format:
//!
//! ```text
//! [u32 little-endian: payload length]
//! [u32 little-endian: crc32c of payload]
//! [u8;  payload (bincode-encoded raft_core::Entry)]
//! ```
//!
//! A torn write at the tail (length read but not all payload bytes flushed)
//! is detected by:
//!   * length-prefix that runs past EOF, OR
//!   * crc32c mismatch
//! Either case truncates the segment at the start of the bad record.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use raft_core::log::{Entry, LogIndex};

use crate::error::StorageError;

/// Append-only log segment.
pub struct Segment {
    pub(crate) path: PathBuf,
    pub(crate) base_index: LogIndex,
    pub(crate) file: File,
    pub(crate) write_offset: u64,
    pub(crate) index: Vec<(LogIndex, u64)>, // (index, file_offset)
    pub(crate) max_size_bytes: u64,
    pub(crate) sync_each: bool,
}

impl Segment {
    /// Create a new segment file (truncating any existing file).
    pub fn create(path: &Path, base_index: LogIndex, max_size_bytes: u64, sync_each: bool) -> Result<Self, StorageError> {
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            base_index,
            file,
            write_offset: 0,
            index: Vec::new(),
            max_size_bytes,
            sync_each,
        })
    }

    /// Open an existing segment, scanning it to recover the index and detect
    /// torn writes. Returns the loaded segment plus the highest valid index.
    pub fn open(path: &Path, base_index: LogIndex, max_size_bytes: u64, sync_each: bool) -> Result<(Self, Option<LogIndex>), StorageError> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let len = file.metadata()?.len();
        file.seek(SeekFrom::Start(0))?;

        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut offset: usize = 0;
        let mut index: Vec<(LogIndex, u64)> = Vec::new();
        let mut last_good_off: u64 = 0;
        let mut highest: Option<LogIndex> = None;

        while offset + 8 <= buf.len() {
            let length = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap()) as usize;
            let crc = u32::from_le_bytes(buf[offset + 4..offset + 8].try_into().unwrap());
            let payload_start = offset + 8;
            let payload_end = payload_start + length;
            if payload_end > buf.len() { break; }
            let payload = &buf[payload_start..payload_end];
            let actual = crc32c::crc32c(payload);
            if actual != crc { break; }
            let entry: Entry = match bincode::deserialize(payload) {
                Ok(e) => e,
                Err(_) => break,
            };
            index.push((entry.index, offset as u64));
            highest = Some(entry.index);
            offset = payload_end;
            last_good_off = offset as u64;
        }
        // Truncate any garbage tail.
        if last_good_off < len {
            file.set_len(last_good_off)?;
        }
        file.seek(SeekFrom::End(0))?;
        Ok((
            Self {
                path: path.to_path_buf(),
                base_index,
                file,
                write_offset: last_good_off,
                index,
                max_size_bytes,
                sync_each,
            },
            highest,
        ))
    }

    /// Append one entry.
    pub fn append(&mut self, entry: &Entry) -> Result<(), StorageError> {
        let payload = bincode::serialize(entry).map_err(|e| StorageError::Codec(e.to_string()))?;
        let crc = crc32c::crc32c(&payload);
        let length = u32::try_from(payload.len()).map_err(|_| StorageError::Invariant("entry too large"))?;
        let mut hdr = [0u8; 8];
        hdr[..4].copy_from_slice(&length.to_le_bytes());
        hdr[4..].copy_from_slice(&crc.to_le_bytes());

        let off = self.write_offset;
        self.file.write_all(&hdr)?;
        self.file.write_all(&payload)?;
        self.write_offset += 8 + payload.len() as u64;
        self.index.push((entry.index, off));
        if self.sync_each {
            self.file.sync_data()?;
        }
        Ok(())
    }

    /// fsync (caller-controlled, used by group commit).
    pub fn sync(&mut self) -> Result<(), StorageError> {
        self.file.sync_data()?;
        Ok(())
    }

    /// Whether this segment is at or above its max size.
    pub fn is_full(&self) -> bool { self.write_offset >= self.max_size_bytes }

    /// Last log index in this segment, if any.
    pub fn last_index(&self) -> Option<LogIndex> { self.index.last().map(|(i, _)| *i) }

    /// Read entry at `index`, if present in this segment.
    pub fn read(&mut self, index: LogIndex) -> Result<Option<Entry>, StorageError> {
        let pos = match self.index.iter().find(|(i, _)| *i == index) {
            Some((_, p)) => *p,
            None => return Ok(None),
        };
        self.file.seek(SeekFrom::Start(pos))?;
        let mut hdr = [0u8; 8];
        self.file.read_exact(&mut hdr)?;
        let length = u32::from_le_bytes(hdr[..4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(hdr[4..].try_into().unwrap());
        let mut buf = vec![0u8; length];
        self.file.read_exact(&mut buf)?;
        if crc32c::crc32c(&buf) != crc {
            return Err(StorageError::Crc { file: self.path.display().to_string(), offset: pos });
        }
        let entry: Entry = bincode::deserialize(&buf).map_err(|e| StorageError::Codec(e.to_string()))?;
        self.file.seek(SeekFrom::End(0))?;
        Ok(Some(entry))
    }

    /// Truncate so that no entries with index >= `from` remain.
    pub fn truncate_from(&mut self, from: LogIndex) -> Result<(), StorageError> {
        // Find first entry index >= from
        let split = self.index.iter().position(|(i, _)| *i >= from);
        if let Some(pos) = split {
            let off = self.index[pos].1;
            self.index.truncate(pos);
            self.file.set_len(off)?;
            self.file.seek(SeekFrom::End(0))?;
            self.write_offset = off;
        }
        Ok(())
    }
}
