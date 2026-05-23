//! Storage error types.

use thiserror::Error;

/// All storage errors.
#[derive(Debug, Error)]
pub enum StorageError {
    /// I/O failed.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// Sled metadata error.
    #[error("sled error: {0}")]
    Sled(#[from] sled::Error),
    /// Encoding/decoding failed.
    #[error("codec error: {0}")]
    Codec(String),
    /// CRC mismatch detected on read.
    #[error("crc mismatch at offset {offset} in {file}")]
    Crc {
        /// File path.
        file: String,
        /// Byte offset.
        offset: u64,
    },
    /// Tried to read an index outside the log.
    #[error("log index {0} out of range")]
    OutOfRange(u64),
    /// Invariant violated.
    #[error("invariant: {0}")]
    Invariant(&'static str),
}
