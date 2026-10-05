//! Versioned user backup envelope, distinct from the Raft snapshot transport.
use raft_core::config::ClusterConfig;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Durable consensus checkpoint and logical state.
#[derive(Serialize, Deserialize)]
pub struct SnapshotPackage {
    pub version: u32,
    pub index: u64,
    pub term: u64,
    pub config: ClusterConfig,
    pub state: Vec<u8>,
}
/// Portable backup metadata.
#[derive(Serialize, Deserialize, Debug)]
pub struct Manifest {
    pub format_version: u32,
    pub cluster_id: String,
    pub created_at: String,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub state_hash: String,
    pub key_count: u64,
    pub checksum: String,
}
/// A backup includes the logical snapshot, manifest and integrity checksum.
#[derive(Serialize, Deserialize)]
pub struct Backup {
    pub manifest: Manifest,
    pub snapshot: Vec<u8>,
}
impl Backup {
    /// Encode a portable backup.
    pub fn encode(
        snapshot: Vec<u8>,
        cluster_id: String,
        created_at: String,
        state_hash: String,
        key_count: u64,
    ) -> Result<Vec<u8>, crate::StorageError> {
        let package: SnapshotPackage = bincode::deserialize(&snapshot)
            .map_err(|e| crate::StorageError::Codec(e.to_string()))?;
        let manifest = Manifest {
            format_version: 1,
            cluster_id,
            created_at,
            last_included_index: package.index,
            last_included_term: package.term,
            state_hash,
            key_count,
            checksum: hex::encode(Sha256::digest(&snapshot)),
        };
        let body = bincode::serialize(&Self { manifest, snapshot })
            .map_err(|e| crate::StorageError::Codec(e.to_string()))?;
        let mut bytes = b"RKVBAK01".to_vec();
        bytes.extend_from_slice(&Sha256::digest(&body));
        bytes.extend_from_slice(&body);
        Ok(bytes)
    }
    /// Validate envelope and checkpoint boundaries before any restore writes.
    pub fn decode(bytes: &[u8]) -> Result<(Self, SnapshotPackage), crate::StorageError> {
        use bincode::Options;
        if bytes.len() < 40
            || bytes.len() > 256 * 1024 * 1024
            || &bytes[..8] != b"RKVBAK01"
            || Sha256::digest(&bytes[40..]).as_slice() != &bytes[8..40]
        {
            return Err(crate::StorageError::Invariant(
                "backup envelope checksum mismatch",
            ));
        }
        let backup: Self = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit(256 * 1024 * 1024)
            .reject_trailing_bytes()
            .deserialize(&bytes[40..])
            .map_err(|e| crate::StorageError::Codec(e.to_string()))?;
        if backup.manifest.format_version != 1
            || hex::encode(Sha256::digest(&backup.snapshot)) != backup.manifest.checksum
        {
            return Err(crate::StorageError::Invariant(
                "backup format/checksum mismatch",
            ));
        }
        let package: SnapshotPackage = bincode::deserialize(&backup.snapshot)
            .map_err(|e| crate::StorageError::Codec(e.to_string()))?;
        if package.version != 1
            || package.index != backup.manifest.last_included_index
            || package.term != backup.manifest.last_included_term
        {
            return Err(crate::StorageError::Invariant("backup boundary mismatch"));
        }
        Ok((backup, package))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn checksum_and_boundary() {
        let p = SnapshotPackage {
            version: 1,
            index: 7,
            term: 3,
            config: ClusterConfig::stable(vec![1]),
            state: vec![1, 2],
        };
        let mut b = Backup::encode(
            bincode::serialize(&p).unwrap(),
            "c".into(),
            "t".into(),
            "h".into(),
            1,
        )
        .unwrap();
        assert_eq!(Backup::decode(&b).unwrap().1.index, 7);
        let n = b.len();
        b[n - 1] ^= 1;
        assert!(Backup::decode(&b).is_err());
    }
}
