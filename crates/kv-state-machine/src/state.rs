//! sled-backed KV state machine. Applies commands deterministically.
//!
//! Trees:
//! * `kv`  : `key -> value`
//! * `ttl` : `expire_at_ms_be(8 bytes) || key -> ()`  (sorted index of TTLs)
//! * `meta`: bookkeeping (state hash, applied index)

use std::path::Path;

use parking_lot::Mutex;
use raft_core::log::LogIndex;
use sha2::{Digest, Sha256};
use sled::IVec;
use thiserror::Error;

use crate::command::{Command, Response};

const META_APPLIED: &[u8] = b"applied_index";
const META_HASH: &[u8] = b"state_hash";

/// State machine errors.
#[derive(Debug, Error)]
pub enum StateMachineError {
    /// Sled error.
    #[error("sled error: {0}")]
    Sled(#[from] sled::Error),
    /// Codec error.
    #[error("codec: {0}")]
    Codec(String),
}

/// KV state machine.
pub struct KvStateMachine {
    db: sled::Db,
    kv: sled::Tree,
    ttl: sled::Tree,
    meta: sled::Tree,
    hash: Mutex<[u8; 32]>,
}

fn ttl_key(expire_at_ms: u64, key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(8 + key.len());
    k.extend_from_slice(&expire_at_ms.to_be_bytes());
    k.extend_from_slice(key);
    k
}

impl KvStateMachine {
    /// Open or create a state machine at the given directory.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, StateMachineError> {
        let db = sled::Config::new().path(path).flush_every_ms(Some(200)).open()?;
        let kv = db.open_tree("kv")?;
        let ttl = db.open_tree("ttl")?;
        let meta = db.open_tree("meta")?;
        let hash = match meta.get(META_HASH)? {
            Some(b) if b.len() == 32 => {
                let mut h = [0u8; 32];
                h.copy_from_slice(&b);
                h
            }
            _ => [0u8; 32],
        };
        Ok(Self { db, kv, ttl, meta, hash: Mutex::new(hash) })
    }

    /// Last applied log index.
    pub fn applied_index(&self) -> LogIndex {
        self.meta
            .get(META_APPLIED)
            .ok()
            .flatten()
            .and_then(|b| {
                let mut buf = [0u8; 8];
                if b.len() >= 8 { buf.copy_from_slice(&b[..8]); Some(u64::from_le_bytes(buf)) } else { None }
            })
            .unwrap_or(0)
    }

    /// Snapshot of state hash (32 bytes, hex-encodable).
    pub fn state_hash(&self) -> [u8; 32] { *self.hash.lock() }

    /// Apply a deserialized command. Returns the response.
    pub fn apply(&self, applied_index: LogIndex, cmd: &Command) -> Result<Response, StateMachineError> {
        let resp = match cmd {
            Command::Set { key, value, expire_at_ms } => {
                self.kv.insert(key.as_slice(), value.as_slice())?;
                if let Some(t) = expire_at_ms {
                    self.ttl.insert(ttl_key(*t, key), &[] as &[u8])?;
                }
                self.update_hash(b"S", key, value);
                Response::Ok
            }
            Command::Del { keys } => {
                let mut n = 0i64;
                for k in keys {
                    if self.kv.remove(k.as_slice())?.is_some() { n += 1; }
                    self.update_hash(b"D", k, &[]);
                }
                Response::Int(n)
            }
            Command::Incr { key, delta } => {
                let cur = self.kv.get(key.as_slice())?.and_then(|v: IVec| {
                    std::str::from_utf8(&v).ok().and_then(|s| s.parse::<i64>().ok())
                }).unwrap_or(0);
                let new = cur.saturating_add(*delta);
                let s = new.to_string();
                self.kv.insert(key.as_slice(), s.as_bytes())?;
                self.update_hash(b"I", key, s.as_bytes());
                Response::Int(new)
            }
            Command::Expire { key, expire_at_ms } => {
                if self.kv.contains_key(key.as_slice())? {
                    self.ttl.insert(ttl_key(*expire_at_ms, key), &[] as &[u8])?;
                    self.update_hash(b"E", key, &expire_at_ms.to_le_bytes());
                    Response::Int(1)
                } else {
                    Response::Int(0)
                }
            }
            Command::Persist { key } => {
                let mut removed = 0;
                let prefix = key.as_slice();
                let to_remove: Vec<_> = self.ttl.iter().keys().filter_map(|k| k.ok()).filter(|k| {
                    if k.len() < 8 { return false; }
                    &k[8..] == prefix
                }).collect();
                for k in to_remove { self.ttl.remove(k)?; removed += 1; }
                Response::Int(if removed > 0 { 1 } else { 0 })
            }
            Command::MSet { pairs } => {
                for (k, v) in pairs {
                    self.kv.insert(k.as_slice(), v.as_slice())?;
                    self.update_hash(b"M", k, v);
                }
                Response::Ok
            }
            Command::FlushDb => {
                self.kv.clear()?;
                self.ttl.clear()?;
                *self.hash.lock() = [0u8; 32];
                Response::Ok
            }
            Command::Tick { now_ms } => {
                self.evict_expired(*now_ms)?;
                Response::Ok
            }
        };
        // Bookkeeping (atomic-ish; sled provides WAL but not transactional
        // multi-tree atomicity — for this project we accept that and rely on
        // applied_index being persisted last so a crash mid-apply re-applies).
        self.meta.insert(META_APPLIED, &applied_index.to_le_bytes())?;
        let h = *self.hash.lock();
        self.meta.insert(META_HASH, &h)?;
        Ok(resp)
    }

    /// GET (read-side). Linearizable reads must be gated by read-index in the
    /// runtime layer — this function does not enforce that.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StateMachineError> {
        Ok(self.kv.get(key)?.map(|v| v.to_vec()))
    }

    /// EXISTS.
    pub fn exists(&self, key: &[u8]) -> Result<bool, StateMachineError> {
        Ok(self.kv.contains_key(key)?)
    }

    /// MGET.
    pub fn mget(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>, StateMachineError> {
        keys.iter().map(|k| self.get(k.as_slice())).collect()
    }

    /// DBSIZE.
    pub fn len(&self) -> usize { self.kv.len() }
    /// Whether store is empty.
    pub fn is_empty(&self) -> bool { self.kv.is_empty() }

    /// Force a sled flush (durability).
    pub fn flush(&self) -> Result<(), StateMachineError> {
        self.db.flush()?;
        Ok(())
    }

    fn evict_expired(&self, now_ms: u64) -> Result<(), StateMachineError> {
        // Iterate keys whose timestamp <= now and delete.
        let upper = (now_ms + 1).to_be_bytes().to_vec();
        let to_delete: Vec<_> = self
            .ttl
            .range(..upper.clone())
            .keys()
            .filter_map(|k| k.ok())
            .collect();
        for tk in to_delete {
            if tk.len() >= 8 {
                let key = &tk[8..];
                self.kv.remove(key)?;
                self.update_hash(b"X", key, &[]);
            }
            self.ttl.remove(tk)?;
        }
        Ok(())
    }

    fn update_hash(&self, op: &[u8], key: &[u8], value: &[u8]) {
        let mut h = Sha256::new();
        let cur = self.hash.lock();
        h.update(*cur);
        drop(cur);
        h.update(op);
        h.update((key.len() as u64).to_le_bytes());
        h.update(key);
        h.update((value.len() as u64).to_le_bytes());
        h.update(value);
        let out = h.finalize();
        let mut buf = [0u8; 32];
        buf.copy_from_slice(&out);
        *self.hash.lock() = buf;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn set_get_del() {
        let dir = tempdir().unwrap();
        let sm = KvStateMachine::open(dir.path()).unwrap();
        sm.apply(1, &Command::Set { key: b"a".to_vec(), value: b"1".to_vec(), expire_at_ms: None }).unwrap();
        assert_eq!(sm.get(b"a").unwrap(), Some(b"1".to_vec()));
        sm.apply(2, &Command::Del { keys: vec![b"a".to_vec()] }).unwrap();
        assert_eq!(sm.get(b"a").unwrap(), None);
    }

    #[test]
    fn determinism() {
        // Same command sequence on two state machines yields the same hash.
        let d1 = tempdir().unwrap();
        let d2 = tempdir().unwrap();
        let s1 = KvStateMachine::open(d1.path()).unwrap();
        let s2 = KvStateMachine::open(d2.path()).unwrap();
        let cmds = vec![
            Command::Set { key: b"x".to_vec(), value: b"1".to_vec(), expire_at_ms: None },
            Command::Set { key: b"y".to_vec(), value: b"2".to_vec(), expire_at_ms: None },
            Command::Incr { key: b"x".to_vec(), delta: 5 },
        ];
        for (i, c) in cmds.iter().enumerate() {
            s1.apply(i as u64 + 1, c).unwrap();
            s2.apply(i as u64 + 1, c).unwrap();
        }
        assert_eq!(s1.state_hash(), s2.state_hash());
    }
}
