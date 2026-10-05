//! Deterministic, transactionally applied KV data, expiry indexes and metadata.
use crate::command::{Command, Response};
use parking_lot::Mutex;
use raft_core::log::LogIndex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sled::transaction::{ConflictableTransactionError, TransactionError, Transactional};
use std::{collections::BTreeMap, path::Path};
use thiserror::Error;

const APPLIED: &[u8] = b"applied_index";
const HASH: &[u8] = b"state_hash";
const TIME: &[u8] = b"logical_time";

#[derive(Debug, Error)]
pub enum StateMachineError {
    #[error("sled error: {0}")]
    Sled(#[from] sled::Error),
    #[error("codec: {0}")]
    Codec(String),
}

#[derive(Serialize, Deserialize)]
struct Snapshot {
    version: u32,
    applied: u64,
    logical_time: u64,
    hash: [u8; 32],
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    expiries: Vec<(Vec<u8>, Vec<u8>)>,
}

pub struct KvStateMachine {
    db: sled::Db,
    kv: sled::Tree,
    ttl: sled::Tree,
    by_key: sled::Tree,
    meta: sled::Tree,
    // Serializes apply, snapshot and restore; sled transactions provide crash atomicity.
    mutation: Mutex<()>,
}
fn number(bytes: Option<sled::IVec>) -> u64 {
    bytes
        .and_then(|b| b.as_ref().try_into().ok().map(u64::from_le_bytes))
        .unwrap_or(0)
}
fn ttl_key(expiry: u64, key: &[u8]) -> Vec<u8> {
    let mut out = expiry.to_be_bytes().to_vec();
    out.extend_from_slice(key);
    out
}
impl KvStateMachine {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, StateMachineError> {
        let db = sled::Config::new().path(path).open()?;
        let me = Self {
            kv: db.open_tree("kv")?,
            ttl: db.open_tree("ttl")?,
            by_key: db.open_tree("ttl_by_key")?,
            meta: db.open_tree("meta")?,
            db,
            mutation: Mutex::new(()),
        };
        // Upgrade legacy TTL indexes once. Keep only the latest expiry for a key.
        if me.meta.get(b"ttl_schema_v2")?.is_none() {
            let mut expiries = BTreeMap::new();
            for pair in me.ttl.iter() {
                let (key, _) = pair?;
                if key.len() >= 8 && me.kv.contains_key(&key[8..])? {
                    expiries.insert(
                        key[8..].to_vec(),
                        u64::from_be_bytes(key[..8].try_into().unwrap()),
                    );
                }
            }
            me.ttl.clear()?;
            for (key, expiry) in expiries {
                me.by_key.insert(&key, &expiry.to_le_bytes())?;
                me.ttl.insert(ttl_key(expiry, &key), &[])?;
            }
            me.meta.insert(b"ttl_schema_v2", &[1])?;
            me.db.flush()?;
        }
        Ok(me)
    }
    pub fn applied_index(&self) -> LogIndex {
        number(self.meta.get(APPLIED).expect("applied metadata"))
    }
    pub fn logical_time(&self) -> u64 {
        number(self.meta.get(TIME).expect("time metadata"))
    }
    pub fn state_hash(&self) -> [u8; 32] {
        self.meta
            .get(HASH)
            .expect("hash metadata")
            .and_then(|b| b.as_ref().try_into().ok())
            .unwrap_or([0; 32])
    }
    pub fn apply(&self, index: LogIndex, cmd: &Command) -> Result<Response, StateMachineError> {
        self.apply_entry(index, Some(cmd))
    }
    pub fn advance(&self, index: LogIndex) -> Result<Response, StateMachineError> {
        self.apply_entry(index, None)
    }
    fn apply_entry(
        &self,
        index: u64,
        cmd: Option<&Command>,
    ) -> Result<Response, StateMachineError> {
        let _guard = self.mutation.lock();
        if index <= self.applied_index() {
            return Ok(Response::Ok);
        }
        if index != self.applied_index() + 1 {
            return Err(StateMachineError::Codec(
                "non-contiguous application".into(),
            ));
        }
        let encoded_cmd = cmd;
        let (cmd, time) = match cmd {
            Some(Command::AtTime { now_ms, command }) => (Some(command.as_ref()), Some(*now_ms)),
            Some(Command::Tick { now_ms }) => (cmd, Some(*now_ms)),
            _ => (cmd, None),
        };
        let time = time.map(|t| t.max(self.logical_time()));
        let logical_now = time.unwrap_or_else(|| self.logical_time());
        let clear = matches!(cmd, Some(Command::FlushDb));
        let kv_keys = if clear {
            self.kv.iter().keys().collect::<Result<Vec<_>, _>>()?
        } else {
            vec![]
        };
        let expiry_keys = if clear {
            self.by_key.iter().keys().collect::<Result<Vec<_>, _>>()?
        } else {
            vec![]
        };
        let expired = if let Some(now_ms) = time {
            self.ttl
                .iter()
                .take_while(|item| match item {
                    Ok((key, _)) if key.len() >= 8 => {
                        u64::from_be_bytes(key[..8].try_into().unwrap()) <= now_ms
                    }
                    _ => true,
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            vec![]
        };
        let mut hasher = Sha256::new();
        hasher.update(self.state_hash());
        hasher.update(index.to_le_bytes());
        if let Some(c) = encoded_cmd {
            hasher.update(
                bincode::serialize(c).map_err(|e| StateMachineError::Codec(e.to_string()))?,
            );
        }
        let hash: [u8; 32] = hasher.finalize().into();
        let result: Result<Response, TransactionError<String>> = (
            &self.kv,
            &self.ttl,
            &self.by_key,
            &self.meta,
        )
            .transaction(|(kv, ttl, by_key, meta)| {
                let remove_expiry =
                    |key: &[u8]| -> Result<bool, ConflictableTransactionError<String>> {
                        if let Some(old) = by_key.remove(key)? {
                            ttl.remove(ttl_key(number(Some(old)), key))?;
                            Ok(true)
                        } else {
                            Ok(false)
                        }
                    };
                let set_expiry =
                    |key: &[u8], expiry: u64| -> Result<(), ConflictableTransactionError<String>> {
                        remove_expiry(key)?;
                        by_key.insert(key, &expiry.to_le_bytes())?;
                        ttl.insert(ttl_key(expiry, key), &[])?;
                        Ok(())
                    };
                if let Some(time) = time {
                    meta.insert(TIME, &time.to_le_bytes())?;
                    for (key, _) in &expired {
                        if key.len() < 8 {
                            return Err(ConflictableTransactionError::Abort(
                                "corrupt TTL index".into(),
                            ));
                        }
                        let expiry = u64::from_be_bytes(key[..8].try_into().unwrap());
                        if by_key.get(&key[8..])?.map(|b| number(Some(b))) == Some(expiry) {
                            kv.remove(&key[8..])?;
                            by_key.remove(&key[8..])?;
                        }
                        ttl.remove(key.as_ref())?;
                    }
                }
                let response = match cmd {
                    None => Response::Ok,
                    Some(Command::Set {
                        key,
                        value,
                        expire_at_ms,
                    }) => {
                        remove_expiry(key)?;
                        kv.insert(key.as_slice(), value.as_slice())?;
                        if let Some(expiry) = expire_at_ms {
                            set_expiry(key, *expiry)?;
                        }
                        Response::Ok
                    }
                    Some(Command::Del { keys }) => {
                        let mut count = 0;
                        for key in keys {
                            if kv.remove(key.as_slice())?.is_some() {
                                count += 1;
                            }
                            remove_expiry(key)?;
                        }
                        Response::Int(count)
                    }
                    Some(Command::Incr { key, delta }) => {
                        let current = kv.get(key.as_slice())?;
                        let value = current
                            .as_ref()
                            .map(|v| {
                                std::str::from_utf8(v)
                                    .ok()
                                    .and_then(|s| s.parse::<i64>().ok())
                            })
                            .unwrap_or(Some(0));
                        match value.and_then(|n| n.checked_add(*delta)) {
                            Some(n) => {
                                kv.insert(key.as_slice(), n.to_string().as_bytes())?;
                                Response::Int(n)
                            }
                            None => Response::Error(
                                "ERR value is not an integer or increment would overflow".into(),
                            ),
                        }
                    }
                    Some(Command::Expire { key, expire_at_ms }) => {
                        if kv.get(key.as_slice())?.is_some() {
                            if *expire_at_ms <= logical_now {
                                kv.remove(key.as_slice())?;
                                remove_expiry(key)?;
                            } else {
                                set_expiry(key, *expire_at_ms)?;
                            }
                            Response::Int(1)
                        } else {
                            Response::Int(0)
                        }
                    }
                    Some(Command::Persist { key }) => Response::Int(i64::from(remove_expiry(key)?)),
                    Some(Command::MSet { pairs }) => {
                        for (key, value) in pairs {
                            remove_expiry(key)?;
                            kv.insert(key.as_slice(), value.as_slice())?;
                        }
                        Response::Ok
                    }
                    Some(Command::FlushDb) => {
                        for key in &kv_keys {
                            kv.remove(key.as_ref())?;
                        }
                        for key in &expiry_keys {
                            remove_expiry(key)?;
                        }
                        Response::Ok
                    }
                    Some(Command::Tick { .. }) => Response::Ok,
                    Some(Command::AtTime { .. }) => {
                        return Err(ConflictableTransactionError::Abort(
                            "nested timestamp command".into(),
                        ))
                    }
                };
                meta.insert(APPLIED, &index.to_le_bytes())?;
                meta.insert(HASH, &hash)?;
                Ok(response)
            });
        let response = result.map_err(|e| StateMachineError::Codec(e.to_string()))?;
        self.db.flush()?;
        Ok(response)
    }
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StateMachineError> {
        Ok(self.kv.get(key)?.map(|v| v.to_vec()))
    }
    pub fn exists(&self, key: &[u8]) -> Result<bool, StateMachineError> {
        Ok(self.kv.contains_key(key)?)
    }
    pub fn mget(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>, StateMachineError> {
        keys.iter().map(|k| self.get(k)).collect()
    }
    pub fn keys(&self) -> Result<Vec<Vec<u8>>, StateMachineError> {
        self.kv
            .iter()
            .keys()
            .map(|k| k.map(|v| v.to_vec()).map_err(Into::into))
            .collect()
    }
    /// Cursor is a hex encoding of the last examined binary key. Work is bounded by COUNT.
    pub fn scan(
        &self,
        cursor: &str,
        count: usize,
        pattern: &str,
    ) -> Result<(String, Vec<Vec<u8>>), StateMachineError> {
        let previous = if cursor == "0" || cursor.is_empty() {
            None
        } else {
            Some(hex::decode(cursor).map_err(|e| StateMachineError::Codec(e.to_string()))?)
        };
        let matcher =
            glob::Pattern::new(pattern).map_err(|e| StateMachineError::Codec(e.to_string()))?;
        let mut iter = match previous {
            Some(key) => self
                .kv
                .range((std::ops::Bound::Excluded(key), std::ops::Bound::Unbounded)),
            None => self.kv.iter(),
        };
        let mut keys = vec![];
        let mut last = None;
        for item in iter.by_ref().take(count.clamp(1, 1000)) {
            let (key, _) = item?;
            if pattern == "*" || std::str::from_utf8(&key).is_ok_and(|s| matcher.matches(s)) {
                keys.push(key.to_vec());
            }
            last = Some(key);
        }
        let cursor = if iter.next().transpose()?.is_some() {
            last.map(|k| hex::encode(&k)).unwrap_or_else(|| "0".into())
        } else {
            "0".into()
        };
        Ok((cursor, keys))
    }
    pub fn ttl_ms(&self, key: &[u8]) -> Result<i64, StateMachineError> {
        if !self.kv.contains_key(key)? {
            return Ok(-2);
        }
        Ok(match self.by_key.get(key)? {
            None => -1,
            Some(b) => number(Some(b))
                .saturating_sub(self.logical_time())
                .min(i64::MAX as u64) as i64,
        })
    }
    pub fn expiring_len(&self) -> usize {
        self.by_key.len()
    }
    pub fn len(&self) -> usize {
        self.kv.len()
    }
    pub fn is_empty(&self) -> bool {
        self.kv.is_empty()
    }
    pub fn flush(&self) -> Result<(), StateMachineError> {
        self.db.flush()?;
        Ok(())
    }
    pub fn snapshot(&self) -> Result<Vec<u8>, StateMachineError> {
        let _guard = self.mutation.lock();
        let collect = |tree: &sled::Tree| {
            tree.iter()
                .map(|r| r.map(|(k, v)| (k.to_vec(), v.to_vec())))
                .collect::<Result<Vec<_>, _>>()
        };
        let snapshot = Snapshot {
            version: 1,
            applied: self.applied_index(),
            logical_time: self.logical_time(),
            hash: self.state_hash(),
            entries: collect(&self.kv)?,
            expiries: collect(&self.by_key)?,
        };
        let data =
            bincode::serialize(&snapshot).map_err(|e| StateMachineError::Codec(e.to_string()))?;
        let mut result = Sha256::digest(&data).to_vec();
        result.extend(data);
        Ok(result)
    }
    pub fn validate_snapshot(data: &[u8], expected_index: u64) -> Result<(), StateMachineError> {
        if data.len() < 32 || Sha256::digest(&data[32..]).as_slice() != &data[..32] {
            return Err(StateMachineError::Codec(
                "snapshot checksum mismatch".into(),
            ));
        }
        use bincode::Options;
        let snapshot: Snapshot = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit(256 * 1024 * 1024)
            .reject_trailing_bytes()
            .deserialize(&data[32..])
            .map_err(|e| StateMachineError::Codec(e.to_string()))?;
        if snapshot.version != 1 || snapshot.applied != expected_index {
            return Err(StateMachineError::Codec(
                "snapshot version/index mismatch".into(),
            ));
        }
        let keys: std::collections::BTreeSet<_> = snapshot.entries.iter().map(|(k, _)| k).collect();
        let expiries: std::collections::BTreeSet<_> =
            snapshot.expiries.iter().map(|(k, _)| k).collect();
        if expiries.len() != snapshot.expiries.len()
            || keys.len() != snapshot.entries.len()
            || snapshot
                .expiries
                .iter()
                .any(|(key, expiry)| expiry.len() != 8 || !keys.contains(key))
        {
            return Err(StateMachineError::Codec(
                "invalid snapshot keys/expiries".into(),
            ));
        }
        Ok(())
    }
    pub fn restore(&self, data: &[u8], expected_index: u64) -> Result<(), StateMachineError> {
        Self::validate_snapshot(data, expected_index)?;
        use bincode::Options;
        let snapshot: Snapshot = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit(256 * 1024 * 1024)
            .reject_trailing_bytes()
            .deserialize(&data[32..])
            .map_err(|e| StateMachineError::Codec(e.to_string()))?;
        let _guard = self.mutation.lock();
        let keys = |tree: &sled::Tree| tree.iter().keys().collect::<Result<Vec<_>, _>>();
        let (old_kv, old_ttl, old_expiry) =
            (keys(&self.kv)?, keys(&self.ttl)?, keys(&self.by_key)?);
        let result: Result<(), TransactionError<String>> =
            (&self.kv, &self.ttl, &self.by_key, &self.meta).transaction(
                |(kv, ttl, by_key, meta)| {
                    for key in &old_kv {
                        kv.remove(key.as_ref())?;
                    }
                    for key in &old_ttl {
                        ttl.remove(key.as_ref())?;
                    }
                    for key in &old_expiry {
                        by_key.remove(key.as_ref())?;
                    }
                    for (key, value) in &snapshot.entries {
                        kv.insert(key.as_slice(), value.as_slice())?;
                    }
                    for (key, expiry) in &snapshot.expiries {
                        if expiry.len() != 8 {
                            return Err(ConflictableTransactionError::Abort(
                                "invalid snapshot expiry".into(),
                            ));
                        }
                        by_key.insert(key.as_slice(), expiry.as_slice())?;
                        ttl.insert(
                            ttl_key(
                                u64::from_le_bytes(expiry.as_slice().try_into().unwrap()),
                                key,
                            ),
                            &[],
                        )?;
                    }
                    meta.insert(APPLIED, &snapshot.applied.to_le_bytes())?;
                    meta.insert(TIME, &snapshot.logical_time.to_le_bytes())?;
                    meta.insert(HASH, &snapshot.hash)?;
                    Ok(())
                },
            );
        result.map_err(|e| StateMachineError::Codec(e.to_string()))?;
        self.db.flush()?;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    fn set(value: &str, expiry: Option<u64>) -> Command {
        Command::Set {
            key: b"k".to_vec(),
            value: value.as_bytes().to_vec(),
            expire_at_ms: expiry,
        }
    }
    #[test]
    fn replay_restart_and_atomic_indices() {
        let dir = tempdir().unwrap();
        {
            let sm = KvStateMachine::open(dir.path()).unwrap();
            sm.apply(1, &set("1", None)).unwrap();
            sm.apply(
                2,
                &Command::Incr {
                    key: b"k".to_vec(),
                    delta: 1,
                },
            )
            .unwrap();
        }
        let sm = KvStateMachine::open(dir.path()).unwrap();
        sm.apply(
            2,
            &Command::Incr {
                key: b"k".to_vec(),
                delta: 1,
            },
        )
        .unwrap();
        assert_eq!(sm.get(b"k").unwrap(), Some(b"2".to_vec()));
        assert!(sm.advance(4).is_err());
        sm.advance(3).unwrap();
    }
    #[test]
    fn expiry_replacement_persist_and_overwrite() {
        let dir = tempdir().unwrap();
        let sm = KvStateMachine::open(dir.path()).unwrap();
        sm.apply(1, &set("v", Some(10))).unwrap();
        sm.apply(
            2,
            &Command::Expire {
                key: b"k".to_vec(),
                expire_at_ms: 20,
            },
        )
        .unwrap();
        sm.apply(3, &Command::Tick { now_ms: 11 }).unwrap();
        assert!(sm.exists(b"k").unwrap());
        assert_eq!(sm.ttl_ms(b"k").unwrap(), 9);
        sm.apply(4, &set("new", None)).unwrap();
        sm.apply(5, &Command::Tick { now_ms: 25 }).unwrap();
        assert_eq!(sm.ttl_ms(b"k").unwrap(), -1);
        sm.apply(
            6,
            &Command::Expire {
                key: b"k".to_vec(),
                expire_at_ms: 30,
            },
        )
        .unwrap();
        assert!(matches!(
            sm.apply(7, &Command::Persist { key: b"k".to_vec() })
                .unwrap(),
            Response::Int(1)
        ));
        sm.apply(8, &Command::Tick { now_ms: 40 }).unwrap();
        assert!(sm.exists(b"k").unwrap());
    }
    #[test]
    fn snapshot_roundtrip_integrity_and_scan() {
        let d1 = tempdir().unwrap();
        let d2 = tempdir().unwrap();
        let a = KvStateMachine::open(d1.path()).unwrap();
        let b = KvStateMachine::open(d2.path()).unwrap();
        a.apply(1, &set("v", Some(100))).unwrap();
        a.apply(2, &Command::Tick { now_ms: 10 }).unwrap();
        let mut data = a.snapshot().unwrap();
        b.restore(&data, 2).unwrap();
        assert_eq!(a.state_hash(), b.state_hash());
        assert_eq!(b.ttl_ms(b"k").unwrap(), 90);
        data[33] ^= 1;
        assert!(b.restore(&data, 2).is_err());
        assert_eq!(b.scan("0", 1, "*").unwrap().1, vec![b"k".to_vec()]);
    }
    #[test]
    fn timed_mutations_expire_before_apply_and_immediate_expiry_deletes() {
        let dir = tempdir().unwrap();
        let sm = KvStateMachine::open(dir.path()).unwrap();
        sm.apply(
            1,
            &Command::AtTime {
                now_ms: 10,
                command: Box::new(set("old", Some(11))),
            },
        )
        .unwrap();
        sm.apply(
            2,
            &Command::AtTime {
                now_ms: 12,
                command: Box::new(Command::Incr {
                    key: b"k".to_vec(),
                    delta: 1,
                }),
            },
        )
        .unwrap();
        assert_eq!(sm.get(b"k").unwrap(), Some(b"1".to_vec()));
        sm.apply(
            3,
            &Command::Expire {
                key: b"k".to_vec(),
                expire_at_ms: 0,
            },
        )
        .unwrap();
        assert!(!sm.exists(b"k").unwrap());
        assert_eq!(sm.ttl_ms(b"k").unwrap(), -2);
        sm.apply(
            4,
            &Command::AtTime {
                now_ms: 9,
                command: Box::new(set("bad", None)),
            },
        )
        .unwrap();
        assert_eq!(sm.logical_time(), 12);
    }
}
