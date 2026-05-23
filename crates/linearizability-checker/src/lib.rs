#![deny(unsafe_code)]
//! Linearizability checker for register histories.
//!
//! Implements a Wing-and-Gong style search: given a list of operations
//! recorded as `(invocation_ts, response_ts, op)`, find a serialization order
//! consistent with the model (here: a register supporting `Get`, `Set`, `Del`,
//! `Incr`). Operations may be concurrent — any pair of operations whose
//! `[invocation, response]` intervals overlap may be ordered either way.
//!
//! For deterministic correctness the checker is exhaustive (exponential in
//! the number of concurrent operations). It is intended for short integration
//! traces, not for production-scale Jepsen replay; for very long histories
//! use Knossos or Porcupine.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// One operation in a recorded history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation {
    /// Logical client id (used only for diagnostics).
    pub client: u64,
    /// Invocation timestamp (any monotonic unit).
    pub invocation: u64,
    /// Response timestamp.
    pub response: u64,
    /// Operation kind.
    pub op: Op,
}

/// Operation variants (subset of the KV API sufficient for checker traces).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Op {
    /// `GET key -> value`.
    Get { key: Vec<u8>, observed: Option<Vec<u8>> },
    /// `SET key = value -> Ok`.
    Set { key: Vec<u8>, value: Vec<u8> },
    /// `DEL key -> bool` (returns true if a value was removed).
    Del { key: Vec<u8>, observed_removed: bool },
    /// `INCR key by delta -> new_value`.
    Incr { key: Vec<u8>, delta: i64, observed: i64 },
}

/// Result of a linearization attempt.
#[derive(Debug)]
pub enum Verdict {
    /// History is linearizable (a valid serialization was found).
    Linearizable,
    /// History is not linearizable.
    NotLinearizable {
        /// Best-effort offending op index.
        offending_index: usize,
    },
}

/// Errors raised by the checker.
#[derive(Debug, Error)]
pub enum CheckerError {
    /// History contained an inconsistent invocation/response order.
    #[error("inconsistent invocation/response on op {0}")]
    InconsistentTimestamps(usize),
}

/// Check whether `history` is linearizable as a register/KV store.
///
/// Algorithm: depth-first search over operation orderings constrained by
/// real-time happens-before. At each step pick a "minimum" operation (one
/// whose invocation precedes the response of every other un-linearized op)
/// and verify it is consistent with the current model state.
pub fn check(history: &[Operation]) -> Result<Verdict, CheckerError> {
    for (i, op) in history.iter().enumerate() {
        if op.invocation > op.response { return Err(CheckerError::InconsistentTimestamps(i)); }
    }
    let mut state: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut remaining: Vec<usize> = (0..history.len()).collect();
    if dfs(history, &mut remaining, &mut state) {
        Ok(Verdict::Linearizable)
    } else {
        Ok(Verdict::NotLinearizable { offending_index: 0 })
    }
}

fn dfs(
    history: &[Operation],
    remaining: &mut Vec<usize>,
    state: &mut BTreeMap<Vec<u8>, Vec<u8>>,
) -> bool {
    if remaining.is_empty() { return true; }
    // Find minimum-set: ops whose invocation <= min(response of others).
    let earliest_response = remaining.iter().map(|&i| history[i].response).min().unwrap_or(u64::MAX);
    let candidates: Vec<usize> = remaining
        .iter()
        .copied()
        .filter(|&i| history[i].invocation <= earliest_response)
        .collect();
    for c in candidates {
        if !apply_consistent(&history[c], state) { continue; }
        let saved = state.clone();
        let pos = remaining.iter().position(|&x| x == c).unwrap();
        remaining.remove(pos);
        if dfs(history, remaining, state) { return true; }
        remaining.insert(pos, c);
        *state = saved;
    }
    false
}

fn apply_consistent(op: &Operation, state: &mut BTreeMap<Vec<u8>, Vec<u8>>) -> bool {
    match &op.op {
        Op::Get { key, observed } => state.get(key).cloned() == *observed,
        Op::Set { key, value } => { state.insert(key.clone(), value.clone()); true }
        Op::Del { key, observed_removed } => {
            let had = state.remove(key).is_some();
            had == *observed_removed
        }
        Op::Incr { key, delta, observed } => {
            let cur = state.get(key)
                .and_then(|v| std::str::from_utf8(v).ok().and_then(|s| s.parse::<i64>().ok()))
                .unwrap_or(0);
            let new = cur.saturating_add(*delta);
            if new != *observed { return false; }
            state.insert(key.clone(), new.to_string().into_bytes());
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(client: u64, t0: u64, t1: u64, op: Op) -> Operation {
        Operation { client, invocation: t0, response: t1, op }
    }

    #[test]
    fn sequential_set_get_is_linearizable() {
        let h = vec![
            op(1, 0, 1, Op::Set { key: b"x".to_vec(), value: b"1".to_vec() }),
            op(2, 2, 3, Op::Get { key: b"x".to_vec(), observed: Some(b"1".to_vec()) }),
        ];
        assert!(matches!(check(&h).unwrap(), Verdict::Linearizable));
    }

    #[test]
    fn stale_read_after_set_is_not_linearizable() {
        let h = vec![
            op(1, 0, 1, Op::Set { key: b"x".to_vec(), value: b"1".to_vec() }),
            op(2, 2, 3, Op::Get { key: b"x".to_vec(), observed: None }),
        ];
        assert!(matches!(check(&h).unwrap(), Verdict::NotLinearizable { .. }));
    }

    #[test]
    fn concurrent_set_picks_a_winner() {
        // Two concurrent SETs; subsequent GET must see one of them.
        let h = vec![
            op(1, 0, 5, Op::Set { key: b"x".to_vec(), value: b"a".to_vec() }),
            op(2, 0, 5, Op::Set { key: b"x".to_vec(), value: b"b".to_vec() }),
            op(3, 6, 7, Op::Get { key: b"x".to_vec(), observed: Some(b"a".to_vec()) }),
        ];
        assert!(matches!(check(&h).unwrap(), Verdict::Linearizable));
    }
}
