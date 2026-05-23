# Architecture

## Layering

```
┌──────────────────────────────────────────────┐
│ raftkv-server (binary)                       │
│  ┌────────────────────────────────────────┐  │
│  │ runtime.rs                             │  │
│  │ - drives raft-core                     │  │
│  │ - executes Action enum                 │  │
│  │ - wires net + storage + state machine  │  │
│  └────────────────────────────────────────┘  │
│        ▲              ▲             ▲        │
│        │              │             │        │
│  ┌──────────┐  ┌─────────────┐  ┌──────────┐ │
│  │ raft-net │  │ raft-storage│  │ resp-srv │ │
│  │ (tonic)  │  │ (segmented  │  │ (RESP2/3)│ │
│  │          │  │  log + sled)│  │          │ │
│  └──────────┘  └─────────────┘  └──────────┘ │
│                       ▲                      │
│                       │                      │
│                ┌──────────────┐              │
│                │  raft-core   │   (PURE)     │
│                │ (no I/O,     │              │
│                │  no async,   │              │
│                │  no clock)   │              │
│                └──────────────┘              │
└──────────────────────────────────────────────┘
```

The PURE-IMPURE split is the central design decision. `raft-core` consumes
`Message`s and produces `Action`s. The runtime is responsible for:

- Persisting `HardState` and the log before any RPC reply that depends on
  them (Raft §5.2 durability invariant).
- Sending messages to peers via `raft-net`.
- Applying committed entries to the state machine.
- Driving the tick clock.

This split means `raft-core` is reproducible from a single PRNG seed: feed
the same sequence of `Message`s and `tick()`s on two builds and they emit
the same sequence of `Action`s. That is the foundation of `sim-tests`.

## Core invariants

1. **Election Safety** — at most one leader per term (Raft §5.2). Enforced
   by `voted_for` persistence + the universal term-bump rule in
   `RaftNode::step`.
2. **Log Matching** — if two logs contain an entry with the same
   `(index, term)`, all preceding entries are identical. Enforced by the
   `prev_log_index/prev_log_term` consistency check in
   `handle_append_entries`.
3. **Leader Append-Only** — leaders never overwrite their own log; followers
   truncate on conflict (`truncate_from`).
4. **Leader Completeness** — only entries from current term may be
   committed by counting (Figure 8 caveat). Enforced in
   `maybe_advance_commit`.
5. **State Machine Safety** — apply order is `last_applied + 1 .. commit`.
   Enforced in `flush_apply`.
6. **Joint Consensus** — config changes traverse `C_old -> C_old,new ->
   C_new`; commit requires quorum across BOTH sets while joint
   (`maybe_advance_commit`'s joint branch).

## Linearizable reads

Read-index (Ongaro thesis §6.4): leader records its current commit index,
broadcasts a heartbeat, waits for quorum to confirm leadership, then waits
until `applied_index >= commit_index_at_issuance` before serving the read
from the local state machine. Implemented in `runtime.rs::handle_read` +
`drain_ready_reads`.

## Pre-vote

Enabled by default (`pre_vote = true` in `ServerConfig`). A server about to
start an election first issues `RequestVote { pre_vote: true }`; this does
not bump terms. Only on receiving a quorum of pre-vote grants does the
server advance to a real candidacy. This prevents disruptive elections from
isolated/recovered nodes (Ongaro thesis §9.6).

## Leadership transfer

`RaftNode::transfer_leadership(target)` marks the target, blocks new
proposals, sends `TimeoutNow` once the target's `match_index` equals the
leader's `last_log_index`. The target promptly starts a new election in
the next term and wins (it is already up-to-date).

## Storage

- **Segmented log** (`raft-storage::SegmentedLog`): files of the form
  `<base_index>.log`, each record framed as
  `[u32 length][u32 crc32c][payload]`. Torn writes at the tail are detected
  on open and truncated.
- **Snapshots** (`SnapshotStore`): write-to-temp + atomic `rename` install.
  Old snapshots are pruned with `keep_last(N)`.
- **MetaStore**: sled-backed `(currentTerm, votedFor, commitIndex)` plus
  the snapshot pointer.

## Design Decisions / Deviations from spec

- **sled instead of RocksDB** for the state machine. Both are LSM-based with
  WAL durability; sled avoids the heavy RocksDB toolchain dependency.
  Swapping is a trait-bounded change — see `kv-state-machine/src/lib.rs`.
- **Sync gRPC ack model**: gRPC handlers ack synchronously and the
  semantically real `AppendEntriesResponse` / `RequestVoteResponse` flow
  back over the *outbound* peer client. This simplifies the message plumbing
  at the cost of two extra round-trip hops; for production we'd switch to
  streaming RPCs to fold response bodies into the request RPC.
- **Linearizability checker is in-process** — short traces only. For full
  Jepsen-scale validation, hand histories to Knossos or Porcupine.
