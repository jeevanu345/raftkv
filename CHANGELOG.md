# Changelog

## 0.1.0 — initial scaffolding (unreleased)

### Working

- `raft-core`: pure consensus state machine with Election Safety, Log
  Matching, Leader Append-Only, Leader Completeness, joint consensus
  membership changes, pre-vote, leadership transfer, read-index, snapshot
  install. Unit tests cover single-node election, three-node election,
  conflict-driven follower log truncation, log slicing, joint quorum.
- `raft-storage`: segmented log with CRC32C, torn-write recovery, segment
  rotation, compaction; sled-backed `MetaStore` (`HardState` +
  snapshot pointer); atomic snapshot install (write-to-temp + rename).
- `raft-net`: tonic gRPC server + outbound peer client for AppendEntries,
  RequestVote, PreVote, InstallSnapshot, TimeoutNow.
- `kv-state-machine`: sled-backed deterministic KV with `Set`, `Get`, `Del`,
  `Incr`, `MSet`, `MGet`, `Exists`, `Expire`, `Persist`, `FlushDb`, `Tick`,
  rolling state hash for divergence detection.
- `resp-server`: RESP2/3 codec (typed + inline), command parser, async
  dispatch.
- `raftkv-server`: runtime that drives the core, plus TCP/gRPC/metrics
  listeners.
- `raftkv-cli`: smoke-test client over the RESP port.
- `linearizability-checker`: exhaustive Wing-and-Gong checker.
- `sim-tests`: deterministic 100% reproducible cluster simulator.
- Helm chart, Docker / Compose, Grafana dashboard + alert rules, CI workflow.

### Not yet done

- Online TLS / mTLS, ACLs, client auth.
- gRPC membership service binding (core-side membership changes work).
- `TTL` command (returns `-1`; backing TTL store is implemented).
- Streaming `InstallSnapshot` over real chunks (currently passes opaque
  `Bytes`).
- Long-running soak suite, Jepsen suite, fault-injection nemeses.
- Fuzz targets and criterion benches.
- Backup / restore CLI flow.
