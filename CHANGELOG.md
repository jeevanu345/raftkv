# Changelog

## 0.1.0 — operator console and consensus hardening (unreleased)

### Operator experience and local scale

- Expanded React console into Cluster overview, Nodes & replication, Keyspace explorer, Telemetry & events, Architecture report, and Connections & runbook panes.
- Added independent node polling, connection persistence, selection, pause/manual refresh, light/dark appearance, bounded charts/event history and downloadable JSON diagnostics.
- Exposed membership/quorum, timing configuration, endpoints, retained/snapshot log boundaries, backlog, runtime counters, bounded request queues and leader-only per-peer progress.
- Distinguished HTTP reachability from Raft availability, local inspection from quorum-confirmed reads, and browser endpoint registration from real voter membership.
- Added configurable 1–31-voter local bootstrap, symmetric peer generation, occupied-port/readiness checks, persistent membership-count guards and PID-scoped shutdown.
- Replaced project documentation with implementation-backed architecture, source/invariant notes, runbook and technical report.

### Correctness and efficiency

- Fixed majority replication selection for even stable/joint voter sets and missing progress records.
- Limited follower success acknowledgments and commit updates to the prefix actually matched by the AppendEntries request.
- Correlated fresh read-index quorum confirmations, required a current-term commit barrier, retried pending probes and rejected stale leadership/term contexts.
- Serialized core transitions and side effects; capped request channels/waiter maps at 1,024 and pruned canceled waiters.
- Bounded transport concurrency to 32 sends per peer and reclaimed stale core in-flight counters on heartbeat retry after outages.
- Advanced application bookkeeping for no-op/config entries and skipped already-applied entries during clean restart replay.
- Suppressed idle AppendEntries response feedback loops.
- Fixed exhaustive checker branch rollback, including mutating inconsistent candidates.
- Validated complete contiguous append batches before writing, synced every touched segment on rotation, and reduced range reads to one lock/initial lookup.
- Bounded RESP frames, arrays and nesting; rejected invalid lengths and bulk terminators with fragmented/pipelined regression coverage.
- Added strict bounded HTTP parsing, fragmented-request support, JSON mutation validation and UTF-8 percent-decoded keys; removed guessed-port write forwarding.
- Added runtime storage health that stops consensus processing/acknowledgments and KV access after a persistence/application failure, with repair/restart required.
- Added populated node-labeled Prometheus gauges/counters and bounded local log-entry metadata inspection.

### Deployment and validation

- Fixed Helm pod-ordinal identity, parallel bootstrap, not-ready peer DNS and ephemeral data volume when persistence is disabled.
- Added launcher tests, real four/five-node integration tests, broader seeded simulator scenarios, core/storage/runtime/checker regressions and UI model tests.
- Added CI definitions for Rust formatting/Clippy/tests, launcher and real cluster integration, and UI tests/build. Hosted CI and live Kubernetes validation are separate from local checks.
- Real four/five-node scenarios verified election, 12 KiB JSON/Unicode-reserved-key replication, quorum reads, bounded idle RPC activity, leader failover, isolated-leader read timeout, clean restart INCR preservation, replicated deletion and matching hashes without storage errors.

### Existing building blocks

- Pure deterministic consensus core with pre-vote, joint configuration and leadership-transfer primitives.
- CRC32C-framed segmented log, flushed sled metadata, snapshot file writer/rename/retention primitives.
- Sled KV store, sorted expiry index and rolling operation-history digest.
- RESP2-compatible subset, tonic peer transport and smoke-test CLI.
- In-memory seeded simulator and exhaustive short-history register checker.
- Docker Compose, Helm and Grafana example assets.

### Remaining integration work

- Transactional multi-tree KV application and robust power-loss/partial-apply crash recovery; clean restart replay checks do not establish exactly-once command semantics.
- Complete snapshot KV/config serialization, streaming transfer, install, automatic scheduling and verified recovery/backup tooling.
- Bound live membership/admin services, learner catch-up, address registration, serialized transitions and configuration recovery.
- Complete TTL/expiry replacement semantics and periodic replicated expiry ticks; `TTL` still returns `-1`.
- Full Redis/RESP3 semantics, transactions, pub/sub and automatic CLI leader discovery/redirect handling.
- TLS/mTLS, authentication, ACLs, rate limits and remote operator routing.
- Reproducible throughput/tail-latency benchmarks, long soak runs, fuzzing and a bundled Jepsen/crash nemesis suite. The Jepsen script skips when no external harness is configured.
