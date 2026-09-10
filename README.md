# raftkv

**A Raft-replicated key-value store in Rust with a Redis-compatible client protocol.**

`raftkv` is an educational distributed-systems implementation built around a deterministic Raft state machine. Nodes communicate over gRPC, persist a replicated log and snapshots, apply committed commands to a sled-backed key-value state machine, and expose the client surface through RESP2/RESP3—the wire protocol used by Redis-compatible clients.

> **Status:** early-stage research/learning implementation (`0.1.0`, unreleased). It is useful for studying consensus, deterministic simulation, storage recovery, and protocol integration. It is not a drop-in production replacement for Redis, etcd, or Consul.

## Why this project is interesting

The central design choice is a strict pure/impure boundary:

```text
messages + ticks ──> raft-core ──> Action values
                                      │
             runtime executes I/O ────┘
                 storage · gRPC · RESP · metrics
```

`raft-core` performs no I/O, asynchronous work, wall-clock reads, or operating-system randomness. It consumes protocol messages and timer ticks and emits explicit `Action` values. The outer runtime interprets those actions. This makes the consensus logic deterministic, replayable, and testable without a live cluster.

## Implemented capabilities

- Raft leader election, replication, conflict repair, quorum commit, and membership joint consensus.
- Pre-vote and leadership transfer support.
- Linearizable reads through the read-index protocol.
- Segmented append-only storage with CRC32C framing and torn-tail recovery.
- Atomic snapshot installation and snapshot retention.
- Sled-backed metadata and key-value state machine.
- RESP2/RESP3 command codec and Redis-compatible TCP client endpoint.
- Tonic gRPC peer transport for Raft messages.
- Deterministic cluster simulator and an in-process linearizability checker.
- Prometheus text metrics endpoint, Grafana dashboard, Docker Compose setup, and Helm chart.

The protocol and implementation trade-offs are documented in [ARCHITECTURE.md](ARCHITECTURE.md), with a source-level Raft invariant map in [RAFT_NOTES.md](RAFT_NOTES.md).

## Workspace structure

| Crate / directory | Responsibility |
| --- | --- |
| `crates/raft-core` | Pure Raft state machine and protocol transitions |
| `crates/raft-storage` | Segmented log, metadata, snapshots, recovery |
| `crates/raft-net` | Tonic gRPC peer transport and protobuf bindings |
| `crates/kv-state-machine` | Deterministic sled-backed key-value state machine |
| `crates/resp-server` | RESP2/RESP3 parsing, encoding, and command dispatch |
| `crates/raftkv-server` | Runtime wiring consensus, storage, network, clients, metrics |
| `crates/raftkv-cli` | Small operator and smoke-test client |
| `crates/linearizability-checker` | Wing–Gong-style checker for short histories |
| `crates/sim-tests` | Deterministic, seed-replayable cluster simulation |
| `proto/` | Raft and administration gRPC definitions |
| `deploy/`, `helm/` | TOML examples and Kubernetes deployment assets |
| `grafana/` | Dashboard and alerting examples |

## Quick start: three-node local cluster

### Prerequisites

- Rust stable (the workspace declares Rust 1.75 or newer).
- macOS or Linux; Windows is supported through WSL.
- Optional: `redis-cli`, Docker, Docker Compose, `kubectl`, and Helm.

The build vendors `protoc`, so a system protobuf installation is not required.

```bash
git clone https://github.com/jeevanu345/raftkv.git
cd raftkv
cargo build --workspace --release
./scripts/run_cluster.sh
```

The helper starts three local nodes:

| Node | Raft gRPC | RESP client | Metrics |
| --- | ---: | ---: | ---: |
| 1 | `7001` | `6379` | `9101` |
| 2 | `7002` | `6380` | `9102` |
| 3 | `7003` | `6381` | `9103` |

After an election, find the leader and exercise the cluster:

```bash
./target/release/raftkv-cli --addr 127.0.0.1:6379 status
./target/release/raftkv-cli --addr 127.0.0.1:6379 members
./target/release/raftkv-cli --addr 127.0.0.1:6379 set greeting "hello raft"
./target/release/raftkv-cli --addr 127.0.0.1:6379 get greeting

# Or use any Redis-compatible client against the leader:
redis-cli -p 6379 PING
redis-cli -p 6379 SET greeting "hello raft"
redis-cli -p 6379 GET greeting
redis-cli -p 6379 INFO raft
```

Writes sent to a follower currently return a `MOVED` response; use the leader directly. Stop the local cluster with:

```bash
./scripts/stop_cluster.sh
```

Logs and local data are written beneath `.run/`.

## Other deployment paths

### Docker Compose

```bash
docker compose up --build
```

### Kubernetes / Helm

```bash
helm install raftkv ./helm/raftkv
```

The chart provides a three-replica StatefulSet, peer discovery service, client service, and persistent volume configuration. Review `helm/raftkv/values.yaml` before using it outside a local cluster.

### Manual configuration

`raftkv-server` accepts a TOML configuration with node identity, Raft and client bind addresses, peers, data directory, election/heartbeat timing, snapshot threshold, and pre-vote settings. See `deploy/node1.toml`, `deploy/node2.toml`, `deploy/node3.toml`, and [HOW_TO_RUN.md](HOW_TO_RUN.md).

## Testing and reproducibility

```bash
cargo fmt --check
cargo test --workspace
cargo test -p sim-tests
cargo test -p linearizability-checker
cargo clippy --workspace --all-targets -- -D warnings
```

The simulator is designed to replay a run from the same seed. The separation between `raft-core` and the runtime also enables focused unit tests for election, log matching, quorum commit, joint consensus, snapshots, and read-index behavior.

## Supported client commands

The current command surface includes `PING`, `ECHO`, `QUIT`, `SELECT`, `COMMAND`, `INFO`, `CLUSTER NODES`, `CLUSTER INFO`, `DBSIZE`, `GET`, `MGET`, `EXISTS`, `SET`, `DEL`, `INCR`, `DECR`, `MSET`, `EXPIRE`, `PERSIST`, and `FLUSHDB`.

The backing state machine has TTL support; the `TTL` command still reports `-1` and is listed on the project backlog.

## Observability

Each node exposes Prometheus text metrics on its configured metrics port:

```bash
curl http://127.0.0.1:9101/metrics
```

Import `grafana/dashboards/raftkv.json` for a reference dashboard and review `grafana/alerting/alerts.yml` for example alerts.

## Current limitations and roadmap

The project intentionally documents its unfinished edges:

- TLS/mTLS, ACLs, and client authentication are not complete.
- Snapshot transfer currently passes an opaque payload rather than streaming chunks.
- Membership-change plumbing is not yet exposed as a complete gRPC administration workflow.
- Jepsen-scale fault injection, fuzzing, long-running soak tests, and benchmark suites remain future work.
- Backup/restore tooling and automatic follower redirect handling are not complete.

See [CHANGELOG.md](CHANGELOG.md) for the maintained implementation checklist.

## Contributing

Small, focused pull requests are welcome. For consensus changes, include the invariant being protected, a deterministic test or simulator scenario where possible, and the failure mode the test covers. Run formatting, tests, and Clippy before opening a pull request.

## License

Apache-2.0. See the workspace metadata and repository history for details.
