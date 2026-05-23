# raftkv

A Raft-replicated key-value store, written in Rust, with a Redis-compatible
(RESP2/RESP3) wire protocol and gRPC peer-to-peer transport.

The consensus core is a **pure state machine** (no I/O, no async, no clock,
no OS RNG): all side-effects are emitted as `Action` values and interpreted
by an outer runtime. This separation is what makes the consensus layer
deterministically testable — see [ARCHITECTURE.md](ARCHITECTURE.md) for the
shape, and [crates/sim-tests](crates/sim-tests) for the deterministic
simulator.

## Status

This is a from-scratch educational implementation. It compiles cleanly with
the stable Rust toolchain, has a passing unit-test suite for the consensus
core, the durable log, the RESP codec, and a deterministic simulation
harness, and ships with a working multi-node cluster runner.

It is **not** a drop-in production substitute for etcd / Consul / Redis —
deeper validation (Jepsen, fuzzing, long-running soak tests) and many
operational features (TLS auth, ACLs, online migration, replication
backpressure metrics) are scaffolded but not yet finished. See
[CHANGELOG.md](CHANGELOG.md).

## Repo layout

```
raftkv/
├── crates/
│   ├── raft-core/                 pure consensus state machine (no I/O)
│   ├── raft-storage/              segmented log + sled-backed meta + snapshots
│   ├── raft-net/                  tonic gRPC peer transport
│   ├── kv-state-machine/          deterministic sled-backed KV
│   ├── resp-server/               RESP2/RESP3 client server
│   ├── raftkv-server/             binary wiring everything together
│   ├── raftkv-cli/                operator/smoke-test CLI
│   ├── linearizability-checker/   Wing-and-Gong style checker
│   └── sim-tests/                 deterministic simulator (no async)
├── proto/                         gRPC service definitions
├── deploy/                        sample TOML configs
├── helm/raftkv/                   Helm chart (StatefulSet + headless svc)
├── grafana/                       Dashboard + alert rules
└── scripts/                       run_cluster.sh, jepsen_run.sh, etc.
```

## Quick start (3-node cluster on localhost)

```bash
./scripts/run_cluster.sh             # builds release binary, starts 3 nodes
redis-cli -p 6379 PING               # talk to node 1
redis-cli -p 6379 SET hello world
redis-cli -p 6379 GET hello
redis-cli -p 6379 INFO raft          # role / term / commit-index / leader-id
./scripts/stop_cluster.sh
```

## Quick start (Docker Compose)

```bash
docker compose up --build
redis-cli -p 6379 INFO raft
```

## Build / test

```bash
cargo build --workspace --release
cargo test  --workspace
```

CI runs: `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test`,
`cargo build --release`. See [.github/workflows/ci.yml](.github/workflows/ci.yml).

## Supported commands

`PING`, `ECHO`, `QUIT`, `SELECT`, `COMMAND`, `CLIENT NO-EVICT`,
`INFO [section]`, `CLUSTER NODES`, `CLUSTER INFO`, `DBSIZE`,
`GET`, `MGET`, `EXISTS`, `SET key value [EX seconds]`, `DEL`, `INCR`,
`DECR`, `MSET`, `EXPIRE`, `PERSIST`, `FLUSHDB`.

Reads are linearizable via the read-index protocol (Ongaro thesis §6.4).
`TTL` currently returns `-1` (TTL surface is implemented; surfacing the
remaining seconds through this command is on the backlog).

## Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md) — system structure + design decisions.
- [RAFT_NOTES.md](RAFT_NOTES.md) — protocol invariants and where in the code
  they're enforced.
- [CHANGELOG.md](CHANGELOG.md) — what's done and what's next.
