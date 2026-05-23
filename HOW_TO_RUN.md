# How to run raftkv

This walks through every supported way to bring up the cluster, talk to it,
and tear it down. Pick the section that matches what you want to do.

## 0. Prerequisites

- **Rust** stable (1.74+ recommended). Install via rustup: <https://rustup.rs>.
- **macOS / Linux**. (Windows works under WSL.)
- *Optional*:
  - `redis-cli` for ad-hoc client testing (any Redis 6+ install ships it).
  - Docker / Docker Compose for the containerized cluster.
  - `kubectl` + `helm` for the Kubernetes deployment.

`protoc` is **not** required on the host — `crates/raft-net/build.rs` vendors a
`protoc` binary via the `protoc-bin-vendored` crate, so a clean
`cargo build` works without any system protobuf install.

## 1. Build

From the repo root:

```bash
cd raftkv
cargo build --release
```

Artifacts you will use most:

- `target/release/raftkv-server` — the node binary.
- `target/release/raftkv-cli` — the smoke-test client.

To run the unit-test suite:

```bash
cargo test --workspace
```

## 2. Three-node localhost cluster (recommended for development)

The shell helper sets up three TOML config files under `.run/` and starts
three `raftkv-server` processes in the background.

```bash
./scripts/run_cluster.sh
```

Default ports:

| Node | Raft (gRPC) | Client (RESP) | Metrics (HTTP) |
|------|------------:|--------------:|---------------:|
| 1    | 7001        | **6379**      | 9101           |
| 2    | 7002        | 6380          | 9102           |
| 3    | 7003        | 6381          | 9103           |

Logs land in `.run/node{1,2,3}.log` and data dirs in `.run/data/node{1,2,3}/`.

Wait ~1 second for an election, then check status:

```bash
./target/release/raftkv-cli --addr 127.0.0.1:6379 status
./target/release/raftkv-cli --addr 127.0.0.1:6380 status
./target/release/raftkv-cli --addr 127.0.0.1:6381 status
```

Exactly one node will report `role:leader`; the other two will report
`role:follower` with the same `term` and `leader_id`.

### Smoke tests with raftkv-cli

```bash
LEADER=127.0.0.1:6380   # whichever address showed role:leader

./target/release/raftkv-cli --addr $LEADER ping
./target/release/raftkv-cli --addr $LEADER set hello world
./target/release/raftkv-cli --addr $LEADER get hello
./target/release/raftkv-cli --addr $LEADER del hello
./target/release/raftkv-cli --addr $LEADER members
```

### Smoke tests with redis-cli (optional)

The server speaks RESP2/RESP3, so a stock `redis-cli` works:

```bash
redis-cli -p 6379 PING
redis-cli -p 6379 SET k v
redis-cli -p 6379 GET k
redis-cli -p 6379 INFO raft
```

Writes against a follower currently return a `MOVED 0 ` redirect; talk to
the leader for now (the CLI does not auto-follow redirects).

### Stop the cluster

```bash
./scripts/stop_cluster.sh
```

To wipe state and start fresh:

```bash
./scripts/stop_cluster.sh
rm -rf .run/data
./scripts/run_cluster.sh
```

## 3. Single binary, manual config

If you want to run one node with your own TOML, build and invoke directly:

```bash
cargo build --release -p raftkv-server

cat > my.toml <<'EOF'
id = 1
raft_listen   = "127.0.0.1:7001"
client_listen = "127.0.0.1:6379"
metrics_listen = "127.0.0.1:9101"
data_dir = "./data/node1"
election_timeout_ms = 300
heartbeat_ms = 50
tick_ms = 10
snapshot_entries_threshold = 10000
pre_vote = true
[[peers]]
id = 1
raft_addr = "http://127.0.0.1:7001"
EOF

RUST_LOG=info ./target/release/raftkv-server --config my.toml
```

A single-node config is self-quorum and elects itself immediately, which is
useful for smoke-testing the RESP path without the cluster.

## 4. Docker Compose

```bash
docker compose up --build
```

This builds the image from `Dockerfile` and starts a 3-node cluster on the
host network with the same port layout as section 2.

## 5. Kubernetes via Helm

```bash
helm install raftkv ./helm/raftkv
```

The chart deploys a 3-replica StatefulSet plus a headless service for
peer-to-peer gRPC and a regular service for client RESP traffic. See
`helm/raftkv/values.yaml` for tunables (replica count, image, persistent
volume size, election parameters).

## 6. Observability

Each node exposes Prometheus metrics on its `metrics_listen` port over plain
HTTP:

```bash
curl http://127.0.0.1:9101/metrics
```

A reference Grafana dashboard lives at `grafana/dashboards/raftkv.json` and
alert rules at `grafana/alerting/alerts.yml`.

## 7. Running tests

- `cargo test --workspace` — unit tests for every crate (consensus, storage,
  state machine, RESP codec, linearizability checker, simulator).
- `cargo test -p sim-tests` — deterministic cluster simulator. Reproducible
  with a seed; failing replays the same failure on every run.
- `cargo test -p linearizability-checker` — exhaustive Wing-and-Gong checker
  on small histories.

## 8. Troubleshooting

- **All three nodes loop in `election started term=N pre_vote=true` with
  no leader.** Check that each node's `[[peers]]` block lists every other
  node and that the `id` values are unique. Symmetric peer lists are
  required — a node will not connect to a peer it has not been told about.
- **Leader elected but writes hang.** Check the leader's log for
  `tonic`/`hyper` connection errors; commonly a peer's `raft_addr` URL has
  the wrong port or scheme. The scheme must be `http://` (not `grpc://`).
- **Client gets `MOVED 0 `.** The node you connected to is a follower. Use
  `cluster_nodes` (`MEMBERS` in the CLI) to find the leader, or write
  through the leader directly. Auto-redirect is on the punch list.
- **`cargo build` complains about `protoc`.** It shouldn't — the build
  vendors `protoc`. If you've manually overridden `PROTOC` in your
  environment, unset it: `unset PROTOC`.

## 9. Where to look next

- `RAFT_NOTES.md` — paper-section to source-code map.
- `ARCHITECTURE.md` — workspace layout and crate-by-crate responsibilities.
- `CHANGELOG.md` — what's done and what's still on the punch list.
