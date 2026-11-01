# Running and inspecting RaftKV

## Prerequisites and build

Use Rust stable, Python 3 and a Unix shell (macOS/Linux/WSL). Workspace metadata declares Rust 1.75, but use current stable for the resolved dependency graph. Node.js 22.12 or newer/npm is required for the dashboard. Optional tools are `redis-cli`, Docker Compose, Helm and `kubectl`. Protobuf compilation uses a vendored `protoc`.

```bash
cargo build --workspace --release
```

This builds `target/release/raftkv-server` and `target/release/raftkv-cli`. Run commands below from the repository root.

## Local cluster with configurable voters

```bash
./scripts/run_cluster.sh --nodes 3 --no-build
```

Arguments:

| Option | Meaning |
| --- | --- |
| `--nodes N` | Bootstrap 1–31 voters; default 3. Prefer odd counts for failure tolerance per replica. |
| `--run-dir PATH` | Config/log/PID/data directory; default `.run`. Use a fresh directory for a new voter count. |
| `--no-build` | Use existing release binaries. |
| `--generate-only` | Write symmetric TOML configurations without starting nodes. |
| `RAFTKV_BIN=/absolute/path` | Select a server binary, useful for integration testing. |

The launcher checks occupied ports, refuses live config rewrites, writes all voters into every peer list, tracks each PID and waits up to 15 seconds for APIs and shared leader discovery. A failed start stops processes it started. Existing data/logs are preserved.

| Node `i` | Formula | Node 1 | Node 5 |
| --- | --- | ---: | ---: |
| Raft gRPC | `7000 + i` | 7001 | 7005 |
| RESP client | `6378 + i` | 6379 | 6383 |
| Prometheus HTTP | `9100 + i` | 9101 | 9105 |
| Dashboard JSON API | `8079 + i` | 8080 | 8084 |

```bash
# Stop the default group before reusing its ports.
./scripts/stop_cluster.sh
./scripts/run_cluster.sh --nodes 5 --run-dir .run-five --no-build
./scripts/stop_cluster.sh --run-dir .run-five
```

A five-voter group needs three voters to commit; four voters also need three. Changing `--nodes` on persisted data is refused because editing bootstrap configuration is not a safe membership change. Adding a UI connection only registers an endpoint in the browser. Online voter addition/removal is unfinished.

The stop helper sends SIGINT only to recorded processes whose command line matches the expected node config. It preserves files. Older/manual processes have to be stopped by their own PID; avoid killing unrelated server processes.

## Start the dashboard

```bash
npm --prefix ui ci
npm --prefix ui run dev
```

Open [http://127.0.0.1:5173](http://127.0.0.1:5173). Vite proxies `/api` to node 1 and `/nodes/<API_PORT>/api/...` to the specified localhost API port. Use node connection settings to inspect ports 8080, 8081, 8082, and additional nodes. The gateway accepts local ports, not arbitrary remote hostnames. It is available in Vite development/preview; deploying only static `dist/` requires an equivalent authenticated HTTP proxy.

The six panes are Cluster overview, Nodes & replication, Keyspace explorer, Telemetry & events, Architecture report, and Connections & runbook. Search and 40-row client pagination organize the key inventory; binary key names are hex/read-only. The console retains up to 120 samples per node and 100 session events, exports JSON diagnostics without values and a Markdown architecture report, and offers 1/2.5/5/10-second polling with pause/refresh. Connections persist in browser storage and check node identity. Membership discovery assumes the launcher port convention unless a custom endpoint is registered.

The UI polls node-local observations. It distinguishes endpoint reachability, locally reported role, quorum arithmetic and leader replication progress. KV inspection reads are local and may lag. Mutations must reach the current leader; a leadership change can require refreshing discovery/retrying after checking the result.

```bash
npm --prefix ui test
npm --prefix ui run build
npm --prefix ui run preview
```

## RESP smoke tests

Find the leader from API status or `raftkv-cli status` on each port:

```bash
curl -s http://127.0.0.1:8080/api/status
./target/release/raftkv-cli --addr 127.0.0.1:6379 status
./target/release/raftkv-cli --addr 127.0.0.1:6380 status
./target/release/raftkv-cli --addr 127.0.0.1:6381 status
```

Use the leader's actual RESP port; 6380 is an example:

```bash
./target/release/raftkv-cli --addr 127.0.0.1:6380 ping
./target/release/raftkv-cli --addr 127.0.0.1:6380 set greeting "hello raft"
./target/release/raftkv-cli --addr 127.0.0.1:6380 get greeting
./target/release/raftkv-cli --addr 127.0.0.1:6380 del greeting
./target/release/raftkv-cli --addr 127.0.0.1:6380 members
```

With `redis-cli`, explicitly use RESP2 if your client defaults to RESP3:

```bash
redis-cli -2 -p 6380 PING
redis-cli -2 -p 6380 SET counter 1
redis-cli -2 -p 6380 INCR counter
redis-cli -2 -p 6380 GET counter
redis-cli -2 -p 6380 INFO raft
```

GET/MGET/EXISTS/DBSIZE use the runtime's quorum read-index path. Follower reads/writes return redirect or error responses; the CLI does not automatically follow them. A timeout does not prove the write failed. Do not blindly retry non-idempotent commands such as INCR.

The command protocol is a RESP2-compatible subset. Full Redis semantics, RESP3/HELLO, multiple databases, transactions and pub/sub are absent. TTL/expiry behavior is incomplete; use it only for implementation study.

## HTTP API examples

Every node has its own API. All dashboard read responses describe local applied data. Writes require the leader, return an error on a follower, and are never forwarded by guessing a port.

```bash
curl -s http://127.0.0.1:8080/api/status
curl -s http://127.0.0.1:8080/api/keys
curl -s http://127.0.0.1:8080/api/log
# Substitute the API port of the current leader for mutations.
curl -s -X PUT http://127.0.0.1:8081/api/keys/greeting \
  -H 'Content-Type: application/json' --data '{"value":"hello raft"}'
curl -s http://127.0.0.1:8081/api/keys/greeting
curl -s -X DELETE http://127.0.0.1:8081/api/keys/greeting
```

Keys must be UTF-8 and URL-percent-encoded when they contain reserved characters; e.g. `a/b` becomes `a%2Fb`. PUT requires a JSON object containing a string `value`. The parser supports fragmented requests and requires Content-Length for PUT, bounds headers to 16 KiB and body to 128 KiB, and applies a five-second read deadline. RESP frames separately allow up to 8 MiB, 16,384 array elements and 32 nesting levels.

GET `/api/log` returns up to the last 100 retained entry metadata records (index, term, kind, payload byte count and committed/applied flags), with no KV values. It has no pagination/query API. `/api/keys` returns the complete local inventory; the UI paginates the returned list, not the server scan.

Status reports storage health/failure, membership/quorum, local endpoints, configured timers, indexes, counters, request queues, capabilities and limitations. Replication progress is leader-only; remote roles can remain unknown. `recent_active` indicates a past protocol response, not a fresh network health check. Counters reset on process restart; disk indexes and terms persist. Send counters count outbound attempts/submitted RPC responses, not confirmed deliveries. Per-peer `transport_inflight`/`transport_limit` and config `transport_concurrency_limit` expose the separate 32-send transport admission cap.

## Manual single-node configuration

```bash
cat > single-node.toml <<'TOML'
id = 1
raft_listen = "127.0.0.1:7001"
client_listen = "127.0.0.1:6379"
metrics_listen = "127.0.0.1:9101"
ui_listen = "127.0.0.1:8080"
data_dir = "./data/single-node"
election_timeout_ms = 300
heartbeat_ms = 50
tick_ms = 10
snapshot_entries_threshold = 10000
pre_vote = true
[[peers]]
id = 1
raft_addr = "http://127.0.0.1:7001"
TOML
RUST_LOG=info ./target/release/raftkv-server --config single-node.toml
```

A one-voter group is self-quorum after its election timeout. For manual multi-node configs, each node needs a unique ID/data directory/listener ports, the same full initial voter set including itself, and valid `http://` peer URLs. Validate election timeout > heartbeat interval and nonzero tick intervals. CLI/env overrides include `--id`, `--raft-listen`, `--client-listen`, `--metrics-listen`, `--ui-listen` and `--data-dir` (`RAFTKV_*` equivalents).

## Metrics and deployment examples

```bash
curl http://127.0.0.1:9101/metrics
```

The endpoint emits node-labeled gauges `raftkv_term`, `raftkv_commit_index`, `raftkv_applied_index`, `raftkv_last_log_index`, `raftkv_keys`, `raftkv_uptime_seconds`, `raftkv_storage_healthy` and `raftkv_is_leader`, plus `raftkv_<counter_name>_total` runtime counters. These reset on restart where appropriate; no client latency percentile histogram is currently measured.

Use `grafana/dashboards/raftkv.json` and `grafana/alerting/alerts.yml` as starting points; check their metric names against the actual endpoint before relying on panels or alerts.

```bash
docker compose up --build
```

Compose uses a bridge network with `node1`/`node2`/`node3` peer DNS and separate volumes. It publishes host ports for peer, client and metrics endpoints. The example does not publish the localhost dashboard API; configure binding/mapping explicitly if needed and keep access restricted.

```bash
helm template raftkv ./helm/raftkv
helm install raftkv ./helm/raftkv
```

The chart derives IDs from pod ordinals, bootstraps pods in parallel and publishes not-yet-ready peer DNS. Validate generated manifests and service/storage settings before installing; live Kubernetes operation was not verified during this review. TCP probes test a listening socket, not quorum availability. The client service can send a connection to a follower.

## Verification and reproducibility

```bash
cargo fmt --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
python3 scripts/tests/test_launcher.py
python3 scripts/tests/live_cluster.py --nodes 4
python3 scripts/tests/live_cluster.py --nodes 5
```

The real cluster suites reserve the default local ports, so stop running groups first. They exercise failover and clean restarts using temporary test data. Unit tests and in-memory simulation are separate from power-loss recovery; the external Jepsen runner is a placeholder unless `JEPSEN_DIR` is configured.

## Troubleshooting

| Symptom | What to inspect |
| --- | --- |
| Port unavailable / startup refused | Stop the previous cluster or inspect the specific listener; launchers share port formulas. |
| Persisted voter count differs | Restart with the original count or choose a fresh run directory. Do not edit around the membership guard. |
| No leader / repeated elections | Check unique IDs, symmetric full peer lists, peer URL scheme/ports, network reachability and disk latency. |
| Writes/read-index time out | Verify a majority is available and that the targeted node is current leader; inspect logs and storage-error counters. |
| HTTP mutation returns not-leader | Refresh status and select the leader's API endpoint; the API does not infer remote ports. |
| Follower shows older data | HTTP KV inspection is local. Compare applied indexes or use leader RESP quorum reads. |
| More connections shown than actual voters | Browser endpoint registration and consensus membership are separate. |
| Static console has no API connectivity | Serve through Vite gateway or configure equivalent HTTP routing; `dist/` alone does not proxy. |
| Snapshot exists but data cannot be restored | Snapshot files do not yet contain complete runtime state; snapshot backup/restore is unfinished. |
| TTL does not expire | Periodic replicated expiration and Redis TTL semantics are unfinished. |

See [PROJECT_REPORT.md](PROJECT_REPORT.md) for confirmed scenarios and remaining durability/security work, and [RAFT_NOTES.md](RAFT_NOTES.md) for protocol review questions.
