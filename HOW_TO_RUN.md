# Running RaftKV

## Local development

Rust 1.94+, Node 22.12+ and npm are required. Build with `cargo build --workspace --locked`. The peer protobuf compiler is vendored. Use `./scripts/run_cluster.sh` from any directory to build release binaries and start three localhost nodes. Data and PID files live in `.runtime/`. `./scripts/stop_cluster.sh` checks PID ownership before sending SIGTERM. Neither script modifies preserved `.run/` databases.

```bash
./scripts/run_cluster.sh
cargo run -p raftkv-server --bin raftkv-lab
# Separate terminal:
cd ui/dashboard
npm ci
VITE_DEMO_MODE=false npm run dev
```

Appearance controls are available only in **Administration → Appearance**. Choose light or dark mode and adjust the global font size from 85% to 150%. Changes apply to every dashboard route immediately and persist in this browser; Reset appearance restores dark mode at 100%. These are local display preferences and do not change cluster configuration.

The development proxy preserves the browser-facing Host header for same-origin validation. Production reverse proxies must preserve the public Host too, or configure the explicit trusted `allowed_origins`; do not disable origin checks.

Vite binds localhost:5173 and proxies `/api`, `/health` and `/metrics` to node 1 on 8080, and `/lab` to the isolated lab on 8090. HTTP key/command/admin requests can forward to the leader. The console's explicit target selector instead exposes that selected node's real response, including follower MOVED errors.

Demo mode (`VITE_DEMO_MODE=true`) is explicitly labelled and contains stateful UI fixtures. It is not the deterministic consensus simulator. To execute actual simulator faults/replay, start `raftkv-lab` and use live mode.

`npm run build` creates `ui/dashboard/dist/`. Serve it through a same-origin reverse proxy that forwards `/api`, `/health`, `/metrics` and `/lab` to the appropriate backend. `npm run preview` only serves the built assets; it does not provide the development API proxy. No backend process serves `dist/` automatically. Do not build a remote admin deployment around the unauthenticated local examples.

## RESP clients

Find the leader using `/api/v1/cluster`, the dashboard or `redis-cli -p 6379 INFO raft`. Follower reads and writes return `MOVED 0 <advertised RESP endpoint>`; they never fall back to stale local reads. Redis's redirect behavior varies by client; the small CLI does not automatically follow MOVED.

```bash
redis-cli -p 6379 SET greeting 'hello raft'
redis-cli -p 6379 GET greeting
redis-cli -p 6379 SET expiring value PX 5000
redis-cli -p 6379 PTTL expiring
redis-cli -p 6379 SCAN 0 MATCH 'user:*' COUNT 100
```

Supported commands: PING, ECHO, QUIT, SELECT (database 0 only), COMMAND, INFO, CLUSTER NODES/INFO, GET/MGET, EXISTS, DBSIZE, SET (EX/PX), SETEX, DEL, INCR/DECR, MSET, EXPIRE, PERSIST, TTL/PTTL, SCAN and FLUSHDB. This is not a full Redis server. Use a TLS-capable Redis client for the optional TLS RESP plane; the basic KV CLI connection is plaintext.

## Manual node configuration

```toml
id = 1
raft_listen = "127.0.0.1:7001"
client_listen = "127.0.0.1:6379"
metrics_listen = "127.0.0.1:9101"
ui_listen = "127.0.0.1:8080"
data_dir = "./node1-data"
election_timeout_ms = 300
heartbeat_ms = 50
tick_ms = 10
snapshot_entries_threshold = 10000
pre_vote = true
[[peers]]
id = 1
raft_addr = "http://127.0.0.1:7001"
client_addr = "127.0.0.1:6379"
admin_addr = "http://127.0.0.1:8080"
```

```bash
./target/debug/raftkv-server --config node1.toml
```

All initial nodes must agree on the voter set. IDs and advertised endpoints must be unique and reachable in the deployment's network. Bind addresses and advertised endpoints serve different purposes. Dynamic nodes must first be registered as learners, catch up, then be promoted. A newly added learner boots with the current initial voters plus itself with `learner=true`. Dynamic membership and endpoint metadata survive snapshots/restart. Existing voters cannot be directly downgraded to learners.

## TLS, tokens and roles

See [SECURITY.md](docs/SECURITY.md). Configure PEM `cert`, `key` and `ca` files independently under `[peer_tls]`, `[client_tls]` and `[admin_tls]`. Peer URLs must be `https://`; each peer record needs SHA-256 of its DER certificate in `certificate_sha256`. Peer certificates must have SANs matching advertised hostnames and client/server usage. Dynamic mTLS learner registration needs its certificate fingerprint as well as its three endpoints.

Use `RAFTKV_ADMIN_TOKEN` and `RAFTKV_CLIENT_TOKEN` secret environment variables, or TOML `admin_users`/`client_users`. Never commit real credentials. Dashboard sign-in exchanges a token for a 12-hour HttpOnly/SameSite=Strict session cookie, Secure with admin TLS. Production browser/API hosting should use a single HTTPS origin.

## Backups and offline restore

```bash
./target/debug/raftkv-cli --admin-addr http://127.0.0.1:8080 backup create backup.rkv
./target/debug/raftkv-cli backup inspect backup.rkv
# For HTTPS, supply --ca-cert cluster-ca.pem and --admin-token via the environment.
./target/debug/raftkv-cli restore backup.rkv   --data-dir ./restored-node   --bootstrap-node-id 1   --raft-addr http://127.0.0.1:7001   --client-addr 127.0.0.1:6379   --admin-addr http://127.0.0.1:8080
```

Backup create rejects an existing destination file. Restore verifies envelope/state/manifest integrity, stages the database, fsyncs and atomically renames it. The destination must be absent or empty. Start the restored directory as the explicitly named **new single-voter cluster**; add fresh learners afterward. Restore is intentionally offline and never replaces a running member. A backup preserves logical state and logical time, not an arbitrary copy of live sled/log files. Expired absolute TTL deadlines may be deleted after the new leader advances replicated time.

Old empty/unchecked snapshot formats are not silently upgraded. Preserve old data and the old executable before attempting an upgrade; do not point the new server at a legacy data directory without testing recovery on a copy.

## Containers and Kubernetes

`docker compose up --build` uses the checked-in development TOML files. Nodes advertise container DNS endpoints, and administrative host ports bind 127.0.0.1:8080–8082. RESP host ports are 6379–6381. These examples have no credentials and are for local development. The release image runs as UID 10001.

```bash
helm lint helm/raftkv
helm template raftkv helm/raftkv
helm install raftkv helm/raftkv
```

The chart is template-validated locally, not tested on a Kubernetes cluster. Set your own built image, token Secret and TLS Secret before remote deployment. `auth.existingSecret` contains `admin-token` and `client-token`. `tls.existingSecret` contains `ca.crt`, `node1.crt`, `node1.key`, etc. Populate `tls.peerCertificates` with string keys `"1"`, `"2"`, etc. and certificate fingerprints. SANs must match the StatefulSet peer DNS names. Pod names derive unique Raft IDs. The headless service publishes not-ready addresses for bootstrap; pods start in parallel. PersistentVolumeClaims are retained.

Set `RAFTKV_LOG_FORMAT=json` for structured JSON tracing.

NetworkPolicy allows labelled `raftkv-client: "true"` and `raftkv-admin: "true"` callers, peer traffic, DNS and Prometheus scraping. Review namespace selectors and your CNI's enforcement. Optional ServiceMonitor requires the Prometheus Operator CRDs. ConfigMaps expose no tokens/private keys.
