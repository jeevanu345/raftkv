#!/usr/bin/env bash
# Spin up a 3-node raftkv cluster on localhost in the background.
# Logs go to ./.run/nodeN.log; data to ./.run/data/nodeN.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
RUN_DIR="$ROOT/.run"
mkdir -p "$RUN_DIR/data/node1" "$RUN_DIR/data/node2" "$RUN_DIR/data/node3"

cargo build -p raftkv-server --release

cat > "$RUN_DIR/node1.toml" <<EOF
id = 1
raft_listen   = "127.0.0.1:7001"
client_listen = "127.0.0.1:6379"
metrics_listen = "127.0.0.1:9101"
data_dir = "$RUN_DIR/data/node1"
election_timeout_ms = 300
heartbeat_ms = 50
tick_ms = 10
snapshot_entries_threshold = 10000
pre_vote = true
[[peers]]
id = 1
raft_addr = "http://127.0.0.1:7001"
[[peers]]
id = 2
raft_addr = "http://127.0.0.1:7002"
[[peers]]
id = 3
raft_addr = "http://127.0.0.1:7003"
EOF
cat > "$RUN_DIR/node2.toml" <<EOF
id = 2
raft_listen   = "127.0.0.1:7002"
client_listen = "127.0.0.1:6380"
metrics_listen = "127.0.0.1:9102"
data_dir = "$RUN_DIR/data/node2"
election_timeout_ms = 300
heartbeat_ms = 50
tick_ms = 10
snapshot_entries_threshold = 10000
pre_vote = true
[[peers]]
id = 1
raft_addr = "http://127.0.0.1:7001"
[[peers]]
id = 2
raft_addr = "http://127.0.0.1:7002"
[[peers]]
id = 3
raft_addr = "http://127.0.0.1:7003"
EOF

cat > "$RUN_DIR/node3.toml" <<EOF
id = 3
raft_listen   = "127.0.0.1:7003"
client_listen = "127.0.0.1:6381"
metrics_listen = "127.0.0.1:9103"
data_dir = "$RUN_DIR/data/node3"
election_timeout_ms = 300
heartbeat_ms = 50
tick_ms = 10
snapshot_entries_threshold = 10000
pre_vote = true
[[peers]]
id = 1
raft_addr = "http://127.0.0.1:7001"
[[peers]]
id = 2
raft_addr = "http://127.0.0.1:7002"
[[peers]]
id = 3
raft_addr = "http://127.0.0.1:7003"
EOF

BIN="$ROOT/target/release/raftkv-server"
RUST_LOG=${RUST_LOG:-info,raftkv=info} "$BIN" --config "$RUN_DIR/node1.toml" >"$RUN_DIR/node1.log" 2>&1 &
echo "node1 pid $!"
RUST_LOG=${RUST_LOG:-info,raftkv=info} "$BIN" --config "$RUN_DIR/node2.toml" >"$RUN_DIR/node2.log" 2>&1 &
echo "node2 pid $!"
RUST_LOG=${RUST_LOG:-info,raftkv=info} "$BIN" --config "$RUN_DIR/node3.toml" >"$RUN_DIR/node3.log" 2>&1 &
echo "node3 pid $!"
echo "logs in $RUN_DIR/nodeN.log"
echo "use redis-cli -p 6379 (or 6380/6381) to talk to a node"
