#!/usr/bin/env bash
# Local development only. Durable data and PID files stay in .runtime/.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
RUN_DIR="$ROOT/.runtime"
mkdir -p "$RUN_DIR"
cd "$ROOT"
cargo build -p raftkv-server -p raftkv-cli --release --locked
for node in 1 2 3; do
  if [[ -f "$RUN_DIR/node$node.pid" ]] && kill -0 "$(cat "$RUN_DIR/node$node.pid")" 2>/dev/null; then
    echo "node$node is already running; stop the cluster first" >&2
    exit 1
  fi
done
for node in 1 2 3; do
  cat > "$RUN_DIR/node$node.toml" <<EOF
id = $node
raft_listen = "127.0.0.1:$((7000+node))"
client_listen = "127.0.0.1:$((6378+node))"
metrics_listen = "127.0.0.1:$((9100+node))"
ui_listen = "127.0.0.1:$((8079+node))"
data_dir = "$RUN_DIR/data/node$node"
election_timeout_ms = 300
heartbeat_ms = 50
tick_ms = 10
snapshot_entries_threshold = 10000
pre_vote = true
EOF
  for peer in 1 2 3; do
    cat >> "$RUN_DIR/node$node.toml" <<EOF
[[peers]]
id = $peer
raft_addr = "http://127.0.0.1:$((7000+peer))"
client_addr = "127.0.0.1:$((6378+peer))"
admin_addr = "http://127.0.0.1:$((8079+peer))"
EOF
  done
  RUST_LOG=${RUST_LOG:-info} "$ROOT/target/release/raftkv-server" --config "$RUN_DIR/node$node.toml" >"$RUN_DIR/node$node.log" 2>&1 &
  pid=$!
  echo "$pid" > "$RUN_DIR/node$node.pid"
  echo "node$node pid $pid; HTTP $((8079+node)), RESP $((6378+node))"
done
echo "Logs and data: $RUN_DIR. Dashboard: cd ui/dashboard && npm ci && npm run dev"
