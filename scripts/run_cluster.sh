#!/usr/bin/env bash
# Bootstrap/restart a static local Raft cluster. Changing persisted membership
# requires the administration workflow; never rewrite it by changing --nodes.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
RUN_DIR="$ROOT/.run"
NODES=3
BUILD=true
GENERATE_ONLY=false
BIN=${RAFTKV_BIN:-"$ROOT/target/release/raftkv-server"}
usage() {
  cat <<'HELP'
Usage: scripts/run_cluster.sh [--nodes 1..31] [--run-dir PATH] [--no-build] [--generate-only]

Defaults: 3 nodes, .run, release build. Use a NEW run directory for a new
voter count. Existing data is preserved; online membership changes are not
implemented. Odd voter counts use resources more efficiently (5 tolerates 2
failures; 6 also tolerates 2). RAFTKV_BIN overrides the server executable.
Node i ports: Raft 7000+i, RESP 6378+i, metrics 9100+i, API 8079+i.
HELP
}
while [[ $# -gt 0 ]]; do
  case "$1" in
    --nodes) [[ $# -ge 2 ]] || { usage; exit 2; }; NODES="$2"; shift 2 ;;
    --run-dir) [[ $# -ge 2 ]] || { usage; exit 2; }; RUN_DIR="$2"; shift 2 ;;
    --no-build) BUILD=false; shift ;;
    --generate-only) GENERATE_ONLY=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done
[[ "$NODES" =~ ^[0-9]+$ ]] && [[ "$NODES" -ge 1 && "$NODES" -le 31 ]] || {
  echo '--nodes must be an integer between 1 and 31' >&2; exit 2;
}
mkdir -p "$RUN_DIR"
RUN_DIR=$(cd "$RUN_DIR" && pwd)
# Refuse live config rewrites and PID duplication, including generate-only.
for pid_file in "$RUN_DIR"/node*.pid; do
  [[ -f "$pid_file" ]] || continue
  pid=$(cat "$pid_file")
  if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
    echo "Recorded process $pid is still running. Stop this cluster before rewriting configs." >&2
    exit 1
  fi
done
# Protect pre-existing clusters created before the membership marker existed.
for old_config in "$RUN_DIR"/node*.toml; do
  [[ -f "$old_config" ]] || continue
  old_count=$(awk '/^\[\[peers\]\]/{n++} END{print n+0}' "$old_config")
  if [[ "$old_count" -ne "$NODES" ]] && [[ -d "$RUN_DIR/data" ]]; then
    echo "Existing directory was configured for $old_count voters; requested $NODES. Use --run-dir with a fresh path." >&2
    exit 1
  fi
done
if [[ -f "$RUN_DIR/cluster-size" ]] && [[ "$(cat "$RUN_DIR/cluster-size")" != "$NODES" ]]; then
  echo 'Persisted cluster size differs. Use a fresh --run-dir; membership cannot be changed by restart.' >&2
  exit 1
fi
if ! $GENERATE_ONLY; then
  command -v python3 >/dev/null || { echo 'python3 is required for port and readiness checks' >&2; exit 1; }
  python3 - "$NODES" <<'PY'
import socket, sys
sockets = []
try:
    for node in range(1, int(sys.argv[1]) + 1):
        for base in (7000, 6378, 9100, 8079):
            port = base + node
            sock = socket.socket()
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sockets.append(sock)
            try:
                sock.bind(('127.0.0.1', port))
            except OSError as error:
                sys.exit(f'Port {port} is unavailable: {error}. Stop the existing listener before starting another cluster.')
finally:
    for sock in sockets:
        sock.close()
PY
  if $BUILD; then
    (cd "$ROOT" && cargo build -p raftkv-server -p raftkv-cli --release)
  fi
  [[ -x "$BIN" ]] || { echo "Server executable missing: $BIN" >&2; exit 1; }
fi
for ((i=1; i<=NODES; i++)); do
  mkdir -p "$RUN_DIR/data/node$i"
  config_tmp="$RUN_DIR/node$i.toml.tmp"
  cat > "$config_tmp" <<CONFIG
id = $i
raft_listen = "127.0.0.1:$((7000+i))"
client_listen = "127.0.0.1:$((6378+i))"
metrics_listen = "127.0.0.1:$((9100+i))"
ui_listen = "127.0.0.1:$((8079+i))"
data_dir = "$RUN_DIR/data/node$i"
election_timeout_ms = 300
heartbeat_ms = 50
tick_ms = 10
snapshot_entries_threshold = 10000
pre_vote = true
CONFIG
  for ((peer=1; peer<=NODES; peer++)); do
    cat >> "$config_tmp" <<PEER
[[peers]]
id = $peer
raft_addr = "http://127.0.0.1:$((7000+peer))"
PEER
  done
  mv "$config_tmp" "$RUN_DIR/node$i.toml"
done
printf '%s\n' "$NODES" > "$RUN_DIR/cluster-size"
if $GENERATE_ONLY; then
  echo "Generated $NODES symmetric node configurations in $RUN_DIR"
  exit 0
fi
started_pids=()
cleanup_failed_start() {
  for pid in "${started_pids[@]}"; do kill "$pid" 2>/dev/null || true; done
}
trap cleanup_failed_start ERR INT TERM
printf '%-6s %-8s %-8s %-8s %-8s %-8s\n' Node PID Raft RESP Metrics API
for ((i=1; i<=NODES; i++)); do
  RUST_LOG=${RUST_LOG:-info,raftkv=info} "$BIN" --config "$RUN_DIR/node$i.toml" >> "$RUN_DIR/node$i.log" 2>&1 &
  pid=$!
  started_pids+=("$pid")
  printf '%s\n' "$pid" > "$RUN_DIR/node$i.pid"
  printf '%-6s %-8s %-8s %-8s %-8s %-8s\n' "$i" "$pid" "$((7000+i))" "$((6378+i))" "$((9100+i))" "$((8079+i))"
done
python3 - "$NODES" <<'PY'
import json, sys, time, urllib.request
count = int(sys.argv[1])
deadline = time.monotonic() + 15
while time.monotonic() < deadline:
    statuses = []
    for node in range(count):
        try:
            with urllib.request.urlopen(f'http://127.0.0.1:{8080+node}/api/status', timeout=.3) as response:
                statuses.append(json.load(response))
        except (OSError, ValueError):
            pass
    leaders = [s for s in statuses if s['role'] == 'leader']
    if len(statuses) == count and len(leaders) == 1 and all(s.get('leader_id') == leaders[0]['node_id'] for s in statuses):
        print(f"All {count} APIs ready; leader node {leaders[0]['node_id']}, term {leaders[0]['term']}.")
        break
    time.sleep(.2)
else:
    sys.exit('Cluster did not converge within 15 seconds; inspect node logs. Started processes will be stopped.')
PY
trap - ERR INT TERM
echo "Logs: $RUN_DIR/nodeN.log"
echo "Stop: $ROOT/scripts/stop_cluster.sh --run-dir $RUN_DIR"
echo "Dashboard: cd $ROOT/ui && npm run dev"
