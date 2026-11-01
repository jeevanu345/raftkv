#!/usr/bin/env bash
# Stop only processes recorded by this cluster launcher; preserve data/logs.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
RUN_DIR="$ROOT/.run"
if [[ ${1:-} == --run-dir && $# -eq 2 ]]; then
  RUN_DIR="$2"
elif [[ $# -ne 0 ]]; then
  echo 'Usage: scripts/stop_cluster.sh [--run-dir PATH]' >&2; exit 2
fi
[[ -d "$RUN_DIR" ]] || { echo "No cluster directory: $RUN_DIR"; exit 0; }
RUN_DIR=$(cd "$RUN_DIR" && pwd)
found=false
for pid_file in "$RUN_DIR"/node*.pid; do
  [[ -f "$pid_file" ]] || continue
  found=true
  pid=$(cat "$pid_file")
  if [[ ! "$pid" =~ ^[0-9]+$ ]]; then
    echo "Invalid PID file: $pid_file" >&2; continue
  fi
  config="${pid_file%.pid}.toml"
  command_line=$(ps -p "$pid" -o command= 2>/dev/null || true)
  if [[ "$command_line" == *raftkv-server* && "$command_line" == *"--config $config"* ]]; then
    kill -INT "$pid" 2>/dev/null || true
    for ((attempt=0; attempt<50; attempt++)); do
      kill -0 "$pid" 2>/dev/null || break
      sleep .1
    done
    if kill -0 "$pid" 2>/dev/null; then
      echo "Node $pid is still stopping; PID file retained." >&2
      continue
    fi
    echo "Stopped node process $pid"
  elif [[ -n "$command_line" ]]; then
    echo "PID $pid no longer belongs to this cluster; leaving it alone." >&2
  fi
  rm -f "$pid_file"
done
$found || echo "No tracked node processes in $RUN_DIR. Older manually started nodes must be stopped by their own PID."
