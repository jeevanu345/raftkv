#!/usr/bin/env bash
# Signal only PIDs whose command matches this checkout's server and config.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
for node in 1 2 3; do
  file="$ROOT/.runtime/node$node.pid"
  [[ -f "$file" ]] || continue
  pid=$(cat "$file")
  [[ "$pid" =~ ^[0-9]+$ ]] || { echo "Invalid PID file: $file" >&2; exit 1; }
  command=$(ps -p "$pid" -o command= || true)
  if [[ "$command" == *"$ROOT/target/release/raftkv-server --config $ROOT/.runtime/node$node.toml"* ]]; then
    kill -TERM "$pid"
    echo "Signaled node$node ($pid)"
  elif [[ -n "$command" ]]; then
    echo "PID $pid belongs to another command; refusing to stop it" >&2
    continue
  fi
  rm -f "$file"
done
