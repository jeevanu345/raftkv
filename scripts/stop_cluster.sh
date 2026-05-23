#!/usr/bin/env bash
# Stop any running raftkv-server processes started by run_cluster.sh.
set -euo pipefail
pkill -f 'target/release/raftkv-server' || true
echo "raftkv-server processes signaled to exit"
