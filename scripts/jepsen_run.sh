#!/usr/bin/env bash
# Placeholder runner for a Jepsen suite. The real Jepsen harness lives in
# Clojure and is out of scope of this repo's CI; this script documents the
# expected entry point and exits 2 when no harness is configured.
set -euo pipefail
if [[ -z "${JEPSEN_DIR:-}" ]]; then
  echo "JEPSEN_DIR not set. External Jepsen suite was not run. Use scripts/integration_cluster.py for the built-in fault harness."
  exit 2
fi
cd "$JEPSEN_DIR"
lein run test --workload register --concurrency 10 --time-limit 1800 --nemesis partition
