#!/usr/bin/env bash
# Placeholder runner for a Jepsen suite. The real Jepsen harness lives in
# Clojure and is out of scope of this repo's CI; this script documents the
# expected entry point and exits 0 when no harness is configured.
set -euo pipefail
if [[ -z "${JEPSEN_DIR:-}" ]]; then
  echo "JEPSEN_DIR not set — skipping Jepsen suite (CI smoke test only)."
  exit 0
fi
cd "$JEPSEN_DIR"
lein run test --workload register --concurrency 10 --time-limit 1800 --nemesis partition
