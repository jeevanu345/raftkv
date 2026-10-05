# Verification record

Local verification completed on 2026-10-05. These results are evidence for the tested cases, not a proof of production correctness.

## Passed locally

- Locked workspace build and Rust formatting; 40 workspace tests (state machine 4, checker 4, core 17, storage 6, RESP 3, simulator 6), including a 1,000-seed simulation sweep.
- Clippy correctness and suspicious gates. Style/pedantic warnings remain; this is not an all-warnings-clean claim.
- Three- and five-node real-process integration, including peer mTLS in the five-node run: binary values, TTL/SCAN, forwarding, exact write receipts, snapshot catch-up, leader failure, full restart, and isolated-leader read rejection/readiness failure.
- Learner snapshot catch-up, promotion from three to four voters, leadership transfer, joint removal back to three, and membership persistence after restart.
- HTTPS, RESP TLS/AUTH and prefix ACLs, RBAC, HMAC session cookies (tampering and expiry), origin checks, audit request correlation, backup integrity, CLI inspection and offline restore.
- Deterministic lab replay. Real-process peer partitions, healing, latency, leader kills, snapshots and restarts for three and five nodes; 72 completed operations checked for linearizability in each exported history. Failed/uncertain operations are not a claim of exhaustive history coverage.
- Seven fuzz targets, 1,000 runs each with seed 42, using local stable smoke binaries. These runs are not sanitizer or coverage-guided nightly validation.
- Ten quick Criterion benchmarks, plus 81 debug and 81 release workload combinations. See BENCHMARKS.md for measurement limits and the release binary identity.
- Dashboard strict TypeScript check and production build; 24 demo browser tests, three live API-fixture tests, and seven route screenshots. Routes exercised at 1440, 1024 and 768 pixels with keyboard, confirmation, loading, empty and error checks. Browser automation was headless test code; computer-use tools were not used.
- npm production dependency audit: zero vulnerabilities. Rust audit: zero vulnerabilities and four maintenance warnings. cargo-deny advisories, bans, licenses and sources passed with the documented maintenance-only exceptions in SECURITY.md.
- Helm lint and template rendering for three and five voters, including optional monitoring resources. Shell/Python syntax and repository whitespace checks.

## Verification not completed locally

Docker image execution/build and Kubernetes deployment require an available Docker daemon/cluster. Neither was available locally. Miri, instrumented nightly fuzzing, long-running soak tests and external Jepsen were not run locally. GitHub Actions configures additional checks; its status must be read separately and is not implied by the local results.

## Reproduce

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D clippy::correctness -D clippy::suspicious
RAFTKV_SIM_SEEDS=1000 cargo test --workspace --locked
cargo build --workspace --locked
python3 scripts/integration_cluster.py
python3 scripts/integration_cluster.py --nodes 5 --peer-tls
python3 scripts/integration_membership.py
python3 scripts/integration_security.py
python3 scripts/test_lab.py
python3 scripts/chaos_cluster.py
python3 scripts/chaos_cluster.py --nodes 5
cargo audit
cargo deny check
cd ui/dashboard
npm ci
npm run typecheck
npm run build
npx playwright install chromium
npm test
RAFTKV_UI_TEST_MODE=live npm test
```

The complete original request is preserved verbatim in IMPLEMENTATION_PROMPT.md. IMPLEMENTATION_STATUS.md maps requirements to implementation and explains remaining operational limits.
