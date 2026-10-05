# Local benchmark evidence

The [release 81-case result file](benchmark-results-release.json) and [earlier debug 81-case sample](benchmark-results.json) cover 1/3/5 nodes × 50%/95%/0% reads × pipeline 1/8/32 × values 64/1024/16384 bytes. Each case used only **100 operations**, the stated build profile, one local host and deterministic client seed 42. Compilation/tests shared that host during collection. The release dataset records its binary SHA-256; it predates the final HMAC/dependency security updates, which do not alter the tested Raft workload. It is smoke evidence that the matrix runs, not a stable throughput or production capacity result.

Recorded: ops/sec; P50/P95/P99/P99.9 latency; process CPU-time percentage and RSS; RESP client bytes/sec; data-file growth/sec. Pipeline latency starts when the batch is sent and ends when each reply is read. CPU can exceed 100% across processes. Data growth is not filesystem write bandwidth, and client wire bytes exclude peer traffic/TLS/TCP overhead. The unavailable OS disk and total peer network bandwidth fields are explicitly null rather than invented.

```bash
cargo build --workspace --release --locked
cargo bench -p raftkv-bench --bench engine
# Reproduce the historical debug sample with --profile debug.
python3 scripts/benchmark_cluster.py --profile release --ops 1000 --output benchmark-results.json
```

The [final Criterion quick sample](criterion-quick.txt) contains ten benchmarks, including `raft_step`. Durable state-machine operations include sled flush, and durable append includes fsync. The observed millisecond write costs on this host explain much of the end-to-end latency; GET microbenchmarks bypass ReadIndex and must not be compared directly with network linearizable reads.

Use dedicated hosts, release binaries, warmups, larger samples, repeated trials and real disk/network instrumentation before performance conclusions. The project measures quorum/linearizability/durability costs, not Redis replacement speed.
