# RaftKV technical project report

**Project:** Raft-replicated key-value store and operator console  
**Version:** 0.1.0, unreleased educational implementation  
**Review date:** 7 October 2026  
**Evidence:** workspace source, protocol definitions, deployment assets, repository Markdown, unit/simulation tests and local verification. Performance targets below are proposals, not measured results.

## 1. Purpose and scope

RaftKV combines a deterministic consensus core, a persistent replicated log, a sled-backed key-value state machine, gRPC peer communication, a subset of the Redis client protocol, and a browser operator console. Its strongest educational feature is the explicit boundary between protocol decisions and side effects: the core emits ordered actions that can be simulated independently of actual networking and disk access.

The console should answer concrete operational questions: which process is being observed, who its leader is, what quorum it requires, how far replication/application have advanced, which endpoints are reachable, what retained entries contain, what data is stored, and which parts of the design remain incomplete. Dense information is useful when its source, units, timestamp, and consistency boundary are visible.

This project provides a consensus group with a replicated full dataset. Increasing voter count improves failure tolerance at the cost of additional replication traffic; it does not add keyspace sharding, independent partitions, or horizontal write scaling.

## 2. Feature maturity and evidence

| Area | Present implementation | Boundary / next validation |
| --- | --- | --- |
| Consensus | Election, pre-vote, term/vote handling, follower conflict repair, current-term quorum commit | Requires broader adversarial schedules, crash/restart and integration fault tests. |
| Quorum arithmetic | Stable and joint voter sets; correct majority selection for even counts and missing progress | Exercise 1/3/4/5/7-voter configurations and transition histories. |
| Replication efficiency | Entry batching, progress states, idle feedback-loop suppression, per-peer transport concurrency cap of 32 | Core progress max-inflight remains 1,024; measure bounded behavior under sustained load and outages. |
| Read coordination | Request-correlated fresh quorum acknowledgment, current-term commit barrier and runtime application gating | Partition/stale-response/leadership-change tests are essential; diagnostics are local observations. |
| Write acknowledgment | Client waiter resolved after committed command application | Timeout can conceal a committed write; no durable idempotency keys. |
| Disk log | CRC32C-framed segments, append batch sync, truncation, startup tail repair | Crash test segment rotation, torn writes, file deletion and directory durability. |
| Hard state / storage health | Sled serialization and explicit flush; failed runtime stops processing/acknowledgments and KV access | Repair/restart is required; fail-closed behavior does not repair partial KV application. |
| KV | GET/SET/DEL/INCR/DECR/MGET/MSET/EXISTS/FLUSHDB and applied cursor | Multi-tree application is not atomic; Redis semantics are a subset. |
| Expiry | Expire/Persist/Tick command representation and sorted expiry records | `TTL` is placeholder; no periodic replicated Tick scheduler, and stale expiry replacement needs work. |
| Snapshots | Core messages, file writer/rename/retention, snapshot pointer | Complete KV/config export, transfer, install and restart recovery are unfinished. |
| Membership | Joint consensus types and core transition methods; protobuf contracts | Live operational membership service, learners, routing and recovery are unfinished. |
| Local scale | Generated symmetric peer lists and separate listener ports for 1–31 voters; readiness and PID checks | Bootstrap/restart a configured group; it does not hot-add voters to a running group. |
| Operator console | Six live panes, node connections, leader-routed mutations, bounded browser histories/export and local log metadata | Sampling is not a coherent cluster-wide transaction; remote address discovery needs explicit configuration. |
| Validation | Unit tests, seeded simulator, short-history exhaustive checker | Simulator is in memory; no bundled full Jepsen nemesis, crash harness or benchmark suite. |
| Deployment | Local scripts, Docker Compose examples, Helm with ordinal identity and parallel bootstrap | Helm rendering/live Kubernetes and hosted CI are unverified; templates alone are not production readiness. |

## 3. Recent engineering improvements

The consensus review addressed several concrete correctness and efficiency issues:

- Majority selection now accounts for even voter sets and absent peer progress; four voters need three acknowledgments.
- Follower acknowledgments and commit advancement are bounded by the portion of the leader's log actually matched by a request, avoiding credit for an unrelated local suffix.
- Read-index requests use distinct contexts and fresh quorum confirmations, wait for a current-term commit barrier, and retry leadership probes while pending.
- Runtime application advances through no-op/configuration entries as well as user commands, so a no-op does not leave the application barrier permanently behind.
- Idle successful AppendEntries responses no longer create continuous request/response traffic when there is no data to replicate.
- Runtime transitions and effects are serialized, read/proposal channels and waiter maps are capped at 1,024, transport admits 32 concurrent sends per peer, canceled waiters are pruned, and persistence/application failures stop the node from continuing consensus or serving KV access.
- Disk range reads now avoid repeated locking/segment searches; appends validate complete batch continuity and sync every touched segment.
- The history checker's exhaustive search restores the pre-branch model after an unsuccessful branch, including mutations from unsuccessful candidate operations.

These changes strengthen specific invariants. They do not close the separate snapshot, transactional state-application, authentication or full crash-recovery gaps.

## 4. Technical architecture and transaction lifecycle

See [ARCHITECTURE.md](ARCHITECTURE.md) for the component diagram and complete source map.

| Stage | Data representation | Owner | Success criterion |
| --- | --- | --- | --- |
| Parse | RESP frame or HTTP JSON/body | Protocol listener | Valid command and bounded request accepted. |
| Propose | Bincode KV command in a Raft `Normal` entry | Runtime + core | Current node can propose as leader. |
| Persist | CRC32C record and durable hard state | Storage interpreter | Required writes complete before dependent acknowledgment. |
| Replicate | AppendEntries with previous index/term and request ID | gRPC peer transport | Follower confirms the actual matching prefix. |
| Commit | Majority `match_index`, current-term guard | Core | Stable quorum, or both joint quorums. |
| Apply | Ordered command mutation + applied cursor | KV state machine | Local application reaches the entry. |
| Complete | Command response or timeout | Client waiter | Result returned; timeout remains ambiguous. |

There is one leader per successfully established term under the intended Raft invariants, while different disconnected processes can temporarily report stale leadership beliefs. A dashboard must distinguish a node's reported role from fresh cluster-wide leadership evidence.

## 5. Interpreting the dashboard's technical data

The six panes are **Cluster overview**, **Nodes & replication**, **Keyspace explorer**, **Telemetry & events**, **Architecture report**, and **Connections & runbook**. They use real API observations; no synthetic node data or benchmark points are inserted. The keyspace inventory supports search and 40-row browser pagination, UTF-8 value inspection/editing, leader-routed SET and confirmed DELETE. Binary key names are displayed in hex and are read-only in the HTTP console.

Connections persist in browser storage and verify the returned node identity. Membership discovery uses the local launcher convention `API port = 8079 + node ID`; custom ports can be registered explicitly. Removing a monitor does not remove a voter and membership discovery can rediscover it. Poll intervals are 1/2.5/5/10 seconds with a 4.5-second request timeout, pause/resume and manual refresh. History is bounded at 120 samples per node and 100 browser events; it resets on page reload. JSON diagnostics export includes telemetry/topology without key values; the architecture pane can export a Markdown report.


| Field / derived quantity | Meaning | Interpretation |
| --- | --- | --- |
| Node ID and endpoint | Identity of the process being sampled | Verify this before interpreting any peer table. |
| Term | Monotonic consensus election term | Rising terms can indicate elections, connectivity issues or unstable timing. |
| Leader ID | Local node's known leader | It is a hint, not a quorum lease. |
| Voters / quorum | Configuration count and required majority | Majority loss blocks new commits even if a process remains reachable. |
| Last log index | Local retained/abstract log tail | May include uncommitted commands. |
| Commit index | Highest locally known committed entry | A follower can lag the leader's knowledge until another append/heartbeat. |
| Applied index | Highest state-machine application position | Sustained `commit - applied` is application backlog. |
| Replication lag | Leader tail minus acknowledged follower match | Available on the leader; no leader-side progress should be invented for follower views. |
| Uncommitted gap | `last_log - commit` | Short gaps occur during normal replication; persistent gaps suggest quorum or transport delay. |
| State hash | Rolling command-history digest | Compare only at the same applied index; it is not a complete database checksum. |
| Runtime health | Healthy/failed state and last storage failure | Failed nodes stop consensus and KV access until repair/restart; socket reachability alone is not health. |
| Log preview | Last 100 stored entry metadata records | Includes index/term/kind/payload bytes/committed/applied flags; omits command values. |
| Uptime | Process lifetime | Resets at restart, unlike persisted terms/log positions. |
| Send counters | Outbound attempts/submitted RPC responses | They do not count confirmed network deliveries; per-peer transport inflight/limit shows admission pressure. |
| Events / charts | Bounded event history or browser polling samples | They are not a durable audit log, server tracing archive or measured benchmark. |
| HTTP latency | Request duration from the browser/API | It includes polling/network/serialization and is not Raft commit latency. |

Offline/unknown/empty are different states. Failed requests should remain visible with their error and last successful observation. A zero is appropriate only when a successful observation measured zero.

## 6. Scaling and capacity reasoning

A five-voter cluster can commit with three available voters and tolerate two unavailable voters; a seven-voter cluster requires four and tolerates three. An even voter count often adds replication cost without increasing failure tolerance over the previous odd count.

For every local node `i`, the launcher allocates independent Raft, RESP, metrics and HTTP endpoints plus an isolated data directory. Each fresh node must start from the same complete voter set. Existing data cannot safely be converted to a new membership by editing bootstrap TOML files.

The console can inspect additional endpoints and the launcher can bootstrap a larger group. A future true **Add voter** feature must perform these steps:

1. Validate unique identity, address, protocol compatibility and admission policy.
2. Register transport/address-book state and start the new process as a non-voting learner.
3. Catch up log or a complete verified snapshot without increasing the current quorum.
4. Propose a serialized joint configuration and wait for both majorities.
5. Commit the final configuration, persist/recover membership and update routing.
6. Handle cancellation, leader change, removal and retries without concurrent unsafe transitions.

This sequence is a roadmap, not the behavior of a dashboard endpoint registration control.

## 7. Failure and durability analysis

| Failure | Intended behavior | Present practical boundary |
| --- | --- | --- |
| One voter stops in a three-node group | Remaining two elect/retain leader and commit | Verify with real process failover and client retry tests. |
| Majority unavailable | New commits and strong reads cannot complete | Timeout is expected; it must not be presented as committed success. |
| Old leader isolated | Fresh read quorum cannot be established | Distinct read contexts and term checks protect against stale confirmations. |
| Follower log diverges | Retry previous prefix, truncate conflict, append matching entries | Follower matching must not credit an unmatched suffix. |
| Partial tail write | Segment scan stops and truncates invalid tail | Does not replace testing corruption in earlier segments or media failures. |
| Disk write error | Mark runtime failed and stop processing/acknowledgments and KV access | Status/metrics preserve the failure; repair/restart is required. Partial effects still need crash-consistency work. |
| Crash midway through KV mutation | Recover/replay deterministically | Separate sled tree writes and non-idempotent commands are not transactionally protected. |
| Snapshot needed for lagging node | Install full state image and continue from boundary | Dataset/config snapshot integration is unfinished. |
| Client times out after proposal | Client learns an unknown result | No exactly-once retry or durable result lookup protocol. |

Disk log durability, state-machine transactionality and snapshot recoverability are separate concerns. The presence of sled WAL, CRCs or `rename()` is insufficient to establish the complete service's crash consistency.

## 8. Performance and efficiency plan

No throughput, latency percentile or recovery-time result is claimed without a recorded workload and hardware description.

| Priority | Work | Why it matters | Evidence to collect |
| --- | --- | --- | --- |
| P0 | Transactional KV + applied-index updates and safe restart replay | Prevent partial/repeated non-idempotent mutation | Kill/restart at each application stage; INCR result agrees with acknowledged history. |
| P0 | Complete snapshot export/install + membership recovery | Preserve data when logs are compacted and followers catch up | Catch up an empty follower exclusively through a snapshot, then compare state/config at equal index. |
| P0 | Extend fault injection for delivered serialized/fail-closed runtime | Verify protocol acknowledgment order under real failures | Inject disk failures and concurrent inbound RPCs; no successful unsafe acknowledgment. |
| P1 | Validate delivered queue/waiter/transport bounds under load; add byte budgets | Count limits/pruning are delivered; large payloads still affect RSS | Saturated load/outage recovery with bounded RSS and clear backpressure. |
| P1 | Dedicated ordered storage worker and group fsync | Reduce timer stalls while retaining persistence ordering | p50/p95/p99 commit latency and election stability under slow disk. |
| P1 | Complete expiry semantics and efficient reverse expiry index | Avoid stale expiry records and full scans | Replacement/PERSIST/DEL/EXPIRE boundary tests and replicated time behavior. |
| P1 | Binary segment point-offset lookup and profiling of improved range reads | Range locking/search overhead is reduced; point lookup remains linear | Profile catch-up and recovery versus retained entries. |
| P1 | Cached bounded diagnostics and cursor pagination | Keep observability from dominating server work | Polling cost at 1K/100K/1M keys; API latency and scan counts. |
| P2 | TLS/mTLS, auth, ACLs and rate limits | Enable controlled network exposure | Unauthorized access denied; certificate rotation and permission tests. |
| P2 | Learners and bound membership administration | Add/remove voters safely online | Joint transition under leader failure and retries. |
| P2 | Reproducible benchmark/soak/fault suite | Detect regressions and document practical limits | Raw histories, seeds, hardware, exact commits and plots. |

## 9. Validation protocol

Run the following from the repository root and record outcomes with the exact revision:

```bash
cargo fmt --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
npm --prefix ui ci
npm --prefix ui run build
```

Confirmed local real-process runs with both four and five voters passed: election and leader discovery, 12 KB JSON values with Unicode/reserved-character keys, replication to every node, RESP quorum reads, bounded idle traffic (30/40 sends respectively in a 0.5-second sample), leader failover, isolated-leader read timeout, clean restart preserving an INCR result, replicated deletion and matching hashes, with no observed storage errors or leftover read waiters. Fail-closed storage behavior has focused runtime tests; real disk-fault integration remains to be added. These idle counts are narrow smoke observations, not performance benchmarks.

Final local verification passed all 53 Rust workspace tests, strict workspace Clippy (`-D warnings`), formatting, debug/release builds, 10 UI model tests and the Vite production build. The Rust suite includes ten focused server tests covering HTTP parsing, read gating, replay, log metadata and failed-storage behavior. Launcher regression tests passed three configuration/guard scenarios. The expanded simulator passed five scenarios including 1/3/4/5/7-node commit replication, dropped read-probe retries, isolated-leader read rejection and an even-cluster half partition failing to commit.

Final real-process verification additionally passed nonempty labeled Prometheus metrics, storage-health diagnostics, bounded retained-log metadata without command values, and rejection of writes with exactly half the voters in a four-node group. The localhost Vite gateway passed HTML delivery, identity-checked status routes to all five nodes, invalid-port HTTP 400 and unreachable-port HTTP 502 JSON responses. A fresh `.run-five` release demo was started with nine explicitly labelled demonstration records, replicated through its leader to every node. Single/three-voter real-process acceptance and broader network/disk fault schedules remain follow-up work.

The simulator's seed makes its own schedule replayable; it does not execute real storage or networking. The checker explores short completed histories and has factorial worst-case branching. `scripts/jepsen_run.sh` skips successfully when `JEPSEN_DIR` is unset; a green invocation is not a Jepsen pass.

Build and tests validate what they execute. Docker Compose and Helm deployment are separate checks; Helm CLI was unavailable, so rendering/live Kubernetes checks were not run. Hosted CI was not observed. No graphical browser/screenshot validation was performed; code/build/model checks are distinct from visual acceptance. Long-duration partition/crash behavior remains future work.

## 10. Navigation and maintenance

- [README.md](README.md): project overview and quick start.
- [HOW_TO_RUN.md](HOW_TO_RUN.md): ports, commands, dashboard setup and troubleshooting.
- [ARCHITECTURE.md](ARCHITECTURE.md): components, dataflow, persistence, complexity and security.
- [RAFT_NOTES.md](RAFT_NOTES.md): source map and consensus reasoning.
- [CHANGELOG.md](CHANGELOG.md): changes and unfinished integration work.

Keep this report synchronized with source. A feature is complete only when the end-to-end surface is bound, its failure behavior is defined, and relevant validation exists. Separate measured observations from derived values, source review from execution, and proposed architecture from delivered behavior.
