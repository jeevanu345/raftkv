# Raft source study and invariant map

This is a reading guide to the implementation, not a claim of formal verification. Read the core together with its runtime action interpreter: correctness in memory depends on persistence and transport ordering outside the pure state machine.

## Source map

| Concept | Paper / thesis topic | Implementation entry point | Review question |
| --- | --- | --- | --- |
| Universal term rule | Raft §5.1 | `RaftNode::step`, `become_follower` | Are pre-votes excluded from term changes and stale messages rejected? |
| Persistent term and vote | §5.2 | `Action::PersistHardState`, `MetaStore::save_hard_state` | Does disk failure prevent a successful response? |
| Random election deadline | §5.2 | `reset_randomized_election_timeout`, `tick` | Can slow storage delay ticks or trigger repeated elections? |
| Candidate freshness | §5.4.1 | `handle_request_vote`, `is_up_to_date` | Is `(last_term, last_index)` compared lexicographically? |
| Log matching | §5.3 | `handle_append_entries`, `RaftLog::term_at` | Is only the first conflicting suffix replaced? |
| Conflict retry | Thesis log-repair optimization | `Progress::maybe_decr_to`, follower rejection hints | Does the next retry reduce `next_index` and preserve committed entries? |
| Current-term commitment | §5.4.2, Figure 8 | `maybe_advance_commit` | Is the chosen index replicated on a true majority, including even voter counts? |
| Ordered application | §5.4.3 | `flush_apply`, runtime `apply_entries` | Are no-op/config entries reflected in the applied cursor? |
| Read-index | Thesis §6.4 | `read_index`, response correlation, runtime pending reads | Is there a fresh current-term quorum, then an application barrier? |
| Pre-vote | Thesis §9.6 | `become_pre_candidate`, pre-vote request/response fields | Can an isolated node disrupt a healthy leader's term? |
| Leadership transfer | Thesis leadership transfer | `transfer_leadership`, `Message::TimeoutNow` | Is the target caught up and can transfer recover if it never wins? |
| Joint configuration | Raft §6 / thesis §4 | `ConfigState`, `propose_config_change`, `maybe_finish_joint` | Are old and new majorities both required until the transition is safely committed? |
| Snapshot boundary | §7 | `handle_install_snapshot`, `on_snapshot_taken`, storage snapshot store | Are KV bytes, configuration, and indexes recovered together? |

## State and indexes

- `current_term` is monotonic durable protocol time; it is unrelated to Unix time.
- `voted_for` constrains real votes in a term; pre-votes do not consume the durable vote.
- `last_log_index` is the local replicated-log tail, including uncommitted entries.
- `commit_index` is the highest position known committed by the local core.
- `last_applied` is the core's application scheduling cursor; the KV applied cursor tracks the state machine. Their agreement requires runtime integration.
- `match_index[p]` is the leader's acknowledged replication position for peer `p`; `next_index[p]` is the next position to send.
- A committed entry is not necessarily applied yet. An appended entry is not necessarily committed. Reachability is neither condition.

Normal stable-cluster progress should satisfy `applied ≤ commit ≤ last_log`; diagnose sustained gaps rather than treating a single asynchronous sample as a violation. Compare replicas only at common applied indexes.

## Majority reasoning

For a stable set of `N` voters, count `floor(N/2)+1` votes or replicas. Sorting ascending match indexes places the quorum replication point at `(N-1)/2` using zero-based indexing. This matters for even clusters: four voters require three acknowledgments, not two.

A leader may advance commit through a current-term entry by quorum replication. It may not independently count replicas of an older-term entry and commit it solely on that basis. The new leader's no-op provides a current-term barrier.

During joint consensus a quorum in the union alone is insufficient. Both `majority(C_old)` and `majority(C_new)` must hold. Adding a row to a browser or a peer to a TOML file does not execute this protocol.

## Read-index reasoning

A leader role cached in memory is insufficient after isolation: another partition may elect a new leader. A strong read needs leadership confirmation from the current configuration after read issuance, a current-term committed barrier, and local application through the confirmed index. Correlating acknowledgments prevents a previous heartbeat response from accidentally satisfying a new read. Leadership/term changes must fail outstanding reads.

Diagnostics such as status, events and log previews can deliberately be local observations. Their consistency must be labeled separately from the KV read path.

## Determinism and the hash

`raft-core` is deterministic for an identical input schedule and seed. KV expiration becomes deterministic only when time is supplied through replicated commands; independent wall-clock eviction on followers would diverge.

The SHA-256 value in KV is a rolling operation-history digest, not a canonical Merkle root over the complete keyspace. It helps compare matching applied histories, but equal values are not a proof of identical TTL metadata or complete database correctness. `FLUSHDB` resets the digest; some metadata changes are not included in it.

## Important remaining boundaries

- Core configuration-change and transfer entry points exist; the membership/admin protobuf services are not a complete bound operational workflow.
- Snapshot file primitives exist, but the runtime does not yet provide complete dataset export/import and reliable snapshot recovery. Do not compact valuable history on the assumption that an empty snapshot is a backup.
- KV application spans several sled trees without a transaction; persisting an applied cursor last is insufficient to make `INCR` safe against partial application/replay.
- Expiry commands and a sorted TTL index exist, but `TTL` is a placeholder returning `-1`, expiry replacement has incomplete Redis semantics, and periodic replicated expiry ticks are not scheduled by the current server.
- The deterministic simulator primarily exercises the pure core and simulated messages; it does not execute real disk writes, OS crashes or gRPC connections.
- The in-process checker accepts only a small completed-operation model. It does not validate TTL, membership, snapshot recovery, or unknown timeout outcomes.

Use [ARCHITECTURE.md](ARCHITECTURE.md) for data flow and [PROJECT_REPORT.md](PROJECT_REPORT.md) for the evidence-based roadmap.
