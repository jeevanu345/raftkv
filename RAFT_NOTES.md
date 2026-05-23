# Raft notes

A working glossary that maps the Ongaro & Ousterhout paper / thesis sections
to the code that implements them.

| Concept | Paper § | Code |
|---|---|---|
| RPC term-bump rule | §5.1 | `RaftNode::step` (universal pre-handler) |
| `currentTerm` / `votedFor` durability | §5.2 | `Action::PersistHardState` |
| Election timeout (randomized) | §5.2 | `reset_randomized_election_timeout` |
| Log Matching property | §5.3 | `handle_append_entries` `prev_log_*` check |
| Conflict-driven log truncation | §5.3 | `RaftLog::truncate_from`, follower path in `handle_append_entries` |
| Fast back-off (conflict_index/term) | thesis §3.5 | `(conflict_index, conflict_term)` in `handle_append_entries` reject branch |
| Commit-only-current-term (Figure 8) | §5.4.2 | `maybe_advance_commit` term check |
| State-machine safety (apply order) | §5.4.3 | `flush_apply` |
| Leader Completeness | §5.4 | enforced by §5.4.2 + `is_up_to_date` |
| Snapshot install / log compaction | §7 | `Action::InstallSnapshot`, `RaftLog::compact_through`, `SegmentedLog::compact` |
| Pre-vote | thesis §9.6 | `become_pre_candidate`, `pre_vote` flag in `RequestVote` |
| Leadership transfer | thesis §3.10 | `transfer_leadership`, `Message::TimeoutNow` |
| Joint consensus (C_old,new) | §6 / thesis §4 | `ConfigState::Joint`, `maybe_finish_joint` |
| Linearizable reads (read-index) | thesis §6.4 | `read_index` in core, runtime `handle_read` |

## Things to watch when reading the code

- The core never directly schedules sends — it pushes `Action::SendMessage`
  onto the `acts` stack the caller passes in. The runtime is the only actor
  that interacts with the network, the disk, or the clock.
- The runtime persists `HardState` *before* dispatching any outbound
  message that depends on it, by interleaving `Action::PersistHardState`
  ahead of the corresponding `Action::SendMessage` in the actions vector.
- Snapshots: the runtime supplies the snapshot bytes — `raft-core` only
  carries the metadata and an opaque `Bytes` blob that the runtime fills
  in at send time and consumes at install time.

## Known gaps versus the spec

- TLS / mTLS wiring is scaffolded in `tonic` deps but not turned on.
- Membership change RPCs are implemented at the core level; the gRPC
  membership service is defined but not yet bound to a runtime endpoint.
- `TTL key` returns `-1` while the TTL machinery (`Expire/Persist`) is
  fully implemented; surfacing remaining-seconds is on the punch list.
- Linearizability checker is exhaustive O(n!) — for Jepsen-scale histories
  feed to Knossos or Porcupine.
