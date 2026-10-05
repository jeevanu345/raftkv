# Raft notes

These notes explain Raft from first principles, then map each idea to this
repository. They are meant to be useful in three modes:

- Study mode: learn what Raft is and why it works.
- Code-reading mode: know exactly where a concept appears in `raftkv`.
- Implementation mode: know the invariants that must stay true when changing
  the code.

The project is a Raft-replicated key-value store written in Rust. Clients speak
a Redis-compatible RESP protocol. Nodes speak gRPC to each other. The key design
choice is that the consensus core is pure: it does not do disk I/O, network I/O,
async work, sleep, read clocks, or use operating-system randomness. It consumes
messages and ticks, mutates in-memory state, and emits actions for the runtime
to execute.

## Table of contents

- The shortest possible mental model
- What Raft solves
- Key terms
- Repository map
- The pure/impure split
- Core data structures
- Messages and actions
- Node startup
- Time, ticks, and randomized election timeouts
- Terms and role transitions
- Leader election
- Pre-vote
- Log replication
- Commit and apply
- Linearizable reads
- Client write path
- Client read path
- Membership changes and joint consensus
- Leadership transfer
- Snapshots and compaction
- Persistent storage
- State machine
- RESP server
- gRPC network layer
- Deterministic simulator
- Linearizability checker
- Build, run, and operate
- Safety invariants
- Scenario walkthroughs
- Code map
- Current gaps and production hardening checklist

## The shortest possible mental model

Raft turns a group of unreliable machines into one reliable replicated state
machine.

Each client write becomes a log entry. The leader appends the entry to its own
log, replicates it to followers, waits until a quorum stores it, marks it
committed, and applies it to the key-value state machine. Followers apply the
same committed entries in the same order, so they reach the same state.

Only one leader should exist in a term. Terms are logical epochs. If a node sees
a higher term, it steps down. Elections choose the node with the most up-to-date
log, which prevents a leader that is missing committed data from overwriting it.

In this repository:

- `raft-core` is the pure Raft brain.
- `raftkv-server` is the runtime that drives the core and executes side effects.
- `raft-storage` stores logs, hard state, and snapshots.
- `raft-net` moves Raft RPCs over gRPC.
- `kv-state-machine` applies committed commands.
- `resp-server` accepts Redis-style client commands.
- `sim-tests` runs deterministic cluster simulations.

## What Raft solves

Raft solves consensus for replicated state machines.

The problem:

- There are multiple servers.
- Servers can crash, restart, lag, or lose network connectivity.
- Clients still want one coherent key-value store.
- All non-faulty replicas must apply the same commands in the same order.
- Once a command is committed, future leaders must not lose it.

Raft's answer:

- Elect one leader.
- Route writes through that leader.
- Store every write in a replicated log.
- Commit an entry only after a quorum has stored it.
- Use term and log freshness rules so future leaders contain committed entries.

Fault tolerance rule:

- A cluster with `2f + 1` voting nodes tolerates `f` failed voting nodes.
- A 3-node cluster tolerates 1 failed node.
- A 5-node cluster tolerates 2 failed nodes.
- A quorum is any majority of the current voting configuration.

Safety versus liveness:

- Safety means Raft never returns contradictory committed results.
- Liveness means the cluster can keep making progress.
- Raft prioritizes safety. During severe partitions, the minority side stops
  committing rather than risk divergence.

## Key terms

Node or server:

One process participating in the Raft cluster.

Voter:

A node that counts toward elections and commit quorums.

Learner:

A non-voting replica. The code has an `is_learner` field in `Progress`, but the
main membership model currently treats configured peers as voters.

Term:

A monotonically increasing logical epoch. Elections happen in terms. A node that
sees a higher term updates `current_term`, clears `voted_for`, and becomes a
follower.

Follower:

Default role. It accepts leader replication and vote requests. It starts an
election if it does not hear from a leader before its election timeout.

PreCandidate:

Role used by the pre-vote extension. A node asks whether an election would
probably succeed before it increments its real term.

Candidate:

A node that has started a real election, incremented its term, voted for itself,
and asked other voters for votes.

Leader:

The node that accepts client writes and replicates log entries to followers.

Log:

An ordered sequence of entries. Each entry has an index and term.

Log index:

The 1-based position of an entry in the log. Index 0 is a sentinel for "before
the first entry".

Entry term:

The leader term in which the entry was created.

Commit index:

The highest log index known to be committed. A committed entry is safe to apply
to the state machine.

Last applied:

The highest committed entry that this node has already applied to its local
state machine.

Hard state:

Persistent Raft metadata: `current_term`, `voted_for`, and `commit_index`.

Soft state:

Volatile state useful for runtime and observability: role and leader hint.

State machine:

The deterministic key-value engine. It applies committed commands in log order.

Quorum:

A majority of voters. In joint consensus, quorum means a majority of the old
configuration and a majority of the new configuration.

Snapshot:

A compact representation of state through some log index. Entries covered by a
snapshot can be removed from the live log.

## Repository map

```text
raftkv/
+-- crates/
|   +-- raft-core/
|   |   Pure Raft state machine. No I/O, no async, no clock, no OS RNG.
|   +-- raft-storage/
|   |   Segmented log, sled-backed metadata, snapshot files.
|   +-- raft-net/
|   |   tonic gRPC transport and protobuf conversion.
|   +-- kv-state-machine/
|   |   sled-backed deterministic key-value state machine.
|   +-- resp-server/
|   |   RESP codec, command parser, client command dispatch.
|   +-- raftkv-server/
|   |   Binary and runtime that wires core, storage, network, and KV together.
|   +-- raftkv-cli/
|   |   Smoke-test/operator CLI.
|   +-- linearizability-checker/
|   |   Exhaustive checker for short histories.
|   +-- sim-tests/
|       Deterministic synchronous simulation harness.
+-- proto/
|   gRPC service definitions.
+-- deploy/
|   Example node configs for containers.
+-- helm/raftkv/
|   Kubernetes chart.
+-- grafana/
|   Dashboard and alert rules.
+-- scripts/
    Local cluster helpers.
```

## The pure/impure split

This is the central design decision.

`raft-core`:

- Stores Raft state in memory.
- Accepts `Message` values from peers.
- Accepts logical `tick()` events from the runtime.
- Accepts local API calls such as `propose`, `read_index`, and
  `transfer_leadership`.
- Emits `Action` values.
- Does not touch disk, network, tasks, timers, or wall-clock time.

`raftkv-server` runtime:

- Opens persistent stores.
- Reconstructs core state on startup.
- Calls `tick()` on a fixed interval.
- Calls `step(message)` when network messages arrive.
- Calls `propose(command)` when clients write.
- Executes every emitted `Action`.
- Applies committed entries to `kv-state-machine`.
- Sends peer messages through `raft-net`.
- Persists hard state and log entries through `raft-storage`.

The main benefit is reproducibility. If two test runs feed the same seed, ticks,
and message sequence into `raft-core`, the core emits the same action sequence.
That makes deterministic simulation possible.

Important rule:

The runtime must persist any hard-state or log change before sending an RPC
response that depends on that change. This is how Raft's durability invariant is
kept outside the pure core.

## Core data structures

### `RaftNode`

Defined in `crates/raft-core/src/node.rs`.

Important fields:

- `cfg`: local Raft configuration.
- `hs`: persisted `HardState`.
- `log`: in-memory `RaftLog`.
- `last_applied`: highest applied entry.
- `role`: follower, precandidate, candidate, or leader.
- `leader_id`: best-known leader in the current term.
- `config`: current cluster configuration.
- `progress`: leader-side replication progress per peer.
- `votes`: votes collected during pre-vote or election.
- `elapsed_election_ticks`: ticks since hearing from a valid leader.
- `elapsed_heartbeat_ticks`: ticks since last leader heartbeat broadcast.
- `randomized_election_timeout`: current randomized election timeout.
- `rng`: deterministic ChaCha20 PRNG seeded from config.
- `next_request_id`: request correlation counter.
- `leader_transfer_target`: pending leadership transfer target.
- `in_joint`: whether a joint consensus transition is in progress.

### `HardState`

Defined in `crates/raft-core/src/state.rs`.

Fields:

- `current_term`: latest term this node has seen.
- `voted_for`: candidate that received this node's vote in this term.
- `commit_index`: highest known committed log index.

`current_term` and `voted_for` are essential for safety. If they are not
persisted before responding, a node could vote twice in the same term after a
crash. `commit_index` is persisted here for faster restart, although Raft safety
does not require it to be persisted.

### `SoftState`

Defined in `crates/raft-core/src/state.rs`.

Fields:

- `role`
- `leader_id`

Soft state is not safety-critical. It is used for status output, metrics, and
client hints.

### `RaftLog`

Defined in `crates/raft-core/src/log.rs`.

Fields:

- `entries`: live entries after the snapshot point.
- `snapshot_last_index`: highest index included in the current snapshot.
- `snapshot_last_term`: term at `snapshot_last_index`.

Important methods:

- `last_index()`: last live entry index, or snapshot index if no live entries.
- `last_term()`: term at `last_index()`.
- `first_index()`: first live index, equal to `snapshot_last_index + 1`.
- `term_at(index)`: term lookup if the index is known.
- `slice(from, to)`: live entries in `[from, to)`.
- `append(entries)`: append contiguous entries.
- `truncate_from(from)`: remove entries with index >= `from`.
- `compact_through(index, term)`: discard entries covered by a snapshot.
- `is_up_to_date(index, term)`: RequestVote log freshness check.

Invariants:

- Log indexes are contiguous in the live portion.
- Entry indexes are monotonic.
- Entry terms are non-decreasing along a leader-created log.
- The snapshot point acts as the base of the live log.

### `Entry`

Defined in `crates/raft-core/src/log.rs`.

Fields:

- `term`
- `index`
- `kind`
- `data`

Entry kinds:

- `Noop`: appended by a new leader to commit prior-term entries safely.
- `Normal`: a state-machine command.
- `ConfigJoint`: joint consensus configuration entry.
- `ConfigNew`: final stable configuration entry.

## Messages and actions

### Messages

Defined in `crates/raft-core/src/message.rs`.

`Message` is the alphabet consumed by the pure core:

| Message | Meaning |
|---|---|
| `AppendEntries` | Leader replication request or heartbeat |
| `AppendEntriesResponse` | Follower response to replication |
| `RequestVote` | Candidate vote request, also used for pre-vote |
| `RequestVoteResponse` | Vote or pre-vote response |
| `InstallSnapshot` | Leader sends snapshot to lagging follower |
| `InstallSnapshotResponse` | Follower acknowledges snapshot |
| `TimeoutNow` | Leadership transfer trigger |

These messages are independent of protobuf. The network crate converts between
`Message` and `proto/raft.proto`.

### Actions

Defined in `crates/raft-core/src/action.rs`.

`Action` is how the pure core asks the runtime to perform side effects:

| Action | Runtime responsibility |
|---|---|
| `PersistHardState` | Save `current_term`, `voted_for`, `commit_index` |
| `AppendEntries` | Append entries to durable log storage |
| `TruncateLog` | Truncate durable log from an index |
| `SendMessage` | Send a Raft message to a peer |
| `ApplyCommitted` | Apply committed entries to the state machine |
| `TakeSnapshot` | Create a local snapshot |
| `InstallSnapshot` | Install a received snapshot |
| `ResetElectionTimer` | Runtime hook for timers |
| `ResetHeartbeatTimer` | Runtime hook for timers |
| `NotifyReadIndex` | Notify read-index progress |
| `BecameLeader` | Runtime hook for logs and metrics |
| `BecameFollower` | Runtime hook for logs and metrics |
| `Metric` | Emit observability event |

The runtime interprets these in `crates/raftkv-server/src/runtime.rs`.

## Node startup

Startup happens in `Runtime::new`.

Sequence:

1. Create the node data directory.
2. Open the segmented Raft log at `data_dir/log`.
3. Open the metadata store at `data_dir/meta`.
4. Open the snapshot store at `data_dir/snapshots`.
5. Open the key-value state machine at `data_dir/kv`.
6. Load `HardState` from metadata.
7. Load the snapshot pointer from metadata.
8. Construct `RaftLog::from_snapshot(snapshot_index, snapshot_term)`.
9. Read durable log entries after the snapshot pointer.
10. Append those entries into the in-memory `RaftLog`.
11. Construct `RaftNode` with `RaftConfig`, `HardState`, and `RaftLog`.
12. Create proposal, read, and network inbox channels.
13. Create one outbound `PeerClient` per remote peer.
14. Return the runtime plus handles for the run loop.

The runtime loop is started by `Runtime::spawn`.

The binary entry point in `crates/raftkv-server/src/main.rs` then starts:

- the runtime loop,
- the gRPC Raft server,
- the RESP client server,
- the Prometheus text endpoint.

## Time, ticks, and randomized election timeouts

The core never reads the clock. Time arrives as `tick()` calls.

Runtime configuration:

- `tick_ms`: wall-clock duration per logical tick.
- `election_timeout_ms`: low end of the election timeout.
- `heartbeat_ms`: leader heartbeat interval.

Core configuration:

- `election_timeout_ticks`
- `heartbeat_ticks`
- `rng_seed`

Election timeout randomization:

```text
randomized_election_timeout in [election_timeout_ticks, 2 * election_timeout_ticks)
```

Why randomization matters:

- If all followers timed out at exactly the same time, they would often start
  elections together.
- Concurrent candidates can split votes.
- Random timeouts make one node likely to start first and win.

`tick()` behavior:

- Leader: increment heartbeat ticks; broadcast heartbeats when due.
- Follower, Candidate, PreCandidate: increment election ticks; start election
  when randomized timeout elapses.

## Terms and role transitions

Terms are the backbone of Raft safety.

Universal term-bump rule:

- Implemented at the start of `RaftNode::step`.
- If an incoming non-pre-vote RPC has a term greater than `current_term`, the
  node becomes a follower in that term.
- `current_term` is updated.
- `voted_for` is cleared.
- hard state is persisted.

Pre-vote messages intentionally do not bump terms.

Main transitions:

```text
Follower --election timeout--> PreCandidate
PreCandidate --pre-vote quorum--> Candidate
Candidate --vote quorum--> Leader
Leader --higher term observed--> Follower
Candidate --higher term observed--> Follower
PreCandidate --valid leader or higher term observed--> Follower
Follower --TimeoutNow--> Candidate/PreCandidate election path
```

`become_follower(term, leader)`:

- Updates term if needed.
- Clears vote on term change.
- Sets role to follower.
- Records leader hint.
- Clears votes.
- Resets election and heartbeat tick counters.
- Clears leadership transfer target.
- Randomizes election timeout.
- emits `PersistHardState`, `ResetElectionTimer`, and `BecameFollower`.

`become_candidate()`:

- Increments `current_term`.
- Votes for self.
- Sets role to candidate.
- Clears leader hint.
- Persists hard state.
- Emits election metrics.

`become_leader()`:

- Sets role to leader.
- Sets leader id to self.
- Initializes `Progress` for voters.
- Appends a no-op entry in the new term.
- Persists the no-op through `Action::AppendEntries`.
- Broadcasts replication.

The no-op is important because a leader can only commit entries from its current
term by counting replicas. Once the current-term no-op is committed, older
entries before it become committed as a consequence.

## Leader election

Raft elections enforce Election Safety: at most one leader can be elected in a
given term.

Election flow with pre-vote enabled:

1. Follower times out.
2. It becomes `PreCandidate`.
3. It asks voters for pre-votes for `current_term + 1`.
4. Receivers do not change term for pre-vote.
5. If a quorum grants pre-votes, it becomes `Candidate`.
6. It increments `current_term`.
7. It votes for itself.
8. It persists hard state.
9. It sends real `RequestVote` RPCs.
10. If a quorum grants votes, it becomes leader.
11. It appends a no-op entry and starts replication.

Vote granting rules:

- A node rejects stale terms.
- A node grants at most one real vote per term.
- A node grants only if the candidate log is at least as up-to-date as its own.

The log freshness check is in `RaftLog::is_up_to_date`:

```text
candidate is up-to-date if:
  candidate last term > our last term
  OR candidate last term == our last term AND candidate last index >= our last index
```

This rule is what protects committed entries during elections. A candidate that
is missing a committed entry cannot gather a quorum if that committed entry was
stored on a quorum.

Real vote persistence:

- When granting a real vote, the follower sets `voted_for = candidate_id`.
- It emits `Action::PersistHardState`.
- The runtime must persist before the vote response is sent.

Split vote behavior:

- If multiple candidates start together, no one may get a quorum.
- Each candidate eventually times out again.
- Randomized timeouts make a clean winner likely on a later attempt.

## Pre-vote

Pre-vote is an extension from the Ongaro thesis.

Problem it solves:

- An isolated node can time out repeatedly.
- Without pre-vote, it increments terms even though it cannot win.
- When it reconnects, its higher term can force a healthy leader to step down.
- This causes avoidable disruption.

Pre-vote behavior:

- The node asks: "Would you vote for me in the next term?"
- Receivers do not update `current_term`.
- Receivers do not update `voted_for`.
- A receiver rejects pre-vote if it still has an active leader lease.
- Only after pre-vote quorum does the node start a real election.

Code:

- `RaftConfig.pre_vote`
- `Role::PreCandidate`
- `become_pre_candidate`
- `RequestVote { pre_vote: true }`
- `handle_request_vote`
- `handle_vote_response`

Default:

- `ServerConfig.pre_vote = true`
- `RaftConfig.pre_vote = cfg.pre_vote`

## Log replication

The leader is the only node that accepts normal client proposals.

`RaftNode::propose(data)`:

1. Reject if not leader.
2. Reject if leadership transfer is in progress.
3. Create `Entry::normal(current_term, last_index + 1, data)`.
4. Append it to the in-memory log.
5. Emit `Action::AppendEntries` so the runtime stores it durably.
6. Update self progress.
7. Try to advance commit.
8. Broadcast replication to followers.

Leader-side progress:

Each follower has a `Progress`:

- `next_index`: next entry to send.
- `match_index`: highest entry known replicated on that follower.
- `state`: `Probe`, `Replicate`, or `Snapshot`.
- `recent_active`: whether the peer has recently responded.
- `inflight`: number of outstanding AppendEntries.
- `pending_snapshot`: snapshot being sent, if any.

Progress states:

- `Probe`: cautious one-at-a-time sends while finding the matching log point.
- `Replicate`: pipelined AppendEntries after a successful match.
- `Snapshot`: waiting for InstallSnapshot to finish.

`send_append(peer, force_heartbeat)`:

1. Check peer progress and inflight limit.
2. If follower is too far behind, send `InstallSnapshot`.
3. Compute `prev_log_index = next_index - 1`.
4. Compute `prev_log_term`.
5. If heartbeat, send no entries.
6. Otherwise slice up to `max_append_entries`.
7. Increment inflight.
8. Send `Message::AppendEntries`.

Follower-side AppendEntries logic:

1. Reject if leader term is stale.
2. Recognize the sender as leader for the term.
3. Reset election timer.
4. Check `prev_log_index` and `prev_log_term`.
5. If the previous entry does not match, reject with conflict hints.
6. If previous entry matches, compare incoming entries with local entries.
7. On first conflict, truncate local log from the conflict index.
8. Append new entries from that point.
9. Advance commit index to `min(leader_commit, last_log_index)`.
10. Apply newly committed entries.
11. Reply success.

The Log Matching property:

If two logs contain an entry with the same index and term, then all preceding
entries are identical. The `prev_log_index` and `prev_log_term` check enforces
this before a follower accepts new entries.

Conflict handling:

- Follower returns `conflict_index` and `conflict_term`.
- The leader currently backs off using `conflict_index` through
  `Progress::maybe_decr_to`.
- The message type carries `conflict_term`, but the current leader response
  handler does not use it for full term-based fast backoff.

Leader Append-Only property:

- Leaders append to their own logs.
- Followers may truncate conflicting uncommitted entries.
- A leader does not overwrite its own entries.

## Commit and apply

Commit means an entry is durable enough that it will survive future leader
changes.

Leader commit rule:

- For a stable configuration, find the highest index replicated on a majority.
- Only advance commit to that index if the entry at that index is from the
  leader's current term.

This "current term only" rule is the Figure 8 caveat from the Raft paper. It
prevents a leader from incorrectly committing an old-term entry solely by
counting replicas.

Implementation:

- `maybe_advance_commit`
- `Progress.match_index`
- `RaftLog::term_at`
- `Action::PersistHardState`
- `flush_apply`

Follower commit rule:

- Follower receives `leader_commit`.
- It sets commit index to `min(leader_commit, local_last_log_index)`.
- Then it applies entries in order.

Apply rule:

```text
apply entries from last_applied + 1 through commit_index, in order
```

Implementation:

- `flush_apply`
- Emits `Action::ApplyCommitted { entries }`
- Runtime calls `apply_entries`
- Normal entries are decoded as `kv_state_machine::Command`
- Config entries produce `Response::Ok`
- No-op entries produce `Response::Ok`

Client proposal completion:

- Runtime records pending proposals by expected log index.
- When `apply_entries` applies that index, it sends the state-machine response
  back to the waiting client task.

## Linearizable reads

Why reads need care:

- A follower may be stale.
- An old leader may still think it is leader during a partition.
- A local read is safe only after leadership is confirmed and the state machine
  has applied all entries that were committed before the read.

Standard Raft read-index idea:

1. Leader records its current commit index.
2. Leader confirms it is still leader by contacting a quorum, usually with
   heartbeat acknowledgements.
3. Leader waits until local `applied_index >= recorded_commit_index`.
4. Leader serves the read from local state.

Intent in this repository:

- `RaftNode::read_index(ctx)` emits heartbeats and `Action::NotifyReadIndex`.
- `Runtime::handle_read` stores a pending read with a commit target.
- `Runtime::drain_ready_reads` serves the read after the state machine has
  applied at least that target.

Important implementation note:

The code is shaped around read-index, but the current runtime does not maintain
a per-read quorum-ack set correlated with heartbeat responses. It gates reads on
leader role and `applied_index >= commit_target`. Before treating this as a
production-grade read-index implementation, add explicit quorum confirmation for
each read round or use a lease-read design with well-defined clock assumptions.

Code:

- `RaftNode::read_index`
- `Action::NotifyReadIndex`
- `Runtime::handle_read`
- `Runtime::drain_ready_reads`
- `CommandHandler::linearizable_read`

## Client write path

Example:

```text
redis-cli SET hello world
```

Path:

1. TCP client connects to `resp-server`.
2. `RespCodec` decodes RESP into `RespFrame`.
3. `commands::parse` converts the frame to `ClientCommand::Set`.
4. `handler::dispatch` builds `kv_state_machine::Command::Set`.
5. `ClientHandler::propose` sends a proposal to the runtime.
6. Runtime serializes the command with bincode.
7. Runtime calls `RaftNode::propose`.
8. Core appends a log entry and emits actions.
9. Runtime appends the entry to durable segmented log.
10. Runtime sends AppendEntries to peers.
11. Followers validate, persist, and respond.
12. Leader updates progress from responses.
13. Leader advances commit after quorum.
14. Runtime applies committed entry to `KvStateMachine`.
15. Pending proposal waiter receives the response.
16. RESP server encodes the response back to the client.

Follower write behavior:

- `RaftNode::propose` returns `NotLeader { hint }`.
- Runtime maps the hint to a peer address when possible.
- RESP replies use a Redis-style `MOVED 0 <hint>` error.

## Client read path

Example:

```text
redis-cli GET hello
```

Path:

1. TCP client connects to `resp-server`.
2. `RespCodec` decodes the command.
3. `commands::parse` returns `ClientCommand::Get`.
4. `handler::dispatch` calls `linearizable_read`.
5. Runtime checks whether local node is leader.
6. If not leader, return a redirect/error.
7. If leader, enqueue pending read with current commit target.
8. Runtime asks the core to begin read-index flow.
9. Runtime waits until the local state machine has applied the target index.
10. Runtime executes the read closure against `KvStateMachine`.
11. RESP response is returned to the client.

Read commands:

- `GET`
- `MGET`
- `EXISTS`
- `DBSIZE`
- `INFO`
- `CLUSTER NODES`
- `CLUSTER INFO`

`INFO` and cluster metadata are served from runtime status rather than the KV
state machine.

## Membership changes and joint consensus

Changing cluster membership is dangerous because two different configurations
could each think they have a majority. Raft solves this with joint consensus.

Transition:

```text
C_old -> C_old,new -> C_new
```

In `C_old,new`, an entry is committed only if it has quorum in both:

- the old voter set,
- the new voter set.

Code:

- `ConfigState::Stable`
- `ConfigState::Joint`
- `ConfigState::has_quorum`
- `ConfigChange::AddServer`
- `ConfigChange::RemoveServer`
- `RaftNode::propose_config_change`
- `apply_config_entry`
- `maybe_finish_joint`
- `maybe_advance_commit`

Flow:

1. Leader receives a config change request.
2. It rejects if not leader.
3. It rejects if already in joint consensus.
4. It computes the new voter set.
5. It appends a `ConfigJoint` entry.
6. It applies the config entry to the core's config state.
7. Replication proceeds.
8. Commit while joint requires quorum in both old and new sets.
9. Once `ConfigJoint` commits, leader appends `ConfigNew`.
10. After `ConfigNew`, the cluster is stable in the new configuration.

Runtime/API status:

- Core-side membership transitions are implemented.
- `proto/membership.proto` defines AddServer, RemoveServer, and
  TransferLeadership services.
- The gRPC membership service is not currently bound to a runtime endpoint.

## Leadership transfer

Leadership transfer is a graceful handoff from the current leader to another
voter.

Goal:

- Move leadership without waiting for a random election timeout.
- Prefer a target that is already caught up.

Code:

- `RaftNode::transfer_leadership(target)`
- `leader_transfer_target`
- `Message::TimeoutNow`
- `handle_append_entries_response`

Flow:

1. Current leader receives a transfer request.
2. It rejects if not leader, if target is self, or if target is not a voter.
3. It records the transfer target.
4. It blocks new proposals while transfer is in progress.
5. If target is caught up, send `TimeoutNow`.
6. If target is not caught up, continue replication.
7. When target catches up, send `TimeoutNow`.
8. Target starts an election promptly.
9. Since target is up-to-date, it should be able to win.

Production notes:

- A complete operator API should expose this through the membership/admin
  service.
- Transfers should have timeout/cancellation behavior.

## Snapshots and compaction

Raft logs cannot grow forever. Snapshots let a node compact old entries after
their effects are represented in state-machine state.

Snapshot concepts:

- `last_included_index`: highest log index covered by the snapshot.
- `last_included_term`: term at that index.
- snapshot data: encoded state-machine state and configuration.
- live log starts after `last_included_index`.

Core log support:

- `RaftLog::from_snapshot`
- `RaftLog::compact_through`
- `RaftLog::first_index`
- `RaftLog::snapshot_index`
- `RaftLog::snapshot_term`

Leader snapshot send:

- In `send_append`, if follower `next_index < log.first_index()`, the leader
  cannot send the missing entries.
- It moves peer progress to `Snapshot`.
- It emits `Message::InstallSnapshot`.

Follower snapshot install:

- Reject stale term.
- Become follower for valid leader.
- Ignore if snapshot is older than current snapshot point.
- Compact local in-memory log through snapshot index.
- Set `last_applied` to snapshot index.
- Advance commit index if needed.
- Emit `Action::InstallSnapshot`.
- Reply with `InstallSnapshotResponse`.

Storage snapshot support:

- `SnapshotStore::begin_write`
- `SnapshotWriter::write_chunk`
- `SnapshotWriter::finish`
- temp file plus atomic rename
- metadata sidecar file
- `keep_last(N)` pruning

Current implementation notes:

- `Action::TakeSnapshot` exists and runtime can write a snapshot shell.
- `snapshot_entries_threshold` is configured but not currently used to trigger
  snapshots automatically.
- Runtime `take_snapshot` currently writes metadata and an empty snapshot body.
- Runtime `InstallSnapshot` updates snapshot pointers and log state, but does
  not restore KV state from snapshot bytes.
- `raft-net` supports streaming `InstallSnapshot` at the proto level, but the
  current client sends one logical chunk.

These pieces are good scaffolding, but production snapshotting still needs full
state-machine serialization, restore, trigger policy, and chunked transfer.

## Persistent storage

Persistent storage is in `crates/raft-storage`.

### Segmented log

Files:

```text
data_dir/log/00000000000000000001.log
data_dir/log/00000000000000012345.log
```

The filename is the base index of the segment.

Record format:

```text
[u32 little-endian payload length]
[u32 little-endian crc32c(payload)]
[payload: bincode-encoded raft_core::Entry]
```

Segment recovery:

1. Read segment bytes.
2. Walk records from the beginning.
3. Stop at the first incomplete record, bad CRC, or decode failure.
4. Truncate the file to the last good offset.
5. Rebuild the in-memory index of entry offsets.

Important methods:

- `SegmentedLog::open`
- `SegmentedLog::append`
- `SegmentedLog::read`
- `SegmentedLog::read_range`
- `SegmentedLog::truncate_from`
- `SegmentedLog::compact`
- `SegmentedLog::set_snapshot_tip`

Durability mode:

- `sync_each_append = false` in runtime config for grouped sync behavior.
- `append` syncs the active segment at the end of a batch.
- `MetaStore::save_hard_state` flushes sled.

### MetaStore

Sled-backed metadata in `crates/raft-storage/src/meta.rs`.

Keys:

- `hs`: bincode-encoded `HardState`.
- `snap_index`: last included snapshot index.
- `snap_term`: snapshot term.

Methods:

- `save_hard_state`
- `load_hard_state`
- `save_snapshot_pointer`
- `load_snapshot_pointer`

### SnapshotStore

Snapshot files live under `data_dir/snapshots`.

Properties:

- snapshot body written to `.snap.tmp`,
- metadata written to `.meta.tmp`,
- both installed with rename,
- old snapshots pruned by `keep_last`.

## State machine

The state machine is in `crates/kv-state-machine`.

It is deterministic: every replica that applies the same command sequence at the
same log indexes should end in the same state.

Sled trees:

- `kv`: key to value.
- `ttl`: sorted TTL index, `expire_at_ms_be || key` to empty value.
- `meta`: applied index and state hash.

Commands:

- `Set`
- `Del`
- `Incr`
- `Expire`
- `Persist`
- `MSet`
- `FlushDb`
- `Tick`

Responses:

- `Ok`
- `Int`
- `Bulk`
- `Array`
- `Error`

Apply behavior:

- `Set`: insert value, optionally add TTL index entry.
- `Del`: remove keys.
- `Incr`: parse current integer or default to 0, saturating add delta.
- `Expire`: add TTL index if key exists.
- `Persist`: remove TTL index entries for key.
- `MSet`: set multiple pairs.
- `FlushDb`: clear KV and TTL trees.
- `Tick`: evict expired keys at the deterministic timestamp in the command.

Determinism details:

- TTL commands store absolute expiry timestamps in the replicated command.
- The state machine never calls wall-clock time while applying.
- The rolling state hash changes with every mutation.
- `applied_index` is persisted after applying the command.

Current TTL notes:

- RESP `TTL key` returns `-1`.
- TTL data structures and `Expire`/`Persist` exist.
- Automatic replicated `Tick` proposal is not currently wired in the runtime.

## RESP server

The RESP layer is in `crates/resp-server`.

Main files:

- `codec.rs`: RESP2 frame decoder/encoder.
- `commands.rs`: parse RESP arrays into `ClientCommand`.
- `handler.rs`: dispatch client commands into reads or proposals.
- `server.rs`: Tokio TCP listener and per-connection task.

Supported command surface:

- `PING`
- `ECHO`
- `QUIT`
- `SELECT`
- `COMMAND`
- `CLIENT`
- `INFO`
- `CLUSTER NODES`
- `CLUSTER INFO`
- `DBSIZE`
- `GET`
- `MGET`
- `EXISTS`
- `SET key value [EX seconds]`
- `DEL`
- `INCR`
- `DECR`
- `MSET`
- `EXPIRE`
- `TTL`
- `PERSIST`
- `FLUSHDB`

Read commands are routed through `linearizable_read` when they depend on KV
state. Write commands are serialized and proposed to Raft.

RESP frame types:

- Simple string
- Error
- Integer
- Bulk string
- Array

The codec also supports inline commands such as:

```text
PING
SET k v
```

## gRPC network layer

The network layer is in `crates/raft-net`.

Proto file:

- `proto/raft.proto`

Services:

- `AppendEntries`
- `RequestVote`
- `PreVote`
- `InstallSnapshot`
- `TimeoutNow`

`PeerClient`:

- Lazily connects to a peer endpoint.
- Sends outbound core messages as gRPC calls.
- Converts synchronous gRPC responses back into `raft_core::Message`.
- Delivers responses into the local runtime inbox.
- Drops and reconnects the channel on RPC failure.

`RaftServer`:

- Receives gRPC calls.
- Converts protobuf requests into core `Message` values.
- Calls the runtime's `MessageProcessor`.
- Extracts the synchronous response message and returns it on the same gRPC
  call.

This is an important mapping:

- Raft RPCs have one request and one response.
- The response should go back on the same RPC call.
- Other side effects are executed by the runtime.

Proto conversion:

- `convert.rs` maps core entries and messages to protobuf and back.
- Request IDs are encoded as little-endian bytes.

## Deterministic simulator

The simulator is in `crates/sim-tests`.

Purpose:

- Test consensus behavior without async scheduling nondeterminism.
- Reproduce failures from one seed.
- Model network delay, drops, reordering, and partitions.

Simulation model:

- Each simulated node owns a `RaftNode`.
- The network is a list of in-flight messages with delivery ticks.
- `Sim::step` advances logical time.
- Each node ticks.
- Due messages are delivered.
- Actions are dispatched into the simulated network.

Key properties:

- No real I/O.
- No Tokio.
- No wall-clock time.
- PRNG is deterministic.

Existing tests:

- Three-node cluster elects a leader.
- Deterministic replay produces the same leader sequence for the same seed.

Useful future tests:

- Leader crash and recovery.
- Minority partition cannot commit.
- Majority partition elects a new leader.
- Log matching after partition heal.
- Committed entries survive leader changes.
- Snapshot install catches up a lagging follower.
- Joint consensus transition under failures.

## Linearizability checker

The checker is in `crates/linearizability-checker`.

It implements a Wing-and-Gong style exhaustive search for short histories.

Input:

- operations with invocation and response timestamps,
- operation kind,
- observed output.

Supported operation model:

- `Get`
- `Set`
- `Del`
- `Incr`

Output:

- `Verdict::Linearizable`
- `Verdict::NotLinearizable`

How it works:

- Respect real-time ordering for non-overlapping operations.
- For concurrent operations, try all valid serializations.
- Apply each candidate operation to a simple model.
- Accept if any serialization explains all observed results.

Limits:

- Exhaustive search is exponential.
- It is appropriate for short integration traces.
- For Jepsen-scale histories, use Knossos or Porcupine.

## Build, run, and operate

Build:

```bash
cargo build --workspace --release
```

Test:

```bash
cargo test --workspace
```

Local 3-node cluster:

```bash
./scripts/run_cluster.sh
```

Default local ports:

| Node | Raft gRPC | RESP client | Metrics |
|---|---:|---:|---:|
| 1 | 7001 | 6379 | 9101 |
| 2 | 7002 | 6380 | 9102 |
| 3 | 7003 | 6381 | 9103 |

Stop local cluster:

```bash
./scripts/stop_cluster.sh
```

Docker Compose:

```bash
docker compose up --build
```

Helm:

```bash
helm install raftkv ./helm/raftkv
```

Useful client commands:

```bash
redis-cli -p 6379 PING
redis-cli -p 6379 SET hello world
redis-cli -p 6379 GET hello
redis-cli -p 6379 INFO raft
redis-cli -p 6379 CLUSTER NODES
```

Configuration fields:

- `id`: local node id.
- `raft_listen`: local gRPC bind address.
- `client_listen`: local RESP bind address.
- `metrics_listen`: local Prometheus endpoint.
- `peers`: initial cluster peer list.
- `data_dir`: node state directory.
- `election_timeout_ms`: low end of election timeout.
- `heartbeat_ms`: heartbeat interval.
- `tick_ms`: runtime tick granularity.
- `snapshot_entries_threshold`: intended snapshot trigger threshold.
- `pre_vote`: enable pre-vote.

Operational reminders:

- All initial voters must have symmetric peer lists at first boot.
- Node IDs must be unique.
- gRPC peer URLs should include `http://`.
- Writes should go to the leader.
- Follower writes currently return a redirect-like error.
- Delete `.run/data` only when you intentionally want a fresh cluster.

## Safety invariants

### Election Safety

At most one leader can be elected in a given term.

Mechanisms:

- `voted_for` allows one real vote per term.
- `current_term` is persisted.
- Higher term messages force step-down.
- Quorum intersection prevents two candidates from both winning with the same
  voter set.

Code:

- `RaftNode::step`
- `handle_request_vote`
- `become_candidate`
- `become_follower`
- `Action::PersistHardState`

### Leader Append-Only

A leader never overwrites or deletes entries in its own log.

Mechanisms:

- Leaders append new entries at `last_index + 1`.
- Conflict truncation happens on followers.

Code:

- `propose`
- `propose_config_change`
- `handle_append_entries`
- `RaftLog::truncate_from`

### Log Matching

If two logs contain an entry with the same index and term, all previous entries
are identical.

Mechanisms:

- AppendEntries includes `prev_log_index` and `prev_log_term`.
- Follower rejects if the previous entry does not match.
- Follower truncates from first conflicting incoming entry.

Code:

- `handle_append_entries`
- `RaftLog::term_at`
- `RaftLog::truncate_from`

### Leader Completeness

If an entry is committed in a term, every future leader contains that entry.

Mechanisms:

- Committed entries are stored on a quorum.
- Future election quorums intersect that quorum.
- Voters reject candidates with stale logs.

Code:

- `RaftLog::is_up_to_date`
- `handle_request_vote`
- `maybe_advance_commit`

### State Machine Safety

No two servers apply different commands at the same log index.

Mechanisms:

- Log matching.
- Commit before apply.
- Apply strictly in index order.
- Deterministic state-machine commands.

Code:

- `flush_apply`
- `Runtime::apply_entries`
- `KvStateMachine::apply`

### Commit Current-Term Rule

Leaders only advance commit by counting replicas for entries from their current
term.

Mechanism:

- `maybe_advance_commit` checks `log.term_at(new_commit) == current_term`.

Why:

- Prevents the Figure 8 stale leader problem from the Raft paper.

### Joint Consensus Safety

During membership changes, commits require quorum in both old and new configs.

Mechanisms:

- `ConfigState::Joint`
- `ConfigState::has_quorum`
- `maybe_advance_commit` joint branch

## Scenario walkthroughs

### A clean election

1. All nodes start as followers.
2. No leader heartbeat arrives.
3. One follower's randomized timeout fires first.
4. It becomes precandidate and sends pre-votes.
5. Peers grant pre-votes because there is no active leader and the log is fresh.
6. It becomes candidate, increments term, votes for itself, persists hard state.
7. It sends RequestVote.
8. A quorum grants votes.
9. It becomes leader.
10. It appends a no-op.
11. It sends AppendEntries heartbeats/replication.

### A client write

1. Client sends `SET k v`.
2. Leader appends a normal entry.
3. Leader persists it locally.
4. Leader sends AppendEntries.
5. Followers verify previous log entry.
6. Followers append and persist.
7. Followers reply success.
8. Leader sees quorum match index.
9. Leader commits the entry.
10. Leader applies it to KV.
11. Client receives `OK`.
12. Followers learn commit index and apply the same entry.

### A follower with conflicting entries

1. Follower has uncommitted entries from an old leader.
2. New leader sends AppendEntries with `prev_log_index` and `prev_log_term`.
3. If the follower's previous entry does not match, it rejects.
4. Leader backs up `next_index`.
5. Eventually leader finds a matching prefix.
6. Follower truncates conflicting suffix.
7. Follower appends leader entries.
8. Logs converge.

### Leader crash after local append but before commit

1. Leader appends entry locally.
2. Leader crashes before replicating to quorum.
3. The entry is not committed.
4. A new leader may not contain that entry.
5. If the old leader returns as follower, its uncommitted entry can be
   overwritten by the new leader's log.

This is safe because the client should not receive success until the entry is
committed and applied.

### Leader crash after commit

1. Leader replicates entry to quorum.
2. Leader advances commit.
3. Leader crashes.
4. Any future election quorum intersects the commit quorum.
5. The up-to-date voting rule prevents a candidate missing the committed entry
   from winning.
6. Future leader contains the committed entry.

### Network partition

If a 3-node cluster splits 2-1:

- The 2-node side can elect/keep a leader and commit.
- The 1-node side cannot get quorum and cannot commit.
- If the isolated old leader receives writes, those writes should not commit.
- When the partition heals, the valid leader's log wins.

### Lagging follower needs snapshot

1. Follower is offline for a long time.
2. Leader compacts old log entries into a snapshot.
3. Follower returns with `next_index` below leader `first_index`.
4. Leader cannot send missing entries.
5. Leader sends InstallSnapshot.
6. Follower installs snapshot and resumes replication after snapshot index.

The current repository has the Raft control flow for this, but full KV snapshot
bytes and restore are still unfinished.

## Code map

| Concept | Main code |
|---|---|
| Pure core | `crates/raft-core/src/node.rs` |
| Node roles | `crates/raft-core/src/role.rs` |
| Hard/soft state | `crates/raft-core/src/state.rs` |
| Log and entries | `crates/raft-core/src/log.rs` |
| Messages | `crates/raft-core/src/message.rs` |
| Actions | `crates/raft-core/src/action.rs` |
| Cluster config | `crates/raft-core/src/config.rs` |
| Leader progress | `crates/raft-core/src/progress.rs` |
| Runtime loop | `crates/raftkv-server/src/runtime.rs` |
| Binary startup | `crates/raftkv-server/src/main.rs` |
| Server config | `crates/raftkv-server/src/config.rs` |
| Segmented log | `crates/raft-storage/src/segmented_log.rs` |
| Segment records | `crates/raft-storage/src/segment.rs` |
| Hard state store | `crates/raft-storage/src/meta.rs` |
| Snapshot store | `crates/raft-storage/src/snapshot_store.rs` |
| gRPC client | `crates/raft-net/src/client.rs` |
| gRPC server | `crates/raft-net/src/server.rs` |
| Proto conversion | `crates/raft-net/src/convert.rs` |
| Raft proto | `proto/raft.proto` |
| Membership proto | `proto/membership.proto` |
| Admin proto | `proto/admin.proto` |
| KV commands | `crates/kv-state-machine/src/command.rs` |
| KV state | `crates/kv-state-machine/src/state.rs` |
| RESP codec | `crates/resp-server/src/codec.rs` |
| RESP parser | `crates/resp-server/src/commands.rs` |
| RESP dispatch | `crates/resp-server/src/handler.rs` |
| RESP TCP server | `crates/resp-server/src/server.rs` |
| Deterministic simulator | `crates/sim-tests/src/lib.rs` |
| Linearizability checker | `crates/linearizability-checker/src/lib.rs` |
| Local cluster script | `scripts/run_cluster.sh` |
| Docker cluster | `docker-compose.yml` |
| Helm chart | `helm/raftkv` |

## Paper-to-code glossary

| Raft concept | Paper/thesis area | Code |
|---|---|---|
| RPC term-bump rule | Raft paper 5.1 | `RaftNode::step` |
| Persistent current term | Raft paper 5.2 | `HardState.current_term`, `Action::PersistHardState` |
| Persistent vote | Raft paper 5.2 | `HardState.voted_for`, `handle_request_vote` |
| Election timeout | Raft paper 5.2 | `tick`, `reset_randomized_election_timeout` |
| RequestVote | Raft paper 5.2 | `Message::RequestVote`, `handle_request_vote` |
| Log freshness vote check | Raft paper 5.4.1 | `RaftLog::is_up_to_date` |
| AppendEntries | Raft paper 5.3 | `Message::AppendEntries`, `handle_append_entries` |
| Log Matching | Raft paper 5.3 | `prev_log_index` and `prev_log_term` check |
| Conflict truncation | Raft paper 5.3 | `RaftLog::truncate_from` |
| Commit by quorum | Raft paper 5.3/5.4 | `maybe_advance_commit` |
| Current-term commit rule | Raft paper 5.4.2 | `maybe_advance_commit` term check |
| Apply committed entries | Raft paper 5.4.3 | `flush_apply`, `Runtime::apply_entries` |
| Joint consensus | Raft paper 6, thesis 4 | `ConfigState::Joint` |
| Log compaction | Raft paper 7 | `RaftLog::compact_through`, `SegmentedLog::compact` |
| InstallSnapshot | Raft paper 7 | `Message::InstallSnapshot`, `handle_install_snapshot` |
| Pre-vote | Ongaro thesis 9.6 | `Role::PreCandidate`, `pre_vote` flag |
| Leadership transfer | Ongaro thesis 3.10 | `transfer_leadership`, `TimeoutNow` |
| Read-index | Ongaro thesis 6.4 | `read_index`, `handle_read`, `drain_ready_reads` |

## Current gaps and production hardening checklist

This repository is an educational implementation with many core pieces in place.
Before treating it as production-grade infrastructure, address the following.

Consensus and runtime:

- Add explicit per-read quorum acknowledgement tracking for read-index.
- Use `conflict_term` on the leader for full fast backoff.
- Add stronger proposal cancellation on leadership loss.
- Add bounded queues and backpressure from runtime to client/network layers.
- Add learner support if non-voting replicas are desired.
- Add membership runtime/gRPC service binding.
- Add membership persistence/recovery tests across restarts.

Snapshots:

- Trigger snapshots based on `snapshot_entries_threshold`.
- Serialize KV state and config into snapshot bytes.
- Restore KV state from installed snapshots.
- Stream large snapshots in real chunks.
- Verify snapshot install under partitions and restarts.
- Include snapshot metadata in status/admin output.

State machine and Redis surface:

- Implement `TTL key` with remaining seconds.
- Wire deterministic replicated `Tick` or another expiry mechanism.
- Decide exact Redis compatibility semantics for `SET` options, `PX`, `NX`,
  `XX`, database selection, and errors.
- Add overflow/error behavior tests for integer commands.

Security and operations:

- Add TLS/mTLS for peer traffic.
- Add client authentication and ACLs.
- Add online config validation.
- Add backup/restore CLI flow.
- Add rolling upgrade procedure.
- Add log and snapshot disk-space guardrails.

Testing:

- Add long-running simulation tests with partitions and crashes.
- Add persistence restart tests.
- Add fuzzing for RESP codec and storage record recovery.
- Add Jepsen or Jepsen-style tests.
- Add benchmarks for replication, storage, and command latency.
- Add model-based tests for membership changes.

Observability:

- Export real Raft metrics through Prometheus, not only logs/status.
- Track leader changes, election count, append latency, commit latency,
  apply latency, log lag, snapshot duration, and replication backpressure.
- Add alerts for leader churn, no leader, growing follower lag, disk pressure,
  and failed snapshot installs.

## Reading order for new contributors

1. `README.md` for project purpose and quick start.
2. `ARCHITECTURE.md` for the pure/impure split.
3. `crates/raft-core/src/log.rs` for entries and log mechanics.
4. `crates/raft-core/src/message.rs` and `action.rs`.
5. `crates/raft-core/src/node.rs`, one section at a time.
6. `crates/raftkv-server/src/runtime.rs` for action execution.
7. `crates/raft-storage` for durability.
8. `crates/raft-net` for gRPC mapping.
9. `crates/resp-server` and `crates/kv-state-machine` for client-visible
   behavior.
10. `crates/sim-tests` for deterministic testing.

## Change checklist

When changing Raft code, check these questions before merging:

- Does any path send a message before the required hard state or log entry is
  durably persisted?
- Can a node vote twice in one term after a crash?
- Can a candidate with a stale log win?
- Can a leader commit an old-term entry by counting replicas directly?
- Can two different commands be applied at the same log index?
- Does a follower truncate only after finding a real conflict?
- Does every commit advance apply in order?
- Does a membership change require the correct quorum set?
- Does a read confirm leadership strongly enough for the chosen read protocol?
- Does restart reconstruct the same Raft state from disk?
- Does the deterministic simulator still replay with the same seed?

If the answer is unclear, add a focused test before changing the behavior.
