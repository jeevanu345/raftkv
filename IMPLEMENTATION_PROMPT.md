I studied the README you pasted and the actual current jeevanu345/raftkv implementation, including the Raft core, runtime, storage, state machine, RESP layer, protobuf definitions, and simulator. Your project already has a strong foundation, but I would change the order of work slightly:
Do not build the full dashboard first. Fix the correctness/control-plane gaps first, then build the dashboard on top of those APIs.

The dashboard is still the best major feature to add, but the code review exposed several important issues that the dashboard would otherwise hide rather than solve.
1. Where RaftKV currently stands
Architecturally, this is already much better than a basic student Raft implementation.
Your core structure is:
                        ┌─────────────────────────────┐
                        │        Web Dashboard         │
                        │ React + TypeScript           │
                        └──────────────┬──────────────┘
                                       │
                             HTTP / SSE / WS
                                       │
                        ┌──────────────▼──────────────┐
                        │       Admin HTTP API         │
                        │          Axum                │
                        └──────────────┬──────────────┘
                                       │
┌──────────────┐             ┌────────▼────────┐             ┌──────────────┐
│ RESP clients │────────────▶│ raftkv-runtime  │◀────────────│ gRPC peers   │
│ redis-cli    │             │                 │             │ other nodes  │
└──────────────┘             └───────┬─────────┘             └──────────────┘
                                     │
               ┌─────────────────────┼───────────────────────┐
               │                     │                       │
          ┌────▼─────┐         ┌────▼──────┐          ┌────▼──────┐
          │raft-core │         │ storage   │          │ KV state  │
          │ PURE     │         │ WAL/snap  │          │ machine   │
          └──────────┘         └───────────┘          └───────────┘

The pure/impure boundary is absolutely the architecture to preserve. The current raft-core deliberately has no disk, network, async runtime, or operating-system clock interaction; actions are interpreted by the outer runtime. That design is the reason your deterministic simulator is possible. 
So I would not rewrite the existing architecture. I would extend it.
2. Priority order I would use
Priority	Work	Why
P0	Fix Raft/read/recovery correctness	Required before claiming strong consistency
P0	Fix real snapshot contents/install	Required for safe log compaction
P0	Fix TTL behavior	Current TTL surface is incomplete
P0	Fix restart/replay semantics	Prevent duplicate state-machine execution
P0	Add tests for even-sized quorums	There appears to be a quorum calculation bug
P1	Finish Admin + Membership services	Creates clean backend for GUI
P1	Add real cluster diagnostics model	Gives dashboard trustworthy data
P1	Add proper Prometheus instrumentation	Current endpoint infrastructure is insufficient
P1	Add SCAN/key enumeration	Needed for Key Explorer
P1	Correct leader redirect behavior	Needed for normal clients and UI
P1	Build React dashboard MVP	Cluster + Keys + Console
P2	Raft event visualizer	Major showcase feature
P2	Simulation playground	Excellent educational differentiator
P2	Backup/restore	Operational completeness
P2	Fuzzing/chaos/benchmarks	Evidence of reliability
P3	TLS/mTLS/auth/ACL/RBAC	Needed before remote administration
P3	Advanced deployment/operations	Production-like polish


3. Most important issue: your current ReadIndex path needs correction
This is more important than the UI.
Your documentation says reads are linearizable using ReadIndex.
The core currently does approximately this:
pub fn read_index(...) {
    if !leader {
        return;
    }

    broadcast_heartbeat();

    acts.push(Action::NotifyReadIndex {
        commit_index: self.hs.commit_index,
        ...
    });
}

The problem is that NotifyReadIndex is emitted immediately after sending heartbeats. The code doesn't wait for quorum acknowledgement of that particular read round. 
Then the runtime receives the notification and checks only:
applied_index >= commit_target

before executing the read.  
That means the intended sequence:
Read request
    ↓
Confirm I am still leader with quorum
    ↓
Obtain safe ReadIndex
    ↓
Wait state machine >= ReadIndex
    ↓
Return result

is effectively closer to:
Read request
    ↓
Send heartbeats
    ↓
Immediately announce ReadIndex
    ↓
Return if already applied

The heartbeats may still be travelling when the read is served.
Correct implementation
Introduce an actual pending ReadIndex round:
ReadContext {
    request_id
    term
    read_index
    acknowledgements
    client_waiters
}

When a read starts:
rid = unique ID
read_index = current commit index
acks = {leader}
send heartbeat(rid) to peers

Heartbeats/AppendEntries must carry this context/request ID.
When responses arrive:
acks.insert(peer)

if current_config.has_quorum(acks):
    ReadIndex becomes safe

Then:
wait until state_machine.applied_index >= read_index

Only after that:
execute GET/MGET/EXISTS/DBSIZE/SCAN/etc.

For joint consensus, use the existing ConfigState::has_quorum() rather than a simple majority, because it already correctly requires quorum in both old and new configurations. 
I would add explicit tests for:
old leader isolated from majority
old leader receives GET
new leader elected elsewhere
old leader must NOT return a successful linearizable read

This should be a P0 item.
4. There appears to be an even-cluster commit-quorum bug
This is another important code-level issue.
Your election quorum logic is correct:
set.len() / 2 + 1

But maybe_advance_commit() derives the commit candidate differently. 
For an even-sized voter set this appears unsafe.
Suppose four nodes have:
match indexes:

node1 = 100
node2 = 100
node3 = 20
node4 = 10

A four-node cluster needs:
3 acknowledgements

to commit.
Sorted:
10 20 100 100

The commit index supported by three replicas is:
20

not 100.
Your current index calculation appears capable of selecting the third sorted value, 100, which only two members have.
The generic formula should be conceptually:
quorum = n / 2 + 1;
candidate = sorted[n - quorum];

For four nodes:
n = 4
quorum = 3
index = 4 - 3 = 1

sorted[1] = 20

Correct.
The same calculation must be independently performed for the old and new voter sets during joint consensus.
Add tests for:
2-node cluster
4-node cluster
6-node cluster
3→4 joint configuration
4→5 joint configuration
4→3 joint configuration

Your current unit tests mostly exercise three-node configurations, which is exactly why a bug like this can remain hidden.
5. Snapshot handling needs to become a real state-machine snapshot
This is probably the largest storage gap.
The storage implementation itself provides a reasonable atomic writer:
temporary snapshot
→ fsync
→ metadata
→ atomic rename

But your current runtime's take_snapshot() creates the writer and finishes it without writing the KV state into it. 
Furthermore, when the Raft core decides a follower needs a snapshot, it sends:
data: Bytes::new()

and explicitly comments that the runtime should populate it. 
The current runtime doesn't appear to replace that empty payload with actual state-machine snapshot contents.
On receiving an InstallSnapshot, the runtime updates snapshot metadata/log state but does not actually restore the sled KV state from the transferred data. 
So I would redefine the snapshot subsystem around something like:
Snapshot
├── format_version
├── last_included_index
├── last_included_term
├── cluster_configuration
├── state_hash
├── logical_time
└── state machine
    ├── kv entries
    └── TTL metadata

Snapshot creation
freeze/read consistent SM state
        ↓
serialize state
        ↓
write chunks
        ↓
compute checksum
        ↓
fsync snapshot
        ↓
persist snapshot metadata
        ↓
compact Raft log

Snapshot transfer
Don't put a multi-MB snapshot inside one protobuf field.
Use streaming:
rpc InstallSnapshot(stream SnapshotChunk)
    returns (InstallSnapshotResponse);

Each chunk should include roughly:
snapshot_id
offset
data
checksum
done

A follower should write:
incoming.snapshot.tmp

verify the checksum and metadata, then atomically install it.
Snapshot install
Correct ordering:
Receive all chunks
        ↓
Verify integrity
        ↓
Restore state machine
        ↓
fsync state
        ↓
Persist snapshot pointer
        ↓
Advance applied/commit indexes
        ↓
Compact local log
        ↓
ACK leader

Do not ACK before the snapshot is safely installed.
6. Automatic snapshot triggering isn't really connected yet
ServerConfig contains:
snapshot_entries_threshold

but repository search shows it being used in configuration/deployment definitions rather than the runtime snapshot decision path. 
You should maintain:
last_snapshot_index
current_applied_index

and trigger when:
applied_index - last_snapshot_index
    >= snapshot_entries_threshold

Potentially add a secondary size threshold:
raft_log_bytes >= snapshot_bytes_threshold

That is better than relying only on number of entries.
Dashboard should show:
Last snapshot         12,450
Applied index         18,220
Entries since snap     5,770
Snapshot threshold    10,000
Next snapshot         ~4,230 entries

7. Restart/recovery semantics need attention
There is another important recovery issue.
RaftNode::new() initializes:
last_applied = snapshot_index

rather than the persisted state machine's actual applied_index. 
But your KV state machine separately persists its own applied index. 
Imagine:
snapshot index = 0
commit index   = 100
KV applied     = 100

Node restarts.
The KV database still contains operations 1–100.
But Raft starts with:
last_applied = 0

When commit later advances, the Raft core can potentially produce an apply range beginning from the old snapshot boundary instead of the state machine's real durable applied point.
For idempotent SET, that may appear harmless.
For:
INCR counter

it is not harmless.
Re-applying it changes state again.
Recovery should explicitly reconcile
snapshot_index
state_machine_applied_index
raft_commit_index
raft_last_log_index

Required invariant:
snapshot_index
    <= state_machine_applied_index
    <= commit_index
    <= last_log_index

with special allowances for snapshot boundaries.
The runtime should initialize Raft's volatile apply pointer from:
sm.applied_index()

not simply from snapshot index.
Better API:
RaftNode::restore(
    config,
    hard_state,
    log,
    last_applied,
)

8. State-machine application should be transactionally crash-safe
Your own state machine comment recognizes that updates across the kv, ttl, and meta sled trees are not currently transactional. 
For operations such as:
INCR

a crash after the KV modification but before updating applied_index creates ambiguity.
Use a multi-tree sled transaction or redesign the storage layout so each command applies atomically together with:
state mutation
TTL mutation
state hash
applied_index

The apply layer should also enforce:
incoming_index == current_applied_index + 1

If:
incoming_index <= current_applied_index

treat it as an already-applied entry rather than mutating state again.
That gives you explicit replay protection.
9. TTL needs a redesign, not merely implementing the TTL command
Your README correctly says TTL currently returns -1. The RESP code literally hardcodes that response. 
But there is a deeper problem.
You have a replicated command:
Command::Tick { now_ms }

which calls:
evict_expired(now_ms)

However, repository search currently finds Command::Tick only in the state-machine implementation rather than a runtime scheduler proposing those ticks. 
So TTL expiration isn't just missing its display command—the expiration lifecycle itself needs finishing.
There is also another data-model problem.
Currently:
ttl tree:
expire_at || key → ()

Calling EXPIRE multiple times can insert multiple expiry records.
Similarly:
SET key newvalue

without an expiry doesn't visibly remove an older TTL record for the same key.
That can allow an old expiry record to later delete a newer value.
Better TTL schema
Use both:
ttl_by_key:
key → expire_at

ttl_index:
expire_at || key → ()

Whenever expiry changes:
lookup previous expiry
remove old ttl_index record
write new ttl_by_key value
write new ttl_index record

PERSIST becomes cheap:
lookup ttl_by_key[key]
remove corresponding ttl_index
remove ttl_by_key[key]

No full TTL-tree scan.
Then implement:
TTL key

-2 → key doesn't exist
-1 → key exists without expiration
 N → seconds remaining

and ideally later:
PTTL
EXPIREAT
PEXPIRE

10. Replicated time needs cleaner semantics
Your RESP server calculates expiration using local SystemTime. 
The expiration timestamp is then replicated, which is much better than followers independently calculating expiration.
But leader changes create potential wall-clock skew issues.
For example:
Leader A clock = 12:00:10
Leader B clock = 11:59:55

If B takes over, cluster time appears to move backwards.
A simple improvement is a replicated monotonically nondecreasing cluster time:
logical_now = max(
    local_wall_clock,
    last_replicated_timestamp + 1
)

You do not need a full distributed time service for this project.
Just make leader-issued timestamps monotonically increase relative to replicated history.
11. Membership changes need runtime-level dynamic peer management
The protocol is already defined nicely:
AddServer
RemoveServer
TransferLeadership

And the core supports joint consensus.
But the current runtime constructs:
peers
addr_book

from the startup config and stores them as ordinary hash maps. 
The core config change itself only carries:
AddServer(NodeId)
RemoveServer(NodeId)

That means the consensus configuration doesn't contain enough information to dynamically learn:
node 4
raft address
client address
admin address
voter/non-voter role

I would change cluster membership records to
struct Member {
    id: NodeId,
    raft_addr: String,
    client_addr: String,
    admin_addr: String,
    role: MemberRole,
}

with:
MemberRole::Voter
MemberRole::Learner

Then replicate membership metadata as part of configuration transitions.
Eventually you can implement the safer joining sequence:
new node
   ↓
join as learner
   ↓
catch up log/snapshot
   ↓
verify match_index close to leader
   ↓
promote to voter through joint consensus

This is considerably safer than immediately making an empty node a voter.
12. Finish the Admin gRPC API before the web API
You already defined:
Status
TriggerSnapshot

and membership RPCs.
Your build already generates all three protobuf services. 
But main.rs currently only registers the Raft peer service. 
I would finish:
Raft gRPC
Admin gRPC
Membership gRPC

first.
Then the HTTP API becomes an ergonomic browser-facing facade rather than the canonical administration implementation.
13. Add a proper diagnostics model
The dashboard should not scrape INFO and parse strings.
Create a typed runtime structure:
pub struct NodeDiagnostics {
    pub node_id: NodeId,
    pub role: Role,
    pub term: u64,
    pub leader_id: Option<NodeId>,

    pub commit_index: u64,
    pub applied_index: u64,
    pub last_log_index: u64,
    pub snapshot_index: u64,

    pub state_hash: [u8; 32],

    pub peers: Vec<PeerDiagnostics>,
}

For each peer:
pub struct PeerDiagnostics {
    pub id: NodeId,
    pub match_index: u64,
    pub next_index: u64,
    pub replication_lag: u64,
    pub inflight: u32,
    pub recently_active: bool,
    pub snapshot_in_progress: bool,
}

Then expose a read-only diagnostics snapshot from raft-core.
Important distinction:
raft-core:
produces deterministic diagnostics values

runtime:
adds runtime/network/disk metrics

HTTP:
serializes them

React:
visualizes them

That maintains your pure-core architecture.
14. Replace the minimal metrics endpoint with a real control HTTP server
Right now main.rs manually accepts TCP sockets and emits:
prometheus::gather()

You are already proposing Axum, and I agree.
Use one HTTP server for:
GET /health/live
GET /health/ready
GET /metrics

GET /api/v1/status
GET /api/v1/cluster
GET /api/v1/events
...

Recommended crate layout:
crates/
├── raft-core
├── raft-storage
├── raft-net
├── kv-state-machine
├── resp-server
├── raftkv-server
├── raftkv-admin-api       ← new
├── raftkv-cli
├── linearizability-checker
├── sim-tests
└── raftkv-lab            ← later simulator backend

ui/
└── dashboard/

I would keep browser-specific code outside raft-core, exactly as you planned.
15. Prometheus observability needs significant expansion
Your core already emits useful MetricEvents, such as:
TermBumped
ElectionStarted
ElectionWon
CommitAdvanced
AppendRejected
LeadershipTransferStarted

but the runtime currently maps these primarily to tracing calls. 
Turn those into real metrics.
Metric	Type
raftkv_raft_term	Gauge
raftkv_raft_role	Gauge
raftkv_raft_commit_index	Gauge
raftkv_raft_applied_index	Gauge
raftkv_raft_last_log_index	Gauge
raftkv_raft_elections_total	Counter
raftkv_raft_leadership_changes_total	Counter
raftkv_raft_append_rejections_total	Counter
raftkv_raft_replication_lag	Gauge per peer
raftkv_raft_messages_sent_total	Counter
raftkv_raft_messages_received_total	Counter
raftkv_raft_rpc_duration_seconds	Histogram
raftkv_client_requests_total	Counter
raftkv_client_request_duration_seconds	Histogram
raftkv_client_errors_total	Counter
raftkv_storage_log_bytes	Gauge
raftkv_storage_snapshot_bytes	Gauge
raftkv_storage_fsync_duration_seconds	Histogram
raftkv_snapshot_total	Counter
raftkv_snapshot_install_total	Counter
raftkv_kv_keys	Gauge
raftkv_kv_expiring_keys	Gauge


Grafana then remains useful for detailed operational metrics.
The custom dashboard should focus more heavily on understanding Raft, not replacing Grafana.
16. Fix follower redirection
At the moment, addr_book is populated using:
raft_addr

but MOVED is a client-protocol redirect.
That means you need separate advertised endpoints:
raft_listen = "0.0.0.0:7001"
raft_advertise = "node1:7001"

client_listen = "0.0.0.0:6379"
client_advertise = "node1:6379"

admin_listen = "0.0.0.0:8080"
admin_advertise = "node1:8080"

Then:
MOVED 0 node1:6379

not:
MOVED 0 http://node1:7001

For the dashboard, you can do something even better: the HTTP API running on any node can internally discover the leader and proxy leader-required operations.
17. Add SCAN before building the Key Explorer
Your original observation here was exactly right.
A database browser cannot work with only:
GET
MGET
EXISTS
DBSIZE

because it has no safe mechanism to enumerate keys.
Don't implement:
KEYS *

as the primary interface.
Implement:
SCAN cursor
    [MATCH pattern]
    [COUNT count]

For the GUI:
GET /api/v1/keys
    ?cursor=...
    &limit=100
    &pattern=user:*

response:
{
  "cursor": "opaque-next-cursor",
  "items": [
    {
      "key": "user:1001",
      "type": "string",
      "ttl_ms": 63000,
      "size_bytes": 241
    }
  ]
}

Keep the cursor opaque.
Do not expose sled internals as the public cursor contract.
18. Key Explorer design
This should become one of your strongest GUI screens.
Layout:
┌───────────────────────────────────────────────────────────┐
│ Key Explorer                             12,491 keys      │
├───────────────────┬───────────────────────────────────────┤
│ Search            │ Key: user:123                        │
│ user:*            │                                      │
│                   │ Type       String                    │
│ user:123          │ Size       421 B                     │
│ user:148          │ TTL        3m 12s                    │
│ user:201          │                                      │
│ cart:442          │ Value                                │
│ ...               │ ┌──────────────────────────────────┐ │
│                   │ │ {"name":"Jeevan", ...}           │ │
│                   │ └──────────────────────────────────┘ │
│                   │                                      │
│                   │ [Edit] [Delete] [Change TTL]         │
└───────────────────┴───────────────────────────────────────┘

Support:
key search/pattern
pagination
binary-safe display
UTF-8/HEX/Base64 representations
TTL
size
edit
delete
set expiry
persist expiry
copy key/value

Dangerous actions should visibly indicate:
This operation will be replicated through Raft.

FLUSHDB should require explicit confirmation.
19. Command Console
This is another high-value, relatively easy feature.
Something like:
raftkv> SET greeting "hello raft"
OK

raftkv> GET greeting
"hello raft"

raftkv> INFO raft
role:leader
term:14
leader_id:2
commit_index:884

Add:
history
arrow-key navigation
syntax highlighting
execution time
target node
leader/follower indicator
pretty RESP rendering
raw RESP toggle

A particularly useful educational feature:
Execution details

Received by: node 2
Leader: node 2
Proposed index: 885
Committed: 18.4 ms
Applied: 18.9 ms
Replicated to: 3/3

That makes the console much more interesting than a generic Redis shell.
20. Cluster Overview should be the homepage
Suggested layout:
RAFTKV
Cluster: Healthy          Leader: Node 2        Term: 17

┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ NODE 1       │ │ NODE 2       │ │ NODE 3       │
│ FOLLOWER     │ │ LEADER       │ │ FOLLOWER     │
│              │ │              │ │              │
│ Term      17 │ │ Term      17 │ │ Term      17 │
│ Commit  9812 │ │ Commit  9812 │ │ Commit  9812 │
│ Applied 9812 │ │ Applied 9812 │ │ Applied 9811 │
│ Lag        0 │ │ Lag        - │ │ Lag        1 │
│ ● Healthy    │ │ ● Healthy    │ │ ● Healthy    │
└──────────────┘ └──────────────┘ └──────────────┘

Replication
Node 1 █████████████████████████ 9812
Node 2 █████████████████████████ 9812
Node 3 ████████████████████████▉ 9811

Show cluster-wide:
leader
current term
quorum health
membership configuration
joint-consensus state
commit index
snapshot index
key count
requests/sec
P50/P95/P99 latency
elections
uptime

Node drawer:
role
term
voted_for
leader
last log index
commit index
applied index
state hash
snapshot index
disk usage
network state
peer replication progress

21. The Raft Visualizer can make this project stand out
This is where RaftKV could go from "good systems project" to an exceptionally good demonstration project.
Use your existing deterministic Action model.
Render nodes:
                 ┌──────────┐
                 │ NODE 1   │
                 │ FOLLOWER │
                 └────▲─────┘
                      │
         AppendEntries│
                      │
┌──────────┐          │          ┌──────────┐
│ NODE 2   │──────────┼─────────▶│ NODE 3   │
│ LEADER   │          │          │ FOLLOWER │
└──────────┘          │          └──────────┘

Animate:
RequestVote
VoteGranted
AppendEntries
AppendResponse
InstallSnapshot
TimeoutNow

Below each node show a small log:
Node 1
[45,t8][46,t8][47,t9][48,t9]

Node 2 LEADER
[45,t8][46,t8][47,t9][48,t9][49,t9]

Node 3
[45,t8][46,t8][47,t9]

Colors/states can identify:
uncommitted
committed
applied
conflicting
compacted

Then a user can watch:
leader writes entry
        ↓
AppendEntries sent
        ↓
followers append
        ↓
majority ACK
        ↓
commitIndex advances
        ↓
ApplyCommitted emitted

That directly visualizes your existing pure Action architecture.
22. Add a proper Raft event stream
For the visualizer, don't poll state every 100 ms.
Have the runtime publish events:
enum RuntimeEvent {
    RoleChanged,
    TermChanged,
    MessageSent,
    MessageReceived,
    LogAppended,
    LogTruncated,
    CommitAdvanced,
    EntryApplied,
    SnapshotStarted,
    SnapshotInstalled,
    ReadIndexStarted,
    ReadIndexQuorumReached,
    LeadershipTransferStarted,
}

Then expose:
GET /api/v1/events
Content-Type: text/event-stream

SSE is an excellent fit because the browser mainly receives events.
Example:
{
  "seq": 18842,
  "nodeId": 2,
  "term": 17,
  "type": "AppendEntriesSent",
  "peerId": 3,
  "prevLogIndex": 884,
  "entries": 1,
  "leaderCommit": 883
}

Keep a bounded ring buffer:
last 10,000 events

so reconnecting browsers can resume.
23. Simulation Playground should be separate from the real cluster
Your simulator already supports:
logical ticks
delays
message drops
partitions
deterministic PRNG
seed replay

That is perfect for a visual simulator.
But do not mix "destroy my live cluster" controls with the educational simulator.
Create:
Live Cluster
Simulation Lab

as distinct modes.
Simulation UI:
Seed             0xCAFE
Nodes            5
Message delay    1–4 ticks
Drop probability 5%

[Start] [Pause] [Step] [Reset]

Node 1 [Partition]
Node 2 [Crash]
Node 3 [Partition]

Network:
[Delay all]
[Heal cluster]
[Drop next message]

Replay:
Seed: 51966
Tick: 842
Scenario: partition-node-2

Allow export/import:
{
  "seed": 51966,
  "nodes": [1,2,3,4,5],
  "events": [...]
}

That gives you reproducible demonstrations.
24. Expand the simulator considerably
Current simulation is a good beginning, but it mainly models message timing/partition state. 
Add:
Fault	Needed
Drop message	Yes
Delay message	Yes
Reorder message	Yes
Duplicate message	Add
Network partition	Existing/basic
Asymmetric partition	Add
Crash node	Add
Restart node	Add
Disk append failure	Add
fsync failure	Add
Torn write	Add
Snapshot failure	Add
Clock/tick stalls	Add
Slow follower	Add
Leader isolation	Add
Membership change during partition	Add
Snapshot during membership change	Add


Then write invariant monitors that execute every simulator step.
For example:
ElectionSafety
LeaderCompleteness
LogMatching
StateMachineSafety
CommittedEntryNeverDisappears
AppliedIndexNeverDecreases
CommitIndexNeverDecreases

Fail immediately and print:
seed
tick
messages
node states
logs
configuration state

This would be exceptionally valuable for debugging.
25. Linearizability verification should evolve
Your current exhaustive checker is fine for short histories, and your own documentation correctly notes its scalability limit. 
Make every integration test capable of recording:
operation ID
client ID
invocation time
completion time
operation
input
result
node contacted
leader term
log index

Then export:
[
  {
    "client": 1,
    "operation": "set",
    "key": "x",
    "value": "1",
    "start": 1024,
    "end": 1041
  }
]

Your small built-in checker can process short test runs.
Larger histories can later go to Porcupine/Knossos/Jepsen.
26. Add Jepsen-style external testing
Once the P0 correctness work is finished, create an external harness that can:
start 3/5 nodes
generate concurrent clients
kill leader
restart leader
partition majority/minority
heal network
introduce latency
run reads and writes
collect history
check linearizability

Important scenarios:
leader dies before commit
leader dies after majority replication
old leader isolated
dual partition
follower restart
all nodes restart
snapshot + restart
snapshot + partition
membership change + failure
leadership transfer + concurrent writes

This gives credibility far beyond adding more features.
27. Fuzzing
Use cargo-fuzz for several surfaces:
RESP decoder
RESP command parser
Raft message conversion
segmented-log recovery parser
snapshot decoder
bincode command decoder
random Raft message sequences

Your RESP codec in particular is network-facing, so fuzzing malformed lengths/nesting/bulk strings is worthwhile.
Storage fuzzing should test:
valid log
truncate arbitrary byte
flip arbitrary byte
append garbage
reopen

Expected result:
recover valid prefix
or fail cleanly

never silently accept corruption

28. Benchmarks
Your README currently lists benchmarks as future work, which is correct.
Create Criterion microbenchmarks:
raft step()
append entry
log serialization
CRC framing
state-machine SET
GET
INCR
snapshot serialization
RESP parse/encode

Then end-to-end benchmark:
1-node
3-node
5-node

with:
50% GET / 50% SET
95% GET / 5% SET
100% SET
pipeline depth 1/8/32
values 64B/1KB/16KB

Report:
ops/sec
P50
P95
P99
P99.9
CPU
memory
disk bytes/sec
network bytes/sec

Most importantly, don't optimize RaftKV into being Redis.
The interesting question is:
What is the measurable cost of quorum replication and linearizability?

That's a much better systems-project story.
29. Introduce bounded queues/backpressure
The runtime currently uses several:
mpsc::unbounded_channel()

That is convenient but can grow memory uncontrollably under overload.
Use bounded channels:
proposal queue
read queue
peer inbound queue
event stream

For example:
client proposal queue: 4096
read queue:            8192
network inbox:         16384

When saturated:
return BUSY
shed load
increment rejection metric

instead of allowing memory use to grow without bound.
30. Refactor duplicated action execution
execute_actions() handles Raft actions.
But MessageProcessor::process() implements essentially another version of action handling because it needs to extract a synchronous RPC response. 
This is dangerous long-term.
You'll eventually add behavior to one path and forget the other.
Create:
ActionExecutor

with something like:
struct ExecutionResult {
    peer_response: Option<Message>,
    events: Vec<RuntimeEvent>,
}

Then both:
runtime loop
gRPC request path

go through the same action executor.
This will substantially improve maintainability.
31. Better networking
Your architecture notes already acknowledge the extra-hop synchronous gRPC model. 
Eventually switch to long-lived bidirectional streams:
rpc RaftStream(stream Envelope)
    returns (stream Envelope);

Benefits:
connection reuse
fewer RPC allocations
responses naturally travel over same stream
better pipelining
easier flow control
easier per-peer metrics

Do this after correctness, not before.
32. Security
Do not expose the eventual admin dashboard remotely until this exists.
You need three separate trust domains:
client plane
peer plane
admin plane

Peer plane
mTLS
node certificates
cluster CA
node identity validation

Client plane
Eventually:
AUTH
ACL
optional TLS

Admin plane
Use:
TLS
authentication
RBAC
CSRF protection
secure cookies/token
audit log

Roles could be:
viewer
operator
admin

Viewer:
cluster
metrics
keys metadata

Operator:
snapshots
leadership transfer

Admin:
membership
delete keys
flush DB
restore backup

33. Audit log
Once GUI writes/admin controls exist, add audit records:
timestamp
identity
action
target
node
term
result
request ID

Example:
2026-10-05T12:31:44Z
user=admin
action=transfer_leadership
from=2
to=3
term=17
result=success

This makes the project feel like an actual distributed database control plane.
34. Backup and restore
Don't equate a Raft snapshot with a proper user-facing backup.
Implement:
raftkv-cli backup create backup.rkv
raftkv-cli backup inspect backup.rkv
raftkv-cli restore backup.rkv

Backup package:
backup.rkv
├── manifest
├── snapshot
├── checksum
└── metadata

Manifest:
format_version
cluster_id
created_at
last_included_index
last_included_term
state_hash
key_count
checksum

Restore should only occur with explicit cluster bootstrap/recovery semantics.
Do not casually restore into an active Raft member.
35. Dashboard architecture I recommend
The complete target architecture would be:
                   ┌─────────────────────────────┐
                   │ React + TypeScript Dashboard│
                   │ Vite                        │
                   │ TanStack Query              │
                   │ React Router                │
                   │ charts                      │
                   │ SSE client                  │
                   └──────────────┬──────────────┘
                                  │ HTTP / SSE
                                  ▼
                   ┌─────────────────────────────┐
                   │ Axum Admin/API Server       │
                   │                             │
                   │ /api/v1/cluster             │
                   │ /api/v1/keys                │
                   │ /api/v1/commands            │
                   │ /api/v1/admin               │
                   │ /api/v1/events              │
                   │ /metrics                    │
                   │ /health/*                   │
                   └──────────────┬──────────────┘
                                  │
                 ┌────────────────┴────────────────┐
                 │                                 │
                 ▼                                 ▼
       ┌──────────────────┐              ┌───────────────────┐
       │ Runtime facade   │              │ Event broadcaster │
       └────────┬─────────┘              └───────────────────┘
                │
     ┌──────────┼───────────┬──────────────┐
     ▼          ▼           ▼              ▼
 raft-core   storage    kv-state       peer network
   PURE

Crucially:
React must never directly interact with raft-core, sled files, or peer gRPC internals.

Everything passes through a typed runtime/control API.
36. Recommended HTTP API
Method	Endpoint	Purpose
GET	/api/v1/cluster	Overall cluster state
GET	/api/v1/nodes	All nodes
GET	/api/v1/nodes/:id	Node details
GET	/api/v1/events	SSE Raft events
GET	/api/v1/keys	SCAN-style enumeration
GET	/api/v1/keys/:key	Read value
PUT	/api/v1/keys/:key	SET
DELETE	/api/v1/keys/:key	DEL
POST	/api/v1/commands	Console
POST	/api/v1/admin/snapshot	Trigger snapshot
POST	/api/v1/admin/leadership	Transfer leader
POST	/api/v1/admin/members	Add member
DELETE	/api/v1/admin/members/:id	Remove member
GET	/api/v1/snapshots	Snapshot history
GET	/health/live	Process alive
GET	/health/ready	Ready for traffic
GET	/metrics	Prometheus


37. Dashboard screen structure
I would eventually build seven major areas:
Screen	Purpose	Phase
Overview	Cluster/node health	MVP
Key Explorer	Browse/edit KV data	MVP
Command Console	Redis-style command shell	MVP
Raft Visualizer	Election/replication animation	Phase 2
Metrics	High-level performance	Phase 2
Simulation Lab	Deterministic fault playground	Phase 2
Administration	snapshots/members/leader	Phase 3


This is much better than putting everything into one giant dashboard.
38. Kubernetes/Helm improvements
Once the backend is stable, improve your chart with:
readinessProbe
livenessProbe
startupProbe
PodDisruptionBudget
pod anti-affinity
topology spread constraints
PVC retention configuration
resource requests
resource limits
securityContext
NetworkPolicy
TLS secrets
ServiceMonitor
Grafana dashboard ConfigMap

For Raft, pod anti-affinity is particularly useful because putting all three nodes on one Kubernetes worker defeats much of the failure tolerance demonstration.
39. CI is worth fixing as a separate workstream
Your changelog says there is a CI workflow, but the current repository root I inspected does not expose a .github/workflows directory.
I would add a serious GitHub Actions pipeline:
format
clippy
unit tests
workspace tests
simulation seed sweep
Miri where practical
cargo-deny
cargo-audit
fuzz smoke tests
Docker build
Helm lint
integration cluster test

Then run deterministic simulation seeds automatically:
0..100

and eventually:
0..1000

on nightly CI.
Failure output must always contain the failing seed.
40. README claims should follow verification
A few capabilities in the current README are architecturally present but not yet fully end-to-end complete.
In particular I would be careful about presenting these as fully solved until the above fixes land:
Linearizable ReadIndex
complete snapshot installation
automatic TTL expiration
dynamic membership administration
Prometheus metrics
automatic snapshot thresholding

Your own changelog already correctly calls several of these unfinished. 
It is much stronger professionally to say:
Implemented core protocol; runtime integration under active validation

than to overclaim.
41. The implementation sequence I recommend
1. Correctness foundation. Fix even-sized commit quorum calculation, implement real ReadIndex quorum acknowledgement, correct restart last_applied restoration, make state-machine application crash/replay-safe, redesign TTL bookkeeping and replicated ticking, and add tests for all of these.
2. Snapshot system. Implement actual KV+TTL snapshot serialization, streaming transfer, restore, checksum validation, automatic threshold triggering, snapshot metrics, and restart/snapshot tests.
3. Control plane. Bind Admin and Membership gRPC services, implement dynamic member metadata and peer connection changes, add accurate client advertised addresses, leadership transfer endpoints, health checks, and typed diagnostics.
4. Observability. Add proper Prometheus metrics, peer replication statistics, state hashes, runtime event broadcaster, request latency histograms, structured tracing and request IDs.
5. GUI MVP. Build React + TypeScript with Overview, Key Explorer, and Command Console. Add Axum HTTP API and SSE. Keep Grafana for detailed time-series metrics.
6. Raft visualizer. Stream elections, replication, commit progression, conflict repair, ReadIndex rounds, leadership transfer and snapshots. Add log timeline and per-peer matchIndex/nextIndex.
7. Simulation Lab. Expose your deterministic simulator through a separate raftkv-lab backend. Add partition, crash, restart, latency, loss, duplication, reordering, pause, single-step and deterministic seed replay.
8. Reliability evidence. Add extensive simulator invariants, randomized seed sweeps, linearizability histories, Jepsen-style external tests, fuzzing, crash recovery suites and benchmarks.
9. Security/operations. Add TLS/mTLS, authentication, ACL/RBAC, audit logs, backup/restore, hardened Helm chart, NetworkPolicies and administrative safety controls.
10. Polish and release. Update documentation to exactly match tested behavior, publish benchmark results, add architectural diagrams and screenshots/video, then move from 0.1.0 unreleased toward a clearly defined 0.2.0 milestone.
42. What I would define as raftkv 0.2.0
A very strong 0.2.0 milestone would be:
RaftKV 0.2.0
│
├── Correctness
│   ├── verified ReadIndex
│   ├── even/odd quorum tests
│   ├── crash-safe replay
│   ├── working TTL
│   └── real snapshots
│
├── Administration
│   ├── Status
│   ├── Add/Remove member
│   ├── Transfer leader
│   └── Trigger snapshot
│
├── Client
│   ├── correct redirects
│   ├── SCAN
│   └── TTL
│
├── Observability
│   ├── Prometheus metrics
│   ├── tracing
│   ├── health endpoints
│   └── event stream
│
└── Dashboard
    ├── Overview
    ├── Key Explorer
    ├── Command Console
    └── Raft Visualizer

Then 0.3.0 can focus on:
Simulation Lab
chaos testing
backup/restore
TLS/mTLS
auth
benchmarks
operational hardening

Bottom line
The web dashboard is absolutely the strongest major feature to add, but I would not start by writing React components. Your project is now at the stage where its biggest gains come from turning the impressive protocol/core work into a provably correct, observable, controllable distributed system.
The best final version of RaftKV is not simply:
"A Redis-compatible KV store that uses Raft."

It becomes:
A from-scratch Raft-backed distributed key-value system with deterministic consensus simulation, linearizability verification, crash-safe persistence and snapshots, Redis-compatible access, live cluster observability, and an interactive visual environment for exploring consensus behavior.

That combination—pure Raft core + deterministic simulator + real storage/recovery + live visualizer + fault playground—is what would make this project genuinely distinctive.





    








Pasted text.txtDocument
To implmenet this give me all the GUI codes properly improvements and agents.md codex.md chatgpt.md files properly rest codes dont give Give me GUI Codes properly perefctly fully best gui possible modern witch matchees to the existing ui no emojis give me












Worked for 34s




Below is a complete frontend-only RaftKV control-plane/dashboard implementation. It does not modify raft-core, Rust storage, gRPC, RESP, or backend behavior. It assumes the browser-facing API/SSE contracts from your architecture, including /api/v1/cluster, /api/v1/keys, /api/v1/commands, /api/v1/events, and admin endpoints.    Pasted text
The design is intentionally restrained: dark enterprise UI, dense systems information, sharp typography, subtle motion, no gradients, no emojis, no oversized marketing-style elements.
1. Frontend structure
ui/dashboard/
├── package.json
├── index.html
├── vite.config.ts
├── tsconfig.json
├── .env.example
│
├── src/
│   ├── main.tsx
│   ├── App.tsx
│   │
│   ├── styles/
│   │   └── globals.css
│   │
│   ├── types/
│   │   └── api.ts
│   │
│   ├── lib/
│   │   ├── api.ts
│   │   ├── format.ts
│   │   └── mock.ts
│   │
│   ├── hooks/
│   │   ├── useCluster.ts
│   │   └── useRaftEvents.ts
│   │
│   ├── components/
│   │   ├── common/
│   │   │   ├── MetricCard.tsx
│   │   │   ├── Panel.tsx
│   │   │   ├── StatusPill.tsx
│   │   │   └── ConfirmDialog.tsx
│   │   │
│   │   ├── layout/
│   │   │   ├── AppShell.tsx
│   │   │   ├── Sidebar.tsx
│   │   │   └── Topbar.tsx
│   │   │
│   │   └── raft/
│   │       ├── NodeCard.tsx
│   │       ├── LogStrip.tsx
│   │       └── RaftGraph.tsx
│   │
│   └── pages/
│       ├── OverviewPage.tsx
│       ├── KeyExplorerPage.tsx
│       ├── CommandConsolePage.tsx
│       ├── RaftVisualizerPage.tsx
│       ├── MetricsPage.tsx
│       ├── SimulationLabPage.tsx
│       └── AdministrationPage.tsx
│
├── AGENTS.md
├── CODEX.md
└── CHATGPT.md

2. package.json
{
  "name": "raftkv-dashboard",
  "private": true,
  "version": "0.2.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "tsc -b && vite build",
    "preview": "vite preview",
    "typecheck": "tsc --noEmit"
  },
  "dependencies": {
    "@tanstack/react-query": "latest",
    "lucide-react": "latest",
    "react": "latest",
    "react-dom": "latest",
    "react-router-dom": "latest",
    "recharts": "latest"
  },
  "devDependencies": {
    "@types/react": "latest",
    "@types/react-dom": "latest",
    "@vitejs/plugin-react": "latest",
    "typescript": "latest",
    "vite": "latest"
  }
}

3. index.html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta
      name="viewport"
      content="width=device-width, initial-scale=1.0"
    />
    <meta
      name="theme-color"
      content="#0b0d10"
    />
    <meta
      name="description"
      content="RaftKV cluster dashboard"
    />
    <title>RaftKV</title>
  </head>

  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>

4. vite.config.ts
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    host: true,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8080",
        changeOrigin: true
      },
      "/health": {
        target: "http://127.0.0.1:8080",
        changeOrigin: true
      },
      "/metrics": {
        target: "http://127.0.0.1:8080",
        changeOrigin: true
      },
      "/lab": {
        target: "http://127.0.0.1:8090",
        changeOrigin: true
      }
    }
  }
});

5. tsconfig.json
{
  "compilerOptions": {
    "target": "ES2022",
    "useDefineForClassFields": true,
    "lib": ["ES2022", "DOM", "DOM.Iterable"],
    "allowJs": false,
    "skipLibCheck": true,
    "esModuleInterop": true,
    "allowSyntheticDefaultImports": true,
    "strict": true,
    "forceConsistentCasingInFileNames": true,
    "module": "ESNext",
    "moduleResolution": "Bundler",
    "resolveJsonModule": true,
    "isolatedModules": true,
    "noEmit": true,
    "jsx": "react-jsx"
  },
  "include": ["src"],
  "references": []
}

6. .env.example
VITE_API_BASE_URL=
VITE_DEMO_MODE=true

Set:
VITE_DEMO_MODE=false

when the Axum API exists.
7. src/main.tsx
import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

import App from "./App";
import "./styles/globals.css";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 2_000,
      refetchOnWindowFocus: false,
      retry: 1
    }
  }
});

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <App />
      </BrowserRouter>
    </QueryClientProvider>
  </React.StrictMode>
);

8. src/App.tsx
import { Navigate, Route, Routes } from "react-router-dom";

import AppShell from "./components/layout/AppShell";

import OverviewPage from "./pages/OverviewPage";
import KeyExplorerPage from "./pages/KeyExplorerPage";
import CommandConsolePage from "./pages/CommandConsolePage";
import RaftVisualizerPage from "./pages/RaftVisualizerPage";
import MetricsPage from "./pages/MetricsPage";
import SimulationLabPage from "./pages/SimulationLabPage";
import AdministrationPage from "./pages/AdministrationPage";

export default function App() {
  return (
    <Routes>
      <Route element={<AppShell />}>
        <Route path="/" element={<OverviewPage />} />
        <Route path="/keys" element={<KeyExplorerPage />} />
        <Route path="/console" element={<CommandConsolePage />} />
        <Route path="/raft" element={<RaftVisualizerPage />} />
        <Route path="/metrics" element={<MetricsPage />} />
        <Route path="/simulation" element={<SimulationLabPage />} />
        <Route path="/administration" element={<AdministrationPage />} />
      </Route>

      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}

9. src/types/api.ts
export type NodeRole =
  | "leader"
  | "follower"
  | "candidate"
  | "pre-candidate"
  | "unknown";

export type HealthState =
  | "healthy"
  | "degraded"
  | "unreachable"
  | "unknown";

export interface PeerDiagnostics {
  id: number;
  matchIndex: number;
  nextIndex: number;
  replicationLag: number;
  inflight: number;
  recentlyActive: boolean;
  snapshotInProgress: boolean;
  address?: string;
}

export interface NodeDiagnostics {
  nodeId: number;
  role: NodeRole;
  health: HealthState;

  term: number;
  leaderId: number | null;
  votedFor?: number | null;

  commitIndex: number;
  appliedIndex: number;
  lastLogIndex: number;
  snapshotIndex: number;

  stateHash: string;
  uptimeSeconds: number;

  raftAddress?: string;
  clientAddress?: string;
  adminAddress?: string;

  storageBytes?: number;
  keyCount?: number;

  peers: PeerDiagnostics[];
}

export interface LatencySummary {
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
}

export interface ClusterMetrics {
  requestsPerSecond: number;
  writeRequestsPerSecond: number;
  readRequestsPerSecond: number;

  latency: LatencySummary;

  electionsTotal: number;
  leadershipChangesTotal: number;
  appendRejectionsTotal: number;

  logBytes: number;
  snapshotBytes: number;
}

export interface ClusterSummary {
  clusterId?: string;

  health: HealthState;
  leaderId: number | null;
  term: number;

  commitIndex: number;
  appliedIndex: number;
  snapshotIndex: number;

  quorumSize: number;
  voterCount: number;

  configurationState: "stable" | "joint";

  totalKeys: number;

  nodes: NodeDiagnostics[];
  metrics: ClusterMetrics;
}

export interface KeySummary {
  key: string;
  type: "string" | "binary";
  sizeBytes: number;
  ttlMs: number | null;
}

export interface KeyPage {
  cursor: string | null;
  items: KeySummary[];
  totalApproximate?: number;
}

export interface KeyDetail extends KeySummary {
  encoding: "utf8" | "base64" | "hex";
  value: string;
}

export interface KeyListRequest {
  cursor?: string | null;
  limit?: number;
  pattern?: string;
}

export interface PutKeyRequest {
  value: string;
  encoding?: "utf8" | "base64" | "hex";
  ttlMs?: number | null;
}

export interface CommandRequest {
  command: string;
  targetNodeId?: number | null;
}

export interface CommandExecution {
  receivedByNodeId: number | null;
  leaderId: number | null;

  term?: number;
  proposedIndex?: number;

  committedMs?: number;
  appliedMs?: number;

  replicatedTo?: number;
  voterCount?: number;
}

export interface CommandResponse {
  raw: string;
  display: string;
  success: boolean;
  durationMs: number;
  execution?: CommandExecution;
}

export type RuntimeEventType =
  | "RoleChanged"
  | "TermChanged"
  | "MessageSent"
  | "MessageReceived"
  | "LogAppended"
  | "LogTruncated"
  | "CommitAdvanced"
  | "EntryApplied"
  | "SnapshotStarted"
  | "SnapshotInstalled"
  | "ReadIndexStarted"
  | "ReadIndexQuorumReached"
  | "LeadershipTransferStarted"
  | "ElectionStarted"
  | "ElectionWon"
  | "AppendRejected";

export interface RuntimeEvent {
  seq: number;
  timestamp?: string;

  nodeId: number;
  term: number;

  type: RuntimeEventType;

  peerId?: number;
  requestId?: number;

  prevLogIndex?: number;
  logIndex?: number;
  commitIndex?: number;

  entries?: number;

  detail?: string;
}

export interface SnapshotInfo {
  id: string;
  createdAt?: string;

  lastIncludedIndex: number;
  lastIncludedTerm: number;

  sizeBytes: number;
  stateHash?: string;
}

export interface MemberInfo {
  id: number;
  role: "voter" | "learner";

  raftAddress: string;
  clientAddress?: string;
  adminAddress?: string;

  health?: HealthState;
}

export interface AddMemberRequest {
  id: number;
  role: "voter" | "learner";

  raftAddress: string;
  clientAddress?: string;
  adminAddress?: string;
}

export interface LeadershipTransferRequest {
  targetId: number;
}

export interface SimulationConfig {
  seed: number;
  nodeCount: number;

  delayMinTicks: number;
  delayMaxTicks: number;

  dropProbability: number;
}

export interface SimulationNode {
  id: number;
  role: NodeRole;
  term: number;

  commitIndex: number;
  lastLogIndex: number;

  partitioned: boolean;
  crashed: boolean;
}

export interface SimulationState {
  running: boolean;
  tick: number;

  leaderId: number | null;
  seed: number;

  nodes: SimulationNode[];
  events: RuntimeEvent[];
}

10. src/lib/format.ts
export function formatNumber(value: number | null | undefined) {
  if (value === null || value === undefined) {
    return "—";
  }

  return new Intl.NumberFormat("en-US").format(value);
}

export function formatBytes(bytes: number | null | undefined) {
  if (bytes === null || bytes === undefined) {
    return "—";
  }

  if (bytes === 0) {
    return "0 B";
  }

  const units = ["B", "KB", "MB", "GB", "TB"];
  const exponent = Math.min(
    Math.floor(Math.log(bytes) / Math.log(1024)),
    units.length - 1
  );

  const value = bytes / 1024 ** exponent;

  return `${value >= 10 ? value.toFixed(0) : value.toFixed(1)} ${
    units[exponent]
  }`;
}

export function formatDuration(seconds: number | null | undefined) {
  if (seconds === null || seconds === undefined) {
    return "—";
  }

  if (seconds < 60) {
    return `${Math.floor(seconds)}s`;
  }

  const minutes = Math.floor(seconds / 60);

  if (minutes < 60) {
    return `${minutes}m`;
  }

  const hours = Math.floor(minutes / 60);

  if (hours < 24) {
    return `${hours}h ${minutes % 60}m`;
  }

  const days = Math.floor(hours / 24);

  return `${days}d ${hours % 24}h`;
}

export function formatTtl(ttlMs: number | null) {
  if (ttlMs === null) {
    return "Persistent";
  }

  if (ttlMs <= 0) {
    return "Expired";
  }

  const seconds = Math.floor(ttlMs / 1000);

  if (seconds < 60) {
    return `${seconds}s`;
  }

  const minutes = Math.floor(seconds / 60);

  if (minutes < 60) {
    return `${minutes}m ${seconds % 60}s`;
  }

  const hours = Math.floor(minutes / 60);

  return `${hours}h ${minutes % 60}m`;
}

export function formatRole(role: string) {
  switch (role) {
    case "leader":
      return "Leader";

    case "follower":
      return "Follower";

    case "candidate":
      return "Candidate";

    case "pre-candidate":
      return "Pre-candidate";

    default:
      return "Unknown";
  }
}

11. src/lib/mock.ts
import type {
  ClusterSummary,
  KeyDetail,
  KeyPage,
  RuntimeEvent,
  SimulationState,
  SnapshotInfo
} from "../types/api";

export const mockCluster: ClusterSummary = {
  clusterId: "local-development",
  health: "healthy",
  leaderId: 2,
  term: 17,

  commitIndex: 9812,
  appliedIndex: 9812,
  snapshotIndex: 7240,

  quorumSize: 2,
  voterCount: 3,

  configurationState: "stable",

  totalKeys: 12491,

  metrics: {
    requestsPerSecond: 864,
    writeRequestsPerSecond: 202,
    readRequestsPerSecond: 662,

    latency: {
      p50Ms: 2.8,
      p95Ms: 8.7,
      p99Ms: 15.4
    },

    electionsTotal: 6,
    leadershipChangesTotal: 4,
    appendRejectionsTotal: 14,

    logBytes: 38_400_000,
    snapshotBytes: 12_800_000
  },

  nodes: [
    {
      nodeId: 1,
      role: "follower",
      health: "healthy",

      term: 17,
      leaderId: 2,
      votedFor: 2,

      commitIndex: 9812,
      appliedIndex: 9812,
      lastLogIndex: 9812,
      snapshotIndex: 7240,

      stateHash: "0d23aa7ce8bf88736fe745bd1ceec7dc",

      uptimeSeconds: 36842,

      raftAddress: "node1:7001",
      clientAddress: "node1:6379",
      adminAddress: "node1:8080",

      storageBytes: 58_300_000,
      keyCount: 12491,

      peers: [
        {
          id: 2,
          matchIndex: 9812,
          nextIndex: 9813,
          replicationLag: 0,
          inflight: 0,
          recentlyActive: true,
          snapshotInProgress: false
        },
        {
          id: 3,
          matchIndex: 9811,
          nextIndex: 9812,
          replicationLag: 1,
          inflight: 0,
          recentlyActive: true,
          snapshotInProgress: false
        }
      ]
    },

    {
      nodeId: 2,
      role: "leader",
      health: "healthy",

      term: 17,
      leaderId: 2,
      votedFor: 2,

      commitIndex: 9812,
      appliedIndex: 9812,
      lastLogIndex: 9812,
      snapshotIndex: 7240,

      stateHash: "0d23aa7ce8bf88736fe745bd1ceec7dc",

      uptimeSeconds: 37102,

      raftAddress: "node2:7002",
      clientAddress: "node2:6380",
      adminAddress: "node2:8080",

      storageBytes: 58_700_000,
      keyCount: 12491,

      peers: [
        {
          id: 1,
          matchIndex: 9812,
          nextIndex: 9813,
          replicationLag: 0,
          inflight: 0,
          recentlyActive: true,
          snapshotInProgress: false
        },
        {
          id: 3,
          matchIndex: 9811,
          nextIndex: 9812,
          replicationLag: 1,
          inflight: 1,
          recentlyActive: true,
          snapshotInProgress: false
        }
      ]
    },

    {
      nodeId: 3,
      role: "follower",
      health: "healthy",

      term: 17,
      leaderId: 2,
      votedFor: 2,

      commitIndex: 9812,
      appliedIndex: 9811,
      lastLogIndex: 9811,
      snapshotIndex: 7240,

      stateHash: "b6ab6eb6995358a9119a935a88afd1ff",

      uptimeSeconds: 36791,

      raftAddress: "node3:7003",
      clientAddress: "node3:6381",
      adminAddress: "node3:8080",

      storageBytes: 58_100_000,
      keyCount: 12491,

      peers: [
        {
          id: 1,
          matchIndex: 9812,
          nextIndex: 9813,
          replicationLag: 0,
          inflight: 0,
          recentlyActive: true,
          snapshotInProgress: false
        },
        {
          id: 2,
          matchIndex: 9812,
          nextIndex: 9813,
          replicationLag: 0,
          inflight: 0,
          recentlyActive: true,
          snapshotInProgress: false
        }
      ]
    }
  ]
};

export const mockKeyPage: KeyPage = {
  cursor: null,
  totalApproximate: 12491,

  items: [
    {
      key: "user:1001",
      type: "string",
      sizeBytes: 241,
      ttlMs: null
    },
    {
      key: "user:1002",
      type: "string",
      sizeBytes: 188,
      ttlMs: 68_000
    },
    {
      key: "cart:442",
      type: "string",
      sizeBytes: 541,
      ttlMs: 185_000
    },
    {
      key: "session:8f2e",
      type: "string",
      sizeBytes: 780,
      ttlMs: 1_250_000
    },
    {
      key: "feature:checkout",
      type: "string",
      sizeBytes: 5,
      ttlMs: null
    }
  ]
};

export const mockKeyDetails: Record<string, KeyDetail> = {
  "user:1001": {
    key: "user:1001",
    type: "string",
    sizeBytes: 241,
    ttlMs: null,
    encoding: "utf8",
    value: `{
  "id": 1001,
  "name": "Jeevan",
  "plan": "developer",
  "active": true
}`
  },

  "user:1002": {
    key: "user:1002",
    type: "string",
    sizeBytes: 188,
    ttlMs: 68_000,
    encoding: "utf8",
    value: `{
  "id": 1002,
  "name": "Demo User",
  "active": true
}`
  },

  "cart:442": {
    key: "cart:442",
    type: "string",
    sizeBytes: 541,
    ttlMs: 185_000,
    encoding: "utf8",
    value: `{
  "items": [
    { "sku": "A1", "qty": 2 },
    { "sku": "B7", "qty": 1 }
  ]
}`
  }
};

export const mockEvents: RuntimeEvent[] = [
  {
    seq: 18842,
    nodeId: 2,
    term: 17,
    type: "MessageSent",
    peerId: 3,
    prevLogIndex: 9811,
    entries: 1,
    commitIndex: 9811,
    detail: "AppendEntries"
  },

  {
    seq: 18843,
    nodeId: 3,
    term: 17,
    type: "LogAppended",
    peerId: 2,
    logIndex: 9812,
    detail: "Normal entry appended"
  },

  {
    seq: 18844,
    nodeId: 2,
    term: 17,
    type: "CommitAdvanced",
    commitIndex: 9812,
    detail: "Quorum confirmed"
  },

  {
    seq: 18845,
    nodeId: 2,
    term: 17,
    type: "EntryApplied",
    logIndex: 9812,
    detail: "SET session:8f2e"
  },

  {
    seq: 18846,
    nodeId: 1,
    term: 17,
    type: "EntryApplied",
    logIndex: 9812,
    detail: "Follower applied committed entry"
  }
];

export const mockSnapshots: SnapshotInfo[] = [
  {
    id: "snapshot-7240",
    createdAt: "2026-10-05T07:20:14Z",
    lastIncludedIndex: 7240,
    lastIncludedTerm: 14,
    sizeBytes: 12_800_000,
    stateHash: "479fa7d449a994382da87f3f9e01f111"
  },

  {
    id: "snapshot-3880",
    createdAt: "2026-10-04T20:31:21Z",
    lastIncludedIndex: 3880,
    lastIncludedTerm: 11,
    sizeBytes: 8_410_000,
    stateHash: "10682f0aa40565afe40b6375ac2cefc8"
  }
];

export const mockSimulation: SimulationState = {
  running: false,
  tick: 842,
  leaderId: 2,
  seed: 51966,

  nodes: [
    {
      id: 1,
      role: "follower",
      term: 18,
      commitIndex: 190,
      lastLogIndex: 190,
      partitioned: false,
      crashed: false
    },
    {
      id: 2,
      role: "leader",
      term: 18,
      commitIndex: 190,
      lastLogIndex: 191,
      partitioned: false,
      crashed: false
    },
    {
      id: 3,
      role: "follower",
      term: 18,
      commitIndex: 189,
      lastLogIndex: 190,
      partitioned: false,
      crashed: false
    },
    {
      id: 4,
      role: "follower",
      term: 18,
      commitIndex: 188,
      lastLogIndex: 188,
      partitioned: true,
      crashed: false
    },
    {
      id: 5,
      role: "follower",
      term: 18,
      commitIndex: 190,
      lastLogIndex: 190,
      partitioned: false,
      crashed: false
    }
  ],

  events: mockEvents
};

12. src/lib/api.ts
import type {
  AddMemberRequest,
  ClusterSummary,
  CommandRequest,
  CommandResponse,
  KeyDetail,
  KeyListRequest,
  KeyPage,
  LeadershipTransferRequest,
  PutKeyRequest,
  SimulationConfig,
  SimulationState,
  SnapshotInfo
} from "../types/api";

import {
  mockCluster,
  mockKeyDetails,
  mockKeyPage,
  mockSimulation,
  mockSnapshots
} from "./mock";

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? "";
const DEMO_MODE = import.meta.env.VITE_DEMO_MODE === "true";

async function request<T>(
  path: string,
  init: RequestInit = {}
): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    ...init,

    headers: {
      "Content-Type": "application/json",
      ...init.headers
    }
  });

  if (!response.ok) {
    let message = `${response.status} ${response.statusText}`;

    try {
      const body = await response.json();

      if (body?.message) {
        message = body.message;
      }
    } catch {
      // Keep HTTP status.
    }

    throw new Error(message);
  }

  if (response.status === 204) {
    return undefined as T;
  }

  return response.json() as Promise<T>;
}

function sleep(ms = 160) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export const api = {
  async getCluster(): Promise<ClusterSummary> {
    if (DEMO_MODE) {
      await sleep();
      return structuredClone(mockCluster);
    }

    return request<ClusterSummary>("/api/v1/cluster");
  },

  async getKeys(params: KeyListRequest): Promise<KeyPage> {
    if (DEMO_MODE) {
      await sleep();

      const pattern = params.pattern?.trim().toLowerCase();

      if (!pattern) {
        return structuredClone(mockKeyPage);
      }

      return {
        ...structuredClone(mockKeyPage),

        items: mockKeyPage.items.filter((item) =>
          item.key.toLowerCase().includes(
            pattern.replaceAll("*", "")
          )
        )
      };
    }

    const query = new URLSearchParams();

    if (params.cursor) {
      query.set("cursor", params.cursor);
    }

    if (params.limit) {
      query.set("limit", String(params.limit));
    }

    if (params.pattern) {
      query.set("pattern", params.pattern);
    }

    return request<KeyPage>(`/api/v1/keys?${query}`);
  },

  async getKey(key: string): Promise<KeyDetail> {
    if (DEMO_MODE) {
      await sleep();

      return (
        structuredClone(mockKeyDetails[key]) ?? {
          key,
          type: "string",
          sizeBytes: 0,
          ttlMs: null,
          encoding: "utf8",
          value: ""
        }
      );
    }

    return request<KeyDetail>(
      `/api/v1/keys/${encodeURIComponent(key)}`
    );
  },

  async putKey(
    key: string,
    body: PutKeyRequest
  ): Promise<void> {
    if (DEMO_MODE) {
      await sleep();
      return;
    }

    return request<void>(
      `/api/v1/keys/${encodeURIComponent(key)}`,
      {
        method: "PUT",
        body: JSON.stringify(body)
      }
    );
  },

  async deleteKey(key: string): Promise<void> {
    if (DEMO_MODE) {
      await sleep();
      return;
    }

    return request<void>(
      `/api/v1/keys/${encodeURIComponent(key)}`,
      {
        method: "DELETE"
      }
    );
  },

  async executeCommand(
    body: CommandRequest
  ): Promise<CommandResponse> {
    if (DEMO_MODE) {
      await sleep(240);

      const command = body.command.trim();

      if (command.toUpperCase().startsWith("GET")) {
        return {
          raw: "$10\\r\\nhello raft\\r\\n",
          display: `"hello raft"`,
          success: true,
          durationMs: 3.1,

          execution: {
            receivedByNodeId: 2,
            leaderId: 2,
            term: 17,
            committedMs: 0,
            appliedMs: 0,
            replicatedTo: 3,
            voterCount: 3
          }
        };
      }

      return {
        raw: "+OK\\r\\n",
        display: "OK",
        success: true,
        durationMs: 8.4,

        execution: {
          receivedByNodeId: 2,
          leaderId: 2,
          term: 17,
          proposedIndex: 9813,
          committedMs: 7.8,
          appliedMs: 8.1,
          replicatedTo: 3,
          voterCount: 3
        }
      };
    }

    return request<CommandResponse>(
      "/api/v1/commands",
      {
        method: "POST",
        body: JSON.stringify(body)
      }
    );
  },

  async triggerSnapshot(): Promise<void> {
    if (DEMO_MODE) {
      await sleep();
      return;
    }

    return request<void>(
      "/api/v1/admin/snapshot",
      {
        method: "POST"
      }
    );
  },

  async transferLeadership(
    body: LeadershipTransferRequest
  ): Promise<void> {
    if (DEMO_MODE) {
      await sleep();
      return;
    }

    return request<void>(
      "/api/v1/admin/leadership",
      {
        method: "POST",
        body: JSON.stringify(body)
      }
    );
  },

  async addMember(body: AddMemberRequest): Promise<void> {
    if (DEMO_MODE) {
      await sleep();
      return;
    }

    return request<void>(
      "/api/v1/admin/members",
      {
        method: "POST",
        body: JSON.stringify(body)
      }
    );
  },

  async removeMember(id: number): Promise<void> {
    if (DEMO_MODE) {
      await sleep();
      return;
    }

    return request<void>(
      `/api/v1/admin/members/${id}`,
      {
        method: "DELETE"
      }
    );
  },

  async getSnapshots(): Promise<SnapshotInfo[]> {
    if (DEMO_MODE) {
      await sleep();
      return structuredClone(mockSnapshots);
    }

    return request<SnapshotInfo[]>(
      "/api/v1/snapshots"
    );
  },

  async getSimulation(): Promise<SimulationState> {
    if (DEMO_MODE) {
      await sleep();
      return structuredClone(mockSimulation);
    }

    return request<SimulationState>(
      "/lab/api/v1/state"
    );
  },

  async configureSimulation(
    body: SimulationConfig
  ): Promise<SimulationState> {
    if (DEMO_MODE) {
      await sleep();
      return structuredClone(mockSimulation);
    }

    return request<SimulationState>(
      "/lab/api/v1/configure",
      {
        method: "POST",
        body: JSON.stringify(body)
      }
    );
  },

  async simulationAction(
    action:
      | "start"
      | "pause"
      | "step"
      | "reset"
      | "heal",
    payload?: Record<string, unknown>
  ): Promise<SimulationState> {
    if (DEMO_MODE) {
      await sleep();
      return structuredClone(mockSimulation);
    }

    return request<SimulationState>(
      `/lab/api/v1/${action}`,
      {
        method: "POST",
        body: JSON.stringify(payload ?? {})
      }
    );
  }
};

export function getEventStreamUrl() {
  return `${API_BASE}/api/v1/events`;
}

export function isDemoMode() {
  return DEMO_MODE;
}

13. src/hooks/useCluster.ts
import { useQuery } from "@tanstack/react-query";
import { api } from "../lib/api";

export function useCluster() {
  return useQuery({
    queryKey: ["cluster"],
    queryFn: api.getCluster,
    refetchInterval: 2_000
  });
}

14. src/hooks/useRaftEvents.ts
import { useEffect, useRef, useState } from "react";

import { getEventStreamUrl, isDemoMode } from "../lib/api";
import { mockEvents } from "../lib/mock";

import type { RuntimeEvent } from "../types/api";

const MAX_EVENTS = 250;

export function useRaftEvents() {
  const [events, setEvents] = useState<RuntimeEvent[]>(
    isDemoMode() ? [...mockEvents].reverse() : []
  );

  const [connected, setConnected] = useState(
    isDemoMode()
  );

  const sourceRef = useRef<EventSource | null>(null);

  useEffect(() => {
    if (isDemoMode()) {
      return;
    }

    const source = new EventSource(getEventStreamUrl());

    sourceRef.current = source;

    source.onopen = () => {
      setConnected(true);
    };

    source.onerror = () => {
      setConnected(false);
    };

    source.onmessage = (message) => {
      try {
        const event = JSON.parse(
          message.data
        ) as RuntimeEvent;

        setEvents((current) =>
          [event, ...current].slice(0, MAX_EVENTS)
        );
      } catch {
        // Ignore malformed event.
      }
    };

    return () => {
      source.close();
      sourceRef.current = null;
    };
  }, []);

  function clear() {
    setEvents([]);
  }

  return {
    events,
    connected,
    clear
  };
}

15. src/components/common/Panel.tsx
import type {
  HTMLAttributes,
  ReactNode
} from "react";

interface PanelProps
  extends HTMLAttributes<HTMLDivElement> {
  title?: string;
  description?: string;
  action?: ReactNode;
  children: ReactNode;
}

export default function Panel({
  title,
  description,
  action,
  children,
  className = "",
  ...props
}: PanelProps) {
  return (
    <section
      className={`panel ${className}`}
      {...props}
    >
      {(title || description || action) && (
        <header className="panel__header">
          <div>
            {title && (
              <h2 className="panel__title">
                {title}
              </h2>
            )}

            {description && (
              <p className="panel__description">
                {description}
              </p>
            )}
          </div>

          {action && (
            <div className="panel__action">
              {action}
            </div>
          )}
        </header>
      )}

      <div className="panel__body">
        {children}
      </div>
    </section>
  );
}

16. src/components/common/MetricCard.tsx
import type { ReactNode } from "react";

interface MetricCardProps {
  label: string;
  value: ReactNode;
  detail?: ReactNode;
  tone?: "default" | "good" | "warn" | "danger";
}

export default function MetricCard({
  label,
  value,
  detail,
  tone = "default"
}: MetricCardProps) {
  return (
    <article
      className={`metric-card metric-card--${tone}`}
    >
      <div className="metric-card__label">
        {label}
      </div>

      <div className="metric-card__value">
        {value}
      </div>

      {detail && (
        <div className="metric-card__detail">
          {detail}
        </div>
      )}
    </article>
  );
}

17. src/components/common/StatusPill.tsx
interface StatusPillProps {
  status:
    | "healthy"
    | "degraded"
    | "unreachable"
    | "leader"
    | "follower"
    | "candidate"
    | "running"
    | "paused"
    | "stable"
    | "joint"
    | "connected"
    | "disconnected";

  label?: string;
}

export default function StatusPill({
  status,
  label
}: StatusPillProps) {
  return (
    <span
      className={`status-pill status-pill--${status}`}
    >
      <span className="status-pill__dot" />
      {label ?? status}
    </span>
  );
}

18. src/components/common/ConfirmDialog.tsx
import {
  useEffect,
  useRef
} from "react";

interface ConfirmDialogProps {
  open: boolean;
  title: string;
  description: string;
  confirmLabel: string;
  danger?: boolean;

  onConfirm: () => void;
  onClose: () => void;
}

export default function ConfirmDialog({
  open,
  title,
  description,
  confirmLabel,
  danger = false,
  onConfirm,
  onClose
}: ConfirmDialogProps) {
  const dialogRef =
    useRef<HTMLDialogElement>(null);

  useEffect(() => {
    const dialog = dialogRef.current;

    if (!dialog) {
      return;
    }

    if (open && !dialog.open) {
      dialog.showModal();
    }

    if (!open && dialog.open) {
      dialog.close();
    }
  }, [open]);

  return (
    <dialog
      ref={dialogRef}
      className="dialog"
      onCancel={(event) => {
        event.preventDefault();
        onClose();
      }}
    >
      <div className="dialog__content">
        <h3>{title}</h3>

        <p>{description}</p>

        <div className="dialog__actions">
          <button
            className="button button--ghost"
            onClick={onClose}
          >
            Cancel
          </button>

          <button
            className={
              danger
                ? "button button--danger"
                : "button button--primary"
            }
            onClick={() => {
              onConfirm();
              onClose();
            }}
          >
            {confirmLabel}
          </button>
        </div>
      </div>
    </dialog>
  );
}

19. src/components/layout/Sidebar.tsx
import {
  Activity,
  Boxes,
  Database,
  FlaskConical,
  Gauge,
  Network,
  Settings,
  TerminalSquare
} from "lucide-react";

import {
  NavLink
} from "react-router-dom";

const navItems = [
  {
    to: "/",
    label: "Overview",
    icon: Boxes,
    end: true
  },
  {
    to: "/keys",
    label: "Key Explorer",
    icon: Database
  },
  {
    to: "/console",
    label: "Command Console",
    icon: TerminalSquare
  },
  {
    to: "/raft",
    label: "Raft Visualizer",
    icon: Network
  },
  {
    to: "/metrics",
    label: "Metrics",
    icon: Gauge
  },
  {
    to: "/simulation",
    label: "Simulation Lab",
    icon: FlaskConical
  },
  {
    to: "/administration",
    label: "Administration",
    icon: Settings
  }
];

export default function Sidebar() {
  return (
    <aside className="sidebar">
      <div className="sidebar__brand">
        <div className="sidebar__brand-mark">
          <Activity size={18} strokeWidth={2} />
        </div>

        <div>
          <div className="sidebar__brand-title">
            RaftKV
          </div>

          <div className="sidebar__brand-subtitle">
            Control Plane
          </div>
        </div>
      </div>

      <nav className="sidebar__nav">
        <div className="sidebar__nav-label">
          Workspace
        </div>

        {navItems.map(
          ({ to, label, icon: Icon, end }) => (
            <NavLink
              key={to}
              to={to}
              end={end}
              className={({ isActive }) =>
                `sidebar-link ${
                  isActive
                    ? "sidebar-link--active"
                    : ""
                }`
              }
            >
              <Icon
                size={17}
                strokeWidth={1.8}
              />

              <span>{label}</span>
            </NavLink>
          )
        )}
      </nav>

      <div className="sidebar__footer">
        <div className="sidebar__version">
          <span>raftkv</span>
          <span>0.2 dashboard</span>
        </div>
      </div>
    </aside>
  );
}

20. src/components/layout/Topbar.tsx
import {
  CircleHelp,
  RefreshCw
} from "lucide-react";

import { useLocation } from "react-router-dom";

import { useCluster } from "../../hooks/useCluster";
import StatusPill from "../common/StatusPill";

const titles: Record<string, string> = {
  "/": "Cluster Overview",
  "/keys": "Key Explorer",
  "/console": "Command Console",
  "/raft": "Raft Visualizer",
  "/metrics": "Metrics",
  "/simulation": "Simulation Lab",
  "/administration": "Administration"
};

export default function Topbar() {
  const location = useLocation();

  const cluster = useCluster();

  const title =
    titles[location.pathname] ??
    "RaftKV";

  return (
    <header className="topbar">
      <div>
        <div className="topbar__eyebrow">
          Distributed KV control plane
        </div>

        <h1 className="topbar__title">
          {title}
        </h1>
      </div>

      <div className="topbar__actions">
        {cluster.data && (
          <>
            <div className="topbar__meta">
              <span>Term</span>
              <strong>
                {cluster.data.term}
              </strong>
            </div>

            <div className="topbar__meta">
              <span>Leader</span>
              <strong>
                {cluster.data.leaderId
                  ? `Node ${cluster.data.leaderId}`
                  : "None"}
              </strong>
            </div>

            <StatusPill
              status={
                cluster.data.health === "healthy"
                  ? "healthy"
                  : "degraded"
              }
              label={
                cluster.data.health === "healthy"
                  ? "Cluster healthy"
                  : "Cluster degraded"
              }
            />
          </>
        )}

        <button
          className="icon-button"
          title="Refresh cluster state"
          onClick={() => cluster.refetch()}
        >
          <RefreshCw size={16} />
        </button>

        <button
          className="icon-button"
          title="About this dashboard"
        >
          <CircleHelp size={16} />
        </button>
      </div>
    </header>
  );
}

21. src/components/layout/AppShell.tsx
import { Outlet } from "react-router-dom";

import Sidebar from "./Sidebar";
import Topbar from "./Topbar";

export default function AppShell() {
  return (
    <div className="app-shell">
      <Sidebar />

      <div className="app-shell__main">
        <Topbar />

        <main className="page-container">
          <Outlet />
        </main>
      </div>
    </div>
  );
}

22. src/components/raft/NodeCard.tsx
import {
  Database,
  Network,
  Server,
  TimerReset
} from "lucide-react";

import type { NodeDiagnostics } from "../../types/api";

import {
  formatBytes,
  formatNumber,
  formatRole
} from "../../lib/format";

import StatusPill from "../common/StatusPill";

interface NodeCardProps {
  node: NodeDiagnostics;
  onClick?: () => void;
}

export default function NodeCard({
  node,
  onClick
}: NodeCardProps) {
  const maxPeerLag = Math.max(
    0,
    ...node.peers.map(
      (peer) => peer.replicationLag
    )
  );

  return (
    <article
      className={`node-card ${
        node.role === "leader"
          ? "node-card--leader"
          : ""
      }`}
      onClick={onClick}
    >
      <header className="node-card__header">
        <div className="node-card__identity">
          <div className="node-card__icon">
            <Server size={17} />
          </div>

          <div>
            <h3>Node {node.nodeId}</h3>

            <span>
              {node.clientAddress ?? "Client address unavailable"}
            </span>
          </div>
        </div>

        <StatusPill
          status={
            node.role === "leader"
              ? "leader"
              : node.role === "candidate"
                ? "candidate"
                : "follower"
          }
          label={formatRole(node.role)}
        />
      </header>

      <div className="node-card__metrics">
        <div>
          <span>Term</span>
          <strong>{formatNumber(node.term)}</strong>
        </div>

        <div>
          <span>Commit</span>
          <strong>
            {formatNumber(node.commitIndex)}
          </strong>
        </div>

        <div>
          <span>Applied</span>
          <strong>
            {formatNumber(node.appliedIndex)}
          </strong>
        </div>

        <div>
          <span>Last log</span>
          <strong>
            {formatNumber(node.lastLogIndex)}
          </strong>
        </div>
      </div>

      <div className="node-card__footer">
        <span>
          <Network size={14} />
          Max peer lag {formatNumber(maxPeerLag)}
        </span>

        <span>
          <Database size={14} />
          {formatBytes(node.storageBytes)}
        </span>

        <span>
          <TimerReset size={14} />
          {Math.floor(node.uptimeSeconds / 3600)}h uptime
        </span>
      </div>
    </article>
  );
}

23. src/components/raft/LogStrip.tsx
interface LogEntryView {
  index: number;
  term: number;
  state:
    | "applied"
    | "committed"
    | "uncommitted"
    | "conflict";
}

interface LogStripProps {
  entries: LogEntryView[];
  snapshotIndex?: number;
}

export default function LogStrip({
  entries,
  snapshotIndex
}: LogStripProps) {
  return (
    <div className="log-strip">
      {snapshotIndex !== undefined && (
        <div className="log-strip__snapshot">
          ≤ {snapshotIndex}
        </div>
      )}

      {entries.map((entry) => (
        <div
          key={entry.index}
          className={`log-entry log-entry--${entry.state}`}
          title={`Index ${entry.index}, term ${entry.term}, ${entry.state}`}
        >
          <span>{entry.index}</span>
          <small>t{entry.term}</small>
        </div>
      ))}
    </div>
  );
}

24. src/components/raft/RaftGraph.tsx
import type {
  ClusterSummary,
  RuntimeEvent
} from "../../types/api";

import {
  formatNumber,
  formatRole
} from "../../lib/format";

interface RaftGraphProps {
  cluster: ClusterSummary;
  events?: RuntimeEvent[];
}

const POSITIONS = [
  { x: 50, y: 12 },
  { x: 16, y: 67 },
  { x: 84, y: 67 },
  { x: 8, y: 32 },
  { x: 92, y: 32 }
];

export default function RaftGraph({
  cluster,
  events = []
}: RaftGraphProps) {
  const recentMessages = events
    .filter(
      (event) =>
        event.type === "MessageSent" &&
        event.peerId !== undefined
    )
    .slice(0, 8);

  const positionFor = (nodeId: number) => {
    const index = cluster.nodes.findIndex(
      (node) => node.nodeId === nodeId
    );

    return (
      POSITIONS[index] ?? {
        x: 50,
        y: 50
      }
    );
  };

  return (
    <div className="raft-graph">
      <svg
        className="raft-graph__edges"
        viewBox="0 0 100 100"
        preserveAspectRatio="none"
      >
        {cluster.nodes.flatMap((node) =>
          cluster.nodes
            .filter(
              (other) =>
                other.nodeId > node.nodeId
            )
            .map((other) => {
              const a = positionFor(node.nodeId);
              const b = positionFor(other.nodeId);

              return (
                <line
                  key={`${node.nodeId}-${other.nodeId}`}
                  x1={a.x}
                  y1={a.y}
                  x2={b.x}
                  y2={b.y}
                  vectorEffect="non-scaling-stroke"
                />
              );
            })
        )}

        {recentMessages.map((event) => {
          const from = positionFor(event.nodeId);
          const to = positionFor(event.peerId!);

          return (
            <line
              key={event.seq}
              className="raft-graph__message"
              x1={from.x}
              y1={from.y}
              x2={to.x}
              y2={to.y}
              vectorEffect="non-scaling-stroke"
            />
          );
        })}
      </svg>

      {cluster.nodes.map((node) => {
        const position =
          positionFor(node.nodeId);

        return (
          <div
            key={node.nodeId}
            className={`raft-node ${
              node.role === "leader"
                ? "raft-node--leader"
                : ""
            }`}
            style={{
              left: `${position.x}%`,
              top: `${position.y}%`
            }}
          >
            <div className="raft-node__top">
              <strong>
                Node {node.nodeId}
              </strong>

              <span>
                {formatRole(node.role)}
              </span>
            </div>

            <div className="raft-node__stats">
              <span>t{node.term}</span>

              <span>
                c{formatNumber(node.commitIndex)}
              </span>

              <span>
                a{formatNumber(node.appliedIndex)}
              </span>
            </div>
          </div>
        );
      })}
    </div>
  );
}

25. src/pages/OverviewPage.tsx
import {
  Activity,
  Database,
  Gauge,
  Network,
  ShieldCheck
} from "lucide-react";

import { useCluster } from "../hooks/useCluster";

import {
  formatBytes,
  formatNumber
} from "../lib/format";

import MetricCard from "../components/common/MetricCard";
import Panel from "../components/common/Panel";
import StatusPill from "../components/common/StatusPill";
import NodeCard from "../components/raft/NodeCard";

export default function OverviewPage() {
  const cluster = useCluster();

  if (cluster.isLoading) {
    return (
      <div className="page-state">
        Loading cluster state
      </div>
    );
  }

  if (!cluster.data) {
    return (
      <div className="page-state page-state--error">
        Cluster data is unavailable.
      </div>
    );
  }

  const data = cluster.data;

  const maxLogIndex = Math.max(
    ...data.nodes.map(
      (node) => node.lastLogIndex
    ),
    1
  );

  return (
    <div className="stack-xl">
      <section className="hero-strip">
        <div>
          <div className="hero-strip__eyebrow">
            Live cluster
          </div>

          <h2>
            {data.voterCount}-node Raft cluster
          </h2>

          <p>
            Consensus state, replication
            progress, client traffic and
            storage health in one operational
            view.
          </p>
        </div>

        <div className="hero-strip__status">
          <StatusPill
            status={
              data.health === "healthy"
                ? "healthy"
                : "degraded"
            }
            label={
              data.health === "healthy"
                ? "Quorum available"
                : "Quorum degraded"
            }
          />

          <div className="hero-strip__kv">
            <span>Leader</span>
            <strong>
              {data.leaderId
                ? `Node ${data.leaderId}`
                : "None"}
            </strong>
          </div>

          <div className="hero-strip__kv">
            <span>Configuration</span>
            <strong>
              {data.configurationState}
            </strong>
          </div>
        </div>
      </section>

      <section className="metric-grid metric-grid--5">
        <MetricCard
          label="Current term"
          value={data.term}
          detail={
            <span className="metric-detail">
              <ShieldCheck size={13} />
              Leader Node {data.leaderId ?? "—"}
            </span>
          }
        />

        <MetricCard
          label="Commit index"
          value={formatNumber(data.commitIndex)}
          detail={
            <span className="metric-detail">
              <Network size={13} />
              Quorum {data.quorumSize}/{data.voterCount}
            </span>
          }
        />

        <MetricCard
          label="Total keys"
          value={formatNumber(data.totalKeys)}
          detail={
            <span className="metric-detail">
              <Database size={13} />
              Replicated state machine
            </span>
          }
        />

        <MetricCard
          label="Requests / sec"
          value={formatNumber(
            Math.round(
              data.metrics.requestsPerSecond
            )
          )}
          detail={
            <span className="metric-detail">
              <Activity size={13} />
              {formatNumber(
                Math.round(
                  data.metrics.readRequestsPerSecond
                )
              )}{" "}
              reads
            </span>
          }
        />

        <MetricCard
          label="P99 latency"
          value={`${data.metrics.latency.p99Ms.toFixed(
            1
          )} ms`}
          detail={
            <span className="metric-detail">
              <Gauge size={13} />
              P50{" "}
              {data.metrics.latency.p50Ms.toFixed(
                1
              )}{" "}
              ms
            </span>
          }
        />
      </section>

      <section className="node-grid">
        {data.nodes.map((node) => (
          <NodeCard
            key={node.nodeId}
            node={node}
          />
        ))}
      </section>

      <div className="overview-grid">
        <Panel
          title="Replication progress"
          description="Per-node log position relative to the most advanced replica."
        >
          <div className="replication-list">
            {data.nodes.map((node) => {
              const percentage =
                (node.lastLogIndex /
                  maxLogIndex) *
                100;

              return (
                <div
                  key={node.nodeId}
                  className="replication-row"
                >
                  <div className="replication-row__head">
                    <div>
                      <strong>
                        Node {node.nodeId}
                      </strong>

                      <span>
                        {node.role}
                      </span>
                    </div>

                    <span>
                      {formatNumber(
                        node.lastLogIndex
                      )}
                    </span>
                  </div>

                  <div className="progress-track">
                    <div
                      className="progress-track__fill"
                      style={{
                        width: `${percentage}%`
                      }}
                    />
                  </div>

                  <div className="replication-row__foot">
                    <span>
                      Commit{" "}
                      {formatNumber(
                        node.commitIndex
                      )}
                    </span>

                    <span>
                      Applied{" "}
                      {formatNumber(
                        node.appliedIndex
                      )}
                    </span>

                    <span>
                      Snapshot{" "}
                      {formatNumber(
                        node.snapshotIndex
                      )}
                    </span>
                  </div>
                </div>
              );
            })}
          </div>
        </Panel>

        <Panel
          title="Storage"
          description="Current replicated log and snapshot footprint."
        >
          <div className="storage-summary">
            <div>
              <span>Raft log</span>
              <strong>
                {formatBytes(
                  data.metrics.logBytes
                )}
              </strong>
            </div>

            <div>
              <span>Snapshots</span>
              <strong>
                {formatBytes(
                  data.metrics.snapshotBytes
                )}
              </strong>
            </div>

            <div>
              <span>Snapshot index</span>
              <strong>
                {formatNumber(
                  data.snapshotIndex
                )}
              </strong>
            </div>

            <div>
              <span>Entries since snapshot</span>
              <strong>
                {formatNumber(
                  Math.max(
                    0,
                    data.appliedIndex -
                      data.snapshotIndex
                  )
                )}
              </strong>
            </div>
          </div>

          <div className="hash-block">
            <span>Leader state hash</span>

            <code>
              {data.nodes.find(
                (node) =>
                  node.nodeId === data.leaderId
              )?.stateHash ?? "Unavailable"}
            </code>
          </div>
        </Panel>
      </div>
    </div>
  );
}

26. src/pages/KeyExplorerPage.tsx
import {
  Copy,
  Database,
  RefreshCw,
  Save,
  Search,
  Trash2
} from "lucide-react";

import {
  useEffect,
  useState
} from "react";

import {
  useMutation,
  useQuery,
  useQueryClient
} from "@tanstack/react-query";

import { api } from "../lib/api";

import {
  formatBytes,
  formatTtl
} from "../lib/format";

import Panel from "../components/common/Panel";
import ConfirmDialog from "../components/common/ConfirmDialog";

export default function KeyExplorerPage() {
  const queryClient = useQueryClient();

  const [pattern, setPattern] =
    useState("");

  const [selectedKey, setSelectedKey] =
    useState<string | null>(null);

  const [draftValue, setDraftValue] =
    useState("");

  const [confirmDelete, setConfirmDelete] =
    useState(false);

  const keys = useQuery({
    queryKey: ["keys", pattern],
    queryFn: () =>
      api.getKeys({
        pattern,
        limit: 100
      })
  });

  const detail = useQuery({
    queryKey: ["key", selectedKey],
    queryFn: () => api.getKey(selectedKey!),
    enabled: selectedKey !== null
  });

  useEffect(() => {
    if (detail.data) {
      setDraftValue(detail.data.value);
    }
  }, [detail.data]);

  useEffect(() => {
    if (
      !selectedKey &&
      keys.data?.items.length
    ) {
      setSelectedKey(
        keys.data.items[0].key
      );
    }
  }, [keys.data, selectedKey]);

  const save = useMutation({
    mutationFn: () =>
      api.putKey(selectedKey!, {
        value: draftValue,
        encoding:
          detail.data?.encoding ?? "utf8",
        ttlMs: detail.data?.ttlMs ?? null
      }),

    onSuccess: async () => {
      await queryClient.invalidateQueries({
        queryKey: ["key", selectedKey]
      });

      await queryClient.invalidateQueries({
        queryKey: ["keys"]
      });
    }
  });

  const remove = useMutation({
    mutationFn: () =>
      api.deleteKey(selectedKey!),

    onSuccess: async () => {
      setSelectedKey(null);

      await queryClient.invalidateQueries({
        queryKey: ["keys"]
      });
    }
  });

  async function copyText(value: string) {
    await navigator.clipboard.writeText(
      value
    );
  }

  return (
    <>
      <div className="key-explorer">
        <Panel
          className="key-browser"
          title="Keys"
          description={
            keys.data?.totalApproximate
              ? `Approximately ${keys.data.totalApproximate.toLocaleString()} keys`
              : "Browse replicated keys"
          }
          action={
            <button
              className="icon-button"
              onClick={() => keys.refetch()}
            >
              <RefreshCw size={15} />
            </button>
          }
        >
          <div className="search-field">
            <Search size={15} />

            <input
              value={pattern}
              onChange={(event) =>
                setPattern(
                  event.target.value
                )
              }
              placeholder="Filter keys, for example user:*"
            />
          </div>

          <div className="key-list">
            {keys.data?.items.map(
              (item) => (
                <button
                  key={item.key}
                  className={`key-list-item ${
                    selectedKey === item.key
                      ? "key-list-item--active"
                      : ""
                  }`}
                  onClick={() =>
                    setSelectedKey(item.key)
                  }
                >
                  <div className="key-list-item__main">
                    <Database size={14} />

                    <span>{item.key}</span>
                  </div>

                  <div className="key-list-item__meta">
                    <span>
                      {formatBytes(
                        item.sizeBytes
                      )}
                    </span>

                    <span>
                      {formatTtl(item.ttlMs)}
                    </span>
                  </div>
                </button>
              )
            )}
          </div>
        </Panel>

        <Panel
          className="key-editor"
          title={
            selectedKey ??
            "Select a key"
          }
          description={
            detail.data
              ? "Linearizable value view"
              : "Select a key from the browser"
          }
          action={
            detail.data && (
              <div className="inline-actions">
                <button
                  className="button button--ghost button--small"
                  onClick={() =>
                    copyText(detail.data!.key)
                  }
                >
                  <Copy size={14} />
                  Copy key
                </button>

                <button
                  className="button button--danger-ghost button--small"
                  onClick={() =>
                    setConfirmDelete(true)
                  }
                >
                  <Trash2 size={14} />
                  Delete
                </button>
              </div>
            )
          }
        >
          {!detail.data ? (
            <div className="empty-panel">
              No key selected.
            </div>
          ) : (
            <div className="key-detail">
              <div className="key-detail__metadata">
                <div>
                  <span>Type</span>
                  <strong>
                    {detail.data.type}
                  </strong>
                </div>

                <div>
                  <span>Size</span>
                  <strong>
                    {formatBytes(
                      detail.data.sizeBytes
                    )}
                  </strong>
                </div>

                <div>
                  <span>TTL</span>
                  <strong>
                    {formatTtl(
                      detail.data.ttlMs
                    )}
                  </strong>
                </div>

                <div>
                  <span>Encoding</span>
                  <strong>
                    {detail.data.encoding.toUpperCase()}
                  </strong>
                </div>
              </div>

              <div className="field-group">
                <div className="field-label-row">
                  <label>Value</label>

                  <button
                    className="text-button"
                    onClick={() =>
                      copyText(draftValue)
                    }
                  >
                    <Copy size={13} />
                    Copy
                  </button>
                </div>

                <textarea
                  className="code-editor"
                  value={draftValue}
                  spellCheck={false}
                  onChange={(event) =>
                    setDraftValue(
                      event.target.value
                    )
                  }
                />
              </div>

              <div className="replication-warning">
                Changes made here are submitted
                through the leader and replicated
                through Raft before they are
                considered committed.
              </div>

              <div className="form-actions">
                <button
                  className="button button--primary"
                  disabled={save.isPending}
                  onClick={() =>
                    save.mutate()
                  }
                >
                  <Save size={15} />

                  {save.isPending
                    ? "Saving"
                    : "Save value"}
                </button>
              </div>
            </div>
          )}
        </Panel>
      </div>

      <ConfirmDialog
        open={confirmDelete}
        title="Delete replicated key"
        description={`Delete "${selectedKey}" from the replicated state machine? This operation will be committed through Raft.`}
        confirmLabel="Delete key"
        danger
        onClose={() =>
          setConfirmDelete(false)
        }
        onConfirm={() =>
          remove.mutate()
        }
      />
    </>
  );
}

27. src/pages/CommandConsolePage.tsx
import {
  ChevronRight,
  Clock3,
  Copy,
  Network,
  Play,
  Server
} from "lucide-react";

import {
  KeyboardEvent,
  useState
} from "react";

import { api } from "../lib/api";

import type {
  CommandResponse
} from "../types/api";

import Panel from "../components/common/Panel";

interface ConsoleEntry {
  id: number;
  command: string;
  response: CommandResponse;
}

export default function CommandConsolePage() {
  const [command, setCommand] =
    useState("INFO raft");

  const [history, setHistory] =
    useState<ConsoleEntry[]>([]);

  const [running, setRunning] =
    useState(false);

  async function execute() {
    const trimmed = command.trim();

    if (!trimmed || running) {
      return;
    }

    setRunning(true);

    try {
      const response =
        await api.executeCommand({
          command: trimmed
        });

      setHistory((current) => [
        {
          id: Date.now(),
          command: trimmed,
          response
        },
        ...current
      ]);
    } finally {
      setRunning(false);
    }
  }

  function onKeyDown(
    event: KeyboardEvent<HTMLInputElement>
  ) {
    if (
      event.key === "Enter" &&
      !event.shiftKey
    ) {
      event.preventDefault();
      execute();
    }
  }

  return (
    <div className="console-layout">
      <Panel
        className="console-panel"
        title="Command Console"
        description="Execute supported RESP commands against RaftKV."
      >
        <div className="console-input">
          <span className="console-prompt">
            raftkv&gt;
          </span>

          <input
            value={command}
            onChange={(event) =>
              setCommand(
                event.target.value
              )
            }
            onKeyDown={onKeyDown}
            spellCheck={false}
          />

          <button
            className="button button--primary"
            onClick={execute}
            disabled={running}
          >
            <Play size={14} />
            Run
          </button>
        </div>

        <div className="command-presets">
          {[
            "INFO raft",
            "CLUSTER NODES",
            "DBSIZE",
            "SET greeting \"hello raft\"",
            "GET greeting"
          ].map((preset) => (
            <button
              key={preset}
              onClick={() =>
                setCommand(preset)
              }
            >
              {preset}
            </button>
          ))}
        </div>

        <div className="console-history">
          {history.length === 0 && (
            <div className="console-empty">
              Run a command to start the
              session.
            </div>
          )}

          {history.map((entry) => (
            <article
              key={entry.id}
              className="console-entry"
            >
              <div className="console-entry__command">
                <ChevronRight size={14} />
                {entry.command}
              </div>

              <pre
                className={
                  entry.response.success
                    ? "console-entry__response"
                    : "console-entry__response console-entry__response--error"
                }
              >
                {entry.response.display}
              </pre>

              <div className="console-entry__meta">
                <span>
                  <Clock3 size={13} />
                  {entry.response.durationMs.toFixed(
                    2
                  )}{" "}
                  ms
                </span>

                {entry.response.execution
                  ?.receivedByNodeId && (
                  <span>
                    <Server size={13} />
                    Received by Node{" "}
                    {
                      entry.response.execution
                        .receivedByNodeId
                    }
                  </span>
                )}

                {entry.response.execution
                  ?.replicatedTo && (
                  <span>
                    <Network size={13} />
                    Replicated{" "}
                    {
                      entry.response.execution
                        .replicatedTo
                    }
                    /
                    {
                      entry.response.execution
                        .voterCount
                    }
                  </span>
                )}

                <button
                  onClick={() =>
                    navigator.clipboard.writeText(
                      entry.response.raw
                    )
                  }
                >
                  <Copy size={13} />
                  Raw response
                </button>
              </div>

              {entry.response.execution && (
                <div className="execution-grid">
                  <div>
                    <span>Leader</span>
                    <strong>
                      Node{" "}
                      {entry.response.execution
                        .leaderId ?? "—"}
                    </strong>
                  </div>

                  <div>
                    <span>Term</span>
                    <strong>
                      {entry.response.execution
                        .term ?? "—"}
                    </strong>
                  </div>

                  <div>
                    <span>Log index</span>
                    <strong>
                      {entry.response.execution
                        .proposedIndex ?? "Read"}
                    </strong>
                  </div>

                  <div>
                    <span>Commit</span>
                    <strong>
                      {entry.response.execution
                        .committedMs !==
                      undefined
                        ? `${entry.response.execution.committedMs.toFixed(
                            1
                          )} ms`
                        : "—"}
                    </strong>
                  </div>

                  <div>
                    <span>Apply</span>
                    <strong>
                      {entry.response.execution
                        .appliedMs !==
                      undefined
                        ? `${entry.response.execution.appliedMs.toFixed(
                            1
                          )} ms`
                        : "—"}
                    </strong>
                  </div>
                </div>
              )}
            </article>
          ))}
        </div>
      </Panel>
    </div>
  );
}

28. src/pages/RaftVisualizerPage.tsx
import {
  Pause,
  Play,
  Radio,
  Trash2
} from "lucide-react";

import { useState } from "react";

import { useCluster } from "../hooks/useCluster";
import { useRaftEvents } from "../hooks/useRaftEvents";

import Panel from "../components/common/Panel";
import StatusPill from "../components/common/StatusPill";

import RaftGraph from "../components/raft/RaftGraph";
import LogStrip from "../components/raft/LogStrip";

export default function RaftVisualizerPage() {
  const cluster = useCluster();

  const {
    events,
    connected,
    clear
  } = useRaftEvents();

  const [paused, setPaused] =
    useState(false);

  if (!cluster.data) {
    return (
      <div className="page-state">
        Loading cluster visualization
      </div>
    );
  }

  const visibleEvents = paused
    ? []
    : events;

  return (
    <div className="visualizer-layout">
      <Panel
        title="Live consensus topology"
        description="Current roles, replication links and recent Raft traffic."
        action={
          <div className="inline-actions">
            <StatusPill
              status={
                connected
                  ? "connected"
                  : "disconnected"
              }
              label={
                connected
                  ? "Event stream connected"
                  : "Event stream disconnected"
              }
            />

            <button
              className="button button--ghost button--small"
              onClick={() =>
                setPaused((value) => !value)
              }
            >
              {paused ? (
                <Play size={14} />
              ) : (
                <Pause size={14} />
              )}

              {paused ? "Resume" : "Pause"}
            </button>
          </div>
        }
      >
        <RaftGraph
          cluster={cluster.data}
          events={visibleEvents}
        />
      </Panel>

      <div className="visualizer-bottom">
        <Panel
          title="Replicated logs"
          description="Compact representation of recent log positions."
        >
          <div className="node-log-list">
            {cluster.data.nodes.map(
              (node) => {
                const begin = Math.max(
                  node.snapshotIndex + 1,
                  node.lastLogIndex - 7
                );

                const entries =
                  Array.from(
                    {
                      length:
                        node.lastLogIndex >= begin
                          ? node.lastLogIndex -
                            begin +
                            1
                          : 0
                    },
                    (_, index) => {
                      const logIndex =
                        begin + index;

                      return {
                        index: logIndex,
                        term: node.term,

                        state:
                          logIndex <=
                          node.appliedIndex
                            ? ("applied" as const)
                            : logIndex <=
                                node.commitIndex
                              ? ("committed" as const)
                              : ("uncommitted" as const)
                      };
                    }
                  );

                return (
                  <div
                    key={node.nodeId}
                    className="node-log-row"
                  >
                    <div className="node-log-row__title">
                      <strong>
                        Node {node.nodeId}
                      </strong>

                      <span>{node.role}</span>
                    </div>

                    <LogStrip
                      entries={entries}
                      snapshotIndex={
                        node.snapshotIndex
                      }
                    />
                  </div>
                );
              }
            )}
          </div>
        </Panel>

        <Panel
          title="Event stream"
          description="Latest consensus and runtime events."
          action={
            <button
              className="button button--ghost button--small"
              onClick={clear}
            >
              <Trash2 size={14} />
              Clear
            </button>
          }
        >
          <div className="event-stream">
            {events.slice(0, 80).map(
              (event) => (
                <div
                  key={event.seq}
                  className="event-row"
                >
                  <div className="event-row__sequence">
                    #{event.seq}
                  </div>

                  <div className="event-row__icon">
                    <Radio size={13} />
                  </div>

                  <div className="event-row__content">
                    <div>
                      <strong>
                        {event.type}
                      </strong>

                      <span>
                        Node {event.nodeId}
                      </span>

                      {event.peerId && (
                        <span>
                          Peer {event.peerId}
                        </span>
                      )}

                      <span>
                        Term {event.term}
                      </span>
                    </div>

                    {event.detail && (
                      <p>{event.detail}</p>
                    )}
                  </div>
                </div>
              )
            )}
          </div>
        </Panel>
      </div>
    </div>
  );
}

29. src/pages/MetricsPage.tsx
import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";

import { useMemo } from "react";

import { useCluster } from "../hooks/useCluster";

import {
  formatBytes,
  formatNumber
} from "../lib/format";

import MetricCard from "../components/common/MetricCard";
import Panel from "../components/common/Panel";

export default function MetricsPage() {
  const cluster = useCluster();

  const data = useMemo(
    () =>
      Array.from({ length: 30 }, (_, index) => {
        const baseline =
          cluster.data?.metrics
            .requestsPerSecond ?? 700;

        return {
          second: index - 29,

          requests:
            baseline +
            Math.sin(index / 3) * 90 +
            Math.cos(index / 5) * 55,

          latency:
            (cluster.data?.metrics.latency
              .p95Ms ?? 8) +
            Math.sin(index / 4) * 2
        };
      }),
    [cluster.data]
  );

  if (!cluster.data) {
    return (
      <div className="page-state">
        Loading metrics
      </div>
    );
  }

  const metrics = cluster.data.metrics;

  return (
    <div className="stack-xl">
      <section className="metric-grid metric-grid--4">
        <MetricCard
          label="Requests / sec"
          value={formatNumber(
            Math.round(
              metrics.requestsPerSecond
            )
          )}
          detail={`${Math.round(
            metrics.readRequestsPerSecond
          )} reads / ${Math.round(
            metrics.writeRequestsPerSecond
          )} writes`}
        />

        <MetricCard
          label="P95 latency"
          value={`${metrics.latency.p95Ms.toFixed(
            1
          )} ms`}
          detail={`P99 ${metrics.latency.p99Ms.toFixed(
            1
          )} ms`}
        />

        <MetricCard
          label="Leader elections"
          value={metrics.electionsTotal}
          detail={`${metrics.leadershipChangesTotal} leadership changes`}
        />

        <MetricCard
          label="Storage"
          value={formatBytes(
            metrics.logBytes +
              metrics.snapshotBytes
          )}
          detail={`${formatBytes(
            metrics.logBytes
          )} log`}
        />
      </section>

      <div className="metrics-grid">
        <Panel
          title="Request throughput"
          description="Recent request rate."
        >
          <div className="chart">
            <ResponsiveContainer
              width="100%"
              height={290}
            >
              <AreaChart data={data}>
                <CartesianGrid
                  stroke="var(--border)"
                  vertical={false}
                />

                <XAxis
                  dataKey="second"
                  tick={{
                    fill: "var(--text-tertiary)",
                    fontSize: 11
                  }}
                  axisLine={false}
                  tickLine={false}
                />

                <YAxis
                  tick={{
                    fill: "var(--text-tertiary)",
                    fontSize: 11
                  }}
                  axisLine={false}
                  tickLine={false}
                />

                <Tooltip
                  contentStyle={{
                    background:
                      "var(--surface-raised)",
                    border:
                      "1px solid var(--border-strong)",
                    borderRadius: 8,
                    color:
                      "var(--text-primary)"
                  }}
                />

                <Area
                  type="monotone"
                  dataKey="requests"
                  stroke="var(--accent)"
                  fill="var(--accent-muted)"
                  strokeWidth={2}
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </Panel>

        <Panel
          title="P95 latency"
          description="Request completion latency."
        >
          <div className="chart">
            <ResponsiveContainer
              width="100%"
              height={290}
            >
              <AreaChart data={data}>
                <CartesianGrid
                  stroke="var(--border)"
                  vertical={false}
                />

                <XAxis
                  dataKey="second"
                  tick={{
                    fill: "var(--text-tertiary)",
                    fontSize: 11
                  }}
                  axisLine={false}
                  tickLine={false}
                />

                <YAxis
                  tick={{
                    fill: "var(--text-tertiary)",
                    fontSize: 11
                  }}
                  axisLine={false}
                  tickLine={false}
                />

                <Tooltip
                  contentStyle={{
                    background:
                      "var(--surface-raised)",
                    border:
                      "1px solid var(--border-strong)",
                    borderRadius: 8,
                    color:
                      "var(--text-primary)"
                  }}
                />

                <Area
                  type="monotone"
                  dataKey="latency"
                  stroke="var(--warning)"
                  fill="rgba(211, 151, 47, 0.11)"
                  strokeWidth={2}
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </Panel>
      </div>

      <Panel
        title="Node metrics"
        description="Replica-level consensus and storage measurements."
      >
        <div className="data-table-wrapper">
          <table className="data-table">
            <thead>
              <tr>
                <th>Node</th>
                <th>Role</th>
                <th>Term</th>
                <th>Commit</th>
                <th>Applied</th>
                <th>Log</th>
                <th>Snapshot</th>
                <th>Storage</th>
              </tr>
            </thead>

            <tbody>
              {cluster.data.nodes.map(
                (node) => (
                  <tr key={node.nodeId}>
                    <td>
                      Node {node.nodeId}
                    </td>

                    <td>{node.role}</td>

                    <td>{node.term}</td>

                    <td>
                      {formatNumber(
                        node.commitIndex
                      )}
                    </td>

                    <td>
                      {formatNumber(
                        node.appliedIndex
                      )}
                    </td>

                    <td>
                      {formatNumber(
                        node.lastLogIndex
                      )}
                    </td>

                    <td>
                      {formatNumber(
                        node.snapshotIndex
                      )}
                    </td>

                    <td>
                      {formatBytes(
                        node.storageBytes
                      )}
                    </td>
                  </tr>
                )
              )}
            </tbody>
          </table>
        </div>
      </Panel>
    </div>
  );
}

30. src/pages/SimulationLabPage.tsx
import {
  Activity,
  Pause,
  Play,
  RotateCcw,
  ShieldAlert,
  SkipForward,
  Unplug
} from "lucide-react";

import {
  useMutation,
  useQuery,
  useQueryClient
} from "@tanstack/react-query";

import { api } from "../lib/api";

import {
  formatNumber,
  formatRole
} from "../lib/format";

import Panel from "../components/common/Panel";
import StatusPill from "../components/common/StatusPill";

export default function SimulationLabPage() {
  const queryClient = useQueryClient();

  const simulation = useQuery({
    queryKey: ["simulation"],
    queryFn: api.getSimulation,
    refetchInterval: 1_000
  });

  const action = useMutation({
    mutationFn: (
      name:
        | "start"
        | "pause"
        | "step"
        | "reset"
        | "heal"
    ) => api.simulationAction(name),

    onSuccess: (state) => {
      queryClient.setQueryData(
        ["simulation"],
        state
      );
    }
  });

  if (!simulation.data) {
    return (
      <div className="page-state">
        Loading simulation
      </div>
    );
  }

  const data = simulation.data;

  return (
    <div className="stack-xl">
      <section className="simulation-toolbar">
        <div>
          <div className="hero-strip__eyebrow">
            Deterministic simulator
          </div>

          <h2>
            Seed {data.seed}
          </h2>

          <p>
            Tick {formatNumber(data.tick)}
            {" · "}
            {data.nodes.length} nodes
          </p>
        </div>

        <div className="inline-actions">
          <button
            className="button button--primary"
            onClick={() =>
              action.mutate(
                data.running
                  ? "pause"
                  : "start"
              )
            }
          >
            {data.running ? (
              <Pause size={14} />
            ) : (
              <Play size={14} />
            )}

            {data.running ? "Pause" : "Start"}
          </button>

          <button
            className="button button--ghost"
            onClick={() =>
              action.mutate("step")
            }
          >
            <SkipForward size={14} />
            Step
          </button>

          <button
            className="button button--ghost"
            onClick={() =>
              action.mutate("heal")
            }
          >
            <Activity size={14} />
            Heal network
          </button>

          <button
            className="button button--ghost"
            onClick={() =>
              action.mutate("reset")
            }
          >
            <RotateCcw size={14} />
            Reset
          </button>
        </div>
      </section>

      <div className="simulation-grid">
        {data.nodes.map((node) => (
          <article
            key={node.id}
            className={`simulation-node ${
              node.partitioned
                ? "simulation-node--partitioned"
                : ""
            } ${
              node.crashed
                ? "simulation-node--crashed"
                : ""
            }`}
          >
            <header>
              <div>
                <strong>
                  Node {node.id}
                </strong>

                <span>
                  {formatRole(node.role)}
                </span>
              </div>

              <StatusPill
                status={
                  node.crashed
                    ? "unreachable"
                    : node.role === "leader"
                      ? "leader"
                      : "follower"
                }
                label={
                  node.crashed
                    ? "Crashed"
                    : node.partitioned
                      ? "Partitioned"
                      : formatRole(node.role)
                }
              />
            </header>

            <div className="simulation-node__stats">
              <div>
                <span>Term</span>
                <strong>{node.term}</strong>
              </div>

              <div>
                <span>Commit</span>
                <strong>
                  {node.commitIndex}
                </strong>
              </div>

              <div>
                <span>Last log</span>
                <strong>
                  {node.lastLogIndex}
                </strong>
              </div>
            </div>

            <div className="simulation-node__actions">
              <button className="button button--ghost button--small">
                <Unplug size={13} />
                Partition
              </button>

              <button className="button button--danger-ghost button--small">
                <ShieldAlert size={13} />
                Crash
              </button>
            </div>
          </article>
        ))}
      </div>

      <Panel
        title="Simulation event timeline"
        description="Reproducible protocol history for the current seed."
      >
        <div className="event-stream">
          {data.events.map((event) => (
            <div
              key={event.seq}
              className="event-row"
            >
              <div className="event-row__sequence">
                #{event.seq}
              </div>

              <div className="event-row__content">
                <div>
                  <strong>
                    {event.type}
                  </strong>

                  <span>
                    Node {event.nodeId}
                  </span>

                  <span>
                    Term {event.term}
                  </span>
                </div>

                <p>
                  {event.detail ??
                    "Protocol transition"}
                </p>
              </div>
            </div>
          ))}
        </div>
      </Panel>
    </div>
  );
}

31. src/pages/AdministrationPage.tsx
import {
  ArrowRightLeft,
  DatabaseBackup,
  Plus,
  Server,
  Trash2
} from "lucide-react";

import { useState } from "react";

import {
  useMutation,
  useQuery,
  useQueryClient
} from "@tanstack/react-query";

import { api } from "../lib/api";
import { useCluster } from "../hooks/useCluster";

import {
  formatBytes,
  formatNumber
} from "../lib/format";

import Panel from "../components/common/Panel";
import ConfirmDialog from "../components/common/ConfirmDialog";

export default function AdministrationPage() {
  const queryClient = useQueryClient();

  const cluster = useCluster();

  const snapshots = useQuery({
    queryKey: ["snapshots"],
    queryFn: api.getSnapshots
  });

  const [transferTarget, setTransferTarget] =
    useState<number | null>(null);

  const [removeTarget, setRemoveTarget] =
    useState<number | null>(null);

  const triggerSnapshot = useMutation({
    mutationFn: api.triggerSnapshot,

    onSuccess: async () => {
      await queryClient.invalidateQueries({
        queryKey: ["snapshots"]
      });
    }
  });

  const transfer = useMutation({
    mutationFn: (targetId: number) =>
      api.transferLeadership({
        targetId
      }),

    onSuccess: async () => {
      await queryClient.invalidateQueries({
        queryKey: ["cluster"]
      });
    }
  });

  const removeMember = useMutation({
    mutationFn: (id: number) =>
      api.removeMember(id),

    onSuccess: async () => {
      await queryClient.invalidateQueries({
        queryKey: ["cluster"]
      });
    }
  });

  if (!cluster.data) {
    return (
      <div className="page-state">
        Loading administration
      </div>
    );
  }

  return (
    <>
      <div className="admin-grid">
        <Panel
          title="Cluster membership"
          description="Current voting members and advertised endpoints."
          action={
            <button className="button button--primary button--small">
              <Plus size={14} />
              Add member
            </button>
          }
        >
          <div className="member-list">
            {cluster.data.nodes.map(
              (node) => (
                <div
                  key={node.nodeId}
                  className="member-row"
                >
                  <div className="member-row__identity">
                    <div className="node-card__icon">
                      <Server size={15} />
                    </div>

                    <div>
                      <strong>
                        Node {node.nodeId}
                      </strong>

                      <span>
                        {node.raftAddress}
                      </span>
                    </div>
                  </div>

                  <div className="member-row__meta">
                    <span>{node.role}</span>
                    <span>
                      Term {node.term}
                    </span>
                    <span>
                      Applied{" "}
                      {formatNumber(
                        node.appliedIndex
                      )}
                    </span>
                  </div>

                  <div className="inline-actions">
                    {node.nodeId !==
                      cluster.data.leaderId && (
                      <button
                        className="button button--ghost button--small"
                        onClick={() =>
                          setTransferTarget(
                            node.nodeId
                          )
                        }
                      >
                        <ArrowRightLeft
                          size={13}
                        />
                        Transfer leader
                      </button>
                    )}

                    <button
                      className="button button--danger-ghost button--small"
                      onClick={() =>
                        setRemoveTarget(
                          node.nodeId
                        )
                      }
                    >
                      <Trash2 size={13} />
                      Remove
                    </button>
                  </div>
                </div>
              )
            )}
          </div>
        </Panel>

        <Panel
          title="Snapshots"
          description="State-machine checkpoints retained by the cluster."
          action={
            <button
              className="button button--primary button--small"
              disabled={
                triggerSnapshot.isPending
              }
              onClick={() =>
                triggerSnapshot.mutate()
              }
            >
              <DatabaseBackup size={14} />
              Trigger snapshot
            </button>
          }
        >
          <div className="snapshot-list">
            {snapshots.data?.map(
              (snapshot) => (
                <article
                  key={snapshot.id}
                  className="snapshot-row"
                >
                  <div>
                    <strong>
                      Index{" "}
                      {formatNumber(
                        snapshot.lastIncludedIndex
                      )}
                    </strong>

                    <span>
                      Term{" "}
                      {snapshot.lastIncludedTerm}
                    </span>
                  </div>

                  <div>
                    <span>Size</span>
                    <strong>
                      {formatBytes(
                        snapshot.sizeBytes
                      )}
                    </strong>
                  </div>

                  <div>
                    <span>Created</span>
                    <strong>
                      {snapshot.createdAt
                        ? new Date(
                            snapshot.createdAt
                          ).toLocaleString()
                        : "Unknown"}
                    </strong>
                  </div>
                </article>
              )
            )}
          </div>
        </Panel>
      </div>

      <Panel
        title="Danger zone"
        description="Operations in this section directly change replicated cluster state."
      >
        <div className="danger-zone">
          <div>
            <strong>
              Destructive database operations
            </strong>

            <p>
              FLUSHDB and restore operations should
              require explicit operator confirmation
              and server-side authorization.
            </p>
          </div>

          <button className="button button--danger">
            Flush database
          </button>
        </div>
      </Panel>

      <ConfirmDialog
        open={transferTarget !== null}
        title="Transfer leadership"
        description={`Transfer Raft leadership to Node ${transferTarget}? New proposals may briefly pause while the target becomes leader.`}
        confirmLabel="Transfer leadership"
        onClose={() =>
          setTransferTarget(null)
        }
        onConfirm={() => {
          if (transferTarget !== null) {
            transfer.mutate(
              transferTarget
            );
          }
        }}
      />

      <ConfirmDialog
        open={removeTarget !== null}
        title="Remove cluster member"
        description={`Remove Node ${removeTarget} from the replicated configuration? Membership changes must preserve quorum.`}
        confirmLabel="Remove member"
        danger
        onClose={() =>
          setRemoveTarget(null)
        }
        onConfirm={() => {
          if (removeTarget !== null) {
            removeMember.mutate(
              removeTarget
            );
          }
        }}
      />
    </>
  );
}

32. src/styles/globals.css
This is the main visual layer. Keep this file centralized so you can make the dashboard match any existing RaftKV branding by changing only tokens at the top.
:root {
  color-scheme: dark;

  --background: #080a0d;
  --background-subtle: #0b0e12;

  --sidebar: #0b0d10;

  --surface: #101318;
  --surface-hover: #151920;
  --surface-raised: #171b22;
  --surface-active: #1c222b;

  --border: #222831;
  --border-strong: #303844;

  --text-primary: #eef1f5;
  --text-secondary: #9ba5b1;
  --text-tertiary: #687381;

  --accent: #7c9cff;
  --accent-hover: #94adff;
  --accent-muted: rgba(124, 156, 255, 0.12);

  --success: #55c28b;
  --success-muted: rgba(85, 194, 139, 0.11);

  --warning: #d5a14b;
  --warning-muted: rgba(213, 161, 75, 0.12);

  --danger: #e06c75;
  --danger-muted: rgba(224, 108, 117, 0.11);

  --cyan: #61c3d9;
  --purple: #a78bfa;

  --sidebar-width: 242px;
  --topbar-height: 76px;

  --radius-xs: 5px;
  --radius-sm: 7px;
  --radius-md: 10px;
  --radius-lg: 14px;

  --shadow-raised:
    0 1px 0 rgba(255, 255, 255, 0.025),
    0 18px 40px rgba(0, 0, 0, 0.18);

  font-family:
    Inter,
    ui-sans-serif,
    system-ui,
    -apple-system,
    BlinkMacSystemFont,
    "Segoe UI",
    sans-serif;

  font-synthesis: none;
  text-rendering: optimizeLegibility;
}

/* ---------- reset ---------- */

* {
  box-sizing: border-box;
}

html,
body,
#root {
  width: 100%;
  min-width: 320px;
  min-height: 100%;
  margin: 0;
}

body {
  min-height: 100vh;

  background:
    linear-gradient(
      180deg,
      rgba(255, 255, 255, 0.012),
      transparent 300px
    ),
    var(--background);

  color: var(--text-primary);

  font-size: 14px;
}

button,
input,
textarea,
select {
  font: inherit;
}

button {
  color: inherit;
}

button,
a {
  -webkit-tap-highlight-color: transparent;
}

a {
  color: inherit;
  text-decoration: none;
}

code,
pre,
textarea.code-editor,
.console-input input {
  font-family:
    "SFMono-Regular",
    Consolas,
    "Liberation Mono",
    Menlo,
    monospace;
}

::selection {
  background: rgba(124, 156, 255, 0.25);
}

/* ---------- app shell ---------- */

.app-shell {
  display: grid;
  grid-template-columns:
    var(--sidebar-width) minmax(0, 1fr);

  min-height: 100vh;
}

.app-shell__main {
  min-width: 0;
}

.page-container {
  min-height:
    calc(100vh - var(--topbar-height));

  padding: 28px 30px 42px;
}

/* ---------- sidebar ---------- */

.sidebar {
  position: sticky;
  top: 0;

  display: flex;
  flex-direction: column;

  height: 100vh;

  padding: 18px 14px;

  background: var(--sidebar);

  border-right:
    1px solid var(--border);
}

.sidebar__brand {
  display: flex;
  align-items: center;

  gap: 11px;

  min-height: 48px;

  padding: 5px 8px 20px;
}

.sidebar__brand-mark {
  display: grid;
  place-items: center;

  width: 34px;
  height: 34px;

  border: 1px solid var(--border-strong);

  border-radius: 9px;

  color: var(--accent);

  background: var(--surface);
}

.sidebar__brand-title {
  font-size: 15px;
  font-weight: 680;
  letter-spacing: -0.02em;
}

.sidebar__brand-subtitle {
  margin-top: 2px;

  color: var(--text-tertiary);

  font-size: 11px;
  font-weight: 500;
}

.sidebar__nav {
  display: flex;
  flex-direction: column;

  gap: 3px;

  margin-top: 9px;
}

.sidebar__nav-label {
  padding: 0 10px 8px;

  color: var(--text-tertiary);

  font-size: 10px;
  font-weight: 700;

  letter-spacing: 0.11em;

  text-transform: uppercase;
}

.sidebar-link {
  display: flex;
  align-items: center;

  gap: 10px;

  height: 39px;

  padding: 0 10px;

  border: 1px solid transparent;

  border-radius: 7px;

  color: var(--text-secondary);

  font-size: 13px;
  font-weight: 540;

  transition:
    background 120ms ease,
    color 120ms ease,
    border 120ms ease;
}

.sidebar-link:hover {
  background: var(--surface);
  color: var(--text-primary);
}

.sidebar-link--active {
  color: var(--text-primary);

  border-color: var(--border);

  background:
    var(--surface-raised);
}

.sidebar-link--active svg {
  color: var(--accent);
}

.sidebar__footer {
  margin-top: auto;

  padding: 16px 10px 4px;
}

.sidebar__version {
  display: flex;
  justify-content: space-between;

  color: var(--text-tertiary);

  font-family:
    "SFMono-Regular",
    Consolas,
    monospace;

  font-size: 10px;
}

/* ---------- topbar ---------- */

.topbar {
  position: sticky;
  z-index: 40;
  top: 0;

  display: flex;
  align-items: center;
  justify-content: space-between;

  min-height: var(--topbar-height);

  padding: 0 30px;

  border-bottom:
    1px solid rgba(34, 40, 49, 0.85);

  background:
    rgba(8, 10, 13, 0.9);

  backdrop-filter: blur(16px);
}

.topbar__eyebrow {
  color: var(--text-tertiary);

  font-size: 10px;
  font-weight: 650;

  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.topbar__title {
  margin: 4px 0 0;

  font-size: 18px;
  font-weight: 650;

  letter-spacing: -0.025em;
}

.topbar__actions {
  display: flex;
  align-items: center;

  gap: 10px;
}

.topbar__meta {
  display: flex;
  flex-direction: column;

  gap: 2px;

  min-width: 68px;

  padding: 0 10px;
}

.topbar__meta span {
  color: var(--text-tertiary);

  font-size: 10px;
}

.topbar__meta strong {
  font-size: 12px;
  font-weight: 600;
}

/* ---------- buttons ---------- */

.button,
.icon-button,
.text-button {
  border: 0;

  cursor: pointer;

  transition:
    background 120ms ease,
    border 120ms ease,
    color 120ms ease,
    opacity 120ms ease;
}

.button {
  display: inline-flex;
  align-items: center;
  justify-content: center;

  gap: 7px;

  min-height: 36px;

  padding: 0 13px;

  border: 1px solid var(--border-strong);

  border-radius: 7px;

  background: var(--surface-raised);

  color: var(--text-primary);

  font-size: 12px;
  font-weight: 600;
}

.button:hover {
  background: var(--surface-active);
}

.button:disabled {
  cursor: default;
  opacity: 0.5;
}

.button--small {
  min-height: 31px;

  padding: 0 10px;

  font-size: 11px;
}

.button--primary {
  border-color:
    rgba(124, 156, 255, 0.45);

  background: var(--accent);

  color: #0b1020;
}

.button--primary:hover {
  background: var(--accent-hover);
}

.button--ghost {
  background: transparent;
}

.button--danger {
  border-color:
    rgba(224, 108, 117, 0.5);

  background: var(--danger);

  color: #16090b;
}

.button--danger-ghost {
  border-color:
    rgba(224, 108, 117, 0.25);

  color: var(--danger);

  background: transparent;
}

.button--danger-ghost:hover {
  background: var(--danger-muted);
}

.icon-button {
  display: grid;
  place-items: center;

  width: 34px;
  height: 34px;

  padding: 0;

  border: 1px solid var(--border);

  border-radius: 7px;

  background: transparent;

  color: var(--text-secondary);
}

.icon-button:hover {
  color: var(--text-primary);

  background: var(--surface-raised);
}

.text-button {
  display: inline-flex;
  align-items: center;

  gap: 5px;

  padding: 0;

  background: none;

  color: var(--text-secondary);

  font-size: 11px;
}

.text-button:hover {
  color: var(--text-primary);
}

.inline-actions {
  display: flex;
  align-items: center;
  flex-wrap: wrap;

  gap: 8px;
}

/* ---------- generic ---------- */

.stack-xl {
  display: flex;
  flex-direction: column;

  gap: 22px;
}

.page-state {
  display: grid;
  place-items: center;

  min-height: 340px;

  color: var(--text-secondary);
}

.page-state--error {
  color: var(--danger);
}

.empty-panel {
  display: grid;
  place-items: center;

  min-height: 300px;

  color: var(--text-tertiary);
}

/* ---------- panel ---------- */

.panel {
  overflow: hidden;

  border: 1px solid var(--border);

  border-radius: var(--radius-md);

  background:
    rgba(16, 19, 24, 0.76);

  box-shadow:
    0 1px 0
      rgba(255, 255, 255, 0.018);
}

.panel__header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;

  gap: 18px;

  min-height: 68px;

  padding: 17px 18px 15px;

  border-bottom:
    1px solid var(--border);
}

.panel__title {
  margin: 0;

  font-size: 13px;
  font-weight: 650;

  letter-spacing: -0.01em;
}

.panel__description {
  margin: 5px 0 0;

  color: var(--text-tertiary);

  font-size: 11px;
  line-height: 1.5;
}

.panel__body {
  padding: 18px;
}

/* ---------- status pill ---------- */

.status-pill {
  display: inline-flex;
  align-items: center;

  gap: 6px;

  height: 25px;

  padding: 0 8px;

  border: 1px solid var(--border);

  border-radius: 999px;

  color: var(--text-secondary);

  font-size: 10px;
  font-weight: 620;

  text-transform: capitalize;
}

.status-pill__dot {
  width: 6px;
  height: 6px;

  border-radius: 50%;

  background: var(--text-tertiary);
}

.status-pill--healthy,
.status-pill--connected,
.status-pill--leader,
.status-pill--stable {
  border-color:
    rgba(85, 194, 139, 0.24);

  color: var(--success);

  background: var(--success-muted);
}

.status-pill--healthy
  .status-pill__dot,
.status-pill--connected
  .status-pill__dot,
.status-pill--leader
  .status-pill__dot,
.status-pill--stable
  .status-pill__dot {
  background: var(--success);
}

.status-pill--follower {
  color: var(--cyan);

  background:
    rgba(97, 195, 217, 0.08);

  border-color:
    rgba(97, 195, 217, 0.2);
}

.status-pill--follower
  .status-pill__dot {
  background: var(--cyan);
}

.status-pill--degraded,
.status-pill--candidate,
.status-pill--joint,
.status-pill--paused {
  color: var(--warning);

  background: var(--warning-muted);

  border-color:
    rgba(213, 161, 75, 0.25);
}

.status-pill--degraded
  .status-pill__dot,
.status-pill--candidate
  .status-pill__dot,
.status-pill--joint
  .status-pill__dot,
.status-pill--paused
  .status-pill__dot {
  background: var(--warning);
}

.status-pill--unreachable,
.status-pill--disconnected {
  color: var(--danger);

  background: var(--danger-muted);

  border-color:
    rgba(224, 108, 117, 0.25);
}

.status-pill--unreachable
  .status-pill__dot,
.status-pill--disconnected
  .status-pill__dot {
  background: var(--danger);
}

/* ---------- hero ---------- */

.hero-strip,
.simulation-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;

  gap: 28px;

  min-height: 134px;

  padding: 24px 26px;

  border: 1px solid var(--border);

  border-radius: var(--radius-md);

  background:
    radial-gradient(
      circle at 85% 40%,
      rgba(124, 156, 255, 0.075),
      transparent 34%
    ),
    var(--surface);
}

.hero-strip__eyebrow {
  margin-bottom: 7px;

  color: var(--accent);

  font-size: 10px;
  font-weight: 700;

  letter-spacing: 0.1em;

  text-transform: uppercase;
}

.hero-strip h2,
.simulation-toolbar h2 {
  margin: 0;

  font-size: 23px;
  font-weight: 650;

  letter-spacing: -0.04em;
}

.hero-strip p,
.simulation-toolbar p {
  max-width: 560px;

  margin: 8px 0 0;

  color: var(--text-secondary);

  line-height: 1.55;
}

.hero-strip__status {
  display: flex;
  align-items: center;

  gap: 22px;
}

.hero-strip__kv {
  display: flex;
  flex-direction: column;

  gap: 4px;
}

.hero-strip__kv span {
  color: var(--text-tertiary);

  font-size: 10px;
}

.hero-strip__kv strong {
  font-size: 12px;
  font-weight: 620;

  text-transform: capitalize;
}

/* ---------- metric cards ---------- */

.metric-grid {
  display: grid;
  gap: 12px;
}

.metric-grid--4 {
  grid-template-columns:
    repeat(4, minmax(0, 1fr));
}

.metric-grid--5 {
  grid-template-columns:
    repeat(5, minmax(0, 1fr));
}

.metric-card {
  position: relative;

  min-height: 114px;

  padding: 16px;

  border: 1px solid var(--border);

  border-radius: 9px;

  background: var(--surface);
}

.metric-card__label {
  color: var(--text-secondary);

  font-size: 11px;
  font-weight: 520;
}

.metric-card__value {
  margin-top: 10px;

  font-size: 24px;
  font-weight: 660;

  letter-spacing: -0.035em;
}

.metric-card__detail {
  margin-top: 11px;

  color: var(--text-tertiary);

  font-size: 10px;
}

.metric-detail {
  display: flex;
  align-items: center;

  gap: 5px;
}

/* ---------- node card ---------- */

.node-grid {
  display: grid;
  grid-template-columns:
    repeat(3, minmax(0, 1fr));

  gap: 12px;
}

.node-card {
  padding: 16px;

  border: 1px solid var(--border);

  border-radius: 9px;

  background: var(--surface);

  transition:
    border 120ms ease,
    transform 120ms ease,
    background 120ms ease;
}

.node-card:hover {
  transform: translateY(-1px);

  border-color: var(--border-strong);

  background: var(--surface-hover);
}

.node-card--leader {
  border-color:
    rgba(85, 194, 139, 0.25);
}

.node-card__header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;

  gap: 12px;
}

.node-card__identity {
  display: flex;
  align-items: center;

  gap: 10px;
}

.node-card__identity h3 {
  margin: 0;

  font-size: 13px;
}

.node-card__identity span {
  display: block;

  margin-top: 4px;

  color: var(--text-tertiary);

  font-size: 10px;
}

.node-card__icon {
  display: grid;
  place-items: center;

  flex: none;

  width: 31px;
  height: 31px;

  border: 1px solid var(--border);

  border-radius: 7px;

  background: var(--surface-raised);

  color: var(--text-secondary);
}

.node-card__metrics {
  display: grid;
  grid-template-columns:
    repeat(4, minmax(0, 1fr));

  gap: 8px;

  margin-top: 22px;
}

.node-card__metrics div {
  display: flex;
  flex-direction: column;

  gap: 6px;
}

.node-card__metrics span {
  color: var(--text-tertiary);

  font-size: 9px;
  text-transform: uppercase;

  letter-spacing: 0.06em;
}

.node-card__metrics strong {
  font-family:
    "SFMono-Regular",
    Consolas,
    monospace;

  font-size: 12px;
  font-weight: 550;
}

.node-card__footer {
  display: flex;
  flex-wrap: wrap;

  gap: 13px;

  margin-top: 18px;
  padding-top: 13px;

  border-top: 1px solid var(--border);

  color: var(--text-tertiary);

  font-size: 9px;
}

.node-card__footer span {
  display: flex;
  align-items: center;

  gap: 5px;
}

/* ---------- overview ---------- */

.overview-grid {
  display: grid;
  grid-template-columns:
    minmax(0, 1.45fr)
    minmax(320px, 0.7fr);

  gap: 14px;
}

.replication-list {
  display: flex;
  flex-direction: column;

  gap: 19px;
}

.replication-row__head,
.replication-row__foot {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.replication-row__head {
  margin-bottom: 8px;
}

.replication-row__head div {
  display: flex;
  align-items: center;

  gap: 7px;
}

.replication-row__head strong {
  font-size: 11px;
}

.replication-row__head span,
.replication-row__foot {
  color: var(--text-tertiary);

  font-family:
    "SFMono-Regular",
    Consolas,
    monospace;

  font-size: 9px;
}

.replication-row__foot {
  margin-top: 7px;
}

.progress-track {
  overflow: hidden;

  height: 5px;

  border-radius: 999px;

  background: var(--surface-active);
}

.progress-track__fill {
  height: 100%;

  border-radius: inherit;

  background: var(--accent);
}

.storage-summary {
  display: grid;
  grid-template-columns:
    repeat(2, minmax(0, 1fr));

  gap: 10px;
}

.storage-summary div {
  display: flex;
  flex-direction: column;

  gap: 7px;

  padding: 13px;

  border: 1px solid var(--border);

  border-radius: 7px;

  background: var(--background-subtle);
}

.storage-summary span {
  color: var(--text-tertiary);

  font-size: 9px;
}

.storage-summary strong {
  font-size: 14px;
}

.hash-block {
  margin-top: 14px;

  padding: 13px;

  border: 1px solid var(--border);

  border-radius: 7px;

  background: var(--background-subtle);
}

.hash-block span {
  display: block;

  margin-bottom: 8px;

  color: var(--text-tertiary);

  font-size: 9px;
}

.hash-block code {
  color: var(--text-secondary);

  font-size: 10px;

  word-break: break-all;
}

/* ---------- key explorer ---------- */

.key-explorer {
  display: grid;

  grid-template-columns:
    minmax(280px, 0.35fr)
    minmax(0, 1fr);

  gap: 14px;

  min-height:
    calc(100vh - 148px);
}

.key-browser,
.key-editor {
  min-height: 660px;
}

.search-field {
  display: flex;
  align-items: center;

  gap: 9px;

  height: 38px;

  padding: 0 11px;

  border: 1px solid var(--border);

  border-radius: 7px;

  background: var(--background-subtle);

  color: var(--text-tertiary);
}

.search-field input {
  flex: 1;

  min-width: 0;

  border: 0;
  outline: none;

  background: transparent;

  color: var(--text-primary);
}

.search-field input::placeholder {
  color: var(--text-tertiary);
}

.key-list {
  display: flex;
  flex-direction: column;

  gap: 3px;

  max-height: 580px;

  margin-top: 12px;

  overflow-y: auto;
}

.key-list-item {
  width: 100%;

  padding: 10px;

  border: 1px solid transparent;

  border-radius: 6px;

  background: transparent;

  color: var(--text-primary);

  text-align: left;

  cursor: pointer;
}

.key-list-item:hover {
  background: var(--surface-hover);
}

.key-list-item--active {
  border-color: var(--border);

  background: var(--surface-raised);
}

.key-list-item__main {
  display: flex;
  align-items: center;

  gap: 7px;

  font-family:
    "SFMono-Regular",
    Consolas,
    monospace;

  font-size: 11px;
}

.key-list-item__meta {
  display: flex;
  justify-content: space-between;

  margin-top: 7px;
  padding-left: 21px;

  color: var(--text-tertiary);

  font-size: 9px;
}

.key-detail {
  display: flex;
  flex-direction: column;

  gap: 18px;
}

.key-detail__metadata {
  display: grid;
  grid-template-columns:
    repeat(4, minmax(0, 1fr));

  gap: 9px;
}

.key-detail__metadata div {
  display: flex;
  flex-direction: column;

  gap: 6px;

  padding: 11px 12px;

  border: 1px solid var(--border);

  border-radius: 7px;

  background: var(--background-subtle);
}

.key-detail__metadata span {
  color: var(--text-tertiary);

  font-size: 9px;
}

.key-detail__metadata strong {
  font-size: 11px;
}

.field-group {
  display: flex;
  flex-direction: column;

  gap: 8px;
}

.field-label-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.field-label-row label {
  color: var(--text-secondary);

  font-size: 11px;
  font-weight: 600;
}

.code-editor {
  width: 100%;
  min-height: 355px;

  resize: vertical;

  padding: 14px;

  border: 1px solid var(--border);

  border-radius: 7px;

  outline: none;

  background: #090b0e;

  color: #d9e0e8;

  font-size: 11px;
  line-height: 1.65;

  tab-size: 2;
}

.code-editor:focus {
  border-color:
    rgba(124, 156, 255, 0.45);
}

.replication-warning {
  padding: 12px 13px;

  border-left: 2px solid var(--warning);

  background: var(--warning-muted);

  color: #c9b184;

  font-size: 10px;
  line-height: 1.5;
}

.form-actions {
  display: flex;
  justify-content: flex-end;
}

/* ---------- console ---------- */

.console-layout {
  max-width: 1260px;
}

.console-panel .panel__body {
  padding: 0;
}

.console-input {
  display: flex;
  align-items: center;

  gap: 8px;

  padding: 15px 17px;

  border-bottom: 1px solid var(--border);

  background: #090b0e;
}

.console-prompt {
  color: var(--accent);

  font-family:
    "SFMono-Regular",
    Consolas,
    monospace;

  font-size: 11px;
}

.console-input input {
  flex: 1;

  min-width: 0;

  border: 0;
  outline: none;

  background: transparent;

  color: var(--text-primary);

  font-size: 11px;
}

.command-presets {
  display: flex;
  flex-wrap: wrap;

  gap: 6px;

  padding: 12px 17px;

  border-bottom: 1px solid var(--border);
}

.command-presets button {
  padding: 5px 8px;

  border: 1px solid var(--border);

  border-radius: 5px;

  background: transparent;

  color: var(--text-tertiary);

  font-family:
    "SFMono-Regular",
    Consolas,
    monospace;

  font-size: 9px;

  cursor: pointer;
}

.command-presets button:hover {
  color: var(--text-primary);

  background: var(--surface-hover);
}

.console-history {
  min-height: 560px;

  background: #090b0e;
}

.console-empty {
  display: grid;
  place-items: center;

  min-height: 420px;

  color: var(--text-tertiary);
}

.console-entry {
  padding: 18px;

  border-bottom: 1px solid var(--border);
}

.console-entry__command {
  display: flex;
  align-items: center;

  gap: 7px;

  color: var(--text-secondary);

  font-family:
    "SFMono-Regular",
    Consolas,
    monospace;

  font-size: 11px;
}

.console-entry__command svg {
  color: var(--accent);
}

.console-entry__response {
  margin: 13px 0 0;

  white-space: pre-wrap;

  color: #d8dee8;

  font-size: 11px;
  line-height: 1.6;
}

.console-entry__response--error {
  color: var(--danger);
}

.console-entry__meta {
  display: flex;
  align-items: center;
  flex-wrap: wrap;

  gap: 13px;

  margin-top: 14px;

  color: var(--text-tertiary);

  font-size: 9px;
}

.console-entry__meta span,
.console-entry__meta button {
  display: inline-flex;
  align-items: center;

  gap: 5px;
}

.console-entry__meta button {
  margin-left: auto;

  border: 0;

  background: transparent;

  color: var(--text-tertiary);

  font-size: 9px;

  cursor: pointer;
}

.execution-grid {
  display: grid;

  grid-template-columns:
    repeat(5, minmax(0, 1fr));

  gap: 7px;

  margin-top: 14px;
}

.execution-grid div {
  display: flex;
  flex-direction: column;

  gap: 5px;

  padding: 9px 10px;

  border: 1px solid var(--border);

  border-radius: 5px;

  background: var(--background-subtle);
}

.execution-grid span {
  color: var(--text-tertiary);

  font-size: 8px;
}

.execution-grid strong {
  font-size: 10px;
  font-weight: 560;
}

/* ---------- raft visualizer ---------- */

.visualizer-layout {
  display: flex;
  flex-direction: column;

  gap: 14px;
}

.raft-graph {
  position: relative;

  min-height: 450px;

  overflow: hidden;

  border: 1px solid var(--border);

  border-radius: 8px;

  background:
    radial-gradient(
      circle at 50% 50%,
      rgba(124, 156, 255, 0.05),
      transparent 42%
    ),
    #0a0d11;
}

.raft-graph__edges {
  position: absolute;
  inset: 0;

  width: 100%;
  height: 100%;

  pointer-events: none;
}

.raft-graph__edges line {
  stroke: #2b333e;
  stroke-width: 1px;
}

.raft-graph__message {
  stroke: var(--accent) !important;

  stroke-width: 1.5px !important;

  stroke-dasharray: 5 4;

  animation:
    flow 1.2s linear infinite;
}

@keyframes flow {
  to {
    stroke-dashoffset: -18;
  }
}

.raft-node {
  position: absolute;

  width: 142px;

  transform:
    translate(-50%, -50%);

  padding: 11px;

  border: 1px solid var(--border-strong);

  border-radius: 8px;

  background: var(--surface-raised);

  box-shadow: var(--shadow-raised);
}

.raft-node--leader {
  border-color:
    rgba(85, 194, 139, 0.48);

  box-shadow:
    0 0 0 1px
      rgba(85, 194, 139, 0.08),
    var(--shadow-raised);
}

.raft-node__top {
  display: flex;
  justify-content: space-between;

  gap: 8px;
}

.raft-node__top strong {
  font-size: 10px;
}

.raft-node__top span {
  color: var(--text-tertiary);

  font-size: 8px;
}

.raft-node__stats {
  display: flex;
  justify-content: space-between;

  margin-top: 9px;
  padding-top: 8px;

  border-top: 1px solid var(--border);

  color: var(--text-secondary);

  font-family:
    "SFMono-Regular",
    Consolas,
    monospace;

  font-size: 8px;
}

.visualizer-bottom {
  display: grid;
  grid-template-columns:
    minmax(0, 1.1fr)
    minmax(350px, 0.9fr);

  gap: 14px;
}

.node-log-list {
  display: flex;
  flex-direction: column;

  gap: 16px;
}

.node-log-row {
  display: grid;
  grid-template-columns: 90px 1fr;

  align-items: center;

  gap: 14px;
}

.node-log-row__title {
  display: flex;
  flex-direction: column;

  gap: 3px;
}

.node-log-row__title strong {
  font-size: 10px;
}

.node-log-row__title span {
  color: var(--text-tertiary);

  font-size: 8px;
}

.log-strip {
  display: flex;
  align-items: stretch;

  gap: 4px;

  overflow-x: auto;
}

.log-strip__snapshot {
  display: grid;
  place-items: center;

  min-width: 48px;

  padding: 7px 6px;

  border: 1px dashed var(--border-strong);

  border-radius: 5px;

  color: var(--text-tertiary);

  font-family:
    "SFMono-Regular",
    Consolas,
    monospace;

  font-size: 8px;
}

.log-entry {
  display: flex;
  flex-direction: column;
  align-items: center;

  gap: 3px;

  min-width: 42px;

  padding: 6px;

  border: 1px solid var(--border);

  border-radius: 5px;

  font-family:
    "SFMono-Regular",
    Consolas,
    monospace;

  font-size: 8px;
}

.log-entry small {
  color: var(--text-tertiary);

  font-size: 7px;
}

.log-entry--applied {
  border-color:
    rgba(85, 194, 139, 0.25);

  background: var(--success-muted);
}

.log-entry--committed {
  border-color:
    rgba(124, 156, 255, 0.3);

  background: var(--accent-muted);
}

.log-entry--uncommitted {
  border-color:
    rgba(213, 161, 75, 0.3);

  background: var(--warning-muted);
}

.log-entry--conflict {
  border-color:
    rgba(224, 108, 117, 0.35);

  background: var(--danger-muted);
}

.event-stream {
  max-height: 450px;

  overflow-y: auto;
}

.event-row {
  display: grid;
  grid-template-columns:
    54px 22px 1fr;

  gap: 8px;

  padding: 9px 0;

  border-bottom: 1px solid var(--border);
}

.event-row:last-child {
  border-bottom: 0;
}

.event-row__sequence {
  color: var(--text-tertiary);

  font-family:
    "SFMono-Regular",
    Consolas,
    monospace;

  font-size: 8px;
}

.event-row__icon {
  color: var(--accent);
}

.event-row__content > div {
  display: flex;
  align-items: center;
  flex-wrap: wrap;

  gap: 8px;
}

.event-row__content strong {
  font-size: 9px;
}

.event-row__content span {
  color: var(--text-tertiary);

  font-size: 8px;
}

.event-row__content p {
  margin: 5px 0 0;

  color: var(--text-secondary);

  font-size: 9px;
}

/* ---------- metrics ---------- */

.metrics-grid {
  display: grid;
  grid-template-columns:
    repeat(2, minmax(0, 1fr));

  gap: 14px;
}

.chart {
  width: 100%;
  min-height: 290px;
}

.data-table-wrapper {
  overflow-x: auto;
}

.data-table {
  width: 100%;

  border-collapse: collapse;
}

.data-table th,
.data-table td {
  padding: 11px 12px;

  border-bottom: 1px solid var(--border);

  text-align: left;

  white-space: nowrap;
}

.data-table th {
  color: var(--text-tertiary);

  font-size: 8px;
  font-weight: 650;

  letter-spacing: 0.06em;

  text-transform: uppercase;
}

.data-table td {
  color: var(--text-secondary);

  font-family:
    "SFMono-Regular",
    Consolas,
    monospace;

  font-size: 9px;
}

/* ---------- simulation ---------- */

.simulation-grid {
  display: grid;
  grid-template-columns:
    repeat(5, minmax(0, 1fr));

  gap: 10px;
}

.simulation-node {
  padding: 14px;

  border: 1px solid var(--border);

  border-radius: 8px;

  background: var(--surface);
}

.simulation-node--partitioned {
  border-color:
    rgba(213, 161, 75, 0.42);
}

.simulation-node--crashed {
  opacity: 0.55;

  border-color:
    rgba(224, 108, 117, 0.35);
}

.simulation-node header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;

  gap: 8px;
}

.simulation-node header div {
  display: flex;
  flex-direction: column;

  gap: 3px;
}

.simulation-node header strong {
  font-size: 11px;
}

.simulation-node header span {
  color: var(--text-tertiary);

  font-size: 8px;
}

.simulation-node__stats {
  display: grid;

  grid-template-columns:
    repeat(3, 1fr);

  gap: 5px;

  margin-top: 15px;
}

.simulation-node__stats div {
  display: flex;
  flex-direction: column;

  gap: 5px;
}

.simulation-node__stats span {
  color: var(--text-tertiary);

  font-size: 8px;
}

.simulation-node__stats strong {
  font-family:
    "SFMono-Regular",
    Consolas,
    monospace;

  font-size: 10px;
}

.simulation-node__actions {
  display: flex;

  gap: 5px;

  margin-top: 15px;
  padding-top: 11px;

  border-top: 1px solid var(--border);
}

/* ---------- administration ---------- */

.admin-grid {
  display: grid;
  grid-template-columns:
    minmax(0, 1.25fr)
    minmax(380px, 0.75fr);

  gap: 14px;

  margin-bottom: 14px;
}

.member-list {
  display: flex;
  flex-direction: column;

  gap: 8px;
}

.member-row {
  display: grid;

  grid-template-columns:
    minmax(200px, 1fr)
    minmax(220px, 0.8fr)
    auto;

  align-items: center;

  gap: 14px;

  padding: 12px;

  border: 1px solid var(--border);

  border-radius: 7px;

  background: var(--background-subtle);
}

.member-row__identity {
  display: flex;
  align-items: center;

  gap: 9px;
}

.member-row__identity > div:last-child {
  display: flex;
  flex-direction: column;

  gap: 4px;
}

.member-row__identity strong {
  font-size: 10px;
}

.member-row__identity span {
  color: var(--text-tertiary);

  font-family:
    "SFMono-Regular",
    Consolas,
    monospace;

  font-size: 8px;
}

.member-row__meta {
  display: flex;

  gap: 12px;

  color: var(--text-tertiary);

  font-size: 8px;
}

.snapshot-list {
  display: flex;
  flex-direction: column;

  gap: 8px;
}

.snapshot-row {
  display: grid;
  grid-template-columns:
    1fr 0.6fr 1fr;

  gap: 10px;

  padding: 11px 12px;

  border: 1px solid var(--border);

  border-radius: 7px;

  background: var(--background-subtle);
}

.snapshot-row div {
  display: flex;
  flex-direction: column;

  gap: 4px;
}

.snapshot-row span {
  color: var(--text-tertiary);

  font-size: 8px;
}

.snapshot-row strong {
  font-size: 9px;
  font-weight: 550;
}

.danger-zone {
  display: flex;
  align-items: center;
  justify-content: space-between;

  gap: 20px;

  padding: 3px;
}

.danger-zone strong {
  font-size: 11px;
}

.danger-zone p {
  max-width: 620px;

  margin: 5px 0 0;

  color: var(--text-tertiary);

  font-size: 9px;
  line-height: 1.5;
}

/* ---------- dialog ---------- */

.dialog {
  width: min(470px, calc(100vw - 32px));

  padding: 0;

  border: 1px solid var(--border-strong);

  border-radius: 10px;

  background: var(--surface-raised);

  color: var(--text-primary);

  box-shadow:
    0 30px 90px
      rgba(0, 0, 0, 0.55);
}

.dialog::backdrop {
  background: rgba(0, 0, 0, 0.68);

  backdrop-filter: blur(3px);
}

.dialog__content {
  padding: 20px;
}

.dialog__content h3 {
  margin: 0;

  font-size: 14px;
}

.dialog__content p {
  margin: 10px 0 0;

  color: var(--text-secondary);

  font-size: 11px;
  line-height: 1.55;
}

.dialog__actions {
  display: flex;
  justify-content: flex-end;

  gap: 8px;

  margin-top: 22px;
}

/* ---------- scrollbars ---------- */

* {
  scrollbar-width: thin;

  scrollbar-color:
    var(--border-strong)
    transparent;
}

::-webkit-scrollbar {
  width: 8px;
  height: 8px;
}

::-webkit-scrollbar-thumb {
  border-radius: 999px;

  background: var(--border-strong);
}

::-webkit-scrollbar-track {
  background: transparent;
}

/* ---------- responsive ---------- */

@media (max-width: 1380px) {
  .metric-grid--5 {
    grid-template-columns:
      repeat(3, minmax(0, 1fr));
  }

  .simulation-grid {
    grid-template-columns:
      repeat(3, minmax(0, 1fr));
  }
}

@media (max-width: 1120px) {
  :root {
    --sidebar-width: 76px;
  }

  .sidebar__brand > div:last-child,
  .sidebar-link span,
  .sidebar__nav-label,
  .sidebar__version {
    display: none;
  }

  .sidebar {
    align-items: center;
  }

  .sidebar-link {
    justify-content: center;

    width: 43px;

    padding: 0;
  }

  .node-grid {
    grid-template-columns:
      repeat(2, minmax(0, 1fr));
  }

  .overview-grid,
  .visualizer-bottom,
  .admin-grid {
    grid-template-columns: 1fr;
  }

  .metric-grid--4,
  .metric-grid--5 {
    grid-template-columns:
      repeat(2, minmax(0, 1fr));
  }

  .simulation-grid {
    grid-template-columns:
      repeat(2, minmax(0, 1fr));
  }

  .member-row {
    grid-template-columns:
      1fr auto;
  }

  .member-row__meta {
    display: none;
  }
}

@media (max-width: 820px) {
  .app-shell {
    display: block;
  }

  .sidebar {
    position: fixed;
    z-index: 80;
    inset:
      auto 0 0 0;

    flex-direction: row;

    width: 100%;
    height: 62px;

    padding: 7px 10px;

    border-top:
      1px solid var(--border);

    border-right: 0;
  }

  .sidebar__brand,
  .sidebar__footer,
  .sidebar__nav-label {
    display: none;
  }

  .sidebar__nav {
    display: flex;
    flex-direction: row;

    justify-content: space-around;

    width: 100%;

    margin: 0;
  }

  .sidebar-link {
    width: 42px;
    height: 42px;
  }

  .topbar {
    padding: 0 17px;
  }

  .topbar__meta,
  .topbar .status-pill {
    display: none;
  }

  .page-container {
    padding:
      18px 15px 86px;
  }

  .hero-strip,
  .simulation-toolbar {
    align-items: flex-start;
    flex-direction: column;
  }

  .hero-strip__status {
    width: 100%;

    justify-content: space-between;
  }

  .node-grid,
  .metric-grid--4,
  .metric-grid--5,
  .metrics-grid,
  .simulation-grid {
    grid-template-columns: 1fr;
  }

  .key-explorer {
    grid-template-columns: 1fr;
  }

  .key-browser {
    min-height: 400px;
  }

  .key-detail__metadata {
    grid-template-columns:
      repeat(2, 1fr);
  }

  .execution-grid {
    grid-template-columns:
      repeat(2, 1fr);
  }

  .member-row {
    grid-template-columns: 1fr;
  }

  .member-row .inline-actions {
    margin-top: 4px;
  }
}

33. AGENTS.md
# AGENTS.md

## Project

RaftKV is a Raft-replicated key-value store implemented primarily in Rust.

This file defines repository-wide instructions for coding agents working on the RaftKV dashboard and related frontend code.

## Primary architectural rule

The existing pure/impure boundary must be preserved.

`raft-core` is a deterministic consensus state machine. It must not gain browser code, HTTP code, React-specific concerns, wall-clock access, filesystem access, asynchronous I/O, or UI state.

The dashboard is an observer and operator interface layered above the runtime/control plane.

The intended direction is:

Browser
→ HTTP/SSE API
→ runtime/control facade
→ Raft runtime
→ raft-core/storage/state machine/network

The browser must never directly access:

- Raft core structures
- sled databases
- Raft log segment files
- snapshot files
- peer gRPC transport internals
- local filesystem state

## Frontend location

All dashboard frontend code belongs under:

`ui/dashboard/`

Do not scatter React components through Rust crates.

## Frontend technology

Use:

- React
- TypeScript
- Vite
- React Router
- TanStack Query
- Lucide React
- Recharts only for quantitative charts
- native EventSource for server-sent events

Avoid adding large dependencies without a clear reason.

Do not introduce a component framework such as Material UI, Ant Design, Chakra, Mantine, or Bootstrap unless explicitly requested.

The visual system is intentionally custom and lightweight.

## UI principles

The dashboard is an engineering control plane, not a marketing website.

Prefer:

- dense but readable layouts
- compact information hierarchy
- clear tables
- restrained color
- strong typography
- consistent spacing
- operational context
- visible system state
- keyboard-friendly interactions
- deterministic layout behavior

Avoid:

- emojis
- gradients used as decoration
- glassmorphism-heavy design
- oversized cards
- oversized text
- excessive rounded corners
- decorative animations
- fake terminal effects
- arbitrary neon styling
- ambiguous status colors

Subtle visualization animation is permitted only when it communicates Raft traffic or state transition.

## Design tokens

Use the centralized CSS variables in:

`ui/dashboard/src/styles/globals.css`

Do not hard-code unrelated colors throughout components.

Status semantics:

- green: healthy, committed, leader, connected
- blue/cyan: followers and neutral distributed state
- amber: transitional, candidate, joint configuration, degraded
- red: unavailable, failed, destructive operation
- primary accent: interactive/highlight state

## Required screens

The frontend supports these primary routes:

- `/` — Cluster Overview
- `/keys` — Key Explorer
- `/console` — Command Console
- `/raft` — Raft Visualizer
- `/metrics` — Metrics
- `/simulation` — Simulation Lab
- `/administration` — Administration

Do not combine all functionality into a single dashboard page.

## API boundary

All HTTP calls must go through:

`src/lib/api.ts`

Components must not issue ad-hoc fetch calls.

Add new API models to:

`src/types/api.ts`

Use TanStack Query for server state.

Use local React state only for transient UI state such as:

- selected key
- dialog state
- console input
- filters
- paused visualization state

## Expected HTTP contracts

Core browser APIs include:

- `GET /api/v1/cluster`
- `GET /api/v1/nodes`
- `GET /api/v1/nodes/:id`
- `GET /api/v1/events`
- `GET /api/v1/keys`
- `GET /api/v1/keys/:key`
- `PUT /api/v1/keys/:key`
- `DELETE /api/v1/keys/:key`
- `POST /api/v1/commands`
- `POST /api/v1/admin/snapshot`
- `POST /api/v1/admin/leadership`
- `POST /api/v1/admin/members`
- `DELETE /api/v1/admin/members/:id`
- `GET /api/v1/snapshots`

Simulation APIs may use the `/lab/api/v1/` namespace.

Do not silently invent backend behavior. If a UI feature needs an endpoint that does not exist, keep the frontend contract explicit and document the dependency.

## SSE

Raft visualizer events use server-sent events.

Centralize event stream behavior in:

`src/hooks/useRaftEvents.ts`

Event handlers must tolerate:

- connection loss
- reconnects
- malformed events
- duplicate events
- event bursts

The rendered event history must remain bounded.

Never allow an unbounded browser event array.

## Key Explorer

The Key Explorer must use SCAN-style pagination.

Do not depend on `KEYS *`.

Requirements:

- pattern search
- paginated key enumeration
- key metadata
- TTL
- size
- encoding
- value view
- editing
- delete confirmation
- copy functionality

Binary values must have an explicit representation such as UTF-8, Base64, or hexadecimal.

Never assume all values are valid UTF-8.

## Command Console

The console is not a fake terminal.

It should surface:

- submitted command
- structured response
- raw RESP response when available
- duration
- receiving node
- current leader
- term
- proposed log index for writes
- commit latency
- apply latency
- replication count

Use monospace typography only where technically appropriate.

## Raft Visualizer

The visualizer should make protocol state understandable.

Display:

- node role
- term
- leader
- commit index
- applied index
- recent protocol traffic
- recent log entries
- snapshot boundary
- event timeline

The visualizer may animate Raft messages, but motion must remain subtle and informational.

Do not use random animation.

## Simulation Lab

Simulation and live-cluster administration are separate concepts.

The Simulation Lab must be visually and logically separated from live production controls.

A destructive simulator action must never be wired to a live-cluster endpoint.

Simulation state should include a reproducible seed.

## Administration safety

Destructive operations require confirmation.

This includes:

- removing members
- FLUSHDB
- restoring backups
- destructive snapshot/restore actions
- operations that intentionally reduce quorum

The frontend must not imply an operation is safe merely because a button is enabled.

Backend authorization remains authoritative.

## Accessibility

Requirements:

- semantic buttons
- visible focus styles
- keyboard navigation
- sufficient contrast
- labels for inputs
- title/aria labels for icon-only controls
- avoid color-only meaning

## Responsive behavior

Desktop is the primary target.

The dashboard must remain usable on:

- 1440px+
- 1280px
- 1024px
- tablet-sized displays

On narrow layouts:

- collapse sidebar to icons
- eventually move navigation to the bottom
- convert grids to one column
- preserve data readability

Do not optimize the Raft visualizer for tiny phone screens at the expense of desktop quality.

## TypeScript rules

Use strict TypeScript.

Avoid:

- `any`
- type assertions used to bypass incorrect models
- duplicated API interfaces
- giant page components when reusable behavior is obvious

Prefer:

- discriminated unions
- explicit nullable values
- typed API responses
- small formatting utilities
- data transformation outside JSX when complex

## CSS rules

Keep the design system centralized.

Prefer BEM-like class naming used by the existing dashboard.

Avoid inline styles except for genuinely dynamic values such as:

- graph positions
- progress percentages

Do not add CSS-in-JS.

## Performance

Do not poll at extreme frequency.

Recommended baseline:

- cluster state: every 2 seconds
- simulation state: every 1 second if needed
- protocol events: SSE instead of polling

Bound long lists.

Use virtualization only when there is evidence it is needed.

## Demo mode

`VITE_DEMO_MODE=true` allows frontend development without a running cluster.

Mock data belongs in:

`src/lib/mock.ts`

Demo behavior must never be mixed into real API response parsing.

## Do not modify backend without instruction

If the task is explicitly frontend-only:

Do not edit:

- `crates/raft-core`
- `crates/raft-storage`
- `crates/raft-net`
- `crates/kv-state-machine`
- `crates/resp-server`
- Rust runtime behavior
- protobuf definitions
- deployment files

Frontend code may define the expected API contract without implementing the backend.

## Quality gate

Before finishing a frontend change:

1. Run TypeScript type checking.
2. Build the Vite application.
3. Verify every route renders.
4. Verify there are no console errors.
5. Test demo mode.
6. Test loading states.
7. Test empty states.
8. Test error states.
9. Test destructive confirmation dialogs.
10. Check keyboard navigation.
11. Check 1440px, 1024px, and narrow layouts.
12. Ensure no emojis were introduced.

## Commands

From `ui/dashboard`:

```bash
npm install
npm run dev
npm run typecheck
npm run build

Final requirement
A dashboard change is complete only if the UI communicates real distributed-system state clearly.
Visual polish must never come at the cost of protocol clarity.

---

# 34. `CODEX.md`

```md
# CODEX.md

## Purpose

Instructions for Codex when modifying the RaftKV frontend.

Read `AGENTS.md` first. `AGENTS.md` is the canonical repository guidance. This file adds Codex-specific execution rules.

## Scope discipline

If the request is about the dashboard, remain inside:

`ui/dashboard/`

unless the user explicitly requests backend integration.

Do not opportunistically refactor Rust code during a frontend task.

Do not modify consensus logic merely to satisfy a frontend assumption.

If a backend capability is missing, define the required typed frontend contract and clearly identify the dependency.

## Before editing

Inspect:

1. `ui/dashboard/package.json`
2. `ui/dashboard/src/types/api.ts`
3. `ui/dashboard/src/lib/api.ts`
4. `ui/dashboard/src/styles/globals.css`
5. the route/page being modified
6. reusable components related to the feature

Do not create duplicate abstractions before checking what exists.

## Design preservation

Preserve the established visual language:

- dark neutral surfaces
- thin borders
- subtle primary accent
- compact typography
- compact status badges
- operational density
- low visual noise
- no emojis
- no decorative gradients
- no unnecessary card explosion

Prefer extending CSS tokens instead of introducing one-off colors.

## Component policy

A page should orchestrate behavior and layout.

Reusable elements belong under `src/components`.

Do not extract components merely because they are ten lines long.

Extract when one of the following is true:

- reused by multiple pages
- logically independent
- difficult to read inline
- has independent interaction state
- represents a consistent design-system primitive

## Data policy

Never fabricate live cluster data inside page components.

Mock data belongs in:

`src/lib/mock.ts`

All production HTTP access belongs in:

`src/lib/api.ts`

All production server-state caching belongs in TanStack Query.

## Event policy

Do not place direct EventSource construction inside visual components.

Use the shared Raft event hook.

Event collections must be bounded.

When adding new event types:

1. update `RuntimeEventType`
2. update `RuntimeEvent`
3. handle optional fields safely
4. ensure older backend payloads do not crash rendering

## Destructive actions

For every destructive control:

- use explicit wording
- require confirmation
- describe Raft implications when relevant
- keep backend authorization authoritative
- do not optimistically pretend the action completed before success

## Error handling

Do not silently swallow API errors in user-facing operations.

Pages should eventually surface:

- request failure
- disconnected event stream
- missing leader
- unavailable cluster
- stale data where relevant

Internal malformed event payloads may be ignored if they do not affect safety.

## Type safety

Do not use:

```ts
as any

Do not suppress TypeScript errors simply to finish a task.
Correct the type or contract.
Running checks
After frontend edits run:
cd ui/dashboard
npm run typecheck
npm run build

If either fails, fix it before finishing.
Route validation
Verify all primary routes:
/
 /keys
 /console
 /raft
 /metrics
 /simulation
 /administration

UX validation
For each modified screen verify:
- loading
- populated
- empty
- backend failure
- narrow viewport
- keyboard focus
- disabled state
- pending mutation state
Working with existing files
Prefer editing existing files over replacing whole systems.
Do not introduce another styling framework.
Do not introduce Tailwind into this dashboard unless explicitly requested.
Do not replace custom CSS with a component library.
Comments
Use comments to explain architecture or subtle behavior.
Do not annotate obvious JSX.
Good:
// Bound event history so a long-running dashboard cannot grow memory forever.

Bad:
// Render the button.

Commit-quality expectation
Changes should look intentional enough to merge directly:
- no placeholders unless clearly marked
- no TODO spam
- no random sample values in production path
- no dead imports
- no unused components
- no inconsistent terminology
- no emojis
- no console.log debugging
Backend dependencies
When the UI expects a backend capability that is not yet implemented, document it using a precise contract such as:
GET /api/v1/keys?cursor=<opaque>&limit=100&pattern=user:*

Do not invent hidden behavior.
Architectural invariant
The React application is not part of Raft consensus.
Never move consensus decisions into browser code.
The browser displays, requests, and administers. The server decides.

---

# 35. `CHATGPT.md`

```md
# CHATGPT.md

## Role

When working on this repository, ChatGPT should treat RaftKV as a distributed-systems project first and a UI project second.

Frontend recommendations must preserve the system's architecture and should make protocol behavior easier to understand rather than obscuring it.

Read `AGENTS.md` before proposing frontend modifications.

## Dashboard objective

The RaftKV dashboard should provide a professional systems-control interface for:

- understanding cluster health
- inspecting node state
- exploring replicated key/value data
- running client commands
- observing Raft behavior
- viewing performance metrics
- running deterministic simulations
- performing controlled administrative operations

It is not intended to imitate RedisInsight exactly.

It should reflect RaftKV's distinct capabilities.

## Visual character

Use a modern enterprise engineering aesthetic.

Characteristics:

- dark neutral background
- restrained blue primary accent
- thin structural borders
- compact typography
- high information density
- clear system status
- subtle motion
- minimal decoration
- no emojis

Do not describe the design using vague language such as "futuristic", "cyberpunk", or "beautiful".

Prefer concrete interface decisions.

## Screens

### Overview

Prioritize:

- leader
- term
- quorum
- node roles
- commit index
- applied index
- log position
- replication lag
- storage
- state hash
- request rate
- latency

### Key Explorer

Prioritize:

- SCAN-based enumeration
- key filtering
- value representation
- TTL
- size
- editing
- delete confirmation
- binary-safe display

Do not recommend `KEYS *` as the normal browsing mechanism.

### Command Console

Prioritize both command result and distributed execution context.

Useful execution metadata:

- receiving node
- leader
- term
- log index
- replication count
- commit time
- apply time
- request duration

### Raft Visualizer

Show causality.

Important transitions include:

- election starts
- vote requests
- votes
- leader election
- AppendEntries
- append rejection
- conflict repair
- commit advancement
- entry application
- ReadIndex rounds
- snapshot transfer
- leadership transfer

Do not turn the visualizer into decorative moving lines.

### Metrics

Keep custom metrics views compact.

Grafana remains appropriate for deep time-series analysis.

The RaftKV UI should surface the metrics required to understand immediate cluster behavior.

### Simulation Lab

Keep simulation clearly separate from the live cluster.

Simulation controls may include:

- start
- pause
- step
- reset
- partition
- heal
- crash
- restart
- delay
- drop
- duplicate
- reorder

Always display the simulation seed.

### Administration

Destructive controls must be explicit.

Use confirmation for:

- removing a member
- FLUSHDB
- restore
- dangerous quorum changes

The frontend does not replace backend authorization.

## Architecture

Expected structure:

```text
React / TypeScript
        |
        | HTTP + SSE
        v
Browser-facing control API
        |
        v
RaftKV runtime
        |
        +-- raft-core
        +-- storage
        +-- state machine
        +-- peer network

Never propose direct browser access to:
- sled
- Raft logs
- snapshot files
- raw peer transport
- in-process Raft structures
Source of truth
API models belong in:
src/types/api.ts
HTTP behavior belongs in:
src/lib/api.ts
Formatting belongs in:
src/lib/format.ts
Mock development state belongs in:
src/lib/mock.ts
SSE behavior belongs in:
src/hooks/useRaftEvents.ts
Pages should consume those abstractions.
Coding responses
When giving code:
- provide complete files when the user requests copy-paste-ready code
- keep imports correct
- preserve existing directory structure
- avoid unrelated backend changes
- do not omit essential CSS
- avoid pseudo-code if implementation code was requested
- do not replace working architecture with a new framework without reason
Terminology
Use consistent distributed-systems terminology:
- leader
- follower
- candidate
- pre-candidate
- term
- voter
- learner
- quorum
- commit index
- applied index
- match index
- next index
- replication lag
- snapshot index
- joint consensus
- linearizable read
- ReadIndex
Avoid generic labels when a precise Raft term exists.
Safety of claims
Do not make UI text claim stronger guarantees than the backend actually provides.
For example, only label a read "Linearizable" if the backend's read path really implements the required ReadIndex quorum confirmation.
Only label membership administration "available" if the runtime control path is wired.
The UI should be able to represent unavailable or experimental capability honestly.
No emojis
Do not insert emojis into:
- navigation
- headings
- buttons
- status messages
- toast messages
- empty states
- documentation examples
Use Lucide icons where visual indicators are useful.
Completion checklist
Before considering a frontend response complete, verify conceptually that the implementation includes:
- route structure
- type definitions
- API layer
- event stream
- shell/navigation
- overview
- key explorer
- command console
- Raft visualizer
- metrics
- simulation lab
- administration
- responsive styling
- loading states
- basic failure states
- destructive confirmation
- demo support
- no emojis
The result should look like one coherent product, not seven unrelated demo pages.

---

## Run it

```bash
cd ui/dashboard
npm install
cp .env.example .env
npm run dev

For the UI by itself:
VITE_DEMO_MODE=true

For your actual RaftKV Axum backend:
VITE_DEMO_MODE=false

This frontend implements the GUI scope only, including the live cluster view, SCAN-oriented Key Explorer, command execution interface, Raft event visualizer, metrics, isolated simulation workspace, and administration surface described in your design. The Key Explorer and Raft Visualizer specifically follow the behavior you laid out for paginated keys and SSE-driven protocol events.    Pasted text