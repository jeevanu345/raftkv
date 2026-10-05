# Control and lab API

HTTP JSON errors carry `message`. Administrative/command bodies are bounded to 2 MiB. Protected responses include an `X-Request-Id`; audit records use the same correlation ID. All browser calls are centralized in `ui/dashboard/src/lib/api.ts`. TypeScript contracts are in `src/types/api.ts`; Rust node diagnostics are typed in `crates/raftkv-server/src/diagnostics.rs`.

| Method | Endpoint | Semantics |
|---|---|---|
| GET | `/health/live`, `/health/ready` | Process liveness; recent current-term quorum/leader contact and no storage fence |
| GET | `/metrics` | Prometheus exposition |
| POST | `/api/v1/auth` | `{token}` → session cookie |
| GET | `/api/v1/status` | Local typed diagnostics; recent log metadata only |
| GET | `/api/v1/cluster`, `/api/v1/nodes`, `/api/v1/nodes/:id` | Cluster summary / parallel bounded remote diagnostics |
| GET | `/api/v1/events` | SSE with event IDs, Last-Event-ID replay and gap notices |
| GET | `/api/v1/keys?cursor=0&limit=100&pattern=*` | Linearizable, bounded SCAN metadata page |
| GET | `/api/v1/keys/:key` | Linearizable value and TTL metadata |
| PUT | `/api/v1/keys/:key` | `{value,encoding:"utf8"|"base64"|"hex",ttlMs:null|number}` through Raft |
| DELETE | `/api/v1/keys/:key` | Replicated DEL |
| POST | `/api/v1/commands` | `{command,confirmed?,targetNodeId?}`; pretty/raw RESP and exact mutation receipt |
| GET | `/api/v1/snapshots` | Published snapshot metadata |
| POST | `/api/v1/admin/snapshot` | Durable checkpoint |
| POST | `/api/v1/admin/leadership` | `{targetId}`; request transfer to another voter |
| POST | `/api/v1/admin/members` | `{id,role,raftAddress,clientAddress,adminAddress,certificateSha256?}` |
| DELETE | `/api/v1/admin/members/:id` | Learner removal or joint voting change; cannot remove final voter |
| GET | `/api/v1/admin/backup` | Linearizable checksummed binary backup; follower forwards to leader |

Binary key names use `~base64:<base64>` in JSON/path representation. A literal UTF-8 key beginning with that prefix is encoded too, avoiding collisions. Encode the complete displayed key as a URL path segment. Cursor tokens are opaque to clients and must not be parsed as storage positions. A page can be empty with a nonempty continuation cursor when MATCH filters examined keys.

Mutation command receipts contain receivedByNodeId, leaderId, term, proposedIndex, committedMs, appliedMs, replicatedTo and voterCount. Reads/non-proposals omit unavailable proposal timing. Explicit targetNodeId executes at that node and can return a MOVED RESP error; an omitted target selects/forwards to the current leader. FLUSHDB requires `confirmed=true`. API forwarding uses advertised admin URLs and retains the caller's bearer/cookie identity; one forward hop prevents leader-change loops.

Lab endpoints exist only on `raftkv-lab` (default localhost:8090): GET `/lab/api/v1/state`, POST `/configure`, GET/POST `/replay`, and POST named actions. Supported actions: start, pause, step, reset, heal, partition, asymmetric, crash, restart, delay, drop, duplicate, reorder, disk-failure, fsync-failure, torn-write, snapshot-failure, clock-stall, slow-follower, snapshot, propose and remove-member. Payload uses nodeId, peerId, ticks or value as appropriate. Replay records exact seed/config/action ticks and is bounded to 10,000 actions and 100,000 logical ticks.

Runtime/audit timestamp strings contain decimal Unix milliseconds; parse them as a number before constructing a browser Date.
