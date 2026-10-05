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
  logEntries?: {index:number;term:number;kind:string}[];
  nodeId: number;
  role: NodeRole;
  health: HealthState;

  term: number | null;
  leaderId: number | null;
  votedFor?: number | null;

  commitIndex: number | null;
  appliedIndex: number | null;
  lastLogIndex: number | null;
  snapshotIndex: number | null;

  stateHash: string;
  uptimeSeconds: number | null;

  raftAddress?: string;
  clientAddress?: string;
  adminAddress?: string;

  storageBytes?: number | null;
  keyCount?: number | null;

  peers: PeerDiagnostics[];
}

export interface LatencySummary {
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
}

export interface ClusterMetrics {
  readRequestsTotal?: number;
  writeRequestsTotal?: number;
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
  members?: MemberInfo[];
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
  confirmed?: boolean;
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
  certificateSha256?:string;
  id: number;
  role: "voter" | "learner";

  raftAddress: string;
  clientAddress?: string;
  adminAddress?: string;

  health?: HealthState;
}

export interface AddMemberRequest {
  certificateSha256?:string;
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
