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

// Stateful UI fixtures. These helpers never run in live mode and do not claim
// to implement the Raft simulator or record real consensus latency.
export function demoPut(key:string,body:import("../types/api").PutKeyRequest){
 const detail:KeyDetail={key,type:body.encoding==="base64"||body.encoding==="hex"?"binary":"string",encoding:body.encoding??"utf8",value:body.value,sizeBytes:new TextEncoder().encode(body.value).length,ttlMs:body.ttlMs??null};
 mockKeyDetails[key]=detail;mockKeyPage.items=Object.values(mockKeyDetails).map(({key,type,sizeBytes,ttlMs})=>({key,type,sizeBytes,ttlMs}));mockCluster.totalKeys=mockKeyPage.items.length;
}
export function demoDelete(key:string){delete mockKeyDetails[key];mockKeyPage.items=mockKeyPage.items.filter(i=>i.key!==key);mockCluster.totalKeys=mockKeyPage.items.length;}
export function demoCommand(body:import("../types/api").CommandRequest):import("../types/api").CommandResponse {
 const parts=body.command.match(/"[^"]*"|'[^']*'|\S+/g)?.map(p=>p.replace(/^['"]|['"]$/g,""))??[];const command=parts[0]?.toUpperCase();let display="OK",raw="+OK\r\n",success=true;
 if(command==="GET"){const value=mockKeyDetails[parts[1]]?.value;display=value??"(nil)";raw=value===undefined?"$-1\r\n":`$${new TextEncoder().encode(value).length}\r\n${value}\r\n`;}
 else if(command==="SET" && parts.length===3)demoPut(parts[1],{value:parts[2]});
 else if(command==="DEL"){const count=parts.slice(1).filter(k=>mockKeyDetails[k]).length;parts.slice(1).forEach(demoDelete);display=String(count);raw=`:${count}\r\n`;}
 else if(command==="DBSIZE"){display=String(mockKeyPage.items.length);raw=`:${display}\r\n`;}
 else if(command==="FLUSHDB" && body.confirmed){Object.keys(mockKeyDetails).forEach(demoDelete);}
 else if(command==="PING"){display="PONG";raw="+PONG\r\n";}
 else if(command==="INFO"||command==="CLUSTER"){display="Demo fixture: node 2, term 17. Live protocol diagnostics require a running cluster.";raw=display;}
 else {display="ERR command unsupported in demo fixture";raw=`-${display}\r\n`;success=false;}
 return {display,raw,success,durationMs:0,execution:{receivedByNodeId:mockCluster.leaderId,leaderId:mockCluster.leaderId,term:mockCluster.term}};
}
export function demoConfigure(config:import("../types/api").SimulationConfig):SimulationState{mockSimulation.seed=config.seed;mockSimulation.tick=0;mockSimulation.running=false;mockSimulation.leaderId=null;mockSimulation.nodes=Array.from({length:config.nodeCount},(_,i)=>({id:i+1,role:"follower",term:0,commitIndex:0,lastLogIndex:0,partitioned:false,crashed:false}));return structuredClone(mockSimulation);}
export function demoSimulationAction(action:string,payload?:Record<string,unknown>):SimulationState{
 const node=mockSimulation.nodes.find(n=>n.id===payload?.nodeId);
 if(action==="start")mockSimulation.running=true;else if(action==="pause")mockSimulation.running=false;else if(action==="step")mockSimulation.tick++;else if(action==="reset"){mockSimulation.tick=0;mockSimulation.events=[];}else if(action==="partition"&&node)node.partitioned=!node.partitioned;else if(action==="crash"&&node)node.crashed=true;else if(action==="restart"&&node)node.crashed=false;else if(action==="heal")mockSimulation.nodes.forEach(n=>n.partitioned=false);
 if(["disk-failure","fsync-failure","torn-write","snapshot-failure"].includes(action)&&node)node.crashed=true;
 if(action==="remove-member"&&node)mockSimulation.nodes=mockSimulation.nodes.filter(n=>n.id!==node.id);
 if(action==="propose"){const leader=mockSimulation.nodes.find(n=>n.role==="leader");if(leader)leader.lastLogIndex++;}
 if(!["start","pause","step","reset","heal","partition","crash","restart"].includes(action))mockSimulation.events.unshift({seq:Date.now(),nodeId:node?.id??0,term:node?.term??0,type:"MessageSent",detail:`Demo fixture action: ${action}`});
 mockSimulation.events=mockSimulation.events.slice(0,250);
 return structuredClone(mockSimulation);
}
export function demoMembers(body:import("../types/api").AddMemberRequest){mockCluster.members??=mockCluster.nodes.map(n=>({id:n.nodeId,role:"voter",raftAddress:n.raftAddress??"",clientAddress:n.clientAddress,adminAddress:n.adminAddress}));mockCluster.members=mockCluster.members.filter(m=>m.id!==body.id);mockCluster.members.push({...body});}
