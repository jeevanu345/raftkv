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
  mockSnapshots, demoPut, demoDelete, demoCommand, demoSimulationAction, demoConfigure, demoMembers
} from "./mock";

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? "";
let rateSample: {at:number;read:number;write:number}|undefined;
const DEMO_MODE = import.meta.env.VITE_DEMO_MODE === "true";

async function request<T>(
  path: string,
  init: RequestInit = {}
): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    ...init,
    credentials:"include",

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
  async downloadBackup():Promise<Blob>{if(DEMO_MODE)throw new Error("Backups require a live cluster.");const response=await fetch(`${API_BASE}/api/v1/admin/backup`,{credentials:"include"});if(!response.ok)throw new Error(`Backup failed: ${response.status}`);return response.blob();},
  async login(token:string):Promise<void>{await request("/api/v1/auth",{method:"POST",body:JSON.stringify({token})});},
  async exportSimulation():Promise<unknown>{if(DEMO_MODE)return structuredClone(mockSimulation);return request("/lab/api/v1/replay");},
  async importSimulation(replay:unknown):Promise<SimulationState>{if(DEMO_MODE)throw new Error("Import replay requires the deterministic lab backend; demo mode contains UI fixtures.");return request("/lab/api/v1/replay",{method:"POST",body:JSON.stringify(replay)});},
  async getCluster(): Promise<ClusterSummary> {
    if (DEMO_MODE) {
      await sleep();
      return structuredClone(mockCluster);
    }

    const cluster=await request<ClusterSummary>("/api/v1/cluster");
    const sample={at:performance.now(),read:cluster.metrics.readRequestsTotal??0,write:cluster.metrics.writeRequestsTotal??0};
    if(rateSample){const seconds=(sample.at-rateSample.at)/1000;cluster.metrics.readRequestsPerSecond=Math.max(0,sample.read-rateSample.read)/seconds;cluster.metrics.writeRequestsPerSecond=Math.max(0,sample.write-rateSample.write)/seconds;cluster.metrics.requestsPerSecond=cluster.metrics.readRequestsPerSecond+cluster.metrics.writeRequestsPerSecond;}
    rateSample=sample;return cluster;
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
      await sleep();demoPut(key,body);return;
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
      await sleep();demoDelete(key);return;
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

      return demoCommand(body);
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
      await sleep();mockSnapshots.unshift({id:`demo-${Date.now()}`,createdAt:new Date().toISOString(),lastIncludedIndex:mockCluster.appliedIndex,lastIncludedTerm:mockCluster.term,sizeBytes:1024,stateHash:"demo fixture"});return;
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
      await sleep();mockCluster.leaderId=body.targetId;mockCluster.term++;mockCluster.nodes.forEach(n=>{n.role=n.nodeId===body.targetId?"leader":"follower";n.leaderId=body.targetId;n.term=mockCluster.term;});return;
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
      await sleep();demoMembers(body);return;
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
      await sleep();mockCluster.members=mockCluster.members?.filter(m=>m.id!==id);return;
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
      await sleep();return demoConfigure(body);
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
    action: string /* Validated by the isolated lab backend. */,

    payload?: Record<string, unknown>
  ): Promise<SimulationState> {
    if (DEMO_MODE) {
      await sleep();return demoSimulationAction(action,payload);
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
