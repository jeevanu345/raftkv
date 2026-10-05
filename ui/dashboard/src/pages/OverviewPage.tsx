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

import { useState } from "react";
import NodeDetails from "../components/raft/NodeDetails";

export default function OverviewPage() {
  const [selected,setSelected]=useState<number|null>(null);
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
        {cluster.error?.message ?? "Cluster data is unavailable."}
      </div>
    );
  }

  const data = cluster.data;

  const maxLogIndex = Math.max(
    ...data.nodes.map(
      (node) => node.lastLogIndex??0
    ),
    1
  );

  return (
    <div className="stack-xl">
      {selected!==null && data.nodes.find(node=>node.nodeId===selected) && <NodeDetails node={data.nodes.find(node=>node.nodeId===selected)!} onClose={()=>setSelected(null)}/>} 
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
            onClick={()=>setSelected(node.nodeId)}
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
                ((node.lastLogIndex??0) /
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
