import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";

import { useEffect, useState } from "react";

import { useCluster } from "../hooks/useCluster";

import {
  formatBytes,
  formatNumber
} from "../lib/format";

import MetricCard from "../components/common/MetricCard";
import Panel from "../components/common/Panel";

export default function MetricsPage() {
  const cluster = useCluster();

  const [data,setData]=useState<{second:string;requests:number;latency:number}[]>([]);
  useEffect(()=>{if(cluster.data){const metrics=cluster.data.metrics;setData(current=>[...current,{second:new Date().toLocaleTimeString(),requests:metrics.requestsPerSecond,latency:metrics.latency.p95Ms}].slice(-60));}},[cluster.data]);

  if (!cluster.data) {
    return (
      <div className="page-state">
        {cluster.isError ? cluster.error.message : "Loading metrics"}
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
                    fontSize: "0.6875rem"
                  }}
                  axisLine={false}
                  tickLine={false}
                />

                <YAxis
                  tick={{
                    fill: "var(--text-tertiary)",
                    fontSize: "0.6875rem"
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
                    fontSize: "0.6875rem"
                  }}
                  axisLine={false}
                  tickLine={false}
                />

                <YAxis
                  tick={{
                    fill: "var(--text-tertiary)",
                    fontSize: "0.6875rem"
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
