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
      role={onClick?"button":undefined}
      tabIndex={onClick?0:undefined}
      aria-label={onClick?`Node ${node.nodeId} diagnostics`:undefined}
      onKeyDown={event=>{if(onClick && (event.key==="Enter" || event.key===" ")){event.preventDefault();onClick();}}}
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
            node.health === "unreachable" ? "unreachable" : node.role === "leader"
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
          {node.uptimeSeconds===null ? "Uptime unavailable" : `${Math.floor(node.uptimeSeconds / 3600)}h uptime`}
        </span>
      </div>
    </article>
  );
}
