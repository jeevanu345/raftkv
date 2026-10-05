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
