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

  const [frozen,setFrozen]=useState<typeof events>([]);
  const [paused, setPaused] =
    useState(false);

  if (!cluster.data) {
    return (
      <div className="page-state">
        {cluster.isError ? cluster.error.message : "Loading cluster visualization"}
      </div>
    );
  }

  const visibleEvents = paused
    ? frozen
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
                { if(!paused) setFrozen(events); setPaused(!paused); }
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
                const entries=(node.logEntries??[]).map(entry=>({index:entry.index,term:entry.term,state:entry.index<=(node.appliedIndex??0)?("applied" as const):entry.index<=(node.commitIndex??0)?("committed" as const):("uncommitted" as const)}));

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
                        node.snapshotIndex??undefined
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
            {visibleEvents.slice(0, 80).map(
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
