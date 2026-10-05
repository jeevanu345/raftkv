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
import { useCluster } from "../hooks/useCluster";

import type {
  CommandResponse
} from "../types/api";

import ConfirmDialog from "../components/common/ConfirmDialog";
import Panel from "../components/common/Panel";

interface ConsoleEntry {
  id: number;
  command: string;
  response: CommandResponse;
}

export default function CommandConsolePage() {
  const cluster = useCluster();
  const [target, setTarget] = useState("");
  const [command, setCommand] =
    useState("INFO raft");

  const [history, setHistory] =
    useState<ConsoleEntry[]>([]);

  const [running, setRunning] =
    useState(false);

  const [rawVisible,setRawVisible]=useState<Record<number,boolean>>({});
  const [error,setError]=useState("");
  const [confirmFlush,setConfirmFlush]=useState(false);
  const [historyIndex,setHistoryIndex]=useState(-1);
  async function execute(confirmed=false) {
    const trimmed = command.trim();

    if (!trimmed || running) {
      return;
    }

    if(/^FLUSHDB(?:\s|$)/i.test(trimmed) && !confirmed){setConfirmFlush(true);return;}
    setError("");
    setRunning(true);

    try {
      const response =
        await api.executeCommand({
          command: trimmed, confirmed, targetNodeId: target ? Number(target) : undefined
        });

      setHistory((current) => [
        {
          id: Date.now(),
          command: trimmed, confirmed,
          response
        },
        ...current
      ].slice(0,200));
    } catch(error) {setError(error instanceof Error?error.message:"Command failed");} finally {
      setRunning(false);
    }
  }

  function onKeyDown(
    event: KeyboardEvent<HTMLInputElement>
  ) {
    if(event.key==="ArrowUp" || event.key==="ArrowDown"){event.preventDefault();const next=event.key==="ArrowUp"?Math.min(historyIndex+1,history.length-1):Math.max(-1,historyIndex-1);setHistoryIndex(next);setCommand(next<0?"":history[next].command);return;}
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
      <ConfirmDialog open={confirmFlush} title="Flush replicated database" description="Delete every key through Raft?" confirmLabel="Flush database" danger onClose={()=>setConfirmFlush(false)} onConfirm={()=>execute(true)}/>
      {error && <div className="feedback feedback--error" role="alert">{error}</div>}
      <Panel
        className="console-panel"
        title="Command Console"
        description="Execute supported RESP commands against RaftKV."
      >
        <label className="field">Command target
          <select aria-label="Command target" value={target} onChange={event=>setTarget(event.target.value)} disabled={running}>
            <option value="">Current leader</option>
            {cluster.data?.nodes.map(node=><option key={node.nodeId} value={node.nodeId}>Node {node.nodeId} ({node.role})</option>)}
          </select>
        </label>
        <div className="console-input">
          <span className="console-prompt">
            raftkv&gt;
          </span>

          <input
            aria-label="RESP command"
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
            onClick={()=>execute()}
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
                <code>{entry.command.split(/(\s+|"[^"]*"|'[^']*')/).filter(Boolean).map((token,index)=><span key={index} className={index===0?"command-token--verb":/^['"]/.test(token)?"command-token--string":undefined}>{token}</span>)}</code>
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

              {rawVisible[entry.id]&&<pre className="console-entry__response">{entry.response.raw.replaceAll("\r","\\r").replaceAll("\n","\\n\n")}</pre>}
              <div className="console-entry__meta">
                <button onClick={()=>setRawVisible(current=>({...current,[entry.id]:!current[entry.id]}))}>{rawVisible[entry.id]?"Hide raw RESP":"Show raw RESP"}</button>
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
                    ).catch(()=>setError("Clipboard access failed"))
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
                        .proposedIndex ?? "Unavailable"}
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
