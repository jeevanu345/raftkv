import {
  Activity,
  Pause,
  Play,
  RotateCcw,
  ShieldAlert,
  SkipForward,
  Unplug
} from "lucide-react";

import {useState} from "react";
import {
  useMutation,
  useQuery,
  useQueryClient
} from "@tanstack/react-query";

import { api } from "../lib/api";

import {
  formatNumber,
  formatRole
} from "../lib/format";

import Panel from "../components/common/Panel";
import StatusPill from "../components/common/StatusPill";

export default function SimulationLabPage() {
  const queryClient = useQueryClient();

  const simulation = useQuery({
    queryKey: ["simulation"],
    queryFn: api.getSimulation,
    refetchInterval: 1_000
  });

  const [seed,setSeed]=useState(51966);const [nodeCount,setNodeCount]=useState(5);const [drop,setDrop]=useState(0);const [delay,setDelay]=useState(4);
  const configure=useMutation({mutationFn:()=>api.configureSimulation({seed,nodeCount,delayMinTicks:1,delayMaxTicks:delay,dropProbability:drop}),onSuccess:state=>queryClient.setQueryData(["simulation"],state)});
  const action=useMutation({mutationFn:(request:{name:string;nodeId?:number;peerId?:number;ticks?:number;value?:string})=>api.simulationAction(request.name,{nodeId:request.nodeId,peerId:request.peerId,ticks:request.ticks,value:request.value}),onSuccess:state=>queryClient.setQueryData(["simulation"],state)});

  if (!simulation.data) {
    return (
      <div className="page-state">
        {simulation.error?.message??"Loading simulation"}
      </div>
    );
  }

  const data = simulation.data;

  return (
    <div className="stack-xl">
      {(action.error??configure.error) && <div className="feedback feedback--error" role="alert">{(action.error??configure.error)?.message}</div>}
      <Panel title="Simulation configuration" description="Isolated from the live cluster. Reset uses this exact seed."><form className="member-form" onSubmit={event=>{event.preventDefault();configure.mutate();}}><label>Seed<input type="number" min="0" value={seed} onChange={event=>setSeed(Number(event.target.value))}/></label><label>Nodes<input type="number" min="1" max="9" value={nodeCount} onChange={event=>setNodeCount(Number(event.target.value))}/></label><label>Maximum delay (ticks)<input type="number" min="1" max="1000" value={delay} onChange={event=>setDelay(Number(event.target.value))}/></label><label>Drop probability<input type="number" min="0" max="1" step="0.01" value={drop} onChange={event=>setDrop(Number(event.target.value))}/></label><button className="button button--primary" disabled={configure.isPending}>Configure and reset</button></form><div className="inline-actions">{(["delay","drop","duplicate","reorder"] as const).map(name=><button key={name} className="button button--ghost" disabled={action.isPending} onClick={()=>action.mutate({name})}>{name} message</button>)}<button className="button button--ghost" onClick={async()=>{const replay=await api.exportSimulation();const link=document.createElement("a");link.href=URL.createObjectURL(new Blob([JSON.stringify(replay,null,2)],{type:"application/json"}));link.download="raftkv-replay.json";link.click();URL.revokeObjectURL(link.href);}}>Export replay</button><label>Import replay<input type="file" accept="application/json" onChange={async event=>{const file=event.target.files?.[0];if(file){try{const state=await api.importSimulation(JSON.parse(await file.text()));queryClient.setQueryData(["simulation"],state);}catch(error){window.alert(error instanceof Error?error.message:"Replay failed");}}}}/></label></div></Panel>
      <section className="simulation-toolbar">
        <div>
          <div className="hero-strip__eyebrow">
            Deterministic simulator
          </div>

          <h2>
            Seed {data.seed}
          </h2>

          <p>
            Tick {formatNumber(data.tick)}
            {" · "}
            {data.nodes.length} nodes
          </p>
        </div>

        <div className="inline-actions">
          <button className="button button--ghost" disabled={action.isPending||!data.leaderId} onClick={()=>action.mutate({name:"propose",value:"lab-entry"})}>Propose entry</button>
          <button
            className="button button--primary"
            onClick={() =>
action.mutate({name:data.running?"pause":"start"})
            }
          >
            {data.running ? (
              <Pause size={14} />
            ) : (
              <Play size={14} />
            )}

            {data.running ? "Pause" : "Start"}
          </button>

          <button
            className="button button--ghost"
            onClick={() =>
              action.mutate({name:"step"})
            }
          >
            <SkipForward size={14} />
            Step
          </button>

          <button
            className="button button--ghost"
            onClick={() =>
              action.mutate({name:"heal"})
            }
          >
            <Activity size={14} />
            Heal network
          </button>

          <button
            className="button button--ghost"
            onClick={() =>
              action.mutate({name:"reset"})
            }
          >
            <RotateCcw size={14} />
            Reset
          </button>
        </div>
      </section>

      <Panel title="Directed partition and membership" description="These actions affect only simulated nodes."><form className="member-form" onSubmit={event=>{event.preventDefault();const form=new FormData(event.currentTarget);action.mutate({name:"asymmetric",nodeId:Number(form.get("from")),peerId:Number(form.get("to"))});}}><label>Block sender<select name="from">{data.nodes.map(node=><option key={node.id}>{node.id}</option>)}</select></label><label>To receiver<select name="to">{data.nodes.map(node=><option key={node.id}>{node.id}</option>)}</select></label><button className="button button--ghost" disabled={action.isPending}>Block directed link</button></form><label>Remove simulated voter<select defaultValue="" disabled={action.isPending} onChange={event=>{if(event.target.value)action.mutate({name:"remove-member",nodeId:Number(event.target.value)});event.target.value="";}}><option value="">Choose a node</option>{data.nodes.map(node=><option key={node.id}>{node.id}</option>)}</select></label></Panel>
      <div className="simulation-grid">
        {data.nodes.map((node) => (
          <article
            key={node.id}
            className={`simulation-node ${
              node.partitioned
                ? "simulation-node--partitioned"
                : ""
            } ${
              node.crashed
                ? "simulation-node--crashed"
                : ""
            }`}
          >
            <header>
              <div>
                <strong>
                  Node {node.id}
                </strong>

                <span>
                  {formatRole(node.role)}
                </span>
              </div>

              <StatusPill
                status={
                  node.crashed
                    ? "unreachable"
                    : node.role === "leader"
                      ? "leader"
                      : "follower"
                }
                label={
                  node.crashed
                    ? "Crashed"
                    : node.partitioned
                      ? "Partitioned"
                      : formatRole(node.role)
                }
              />
            </header>

            <div className="simulation-node__stats">
              <div>
                <span>Term</span>
                <strong>{node.term}</strong>
              </div>

              <div>
                <span>Commit</span>
                <strong>
                  {node.commitIndex}
                </strong>
              </div>

              <div>
                <span>Last log</span>
                <strong>
                  {node.lastLogIndex}
                </strong>
              </div>
            </div>

            <div className="simulation-node__actions">
              <button className="button button--ghost button--small" disabled={action.isPending} onClick={()=>action.mutate({name:"partition",nodeId:node.id})}>
                <Unplug size={13} />
                {node.partitioned?"Reconnect":"Partition"}
              </button>

              <button className="button button--danger-ghost button--small" disabled={action.isPending} onClick={()=>action.mutate({name:node.crashed?"restart":"crash",nodeId:node.id})}>
                <ShieldAlert size={13} />
                {node.crashed?"Restart":"Crash"}
              </button>
              <label>Storage/clock fault<select aria-label={`Node ${node.id} fault`} defaultValue="" disabled={action.isPending} onChange={event=>{if(event.target.value)action.mutate({name:event.target.value,nodeId:node.id,ticks:20});event.target.value="";}}><option value="">Choose a fault</option>{["disk-failure","fsync-failure","torn-write","snapshot-failure","clock-stall","slow-follower","snapshot"].map(name=><option key={name} value={name}>{name}</option>)}</select></label>
            </div>
          </article>
        ))}
      </div>

      <Panel
        title="Simulation event timeline"
        description="Reproducible protocol history for the current seed."
      >
        <div className="event-stream">
          {data.events.map((event) => (
            <div
              key={event.seq}
              className="event-row"
            >
              <div className="event-row__sequence">
                #{event.seq}
              </div>

              <div className="event-row__content">
                <div>
                  <strong>
                    {event.type}
                  </strong>

                  <span>
                    Node {event.nodeId}
                  </span>

                  <span>
                    Term {event.term}
                  </span>
                </div>

                <p>
                  {event.detail ??
                    "Protocol transition"}
                </p>
              </div>
            </div>
          ))}
        </div>
      </Panel>
    </div>
  );
}
