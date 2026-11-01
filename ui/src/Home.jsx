import { useEffect, useRef, useState } from "react";
import { Activity, ArrowRight, BookOpen, Check, ChevronRight, Copy, Crown, KeyRound, Network, Server, Settings2, Terminal, TriangleAlert } from "lucide-react";
import { EventList, Pill, time } from "./components.jsx";
import { number } from "./model.js";
import "./home.css";

const TOOLS = [
  { pane: "keys", icon: KeyRound, title: "Explore your keyspace", description: "Browse stored values and send a write through the observed leader.", label: "Open keyspace" },
  { pane: "nodes", icon: Network, title: "Inspect replication", description: "Compare node roles, applied indices, and leader-reported progress.", label: "Inspect nodes" },
  { pane: "telemetry", icon: Activity, title: "Follow cluster activity", description: "Trace elections, runtime counters, and recent browser observations.", label: "View telemetry" },
];

function Command({ value }) {
  const [feedback, setFeedback] = useState("");
  const timeout = useRef(null);
  useEffect(() => () => clearTimeout(timeout.current), []);
  async function copy() {
    clearTimeout(timeout.current);
    try {
      await navigator.clipboard.writeText(value);
      setFeedback("Copied");
    } catch {
      setFeedback("Select the command to copy it manually.");
    }
    timeout.current = setTimeout(() => setFeedback(""), 3000);
  }
  return <div className="home-command-wrap">
    <div className="home-command">
      <span aria-hidden="true">$</span>
      <code>{value}</code>
      <button className="icon-button" aria-label="Copy cluster startup command" onClick={copy} title="Copy command">
        {feedback === "Copied" ? <Check size={15} /> : <Copy size={15} />}
      </button>
    </div>
    <span className="home-copy-feedback" role="status">{feedback}</span>
  </div>;
}

export default function Home({ summary, observations = [], monitors = [], events = [], lastRefresh, refreshing, onNavigate, onSelectNode, user }) {
  const online = summary?.online || observations.filter((node) => node.online && node.status);
  const reference = summary?.reference;
  const leader = summary?.leaders?.length === 1 ? summary.leaders[0] : null;
  const pending = !lastRefresh && !observations.length;
  const offline = !online.length;
  const hasWarning = !!(summary?.conflictingLeaders || summary?.hashMismatch || (online.length && (!leader || online.length < monitors.length)));
  const status = pending ? "Checking node APIs" : offline ? "No reachable node APIs" : hasWarning ? "Review cluster observations" : "Node APIs connected";
  const tone = pending ? "" : offline || hasWarning ? "warn" : "good";
  const name = (user?.name || user?.displayName || user?.email?.split("@")[0] || "").trim().split(/\s+/)[0];
  const endpoints = monitors.length ? monitors : observations;
  const visibleEndpoints = endpoints.slice(0, 6);

  return <div className="home-view">
    <section className="home-welcome" aria-labelledby="home-welcome-title">
      <div>
        <h2 id="home-welcome-title">Welcome{name ? `, ${name}` : " to your workspace"}.</h2>
        <p>Your local cluster, a little clearer. Connect, inspect, and follow the state of your data.</p>
      </div>
      <button className="button" onClick={() => onNavigate("settings")}><Settings2 size={15} />Manage connections</button>
    </section>

    <section className={`home-cluster ${tone}`} aria-labelledby="home-cluster-title">
      <div className="home-cluster-heading">
        <div className="home-cluster-intro">
          <div className="home-title-row"><h2 id="home-cluster-title">Local cluster</h2><Pill tone={tone}><span className={`status-dot ${tone}`} />{status}</Pill></div>
          <p>{pending ? "Waiting for the first status response from your configured endpoints." : offline ? "Start a local cluster or check the API ports in your connections." : "Independent node samples from your configured local API endpoints."}</p>
        </div>
        <button className="text-button home-cluster-open" onClick={() => onNavigate("overview")}>Cluster overview<ArrowRight size={15} /></button>
      </div>
      <dl className="home-cluster-facts">
        <div><dt>Reachable endpoints</dt><dd>{pending ? "—" : <>{online.length}<span> / {monitors.length || observations.length}</span></>}</dd></div>
        <div>
          <dt>Observed leader</dt>
          <dd>
            {leader ? <><Crown size={16} aria-hidden="true" />Node {leader.id}</> : "—"}
            <small>{pending ? "Waiting for a sample" : summary?.leaders?.length > 1 ? "Multiple leaders reported" : offline ? "Connect an API to inspect" : !leader ? "No single leader observed" : `Term ${leader.status.term}`}</small>
          </dd>
        </div>
        <div><dt>Committed index</dt><dd>{number(reference?.commit_index)}<small>{reference ? `Applied ${number(reference.applied_index)} on reference node ${reference.node_id}` : "Available after connecting"}</small></dd></div>
      </dl>
      <div className="home-endpoints" role="group" aria-label="Configured node endpoints">
        {visibleEndpoints.map((endpoint) => {
          const node = observations.find((item) => item.id === endpoint.id);
          return <button className="home-endpoint" key={endpoint.id} onClick={() => {
            onSelectNode?.(endpoint.id);
            onNavigate("nodes");
          }} title={`Inspect node ${endpoint.id}`}>
            <Server size={15} aria-hidden="true" />
            <span><strong>Node {endpoint.id}</strong><code>127.0.0.1:{endpoint.port}</code></span>
            <span className={`home-endpoint-state ${node?.online ? "good" : node ? "warn" : ""}`}>{node?.online ? node.status?.role || "reachable" : node ? "unreachable" : "checking"}</span>
          </button>;
        })}
        {endpoints.length > visibleEndpoints.length && <button className="text-button home-more-nodes" onClick={() => onNavigate("nodes")}>View all {endpoints.length} nodes<ChevronRight size={14} /></button>}
        {!endpoints.length && <p className="home-no-endpoints">Add a local API endpoint in connections to begin.</p>}
      </div>
      <div className="home-observation-note"><span>{refreshing ? "Checking endpoints…" : lastRefresh ? `Last check ${time(lastRefresh)}` : "Awaiting first check"}</span><span>API reachability does not establish a voting majority.</span></div>
      {(summary?.conflictingLeaders || summary?.hashMismatch) && <div className="home-cluster-warning"><TriangleAlert size={15} /><p>{summary.conflictingLeaders ? "Multiple nodes reported leadership in the same term. Review the independent samples in cluster overview." : "Different state hashes were observed at equal applied indices. Review the node samples in cluster overview."}</p></div>}
    </section>

    <section className="home-tools" aria-labelledby="home-tools-title">
      <div className="home-section-heading"><h2 id="home-tools-title">Your next move</h2><span>Tools for understanding your cluster</span></div>
      <div className="home-tool-grid">
        {TOOLS.map(({ pane, icon: Icon, title, description, label }) => <button className="home-tool" key={pane} onClick={() => onNavigate(pane)}>
          <span className="home-tool-title"><Icon size={17} aria-hidden="true" />{title}</span><span className="home-tool-description">{description}</span><span className="home-tool-action">{label}<ArrowRight size={14} aria-hidden="true" /></span>
        </button>)}
      </div>
    </section>

    <div className="home-bottom-grid">
      <section className="panel home-activity" aria-labelledby="home-activity-title">
        <div className="panel-heading"><div><h2 id="home-activity-title">Recent activity</h2><p>Observed transitions and actions in this browser.</p></div><button className="text-button" aria-label="View all recent activity" onClick={() => onNavigate("telemetry")}>View all<ChevronRight size={14} aria-hidden="true" /></button></div>
        {events.length ? <EventList events={events.slice(0, 4)} compact /> : <div className="home-empty-activity"><Activity size={22} aria-hidden="true" /><div><strong>No activity observed yet</strong><p>Node connections, elections, and key operations appear here as you work.</p></div></div>}
      </section>

      <section className="panel home-guide" aria-labelledby="home-guide-title">
        <div className="panel-heading"><div><h2 id="home-guide-title">{offline ? "Bring your cluster online" : "Know what you’re observing"}</h2><p>{offline ? "A quick start from the repository root." : "A short path from signals to understanding."}</p></div>{offline ? <Terminal size={18} /> : <BookOpen size={18} />}</div>
        {offline ? <div className="home-startup"><p>Start the default three-node cluster. The launcher builds the server and waits for a leader.</p><Command value="./scripts/run_cluster.sh" /><p className="home-startup-note">Requires Rust stable and Python 3. If a cluster is already running, check its API port in connections.</p><button className="text-button" onClick={() => onNavigate("settings")}>Open connections & runbook<ArrowRight size={14} /></button></div> : <div className="home-guide-links"><button onClick={() => onNavigate("report")}><span><strong>How RaftKV works</strong><small>Consensus, storage, and current trade-offs</small></span><ChevronRight size={16} /></button><button onClick={() => onNavigate("settings")}><span><strong>Plan a larger local cluster</strong><small>Endpoint settings and a step-by-step runbook</small></span><ChevronRight size={16} /></button></div>}
      </section>
    </div>
  </div>;
}
