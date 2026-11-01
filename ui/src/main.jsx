import { useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { Activity, ArrowDownToLine, ArrowRight, BookOpen, Check, ChevronRight, CircleHelp, Database, FileJson, GitBranch, Home as HomeIcon, KeyRound, LayoutDashboard, LogOut, Menu, Moon, Network, Pause, Play, Plus, RefreshCw, Search, Settings2, ShieldCheck, Sun, TriangleAlert, X } from "lucide-react";
import { clusterSummary, counterRate, diagnosticReport, duration, loadMonitors, number, validateMonitor } from "./model.js";
import { useCluster } from "./useCluster.js";
import { time, relative, text, safeStore, download, Pill, Panel, Metric, Detail, Empty, CopyButton, Sparkline, Topology, NodeTable, EventList, CodeBlock } from "./components.jsx";
import Keyspace from "./Keyspace.jsx";
import Architecture from "./Architecture.jsx";
import Connections from "./Connections.jsx";
import LogInspector from "./LogInspector.jsx";
import Home from "./Home.jsx";
import Welcome from "./Welcome.jsx";
import CommandMenu from "./CommandMenu.jsx";
import { loadSession, saveSession, clearSession } from "./session.js";
import "./styles.css";
const PANES = [
	{ id: "home", label: "Home", icon: HomeIcon, description: "Your local workspace, connected nodes, and tools to get things moving." },
	{
		id: "overview",
		label: "Cluster overview",
		icon: LayoutDashboard,
		description: "Consensus progress, topology, and operational signals across your local cluster."
	},
	{
		id: "nodes",
		label: "Nodes & replication",
		icon: Network,
		description: "Inspect every observed runtime and the leader’s per-peer replication progress."
	},
	{
		id: "keys",
		label: "Keyspace explorer",
		icon: KeyRound,
		description: "Inspect local state and propose durable writes through the observed leader."
	},
	{
		id: "telemetry",
		label: "Telemetry & events",
		icon: Activity,
		description: "Bounded browser histories, runtime counters, and observed consensus transitions."
	},
	{
		id: "report",
		label: "Architecture report",
		icon: BookOpen,
		description: "A source-grounded guide to the consensus engine, storage boundary, and current trade-offs."
	},
	{
		id: "settings",
		label: "Connections & runbook",
		icon: Settings2,
		description: "Connect more local node APIs and prepare a larger, fresh cluster safely."
	}
];
const COUNTERS = [
	["proposals_received", "Proposals received"],
	["proposals_committed", "Proposals committed"],
	["proposal_failures", "Proposal failures"],
	["reads_received", "Read-index requests"],
	["raft_messages_received", "Raft messages received"],
	["raft_messages_sent", "Raft messages sent"],
	["elections_started", "Elections started"],
	["elections_won", "Elections won"],
	["append_rejections", "Append rejections"],
	["storage_errors", "Storage errors"]
];
const readPane = () => PANES.some(pane => pane.id === location.hash.slice(1)) ? location.hash.slice(1) : "home";
function App({ user, onSignOut, theme, setTheme }) {
	const [pane, setActivePane] = useState(readPane);
	const [commandOpen, setCommandOpen] = useState(false);
	const [mobileNav, setMobileNav] = useState(false);
	const heading = useRef(null);
	const previousPane = useRef(pane);
	const setPane = (id) => {
		if (!PANES.some(p => p.id === id)) return;
		location.hash = id;
		setActivePane(id);
		setMobileNav(false);
	};
	useEffect(() => {
		const onHashChange = () => { setActivePane(readPane()); setMobileNav(false); };
		const onKeyDown = event => {
			if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
				event.preventDefault();
				setCommandOpen(value => !value);
			}
		};
		window.addEventListener("hashchange", onHashChange);
		window.addEventListener("keydown", onKeyDown);
		return () => { window.removeEventListener("hashchange", onHashChange); window.removeEventListener("keydown", onKeyDown); };
	}, []);
	useEffect(() => {
		document.title = `${PANES.find(p => p.id === pane).label} · RaftKV`;
		if (previousPane.current !== pane) { heading.current?.focus({ preventScroll: true }); window.scrollTo(0, 0); }
		previousPane.current = pane;
	}, [pane]);
	const [monitors, setMonitors] = useState(loadMonitors);
	const [paused, setPaused] = useState(false);
	const [interval, setIntervalMs] = useState(2500);
	const [nonce, setNonce] = useState(0);
	const [selectedId, setSelectedId] = useState(1);
	const [notice, setNotice] = useState(null);
	const discover = (nodes) => {
		const ids = new Set(nodes.flatMap((n) => n.online ? n.status.nodes?.map((peer) => peer.id) || [] : []));
		setMonitors((old) => {
			const next = [...old];
			for (const id of ids) if (next.length < 64 && !next.some((n) => n.id === id) && !validateMonitor(id, 8079 + id)) next.push({
				id,
				port: 8079 + id
			});
			return next.length === old.length ? old : next.sort((a, b) => a.id - b.id);
		});
	};
	const cluster = useCluster(monitors, discover, paused, interval, nonce);
	const summary = useMemo(() => clusterSummary(cluster.observations), [cluster.observations]);
	const selected = cluster.observations.find((n) => n.id === selectedId) || cluster.observations[0];
	const status = selected?.status;
	const reference = summary.reference;
	const selectedHistory = cluster.history[selected?.id] || [];
	const activePane = PANES.find((p) => p.id === pane);
	const refresh = () => {
		setPaused(false);
		setNonce((n) => n + 1);
	};
	useEffect(() => {
		safeStore("raftkv-monitors-v1", JSON.stringify(monitors));
	}, [monitors]);
	useEffect(() => {
		if (!notice) return;
		const timer = setTimeout(() => setNotice(null), 6e3);
		return () => clearTimeout(timer);
	}, [notice]);
	const exportData = () => {
		download(`raftkv-diagnostics-${Date.now()}.json`, JSON.stringify(diagnosticReport(cluster.observations, cluster.history, cluster.events), null, 2));
		setNotice("Diagnostic snapshot exported. It contains topology and telemetry, without key values.");
	};
	const alerts = [];
	if (cluster.observations.length && summary.online.length === 0) alerts.push("No node API is reachable. Start the cluster or check the local API ports in Connections & runbook.");
	for (const node of summary.online) if (node.status.health?.state === "failed") alerts.push(`Node ${node.id} runtime stopped after a storage failure: ${node.status.health.error || "inspect node logs"}. Writes to this node are disabled.`);
	if (summary.online.length && !summary.leaders.length) alerts.push("No leader was observed. Writes are disabled until a reachable node reports leadership.");
	if (summary.conflictingLeaders) alerts.push("Multiple nodes reported leadership in the same term. Samples are asynchronous; inspect node logs before drawing a safety conclusion.");
	if (summary.hashMismatch) alerts.push("State hashes differ at the same sampled applied index. Recheck and inspect logs; these are independent observations, not an atomic snapshot.");
	if (summary.quorum && summary.reachableVoters < summary.quorum) alerts.push(`Only ${summary.reachableVoters} of ${summary.voters.length} voter APIs are reachable. API reachability does not establish Raft quorum availability.`);
	return <div className="app-shell">
    <a href="#content" className="skip-link" onClick={event => {
      event.preventDefault();
      const content = document.getElementById("content");
      content?.focus();
      content?.scrollIntoView({ block: "start" });
    }}>Skip to dashboard</a>
    <div className={`sidebar-rail ${mobileNav ? "nav-open" : ""}`}>
    <aside className={`sidebar ${mobileNav ? "nav-open" : ""}`}>
      <a className="brand" href="#home">
        <img src="/mark.svg" alt="" width="36" height="36" />
        <div>
          <strong>RaftKV</strong>
          <span>Operator console</span>
        </div>
      </a>
      <button className="mobile-nav-toggle icon-button" aria-label={mobileNav ? "Close navigation" : "Open navigation"} aria-expanded={mobileNav} aria-controls="workspace-navigation" onClick={() => setMobileNav(value => !value)}>{mobileNav ? <X size={21} /> : <Menu size={21} />}</button>
      <button className="workspace-search" aria-label="Find a tool" onClick={() => setCommandOpen(true)}><Search size={16} aria-hidden="true" /><span>Find a tool</span><kbd>⌘K</kbd></button>
      <nav id="workspace-navigation" aria-label="Dashboard panes">
        {PANES.map(({ id, label, icon: Icon }) => <a href={`#${id}`} className={pane === id ? "active" : ""} key={id} onClick={() => setMobileNav(false)} aria-current={pane === id ? "page" : undefined}>
          <Icon size={17} aria-hidden="true" />
          {label}
        </a>)}
      </nav>
      <div className="sidebar-bottom">
        <div className="sidebar-cluster"><span className={`status-dot ${summary.online.length ? "good" : "warn"}`} />{summary.online.length ? `${summary.online.length} of ${monitors.length} nodes reachable` : "No nodes reachable"}</div>
        <button className="theme-button" onClick={() => setTheme((t) => t === "dark" ? "light" : "dark")}>
          {theme === "dark" ? <Sun size={16} /> : <Moon size={16} />}
          {theme === "dark" ? "Light appearance" : "Dark appearance"}
        </button>
        <div className="operator-profile"><span className="operator-avatar" aria-hidden="true">{user.name.charAt(0).toUpperCase()}</span><div><strong>{user.name}</strong><span>{user.email}</span></div><button className="icon-button" onClick={onSignOut} aria-label="Sign out" title="Sign out"><LogOut size={16} /></button></div>
      </div>
    </aside>
    </div>
    <div className="main-column">
      <header className="topbar">
        <div className="breadcrumb">Workspace <ChevronRight size={13} /><strong>
            {activePane.label}
          </strong></div>
        <div className="topbar-actions">
          <span className={`live-state ${paused ? "paused" : ""}`}>
            <span className={`status-dot ${paused ? "warn" : summary.online.length ? "good" : "bad"}`} />
            {paused ? "Polling paused" : cluster.refreshing ? "Refreshing…" : summary.online.length ? "Live observations" : "Nodes unavailable"}
          </span>
          <button
            className="icon-button"
            onClick={() => setPaused((p) => !p)}
            aria-label={paused ? "Resume polling" : "Pause polling"}
            title={paused ? "Resume polling" : "Pause polling"}>
            {paused ? <Play size={16} /> : <Pause size={16} />}
          </button>
          <button className="icon-button mobile-search" onClick={() => setCommandOpen(true)} aria-label="Find a tool"><Search size={17} /></button>
          <button className="icon-button" onClick={refresh} aria-label="Refresh all nodes" title="Refresh all nodes">
            <RefreshCw size={16} className={cluster.refreshing ? "refreshing" : ""} />
          </button>
          <button className="button export-button" onClick={exportData} aria-label="Export diagnostics"><ArrowDownToLine size={15} /><span>Export diagnostics</span></button>
        </div>
      </header>
      <main id="content" tabIndex={-1}>
        <div className="page-heading">
          <div>
            <h1 ref={heading} tabIndex={-1}>
              {activePane.label}
            </h1>
            {pane !== "home" && <p>
              {activePane.description}
            </p>}
          </div>
          <span className="sample-label">Last cycle <strong>
              {time(cluster.lastRefresh)}
            </strong><small>{monitors.length} endpoints · {interval / 1e3}s interval</small></span>
        </div>
        {alerts.length > 0 && <div className="alerts" role="status">
          {alerts.map((alert) => <p key={alert}>
            <TriangleAlert size={16} />
            <span>
              {alert}
            </span>
          </p>)}
          {!summary.online.length && pane !== "settings" && <button className="text-button" onClick={() => setPane("settings")}>Check connections<ArrowRight size={14} /></button>}
        </div>}
        {notice && <div className="notice" role="status">
          <Check size={16} />
          <span>
            {notice}
          </span>
          <button aria-label="Dismiss notification" className="icon-button" onClick={() => setNotice(null)}>
            <X size={15} />
          </button>
        </div>}

        <div className="pane-content" key={pane}>
        {pane === "home" && <Home user={user} summary={summary} observations={cluster.observations} monitors={monitors} events={cluster.events} lastRefresh={cluster.lastRefresh} refreshing={cluster.refreshing} onNavigate={setPane} onSelectNode={setSelectedId} />}
        {pane === "overview" && <>
          <div className="overview-strip">
            <div className="cluster-title">
              <div>
                <h2>
                  {summary.leaders.length === 1 ? `Node ${summary.leaders[0].id} leads the cluster` : summary.leaders.length > 1 ? "Leadership needs inspection" : "Waiting for an observed leader"}
                </h2>
                <p>
                  {reference ? `Term ${reference.term} · ${summary.voters.length} configured voters · ${reference.membership?.joint ? "joint membership" : "stable membership"}` : "Node APIs provide independently sampled runtime state."}
                </p>
              </div>
            </div>
            <Pill tone={summary.leaders.length === 1 ? "good" : "warn"}>
              {summary.leaders.length === 1 ? "Leader observed" : "Election / connection unknown"}
            </Pill>
          </div>
          <div className="metrics-row">
            <Metric label="Reachable node APIs" value={`${summary.online.length} / ${monitors.length}`} detail="HTTP observation, not Raft liveness" />
            <Metric
              label="Voting majority"
              value={reference?.membership?.joint ? "Both sets" : summary.quorum ?? "—"}
              detail={reference?.membership?.joint ? "Joint state: inspect both voter sets" : `⌊${summary.voters.length} / 2⌋ + 1 required`} />
            <Metric label="Commit index" value={number(reference?.commit_index)} detail={`${number(reference?.log?.uncommitted_entries)} uncommitted entries`} />
            <Metric label="Applied index" value={number(reference?.applied_index)} detail={`${number(reference?.log?.unapplied_entries)} awaiting application`} />
            <Metric label="Stored keys" value={number(reference?.keys)} detail={`Local state of node ${reference?.node_id ?? "—"}`} />
          </div>
          <Panel
            title="Cluster topology"
            subtitle="Select a node to inspect its runtime. Peer API ports default to 8080 + node ID − 1."
            action={<button className="text-button" onClick={() => setPane("settings")}><Plus size={14} />Connect node</button>}>
            <Topology observations={cluster.observations} selectedId={selected?.id} onSelect={setSelectedId} />
          </Panel>
          <div className="two-columns">
            <Panel
              title={`Consensus progress · node ${selected?.id ?? "—"}`}
              subtitle={selected?.online ? "Runtime values sampled directly from this node." : "Waiting for a fresh node sample."}
              action={<Pill tone={selected?.online ? "good" : "bad"}>
                {selected?.online ? "Reachable" : "Unavailable"}
              </Pill>}>
              <Detail
                rows={[
		["Current term", selected?.online ? number(status?.term) : "—"],
		["Leader reported", selected?.online ? status?.leader_id == null ? "None" : `Node ${status.leader_id}` : "—"],
		["Last log index / term", selected?.online ? `${number(status?.last_log_index)} / ${number(status?.log?.last_term)}` : "—"],
		["Storage health", selected?.online ? status?.health?.state || "Unknown" : "—"],
		["Pending proposals / reads", selected?.online ? `${number(status?.queues?.pending_proposals)} / ${number(status?.queues?.pending_reads)}` : "—"],
		["Runtime uptime", selected?.online ? duration(status?.uptime_ms) : "—"]
	]} />
              <div className="chart-heading">
                <span>Commit progression</span>
                <Pill>{selectedHistory.length} samples</Pill>
              </div>
              <Sparkline history={selectedHistory} field="commit" label="Commit index" />
            </Panel>
            <Panel
              title="Observed activity"
              subtitle="Role changes, term changes, and operator requests in this browser session."
              action={<button className="text-button" onClick={() => setPane("telemetry")}>All events<ArrowRight size={14} /></button>}>
              <EventList events={cluster.events} compact />
            </Panel>
          </div>
          <div className="two-columns">
            <Panel title="State integrity" subtitle="Compare hashes only when applied indices match; nodes are sampled independently.">
              <Detail
                rows={[
		["Reference node", reference ? `Node ${reference.node_id}` : "—"],
		["Applied index", number(reference?.applied_index)],
		["Comparable node samples", number(summary.comparable)],
		["Hash comparison", summary.comparable < 2 ? "Waiting for comparable samples" : summary.hashMismatch ? "Mismatch observed; recheck" : "Sampled hashes agree"]
	]} />
              <div className="hash-display">
                <ShieldCheck size={17} />
                <code>
                  {reference?.state_hash || "No state hash observed"}
                </code>
              </div>
            </Panel>
            <Panel title="Runtime boundaries" subtitle="What this console can establish from the available HTTP surface.">
              <div className="facts">
                <div>
                  <Database size={17} />
                  <p>
                    <strong>Local diagnostic reads</strong>
                    <span>Key listing and inspection may lag the leader. Use RESP read-index for linearizable reads.</span>
                  </p>
                </div>
                <div>
                  <GitBranch size={17} />
                  <p>
                    <strong>Committed mutations</strong>
                    <span>SET and DELETE target the currently observed leader and wait for the runtime response.</span>
                  </p>
                </div>
                <div>
                  <CircleHelp size={17} />
                  <p>
                    <strong>Static initial membership</strong>
                    <span>Connect node APIs here. Bootstrap additional voters with the cluster launcher.</span>
                  </p>
                </div>
              </div>
            </Panel>
          </div>
        </>}

        {pane === "nodes" && <>
          <Panel
            title="Node observation matrix"
            subtitle="The reported role comes from each node’s own API; leader match is the reference leader’s view."
            action={<button className="text-button" onClick={() => setPane("settings")}><Plus size={14} />Manage connections</button>}>
            <NodeTable observations={cluster.observations} reference={reference} selectedId={selected?.id} onSelect={setSelectedId} />
          </Panel>
          <div className="node-selector">
            <label htmlFor="inspect-node">Inspect runtime</label>
            <select id="inspect-node" value={selected?.id || ""} onChange={(e) => setSelectedId(Number(e.target.value))}>
              {monitors.map((node) => <option key={node.id} value={node.id}>Node {node.id} · localhost:{node.port}</option>)}
            </select>
            {selected && <span className="muted">
              {selected.online ? `Observed ${relative(selected.time)}` : `Last successful observation: ${relative(selected.time)}`}
            </span>}
          </div>
          {status ? <>
            <div className="three-columns">
              <Panel title="Consensus state" subtitle={`Node ${selected.id}${selected.online ? "" : " · last known sample"}`}>
                <Detail
                  rows={[
		["Role", status.role],
		["Term", number(status.term)],
		["Leader ID", text(status.leader_id)],
		["Commit index", number(status.commit_index)],
		["Applied index", number(status.applied_index)],
		["Last log index", number(status.last_log_index)],
		["Pending proposals", number(status.queues?.pending_proposals)],
		["Pending reads", number(status.queues?.pending_reads)]
	]} />
              </Panel>
              <Panel title="Log & snapshots" subtitle="Persistent log boundaries and application backlog.">
                <Detail
                  rows={[
		["First retained index", number(status.log?.first_index)],
		["Last log term", number(status.log?.last_term)],
		["Snapshot index", number(status.log?.snapshot_index)],
		["Snapshot term", number(status.log?.snapshot_term)],
		["Uncommitted entries", number(status.log?.uncommitted_entries)],
		["Unapplied entries", number(status.log?.unapplied_entries)],
		["Snapshot threshold", `${number(status.config?.snapshot_entries_threshold)} entries`],
		["Stored keys", number(status.keys)]
	]} />
              </Panel>
              <Panel title="Runtime configuration" subtitle="Read-only configuration; changes require a node restart.">
                <Detail
                  rows={[
		["Election timeout", `${number(status.config?.election_timeout_ms)} ms`],
		["Heartbeat interval", `${number(status.config?.heartbeat_ms)} ms`],
		["Tick interval", `${number(status.config?.tick_ms)} ms`],
		["Pre-vote", status.config ? status.config.pre_vote ? "Enabled" : "Disabled" : "—"],
		["Append batch limit", number(status.config?.max_append_entries)],
		["Inflight limit", number(status.config?.max_inflight_msgs)],
		["Transport concurrency", number(status.config?.transport_concurrency_limit)],
		["Uptime", duration(status.uptime_ms)],
		["Membership", status.membership?.joint ? "Joint" : "Stable"]
	]} />
              </Panel>
            </div>
            <div className="two-columns">
              <Panel title="Peer replication progress" subtitle="Available from the selected leader. Recent activity is reported by the core, not an HTTP probe.">
                {status.nodes?.some((n) => n.replication) ? <div className="table-scroll">
                  <table>
                    <thead>
                      <tr>
                        <th>Peer</th>
                        <th>Match</th>
                        <th>Next</th>
                        <th>Lag</th>
                        <th>Inflight</th>
                        <th>State</th>
                        <th>Recent</th>
                      </tr>
                    </thead>
                    <tbody>
                      {status.nodes?.filter((n) => n.replication).map((node) => <tr key={node.id}>
                        <td>Node {node.id}</td>
                        <td>
                          {number(node.replication.match_index)}
                        </td>
                        <td>
                          {number(node.replication.next_index)}
                        </td>
                        <td>
                          {number(node.replication.lag)}
                        </td>
                        <td>
                          {number(node.replication.inflight)}
                        </td>
                        <td>
                          {node.replication.state}
                        </td>
                        <td>
                          {node.replication.recent_active ? "Yes" : "No"}
                        </td>
                      </tr>)}
                    </tbody>
                  </table>
                </div> : <div className="panel-empty">
                  <p>Node {selected.id} is a {status.role}. Per-peer match and next indices are tracked only by the leader.</p>
                  {summary.leaders.length === 1 && <button className="button" onClick={() => setSelectedId(summary.leaders[0].id)}><Network size={15} />Inspect node {summary.leaders[0].id}</button>}
                </div>}
              </Panel>
              <Panel title="Membership & endpoints" subtitle="Configured addresses describe intent; they do not prove connectivity.">
                <Detail
                  rows={[
		["Voters", status.membership?.voters?.join(", ") || "—"],
		["Old voters (joint)", status.membership?.old_voters?.join(", ") || "—"],
		["New voters (joint)", status.membership?.new_voters?.join(", ") || "—"],
		["Raft gRPC", status.endpoints?.raft || "—"],
		["Client RESP", status.endpoints?.client || "—"],
		["Prometheus", status.endpoints?.metrics || "—"],
		["Node API", status.endpoints?.api || "—"]
	]} />
              </Panel>
            </div>
            <LogInspector monitor={selected} />
            <Panel
              title="Deterministic state fingerprint"
              subtitle="The rolling command-history hash is a diagnostic aid, not a checksum of the current keyspace.">
              <div className="hash-display">
                <ShieldCheck size={18} />
                <code>
                  {status.state_hash}
                </code>
                <CopyButton value={status.state_hash} label="Copy hash" />
              </div>
            </Panel>
          </> : <Empty title="No runtime sample yet">Connect the node API and refresh to inspect consensus, storage, and replication details.</Empty>}
        </>}

        {pane === "keys" && <Keyspace
          observations={cluster.observations}
          leaders={summary.leaders}
          selectedId={selected?.id}
          onSelect={setSelectedId}
          refresh={refresh}
          appendEvent={cluster.appendEvent} />}

        {pane === "telemetry" && <>
          <div className="node-selector">
            <label htmlFor="telemetry-node">Telemetry source</label>
            <select id="telemetry-node" value={selected?.id || ""} onChange={(e) => setSelectedId(Number(e.target.value))}>
              {monitors.map((n) => <option key={n.id} value={n.id}>Node {n.id}</option>)}
            </select>
            <span className="muted">Up to 120 samples per node. History resets when this page reloads.</span>
          </div>
          <div className="metrics-row four">
            <Metric
              label="Proposal commit rate"
              value={counterRate(selectedHistory, "proposals_committed") == null ? "—" : `${counterRate(selectedHistory, "proposals_committed").toFixed(1)}/s`}
              detail="Delta between latest runtime counters" />
            <Metric
              label="Raft send rate"
              value={counterRate(selectedHistory, "raft_messages_sent") == null ? "—" : `${counterRate(selectedHistory, "raft_messages_sent").toFixed(1)}/s`}
              detail="Local outbound messages per second" />
            <Metric label="Status API round trip" value={selected?.online ? `${selected.latency} ms` : "—"} detail="Browser → Vite → node; not Raft RTT" />
            <Metric
              label="Session samples"
              value={selectedHistory.length}
              detail={`Window ${duration(selectedHistory.length ? selectedHistory.at(-1).time - selectedHistory[0].time : 0)}`} />
          </div>
          <div className="two-columns">
            <Panel title="Commit / applied progression" subtitle="Each line shows the selected node’s independently sampled indices.">
              <div className="legend">
                <span><i />Commit</span>
                <span className="secondary"><i />Applied</span>
              </div>
              <Sparkline history={selectedHistory} field="commit" label="Commit index" />
              <Sparkline history={selectedHistory} field="applied" label="Applied index" color="var(--violet)" />
            </Panel>
            <Panel title="Status API latency" subtitle="Observed request round trip in milliseconds; no synthetic points.">
              <Sparkline history={selectedHistory} field="latency" label="Status API round trip" />
              <div className="chart-heading">
                <span>Pending proposals</span>
              </div>
              <Sparkline history={selectedHistory} field="pending" label="Pending proposals" color="var(--amber)" />
            </Panel>
          </div>
          <div className="two-columns">
            <Panel
              title="Runtime counters"
              subtitle={`Node ${selected?.id ?? "—"} · cumulative since process start${selected?.online ? "" : " · last known sample"}`}>
              <div className="table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>Counter</th>
                      <th>Total</th>
                      <th>Latest rate</th>
                    </tr>
                  </thead>
                  <tbody>
                    {COUNTERS.map(([field, label]) => <tr key={field}>
                      <td>
                        {label}
                        <small className="mono">
                          {field}
                        </small>
                      </td>
                      <td className="mono">
                        {number(status?.counters?.[field])}
                      </td>
                      <td className="mono">
                        {counterRate(selectedHistory, field) == null ? "—" : `${counterRate(selectedHistory, field).toFixed(2)}/s`}
                      </td>
                    </tr>)}
                  </tbody>
                </table>
              </div>
            </Panel>
            <Panel
              title="Event history"
              subtitle="Last 100 browser-observed transitions and console operations. This is not a server audit log."
              action={<button className="small" onClick={cluster.clearEvents}>Clear events</button>}>
              <EventList events={cluster.events} />
            </Panel>
          </div>
          <Panel title="Prometheus integration" subtitle="Use the metrics endpoint and bundled Grafana configuration for persistent monitoring.">
            <CodeBlock value={`curl http://${status?.endpoints?.metrics || "127.0.0.1:9101"}/metrics`} />
            <div className="inline-notes">
              <span><FileJson size={16} />grafana/dashboards/raftkv.json</span>
              <span><TriangleAlert size={16} />grafana/alerting/alerts.yml</span>
            </div>
            <p className="muted inset">Process counters reset on restart. Rates are omitted across detected resets. Metrics do not include end-to-end client latency percentiles.</p>
          </Panel>
        </>}

        {pane === "report" && <Architecture exportData={exportData} />}

        {pane === "settings" && <Connections
          monitors={monitors}
          setMonitors={setMonitors}
          observations={cluster.observations}
          interval={interval}
          setIntervalMs={setIntervalMs}
          refresh={refresh}
          setNotice={setNotice} />}
        </div>

        <footer>
          <span><GitBranch size={14} />RaftKV 0.1.0 · Rust workspace · RESP2 command subset</span>
          <span>Local profile · Independent node samples · Unauthenticated node APIs</span>
        </footer>
      </main>
    </div>
    <CommandMenu open={commandOpen} onClose={() => setCommandOpen(false)} panes={PANES} onNavigate={setPane} />
  </div>;
}
function Workspace() {
  const [user, setUser] = useState(loadSession);
  const [theme, setTheme] = useState(() => {
    try { return localStorage.getItem("raftkv-theme") === "light" ? "light" : "dark"; }
    catch { return "dark"; }
  });
  useEffect(() => { if (!user) document.title = "Welcome · RaftKV"; }, [user]);
  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    safeStore("raftkv-theme", theme);
    document.querySelector('meta[name="theme-color"]')?.setAttribute("content", getComputedStyle(document.documentElement).getPropertyValue("--bg").trim());
  }, [theme]);
  if (!user) return <Welcome theme={theme} onToggleTheme={() => setTheme(value => value === "dark" ? "light" : "dark")} onContinue={profile => { const saved = saveSession(profile); if (saved) setUser(saved); }} />;
  return <App user={user} theme={theme} setTheme={setTheme} onSignOut={() => { clearSession(); setUser(null); }} />;
}
createRoot(document.getElementById("root")).render(<Workspace />);
