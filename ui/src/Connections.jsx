import { useState } from "react";
import { Plus, X } from "lucide-react";
import { validateMonitor } from "./model.js";

import { Panel, Metric, Detail, CodeBlock } from "./components.jsx";
function Connections({ monitors, setMonitors, observations, interval, setIntervalMs, refresh, setNotice }) {
	const [id, setId] = useState("4");
	const [port, setPort] = useState("8083");
	const [error, setError] = useState("");
	const [count, setCount] = useState(5);
	const add = (e) => {
		e.preventDefault();
		const problem = validateMonitor(id, port);
		if (problem) {
			setError(problem);
			return;
		}
		if (monitors.length >= 64 && !monitors.some((n) => n.id === Number(id))) {
			setError("The browser supports up to 64 monitored node APIs.");
			return;
		}
		if (monitors.some((n) => n.port === Number(port) && n.id !== Number(id))) {
			setError("This API port is already assigned to another node.");
			return;
		}
		setMonitors((old) => [...old.filter((n) => n.id !== Number(id)), {
			id: Number(id),
			port: Number(port)
		}].sort((a, b) => a.id - b.id));
		setError("");
		refresh();
		setNotice(`Monitoring node ${id} through localhost:${port}. This changes console connections only.`);
	};
	const command = `./scripts/run_cluster.sh --nodes ${count} --run-dir .run-${count}`;
	return <>
    <div className="two-columns">
      <Panel title="Connect a local node API" subtitle="Connect an existing process. This does not create a process or change Raft voting membership.">
        <form className="connection-form" onSubmit={add}>
          <label>Node ID<input
              type="number"
              min="1"
              max="65535"
              value={id}
              onChange={(e) => {
		setId(e.target.value);
		if (Number(e.target.value) + 8079 <= 65535) setPort(String(Number(e.target.value) + 8079));
	}}
              required /></label>
          <label>Local API port<input type="number" min="1024" max="65535" value={port} onChange={(e) => setPort(e.target.value)} required /></label>
          <button type="submit" className="primary"><Plus size={15} />Connect / update</button>
        </form>
        {error && <p className="inline-error" role="status">
          {error}
        </p>}
        <p className="muted inset">The Vite gateway connects only to 127.0.0.1. The reported node ID must match this connection. Conventional peer API ports are discovered automatically; override them here when using custom configs.</p>
        <div className="connection-list">
          {monitors.map((n) => {
		const observed = observations.find((o) => o.id === n.id);
		return <div key={n.id}>
            <span className={`status-dot ${observed?.online ? "good" : "bad"}`} />
            <strong>Node {n.id}</strong>
            <code>127.0.0.1:{n.port}</code>
            <button className="small" onClick={() => {
			setId(String(n.id));
			setPort(String(n.port));
		}}>Edit</button>
            <button
              className="icon-button danger"
              aria-label={`Remove node ${n.id} monitor`}
              disabled={monitors.length === 1}
              onClick={() => {
			setMonitors((old) => old.filter((node) => node.id !== n.id));
			setNotice("Connection removed. Configured peers may be rediscovered by another node; Raft membership is unchanged.");
		}}>
              <X size={15} />
            </button>
          </div>;
	})}
        </div>
      </Panel>
      <Panel title="Observation settings" subtitle="Settings apply to this browser session. Node connections persist locally.">
        <label className="setting-field">Refresh interval<select value={interval} onChange={(e) => setIntervalMs(Number(e.target.value))}>
            {[
		1e3,
		2500,
		5e3,
		1e4
	].map((value) => <option value={value} key={value}>{value / 1e3} seconds</option>)}
          </select></label>
        <Detail
          rows={[
		["Maximum node monitors", "64"],
		["Node request deadline", "4.5 seconds"],
		["Request scheduling", "Next cycle after prior cycle completes"],
		["History retention", "120 samples / node"],
		["Event retention", "100 browser events"],
		["Key value exports", "Excluded from diagnostics"],
		["HTTP read consistency", "Local"],
		["Vite gateway bind", "127.0.0.1"]
	]} />
        <p className="muted inset">API observations are asynchronous. A reachable HTTP endpoint does not prove that Raft messages can reach peers or that a voting majority is available.</p>
      </Panel>
    </div>
    <Panel
      title="Bootstrap a larger fresh cluster"
      subtitle="Run these commands in a terminal from the repository root. This creates a new initial configuration, not an online membership change.">
      <div className="bootstrap-plan">
        <label>Initial voter count<select value={count} onChange={(e) => setCount(Number(e.target.value))}>
            {[
		1,
		3,
		5,
		7,
		9
	].map((n) => <option key={n} value={n}>{n} node{n > 1 ? "s" : ""}</option>)}
          </select></label>
        <Metric label="Voting majority" value={Math.floor(count / 2) + 1} detail="Stable membership commit threshold" />
        <Metric label="Unavailable voters tolerated" value={Math.floor((count - 1) / 2)} detail="With a communicating majority remaining" />
      </div>
      <ol className="runbook">
        <li>
          <strong>Stop the existing default cluster to free its ports.</strong>
          <CodeBlock value="./scripts/stop_cluster.sh" />
          <p>For a custom run directory, stop that directory’s cluster. Different data directories still share the default port ranges.</p>
        </li>
        <li>
          <strong>Generate and start a fresh initial configuration.</strong>
          <CodeBlock value={command} />
          <p>Use an unused run directory. The launcher refuses to rewrite existing persisted membership. An odd voter count avoids adding a voter without increasing failure tolerance.</p>
        </li>
        <li>
          <strong>Connect the seed API and inspect every voter.</strong>
          <p>Node 1 defaults to HTTP 8080. The console discovers configured IDs and probes HTTP 8080 + ID − 1. Check that one leader is observed and inspect all applied indices before sending test writes.</p>
        </li>
        <li>
          <strong>Exercise a write, then inspect replication.</strong>
          <p>Use Keyspace explorer to commit a SET. Compare each node’s local applied index and hash at equal indices. API reachability and hash agreement are diagnostic observations, not a correctness proof.</p>
        </li>
      </ol>
      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              <th>Nodes</th>
              <th>Majority</th>
              <th>Failure tolerance</th>
              <th>Trade-off</th>
            </tr>
          </thead>
          <tbody>
            {[
		1,
		3,
		5,
		7,
		9
	].map((n) => <tr key={n}>
              <td>
                {n}
              </td>
              <td>
                {Math.floor(n / 2) + 1}
              </td>
              <td>
                {Math.floor((n - 1) / 2)}
              </td>
              <td>
                {n === 1 ? "Single-process smoke tests; no redundancy" : `${n - 1} follower replication streams; more network and persistence work`}
              </td>
            </tr>)}
          </tbody>
        </table>
      </div>
    </Panel>
    <div className="two-columns">
      <Panel title="Default endpoint plan" subtitle="Node IDs are generated consecutively by the local launcher.">
        <Detail
          rows={[
		["Raft gRPC", "7000 + node ID"],
		["Client RESP", "6378 + node ID"],
		["Prometheus HTTP", "9100 + node ID"],
		["Diagnostic HTTP API", "8079 + node ID"],
		["Node log", ".run-N/node<ID>.log"],
		["Persistent state", ".run-N/data/node<ID>/"]
	]} />
        <p className="muted inset">Custom configurations may use other addresses. The HTTP API bind must be reachable through the local gateway.</p>
      </Panel>
      <Panel title="Troubleshooting sequence" subtitle="Inspect durable state and communication before changing configuration.">
        <div className="technical-list">
          <h3>No leader observed</h3>
          <p>Verify unique node IDs, symmetric peer configuration, reachable gRPC addresses, and node logs. A working status API does not imply working peer transport.</p>
          <h3>Writes rejected or waiting</h3>
          <p>Inspect proposal queues, append rejections, storage errors, leader match indices, and majority connectivity. Refresh leadership before retrying.</p>
          <h3>Hashes differ</h3>
          <p>Compare only equal applied indices. Recheck independent samples and inspect logs; application and concurrent writes may move between observations.</p>
          <h3>Need to add live voters?</h3>
          <p>The core supports joint consensus, but an administration workflow is not exposed here. Prepare a fresh cluster for experiments instead of rewriting persisted peer lists.</p>
        </div>
      </Panel>
    </div>
  </>;
}
export default Connections;
