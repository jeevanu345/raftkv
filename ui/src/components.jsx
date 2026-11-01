import { useState } from "react";
import { Check, ChevronRight, Copy, Crown, Radio, Server } from "lucide-react";
import { number } from "./model.js";

const time = (value) => value ? new Date(value).toLocaleTimeString() : "—";
const relative = (value) => value ? `${Math.max(0, Math.floor((Date.now() - value) / 1e3))}s ago` : "Never";
const text = (value) => value == null ? "—" : String(value);
function safeStore(key, value) {
	try {
		localStorage.setItem(key, value);
	} catch {}
}
function download(filename, content, type = "application/json") {
	const url = URL.createObjectURL(new Blob([content], { type }));
	const link = document.createElement("a");
	link.href = url;
	link.download = filename;
	link.click();
	setTimeout(() => URL.revokeObjectURL(url), 1e3);
}
function Pill({ children, tone = "" }) {
	return <span className={`pill ${tone}`}>
    {children}
  </span>;
}
function Panel({ title, subtitle, action, children, className = "" }) {
	return <section className={`panel ${className}`}>
    <div className="panel-heading">
      <div>
        <h2>
          {title}
        </h2>
        {subtitle && <p>
          {subtitle}
        </p>}
      </div>
      {action}
    </div>
    {children}
  </section>;
}
function Metric({ label, value, detail, tone = "" }) {
	return <div className={`metric ${tone}`}>
    <span>
      {label}
    </span>
    <strong>
      {value}
    </strong>
    <small>
      {detail}
    </small>
  </div>;
}
function Detail({ rows }) {
	return <dl className="detail-list">
    {rows.map(([label, value]) => <div key={label}>
      <dt>
        {label}
      </dt>
      <dd>
        {value}
      </dd>
    </div>)}
  </dl>;
}
function Empty({ title, children }) {
	return <div className="empty">
    <Radio size={24} />
    <strong>
      {title}
    </strong>
    <p>
      {children}
    </p>
  </div>;
}
function CopyButton({ value, label = "Copy" }) {
	const [copied, setCopied] = useState(false);
	return <button
    className="small"
    onClick={async () => {
		try {
			await navigator.clipboard.writeText(value);
			setCopied(true);
			setTimeout(() => setCopied(false), 1800);
		} catch {
			setCopied(false);
		}
	}}
    aria-label={label}>
    {copied ? <Check size={14} /> : <Copy size={14} />}
    {copied ? "Copied" : label}
  </button>;
}
function Sparkline({ history, field, label, color = "var(--accent)" }) {
	const samples = history.filter((n) => Number.isFinite(n[field]));
	if (samples.length < 2) return <div className="chart-empty">Collecting {label.toLowerCase()} samples…</div>;
	const values = samples.map((n) => n[field]);
	const min = Math.min(...values);
	const max = Math.max(...values);
	const start = samples[0].time;
	const elapsed = samples.at(-1).time - start || 1;
	const flat = max === min;
	const points = samples.map((n) => `${((n.time - start) / elapsed * 600).toFixed(1)},${(flat ? 42 : 72 - (n[field] - min) / (max - min) * 60).toFixed(1)}`).join(" ");
	return <div className="sparkline">
    <div className={`chart-scale ${flat ? "flat" : ""}`}>
      {!flat && <span>
        {number(max)}
      </span>}
      <span>
        {number(min)}
      </span>
    </div>
    <svg
      viewBox="0 0 600 84"
      role="img"
      aria-label={`${label}, ${values.length} observed samples, minimum ${min}, maximum ${max}`}
      preserveAspectRatio="none">
      <path d="M0 12H600 M0 72H600" className="chart-grid" />
      <polyline points={points} fill="none" stroke={color} strokeWidth="2" vectorEffect="non-scaling-stroke" />
    </svg>
    <div className="chart-times">
      <span>
        {time(start)}
      </span>
      <span>
        {time(samples.at(-1).time)}
      </span>
    </div>
  </div>;
}
function Topology({ observations, onSelect, selectedId }) {
	if (!observations.length) return <Empty title="Waiting for node APIs">Start a node, then connect its local API in Connections & runbook.</Empty>;
	return <div className="topology">
    {observations.map((node) => <button
      key={node.id}
      className={`topology-node ${node.online ? node.status.role : "unavailable"} ${selectedId === node.id ? "selected" : ""}`}
      onClick={() => onSelect(node.id)}
      aria-pressed={selectedId === node.id}>
      <span className="topology-node-head"><span className={`status-dot ${node.online ? "good" : "bad"}`} />Node {node.id}{node.online && node.status.role === "leader" ? <Crown size={16} /> : <Server size={16} />}</span>
      <strong>
        {node.online ? node.status.role : "Unavailable"}
      </strong>
      <span className="mono">:{node.port} <span>HTTP</span></span>
      <span className="topology-meta">Term {node.online ? node.status.term : "—"}<span>Commit {node.online ? number(node.status.commit_index) : "—"}</span></span>
    </button>)}
  </div>;
}
function NodeTable({ observations, reference, onSelect, selectedId }) {
	return <div className="table-scroll">
    <table>
      <caption className="sr-only">Observed node status and leader-reported replication</caption>
      <thead>
        <tr>
          <th>Node / endpoint</th>
          <th>Role / term</th>
          <th>Log / commit</th>
          <th>Applied / lag</th>
          <th>Leader match</th>
          <th>API round trip</th>
          <th>Observation</th>
        </tr>
      </thead>
      <tbody>
        {observations.map((node) => {
		const status = node.status;
		const replication = reference?.nodes?.find((n) => n.id === node.id)?.replication;
		return <tr key={node.id} className={selectedId === node.id ? "selected-row" : ""}>
          <td>
            <button className="text-button" onClick={() => onSelect(node.id)}>Node {node.id} <ChevronRight size={13} /></button>
            <small>127.0.0.1:{node.port}</small>
          </td>
          <td>
            <Pill tone={node.online ? status.role === "leader" ? "accent role" : "role" : "bad role"}>
              {node.online ? status.role : "unavailable"}
            </Pill>
            <small>Term {node.online ? status.term : "—"}</small>
          </td>
          <td className="num">{node.online ? number(status.last_log_index) : "—"} / {node.online ? number(status.commit_index) : "—"}</td>
          <td className="num">
            {node.online ? number(status.applied_index) : "—"}
            <small>
              {node.online ? `${Math.max(0, status.commit_index - status.applied_index)} unapplied` : "No fresh sample"}
            </small>
          </td>
          <td className="num">
            {replication ? number(replication.match_index) : "—"}
            <small>
              {replication ? `${number(replication.lag)} entries behind` : node.id === reference?.node_id && reference?.role === "leader" ? "Leader local log" : "No leader progress"}
            </small>
          </td>
          <td className="num">
            {node.online ? `${node.latency} ms` : "—"}
          </td>
          <td>
            <span className={`status-dot ${node.online ? "good" : "bad"}`} />
            {node.online ? "Observed" : "Unreachable"}
            <small title={node.error || ""}>
              {node.online ? time(node.time) : node.error}
            </small>
          </td>
        </tr>;
	})}
      </tbody>
    </table>
  </div>;
}
function EventList({ events, compact = false }) {
	const [filter, setFilter] = useState("all");
	const visible = (filter === "all" ? events : events.filter((e) => e.kind === filter)).slice(0, compact ? 5 : 100);
	return <>
    {!compact && <div className="segmented" aria-label="Filter event history">
      {[
		"all",
		"consensus",
		"operation",
		"warning",
		"recovery"
	].map((kind) => <button key={kind} className={filter === kind ? "active" : ""} onClick={() => setFilter(kind)}>
        {kind}
      </button>)}
    </div>}
    <div className="events">
      {visible.map((event, index) => <div className={`event ${event.kind}`} key={`${event.time}-${event.title}-${index}`}>
        <span className="event-dot" />
        <div>
          <strong>
            {event.title}
          </strong>
          <p>
            {event.detail}
          </p>
        </div>
        <time dateTime={new Date(event.time).toISOString()}>
          {time(event.time)}
        </time>
      </div>)}
      {!visible.length && <p className="muted inset">No {filter === "all" ? "" : `${filter} `}events observed. Node transitions appear here as polling continues.</p>}
    </div>
  </>;
}
function CodeBlock({ value }) {
	return <div className="code-block">
    <pre>
      <code>
        {value}
      </code>
    </pre>
    <CopyButton value={value} label="Copy command" />
  </div>;
}
export { time, relative, text, safeStore, download, Pill, Panel, Metric, Detail, Empty, CopyButton, Sparkline, Topology, NodeTable, EventList, CodeBlock };
