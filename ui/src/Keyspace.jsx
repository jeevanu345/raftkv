import { useEffect, useMemo, useRef, useState } from "react";
import { ArrowRight, KeyRound, RefreshCw, Search, Trash2 } from "lucide-react";
import { decodeKey, formatBytes, number } from "./model.js";
import { endpoint, request } from "./useCluster.js";
import { time, text, Pill, Panel, Detail, Empty, CopyButton } from "./components.jsx";
function Keyspace({ observations, leaders, selectedId, onSelect, refresh, appendEvent }) {
	const monitor = observations.find((n) => n.id === selectedId);
	const leader = leaders.length === 1 ? leaders[0] : null;
	const [keys, setKeys] = useState([]);
	const [meta, setMeta] = useState(null);
	const [error, setError] = useState("");
	const [loading, setLoading] = useState(false);
	const [revision, setRevision] = useState(0);
	const [query, setQuery] = useState("");
	const [page, setPage] = useState(0);
	const [key, setKey] = useState("");
	const [value, setValue] = useState("");
	const [inspected, setInspected] = useState(null);
	const [busy, setBusy] = useState(false);
	const [operation, setOperation] = useState("");
	const [confirmDelete, setConfirmDelete] = useState(false);
	const generation = useRef(0);
	const currentPort = monitor?.port;
	const online = monitor?.online;
	useEffect(() => {
		const token = ++generation.current;
		setInspected(null);
		setConfirmDelete(false);
		setKeys([]);
		setMeta(null);
		setPage(0);
		setError("");
		if (!currentPort || !online) {
			setError("Selected node API is unavailable. Choose a reachable node.");
			return;
		}
		const controller = new AbortController();
		const timeout = setTimeout(() => controller.abort(), 6e3);
		setLoading(true);
		request(`/nodes/${currentPort}/api/keys`, { signal: controller.signal }).then((data) => {
			if (generation.current !== token) return;
			setKeys(data.keys.map(decodeKey));
			setMeta({
				...data,
				keys: undefined,
				time: Date.now()
			});
		}).catch((e) => {
			if (generation.current === token && !controller.signal.aborted) setError(e.message);
			else if (generation.current === token) setError("Key listing timed out. Retry when the node is responsive.");
		}).finally(() => {
			clearTimeout(timeout);
			if (generation.current === token) setLoading(false);
		});
		return () => {
			generation.current++;
			controller.abort();
			clearTimeout(timeout);
		};
	}, [
		currentPort,
		online,
		revision
	]);
	const filtered = useMemo(() => keys.filter((item) => item.text.toLowerCase().includes(query.toLowerCase())), [keys, query]);
	const pages = Math.max(1, Math.ceil(filtered.length / 40));
	const visible = filtered.slice(Math.min(page, pages - 1) * 40, (Math.min(page, pages - 1) + 1) * 40);
	const inspect = async (item) => {
		const token = generation.current;
		setBusy(true);
		setOperation("");
		setConfirmDelete(false);
		try {
			const data = await request(`${endpoint(monitor)}/keys/${encodeURIComponent(item.text)}`);
			if (generation.current === token) setInspected({
				key: item.text,
				...data
			});
		} catch (e) {
			if (generation.current === token) setOperation(e.message);
		} finally {
			setBusy(false);
		}
	};
	const mutate = async (method, targetKey, targetValue) => {
		if (!leader || busy) return;
		setBusy(true);
		setOperation("");
		try {
			const result = await request(`${endpoint(leader)}/keys/${encodeURIComponent(targetKey)}`, method === "PUT" ? {
				method,
				headers: { "content-type": "application/json" },
				body: JSON.stringify({ value: targetValue })
			} : { method });
			const message = method === "PUT" ? `SET committed through node ${leader.id}: ${targetKey}` : `DELETE committed through node ${leader.id}: ${targetKey}${result.removed === false ? " (key did not exist)" : ""}`;
			setOperation(message);
			appendEvent({
				title: method === "PUT" ? "SET request committed" : "DELETE request committed",
				detail: `Node ${leader.id} · key ${targetKey}`,
				node: leader.id
			});
			setConfirmDelete(false);
			setInspected(null);
			setRevision((n) => n + 1);
			refresh();
		} catch (e) {
			setOperation(e.message);
		} finally {
			setBusy(false);
		}
	};
	return <>
    <div className="node-selector">
      <label htmlFor="key-source">Read from node</label>
      <select id="key-source" value={selectedId || ""} onChange={(e) => onSelect(Number(e.target.value))}>
        {observations.map((n) => <option value={n.id} key={n.id}>Node {n.id}{n.online ? "" : " · unavailable"}</option>)}
      </select>
      <Pill tone="warn">Local consistency</Pill>
      <span className="muted">Writes: {leader ? `node ${leader.id} (observed leader)` : "disabled until one leader is observed"}</span>
    </div>
    <div className="keyspace-grid">
      <Panel
        title="Key inventory"
        subtitle={meta ? `Node ${meta.node_id ?? selectedId} · applied index ${number(meta.applied_index)} · listed ${time(meta.time)}` : "Local state is diagnostic and may lag committed cluster state."}
        action={<button className="small" disabled={loading || !online} onClick={() => setRevision((n) => n + 1)}><RefreshCw size={14} />Refresh keys</button>}>
        <label className="search-field">
          <Search size={16} />
          <input
            aria-label="Filter keys"
            placeholder="Search this key inventory…"
            value={query}
            onChange={(e) => {
		setQuery(e.target.value);
		setPage(0);
	}} />
          <span>{filtered.length} / {keys.length}</span>
        </label>
        {error && <p className="inline-error" role="status">
          {error}
        </p>}
        {loading ? <Empty title="Reading local key inventory">The node’s state machine is being inspected.</Empty> : <div className="table-scroll key-table">
          <table>
            <thead>
              <tr>
                <th>Key</th>
                <th>Key bytes</th>
                <th>Encoding</th>
                <th>Inspect</th>
              </tr>
            </thead>
            <tbody>
              {visible.map((item) => <tr key={item.identity}>
                <td className="mono key-cell">
                  {item.text || "(empty key)"}
                </td>
                <td className="num">
                  {item.bytes}
                </td>
                <td>
                  {item.editable ? "UTF-8" : "Binary / hex"}
                </td>
                <td>
                  <button className="small" disabled={!item.editable || busy} onClick={() => inspect(item)}>View</button>
                </td>
              </tr>)}
            </tbody>
          </table>
          {!visible.length && !error && <Empty title={query ? "No matching keys" : "Keyspace is empty"}>
            {query ? "Change the search text to inspect other keys." : "Create a key with a replicated SET request."}
          </Empty>}
        </div>}
        <div className="pagination">
          <span>{filtered.length} matching keys · 40 per page</span>
          <button className="small" disabled={page <= 0} onClick={() => setPage((n) => n - 1)}>Previous</button>
          <span>{Math.min(page, pages - 1) + 1} / {pages}</span>
          <button className="small" disabled={page >= pages - 1} onClick={() => setPage((n) => n + 1)}>Next</button>
        </div>
        <p className="muted inset">Filtering and pagination happen in this browser after loading the full key list. Binary keys are shown as hex; the string-based HTTP API cannot inspect them losslessly.</p>
      </Panel>
      <div>
        <Panel title="Propose a write" subtitle="The leader appends the command, replicates it, then applies the committed entry.">
          <form className="write-form" onSubmit={(e) => {
		e.preventDefault();
		mutate("PUT", key, value);
	}}>
            <label>Key<input value={key} onChange={(e) => setKey(e.target.value)} placeholder="e.g. service:checkout:version" required /></label>
            <label>Value<textarea rows={5} value={value} onChange={(e) => setValue(e.target.value)} placeholder={"{\"version\":\"1.0\",\"region\":\"local\"}"} /></label>
            <div className="form-meta">
              <span>{formatBytes(new TextEncoder().encode(value).length)} UTF-8 value</span>
              <Pill>
                {leader ? `Leader ${leader.id}` : "No leader"}
              </Pill>
            </div>
            <button className="primary" type="submit" disabled={busy || !leader || !key}>
              {busy ? "Request in progress…" : "Commit SET"}
              <ArrowRight size={15} />
            </button>
          </form>
          {operation && <p className="operation-result" role="status">
            {operation}
          </p>}
          <p className="muted inset">A leadership change may reject a request. Refresh and retry after the cluster stabilizes. Writes require a healthy durable runtime.</p>
        </Panel>
        <Panel title="Value inspector" subtitle="Local diagnostic GET; values are represented as UTF-8 by the API.">
          {inspected ? <>
            <div className="inspector-key">
              <KeyRound size={16} />
              <strong>
                {inspected.key}
              </strong>
              <CopyButton value={inspected.key} />
            </div>
            <Detail
              rows={[
		["Source node", inspected.node_id ?? selectedId],
		["Read consistency", inspected.consistency || "local"],
		["Applied index", number(inspected.applied_index)],
		["Value bytes (UTF-8 rendering)", inspected.value == null ? "Missing key" : formatBytes(new TextEncoder().encode(inspected.value).length)]
	]} />
            <pre className="value-preview">
              {inspected.value === null ? "(key no longer exists)" : inspected.value}
            </pre>
            <div className="button-row">
              <button className="small" onClick={() => {
		setKey(inspected.key);
		setValue(inspected.value ?? "");
	}}>Edit value</button>
              <CopyButton value={inspected.value ?? ""} label="Copy value" />
              <button className="small danger" disabled={!leader || busy} onClick={() => setConfirmDelete(true)}><Trash2 size={14} />Delete</button>
            </div>
            {confirmDelete && <div className="delete-confirm">
              <p>Commit deletion of <strong>
                  {inspected.key}
                </strong> through node {leader?.id}?</p>
              <div className="button-row">
                <button className="danger small" disabled={busy || !leader} onClick={() => mutate("DELETE", inspected.key)}>Confirm DELETE</button>
                <button className="small" onClick={() => setConfirmDelete(false)}>Cancel</button>
              </div>
            </div>}
          </> : <Empty title="Select a key to inspect">View a key from the inventory to read its value and source applied index.</Empty>}
        </Panel>
      </div>
    </div>
  </>;
}
export default Keyspace;
