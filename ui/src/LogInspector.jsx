import { useEffect, useState } from "react";
import { RefreshCw } from "lucide-react";
import { Panel, Pill, time } from "./components.jsx";
import { request } from "./useCluster.js";
import { number, formatBytes } from "./model.js";
export default function LogInspector({ monitor }) {
	const [log, setLog] = useState(null);
	const [error, setError] = useState("");
	const [revision, setRevision] = useState(0);
	const [loading, setLoading] = useState(false);
	const port = monitor?.port;
	const online = monitor?.online;
	useEffect(() => {
		setLog(null);
		setError("");
		if (!online || !port) return;
		const controller = new AbortController();
		let active = true;
		setLoading(true);
		const timeout = setTimeout(() => controller.abort(), 6e3);
		request(`/nodes/${port}/api/log`, { signal: controller.signal }).then((data) => {
			if (active) setLog({
				...data,
				time: Date.now()
			});
		}).catch((e) => {
			if (active) setError(e.name === "AbortError" ? "Log request timed out." : e.message);
		}).finally(() => {
			clearTimeout(timeout);
			if (active) setLoading(false);
		});
		return () => {
			active = false;
			controller.abort();
			clearTimeout(timeout);
		};
	}, [
		port,
		online,
		revision
	]);
	return <Panel
    title="Replicated log inspector"
    subtitle={`Local metadata only; last 100 retained entries. Command values are excluded.${log ? ` Sampled ${time(log.time)}.` : ""}`}
    action={<button className="small" onClick={() => setRevision((n) => n + 1)} disabled={!online || loading}><RefreshCw size={14} />Refresh log</button>}>

    {error && <p className="inline-error" role="status">
      {error}
    </p>}

    <div className="table-scroll">
      <table>
        <thead>
          <tr>
            <th>Index</th>
            <th>Term</th>
            <th>Entry kind</th>
            <th>Payload</th>
            <th>Committed</th>
            <th>Applied</th>
          </tr>
        </thead>
        <tbody>
          {log?.entries?.map((entry) => <tr key={entry.index}>
            <td className="num">
              {number(entry.index)}
            </td>
            <td className="num">
              {number(entry.term)}
            </td>
            <td>
              <Pill>
                {entry.kind}
              </Pill>
            </td>
            <td>
              {formatBytes(entry.payload_bytes)}
            </td>
            <td>
              {entry.committed ? "Yes" : "No"}
            </td>
            <td>
              {entry.applied ? "Yes" : "No"}
            </td>
          </tr>)}
        </tbody>
      </table>
    </div>

    {!log?.entries?.length && <p className="muted inset">
      {loading ? "Reading retained log metadata…" : "No log entries available from this sample."}
    </p>}

  </Panel>;
}
