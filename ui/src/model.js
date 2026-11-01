export const HISTORY_LIMIT = 120;
export const EVENT_LIMIT = 100;
export const DEFAULT_MONITORS = [{
	id: 1,
	port: 8080
}];
export const number = (value) => value == null ? "—" : Number(value).toLocaleString();
export function formatBytes(value) {
	if (value == null) return "—";
	if (value < 1024) return `${value} B`;
	const unit = Math.min(Math.floor(Math.log(value) / Math.log(1024)), 3);
	return `${(value / 1024 ** unit).toFixed(1)} ${[
		"B",
		"KiB",
		"MiB",
		"GiB"
	][unit]}`;
}
export function duration(ms) {
	if (ms == null) return "—";
	const seconds = Math.floor(ms / 1e3);
	return seconds < 60 ? `${seconds}s` : seconds < 3600 ? `${Math.floor(seconds / 60)}m ${seconds % 60}s` : `${Math.floor(seconds / 3600)}h ${Math.floor(seconds % 3600 / 60)}m`;
}
export function validateMonitor(id, port) {
	if (!Number.isSafeInteger(Number(id)) || Number(id) < 1 || Number(id) > 65535) return "Node ID must be an integer between 1 and 65535.";
	if (!Number.isInteger(Number(port)) || Number(port) < 1024 || Number(port) > 65535) return "API port must be an integer between 1024 and 65535.";
	return null;
}
export function loadMonitors() {
	try {
		const saved = JSON.parse(localStorage.getItem("raftkv-monitors-v1"));
		if (Array.isArray(saved) && saved.length && saved.length <= 64 && saved.every((n) => !validateMonitor(n.id, n.port)) && new Set(saved.map((n) => Number(n.id))).size === saved.length) return saved.map((n) => ({
			id: Number(n.id),
			port: Number(n.port)
		}));
	} catch {}
	return DEFAULT_MONITORS;
}
export function decodeKey(bytes) {
	const raw = Uint8Array.from(bytes);
	let text;
	let editable = true;
	try {
		text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(raw);
	} catch {
		text = `0x${Array.from(raw, (b) => b.toString(16).padStart(2, "0")).join("")}`;
		editable = false;
	}
	return {
		text,
		bytes: raw.length,
		editable,
		identity: Array.from(raw).join(",")
	};
}
export function clusterSummary(observations) {
	const online = observations.filter((n) => n.online && n.status);
	const reportedLeaders = online.filter((n) => n.status.role === "leader");
	const leaders = reportedLeaders.filter((n) => n.status.health?.state !== "failed");
	const reference = leaders[0]?.status || online[0]?.status;
	const voters = reference?.membership?.voters || reference?.nodes?.filter((n) => n.voter).map((n) => n.id) || [];
	const quorum = reference?.membership?.joint ? null : reference?.membership?.quorum_size ?? (voters.length ? Math.floor(voters.length / 2) + 1 : null);
	const reachableVoters = online.filter((n) => voters.includes(n.status.node_id)).length;
	const terms = new Set(online.map((n) => n.status.term));
	const leadersByTerm = new Map();
	for (const node of reportedLeaders) leadersByTerm.set(node.status.term, (leadersByTerm.get(node.status.term) || 0) + 1);
	// Samples arrive independently. This is an observation warning, never proof of a Raft invariant violation.
	const conflictingLeaders = [...leadersByTerm.values()].some((count) => count > 1);
	const comparable = online.filter((n) => n.status.applied_index === reference?.applied_index);
	const hashMismatch = comparable.length > 1 && new Set(comparable.map((n) => n.status.state_hash)).size > 1;
	return {
		online,
		leaders,
		reference,
		voters,
		quorum,
		reachableVoters,
		terms,
		conflictingLeaders,
		hashMismatch,
		comparable: comparable.length
	};
}
export function counterRate(history, field) {
	if (history.length < 2) return null;
	const current = history.at(-1);
	const previous = history.at(-2);
	if (current.node_id !== previous.node_id || current.uptime_ms < previous.uptime_ms || current[field] == null || previous[field] == null || current[field] < previous[field]) return null;
	const elapsed = (current.time - previous.time) / 1e3;
	return elapsed > 0 ? (current[field] - previous[field]) / elapsed : null;
}
export function observationEvents(previous, next, time) {
	const events = [];
	for (const node of next) {
		const old = previous.find((n) => n.id === node.id);
		if (!old || old.online !== node.online) events.push({
			time,
			node: node.id,
			kind: node.online ? "recovery" : "warning",
			title: node.online ? `Node ${node.id} API reachable` : `Node ${node.id} API unavailable`,
			detail: node.online ? `Observed through localhost:${node.port}` : node.error
		});
		if (node.online && old?.online && old.status) {
			if (old.status.role !== node.status.role) events.push({
				time,
				node: node.id,
				kind: "consensus",
				title: `Node ${node.id}: ${old.status.role} → ${node.status.role}`,
				detail: `Observed at term ${node.status.term}`
			});
			if (old.status.term !== node.status.term) events.push({
				time,
				node: node.id,
				kind: "consensus",
				title: `Node ${node.id} term ${old.status.term} → ${node.status.term}`,
				detail: `Leader reported: ${node.status.leader_id ?? "none"}`
			});
			if (node.status.uptime_ms < old.status.uptime_ms) events.push({
				time,
				node: node.id,
				kind: "warning",
				title: `Node ${node.id} runtime restarted`,
				detail: "Uptime decreased; runtime counters may have reset."
			});
		}
	}
	return events;
}
export function diagnosticReport(observations, history, events) {
	return {
		schema: "raftkv-diagnostics/v1",
		exported_at: new Date().toISOString(),
		scope: "Browser observations; independent node samples are not an atomic cluster snapshot.",
		observations,
		history,
		events
	};
}
