import { useEffect, useRef, useState } from "react";
import { HISTORY_LIMIT, EVENT_LIMIT, observationEvents } from "./model.js";
export async function request(url, options = {}) {
	const response = await fetch(url, {
		...options,
		signal: options.signal || AbortSignal.timeout(8e3)
	});
	const body = await response.text();
	let data;
	try {
		data = JSON.parse(body);
	} catch {
		throw new Error(`HTTP ${response.status}: API returned no JSON. Check the node API port.`);
	}
	if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`);
	return data;
}
export const endpoint = (monitor) => `/nodes/${monitor.port}/api`;
export function useCluster(monitors, discover, paused, interval, nonce) {
	const [observations, setObservations] = useState([]);
	const [history, setHistory] = useState({});
	const [events, setEvents] = useState([]);
	const [refreshing, setRefreshing] = useState(false);
	const [lastRefresh, setLastRefresh] = useState(null);
	const latest = useRef([]);
	const discoverRef = useRef(discover);
	discoverRef.current = discover;
	const appendEvent = (event) => setEvents((old) => [{
		...event,
		time: Date.now(),
		kind: "operation"
	}, ...old].slice(0, EVENT_LIMIT));
	useEffect(() => {
		if (paused) {
			setRefreshing(false);
			return;
		}
		let disposed = false;
		let timer;
		let controller;
		const poll = async () => {
			controller = new AbortController();
			const timeout = setTimeout(() => controller.abort(), 4500);
			setRefreshing(true);
			const next = await Promise.all(monitors.map(async (monitor) => {
				const start = performance.now();
				try {
					const status = await request(`${endpoint(monitor)}/status`, { signal: controller.signal });
					if (status.node_id !== monitor.id) throw new Error(`Expected node ${monitor.id}; endpoint reports node ${status.node_id}. Update the connection.`);
					return {
						...monitor,
						status,
						online: true,
						time: Date.now(),
						latency: Math.round(performance.now() - start),
						error: null
					};
				} catch (error) {
					const old = latest.current.find((n) => n.id === monitor.id);
					return {
						...monitor,
						status: old?.status,
						time: old?.time,
						online: false,
						latency: null,
						error: error.name === "AbortError" ? "Status request timed out after 4.5 seconds." : error.message
					};
				}
			}));
			clearTimeout(timeout);
			if (disposed) return;
			const time = Date.now();
			const changes = observationEvents(latest.current, next, time);
			latest.current = next;
			setObservations(next);
			setLastRefresh(time);
			setEvents((old) => [...changes.reverse(), ...old].slice(0, EVENT_LIMIT));
			setHistory((old) => {
				const updated = { ...old };
				for (const node of next) if (node.online) updated[node.id] = [...old[node.id] || [], {
					time,
					node_id: node.id,
					uptime_ms: node.status.uptime_ms,
					commit: node.status.commit_index,
					applied: node.status.applied_index,
					log: node.status.last_log_index,
					term: node.status.term,
					latency: node.latency,
					pending: node.status.queues?.pending_proposals,
					...node.status.counters
				}].slice(-HISTORY_LIMIT);
				return updated;
			});
			discoverRef.current(next);
			setRefreshing(false);
			timer = setTimeout(poll, interval);
		};
		poll();
		return () => {
			disposed = true;
			clearTimeout(timer);
			controller?.abort();
		};
	}, [
		monitors,
		paused,
		interval,
		nonce
	]);
	return {
		observations,
		history,
		events,
		refreshing,
		lastRefresh,
		appendEvent,
		clearEvents: () => setEvents([])
	};
}
