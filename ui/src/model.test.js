import test from "node:test";
import assert from "node:assert/strict";
import { clusterSummary, counterRate, decodeKey, diagnosticReport, observationEvents, validateMonitor } from "./model.js";
const node = (id, overrides = {}, online = true) => ({
	id,
	port: 8079 + id,
	online,
	status: {
		node_id: id,
		role: id === 1 ? "leader" : "follower",
		term: 4,
		applied_index: 10,
		state_hash: "same",
		membership: {
			voters: [
				1,
				2,
				3,
				4
			],
			quorum_size: 3,
			joint: false
		},
		...overrides
	}
});
test("even voter configuration requires a strict majority; HTTP reachability remains observation only", () => {
	const summary = clusterSummary([
		node(1),
		node(2),
		node(3, {}, false),
		node(4, {}, false)
	]);
	assert.equal(summary.quorum, 3);
	assert.equal(summary.reachableVoters, 2);
	assert.equal("quorumActive" in summary, false);
});
test("joint configuration does not invent a union quorum", () => {
	const summary = clusterSummary([node(1, { membership: {
		voters: [
			1,
			2,
			3,
			4,
			5
		],
		old_voters: [
			1,
			2,
			3
		],
		new_voters: [
			3,
			4,
			5
		],
		joint: true,
		quorum_size: null
	} })]);
	assert.equal(summary.quorum, null);
});
test("failed durable runtime cannot be selected as a write leader", () => {
	assert.equal(clusterSummary([node(1, { health: { state: "failed" } })]).leaders.length, 0);
});
test("compare hashes only for equal applied indices and reachable nodes", () => {
	assert.equal(clusterSummary([
		node(1),
		node(2, {
			applied_index: 9,
			state_hash: "other"
		}),
		node(3, { state_hash: "other" }, false)
	]).hashMismatch, false);
	assert.equal(clusterSummary([node(1), node(2, { state_hash: "other" })]).hashMismatch, true);
});
test("counter rate omits restarts, decrements, missing counters, and node changes", () => {
	const first = {
		time: 1e3,
		node_id: 1,
		uptime_ms: 100,
		writes: 5
	};
	const last = {
		time: 3e3,
		node_id: 1,
		uptime_ms: 300,
		writes: 9
	};
	assert.equal(counterRate([first, last], "writes"), 2);
	assert.equal(counterRate([first, {
		...last,
		uptime_ms: 10
	}], "writes"), null);
	assert.equal(counterRate([first, {
		...last,
		writes: 1
	}], "writes"), null);
	assert.equal(counterRate([first, {
		...last,
		node_id: 2
	}], "writes"), null);
	assert.equal(counterRate([first, last], "missing"), null);
});
test("binary and valid UTF-8 key names remain distinguishable", () => {
	const binary = decodeKey([255, 0]);
	assert.equal(binary.text, "0xff00");
	assert.equal(binary.editable, false);
	const unicode = decodeKey(new TextEncoder().encode("λ"));
	assert.equal(unicode.text, "λ");
	assert.equal(unicode.bytes, 2);
	assert.equal(unicode.editable, true);
	assert.notEqual(binary.identity, decodeKey(new TextEncoder().encode(binary.text)).identity);
});
test("connection validation rejects fractional IDs, unsafe ports and nonnumeric values", () => {
	for (const [id, port] of [
		[0, 8080],
		[1.5, 8080],
		[1, 80],
		[1, 65536],
		["x", 8080]
	]) assert.ok(validateMonitor(id, port));
	assert.equal(validateMonitor("5", "8084"), null);
});
test("observed transitions capture role changes and availability, without inferring unseen elections", () => {
	const events = observationEvents([node(1)], [node(1, {
		role: "follower",
		term: 5
	})], 100);
	assert.equal(events.length, 2);
	assert.ok(events.every((e) => e.kind === "consensus"));
	assert.equal(observationEvents([node(1)], [node(1, {}, false)], 101)[0].kind, "warning");
});
test("diagnostics have an explicit non-atomic observation scope and schema", () => {
	const report = diagnosticReport([node(1)], {}, []);
	assert.equal(report.schema, "raftkv-diagnostics/v1");
	assert.match(report.scope, /not an atomic/);
});

test("UTF-8 byte order mark is preserved in key names", () => { const key=decodeKey([239,187,191,97]); assert.equal(key.text, "\ufeffa"); assert.deepEqual([...new TextEncoder().encode(key.text)], [239,187,191,97]); });
