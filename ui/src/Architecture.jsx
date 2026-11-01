import React from "react";
import { ArrowDownToLine, BookOpen, ChevronRight, FileJson, TriangleAlert } from "lucide-react";

import { download, Pill, Panel } from "./components.jsx";
const ARCHITECTURE_LAYERS = [
	[
		"Client protocols",
		"resp-server · raftkv-cli",
		"RESP2 command parsing and an HTTP diagnostic surface. Redis command subset; RESP3 negotiation is not implemented."
	],
	[
		"Runtime / effect execution",
		"raftkv-server",
		"Serializes consensus actions, executes persistence and peer messages, applies committed commands, and serves read-index requests."
	],
	[
		"Pure consensus",
		"raft-core",
		"Inputs: messages and ticks. Outputs: Action values. Election, append validation, current-term commit, joint consensus, and leadership transfer are deterministic."
	],
	[
		"Transport",
		"raft-net",
		"Tonic gRPC transports protocol messages. Outbound peer clients carry responses; streaming transport remains a future optimization."
	],
	[
		"Durable state",
		"raft-storage · kv-state-machine",
		"CRC32C framed segmented log, sled metadata and KV trees, snapshot scaffolding, ordered state-machine application, deterministic state fingerprints."
	],
	[
		"Verification",
		"sim-tests · linearizability-checker",
		"Seeded deterministic scenarios and exhaustive checks on short histories. These are foundations for testing, not proof of production safety."
	]
];
const INVARIANTS = [
	[
		"Election safety",
		"At most one leader in a term.",
		"Persist currentTerm/votedFor before dependent messages; apply the universal term-bump rule."
	],
	[
		"Log matching",
		"Matching index + term imply matching prefix.",
		"Followers validate prev_log_index and prev_log_term, reject conflicts, and truncate divergent suffixes."
	],
	[
		"Leader completeness",
		"Committed entries survive later leadership.",
		"Election log freshness and current-term-only quorum counting protect commit advancement."
	],
	[
		"State-machine safety",
		"Committed commands apply in log order.",
		"Apply last_applied + 1 through commit_index; never infer global consistency from HTTP reachability."
	],
	[
		"Joint consensus",
		"Membership transitions require overlapping majorities.",
		"Core supports old → old,new → new. Live administration is not exposed by this operator API."
	],
	[
		"Linearizable reads",
		"Reads must confirm current leadership and applied progress.",
		"RESP runtime uses quorum read-index and waits for application. HTTP inspection is explicitly local."
	]
];
const LIMITATIONS = [
	["Security boundary", "No completed TLS/mTLS, ACL, or client authentication. Keep the operator API and Vite gateway on localhost."],
	["Online membership", "Core joint consensus exists; a complete administration API and safe orchestration workflow remain unfinished."],
	["Snapshot transfer", "Runtime KV snapshot state transfer is incomplete. Core and storage snapshot primitives exist; full recovery and streaming chunk transfer remain future work."],
	["Protocol coverage", "RESP2 codec and a Redis command subset. RESP3 negotiation and full Redis behavioral compatibility are not implemented."],
	["TTL command", "Expire/Persist state-machine support exists, while TTL command reporting is incomplete."],
	["Operational maturity", "Backup/restore workflow, long-duration soak tests, fuzz targets, Jepsen workloads, and benchmark baselines remain future work."]
];
function Architecture({ exportData }) {
	const markdown = "# RaftKV architecture field guide\n\nEducational Rust Raft-replicated key-value store. Version 0.1.0.\n\n## Layers\n\n" + ARCHITECTURE_LAYERS.map(([title, crate, body]) => `### ${title}\n${crate}\n\n${body}`).join("\n\n") + "\n\n## Invariants\n\n" + INVARIANTS.map(([name, rule, mechanism]) => `### ${name}\n${rule}\n\n${mechanism}`).join("\n\n") + "\n\n## Limitations\n\n" + LIMITATIONS.map(([name, body]) => `- **${name}:** ${body}`).join("\n") + "\n\nSources: README.md, ARCHITECTURE.md, RAFT_NOTES.md, PROJECT_REPORT.md, source workspace.\n";
	return <>
    <div className="report-intro">
      <div>

        <h2>Consensus is a state machine.<br />Everything else is an effect.</h2>
        <p>RaftKV keeps protocol decisions reproducible by isolating pure consensus transitions from disk, networking, clocks, and client handling. The operator console exposes that boundary rather than treating the cluster as a single opaque service.</p>
      </div>
      <div className="report-actions">
        <button className="primary" onClick={() => download("raftkv-architecture.md", markdown, "text/markdown")}><ArrowDownToLine size={16} />Export field guide</button>
        <button className="button" onClick={exportData}><FileJson size={16} />Export live diagnostics</button>
      </div>
    </div>
    <Panel title="Request and effect pipeline" subtitle="Durability precedes dependent protocol messages; application follows quorum commit.">
      <div className="pipeline">
        {[
		"Client command",
		"Leader proposal",
		"Persist log",
		"Replicate / majority",
		"Commit & apply",
		"Client response"
	].map((stage, index) => <React.Fragment key={stage}>
          <div>
            <span>
              {index + 1}
            </span>
            <strong>
              {stage}
            </strong>
          </div>
          {index < 5 && <ChevronRight size={18} />}
        </React.Fragment>)}
      </div>
      <p className="muted inset">During a storage failure, the runtime must stop making progress rather than send acknowledgments for state it did not durably persist. This project remains an educational implementation with known operational gaps.</p>
    </Panel>
    <Panel title="Architecture layers" subtitle="Crate responsibilities map directly to the source workspace.">
      <div className="architecture-layers">
        {ARCHITECTURE_LAYERS.map(([title, crate, body]) => <article key={title}>
          <div>
            <h3>
              {title}
            </h3>
            <code>
              {crate}
            </code>
          </div>
          <p>
            {body}
          </p>
        </article>)}
      </div>
    </Panel>
    <Panel title="Consensus invariant map" subtitle="Read RAFT_NOTES.md for paper sections and implementation symbols.">
      <div className="table-scroll">
        <table className="invariant-table">
          <thead>
            <tr>
              <th>Invariant</th>
              <th>Required property</th>
              <th>Implementation mechanism</th>
            </tr>
          </thead>
          <tbody>
            {INVARIANTS.map(([name, rule, mechanism]) => <tr key={name}>
              <td>
                <strong>
                  {name}
                </strong>
              </td>
              <td>
                {rule}
              </td>
              <td>
                {mechanism}
              </td>
            </tr>)}
          </tbody>
        </table>
      </div>
    </Panel>
    <div className="two-columns">
      <Panel title="Storage mechanics" subtitle="The replicated log and applied state are separate recovery concerns.">
        <div className="technical-list">
          <h3>Framed append-only log</h3>
          <code>[u32 length][u32 crc32c][payload]</code>
          <p>Segment files rotate by base index. Recovery validates framing and checksums and truncates a torn tail. Compaction removes log prefixes covered by snapshots.</p>
          <h3>Metadata and application</h3>
          <p>Sled stores hard state, commit metadata, and the KV state machine. Snapshots install through temporary files and atomic rename. The rolling command-history hash supports equal-index divergence inspection; it is not a checksum of the current KV contents.</p>
        </div>
      </Panel>
      <Panel title="Read and membership semantics" subtitle="The public surface and core capabilities have different scopes.">
        <div className="technical-list">
          <h3>Read-index barrier</h3>
          <p>The leader confirms authority with a voting majority, records a read barrier, then waits until the local state machine has applied the required index. HTTP key inspection deliberately returns local state and reports its applied index.</p>
          <h3>Initial configuration</h3>
          <p>A fresh cluster can bootstrap one or many voters. Rewriting the peer list for existing persisted state is not a membership transition. Use the runbook for fresh clusters and keep live reconfiguration outside this UI’s scope.</p>
        </div>
      </Panel>
    </div>
    <Panel title="Known limitations & next engineering work" subtitle="Capabilities described here are bounded by the actual implementation.">
      <div className="limitation-grid">
        {LIMITATIONS.map(([title, body]) => <article key={title}>
          <TriangleAlert size={17} />
          <div>
            <h3>
              {title}
            </h3>
            <p>
              {body}
            </p>
          </div>
        </article>)}
      </div>
    </Panel>
    <Panel title="Repository reading map" subtitle="The project report complements the source-level architecture and invariant documentation.">
      <div className="doc-map">
        {[
		["README.md", "Project scope and command surface"],
		["ARCHITECTURE.md", "Layering, recovery, transport, and trade-offs"],
		["RAFT_NOTES.md", "Raft paper to implementation map"],
		["PROJECT_REPORT.md", "Technical project report and roadmap"],
		["HOW_TO_RUN.md", "Bootstrap, validation, and troubleshooting"],
		["CHANGELOG.md", "Maintained implementation checklist"]
	].map(([file, purpose]) => <div key={file}>
          <BookOpen size={16} />
          <code>
            {file}
          </code>
          <span>
            {purpose}
          </span>
        </div>)}
      </div>
    </Panel>
  </>;
}
export default Architecture;
