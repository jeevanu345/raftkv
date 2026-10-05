# Changelog

## 0.2 development milestone — unreleased

Implements the supplied `IMPLEMENTATION_PROMPT.md` across correctness, snapshots/recovery, TTL, dynamic membership, control APIs, observability, the seven-screen dashboard, isolated simulation, security, backup/restore, fuzz/chaos/benchmarks and deployment/CI.

Key fixes include even-sized commit quorum selection, ReadIndex quorum correlation/current-term prerequisites, atomic durable applied state, real snapshot contents/install ordering, shared runtime/gRPC action execution, committed-prefix AppendEntries handling, leadership-transfer pre-vote bypass and bounded retry windows after dropped heartbeat acknowledgements.

New storage/control formats are experimental. Legacy empty checkpoints fail validation. Existing original local work was committed and pushed separately before implementation. See `docs/IMPLEMENTATION_STATUS.md` for scope and `docs/VERIFICATION.md` for executed checks and explicit unverified items. Workspace package version remains `0.1.0` pending an actual release decision.

## 0.1 scaffold — unreleased

Original deterministic Raft core, segmented storage, sled state machine, RESP/gRPC protocol scaffolding, simulator/checker and deployment assets. Original reports, dashboard, runtime files and local tracked data were preserved in the baseline commit before this milestone work.
