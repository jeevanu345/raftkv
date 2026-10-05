# AGENTS.md

## Project

RaftKV is a Raft-replicated key-value store implemented primarily in Rust.

This file defines repository-wide instructions for coding agents working on the RaftKV dashboard and related frontend code.

## Primary architectural rule

The existing pure/impure boundary must be preserved.

`raft-core` is a deterministic consensus state machine. It must not gain browser code, HTTP code, React-specific concerns, wall-clock access, filesystem access, asynchronous I/O, or UI state.

The dashboard is an observer and operator interface layered above the runtime/control plane.

The intended direction is:

Browser
→ HTTP/SSE API
→ runtime/control facade
→ Raft runtime
→ raft-core/storage/state machine/network

The browser must never directly access:

- Raft core structures
- sled databases
- Raft log segment files
- snapshot files
- peer gRPC transport internals
- local filesystem state

## Frontend location

All dashboard frontend code belongs under:

`ui/dashboard/`

Do not scatter React components through Rust crates.

## Frontend technology

Use:

- React
- TypeScript
- Vite
- React Router
- TanStack Query
- Lucide React
- Recharts only for quantitative charts
- native EventSource for server-sent events

Avoid adding large dependencies without a clear reason.

Do not introduce a component framework such as Material UI, Ant Design, Chakra, Mantine, or Bootstrap unless explicitly requested.

The visual system is intentionally custom and lightweight.

## UI principles

The dashboard is an engineering control plane, not a marketing website.

Prefer:

- dense but readable layouts
- compact information hierarchy
- clear tables
- restrained color
- strong typography
- consistent spacing
- operational context
- visible system state
- keyboard-friendly interactions
- deterministic layout behavior

Avoid:

- emojis
- gradients used as decoration
- glassmorphism-heavy design
- oversized cards
- oversized text
- excessive rounded corners
- decorative animations
- fake terminal effects
- arbitrary neon styling
- ambiguous status colors

Subtle visualization animation is permitted only when it communicates Raft traffic or state transition.

## Design tokens

Use the centralized CSS variables in:

`ui/dashboard/src/styles/globals.css`

Do not hard-code unrelated colors throughout components.

Status semantics:

- green: healthy, committed, leader, connected
- blue/cyan: followers and neutral distributed state
- amber: transitional, candidate, joint configuration, degraded
- red: unavailable, failed, destructive operation
- primary accent: interactive/highlight state

## Required screens

The frontend supports these primary routes:

- `/` — Cluster Overview
- `/keys` — Key Explorer
- `/console` — Command Console
- `/raft` — Raft Visualizer
- `/metrics` — Metrics
- `/simulation` — Simulation Lab
- `/administration` — Administration

Do not combine all functionality into a single dashboard page.

## API boundary

All HTTP calls must go through:

`src/lib/api.ts`

Components must not issue ad-hoc fetch calls.

Add new API models to:

`src/types/api.ts`

Use TanStack Query for server state.

Use local React state only for transient UI state such as:

- selected key
- dialog state
- console input
- filters
- paused visualization state

## Expected HTTP contracts

Core browser APIs include:

- `GET /api/v1/cluster`
- `GET /api/v1/nodes`
- `GET /api/v1/nodes/:id`
- `GET /api/v1/events`
- `GET /api/v1/keys`
- `GET /api/v1/keys/:key`
- `PUT /api/v1/keys/:key`
- `DELETE /api/v1/keys/:key`
- `POST /api/v1/commands`
- `POST /api/v1/admin/snapshot`
- `POST /api/v1/admin/leadership`
- `POST /api/v1/admin/members`
- `DELETE /api/v1/admin/members/:id`
- `GET /api/v1/snapshots`

Simulation APIs may use the `/lab/api/v1/` namespace.

Do not silently invent backend behavior. If a UI feature needs an endpoint that does not exist, keep the frontend contract explicit and document the dependency.

## SSE

Raft visualizer events use server-sent events.

Centralize event stream behavior in:

`src/hooks/useRaftEvents.ts`

Event handlers must tolerate:

- connection loss
- reconnects
- malformed events
- duplicate events
- event bursts

The rendered event history must remain bounded.

Never allow an unbounded browser event array.

## Key Explorer

The Key Explorer must use SCAN-style pagination.

Do not depend on `KEYS *`.

Requirements:

- pattern search
- paginated key enumeration
- key metadata
- TTL
- size
- encoding
- value view
- editing
- delete confirmation
- copy functionality

Binary values must have an explicit representation such as UTF-8, Base64, or hexadecimal.

Never assume all values are valid UTF-8.

## Command Console

The console is not a fake terminal.

It should surface:

- submitted command
- structured response
- raw RESP response when available
- duration
- receiving node
- current leader
- term
- proposed log index for writes
- commit latency
- apply latency
- replication count

Use monospace typography only where technically appropriate.

## Raft Visualizer

The visualizer should make protocol state understandable.

Display:

- node role
- term
- leader
- commit index
- applied index
- recent protocol traffic
- recent log entries
- snapshot boundary
- event timeline

The visualizer may animate Raft messages, but motion must remain subtle and informational.

Do not use random animation.

## Simulation Lab

Simulation and live-cluster administration are separate concepts.

The Simulation Lab must be visually and logically separated from live production controls.

A destructive simulator action must never be wired to a live-cluster endpoint.

Simulation state should include a reproducible seed.

## Administration safety

Destructive operations require confirmation.

This includes:

- removing members
- FLUSHDB
- restoring backups
- destructive snapshot/restore actions
- operations that intentionally reduce quorum

The frontend must not imply an operation is safe merely because a button is enabled.

Backend authorization remains authoritative.

## Accessibility

Requirements:

- semantic buttons
- visible focus styles
- keyboard navigation
- sufficient contrast
- labels for inputs
- title/aria labels for icon-only controls
- avoid color-only meaning

## Responsive behavior

Desktop is the primary target.

The dashboard must remain usable on:

- 1440px+
- 1280px
- 1024px
- tablet-sized displays

On narrow layouts:

- collapse sidebar to icons
- eventually move navigation to the bottom
- convert grids to one column
- preserve data readability

Do not optimize the Raft visualizer for tiny phone screens at the expense of desktop quality.

## TypeScript rules

Use strict TypeScript.

Avoid:

- `any`
- type assertions used to bypass incorrect models
- duplicated API interfaces
- giant page components when reusable behavior is obvious

Prefer:

- discriminated unions
- explicit nullable values
- typed API responses
- small formatting utilities
- data transformation outside JSX when complex

## CSS rules

Keep the design system centralized.

Prefer BEM-like class naming used by the existing dashboard.

Avoid inline styles except for genuinely dynamic values such as:

- graph positions
- progress percentages

Do not add CSS-in-JS.

## Performance

Do not poll at extreme frequency.

Recommended baseline:

- cluster state: every 2 seconds
- simulation state: every 1 second if needed
- protocol events: SSE instead of polling

Bound long lists.

Use virtualization only when there is evidence it is needed.

## Demo mode

`VITE_DEMO_MODE=true` allows frontend development without a running cluster.

Mock data belongs in:

`src/lib/mock.ts`

Demo behavior must never be mixed into real API response parsing.

## Do not modify backend without instruction

If the task is explicitly frontend-only:

Do not edit:

- `crates/raft-core`
- `crates/raft-storage`
- `crates/raft-net`
- `crates/kv-state-machine`
- `crates/resp-server`
- Rust runtime behavior
- protobuf definitions
- deployment files

Frontend code may define the expected API contract without implementing the backend.

## Quality gate

Before finishing a frontend change:

1. Run TypeScript type checking.
2. Build the Vite application.
3. Verify every route renders.
4. Verify there are no console errors.
5. Test demo mode.
6. Test loading states.
7. Test empty states.
8. Test error states.
9. Test destructive confirmation dialogs.
10. Check keyboard navigation.
11. Check 1440px, 1024px, and narrow layouts.
12. Ensure no emojis were introduced.

## Commands

From `ui/dashboard`:

```bash
npm install
npm run dev
npm run typecheck
npm run build
```

Final requirement
A dashboard change is complete only if the UI communicates real distributed-system state clearly.
Visual polish must never come at the cost of protocol clarity.

---
