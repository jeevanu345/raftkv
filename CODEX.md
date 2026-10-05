# CODEX.md

## Purpose

Instructions for Codex when modifying the RaftKV frontend.

Read `AGENTS.md` first. `AGENTS.md` is the canonical repository guidance. This file adds Codex-specific execution rules.

## Scope discipline

If the request is about the dashboard, remain inside:

`ui/dashboard/`

unless the user explicitly requests backend integration.

Do not opportunistically refactor Rust code during a frontend task.

Do not modify consensus logic merely to satisfy a frontend assumption.

If a backend capability is missing, define the required typed frontend contract and clearly identify the dependency.

## Before editing

Inspect:

1. `ui/dashboard/package.json`
2. `ui/dashboard/src/types/api.ts`
3. `ui/dashboard/src/lib/api.ts`
4. `ui/dashboard/src/styles/globals.css`
5. the route/page being modified
6. reusable components related to the feature

Do not create duplicate abstractions before checking what exists.

## Design preservation

Preserve the established visual language:

- dark neutral surfaces
- thin borders
- subtle primary accent
- compact typography
- compact status badges
- operational density
- low visual noise
- no emojis
- no decorative gradients
- no unnecessary card explosion

Prefer extending CSS tokens instead of introducing one-off colors.

## Component policy

A page should orchestrate behavior and layout.

Reusable elements belong under `src/components`.

Do not extract components merely because they are ten lines long.

Extract when one of the following is true:

- reused by multiple pages
- logically independent
- difficult to read inline
- has independent interaction state
- represents a consistent design-system primitive

## Data policy

Never fabricate live cluster data inside page components.

Mock data belongs in:

`src/lib/mock.ts`

All production HTTP access belongs in:

`src/lib/api.ts`

All production server-state caching belongs in TanStack Query.

## Event policy

Do not place direct EventSource construction inside visual components.

Use the shared Raft event hook.

Event collections must be bounded.

When adding new event types:

1. update `RuntimeEventType`
2. update `RuntimeEvent`
3. handle optional fields safely
4. ensure older backend payloads do not crash rendering

## Destructive actions

For every destructive control:

- use explicit wording
- require confirmation
- describe Raft implications when relevant
- keep backend authorization authoritative
- do not optimistically pretend the action completed before success

## Error handling

Do not silently swallow API errors in user-facing operations.

Pages should eventually surface:

- request failure
- disconnected event stream
- missing leader
- unavailable cluster
- stale data where relevant

Internal malformed event payloads may be ignored if they do not affect safety.

## Type safety

Do not use:

```ts
as any
```

Do not suppress TypeScript errors simply to finish a task.
Correct the type or contract.
Running checks
After frontend edits run:
cd ui/dashboard
npm run typecheck
npm run build

If either fails, fix it before finishing.
Route validation
Verify all primary routes:
/
 /keys
 /console
 /raft
 /metrics
 /simulation
 /administration

UX validation
For each modified screen verify:
- loading
- populated
- empty
- backend failure
- narrow viewport
- keyboard focus
- disabled state
- pending mutation state
Working with existing files
Prefer editing existing files over replacing whole systems.
Do not introduce another styling framework.
Do not introduce Tailwind into this dashboard unless explicitly requested.
Do not replace custom CSS with a component library.
Comments
Use comments to explain architecture or subtle behavior.
Do not annotate obvious JSX.
Good:
// Bound event history so a long-running dashboard cannot grow memory forever.

Bad:
// Render the button.

Commit-quality expectation
Changes should look intentional enough to merge directly:
- no placeholders unless clearly marked
- no TODO spam
- no random sample values in production path
- no dead imports
- no unused components
- no inconsistent terminology
- no emojis
- no console.log debugging
Backend dependencies
When the UI expects a backend capability that is not yet implemented, document it using a precise contract such as:
GET /api/v1/keys?cursor=<opaque>&limit=100&pattern=user:*

Do not invent hidden behavior.
Architectural invariant
The React application is not part of Raft consensus.
Never move consensus decisions into browser code.
The browser displays, requests, and administers. The server decides.
