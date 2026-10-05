# CHATGPT.md

## Role

When working on this repository, ChatGPT should treat RaftKV as a distributed-systems project first and a UI project second.

Frontend recommendations must preserve the system's architecture and should make protocol behavior easier to understand rather than obscuring it.

Read `AGENTS.md` before proposing frontend modifications.

## Dashboard objective

The RaftKV dashboard should provide a professional systems-control interface for:

- understanding cluster health
- inspecting node state
- exploring replicated key/value data
- running client commands
- observing Raft behavior
- viewing performance metrics
- running deterministic simulations
- performing controlled administrative operations

It is not intended to imitate RedisInsight exactly.

It should reflect RaftKV's distinct capabilities.

## Visual character

Use a modern enterprise engineering aesthetic.

Characteristics:

- dark neutral background
- restrained blue primary accent
- thin structural borders
- compact typography
- high information density
- clear system status
- subtle motion
- minimal decoration
- no emojis

Do not describe the design using vague language such as "futuristic", "cyberpunk", or "beautiful".

Prefer concrete interface decisions.

## Screens

### Overview

Prioritize:

- leader
- term
- quorum
- node roles
- commit index
- applied index
- log position
- replication lag
- storage
- state hash
- request rate
- latency

### Key Explorer

Prioritize:

- SCAN-based enumeration
- key filtering
- value representation
- TTL
- size
- editing
- delete confirmation
- binary-safe display

Do not recommend `KEYS *` as the normal browsing mechanism.

### Command Console

Prioritize both command result and distributed execution context.

Useful execution metadata:

- receiving node
- leader
- term
- log index
- replication count
- commit time
- apply time
- request duration

### Raft Visualizer

Show causality.

Important transitions include:

- election starts
- vote requests
- votes
- leader election
- AppendEntries
- append rejection
- conflict repair
- commit advancement
- entry application
- ReadIndex rounds
- snapshot transfer
- leadership transfer

Do not turn the visualizer into decorative moving lines.

### Metrics

Keep custom metrics views compact.

Grafana remains appropriate for deep time-series analysis.

The RaftKV UI should surface the metrics required to understand immediate cluster behavior.

### Simulation Lab

Keep simulation clearly separate from the live cluster.

Simulation controls may include:

- start
- pause
- step
- reset
- partition
- heal
- crash
- restart
- delay
- drop
- duplicate
- reorder

Always display the simulation seed.

### Administration

Destructive controls must be explicit.

Use confirmation for:

- removing a member
- FLUSHDB
- restore
- dangerous quorum changes

The frontend does not replace backend authorization.

## Architecture

Expected structure:

```text
React / TypeScript
        |
        | HTTP + SSE
        v
Browser-facing control API
        |
        v
RaftKV runtime
        |
        +-- raft-core
        +-- storage
        +-- state machine
        +-- peer network
```

Never propose direct browser access to:
- sled
- Raft logs
- snapshot files
- raw peer transport
- in-process Raft structures
Source of truth
API models belong in:
src/types/api.ts
HTTP behavior belongs in:
src/lib/api.ts
Formatting belongs in:
src/lib/format.ts
Mock development state belongs in:
src/lib/mock.ts
SSE behavior belongs in:
src/hooks/useRaftEvents.ts
Pages should consume those abstractions.
Coding responses
When giving code:
- provide complete files when the user requests copy-paste-ready code
- keep imports correct
- preserve existing directory structure
- avoid unrelated backend changes
- do not omit essential CSS
- avoid pseudo-code if implementation code was requested
- do not replace working architecture with a new framework without reason
Terminology
Use consistent distributed-systems terminology:
- leader
- follower
- candidate
- pre-candidate
- term
- voter
- learner
- quorum
- commit index
- applied index
- match index
- next index
- replication lag
- snapshot index
- joint consensus
- linearizable read
- ReadIndex
Avoid generic labels when a precise Raft term exists.
Safety of claims
Do not make UI text claim stronger guarantees than the backend actually provides.
For example, only label a read "Linearizable" if the backend's read path really implements the required ReadIndex quorum confirmation.
Only label membership administration "available" if the runtime control path is wired.
The UI should be able to represent unavailable or experimental capability honestly.
No emojis
Do not insert emojis into:
- navigation
- headings
- buttons
- status messages
- toast messages
- empty states
- documentation examples
Use Lucide icons where visual indicators are useful.
Completion checklist
Before considering a frontend response complete, verify conceptually that the implementation includes:
- route structure
- type definitions
- API layer
- event stream
- shell/navigation
- overview
- key explorer
- command console
- Raft visualizer
- metrics
- simulation lab
- administration
- responsive styling
- loading states
- basic failure states
- destructive confirmation
- demo support
- no emojis
The result should look like one coherent product, not seven unrelated demo pages.
