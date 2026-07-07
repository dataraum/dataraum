# Orchestration

How work moves between the cockpit and the engine.

## Temporal is the only orchestration substrate

- All long-running work runs as Temporal workflows. There is no second
  scheduler, no job queue, no cron.
- The engine has **no HTTP server and no MCP transport**. No
  OpenAPI, no client codegen. The engine↔cockpit integration surface is exactly
  two things: Postgres (see [persistence](./persistence.md)) and Temporal.
- Worker health is the Temporal heartbeat (`temporal worker list`), not an HTTP
  probe — there is no health endpoint to poll.

## Two workers, two queues

| Worker | Language | Queue | Hosts |
|---|---|---|---|
| Engine — one per workspace | Python | `engine-<workspace_id>` | Analysis workflows + all activities |
| Cockpit — co-located in the server process | TypeScript | `cockpit-orchestration` | Short-lived orchestration workflows |

- The engine worker bundles workflows and activities together and polls exactly
  its own workspace queue; a queue↔workspace mismatch fails loud at boot
  (`server/workspace.py`), before the worker advertises itself.
- The cockpit is otherwise a Temporal **Client**: it starts workflows and
  renders progress. Analysis logic never lives in TypeScript; orchestration
  logic never lives on the engine worker.

## Workflows cross the language boundary by name

- There is no shared workflow catalogue and no codegen. Engine workflows are
  addressed by registered string name (`addSourceWorkflow`,
  `beginSessionWorkflow`, `operatingModelWorkflow`); their input/result shapes
  are Pydantic contracts (`worker/contracts.py`), **hand-mirrored** in the
  cockpit (`src/temporal/types.ts`). Changing a workflow signature or return
  shape is a cross-package edit.
- Cockpit orchestration workflows start engine stages as cross-language child
  workflows (string type name, `ParentClosePolicy.ABANDON`) on the workspace's
  engine queue.

## Orchestration workflows are short-lived and per-trigger

- Each orchestration workflow runs once per trigger under a deterministic
  per-workspace id (`grounding-<ws>`, `session-<ws>`), carries its state on the
  start payload, and holds no cross-run state. No signals, no `patched()`, no
  continue-as-new — no resident per-workspace coordinator exists.
- **Single-flight** per workflow id: reuse policy `ALLOW_DUPLICATE` (restartable
  once the prior run closed) + conflict policy `FAIL` (rejected while one runs),
  surfaced as an actionable "already running" error. An orchestration workflow
  starts its engine child under the same id a direct start would use, so
  single-flight holds across both paths.
- A stage with no follow-on starts as a direct single-shot engine workflow — no
  orchestration wrapper.

## Workflow code is deterministic; everything else is an activity

- Engine workflow code (`worker/workflows.py`) imports only `temporalio` plus
  the engine-free contract shapes (through `imports_passed_through`) and calls
  activities by string name — it never imports activity implementations. All
  IO, persistence, and LLM calls happen inside activities.
- Cockpit workflow code is pre-bundled at build time (`workflowBundle`); the
  sandbox imports only `@temporalio/workflow` plus pure modules. A worker-code
  change lands only when a new bundle loads (container rebuild + recreate;
  dev-server restart in dev).

## Agentic LLM lives in the cockpit; the engine is durable pipeline + grounding

- Streaming, tool-calling, conversational LLM work — chat, concept induction,
  agent prompts — is cockpit-only, in the TS tier, outside Temporal workflows
  entirely. A streaming LLM loop never runs inside a Temporal activity.
- The engine's LLM use is non-streaming calls inside durable activities; the
  engine owns the reproducible pipeline and grounding, nothing conversational.
