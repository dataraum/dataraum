# Project: Progressive Result Delivery

*Stream pipeline results to consumers as they become ready, instead of making users wait for the full pipeline.*

---

## Problem

The pipeline takes ~610s for a medium dataset. Users wait for all 19 phases to complete before seeing any results. But valuable data (entropy scores, resolution actions) is ready at ~460s — the final 150s is `graph_execution` which only `query` needs.

Worse: the current delivery model is batch-oriented. Run pipeline → wait → consume results. This wastes the user's time and breaks the collaborative workflow where an LLM agent could start working on data quality fixes while later phases are still running.

### Timing breakdown (medium dataset, 8 tables)

```
Phase                     Duration   Cumulative   What becomes available
─────────────────────────────────────────────────────────────────────────
import                       0.2s        0.2s     Raw data in DuckDB
typing                       0.5s        0.7s     Typed tables, schema
statistics                   0.0s        0.7s     Statistical profiles
column_eligibility            0.0s        0.7s     Eligible column list
correlations                 0.0s        0.7s     Correlation matrix
statistical_quality          0.1s        0.8s     Quality metrics
relationships                0.3s        1.1s     Table relationships
temporal                     1.0s        2.1s     Temporal profiles
semantic                    85.2s       87.3s     Column/table annotations
enriched_views               0.0s       87.3s     Pre-joined views
slicing                     18.3s      105.6s     Slice definitions
slice_analysis               0.5s      106.1s     Slice statistics
temporal_slice_analysis      1.1s      107.2s     Temporal drift
quality_summary             51.9s      159.1s     Quality grades (A-F)
entropy                      0.4s      159.5s     Entropy scores, network
validation                  72.1s      231.6s     Validation results
business_cycles             67.3s      298.9s     Detected cycles
entropy_interpretation     158.9s      457.8s     Interpretations + actions
graph_execution            152.4s      610.2s     Calculated metrics
```

Note: validation, business_cycles run in parallel with quality_summary→entropy→entropy_interpretation. Wall clock is shorter than cumulative.

## Core Insight

The pipeline already persists results to SQLite immediately after each phase completes. The data IS available mid-pipeline — nothing tells the consumer about it. The gap is notification, not storage.

---

## The Answer: SEP-1686 (Tasks)

[SEP-1686](https://modelcontextprotocol.io/community/seps/1686-tasks) is the accepted
MCP standard for background task execution. Status: **Final** (standards track).
It solves our problem exactly.

### How It Works

SEP-1686 shifts polling from the LLM to the **host application**. The host is a
deterministic program — it can reliably poll every N seconds without prompt engineering.

```
1. LLM calls analyze
2. Host sends tools/call with _meta: {"modelcontextprotocol.io/task": {taskId: "..."}}
3. Server returns immediately, sends notifications/tasks/created
4. HOST APPLICATION polls tasks/get on a timer (guided by pollFrequency)
5. Host informs LLM when status changes
6. Host fetches tasks/result when completed, gives to LLM
```

Key architectural properties:
- **No capability negotiation needed.** Client optimistically sends task metadata.
  Server either handles it (creates a task) or ignores it (processes normally).
- **No push/injection risk.** The host pulls on its own schedule. The server only
  responds to requests — no unsolicited content into the LLM's context.
- **Non-blocking.** The LLM continues the conversation while the pipeline runs.
  Other tools (get_actions, get_entropy) can be called mid-pipeline.

### Wire Protocol

```
Client → Server:  tools/call {name: "analyze", _meta: {"modelcontextprotocol.io/task": {taskId: "abc-123"}}}
Server → Client:  notifications/tasks/created {_meta: {related-task: {taskId: "abc-123"}}}

[Host polls every pollFrequency ms]
Client → Server:  tasks/get {taskId: "abc-123"}
Server → Client:  {taskId: "abc-123", status: "working", pollFrequency: 5000}

[...pipeline completes...]
Client → Server:  tasks/get {taskId: "abc-123"}
Server → Client:  {taskId: "abc-123", status: "completed", keepAlive: 60000}

Client → Server:  tasks/result {taskId: "abc-123"}
Server → Client:  {content: [...full pipeline results...]}
```

### Implementation Status

| Layer | Status | Notes |
|---|---|---|
| SEP-1686 spec | **Final** | Accepted MCP standard |
| MCP Python SDK (types) | **Stable** | `tasks/get`, `tasks/result`, `TaskStatus` in `mcp/types.py` |
| MCP Python SDK (server) | **Experimental** | `server.experimental.enable_tasks()`, full implementation under `experimental/` |
| FastMCP (gofastmcp.com) | **Stable** | `@mcp.tool(task=True)` + `Progress` dependency |
| DataRaum server | **Implemented** | Uses experimental API: `run_task()`, `update_status()` |
| Claude Code client | **Not implemented** | [Issue #18617](https://github.com/anthropics/claude-code/issues/18617) — open, stale |
| Claude Desktop client | **Not implemented** | No `_meta` sent, `tasks: None` in capabilities |

**The blocker is purely client-side.** The spec is done, the SDK supports it,
our server supports it. Claude Code and Claude Desktop haven't implemented the
client side yet.

### DataRaum's Current Server Implementation

Already in `server.py`:

```python
server.experimental.enable_tasks()

# Tool declaration
Tool(name="analyze", ..., execution=ToolExecution(taskSupport="optional"))

# Handler
if experimental and experimental.is_task:
    # Task-augmented path: return immediately, run in background
    return await experimental.run_task(
        _work,
        model_immediate_response="Pipeline started..."
    )
# else: blocking fallback
```

Progress updates bridge the sync pipeline thread to async MCP notifications:

```python
def _make_task_progress_callback(task, loop):
    def _callback(current, total, message):
        label = _PHASE_LABELS.get(message, message)
        asyncio.run_coroutine_threadsafe(
            task.update_status(f"Phase {current}/{total}: {label}"),
            loop,
        )
    return _callback
```

### What's Missing for Full Progressive Delivery

SEP-1686 as accepted provides:
- Non-blocking execution (`tasks/get` polling)
- Simple status (`working` / `completed` / `failed`)
- Final result retrieval (`tasks/result`)

SEP-1686 **Future Work** (not yet standardized) lists exactly our needs:
- **Intermediate Results**: "Streaming analysis results as they become available.
  Reporting completed phases of multi-step operations. Providing preview data
  while full processing continues."
- **Push Notifications**: Server-initiated status change notifications for
  long-running tasks.

Until intermediate results are standardized, our enriched polling approach
(Phase 2 below) is the bridge.

---

## Design Principles

### 1. Don't optimize backend, optimize perceived progress

Making `entropy_interpretation` 30% faster saves ~50s. Streaming its results as soon as they're ready and letting agents act on them while `graph_execution` runs saves ~150s of *perceived* wait AND lets work start earlier. UX beats backend optimization.

### 2. HATEOAS-inspired events

Events don't just say "phase X done" — they tell the consumer what capabilities are now available and what data can be consumed. The consumer doesn't need to know the phase graph; it just follows the links.

### 3. Actions are the primary deliverable

The entropy system exists to produce resolution actions that users (with agent collaboration) can act on. Actions should stream to consumers the moment they're generated, not wait for unrelated phases.

### 4. Progressive enhancement, not progressive degradation

Each MCP function should work with whatever data exists, returning the best result possible with available information, plus metadata about what's still pending.

---

## Architecture

### MCP Tool Readiness Model

Each MCP tool declares what phases it needs (hard blocks) and what enhances its output (soft blocks):

```python
@dataclass
class ToolReadiness:
    """Declares what a tool needs to function."""
    hard_blocks: list[str]    # Cannot run without these phases
    soft_blocks: list[str]    # Better results with these, but works without

    def status(self, completed_phases: set[str]) -> ToolStatus:
        if not all(p in completed_phases for p in self.hard_blocks):
            return ToolStatus.BLOCKED
        if all(p in completed_phases for p in self.soft_blocks):
            return ToolStatus.READY
        return ToolStatus.PARTIAL

TOOL_READINESS = {
    "get_context": ToolReadiness(
        hard_blocks=["typing"],
        soft_blocks=["semantic", "temporal", "column_eligibility",
                     "statistical_quality", "relationships", "correlations",
                     "slicing", "quality_summary", "entropy_interpretation",
                     "validation", "business_cycles"],
    ),
    "get_entropy": ToolReadiness(
        hard_blocks=["entropy", "entropy_interpretation"],
        soft_blocks=[],
    ),
    "evaluate_contract": ToolReadiness(
        hard_blocks=["entropy"],
        soft_blocks=["entropy_interpretation"],
    ),
    "query": ToolReadiness(
        hard_blocks=["typing"],
        soft_blocks=["semantic", "enriched_views", "graph_execution"],
    ),
    "get_actions": ToolReadiness(
        hard_blocks=["entropy", "entropy_interpretation"],
        soft_blocks=[],
    ),
    "discover_sources": ToolReadiness(
        hard_blocks=[],
        soft_blocks=["import"],
    ),
}
```

### Interaction Model: Agent Collaboration

The end goal is a workflow like this:

```
User: "Analyze my financial data"

[Pipeline starts, returns immediately via SEP-1686 task]

Agent (at ~90s): "I've imported and annotated 8 tables with 47 columns.
  Schema and relationships are ready — I can answer questions about
  your data structure while the quality analysis continues."

Agent (at ~460s): "Entropy analysis complete. I found 5 high-priority
  data quality issues:
  1. bank_transactions.amount lacks unit documentation (EUR? USD?)
  2. invoices show temporal drift in 6 periods
  3. journal_entries.description has quality grade D
  ...
  Should I start working on these while metrics calculations finish?"

User: "Yes, start with the currency issue"

Agent: [calls get_actions, starts document_unit workflow]

Agent (at ~610s): "Pipeline complete. Calculated metrics are now
  available for queries. Meanwhile, I've documented the currency
  unit for bank_transactions.amount — ready for your review."
```

The key insight: agents should be able to start working on actions *before the pipeline finishes*. This turns 610s of dead wait into 150s of collaborative work.

---

## Dependency Fix: graph_execution (DONE)

`graph_execution` declared a dependency on `entropy_interpretation`, but only reads
`EntropySnapshotRecord` + `EntropyObjectRecord` + Bayesian network inference — all
produced by the `entropy` phase. It does NOT read `EntropyInterpretationRecord`.

**Fixed:** Changed dependency from `entropy_interpretation` to `entropy` in
`graph_execution_phase.py`. Now `entropy_interpretation` and `graph_execution`
run in parallel:

```
Before (sequential):
  ... → entropy → entropy_interpretation (159s) → graph_execution (152s)
  Total: ~311s for these two phases

After (parallel):
  ... → entropy ─┬─ entropy_interpretation (159s) ─┐
                  └─ graph_execution (152s) ────────┘
  Total: ~159s (max of the two)

  Saves ~152s wall time.
```

This is the single highest-impact change: zero code complexity, ~25% total pipeline reduction.

---

## Implementation Phases

### Phase 1: Dependency fix (S-sized) — DONE
- Changed `graph_execution` dependency from `entropy_interpretation` to `entropy`
- `entropy_interpretation` and `graph_execution` now run in parallel
- ~152s wall time savings

### Phase 2: Enriched progress callback (S-sized, next)
- Change `_make_task_progress_callback` to include capability info in status message
- Add `ToolReadiness` declarations (dict mapping tool name → required phases)
- Compute which tools became available after each phase completes
- Status message: `"Phase 15/19: Entropy complete. get_entropy, get_actions now available."`
- Update `analyze/skill.md` to instruct LLM to act on capability-enriched status

### Phase 3: Tool readiness metadata on responses (M-sized)
- Each MCP tool response includes `pipeline_status` section when pipeline is running
- Shows: which tools are ready, which are waiting, what's in progress
- `get_actions` called mid-pipeline returns available actions + note about pending phases
- Enables "call early, get partial results" pattern

### Phase 4: Plugin skill update (S-sized)
- Update `analyze/skill.md` to teach the LLM about progressive delivery
- Instead of "poll get_context every 2 min", instruct: "watch status messages,
  call get_actions as soon as entropy_interpretation is complete"
- Update phase table with HATEOAS-like "what you can do now" guidance

### Phase 5: Event bus for non-MCP consumers (M-sized, optional)
- Pipeline orchestrator emits structured events (for CLI, TUI, webhooks)
- CLI shows progressive results during pipeline run
- Foundation for `dataraum-ui` real-time updates

---

## Alternative Channels Investigated (Dead Ends)

Exhaustive testing of push alternatives (2026-02-28) confirmed that SEP-1686 is
the only viable path. All other channels were tested via
[`mcp-client-probe`](/mcp-client-probe/) and found non-functional in Claude clients.

### What Was Tested

| Channel | How tested | Claude Desktop | Claude Code |
|---|---|---|---|
| `notifications/tasks/status` | `task.update_status()` during tool call | Not surfaced (`tasks: None`) | Not surfaced |
| `notifications/progress` | `send_progress_notification()` with fabricated token | Not surfaced (no `progressToken` sent) | Not surfaced |
| `notifications/message` | `send_log_message()` during tool call | Sent over wire, not shown in UI | Not shown |
| MCP Apps (`ui://`) | Resource with `text/html;profile=mcp-app` | Fetched but rendering broken | No rendering (CLI) |
| Custom connectors | HTTPS endpoint as connector URL | Impossible: connection from Anthropic servers, not localhost. Self-signed certs rejected. | N/A |

### Why Push Is Blocked

All server-initiated push channels are blocked in Claude clients — likely by
design. Allowing MCP servers to push content into the LLM's context
mid-conversation would be a **prompt injection vector**: any server could inject
instructions the LLM acts on without user or LLM consent.

SEP-1686 solves this architecturally: the **host application** polls `tasks/get`
on its own schedule, decides what to surface to the LLM, and maintains control.
No unsolicited server→LLM content.

See also [MCP #117](https://github.com/modelcontextprotocol/modelcontextprotocol/issues/117)
for the open discussion on streaming tool results (future spec work).

### Probe Details

Claude Desktop (`claude-ai v0.1.0`, protocol `2025-11-25`) over stdio:
- Declares `extensions: {io.modelcontextprotocol/ui: {mimeTypes: [text/html;profile=mcp-app]}}`
- Declares NO tasks, sampling, elicitation, roots, or progress capabilities
- Sends NO `_meta` with `modelcontextprotocol.io/task` on tool calls
- `experimental.is_task` always `false` despite server enabling tasks
- No `progressToken` sent

Claude Code (`claude-code v2.1.62`, protocol `2025-11-25`) over stdio:
- Same: no tasks, no progress, no `_meta`
- [Issue #18617](https://github.com/anthropics/claude-code/issues/18617) tracks
  SEP-1686 client support (open, stale)

Custom connectors: connection originates from **Anthropic's servers** (published
static IPs), not the user's machine. `localhost` unreachable, self-signed certs
rejected. Designed for cloud SaaS, not local tools.

---

## Open Questions

1. ~~**MCP protocol support for streaming**~~ **ANSWERED**: SEP-1686 is the
   accepted standard. Intermediate results and push notifications are listed as
   future work in the SEP. No client implements it yet.

2. **Concurrent tool calls during pipeline**: If `get_actions` is called while
   `graph_execution` is still running, are there SQLite contention issues?
   Likely fine — WAL mode handles concurrent reads. But need to verify the MCP
   server can handle a second tool call while `analyze` is still running. The
   MCP spec allows multiple concurrent requests.

3. **Action execution during pipeline**: If an agent executes a `document_unit`
   action (writes metadata back to DB) while the pipeline is running, could that
   interfere with running phases? SQLite WAL handles concurrent writes with
   busy_timeout, but semantic conflicts (e.g., phase overwrites user-provided
   metadata) need careful thought.

4. ~~**Event granularity**~~ **ANSWERED**: Per-phase. That's when capabilities
   change and new tools become available. Sub-phase granularity adds complexity
   without changing what the consumer can do.

5. **UI integration**: How does `dataraum-ui` consume events? WebSocket? SSE?
   Or does it poll the DB? For Streamable HTTP transport (if we add it later),
   SSE is built into the MCP spec.

6. ~~**Client behavior on progress**~~ **ANSWERED (2026-02-28)**: No Claude client
   surfaces any push notification. SEP-1686 is the accepted solution, but no
   client implements the client side yet.

7. ~~**MCP Apps as alternative UI**~~ **ANSWERED (2026-02-28)**: Tested via
   `mcp-client-probe`. Claude Desktop fetches `ui://` resources but doesn't
   render them properly. Not viable for local MCP servers today.

8. ~~**Custom connectors for push**~~ **ANSWERED (2026-02-28)**: Connection
   originates from Anthropic's servers. localhost unreachable, self-signed certs
   rejected. Dead end for local tools.

9. **When will clients implement SEP-1686?** Unknown. Claude Code
   [#18617](https://github.com/anthropics/claude-code/issues/18617) is open but
   stale. FastMCP server-side is ready. The spec is Final. We should be ready
   when clients catch up.
