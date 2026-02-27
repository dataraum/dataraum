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

### Pipeline Event System

```
Pipeline Orchestrator
    │
    ├── phase completes ──▶ PipelineEvent
    │                          ├── phase_name
    │                          ├── status (completed/failed/skipped)
    │                          ├── duration
    │                          ├── outputs (phase-specific summary)
    │                          └── capabilities (HATEOAS)
    │                                ├── tools_ready: [{name, status, summary}]
    │                                ├── tools_partial: [{name, waiting_for, available_data}]
    │                                └── tools_blocked: [{name, waiting_for}]
    │
    ├── EventBus / callback
    │       │
    │       ├──▶ MCP Server (notify connected client)
    │       ├──▶ CLI (update progress display)
    │       ├──▶ Webhook (external integrations)
    │       └──▶ Log (structured event log for replay)
    │
    └── (phases continue running in parallel)
```

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
        soft_blocks=[],  # Fully ready once hard blocks complete
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

### Event Examples

After `entropy_interpretation` completes (~458s):

```json
{
  "event": "phase_completed",
  "phase": "entropy_interpretation",
  "duration_s": 158.9,
  "outputs": {
    "columns_interpreted": 47,
    "tables_interpreted": 8,
    "actions_generated": 38
  },
  "capabilities": {
    "ready": [
      {
        "tool": "get_entropy",
        "description": "Full entropy analysis with interpretations",
        "summary": "47 columns analyzed, 8 tables, 12 dimensions"
      },
      {
        "tool": "get_actions",
        "description": "Resolution actions ready for review",
        "summary": {"high_priority": 5, "medium": 12, "low": 21}
      },
      {
        "tool": "evaluate_contract",
        "description": "Data quality contracts can be evaluated"
      }
    ],
    "partial": [
      {
        "tool": "query",
        "status": "partial",
        "available": "SQL queries against typed tables with semantic context",
        "waiting_for": ["graph_execution"],
        "missing": "Calculated metrics not yet available"
      }
    ],
    "blocked": []
  }
}
```

After `graph_execution` completes (~610s):

```json
{
  "event": "pipeline_completed",
  "total_duration_s": 610.2,
  "capabilities": {
    "ready": [
      {"tool": "query", "description": "Full query capability with calculated metrics"}
    ]
  }
}
```

### MCP Integration: Progress Notifications

The MCP spec (2025-06-18) has built-in progress tracking via `notifications/progress`.
This works over **stdio** — no transport change needed.

**How it works:**
1. Client sends `analyze` request with `_meta.progressToken`
2. Server sends `notifications/progress` as phases complete
3. Client receives updates, can show progress or act on them
4. Server sends final response when pipeline completes

```json
// Client request
{
  "jsonrpc": "2.0", "id": 1,
  "method": "tools/call",
  "params": {
    "name": "analyze",
    "arguments": {"path": "/data"},
    "_meta": {"progressToken": "run-abc123"}
  }
}

// Server notification after entropy_interpretation completes
{
  "jsonrpc": "2.0",
  "method": "notifications/progress",
  "params": {
    "progressToken": "run-abc123",
    "progress": 18,
    "total": 19,
    "message": "entropy_interpretation complete. get_entropy, get_actions now available (38 actions across 8 tables). Waiting for: graph_execution"
  }
}
```

The `message` field carries HATEOAS-like capability information as human-readable
text. The LLM client (Claude Desktop, Cursor) can parse this to decide whether to
call `get_actions` immediately or wait.

**Richer capability data:** The `message` is a string, but we could encode structured
capability metadata as JSON in the message, or use a custom notification method
alongside the standard progress notification. The spec allows server-initiated
JSON-RPC notifications over stdio at any time.

**Key constraint:** Progress notifications only flow while the `analyze` tool call
is active. Once the response is sent, no more notifications. This is fine — the
pipeline is the long-running operation, and all notifications happen during it.

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

## Interaction Model: Agent Collaboration

The end goal is a workflow like this:

```
User: "Analyze my financial data"

[Pipeline starts, phases run...]

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

## Implementation Phases

### Phase 1: Dependency fix (S-sized, immediate win)
- Remove `entropy_interpretation` from `graph_execution` dependencies
- Replace with `entropy`
- Verify with tests that graph execution still works
- ~152s wall time savings, zero risk

### Phase 2: Tool readiness model (M-sized)
- Add `ToolReadiness` declarations to MCP tools
- MCP tools return readiness metadata (what's available, what's pending)
- `get_actions` / `get_entropy` responses include `pipeline_status` field
- No event system yet — just metadata on responses

### Phase 3: Pipeline event system (M-sized)
- Add event callback to pipeline orchestrator
- Events emitted on phase completion with capability summaries
- Event bus supports multiple listeners (log, callback, MCP)
- CLI shows progressive results during pipeline run

### Phase 4: MCP streaming (L-sized)
- MCP `analyze` tool sends progress notifications
- Connected client receives capability events
- Agent can call tools as they become ready
- Full progressive delivery loop

---

## Open Questions

1. ~~**MCP protocol support for streaming**~~ **ANSWERED**: Yes. MCP spec (2025-06-18)
   has `notifications/progress` with `progressToken`. Works over stdio. Server sends
   progress notifications during a tool call. Client receives them as JSON-RPC
   notifications on stdout. No transport change needed.

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

4. **Event granularity**: Should we emit events per-phase, per-batch (within
   entropy_interpretation), or per-action? Per-phase is the natural boundary
   since that's when data becomes available and capabilities change.

5. **UI integration**: How does `dataraum-ui` consume events? WebSocket? SSE?
   Or does it poll the DB? For Streamable HTTP transport (if we add it later),
   SSE is built into the MCP spec.

6. **Client behavior on progress**: Do Claude Desktop and Cursor actually surface
   `notifications/progress` messages to the LLM? Or do they only show them in a
   UI progress bar? This determines whether the agent can autonomously act on
   capability changes, or if we need a different mechanism.
