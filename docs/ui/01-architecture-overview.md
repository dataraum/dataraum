# Dataraum UI Architecture: Hypermedia-Driven Data Engineering

## Executive Summary

This document describes an architecture for dataraum's user interface that treats the UI as a **hypermedia application** where the server—including AI agents—controls available actions based on context. Users can both **analyze** and **fix** their data interactively through a conversational interface that surfaces contextual actions.

The key principle: **every response tells the user what they can do next**.

---

## Core Philosophy

### HATEOAS for Data Engineering

HATEOAS (Hypermedia As The Engine Of Application State) means the server returns not just data, but also the valid actions that can be taken on that data. Applied to data engineering:

```
Traditional API approach:
  GET /datasets/123/entropy → { "score": 0.65, "issues": [...] }
  (Client must know what endpoints exist, hardcode buttons)

HATEOAS approach:
  GET /datasets/123/entropy → {
    "score": 0.65,
    "issues": [...],
    "_actions": [
      { "rel": "fix-nulls", "href": "/datasets/123/fix/nulls", "method": "POST" },
      { "rel": "view-details", "href": "/datasets/123/entropy/details" },
      { "rel": "export", "href": "/datasets/123/export" }
    ]
  }
  (Server decides what's possible, client renders affordances)
```

This is powerful for dataraum because:

1. **Context-aware actions**: A column with 5% nulls shows "Review nulls". A column with 80% nulls shows "Drop column or impute?".

2. **Progressive disclosure**: Users see only relevant actions, not every possible feature.

3. **Agent-driven discovery**: The AI agent can surface actions the user didn't know existed.

4. **Safe mutations**: Destructive actions require confirmation; the server controls what's offered.

5. **Unified interface**: Same actions available via web UI, CLI, and MCP—derived from same logic.

### Analysis AND Fixing

The UI is not read-only. Users can:

| Analysis Action | Leads to Fix Action |
|-----------------|---------------------|
| "Show me data quality issues" | "Apply suggested fixes" |
| "Why is this column's entropy high?" | "Standardize values" / "Add validation rule" |
| "Find duplicate records" | "Merge duplicates" / "Mark as canonical" |
| "Show schema inconsistencies" | "Update schema" / "Add foreign key" |
| "Explain this query's performance" | "Create index" / "Rewrite query" |

Every analysis surfaces potential fixes. Every fix updates the analysis.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              Clients                                    │
├─────────────────┬─────────────────┬─────────────────┬───────────────────┤
│    Web UI       │      CLI        │   MCP Server    │   Notebooks       │
│  (Browser)      │   (Terminal)    │   (Claude, etc) │   (Marimo, etc)   │
│                 │                 │                 │                   │
│  HTMX + Alpine  │   Rich/Textual  │   JSON + Tools  │   Python API      │
└────────┬────────┴────────┬────────┴────────┬────────┴─────────┬─────────┘
         │                 │                 │                  │
         │    All share same session state and action space     │
         │                 │                 │                  │
         └─────────────────┴────────┬────────┴──────────────────┘
                                    │
                          ┌─────────▼─────────┐
                          │   FastAPI Server  │
                          │                   │
                          │  Content Negotiation:
                          │  Accept: text/html → Templates
                          │  Accept: application/json → JSON
                          │  Accept: arrow → Arrow IPC
                          └─────────┬─────────┘
                                    │
         ┌──────────────────────────┼──────────────────────────┐
         │                          │                          │
         ▼                          ▼                          ▼
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│  Agent Engine   │      │ Action Deriver  │      │  Session Store  │
│                 │      │                 │      │                 │
│  - Tool calls   │      │  - Analyzes     │      │  - Conversation │
│  - LLM routing  │      │    context      │      │  - Artifacts    │
│  - Streaming    │      │  - Returns      │      │  - Undo stack   │
│                 │      │    valid actions│      │                 │
└────────┬────────┘      └────────┬────────┘      └─────────────────┘
         │                        │
         └────────────┬───────────┘
                      │
                      ▼
         ┌─────────────────────────┐
         │      DuckDB Engine      │
         │                         │
         │  Returns Arrow IPC      │
         │  (zero-copy to browser) │
         └─────────────────────────┘
```

---

## The Unified Arrow Data Layer

A key architectural decision: **Apache Arrow as the universal data format**.

```
┌─────────────────────────────────────────────────────────────────┐
│                         DuckDB Query                            │
│                              │                                  │
│                    Arrow IPC (zero-copy)                        │
│                              │                                  │
│                    ┌─────────▼─────────┐                        │
│                    │   ArrayBuffer     │                        │
│                    │   (in browser)    │                        │
│                    └─────────┬─────────┘                        │
│                              │                                  │
│              ┌───────────────┼───────────────┐                  │
│              │               │               │                  │
│              ▼               ▼               ▼                  │
│      ┌───────────────┐ ┌───────────────┐ ┌─────────────────┐    │
│      │ regular-table │ │   Vega-Lite   │ │  Export/Filter  │    │
│      │  (data grid)  │ │   (charts)    │ │                 │    │
│      │               │ │               │ │                 │    │
│      │ Arrow JS      │ │ vega-loader-  │ │  Arrow JS       │    │
│      │ setDataListener│ │ arrow        │ │  .filter()      │    │
│      └───────────────┘ └───────────────┘ └─────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

**Why Arrow everywhere:**

| Benefit | Description |
|---------|-------------|
| **Zero serialization** | No JSON.parse/stringify overhead |
| **Columnar format** | Matches analytical query patterns |
| **Shared memory** | Same buffer feeds tables and charts |
| **DuckDB native** | Zero-copy from query engine |
| **Streaming** | IPC format supports chunked transfer |
| **Type preservation** | Dates, decimals, nulls handled correctly |

**Libraries:**
- `regular-table` (~10KB) — Virtual DOM table with async data model
- `vega-loader-arrow` — Arrow format parser for Vega/Vega-Lite
- `apache-arrow` — Core Arrow JS library for filtering/export

---

## Technology Stack

| Layer | Technology | Rationale |
|-------|------------|-----------|
| **Server** | FastAPI + Jinja2 | Python everywhere, async, type-safe |
| **CSS** | daisyUI + Tailwind | Pre-built components, dark mode, enterprise themes |
| **Hypermedia** | HTMX 4 | Native SSE, morphing, `<hx-partial>` |
| **Client state** | Alpine.js | Minimal, declarative |
| **Data format** | Apache Arrow | Zero-copy from DuckDB to browser |
| **Charts** | Vega-Lite + vega-loader-arrow | Declarative grammar, LLM-friendly |
| **Tables** | regular-table + Arrow JS | Virtual DOM, async model, ~10KB |
| **Graphs** | Cytoscape.js | Schema visualization, layouts |
| **Code editing** | CodeMirror 6 | SQL/YAML modes |

### What We're NOT Using

| Technology | Why Not |
|------------|---------|
| React/Vue/Svelte | Build complexity; HTMX + Alpine sufficient |
| GraphQL | Over-engineering; REST + HATEOAS simpler |
| WebSocket | SSE sufficient for streaming |
| ECharts/Chart.js | Not declarative; Vega-Lite better for LLM generation |
| AG Grid/Handsontable | Heavy, commercial; regular-table lighter |
| Node.js runtime | Python serves everything; Tailwind build-only |

---

## The Conversational Interface

### Message Flow

```
User types: "Show me data quality issues in the orders table"
                    │
                    ▼
┌─────────────────────────────────────────────────────────────┐
│ Agent analyzes request, calls tools:                        │
│   - entropy_report(table="orders")                          │
│   - column_analysis(table="orders")                         │
└─────────────────────────────────────────────────────────────┘
                    │
                    ▼ SSE stream
┌─────────────────────────────────────────────────────────────┐
│ Server streams response:                                    │
│                                                             │
│   data: <article>Analyzing orders table...</article>        │
│                                                             │
│   data: <figure class="artifact-entropy">                   │
│           [Entropy radar chart - Arrow data]                │
│         </figure>                                           │
│                                                             │
│   data: <figure class="artifact-table">                     │
│           [Issues table - Arrow data]                       │
│         </figure>                                           │
│                                                             │
│   data: <hx-partial hx-target="#actions">                   │
│           <button hx-post="/fix/nulls">Fix nulls</button>   │
│           <button hx-post="/fix/types">Fix types</button>   │
│         </hx-partial>                                       │
└─────────────────────────────────────────────────────────────┘
```

### Artifact Types

| Type | Visualization | Data Format |
|------|---------------|-------------|
| **Table** | regular-table (virtual scroll) | Arrow IPC |
| **Chart** | Vega-Lite (via vega-embed) | Arrow IPC via vega-loader-arrow |
| **Entropy Report** | Radar chart + issues table | Arrow IPC |
| **Schema Graph** | Cytoscape.js | JSON (small, topology only) |
| **Code** | CodeMirror (read-only or editable) | Text |
| **Error** | Alert component | JSON |

---

## Action System

### Action Derivation

The server determines available actions based on context:

```python
def derive_actions(session: Session, result: AnalysisResult) -> list[Action]:
    actions = []
    
    # Context-aware action generation
    if result.null_percentage > 0.5:
        actions.append(Action(
            rel="fix-drop-column",
            label="Drop column (>50% null)",
            href=f"/sessions/{session.id}/fix/drop/{result.column}",
            style="destructive",
            confirm=f"Drop {result.column}? This affects {result.row_count} rows."
        ))
    elif result.null_percentage > 0:
        actions.append(Action(
            rel="fix-nulls",
            label=f"Fix {result.null_count} nulls",
            href=f"/sessions/{session.id}/fix/nulls/{result.column}",
            style="primary"
        ))
    
    # Pattern-based suggestions
    if result.detected_pattern == "email":
        actions.append(Action(
            rel="validate-email",
            label="Add email validation",
            href=f"/sessions/{session.id}/validate/email/{result.column}"
        ))
    
    return actions
```

### Action Execution

```
User clicks: [Fix 847 nulls]
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│ POST /sessions/abc/fix/nulls/customer_id                    │
│                                                             │
│ Server:                                                     │
│   1. Generate preview SQL                                   │
│   2. Count affected rows                                    │
│   3. Return confirmation dialog                             │
└─────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│ Response (streamed):                                        │
│                                                             │
│   <dialog open>                                             │
│     Preview: UPDATE orders SET customer_id = 'GUEST'        │
│              WHERE customer_id IS NULL                      │
│     Affects: 847 rows                                       │
│     [Cancel] [Apply]                                        │
│   </dialog>                                                 │
└─────────────────────────────────────────────────────────────┘
         │
         ▼ User clicks [Apply]
┌─────────────────────────────────────────────────────────────┐
│ Server:                                                     │
│   1. Execute SQL                                            │
│   2. Record change for undo                                 │
│   3. Recompute entropy                                      │
│   4. Stream updated UI                                      │
└─────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│ Response (multi-target via hx-partial):                     │
│                                                             │
│   <hx-partial hx-target="#conversation">                    │
│     <article>Fixed 847 null values.</article>               │
│   </hx-partial>                                             │
│                                                             │
│   <hx-partial hx-target="#entropy-summary">                 │
│     <div class="score">0.72 (+0.07)</div>                   │
│   </hx-partial>                                             │
│                                                             │
│   <hx-partial hx-target="#actions">                         │
│     <button hx-post="/undo">Undo</button>                   │
│   </hx-partial>                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## Undo System

Every mutation is tracked and reversible:

```python
@dataclass
class Change:
    id: str
    timestamp: datetime
    action_id: str
    action_label: str
    
    # What changed
    table: str
    operation: Literal["INSERT", "UPDATE", "DELETE", "DDL"]
    affected_rows: int
    
    # How to undo
    undo_sql: str | None
    undo_snapshot: str | None  # Path to backup if complex
    
    # State
    committed: bool = False
    undone: bool = False
```

The undo stack is visible in the context panel and shared across CLI/Web.

---

## Chart Generation for LLMs

AI agents generate charts using a simplified DSL that compiles to Vega-Lite:

```python
# Agent generates this (constrained, simple)
{
  "type": "radar",
  "title": "Entropy by Dimension", 
  "dimensions": ["completeness", "consistency", "accuracy"],
  "y": "score"
}

# Server compiles to Vega-Lite (validated via Altair)
# Browser renders with vega-embed + vega-loader-arrow
```

**Why Vega-Lite:**
- Formal grammar (LLMs generate valid specs)
- Declarative (describe what, not how)
- Python validation via Altair
- Arrow-native via vega-loader-arrow
- Research-proven for LLM generation

---

## CLI ↔ Web Synchronization

Same session, same undo stack:

```
Terminal                          │ Browser
                                  │
$ dataraum connect sales.db       │ 
Session: abc123                   │ ← Auto-opens
                                  │
> analyze orders                  │ Shows entropy report
                                  │
> fix nulls --preview             │ Shows preview
                                  │
                                  │ User clicks [Apply] in browser
                                  │
> status                          │ 
Changes: 1 uncommitted            │ Shows same change
                                  │
> undo                            │ Both revert
```

Synchronization via:
1. Shared session store (Redis/SQLite)
2. SSE for real-time updates
3. Same action derivation logic

---

## Security Model

### Action Authorization

```python
def authorize_action(session: Session, action: Action) -> bool:
    # Only server-derived actions are valid
    valid_actions = derive_actions(session, session.last_result)
    if action.id not in [a.id for a in valid_actions]:
        return False
    
    # Check user permissions
    if action.rel in ["fix", "delete", "transform"]:
        if not session.user.can_write:
            return False
    
    return True
```

### SQL Safety

- User queries run read-only by default
- Write operations only through validated tools
- All mutations recorded for audit
- Connection credentials never exposed to client

---

## Performance Strategy

### Server-Side
- Async everywhere (FastAPI, async DB drivers)
- Arrow IPC streaming (no JSON overhead)
- Entropy computation caching
- Connection pooling

### Client-Side
- Arrow eliminates JSON parsing
- regular-table renders only visible rows
- Vega-Lite compiles once, updates data
- HTMX morphing preserves DOM state

### Data Transfer

| Approach | 100K rows |
|----------|-----------|
| JSON | ~15MB, 800ms parse |
| Arrow IPC | ~4MB, 50ms parse |
| Arrow + virtual scroll | ~4MB, <10ms to first paint |

---

## File Structure

```
dataraum-ui/
├── app/
│   ├── main.py              # FastAPI app
│   ├── routes/
│   │   ├── sessions.py      # Session management
│   │   ├── actions.py       # Action execution
│   │   ├── stream.py        # SSE streaming
│   │   └── data.py          # Arrow data endpoints
│   ├── models/
│   │   ├── session.py
│   │   ├── action.py
│   │   └── chart.py         # Chart DSL
│   ├── services/
│   │   ├── agent.py         # LLM agent
│   │   ├── actions.py       # Action derivation
│   │   └── entropy.py       # Entropy computation
│   └── templates/
│       ├── base.html
│       ├── session.html
│       ├── messages/
│       ├── artifacts/
│       └── partials/
├── static/
│   ├── css/
│   │   └── app.css          # Compiled Tailwind
│   └── js/
│       └── islands/
│           ├── arrow-table.ts
│           ├── vega-chart.ts
│           ├── graph-viewer.ts
│           └── sql-editor.ts
├── styles/
│   └── app.css              # Tailwind source
├── tailwind.config.js
└── package.json             # Build dependencies only
```

---

## Summary

| Principle | Implementation |
|-----------|----------------|
| **HATEOAS** | Server derives actions from context |
| **Hypermedia** | HTMX + SSE, no SPA complexity |
| **Arrow everywhere** | Unified data format for tables and charts |
| **LLM-friendly charts** | Simplified DSL → Vega-Lite |
| **Analysis + Fixing** | Every analysis surfaces actionable fixes |
| **Reversible** | Every mutation tracked, undoable |
| **Unified** | CLI, Web, MCP share same session |
