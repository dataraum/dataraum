# Dataraum UI Architecture — Summary

## One-Line Description

A hypermedia-driven conversational interface where users analyze AND fix their data, with the server (including AI agents) determining what actions are available at each step.

---

## Core Principle: HATEOAS

**Every response tells the user what they can do next.**

```
User: "Show me data quality issues"

Server returns:
┌─────────────────────────────────────────────────────────────┐
│ Found 3 issues in the orders table:                        │
│ • customer_id: 12% null values                             │
│ • shipping_date: 8% invalid dates                          │
│ • amount: 2 negative values                                │
│                                                            │
│ [Fix all issues] [Review customer_id] [Export report]      │  ← Server decides these
└─────────────────────────────────────────────────────────────┘

User clicks: [Review customer_id]

Server returns:
┌─────────────────────────────────────────────────────────────┐
│ 847 orders have null customer_id:                          │
│ • 92% are guest checkout                                   │
│ • 8% are import errors                                     │
│                                                            │
│ [Assign to GUEST] [Backfill from email] [Write custom SQL] │  ← Different actions now
└─────────────────────────────────────────────────────────────┘
```

The UI doesn't know what actions exist. The server determines them based on context.

---

## Tech Stack

| Layer | Choice | Why |
|-------|--------|-----|
| **Server** | FastAPI + Jinja2 | Python everywhere, async, type-safe |
| **CSS** | daisyUI + Tailwind | Pre-built components, dark mode, enterprise themes |
| **Hypermedia** | HTMX 4 | Native SSE, morphing, explicit inheritance |
| **Client state** | Alpine.js | Minimal, declarative, complements HTMX |
| **Data format** | Apache Arrow | Zero-copy streaming from DuckDB to browser |
| **Charts** | Vega-Lite + vega-loader-arrow | Declarative grammar, LLM-friendly, Arrow-native |
| **Tables** | regular-table + Arrow JS | Virtual DOM, Arrow-native, ~10KB |
| **Graphs** | Cytoscape.js | Handles large schemas, good layouts |
| **Code editing** | CodeMirror 6 | Modern, extensible |

### The Arrow Advantage

DuckDB returns Arrow natively. Both tables and charts consume Arrow directly:

```
DuckDB Query
     ↓ Arrow IPC (zero-copy)
Browser receives ArrayBuffer
     ↓
┌────────────────────┬────────────────────┐
│   regular-table    │     Vega-Lite      │
│   (data grids)     │     (charts)       │
│                    │                    │
│   Arrow JS slice   │  vega-loader-arrow │
└────────────────────┴────────────────────┘
```

No JSON serialization. Millions of rows feel instant.

---

## The One Screen

```
┌───────────────────────────────────────────────────────────────┐
│ Header                                                        │
├─────────────────┬─────────────────────────────────────────────┤
│ Context Panel   │ Conversation Canvas                         │
│                 │                                             │
│ • Schema        │ Messages, artifacts, editors stream here    │
│ • Entropy score │                                             │
│ • Active focus  │                                             │
│                 ├─────────────────────────────────────────────┤
│                 │ Input: [Ask your data...]  [Actions]        │
└─────────────────┴─────────────────────────────────────────────┘
```

Not multiple pages. One workspace that adapts to context.

---

## Key Differentiator: Analysis → Fix → Undo

Most tools are read-only. Dataraum lets users **fix** data through the same interface:

```
Analysis: "12% null values in customer_id"
     ↓
Fix action: [Backfill from email]
     ↓
Preview: "Will update 68 records. [Cancel] [Apply]"
     ↓
Result: "Done. Entropy improved 0.45 → 0.52. [Undo]"
     ↓
Undo: "Reverted 68 records."
```

Every fix is tracked, reversible, and updates the analysis in real-time.

---

## JS Islands (The Only JavaScript)

We use exactly 3 JavaScript web components:

1. **`<sql-editor>`** — CodeMirror for SQL/YAML editing
2. **`<vega-chart>`** — Vega-Lite for entropy radar, trends, distributions
3. **`<arrow-table>`** — regular-table + Arrow JS for data grids
4. **`<graph-viewer>`** — Cytoscape for schema/lineage graphs

Everything else is server-rendered HTML + HTMX + Alpine.

---

## CLI ↔ Web Sync

```
Terminal                          │ Browser (same session)
                                  │
$ dataraum connect sales.db       │ ← Opens automatically
Session: abc123                   │
Web UI: localhost:8000/s/abc123   │
                                  │
> show tables                     │ ┌─────────────────────┐
┌────────┬───────┐                │ │ CLI: show tables    │
│ name   │ rows  │                │ ├─────────────────────┤
├────────┼───────┤                │ │ [Interactive table] │
│ orders │ 50000 │                │ │ [Visualize] [Export]│
└────────┴───────┘                │ └─────────────────────┘
                                  │
> ask "fix the nulls"             │ Agent runs, both see result
```

Same session, same undo stack, real-time sync via SSE.

---

## Chart DSL: Agent-Friendly

AI agents generate a simplified DSL that compiles to Vega-Lite:

```python
# Agent generates this (simple, constrained)
{
  "type": "radar",
  "title": "Entropy by Dimension",
  "dimensions": ["completeness", "consistency", "accuracy"],
  "y": "score"
}

# Server compiles to Vega-Lite (validated via Altair)
# Browser renders via vega-embed + vega-loader-arrow
```

Same Arrow data feeds both tables and charts.

---

## Action Data Model

```python
@dataclass
class Action:
    id: str              # "fix-nulls-customer-id"
    rel: str             # "fix" | "analyze" | "export" | "undo"
    label: str           # "Fix null values"
    href: str            # "/sessions/abc/fix/nulls"
    method: str          # "POST"
    style: str           # "primary" | "destructive"
    confirm: str | None  # "This will update 847 rows"
```

Actions are data. Templates render them. Server derives them from context.

---

## Documents in This Package

| File | Purpose |
|------|---------|
| `01-architecture-overview.md` | Philosophy, diagrams, why HATEOAS |
| `02-technical-specification.md` | Data models, API design, code examples |
| `03-ui-components.md` | Templates, HTML structure, CSS patterns |
| `04-implementation-plan.md` | Week-by-week roadmap, milestones |
| `05-summary.md` | This document |

---

## Open Questions

1. Session expiry policy?
2. Undo history depth limit?
3. MCP integration timeline?
4. Semantic layer editing in v1?

---

## Next Step

Read `04-implementation-plan.md` and start with Phase 0: project structure and basic shell.
