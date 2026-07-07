# ADR-0015 — Charting library for agent-authored report charts: Vega-Lite

- **Status:** Accepted (deviation: the shipped gate validates via a zod subset + column check + `vega-lite compile()` — `cockpit/src/charts/validate.ts` — instead of the bundled 1.88 MB JSON Schema; same gate intent, cheaper mechanism)
- **Date:** 2026-06-24
- **Ticket:** DAT-626 (entry-criteria spike), epic DAT-623 (Cockpit Reports)
- **Design doc:** Confluence DD/40337410 (Cockpit Reports)

## Context

A cockpit report is a frozen `{ SQL + chart config + summary }` over **live** DuckDB data (epic
DAT-623). DAT-626 adds the chart. The author is an **LLM agent**, and the config must be a
**serializable, validatable JSON** artifact: emitted via a forced-tool call, frozen in
`cockpit_db`, re-rendered in React over data that is re-run on every open ("data is live, never
frozen"). The deciding axis is therefore **agent-generatability of a storable, validatable config**
— not developer ergonomics or aesthetics, the axis most library comparisons use. That single
requirement disqualifies every code/JSX library (Recharts, Observable Plot, Nivo, Victory, visx):
their "config" is code, not data. Four serializable-JSON candidates remain: Vega-Lite, ECharts,
Plotly.js, and Mosaic/vgplot.

## Decision

Use **Vega-Lite** (`vega` + `vega-lite`, no wrapper dep). The frozen config carries a **named-data
reference** (`data:{name:"table"}`) — encoding is frozen, rows are bound live at render — and the
agent authors it from **`columns+types` only** (never the rows). Mint via a **forced-tool with a
thin zod subset** (`{mark, encoding:{x,y,color?,…}}`, fixed keys, no `z.record`), then resolve to a
full spec and run a **validate-and-repair gate** against the bundled Vega-Lite JSON Schema before
freezing. Render client-side (SSR-guarded) via `compile(spec) → new vega.View(...)`.

## Consequences

- **Validatable + low-context by construction.** Vega-Lite is the only candidate with an official
  JSON Schema (draft-07, 458 defs), so a malformed emission is caught before it is frozen. Named-data
  + author-from-schema keep rows out of *both* the frozen config and the LLM context — proven in the
  spike: the long-format multi-series case is authored with `color:{field:"region"}` and **zero data
  values**, where ECharts needs the distinct region values it cannot see from the schema.
- **The validate-and-repair gate is load-bearing, not optional** — published systems (VegaChat) only
  reach ~0% invalid-spec rate *with* it. **Do not inline the full 1.88 MB schema into the tool**;
  force the thin subset and validate the resolved spec post-hoc.
- **Retires the alternatives:** ECharts (no official schema → no pre-freeze validation; data-coupled
  multi-series), Plotly.js (clean JSON but ~1.4 MB bundle + Python-side headless path), and
  image-URL chart MCP servers (return a PNG, not a re-renderable spec).
- **Mosaic/vgplot is the deliberate not-yet.** It is the only DuckDB-native option (spec references
  SQL/tables; rows never materialize in the config or context via Arrow push-down) — but that edge is
  already neutralized here by named-data + author-from-schema, and its residual benefit (push-down at
  interactive-dashboard scale: cross-filter/brush over millions of points) is not the DAT-626 use
  case (a single frozen widget over a ≤~50-row aggregate). It also has the lowest LLM prior (no
  NL→vgplot generator exists) and is pre-1.0 (v0.28.1). **Revisit Mosaic if/when the cockpit builds
  interactive dashboards** (the "reports = widget library" direction).
- **No head-to-head LLM-validity benchmark exists** across these libraries; the Vega-Lite edge rests
  on training-prevalence evidence (VisEval: invalid-rate tracks training-data prevalence) plus its
  status as the de-facto NL2VIS target. The validate-and-repair gate is the hedge against that
  residual uncertainty.
