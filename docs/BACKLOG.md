# Backlog

Prioritized backlog for the dataraum-context project.

**Current work:** Phase A — local, open-source MCP-first architecture.

**Related:**
- [PROGRESS.md](./PROGRESS.md) - Completed work log
- [plans/testdata-and-calibration-roadmap.md](./plans/testdata-and-calibration-roadmap.md) - Test data and calibration

---

## Current Focus: Agent Refactoring (graphs/ + query/)

> See [plans/agent-refactoring-plan.md](./plans/agent-refactoring-plan.md) for master plan.

Streamline the four LLM agents around vertical configuration, fix context loading,
merge graph + query agents into unified DataAgent.

| Phase | Step | Description | Strategy | Status |
|-------|------|-------------|----------|--------|
| A | A0 | Audit + deep metadata evaluation | Research | ✅ Done |
| A | A1a | New cycle context builder (all pipeline metadata) | Rewrite | ✅ Done |
| A | A1b | New cycle agent (single-call, no tools) | Rewrite | ✅ Done |
| A | A2a | Enrich validation resolver | In-place | ✅ Done |
| A | A2b | Fix validation execution | In-place | ✅ Done |
| B | B1 | Create VerticalConfig abstraction | New code | ✅ Done |
| B | B2 | Extract quality metrics to entropy system | Move | ✅ Done |
| C | C1 | Surface validation results in GraphExecutionContext | Additive | **Next** |
| C | C2 | Forward full cycle data to context | Additive | Pending |
| C | C3 | Align ontology concepts ↔ standard_field vocabulary | Audit | Pending |
| D | D1 | Consolidate shared agent infrastructure | Refactor | Pending |
| D | D2 | Create unified DataAgent | Rewrite | Pending |
| D | D3 | Clean up old modules | Delete | Pending |

### Previous: Module-by-Module Streamlining ✅

> See [plans/restructuring-plan.md](./plans/restructuring-plan.md) for full plan.

All 18 modules complete. `graphs/` + `query/` were out of scope — now addressed above.

### After Agent Refactoring (Part 4)
- [ ] Verify TUI screens with cleaned models
- [ ] Verify MCP tools with cleaned context assembly
- [ ] Dependency audit (pandas vs pyarrow, ruptures, networkx)
- [ ] Retire stale docs, move completed specs to `docs/archive/`

---

## Roadmap

### Phase A — Local, Open-Source (NOW)

- [x] MCP server with 6 tools, plugin rewritten
- [x] `analyze` tool — run pipeline from MCP (no CLI required)
- [x] Parquet source type (DuckDB-native, strong types)
- [x] Plugin skills for `analyze`
- [ ] Tiered `get_context` detail levels (summary/standard/full) — needed before Connectors
- [ ] `list_sources` tool for multi-source workspaces
- [ ] Read-only mode for shared deployments
- [ ] Claude Code slash commands (`/project:context`, `/project:entropy`)
- [ ] PostgreSQL source loader

### Phase B — Remote MCP + Connectors Directory (Q3-Q4 2026)

- Streamable HTTP transport for remote MCP server (Python MCP SDK supports `FastMCP`)
- OAuth authentication layer for multi-user
- Submit to Claude Connectors Directory (50+ curated integrations)
- Token budget enforcement: tiered `get_context` mandatory, 25K token cap per tool result
- Desktop Extension packaging for easy install
- Data ingest from other connectors (Google Sheets → analyze flow via `data` parameter)

### Phase C — Cloud-Hosted Service (2027+)

- Hosted pipeline: upload data, analysis runs server-side
- Connector-native data sources: pull directly from Google Sheets, PostgreSQL, Salesforce
- Interactive artifacts: entropy charts, contract dashboards rendered inline in Claude Desktop
- Team workspaces with access control

### Key Insight: Claude Desktop Connectors

Connectors ARE remote MCP servers (HTTPS + OAuth). Building a remote version of our current stdio MCP server is the path to the Connectors Directory. Same 6 tools, different transport. The Python MCP SDK makes this straightforward — main work is auth and hosting, not protocol.

Data source integration via connectors: in Phase B, the `analyze` tool can accept a `data` parameter (CSV text from another connector). In Phase C, DataRaum pulls directly from source connectors. This eliminates the "bring your own file" step.

---

## Deferred Work

Items identified during development but deferred to keep focus.

### Entropy Enhancements
- [ ] **Unit entropy currency not working in practice** — Architecture is wired (semantic prompt → `unit_source_column` → detector), but LLM never populates `unit_source_column`. All unit_entropy scores = 0.8 (missing). Needs: investigate semantic prompt or add explicit currency detection heuristic. *Medium effort, high value.*
- [ ] **Table-level interpretation** — Schema exists (`TableInterpretation`), but interpretation phase only processes columns. Needs `TableInterpretationInput`, prompt template, and phase changes. ~400-500 lines. *Medium effort, medium value.*
- [ ] **Contract violation text not business-focused** — Technical jargon instead of business language. Purely cosmetic prompt tuning. *Low effort, low priority. Batch with interpretation work.*
- [ ] **TypeDecision detector** — Measure type decision certainty (automatic vs fallback vs override). Data already in pipeline (94.8% automatic, 5.2% fallback). Most useful when manual overrides become common. *Low priority now.*
- [ ] Entropy history/trending (needs snapshot infrastructure)

### Interfaces
- [ ] TUI enhancements (real-time progress, `--from-phase`, `--force-restart`)

### Agents
- [x] Graph Agent + Query Agent: Now addressed in Agent Refactoring plan (see Current Focus)
- [ ] Semantic Agent: entropy enrichment fields

### Infrastructure
- [ ] Database migrations (currently auto-create in dev)
- [ ] Wire `TemporalTopologyAnalysis` into `graphs/context.py`

---

## Completed / Resolved (Historical)

All feature development through Priorities 1-4 is complete. See [PROGRESS.md](./PROGRESS.md) for details.

- ✅ Entropy foundation: models, detectors, compound risk, scoring
- ✅ Context integration: builder, prompt formatting, contracts
- ✅ Graph agent: entropy awareness, assumptions, query behavior
- ✅ LLM entropy interpretation: batch, fallback, dashboard models
- ✅ Pipeline orchestrator: 18 phases, DAG, checkpoints, CLI
- ✅ Project restructure: flattened layout, FastAPI removed, docs consolidated
- ✅ Topology simplification: slice-based only, temporal bottleneck distance
- ✅ Entropy scoring fine-tuning: piecewise outlier scoring, weighted avg for relationship entropy, proportional join path scoring, empty layer normalization, compound risk gradient preservation, evidence self-identification, Benford detector, quality context completeness, LLM resolution action parameters
- ✅ Action taxonomy: `add_time_filter` → `transform_add_time_filter` prefix fix

### Resolved — Phase F Architecture Refactor

The Phase F plan (`docs/plans/entropy-phase-f-implementation.md`) documented 6 issues. **5 of 6 were resolved** during the restructuring work (modules 1-15). The plan is now largely obsolete:

| Issue | Status | Notes |
|-------|--------|-------|
| 1. Duplicate context building | ✅ Resolved | `query/agent.py` builds once via `build_for_query()` |
| 2. Dict copying instead of references | ✅ Resolved | Layered views (`EntropyForGraph`, `EntropyForQuery`, `EntropyForDashboard`) by design |
| 3. Evidence partially lost | ⚠️ Verify | Evidence preserved in `EntropyObject`, but confirm LLM interpreter receives full detector evidence |
| 4. Redundant profile classes | ✅ Resolved | Replaced with computed `ColumnSummary`/`TableSummary`/`RelationshipSummary` |
| 5. LLMContext/HumanContext unused | ✅ Resolved | Removed; replaced by `EntropyInterpretation` |
| 6. Typed tables not enforced | ✅ Resolved | Centralized in `EntropyRepository` with `enforce_typed=True` default |

### Resolved — Entropy Scoring Items
- ~~Compound risk YAML config (currently hardcoded)~~ → Config in `thresholds.yaml`
- ~~Threshold extraction to `config/entropy/thresholds.yaml`~~ → Done
- ~~Medium-priority detectors~~ → Benford detector added; Unit, Temporal already existed; Pattern/Range/Freshness not justified by data
