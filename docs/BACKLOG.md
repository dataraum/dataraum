# Backlog

Prioritized backlog for the dataraum-context project.

**Current work:** Phase A — local, open-source MCP-first architecture.

**Related:**
- [PROGRESS.md](./PROGRESS.md) - Completed work log
- [plans/testdata-and-calibration-roadmap.md](./plans/testdata-and-calibration-roadmap.md) - Test data and calibration

---

## Current Focus

### Next Up
- [ ] Run e2e tests to verify cycle health scoring (`tests/e2e/test_cycle_health.py`)
- [ ] Verify TUI screens with cleaned models
- [ ] Verify MCP tools with cleaned context assembly
- [ ] Dependency audit (pandas vs pyarrow, ruptures, networkx)
- [ ] Docs cleanup: triage `docs/projects/`, swap `docs_new/` → `docs/`, update README + CLAUDE.md cross-refs
- [ ] Align ontology concepts ↔ standard_field vocabulary (audit)

### Completed: Cycle-Scoped Validations + Cycle Health Score ✅

Additive feature — no existing signatures changed.

| Step | Description | Status |
|------|-------------|--------|
| 1 | `relevant_cycles` field on `ValidationSpec` | ✅ Done |
| 2 | `relevant_cycles` added to all 7 finance validation YAMLs | ✅ Done |
| 3 | `get_validation_specs_for_cycles()` filter in validation config | ✅ Done |
| 4 | `cycles/health.py` — `compute_cycle_health()` composite scoring | ✅ Done |
| 5 | Unit tests (4 config filter + 4 health scoring) | ✅ Done |
| 6 | E2E test (`tests/e2e/test_cycle_health.py`) | ✅ Written, not yet run |

Exported from `dataraum.analysis.cycles`. Pure query function — no LLM calls, no DB writes, no pipeline phase. Available for CLI/dashboard integration later.

### Completed: Agent Refactoring ✅

> See [plans/agent-refactoring-plan.md](./plans/agent-refactoring-plan.md) for master plan.

All phases complete. Agents streamlined around vertical configuration, context loading fixed,
shared SQL infrastructure extracted (graph + query agents stay separate).

| Phase | Description | Status |
|-------|-------------|--------|
| A | Cycle agent rewrite + validation resolver enrichment | ✅ Done |
| B | VerticalConfig abstraction + quality metrics → entropy | ✅ Done |
| C | Validation results + full cycle data in GraphExecutionContext | ✅ Done |
| D | Shared SQL execution base, both agents refactored | ✅ Done |
| 0.6 | E2E validation tests (testdata → pipeline → verify) | ✅ Done |
| — | temporal_behavior propagated end-to-end (ontology → SQL prompt) | ✅ Done |
| — | Entropy detector calibration (temporal_drift, benford, outlier_rate) | ✅ Done |

### Previous: Module-by-Module Streamlining ✅

> See [plans/restructuring-plan.md](./plans/restructuring-plan.md) for full plan.

All 18 modules complete.

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
- [ ] **Unit entropy: data consistency check** — Current detector only checks metadata (is unit declared?). Should also detect mixed unit data (e.g., amounts in mixed currencies). The `mix_units` injection changes values but detector only reads `unit_source_column`. Needs: analyze measure values grouped by unit source column, detect inconsistent distributions. *Medium effort.*
- [ ] **Dimensional entropy: mutual exclusivity** — Accounting data has structural constraints (debit XOR credit). Breaking mutual exclusivity is real data corruption. Needs: quality summary to identify exclusivity patterns, dimensional_entropy detector to consume them. *Medium effort.*
- [ ] **Derived value detection after enriched_views** — Currently runs before semantic phase, per-table only. Running after enriched_views (which have cross-table joins) would find more derived column relationships. *Large effort.*
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
