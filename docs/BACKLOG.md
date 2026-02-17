# Backlog

Prioritized backlog for the dataraum-context project.

**Current work:** [plans/restructuring-plan.md](./plans/restructuring-plan.md) — module-by-module cleanup on `refactor/streamline` branch.

**Related:**
- [PROGRESS.md](./PROGRESS.md) - Completed work log
- [plans/testdata-and-calibration-roadmap.md](./plans/testdata-and-calibration-roadmap.md) - Next major phase

---

## Current Focus: Module-by-Module Streamlining

> See [plans/restructuring-plan.md](./plans/restructuring-plan.md) for full plan, checklist, and spec template.

Bottom-up cleanup: each module gets dead code removal, config streamlining, logging, tests, and a spec doc.

| # | Module | Status |
|---|--------|--------|
| 1 | `core/` + `storage/` | ✅ Done |
| 2 | `sources/` | ✅ Done |
| 3 | `analysis/typing/` | ✅ Done |
| 4 | `analysis/statistics/` | ✅ Done |
| 5 | `analysis/eligibility/` | ✅ Done |
| 6 | `analysis/relationships/` | ✅ Done |
| 7 | `analysis/correlation/` | ✅ Done |
| 8 | `analysis/semantic/` | ✅ Done |
| 9 | `analysis/temporal/` | ✅ Done |
| 10 | `analysis/validation/` | ✅ Done |
| 11 | `analysis/slicing/` | ✅ Done |
| 12 | `analysis/cycles/` | ✅ Done |
| 13 | `analysis/quality_summary/` | ✅ Done |
| 14 | `analysis/temporal_slicing/` | ✅ Done |
| 15 | `analysis/topology/` | ✅ Removed (TDA unused, relationships/graph_topology covers schema graph) |
| 16 | `entropy/` | ✅ ~95% done (spec, tests, logging, dead code all done; minor fallback cleanup remains) |
| 17 | `llm/` | ⚠️ ~70% done (spec + dead code done; missing: logging in error paths, test gaps) |
| 18 | `pipeline/` | ⚠️ ~75% done (spec + tests done; remaining: 30 print() in runner.py, minor fallbacks) |
| — | `graphs/` + `query/` | **Out of scope** (re-introduce after pipeline is clean) |

### After All Modules (Part 4)
- [ ] Verify TUI screens with cleaned models
- [ ] Verify MCP tools with cleaned context assembly
- [ ] Dependency audit (pandas vs pyarrow, ruptures, networkx)
- [ ] Retire stale docs, move completed specs to `docs/archive/`

---

## Deferred Work

Items that were identified during development but deferred to keep focus.

### Entropy Enhancements
- [ ] **Unit entropy currency not working in practice** — Architecture is wired (semantic prompt → `unit_source_column` → detector), but LLM never populates `unit_source_column`. All unit_entropy scores = 0.8 (missing). Needs: investigate semantic prompt or add explicit currency detection heuristic. *Medium effort, high value.*
- [ ] **Table-level interpretation** — Schema exists (`TableInterpretation`), but interpretation phase only processes columns. Needs `TableInterpretationInput`, prompt template, and phase changes. ~400-500 lines. *Medium effort, medium value.*
- [ ] **Contract violation text not business-focused** — Technical jargon instead of business language. Purely cosmetic prompt tuning. *Low effort, low priority. Batch with interpretation work.*
- [ ] **TypeDecision detector** — Measure type decision certainty (automatic vs fallback vs override). Data already in pipeline (94.8% automatic, 5.2% fallback). Most useful when manual overrides become common. *Low priority now.*
- [ ] Entropy history/trending (needs snapshot infrastructure)

### Interfaces
- [ ] MCP Server (4 tools: get_context, get_entropy, evaluate_contract, query)
- [ ] TUI enhancements (real-time progress, `--from-phase`, `--force-restart`)

### Agents (Out of Scope for Restructuring)
- [ ] Graph Agent: field mapping with entropy, multi-table execution, validation
- [ ] Query Agent: RAG-based query reuse
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
