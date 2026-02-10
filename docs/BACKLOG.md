# Backlog

Prioritized backlog for the dataraum-context project.

**Current work:** [plans/restructuring-plan.md](./plans/restructuring-plan.md) — module-by-module cleanup on `refactor/streamline` branch.

**Related:**
- [PROGRESS.md](./PROGRESS.md) - Completed work log
- [ENTROPY_IMPLEMENTATION_PLAN.md](./ENTROPY_IMPLEMENTATION_PLAN.md) - Entropy system architecture

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
| 14 | `analysis/temporal_slicing/` | **Next** |
| 15 | `analysis/topology/` | Pending |
| 16 | `entropy/` | Pending |
| 17 | `llm/` | Pending |
| 18 | `pipeline/` | Pending |
| — | `graphs/` + `query/` | **Out of scope** (re-introduce after pipeline is clean) |

### After All Modules (Part 4)
- [ ] Verify TUI screens with cleaned models
- [ ] Verify MCP tools with cleaned context assembly
- [ ] Dependency audit (pandas vs pyarrow, ripser/persim, ruptures, networkx)
- [ ] Retire stale docs, move completed specs to `docs/archive/`

---

## Deferred Work

Items that were identified during development but deferred to keep focus.

### Entropy Enhancements
- [ ] Medium-priority detectors (Pattern, Unit, Temporal, Range, Freshness)
- [ ] Compound risk YAML config (currently hardcoded)
- [ ] Threshold extraction to `config/entropy/thresholds.yaml`
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

## Completed (Historical)

All feature development through Priorities 1-4 is complete. See [PROGRESS.md](./PROGRESS.md) for details.

- ✅ Entropy foundation: models, detectors, compound risk, scoring
- ✅ Context integration: builder, prompt formatting, contracts
- ✅ Graph agent: entropy awareness, assumptions, query behavior
- ✅ LLM entropy interpretation: batch, fallback, dashboard models
- ✅ Pipeline orchestrator: 18 phases, DAG, checkpoints, CLI
- ✅ Project restructure: flattened layout, FastAPI removed, docs consolidated
- ✅ Topology simplification: slice-based only, temporal bottleneck distance
