# DataRaum Plugin — Product Roadmap

*Based on product review session, Feb 2026. Revised Feb 23, 2026.*

---

## Overview

This roadmap covers the **MCP plugin** — the Claude Desktop integration. It is one of several projects. Items that belong elsewhere are scoped into their own project definitions.

**Current state:** 6 MCP tools (`analyze`, `get_context`, `get_entropy`, `evaluate_contract`, `query`, `get_actions`) and 6 skills. The `analyze` tool returns immediately with background execution; `get_context` polls for progress.

### Related Projects (priority order)

| # | Project | Scope | Why this order |
|---|---|---|---|
| 1 | [Release & Productization](projects/release.md) | CI, PyPI publish, plugin repo, Zensical docs, GitHub Issues, Claude Desktop | Foundation — nothing else ships without this |
| 2 | [Data Fixes](projects/fixes.md) | Persistent state, resolve workflow, fixes ledger, re-application | Highest product value — semantic pinning solves measured LLM reproducibility problem |
| 3 | [Onboarding](projects/onboarding.md) | Source configuration, data backends (DuckDB extensions), multi-source management | Unblocks non-developer users and database/cloud sources |
| 4 | [Quality UI](projects/quality-ui.md) | Web dashboard, report export, trend charts — lives in `dataraum-ui` | Visualization layer, depends on fixes for interesting data to display |
| 5 | [Incremental Imports](projects/incremental-imports.md) | Change detection, partial re-analysis, run comparison | Optimization — full pipeline works, just slow. Value grows after fixes exist |

---

## Pipeline Performance Baseline

From benchmarks (Feb 2026, financial dataset, 41 tables):

| Phase group | Time | % of total |
|---|---|---|
| Structural (import → correlations) | 4.7s | 0.7% |
| semantic | 84s | 12% |
| slicing + slice_analysis | 17s | 2.5% |
| quality_summary | 51s | 7.4% |
| **entropy_interpretation** | **539s** | **78%** |
| **Total** | **693s (~11.5 min)** | **100%** |

The structural core is fully reproducible across runs. LLM-decided areas (entity type classification, slice dimension selection, quality grading) vary between runs. Pinning the semantic layer after human review via the [fixes ledger](projects/fixes.md) eliminates this variance.

Note: LLM call counters show 0 in phase metadata — counters may not be wired to actual API calls yet.

---

## Completed

### Fix the analyze UX (timeout problem) — DONE

- `analyze` returns immediately. Pipeline runs in background via `asyncio.create_task(asyncio.to_thread(...))`.
- `get_context` checks `PipelineRun.status`. If `status == "running"`, returns progress (completed/total phases, currently running phase names) instead of partial context.
- MCP Task API path preserved (`taskSupport="optional"`) for future client support.
- Tool descriptions and `analyze/SKILL.md` document the polling pattern.

---

## Plugin-Scope Work

What remains in the plugin project is improving the existing tools' output quality.

### 1. Improve output formatting

The current Markdown output is functional but dense. Since Claude Desktop renders Markdown (not HTML), the improvement is better-structured Markdown.

**Implementation:** Update `mcp/formatters.py`:
- **Actions**: Group by type prefix (`document_` / `investigate_` / `transform_`), priority markers, effort inline
- **Context**: Compact tables, quality inline markers
- **Entropy**: Letter grades (A–D) alongside scores, dimension one-liners
- **Contracts**: Lead with pass/fail verdict and score, then details

HTML output belongs in the [Quality UI](projects/quality-ui.md) project.

**Effort:** Low-Medium

### 2. Show example bad rows

Actions describe problems statistically ("34% nulls", "high outliers"). Users need to see actual data.

**Implementation:** New MCP tool `show_examples(action_key, table_name, limit=5)`. Executes DuckDB query to return rows matching the quality issue condition. The action's `parameters` dict already contains column names and thresholds — these translate to SQL WHERE clauses.

**Effort:** Low-Medium

---

## Implementation Summary

### New MCP Tools (this project)

| Tool | Purpose | Effort |
|---|---|---|
| `show_examples` | N real rows matching a quality issue | Low |

### Updated Skills

| Skill | Change | Effort |
|---|---|---|
| `actions` | Better Markdown grouping | Low |
| `context` | Compact format, inline markers | Low |
| `entropy` | Letter grades, dimension summaries | Low |
| `contracts` | Lead with verdict, compact layout | Low |

Items 1 and 2 are independent and can start now.

### What moved to other projects

| Item | Now lives in |
|---|---|
| Source configuration, data backends | [Onboarding](projects/onboarding.md) |
| Persistent state layer (DB models) | [Data Fixes](projects/fixes.md) |
| Guided fix workflow (`resolve` skill) | [Data Fixes](projects/fixes.md) |
| Progress dashboard entry point | [Data Fixes](projects/fixes.md) |
| Apply transforms / save cleaned dataset | [Data Fixes](projects/fixes.md) |
| Compare runs | [Incremental Imports](projects/incremental-imports.md) |
| HTML output / quality dashboard | [Quality UI](projects/quality-ui.md) |
| Quality score trend charts | [Quality UI](projects/quality-ui.md) |
| "What changed in my data?" | [Incremental Imports](projects/incremental-imports.md) |
