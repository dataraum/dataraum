# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-03-28

### Breaking Changes
- Pipeline reduced from 23 to 17 phases — gate phases, quality_summary, entropy_interpretation, graph_execution, quality_review, analysis_review removed
- `column_quality` detector retired (circular with BBN readiness)
- CLI commands removed: `tui`, `query`, `sources`, `dev inspect`, `dev reset`, `fix`. Only `run` and `dev {phases, context}` remain
- MCP tools redesigned: old tools (`analyze`, `get_context`, `get_quality`, `apply_fix`, `continue_pipeline`, `discover_sources`, `export`) replaced by 7 practitioner tools
- Table names now prefixed with `{source_name}__` (e.g., `medium__invoices`)
- Default output directory changed from `./pipeline_output` to `~/.dataraum/workspace/`
- `textual` dependency removed
- Python minimum version lowered from 3.14 to 3.12

### Added
- **MCP practitioner tools**: `look`, `measure`, `begin_session`, `end_session`, `query`, `run_sql`, `add_source`
- **Session lifecycle**: `begin_session` creates investigation session with contract selection, `end_session` archives workspace
- **Session resumption**: `begin_session` resumes existing session after server restart
- **Investigation trace**: `InvestigationSession` + `InvestigationStep` models for MCP audit trail
- **JSON/JSONL source support**: loaded as VARCHAR via DuckDB `read_json`
- **Directory source support**: `add_source` accepts directories, scans for supported files
- **Format rejection**: unsupported file formats rejected with clear error at `add_source`
- **Export from MCP tools**: `run_sql` and `query` support `export_format` (CSV, Parquet) via DuckDB COPY
- **Export sidecar**: metadata file alongside each export with SQL, confidence, assumptions
- **Display limit push-down**: `run_sql` and `query` push LIMIT to DuckDB, no unbounded fetchall
- **Truncation signaling**: responses include `row_count`, `rows_returned`, `truncated`, `hint`
- **Prerequisite check**: `begin_session` validates API key availability
- **Source sealing**: `add_source` blocked during active session
- **Root dir architecture**: `DATARAUM_HOME` env var, with `workspace/`, `archive/`, `exports/` co-located
- **Readiness at 3 levels**: column, table, and dataset readiness via BBN
- **Target filtering**: `measure` and `look` accept target parameter with short-name resolution
- **Python 3.12/3.13 support**: lowered from 3.14-only; free-threading is optional performance enhancement
- **CI matrix**: tests run on Python 3.12, 3.13, and 3.14
- **PyPI release automation**: GitHub Release triggers publish via OIDC trusted publisher
- **UTF-8 encoding detection**: clear error message for non-UTF-8 CSV files

### Changed
- Pipeline measures entropy but does not interpret — interpretation is interactive via MCP tools
- BBN readiness replaces LLM quality grades
- `measure_entropy()` replaces gate infrastructure for on-demand scoring
- Agents use tool-use pattern (LLM generates structured output via Pydantic tool schemas)
- `execute_sql_steps()` shared by graph and query agents
- Export rewritten: single `export_sql()` using DuckDB COPY (no Python materialization)
- Session state DB-derived (not closure variables)
- Import path unified: `_load_from_path` delegates to `_load_file_source`
- Path escaping fixed across all loaders (CSV, Parquet, JSON, directory discovery)
- `pgmpy` bumped to >=1.0.0, `torch` to >=2.8.0

### Removed
- Gate phases and gate infrastructure (`persist_gate_result`, `EXIT_CHECK` events, `Resolution`/`ResolutionAction`)
- `PhaseLog.entropy_scores` field
- `is_quality_gate` from Phase protocol
- `apply_fix` MCP tool
- CLI TUI, query, sources, dev inspect, dev reset commands
- `textual` dependency
- `export_query_result()`, `export_data()`, `_export_tool_result()` — replaced by `export_sql()`
- Dead LLM prompts: `entropy_interpretation.yaml`, `quality_summary_batch.yaml`

## [0.1.0] - 2025-12-01

### Added
- 18-phase analysis pipeline (staging, profiling, enrichment, quality, context)
- MCP server with 6 tools: `analyze`, `get_context`, `get_entropy`, `evaluate_contract`, `query`, `get_actions`
- Entropy system with uncertainty quantification across 8 dimensions
- Data readiness contracts (`aggregation_safe`, `executive_dashboard`, etc.)
- CLI (`dataraum`) with run, status, entropy, and contracts commands
- TUI for interactive pipeline monitoring
- Semantic analysis via LLM (Claude, OpenAI) or manual overrides
- Domain ontologies for financial, marketing, and custom verticals
- DuckDB compute engine with SQLite metadata storage
- Temporal analysis (granularity, gaps, seasonality, trends)
- Topological relationship detection and join path inference
- Statistical profiling (distributions, cardinality, null rates, patterns)
- Quality rule generation and scoring
- Privacy support via synthetic data generation (SDV)
- PostgreSQL backend option
