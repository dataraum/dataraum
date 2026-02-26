# Documentation Triage Findings

Intermediate document collecting issue candidates from docs/ triage.
These need verification against the actual codebase(s) before creating issues.

## Created Issues (Reviewed 2026-02-26)

Other repos discovered during review: dataraum-plugin (MCP plugin), dataraum-ui (web UI, design docs only), dataraum-testdata (synthetic data generation with entropy injection).

### GitHub Issues (public)

| # | Title | Status | Review Outcome |
|---|-------|--------|----------------|
| 7 | Extensible plugin system | **OPEN** | Verified: no entry points, no plugin registry |
| 8 | ~~Detect business cycle anomalies~~ | **CLOSED** | Already detected via cycles.yaml red flags config |
| 9 | Complete multi-table business cycle detection | **OPEN** | Verified: cross-table patterns missing from config |
| 10 | REST API for pipeline metadata | **OPEN (updated)** | MCP provides 6 tools covering this; REST may not be needed |
| 11 | ~~Web UI for exploring data quality~~ | **CLOSED** | Tracked in dedicated dataraum-ui repo |
| 12 | ~~Jupyter/Python API~~ | **CLOSED** | Already implemented: `from dataraum import Context` |

### Linear Issues (private)

| ID | Title | Status | Review Outcome |
|----|-------|--------|----------------|
| DAT-35 | Entropy bugs backlog | **OPEN (updated)** | Bugs #2,#4 FIXED; #7,#8,#11 remain |
| DAT-36 | ~~Externalize cycle prompts~~ | **CLOSED** | Done in Phase A1b refactoring |
| DAT-37 | Graph cache/filter/multi-table | **OPEN** | Cache invalidation + JOIN generation confirmed missing |
| DAT-38 | ~~Relationship context assembly~~ | **CLOSED** | Implemented in graph_topology.py + GraphExecutionContext |
| DAT-39 | ~~ContextDocument metrics~~ | **CLOSED** | GraphExecutionContext is already comprehensive |
| DAT-40 | Code cleanup: print→logging, indexes | **OPEN (updated)** | Dead code claim removed; print + indexes confirmed |
| DAT-41 | ~~Filter gen + formatters~~ | **CANCELLED** | quality/formatting/ never existed |
| DAT-42 | ~~API endpoints~~ | **CANCELLED** | No API module; MCP-first architecture |

### Remaining open issues: GH#7, GH#9, GH#10, DAT-35, DAT-37, DAT-40

---

## Phase 3: Specs Triage — Issue Candidates

Candidates collected from docs/specs/ roadmap sections. NOT yet created as issues.

### Per-spec findings (~110 NOT DONE roadmap items)

Items below are grouped by module. Many will consolidate into fewer issues.
Complexity: S=small, M=medium, L=large. Effort: S/M/H.

#### entropy.md (13 items)
- Fix quality context pipeline (raw_metrics pass-through)
- Dimension-keyed resolution actions
- Simplify detector heuristics (centralize evaluation)
- Wire Isolation Forest into entropy outlier detector
- Business-focused contract violation text (ontology integration)
- Self-identifying evidence in entropy objects (add column_name/table_name)
- Temporal wiring (quality summary temporal context)
- Topology persistence (TemporalTopologyAnalysis table)
- Detector plugin system (extensible registry)
- Cross-run trending (compare entropy across pipeline runs)
- Custom contract `extends` support
- Path to compliance / resolution cascade logic
- Graph/query agent contract integration

#### query.md (7 items)
- Entropy-aware response templates (implement in query agent)
- Externalize behavior config to YAML
- Assumption promotion & success tracking
- User feedback loop collection
- Multi-table join suggestions
- Query result caching
- SQL comments with entropy/assumption context

#### pipeline.md (5 items)
- Phase invalidation on upstream data change
- Progress callbacks to TUI/API
- Re-enable graph/query phases (blocked by agent refactor)
- Evaluate cross-table quality for entropy
- Migrate print() to logger() (~233 calls remain)

#### typing.md (8 items — 1 umbrella)
- Eliminate TypeCandidate/TypeDecision duplication via raw_column_id FK
- Add Parquet/PostgreSQL loaders to import phase
- LLM-assisted date format detection (umbrella):
  - Tier 1: DuckDB multi-format probing
  - Tier 2: LLM fallback + DateFormatAgent
  - Tier 3: Ambiguous date format entropy signals
  - Expand typing.yaml with strptime_format field
  - Generate COALESCE standardization expressions

#### semantic.md (3 items)
- Incremental re-analysis of specific tables
- Confidence calibration against ground truth
- Populate unit_source_column field from unit-defining dimension columns

#### graphs.md (10 items)
- Complete filter graph execution pipeline
- Multi-level graph dependency resolution
- Schema mapping inference from semantic annotations
- Slice-based GROUP BY in SQL generation
- Graph/schema hash for cache invalidation
- Wire formatters (statistical, temporal, topological) into GraphExecutionContext
- Refactor graph agent to async (consistency with query agent)
- Externalize EntropyBehaviorConfig to YAML
- MCP tools: get_metric_explanation(), list_metric_executions()
- Parameterized graph support (validation, safe substitution)

#### cycles.md (4 items)
- Cycle anomaly detection (overlaps GH#8)
- Anomaly config thresholds in cycles.yaml
- Externalize prompts (overlaps DAT-36)
- Cross-run cycle comparison

#### validation.md (5 items)
- Surface validation results in TUI/CLI/API/MCP
- Include validation status in context documents
- Cross-vertical validation specs (beyond finance)
- Transient failure retry logic for LLM calls
- Execution timeout for LLM-generated SQL

#### slicing.md (4 items)
- Extract name sanitization to shared utility
- Numeric/range-based slicing (currently categorical only)
- Cross-slice quality metrics comparison
- Cross-table slicing via enriched views

#### correlations.md (4 items)
- String transform detection (UPPER/LOWER/TRIM)
- Concatenation detection (col3 = col1 || col2)
- Composite FDs (multi-column determinants)
- Cross-source correlations

#### statistics.md (4 items)
- Distribution type detection (normal, log-normal, exponential)
- Shannon entropy per-column metric
- PK violation count metric
- Ordering properties (sorted, monotonic)

#### relationships.md (4 items)
- Context assembly for semantic agent (overlaps DAT-38)
- Composite key detection
- Name-based hint boost for join confidence
- Cross-source relationships

#### temporal.md (4 items)
- Stratified sampling strategy (replace Bernoulli)
- Calibration with testdata
- TUI integration for TemporalTableSummary
- Streaming/incremental temporal analysis

#### temporal_slicing.md (4 items)
- TemporalSliceConfig YAML defaults
- Level 5 UI/API exposure
- Entropy wiring for topology results
- Numeric drift detection (KS test, mean-shift, rolling window)

#### core-storage.md (6 items)
- CLI --config flag (currently env var only)
- PostgreSQL support testing
- Vertical-aware ontology loading
- Metrics export (JSON/Prometheus)
- Missing foreign keys (5 tables)
- Missing indexes (12 tables) — overlaps DAT-40

#### topology.md (6 items)
- DB persistence (TopologicalMetrics model)
- Multi-table topology analysis
- Formatter thresholds externalization to YAML
- Financial domain consolidation
- Column-level topology exposure
- Dependency audit (ripser/persim weight)

#### sources.md (5 items)
- SQLite loader
- PostgreSQL loader
- Numeric/date null handling from config
- Whitespace rules application
- Test data fixture verification

#### eligibility.md (1 item)
- Vertical-specific eligibility rules

#### llm.md (3 items)
- Provider extensibility (OpenAI/Ollama)
- Token tracking (aggregate usage)
- Prompt versioning

#### quality_summary.md (6 items)
- Vertical-specific scoring
- Trend tracking across runs
- Duplicate row detection
- Format validation metrics
- Statistical quality formatter
- Temporal quality formatter

### Overlap notes

Several spec items duplicate already-created issues:
- Cycle anomaly detection → overlaps GH#8
- Prompt externalization → overlaps DAT-36
- Cache invalidation, filter linking → overlaps DAT-37
- Context assembly → overlaps DAT-38
- Missing indexes/FKs → overlaps DAT-40
- Print→logging → overlaps DAT-40
- PostgreSQL loader → could be part of GH#7 (extensibility)

---

## Phase 4: Top-Level Files — Issue Candidates

### Copied to docs_new/ (user-facing content)
- ARCHITECTURE.md → docs_new/architecture.md (edit later with REFERENCE.md content)
- DATA_MODEL.md → docs_new/data-model.md
- CLI.md → docs_new/cli.md
- MCP_SETUP.md → docs_new/mcp-setup.md

### Deleted (no issue extraction needed)
- PROGRESS.md — internal session log, no extraction needed
- INTERFACES.md — code is source of truth, outdated code samples
- METRICS_ANALYSIS.md — reference inventory, key concepts folded into entropy spec

### Issue candidates (NOT yet created)
- **bayesian-entropy-network.md** — Research concept: probabilistic entropy dependency modeling via Bayesian Network over 18 sub-dimensions. NOT IMPLEMENTED. Candidate for: GitHub research issue "Probabilistic entropy dependency modeling"
- **dataraum-plugin-roadmap.md** — MCP plugin improvements: better formatting, show_examples tool. Partially done (analyze UX fixed). Remaining: formatter improvements (Low), show_examples tool (Low). Candidate for: GitHub feature issue "MCP plugin: better formatting and example rows"
- **BACKLOG.md** — Extract remaining items not captured in Phases 1-4 (deferred to review checkpoint)
- **REFERENCE.md** — Content feeds into docs_new/architecture.md and docs_new/contributing.md (Phase 5)

---

## Phase 2: Plans — Uncaptured Items

Items identified during plans/ triage that were NOT turned into issues yet:

- **restructuring-plan.md Part 4**: Items 4.1 (config-driven pipeline), 4.2 (config audit), 4.3 (test cleanup), 4.8 (dependency audit) remain unfinished
- **entropy-phase-f-implementation.md**: Evidence preservation (#3), profile class cleanup (#4), context module creation — none implemented
- **testdata-and-calibration-roadmap.md**: Phase 2 (config-driven pipeline), Phase 4 (dataraum-evaluate, closed-source), Phases 5-6 (vertical calibration, marketing scenarios)
- **interface-strategy.md**: Phase 1 (HTMX Web UI) — may overlap with GH#11. Phase 2b (Jupyter) — created as GH#12. Phase 3 (UI completion) — covered by GH#11.
