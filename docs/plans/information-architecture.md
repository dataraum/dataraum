# DataRaum Context Engine - Information Architecture

> **Status (2026-02-17):** Complete. Reference document — no implementation actions. Defines user journeys and interface responsibilities.

> This document defines the user journeys, data needs, and interface responsibilities
> for the DataRaum Context Engine. It serves as the reference for all UX decisions.

## User Personas & Journeys

### Data Engineer
**Goal:** Load, profile, and prepare data for AI consumption.

| Step | Action | Interface | Data |
|------|--------|-----------|------|
| 1 | Load data | CLI `run` | Source files → DuckDB raw tables |
| 2 | Check phase status | CLI `status` / TUI HomeScreen | PhaseCheckpoint, PipelineRun |
| 3 | Inspect typing decisions | TUI TableScreen | TypeCandidate, TypeDecision, quarantine counts |
| 4 | Fix issues, re-run phases | CLI `reset --phase` + `run` | Phase dependency graph |
| 5 | Verify entropy scores | TUI EntropyScreen | EntropyObjectRecord, CompoundRiskRecord |
| 6 | Evaluate contracts | TUI ContractsScreen | Contract evaluation results |
| 7 | Deploy for AI use | CLI / MCP setup | GraphExecutionContext |

### Data Analyst
**Goal:** Explore data quality and ask questions.

| Step | Action | Interface | Data |
|------|--------|-----------|------|
| 1 | Browse tables | TUI HomeScreen | Table list, row counts, column counts |
| 2 | Check quality | TUI TableScreen | ColumnQualityReport, StatisticalProfile |
| 3 | View semantic annotations | TUI TableScreen | SemanticAnnotation, TableEntity |
| 4 | Ask questions | TUI QueryScreen | Query results via DuckDB |
| 5 | Save useful queries | TUI QueryScreen | QueryLibraryEntry |
| 6 | View slice profiles | TUI TableScreen | SliceProfile, SliceDefinition |

### AI Agent (via MCP)
**Goal:** Get structured context for deterministic data decisions.

| Step | Action | Tool | Data |
|------|--------|------|------|
| 1 | Get full context | `get_context` | GraphExecutionContext (assembled) |
| 2 | Check data readiness | `evaluate_contract` | Contract evaluation + entropy summary |
| 3 | Run queries | `query` | DuckDB results with entropy awareness |
| 4 | Get entropy details | `get_entropy` | EntropyObjectRecord per table/column |

### Business User (Future - Web UI)
**Goal:** Monitor data quality and view KPIs.

| Step | Action | Interface | Data |
|------|--------|-----------|------|
| 1 | View dashboard | Web UI | Quality grades, metric values |
| 2 | Check KPIs | Web UI | Computed metrics from graph execution |
| 3 | Monitor trends | Web UI | Entropy trends (future) |
| 4 | Share reports | Web UI | HTML reports via API |

### Jupyter User
**Goal:** Programmatic exploration and analysis.

| Step | Action | Interface | Data |
|------|--------|-----------|------|
| 1 | Connect | `Context("./output")` | ConnectionManager |
| 2 | Explore tables | `.tables` | Table metadata as dataclasses |
| 3 | Check entropy | `.entropy` | Entropy records as DataFrames |
| 4 | Run queries | `.query("SELECT ...")` | PyArrow/DataFrame results |
| 5 | Evaluate contracts | `.contracts` | Contract evaluation as dataclass |

---

## Interface Responsibilities

### CLI (Typer + Rich)
**Purpose:** Automation, CI/CD, summary output.

| Command | Purpose | Output |
|---------|---------|--------|
| `run` | Execute pipeline | Progress bar, phase status |
| `status` | Show pipeline state | Table of phases + status |
| `phases` | List available phases | Phase names + dependencies |
| `reset` | Reset phases for re-run | Confirmation + affected phases |
| `entropy` | Launch entropy TUI | TUI screen |
| `contracts` | Launch contracts TUI | TUI screen |
| `inspect` | Launch inspect TUI | TUI screen |

**Flags:** `--json` (machine-readable), `--no-tui` (raw text), `--phase` (target specific phase), `--output` (output directory).

### TUI (Textual)
**Purpose:** Interactive exploration and drill-down.

| Screen | Content | Key Data Models |
|--------|---------|-----------------|
| HomeScreen | Pipeline status, table list, quick stats | PipelineRun, PhaseCheckpoint, Table |
| EntropyScreen | Entropy tree, dimension details, compound risks | EntropyObjectRecord, CompoundRiskRecord |
| ContractsScreen | Contract definitions, evaluation results | Contract configs, evaluation records |
| QueryScreen | SQL editor, results, saved queries | DuckDB results, QueryLibraryEntry |
| TableScreen | Schema, profiles, quality, samples | Column, StatisticalProfile, ColumnQualityReport |

**Navigation:** Screen-to-screen via keyboard shortcuts and action buttons.

### API (FastAPI)
**Purpose:** Foundation for Web UI, programmatic access.

| Endpoint Group | Purpose | Future |
|----------------|---------|--------|
| `/api/v1/tables` | Table metadata | Web UI data source |
| `/api/v1/entropy` | Entropy data | Dashboard widgets |
| `/api/v1/contracts` | Contract evaluation | Traffic light displays |
| `/api/v1/query` | SQL execution | Interactive query builder |
| `/reports/` | HTML reports | Shareable report pages |

### MCP (Model Context Protocol)
**Purpose:** LLM integration with minimal, high-level tools.

| Tool | Input | Output |
|------|-------|--------|
| `get_context` | table name (optional) | Full GraphExecutionContext |
| `get_entropy` | table/column (optional) | Entropy summary with interpretation |
| `evaluate_contract` | contract name | Pass/fail with details |
| `query` | SQL string | Results with entropy annotations |

### Python API
**Purpose:** Jupyter notebooks, scripting.

```python
from dataraum import Context

ctx = Context("./output")
ctx.tables                    # List of table metadata
ctx.entropy                   # Entropy summary
ctx.contracts                 # Contract evaluations
ctx.query("SELECT * FROM typed_transactions LIMIT 10")  # DataFrame
```

---

## Data Model Summary

### Core Models (used by all interfaces)

| Model | Purpose | Created By |
|-------|---------|------------|
| Source | Data source metadata | import phase |
| Table | Table metadata | import phase |
| Column | Column metadata | import phase |
| PipelineRun | Pipeline execution record | pipeline runner |
| PhaseCheckpoint | Per-phase status | pipeline runner |

### Analysis Models (populated by pipeline phases)

| Model | Purpose | Phase |
|-------|---------|-------|
| TypeCandidate | Type inference candidates | typing |
| TypeDecision | Final type decisions | typing |
| StatisticalProfile | Column statistics | statistics |
| ColumnEligibilityRecord | Analysis gate | column_eligibility |
| StatisticalQualityMetrics | Statistical quality | stat_quality |
| Relationship | Table relationships | relationships |
| Correlation models (7) | Column correlations | correlations |
| SemanticAnnotation | LLM semantic tags | semantic |
| TableEntity | Entity recognition | semantic |
| ValidationResult | Data validation | validation |
| SliceDefinition | Data slice definitions | slicing |
| SliceProfile | Slice statistics | slice_analysis |
| DetectedBusinessCycle | Business cycles | business_cycles |
| TemporalProfile | Time series analysis | temporal |
| DriftAnalysis | Data drift detection | temporal |
| ColumnQualityReport | Quality summary | quality_summary |

### Entropy Models

| Model | Purpose | Phase |
|-------|---------|-------|
| EntropyObjectRecord | Per-dimension entropy | entropy |
| CompoundRiskRecord | Multi-dimension risk | entropy |
| EntropyInterpretationRecord | LLM interpretation | entropy_interpretation |

### Graph/Query Models (OUT OF SCOPE for restructuring)

| Model | Purpose | Phase |
|-------|---------|-------|
| GraphExecutionRecord | Graph computation results | graph_execution |
| QueryLibraryEntry | Saved queries | query agent |

---

## Data Flow: Pipeline → Interfaces

```
Pipeline Phases (sequential + parallel groups)
  │
  ├─→ SQLAlchemy DB (metadata)  ─→  TUI / API / MCP / Python API
  │
  ├─→ DuckDB (data tables)     ─→  Query execution (all interfaces)
  │
  └─→ GraphExecutionContext     ─→  MCP get_context (assembled on demand)
```

**Key principle:** All interfaces read from the same SQLAlchemy + DuckDB stores.
No interface has its own data store. The pipeline is the single writer.
