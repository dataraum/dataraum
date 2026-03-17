# Architecture Overview

## Vision

Traditional semantic layers tell BI tools "what things are called." DataRaum tells AI "what the data means, how it behaves, how it relates, and what you can compute from it."

**Core insight**: AI agents don't need tools to discover metadata at runtime. They need **rich, pre-computed context** delivered in a format optimized for LLM consumption.

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CONSUMERS                                         │
│                                                                             │
│   Claude Code ──── MCP Server (6 tools)                                      │
│   Claude Desktop ─┘                       ContextDocument (pre-assembled)   │
│   Python ──────── Context API                                               │
│   Terminal ────── CLI + TUI (4 commands + 2 subgroups)                       │
└─────────────────────────────────────────────────────────────────────────────┘
                                       ↑
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ENTROPY LAYER                                     │
│                                                                             │
│   12 Detectors (8 hard, 4 soft) ──▶  Contracts (6 built-in)                 │
│   Bayesian Network              ──▶  Actions (prioritized fixes)            │
│   Gates (entropy preconditions) ──▶  Fix Ledger (domain knowledge)         │
│   LLM Interpretation            ──▶  Confidence Levels (traffic light)      │
└─────────────────────────────────────────────────────────────────────────────┘
                                       ↑
┌─────────────────────────────────────────────────────────────────────────────┐
│                           METADATA MODULES                                  │
│                                                                             │
│   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐      │
│   │ Statistical │  │  Semantic   │  │Relationships│  │  Temporal   │      │
│   │             │  │   (LLM)     │  │             │  │             │      │
│   │ • profiles  │  │ • roles     │  │ • FK detect │  │ • granular- │      │
│   │ • distribs  │  │ • entities  │  │ • topology  │  │   ity       │      │
│   │ • patterns  │  │ • terms     │  │ • join paths│  │ • gaps      │      │
│   │ • outliers  │  │ • relations │  │ • graph     │  │ • drift     │      │
│   └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘      │
│          │                │                │                │              │
│          └────────────────┴────────────────┴────────────────┘              │
│                                    │                                        │
│          ┌─────────────────────────┼─────────────────────────┐             │
│          ▼                         ▼                         ▼             │
│   ┌─────────────┐  ┌──────────────────┐  ┌─────────────────────┐          │
│   │   Quality   │  │   Slicing &      │  │   Business Cycles   │          │
│   │             │  │   Enriched Views │  │   & Validation      │          │
│   │ • summaries │  │ • dimensions    │  │ • multi-table       │          │
│   │ • Benford   │  │ • drift         │  │ • domain rules      │          │
│   └─────────────┘  └──────────────────┘  └─────────────────────┘          │
└─────────────────────────────────────────────────────────────────────────────┘
                                       ↑
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA LAYER                                        │
│                                                                             │
│   DuckDB                                                                    │
│   raw_{table} ──▶ typed_{table} ──▶ enriched views ──▶ queries              │
│        │                │                                                   │
│        │                ▼                                                   │
│        │        quarantine_{table}                                          │
│        ▼                                                                    │
│   Source files (CSV, Parquet)                                               │
└─────────────────────────────────────────────────────────────────────────────┘
                                       ↑
┌─────────────────────────────────────────────────────────────────────────────┐
│                           PIPELINE ORCHESTRATOR                             │
│                                                                             │
│   19 phases with dependency-based execution                                 │
│   Entropy gates between phases (detector preconditions)                     │
│   ThreadPoolExecutor (true parallelism via Python 3.14 free-threading)      │
│   Idempotent phases, checkpoint-based resumption                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
┌─────────────────────────────────────────────────────────────────────────────┐
│                           STORAGE                                           │
│                                                                             │
│   metadata.db (SQLite)          data.duckdb                                 │
│   ┌──────────────────────┐      ┌──────────────────────┐                   │
│   │ sources, tables,     │      │ raw_{table}          │                   │
│   │ columns, profiles,   │      │ typed_{table}        │                   │
│   │ type_candidates,     │      │ quarantine_{table}   │                   │
│   │ semantic_annotations,│      │ enriched views       │                   │
│   │ relationships,       │      └──────────────────────┘                   │
│   │ quality_reports,     │                                                  │
│   │ entropy_snapshots,   │                                                  │
│   │ entropy_records,     │                                                  │
│   │ decisions,           │                                                  │
│   │ slice_definitions,   │                                                  │
│   │ validation_results,  │                                                  │
│   │ ...                  │                                                  │
│   └──────────────────────┘                                                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Technology Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| **AI Interface** | MCP Server | 6 tools for AI agents (Claude Code, Claude Desktop) |
| **CLI** | Typer + Rich | 4 commands + 2 subgroups for terminal use |
| **TUI** | Textual | Interactive terminal dashboards |
| **Python API** | `Context` class | Programmatic access for notebooks and scripts |
| **Pipeline** | ThreadPoolExecutor | Parallel phase execution (free-threaded Python 3.14) |
| **Metadata Store** | SQLAlchemy + SQLite | Structured metadata persistence |
| **Data Compute** | DuckDB | Load, query, transform data |
| **Data Exchange** | PyArrow | Efficient data transfer between engines |
| **Statistical** | SciPy, StatsModels, Ruptures | Distributions, regressions, changepoints |
| **Probabilistic** | PGMPy, NetworkX | Bayesian networks for entropy causality |
| **LLM** | Anthropic (primary) | Semantic analysis, quality rules, interpretation |
| **Configuration** | YAML | Ontologies, verticals, thresholds, prompts |

## Key Design Decisions

### VARCHAR-First Staging

All data is loaded as VARCHAR to preserve raw values. Type inference happens in the typing phase, not during import. Failed type casts go to quarantine tables for review — no silent data loss.

### Pre-Computed Context

AI doesn't discover metadata at runtime via tools. It receives a pre-assembled context document with all relevant metadata already computed and interpreted through domain ontologies. This makes AI interactions faster and more reliable.

### Minimal Tool Surface

6 MCP tools: 4 core (`analyze`, `get_context`, `get_quality`, `query`) + 2 source management (`discover_sources`, `add_source`). `get_quality` returns a unified report combining entropy, contract evaluation, and resolution actions. Rich context upfront instead of many discovery tools.

### Entropy Over Binary Quality

Instead of pass/fail quality checks, entropy quantifies uncertainty on a continuous 0–1 scale across 4 dimensions. Contracts translate these scores into use-case-specific readiness assessments.

### Ontologies as Configuration

Domain knowledge is encoded in YAML verticals (`config/verticals/`), not hard-coded. Each vertical provides:
- Concept definitions with pattern matching
- Metric computation graphs
- Quality filters by role/type/pattern
- Domain-specific validation rules

### Free-Threading

Python 3.14 free-threaded build enables true CPU parallelism. Pipeline phases run in parallel via `ThreadPoolExecutor` without GIL contention. DuckDB read cursors are thread-safe; writes are serialized via mutex.

## Module Structure

```
src/dataraum/
├── analysis/              # Data analysis modules
│   ├── typing/            # Type inference, pattern detection, quarantine
│   ├── statistics/        # Column profiling, distributions
│   ├── correlation/       # Numeric/categorical correlations
│   ├── relationships/     # Join detection, FK candidates, topology
│   ├── semantic/          # LLM-powered semantic analysis
│   ├── temporal/          # Time series analysis
│   ├── slicing/           # Data slicing and drift detection
│   ├── cycles/            # Business cycle detection
│   ├── validation/        # Domain validation rules
│   └── quality_summary/   # Quality report synthesis
├── entropy/               # Uncertainty quantification
│   ├── detectors/         # 12 detectors across 4 layers (8 hard, 4 soft)
│   │   ├── structural/    # Type fidelity, join paths, relationship quality
│   │   ├── semantic/      # Business meaning, units, temporal, dimensional
│   │   ├── value/         # Nulls, outliers, drift, Benford
│   │   └── computational/ # Derived values, aggregation safety
│   ├── contracts/         # Use-case threshold evaluation
│   ├── network/           # Bayesian causal network
│   ├── snapshot.py        # Snapshot: run detectors, before/after measurement
│   ├── actions.py         # Merge resolution actions from all sources
│   ├── context.py         # Entropy context builder
│   └── interpretation.py  # LLM entropy interpretation
├── documentation/           # Fix ledger + document agent
├── graphs/                # Metric calculation graphs
├── query/                 # Natural language query execution
├── pipeline/              # Pipeline orchestrator
│   ├── registry.py        # Phase auto-discovery
│   ├── runner.py          # Execution engine + RunConfig
│   └── phases/            # 20 phase implementations
├── sources/               # Data source loaders (CSV, Parquet)
├── storage/               # SQLAlchemy base, migrations
├── llm/                   # LLM provider abstraction, prompt management
├── core/                  # Config, connections, utilities, models
├── cli/                   # Typer commands + Textual TUI
│   ├── main.py            # CLI entry point (4 commands + 2 subgroups)
│   ├── commands/          # run, tui, query, fix, sources (subgroup), dev (subgroup)
│   ├── gate_handler.py    # Interactive CLI gate handler (Rich prompts)
│   ├── tui/               # Textual application + screens + widgets
│   └── ...
└── mcp/                   # MCP server
    ├── server.py          # 6 tool definitions
    └── formatters.py      # LLM-optimized markdown output
```

SQLAlchemy models are co-located with business logic in `db_models.py` files within each module.

## Data Flow

```
Source (CSV/Parquet)
    ↓
[import] Load as VARCHAR → raw_{table}
    ↓
[typing] Type inference + cast testing → typed_{table}, quarantine_{table}
    ↓
 ⊘ GATE: type_fidelity ≤ 0.5
    ↓
[statistics, temporal, correlations, relationships] Statistical metadata
    ↓
 ⊘ GATE: type_fidelity ≤ 0.3, join_path_determinism ≤ 0.5
    ↓
[semantic] LLM analysis → roles, entities, business terms
    ↓
[enriched_views, slicing, slice_analysis] Joined views, data segments
    ↓
[quality_summary] Per-table quality reports
    ↓
[entropy] 12 detectors → scores per column per dimension
    ↓
[entropy_interpretation] LLM → human-readable explanations + actions
    ↓
 ⊘ GATE: type_fidelity ≤ 0.3, naming_clarity ≤ 0.4
    ↓
[business_cycles, validation, graph_execution] Domain-specific analysis
    ↓
Context document → MCP / CLI / Python API → AI consumer
```

At each gate (⊘), the pipeline checks detector scores against contract thresholds. `dataraum run` defers violations; `dataraum fix` pauses interactively.

## Interfaces

### MCP Server (6 tools)

Primary interface for AI agents. Tools return markdown formatted for LLM consumption.

| Tool | Purpose |
|------|---------|
| `analyze` | Run pipeline on CSV/Parquet data |
| `get_context` | Full metadata context document |
| `get_quality` | Unified quality report (entropy + contracts + actions) |
| `query` | Natural language data queries |
| `discover_sources` | Scan workspace for data files |
| `add_source` | Register a file or database source |

### CLI (4 commands + 2 subgroups)

| Command | Purpose |
|---------|---------|
| `dataraum run` | Execute pipeline (with interactive gate handling) |
| `dataraum tui` | Interactive dashboard (5 screens) |
| `dataraum query` | Natural language data query |
| `dataraum fix` | Document domain knowledge interactively |
| `dataraum sources {list,add,discover,remove}` | Manage data sources |
| `dataraum dev {phases,inspect,reset}` | Developer utilities |

### Python API

```python
from dataraum import Context

with Context("./pipeline_output") as ctx:
    # Metadata
    ctx.tables                              # Table names
    ctx.context_document()                  # Full context for LLM

    # Entropy (returns wrapper with Jupyter _repr_html_)
    ctx.entropy.summary()                   # Overall entropy
    ctx.entropy.table("orders")             # Per-table
    ctx.entropy.details("orders", "amount") # Per-column with evidence

    # Contracts
    ctx.contracts.list()                    # Available contracts
    ctx.contracts.evaluate("aggregation_safe")

    # Actions
    ctx.actions(contract="executive_dashboard")

    # Query
    result = ctx.query("total revenue by month")
    result.answer                           # Natural language answer
    result.sql                              # Generated SQL
    result.to_dataframe()                   # Pandas DataFrame

    # Source management
    ctx.sources.list()                      # Registered sources
    ctx.sources.discover("/path/to/data")   # Scan for files

    # Pipeline execution
    ctx.run("/path/to/data.csv")            # Run pipeline from notebook
```

## LLM Integration

7 of 20 pipeline phases use LLM (19 active, 1 de-configured).

| Feature | Model Tier | Purpose |
|---------|------------|---------|
| Semantic Analysis | balanced | Column roles, entity types, relationships |
| Column Annotation | fast | Individual column descriptions |
| Slicing Analysis | balanced | Identify meaningful data segments |
| Quality Summary | fast | Synthesize per-table quality reports |
| Entropy Interpretation | balanced | Human-readable entropy explanations |
| Business Cycles | balanced | Multi-table process detection |
| Validation | balanced | Domain-specific SQL generation |

LLM configuration: `config/llm/config.yaml`. Prompts: `config/llm/prompts/`. Provider: Anthropic (primary).

Privacy: sensitive columns (email, SSN, etc.) are redacted before LLM calls based on patterns in `config/llm/config.yaml`.
