# Architecture Overview

## Vision

Traditional semantic layers tell BI tools "what things are called." DataRaum tells AI "what the data means, how it behaves, how it relates, and what you can compute from it."

**Core insight**: AI agents don't need tools to discover metadata at runtime. They need **rich, pre-computed context** delivered in a format optimized for LLM consumption.

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CONSUMERS                                         │
│                                                                             │
│   Claude Code ──── MCP Server (7 tools)                                     │
│   Claude Desktop ─┘                       ContextDocument (pre-assembled)   │
│   Python ──────── Context API                                               │
│   Terminal ────── CLI (run + dev)                                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                       ↑
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ENTROPY LAYER                                     │
│                                                                             │
│   16 Detectors (4 layers)           ──▶  Contracts (6 built-in)             │
│   Bayesian Network                  ──▶  Readiness (ready/investigate/      │
│   Post-phase measurement            ──▶    blocked per column)              │
│                                          Confidence Levels (traffic light)  │
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
│   │ • Benford   │  │ • dimensions    │  │ • multi-table       │          │
│   │ • outliers  │  │ • drift         │  │ • domain rules      │          │
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
│   Source files (CSV, Parquet, JSON)                                         │
└─────────────────────────────────────────────────────────────────────────────┘
                                       ↑
┌─────────────────────────────────────────────────────────────────────────────┐
│                           PIPELINE ORCHESTRATOR                             │
│                                                                             │
│   17 phases with dependency-based execution                                 │
│   Post-phase entropy detectors (scores computed incrementally)              │
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
│   │ entropy_records,     │                                                  │
│   │ entropy_objects,     │                                                  │
│   │ investigation_steps, │                                                  │
│   │ slice_definitions,   │                                                  │
│   │ validation_results,  │                                                  │
│   │ ...                  │                                                  │
│   └──────────────────────┘                                                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Technology Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| **AI Interface** | MCP Server | 7 tools for AI agents (Claude Code, Claude Desktop) |
| **CLI** | Typer + Rich | `run` command + `dev` subgroup for terminal use |
| **Python API** | `Context` class | Programmatic access for notebooks and scripts |
| **Pipeline** | ThreadPoolExecutor | Parallel phase execution (free-threaded Python 3.14) |
| **Metadata Store** | SQLAlchemy + SQLite | Structured metadata persistence |
| **Data Compute** | DuckDB | Load, query, transform data |
| **Data Exchange** | PyArrow | Efficient data transfer between engines |
| **Statistical** | SciPy, StatsModels, Ruptures | Distributions, regressions, changepoints |
| **Probabilistic** | PGMPy, NetworkX | Bayesian networks for entropy causality |
| **LLM** | Anthropic (primary) | Semantic analysis, enrichment, interpretation |
| **Configuration** | YAML | Ontologies, verticals, thresholds, prompts |

## Key Design Decisions

### VARCHAR-First Staging

All data is loaded as VARCHAR to preserve raw values. Type inference happens in the typing phase, not during import. Failed type casts go to quarantine tables for review — no silent data loss.

### Pre-Computed Context

AI doesn't discover metadata at runtime via tools. It receives a pre-assembled context document with all relevant metadata already computed and interpreted through domain ontologies. This makes AI interactions faster and more reliable.

### Session-Based Tool Surface

7 MCP tools organized around investigation sessions: `begin_session` → `add_source` → `look` / `measure` / `query` / `run_sql` → `end_session`. The session carries the contract (entropy threshold profile) so the agent doesn't need to pass it on every call.

### Entropy Over Binary Quality

Instead of pass/fail quality checks, entropy quantifies uncertainty on a continuous 0–1 scale across 4 layers. Contracts translate these scores into use-case-specific readiness assessments.

### Ontologies as Configuration

Domain knowledge is encoded in YAML verticals (`config/verticals/`), not hard-coded. Each vertical provides:
- Concept definitions with pattern matching
- Metric computation graphs
- Quality filters by role/type/pattern
- Domain-specific validation rules

### Free-Threading

Python 3.14 free-threaded build enables true CPU parallelism. Pipeline phases run in parallel via `ThreadPoolExecutor` without GIL contention. On Python 3.12/3.13, the same code runs under the GIL with no functional difference. DuckDB read cursors are thread-safe; writes are serialized via mutex.

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
│   ├── eligibility/       # Column eligibility evaluation
│   └── views/             # Enriched view construction
├── entropy/               # Uncertainty quantification
│   ├── detectors/         # 16 detectors across 4 layers
│   │   ├── structural/    # Type fidelity, join paths, relationship quality
│   │   ├── semantic/      # Business meaning, units, temporal, dimensional, coverage, cycles
│   │   ├── value/         # Nulls, outliers, drift, Benford, slice variance
│   │   └── computational/ # Derived values, cross-table consistency
│   ├── contracts/         # Use-case threshold evaluation
│   ├── network/           # Bayesian causal network
│   ├── measurement.py     # measure_entropy(), check_contracts()
│   ├── actions.py         # Merge resolution actions from all sources
│   └── engine.py          # Detector orchestration, post-step hooks
├── investigation/         # Session trace models (MCP audit trail)
├── documentation/         # Fix ledger + document agent
├── graphs/                # Metric calculation graphs, context assembly
├── query/                 # Natural language query execution
├── pipeline/              # Pipeline orchestrator
│   ├── registry.py        # Phase auto-discovery
│   ├── runner.py          # Execution engine + RunConfig
│   └── phases/            # 17 phase implementations
├── sources/               # Data source loaders (CSV, Parquet, JSON)
├── storage/               # SQLAlchemy base, migrations
├── llm/                   # LLM provider abstraction, prompt management
├── core/                  # Config, connections, utilities, models
├── cli/                   # Typer CLI
│   ├── main.py            # CLI entry point
│   └── commands/          # run, dev (subgroup)
└── mcp/                   # MCP server
    ├── server.py          # 7 tool definitions
    ├── formatters.py      # LLM-optimized markdown output
    ├── sections.py        # Response section builders
    └── sql_executor.py    # SQL execution with export support
```

SQLAlchemy models are co-located with business logic in `db_models.py` files within each module.

## Data Flow

```
Source (CSV/Parquet/JSON)
    ↓
[import] Load as VARCHAR → raw_{table}
    ↓
[typing] Type inference + cast testing → typed_{table}, quarantine_{table}
    ↓
[statistics, temporal, relationships, statistical_quality] Statistical metadata
    ↓                                                      (+ post-phase detectors)
[semantic] LLM analysis → roles, entities, business terms
    ↓
[enriched_views, slicing, slice_analysis, correlations] Joined views, data segments
    ↓
[business_cycles, validation] Domain-specific analysis
    ↓
Context document → MCP / CLI / Python API → AI consumer
```

Entropy detectors run as post-steps after each phase, building up scores incrementally. The `measure` MCP tool (or `check_contracts()` in Python) evaluates these scores against contract thresholds at any point.

## Interfaces

### MCP Server (7 tools)

Primary interface for AI agents. Tools return markdown formatted for LLM consumption.

| Tool | Purpose |
|------|---------|
| `begin_session` | Start an investigation session with a contract |
| `add_source` | Register and analyze a data source |
| `look` | Explore schema, relationships, semantic metadata |
| `measure` | Entropy scores, readiness, data quality |
| `query` | Natural language data queries |
| `run_sql` | Execute SQL with export support |
| `end_session` | Archive workspace and end session |

### CLI

| Command | Purpose |
|---------|---------|
| `dataraum run` | Execute the analysis pipeline |
| `dataraum dev phases` | List pipeline phases and dependencies |
| `dataraum dev context` | Print the full metadata context document |

### Python API

```python
from dataraum import Context

with Context("./pipeline_output") as ctx:
    # Metadata
    ctx.tables                              # Table names

    # Entropy (returns wrapper with Jupyter _repr_html_)
    ctx.entropy.summary()                   # Overall entropy
    ctx.entropy.table("orders")             # Per-table
    ctx.entropy.details("orders", "amount") # Per-column with evidence

    # Contracts
    ctx.contracts.list()                    # Available contracts
    ctx.contracts.evaluate("aggregation_safe")

    # Actions
    ctx.actions(contract="executive_dashboard")

    # Query (requires ANTHROPIC_API_KEY)
    result = ctx.query("total revenue by month")
    result.answer                           # Natural language answer
    result.sql                              # Generated SQL

    # Pipeline execution
    ctx.run("/path/to/data.csv")            # Run pipeline from notebook
```

## LLM Integration

5 of 17 pipeline phases use LLM.

| Feature | Model Tier | Purpose |
|---------|------------|---------|
| Semantic Analysis | balanced | Column roles, entity types, relationships |
| Column Annotation | fast | Individual column descriptions |
| Enrichment Analysis | balanced | Enriched view construction |
| Slicing Analysis | balanced | Identify meaningful data segments |
| Business Cycles | balanced | Multi-table process detection |
| Validation | balanced | Domain-specific SQL generation |
| SQL Repair | fast | Fix broken generated SQL |

LLM configuration: `config/llm/config.yaml`. Prompts: `config/llm/prompts/`. Provider: Anthropic (primary).

Privacy: sensitive columns (email, SSN, etc.) are redacted before LLM calls based on patterns in `config/llm/config.yaml`.
