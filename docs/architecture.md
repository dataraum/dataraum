# Architecture Overview

## Vision

Traditional semantic layers tell BI tools "what things are called." DataRaum tells AI "what the data means, how it behaves, how it relates, and what you can compute from it."

**Core insight**: AI agents don't need tools to discover metadata at runtime. They need **rich, pre-computed context** delivered in a format optimized for LLM consumption.

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CONSUMERS                                         │
│                                                                             │
│   Claude Code ──── MCP Server (10 tools)                                    │
│   Claude Desktop ─┘                       Session + Operation Model         │
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
│   18 phases with dependency-based execution                                 │
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
│   │ entropy_objects,     │                                                  │
│   │ investigation_*,     │                                                  │
│   │ slice_definitions,   │                                                  │
│   │ validation_results,  │                                                  │
│   │ graph_executions,    │                                                  │
│   │ sql_snippets (+prov),│                                                  │
│   │ query_executions,    │                                                  │
│   │ ...                  │                                                  │
│   └──────────────────────┘                                                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Technology Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| **AI Interface** | MCP Server | 10 tools for AI agents (Claude Code, Claude Desktop) |
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

10 MCP tools organized around investigation sessions: `add_source` → `begin_session` → `look` / `measure` / `why` / `teach` / `query` / `run_sql` / `search_snippets` → `end_session`. Sources are registered first, then the session seals them. The session carries the contract (entropy threshold profile) so the agent doesn't need to pass it on every call.

### The Understanding Layer (Operation Model)

DataRaum maintains an **operation model** for each dataset — a structured, queryable representation of what exists and what it means. Five facets:

| Facet | What | Pipeline phase |
|-------|------|---------------|
| Concepts | What exists in business terms | semantic |
| Metrics | What we measure and compute | graph_execution |
| Cycles | How things flow | business_cycles |
| Validations | What must be true | validation |
| Filters | What counts, what's excluded | graph_execution |

The operation model is built progressively through **teach + rerun** cycles. `teach` extends a vertical YAML overlay (session-scoped at `DATARAUM_HOME/workspace/<session>/vertical/`); `measure(target_phase=...)` reruns the affected phase so downstream metadata reflects the new knowledge. Config teaches (concept, metric, cycle, validation, relationship, type_pattern, null_value) trigger a rerun; metadata teaches (concept_property, explanation) apply immediately.

### Snippet Provenance and Promotion

The `graph_execution` phase and the `query` / `run_sql` tools produce **SQL snippets** — named, reusable SQL fragments stored with grounding provenance (field resolution, column mappings, LLM reasoning, repair status). Snippets are discoverable via `search_snippets` and injected as cached steps into downstream executions.

Snippet sources form a hierarchy:

- `graph:{graph_id}` — authoritative, produced by graph_execution from vertical metric YAML
- `query:{execution_id}` — exploratory, produced by the query agent
- `mcp:session_...` — ad-hoc, produced by `run_sql`

Ad-hoc snippets can be **promoted** into authoritative graph snippets via `teach(type="metric", inspiration_snippet_id=...)`: the teach stores the inspiration ID in metric YAML, graph_execution reruns, and the ad-hoc snippet is deleted once the authoritative snippet is produced.

Only `graph:` snippets currently contribute to the search vocabulary, keeping ad-hoc run_sql noise out of discoverability.

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
│   ├── measurement.py     # measure_entropy(), match_threshold()
│   └── engine.py          # Detector orchestration, post-step hooks
├── investigation/         # Session trace models (MCP audit trail)
├── graphs/                # Metric calculation graphs, graph agent, context assembly
├── query/                 # Natural language query execution + SQL snippet store
│   ├── agent.py           # Query agent (LLM SQL planner)
│   ├── snippet_library.py # Snippet search, promotion, vocabulary
│   └── snippet_models.py  # SQLSnippetRecord with provenance
├── pipeline/              # Pipeline orchestrator
│   ├── registry.py        # Phase auto-discovery
│   ├── runner.py          # Execution engine + RunConfig
│   └── phases/            # 18 phase implementations
├── sources/               # Data source loaders (CSV, Parquet, JSON)
├── storage/               # SQLAlchemy base, migrations
├── llm/                   # LLM provider abstraction, prompt management
├── core/                  # Config, connections, utilities, models, file logging
├── cli/                   # Typer CLI
│   ├── main.py            # CLI entry point
│   └── commands/          # run, dev (subgroup)
└── mcp/                   # MCP server
    ├── server.py          # 10 tool definitions + session instructions
    ├── teach.py           # teach dispatch (9 types) + YAML overlay writer
    ├── formatters.py      # LLM-optimized markdown output
    ├── sections.py        # Response section builders
    └── sql_executor.py    # SQL execution with auto-repair + export support
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

Entropy detectors run as post-steps after each phase, building up scores incrementally. The `measure` MCP tool evaluates these scores against contract thresholds at any point.

## Interfaces

### MCP Server (10 tools)

Primary interface for AI agents. Tools return markdown formatted for LLM consumption. The server emits session instructions on connect describing when to use each tool.

| Tool | Purpose |
|------|---------|
| `begin_session` | Start an investigation session with a contract; triggers pipeline on first run |
| `add_source` | Register a data source |
| `look` | Explore schema, relationships, semantic metadata, readiness |
| `measure` | Entropy scores + readiness; reruns a target phase on demand |
| `why` | Evidence synthesis — explains elevated entropy, proposes teaches |
| `teach` | Extend the operation model (9 teach types) |
| `query` | Natural-language data queries with confidence + assumptions |
| `run_sql` | Execute SQL with auto-repair, export support, snippet caching |
| `search_snippets` | Discover reusable SQL snippets with provenance |
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

6 of 18 pipeline phases use LLM, plus interactive agents invoked via MCP.

| Feature | Model Tier | Purpose |
|---------|------------|---------|
| Semantic Analysis | balanced | Column roles, entity types, relationships |
| Column Annotation | fast | Individual column descriptions |
| Enrichment Analysis | balanced | Enriched view construction |
| Slicing Analysis | balanced | Identify meaningful data segments |
| Business Cycles | balanced | Multi-table process detection |
| Validation | balanced | Domain-specific SQL generation |
| Graph SQL Generation | balanced | Metric SQL grounded in the semantic layer + provenance |
| Metric Induction | balanced | Cold-start metric YAML generation from ontology |
| Query Agent | balanced | Natural-language → SQL plan with assumptions |
| Why Agent | balanced | Evidence synthesis → explanation + teach suggestions |
| SQL Repair | fast | Fix broken generated SQL |

LLM configuration: `config/llm/config.yaml`. Prompts: `config/llm/prompts/`. Provider: Anthropic (primary).

Privacy: sensitive columns (email, SSN, etc.) are redacted before LLM calls based on patterns in `config/llm/config.yaml`.
