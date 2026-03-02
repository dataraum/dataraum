# Module Documentation

Technical documentation for each module in the DataRaum Context Engine. These docs are written for contributors — for user-facing documentation, see the parent `docs/` directory.

## Modules

### Data Analysis
| Module | File | Description |
|--------|------|-------------|
| [Typing](typing.md) | `analysis/typing/` | Type inference, pattern detection, quarantine |
| [Statistics](statistics.md) | `analysis/statistics/` | Column profiling, distributions |
| [Correlations](correlations.md) | `analysis/correlation/` | Numeric and categorical correlations |
| [Relationships](relationships.md) | `analysis/relationships/` | Join detection, FK candidates |
| [Semantic](semantic.md) | `analysis/semantic/` | LLM-powered business meaning analysis |
| [Temporal](temporal.md) | `analysis/temporal/` | Time series analysis |
| [Slicing](slicing.md) | `analysis/slicing/` | Data segmentation |
| [Temporal Slicing](temporal-slicing.md) | `analysis/slicing/` | Distribution drift over time |
| [Cycles](cycles.md) | `analysis/cycles/` | Business process detection |
| [Validation](validation.md) | `analysis/validation/` | Domain-specific rule checks |
| [Quality Summary](quality-summary.md) | `analysis/quality_summary/` | Per-table quality synthesis |
| [Eligibility](eligibility.md) | `analysis/eligibility/` | Column analysis eligibility |

### Entropy
| Module | File | Description |
|--------|------|-------------|
| [Entropy](entropy.md) | `entropy/` | Uncertainty measurement (12 detectors, 4 layers) |

### Infrastructure
| Module | File | Description |
|--------|------|-------------|
| [Pipeline](pipeline.md) | `pipeline/` | Phase orchestrator, registry, runner |
| [Core Storage](core-storage.md) | `storage/`, `core/` | SQLAlchemy models, DuckDB, connections |
| [Sources](sources.md) | `sources/` | CSV/Parquet data loaders |
| [LLM](llm.md) | `llm/` | Provider abstraction, prompt management |
| [Topology](topology.md) | `analysis/relationships/` | TDA-based relationship detection |

### Context & Execution
| Module | File | Description |
|--------|------|-------------|
| [Graphs](graphs.md) | `graphs/` | Metric calculation graphs |
| [Query](query.md) | `query/` | Natural language query execution |
