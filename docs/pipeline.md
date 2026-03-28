# Pipeline

The DataRaum pipeline extracts metadata from data files through 17 phases. Each phase produces structured metadata stored in SQLite (metadata) and DuckDB (data). Phases declare their dependencies and execute in topological order with parallel execution where possible.

## Running the Pipeline

```bash
# Full pipeline on a directory of CSV files
dataraum run /path/to/data

# Single file with custom output directory
dataraum run /path/to/file.csv --output ./my_output

# Run up to a specific phase (includes dependencies)
dataraum run /path/to/data --phase statistics

# Run with a target contract
dataraum run /path/to/data --contract aggregation_safe
```

Via MCP (Claude Code, Claude Desktop):
```
> Add the CSV files in /path/to/data and analyze them
```

Via Python:
```python
from dataraum import Context

ctx = Context("./pipeline_output")
ctx.run("/path/to/data")
```

## Phase Overview

| # | Phase | Purpose | LLM |
|---|-------|---------|-----|
| 1 | **import** | Load files into raw VARCHAR tables | — |
| 2 | **typing** | Type inference and resolution with quarantine | — |
| 3 | **statistics** | Statistical profiling of typed columns | — |
| 4 | **column_eligibility** | Determine which columns qualify for analysis | — |
| 5 | **statistical_quality** | Benford's Law compliance, outlier detection | — |
| 6 | **relationships** | Cross-table join detection (FK candidates) | — |
| 7 | **temporal** | Temporal pattern and trend analysis | — |
| 8 | **semantic** | Business meaning, roles, entity types (dual-tier) | Yes |
| 9 | **data_fixes** | Apply stored metadata fixes | — |
| 10 | **enriched_views** | Fact + dimension joined views | Yes |
| 11 | **slicing** | Identify slice dimensions for analysis | Yes |
| 12 | **slicing_view** | Create enriched views projected to slice-relevant columns | — |
| 13 | **slice_analysis** | Execute slicing SQL, build slice tables | — |
| 14 | **temporal_slice_analysis** | Distribution drift across slices over time | — |
| 15 | **correlations** | Derived column detection (same + cross-table via enriched views) | — |
| 16 | **business_cycles** | Detect business processes across tables | Yes |
| 17 | **validation** | Domain-specific validation checks | Yes |

5 of 17 phases require an LLM.

## Phase Categories

### Data Layer (Phases 1–2)
**Import** loads CSV, Parquet, and JSON files as VARCHAR columns into DuckDB. **Typing** infers column types via pattern matching and cast testing, producing typed tables and quarantine tables for rows that fail type conversion.

### Profiling Layer (Phases 3–7)
Statistical profiling, column eligibility evaluation, Benford's Law and outlier detection, relationship discovery, and temporal analysis. These phases are purely computational — no LLM calls.

### Enrichment Layer (Phases 8–15)
LLM-powered semantic analysis assigns business meaning to columns. Enriched views join fact and dimension tables. Slicing identifies meaningful data segments for deeper analysis. Correlations detect derived columns.

### Domain Layer (Phases 16–17)
Business cycle detection finds multi-table processes (e.g., order-to-cash). Validation runs domain-specific checks (e.g., debits = credits).

## Post-Phase Detectors

After each phase completes, the orchestrator runs **post-step detectors** — entropy detectors declared in `config/pipeline.yaml` that measure uncertainty in the outputs of that phase. For example:

- After `typing`: measures `type_fidelity`
- After `statistics`: measures `null_ratio`
- After `semantic`: measures `business_meaning`, `unit_entropy`, `temporal_entropy`, `outlier_rate`, `benford`, `join_path_determinism`, `relationship_entropy`
- After `enriched_views`: measures `dimension_coverage`
- After `correlations`: measures `derived_value`

This populates entropy scores incrementally so that `measure` (via MCP) can report quality at any point during or after the pipeline run.

## Dependency Graph

```
import ──► typing ──► statistics ──► column_eligibility ──┬──► statistical_quality ───┐
                                                          ├──► relationships           │
                                                          └──► temporal                │
                                                                   │                   │
                                                          semantic ◄┘                  │
                                                              │                        │
                                                         data_fixes                    │
                                                              │                        │
                                                     enriched_views ◄──────────────────┘
                                                         │       │
                                                  ┌──────┘       └──────┐
                                                  ▼                     ▼
                                               slicing            correlations
                                                  │
                                            slicing_view
                                                  │
                                           slice_analysis
                                                  │
                                    temporal_slice_analysis ◄── temporal
                                                  │
                                    business_cycles, validation
                                    (depend on semantic, relationships,
                                     temporal, enriched_views, slicing)
```

## Output Files

The pipeline writes to the output directory (default: `./pipeline_output`):

| File | Engine | Contents |
|------|--------|----------|
| `metadata.db` | SQLite | All metadata: sources, tables, columns, relationships, semantic annotations, entropy scores, etc. |
| `data.duckdb` | DuckDB | Raw, typed, and quarantine data tables plus enriched views |

## Rerunning Phases

Phases are idempotent. Rerunning the pipeline skips already-completed phases unless source data has changed.

```bash
# Force re-run of a specific phase
dataraum run /path/to/data --phase statistics --force
```

To start completely fresh, delete the output directory and re-run.
