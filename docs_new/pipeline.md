# Pipeline

The DataRaum pipeline extracts metadata from CSV and Parquet files through 19 phases (20 exist, 1 de-configured by default). Each phase produces structured metadata stored in SQLite (metadata) and DuckDB (data). Phases declare their dependencies and execute in topological order with parallel execution where possible.

## Running the Pipeline

```bash
# Full pipeline on a directory of CSV files
dataraum run /path/to/data

# Single file with custom output directory
dataraum run /path/to/file.csv --output ./my_output

# Skip LLM phases (faster, no API calls)
dataraum run /path/to/data --skip-llm

# Run up to a specific phase (includes dependencies)
dataraum run /path/to/data --phase statistics
```

Via MCP (Claude Code, Claude Desktop):
```
> Analyze the CSV at /path/to/data.csv
```

Via Python:
```python
from dataraum import Context

# Pipeline runs via CLI or MCP; Context reads the results
ctx = Context("./pipeline_output")
```

## Phase Overview

| # | Phase | Purpose | LLM | Dependencies |
|---|-------|---------|-----|--------------|
| 1 | **import** | Load files into raw VARCHAR tables | — | — |
| 2 | **typing** | Type inference and resolution with quarantine | — | import |
| 3 | **temporal** | Temporal pattern and trend analysis | — | — |
| 4 | **statistics** | Statistical profiling of typed columns | — | typing |
| 5 | **column_eligibility** | Determine which columns qualify for analysis | — | statistics |
| 6 | **correlations** | Within-table correlation analysis | — | column_eligibility |
| 7 | **relationships** | Cross-table join detection (FK candidates) | — | column_eligibility |
| 8 | **statistical_quality** | Benford's Law compliance, outlier detection | — | column_eligibility |
| 9 | **semantic** | Business meaning, roles, entity types (dual-tier) | Yes | relationships, correlations |
| — | ~~cross_table_quality~~ | Cross-table correlation analysis (de-configured) | — | semantic |
| 10 | **enriched_views** | Fact + dimension joined views | — | semantic |
| 11 | **slicing** | Identify slice dimensions for analysis | Yes | enriched_views |
| 12 | **slice_analysis** | Execute slicing SQL, build slice tables | — | slicing |
| 13 | **temporal_slice_analysis** | Distribution drift across slices over time | — | slice_analysis, temporal |
| 14 | **quality_summary** | Synthesize quality report per table | Yes | slice_analysis, temporal_slice_analysis |
| 15 | **entropy** | Measure uncertainty across 4 dimensions | — | typing, column_eligibility, semantic, relationships, correlations, quality_summary, temporal_slice_analysis |
| 16 | **entropy_interpretation** | LLM interpretation of entropy scores | Yes | entropy |
| 17 | **business_cycles** | Detect business processes across tables | Yes | — |
| 18 | **validation** | Domain-specific validation checks | Yes | semantic, relationships, enriched_views, slicing |
| 19 | **graph_execution** | Execute metric calculation graphs | Yes | — |

7 of 19 active phases require an LLM. Use `--skip-llm` to run only the 12 non-LLM phases.

## Phase Categories

### Data Layer (Phases 1–2)
**Import** loads CSV/Parquet files as VARCHAR columns into DuckDB (`raw_{table}`). **Typing** infers column types via pattern matching and cast testing, producing `typed_{table}` and `quarantine_{table}` for rows that fail type conversion.

### Profiling Layer (Phases 3–8)
Statistical profiling, temporal analysis, correlation detection, relationship discovery, and quality checks. These phases are purely computational — no LLM calls.

### Enrichment Layer (Phases 9–15)
LLM-powered semantic analysis assigns business meaning to columns. Enriched views join fact and dimension tables. Slicing identifies meaningful data segments for deeper analysis.

### Quality Layer (Phases 15–17)
Quality summaries synthesize per-table reports. Entropy detection measures uncertainty across structural, semantic, value, and computational dimensions. LLM interpretation provides human-readable explanations.

### Domain Layer (Phases 18–20)
Business cycle detection finds multi-table processes (e.g., order-to-cash). Validation runs domain-specific checks (e.g., debits = credits). Graph execution computes configured metrics.

## Dependency Graph

```
import ──► typing ──► statistics ──► column_eligibility ──┬──► correlations ──────┐
                                                          ├──► relationships ─────┤
                                                          └──► statistical_quality│
                                                                                  │
temporal ─────────────────────────────────────────────────────────────────────┐    │
                                                                             │    │
                                          semantic ◄──────────────────────────────┘
                                             │
                                    ┌────────┼──────────┐
                                    ▼        ▼          ▼
                          cross_table    enriched    validation
                          _quality       _views
                                           │
                                           ▼
                                        slicing
                                           │
                                           ▼
                                     slice_analysis
                                           │
                              ┌─────────────┤
                              ▼             ▼
                     temporal_slice    quality_summary
                     _analysis
                              │             │
                              └──────┬──────┘
                                     ▼
                                  entropy
                                     │
                                     ▼
                            entropy_interpretation

business_cycles ──── (independent)
graph_execution ──── (independent)
```

## Output Files

The pipeline writes to the output directory (default: `./pipeline_output`):

| File | Engine | Contents |
|------|--------|----------|
| `metadata.db` | SQLite | All metadata: sources, tables, columns, relationships, semantic annotations, quality reports, entropy scores, etc. |
| `data.duckdb` | DuckDB | Raw, typed, and quarantine data tables plus enriched views |

## Checking Pipeline Status

```bash
# Show phase execution history, table counts, timing
dataraum status ./pipeline_output

# List all phases with dependencies and LLM requirements
dataraum phases
```

## Rerunning and Resetting

Phases are idempotent. Rerunning the pipeline skips already-completed phases unless source data has changed.

```bash
# Delete databases and start fresh
dataraum reset ./pipeline_output

# Force reset without confirmation
dataraum reset ./pipeline_output --force
```
