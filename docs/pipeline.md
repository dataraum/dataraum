# Pipeline

The DataRaum pipeline extracts metadata from CSV and Parquet files through 20 phases (21 exist, 1 de-configured by default). Each phase produces structured metadata stored in SQLite (metadata) and DuckDB (data). Phases declare their dependencies and execute in topological order with parallel execution where possible.

## Running the Pipeline

```bash
# Full pipeline on a directory of CSV files
dataraum run /path/to/data

# Single file with custom output directory
dataraum run /path/to/file.csv --output ./my_output

# Run up to a specific phase (includes dependencies)
dataraum run /path/to/data --phase statistics

# Run with interactive gate handling (pause at entropy violations)
dataraum run /path/to/data --gate-mode pause

# Run with a target contract
dataraum run /path/to/data --gate-mode pause --contract aggregation_safe
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

| # | Phase | Purpose | LLM | Gate Preconditions |
|---|-------|---------|-----|--------------------|
| 1 | **import** | Load files into raw VARCHAR tables | — | — |
| 2 | **typing** | Type inference and resolution with quarantine | — | — |
| 3 | **temporal** | Temporal pattern and trend analysis | — | — |
| 4 | **statistics** | Statistical profiling of typed columns | — | type_fidelity ≤ 0.5 |
| 5 | **column_eligibility** | Determine which columns qualify for analysis | — | — |
| 6 | **correlations** | Within-table correlation analysis | — | — |
| 7 | **relationships** | Cross-table join detection (FK candidates) | — | — |
| 8 | **statistical_quality** | Benford's Law compliance, outlier detection | — | — |
| 9 | **semantic** | Business meaning, roles, entity types (dual-tier) | Yes | type_fidelity ≤ 0.3, join_path_determinism ≤ 0.5 |
| — | ~~cross_table_quality~~ | Cross-table correlation analysis (de-configured) | — | — |
| 10 | **enriched_views** | Fact + dimension joined views | — | — |
| 11 | **slicing** | Identify slice dimensions for analysis | Yes | — |
| 12 | **slicing_view** | Create enriched views projected to slice-relevant columns | — | — |
| 13 | **slice_analysis** | Execute slicing SQL, build slice tables | — | — |
| 14 | **temporal_slice_analysis** | Distribution drift across slices over time | — | — |
| 15 | **quality_summary** | Synthesize quality report per table | Yes | — |
| 16 | **entropy** | Measure uncertainty across 4 dimensions | — | — |
| 17 | **entropy_interpretation** | LLM interpretation of entropy scores | Yes | — |
| 18 | **business_cycles** | Detect business processes across tables | Yes | — |
| 19 | **validation** | Domain-specific validation checks | Yes | — |
| 20 | **graph_execution** | Execute metric calculation graphs | Yes | type_fidelity ≤ 0.3, naming_clarity ≤ 0.4 |

7 of 20 active phases require an LLM.

## Phase Categories

### Data Layer (Phases 1–2)
**Import** loads CSV/Parquet files as VARCHAR columns into DuckDB (`raw_{table}`). **Typing** infers column types via pattern matching and cast testing, producing `typed_{table}` and `quarantine_{table}` for rows that fail type conversion.

### Profiling Layer (Phases 3–8)
Statistical profiling, temporal analysis, correlation detection, relationship discovery, and quality checks. These phases are purely computational — no LLM calls.

### Enrichment Layer (Phases 9–14)
LLM-powered semantic analysis assigns business meaning to columns. Enriched views join fact and dimension tables. Slicing identifies meaningful data segments for deeper analysis.

### Quality Layer (Phases 15–17)
Quality summaries synthesize per-table reports. Entropy detection measures uncertainty across structural, semantic, value, and computational dimensions. LLM interpretation provides human-readable explanations.

### Domain Layer (Phases 18–20)
Business cycle detection finds multi-table processes (e.g., order-to-cash). Validation runs domain-specific checks (e.g., debits = credits). Graph execution computes configured metrics.

## Entropy Gates

Gates are checkpoints between pipeline phases where **entropy preconditions** are verified. When a phase's preconditions are not met (entropy is too high), the gate fires and the pipeline's behavior depends on the `--gate-mode`:

| Mode | Behavior |
|------|----------|
| `skip` (default for non-TTY) | Log warning, continue anyway |
| `pause` (default for interactive TTY) | Block the phase, present violations and fix options interactively |
| `fail` | Treat as pipeline failure |
| `auto_fix` | Attempt automatic fix via FixExecutor, skip if fix fails |

Gates use only **hard detectors** — machine-verifiable scores that can objectively gate the pipeline. Soft detectors (LLM-derived, like business meaning) inform actions but don't block phases. See [Entropy: Detector Trust](entropy.md#detector-trust) for the classification.

When a gate fires in `pause` mode, the user sees:
- Which dimensions are blocking (e.g., `type_fidelity: 0.62 > threshold 0.5`)
- Suggested fix actions with confidence levels
- A skip option to continue anyway
- A free-text escape hatch for LLM-powered questions about the gate

Fix actions execute through the `FixExecutor`, which takes before/after hard snapshots to verify improvement. Every decision is recorded in an immutable **decision ledger** for audit and reproducibility.

### Post-Verification

After each phase completes, the orchestrator runs **post-verification** — re-measuring the hard detector scores for dimensions that the phase's output affects. For example:

- After `typing`: measures `type_fidelity`
- After `statistics`: measures `null_ratio`, `outlier_rate`
- After `relationships`: measures `join_path_determinism`, `relationship_quality`
- After `semantic`: measures `naming_clarity`, `unit_declaration`

This populates the `PipelineEntropyState` so that subsequent gates can fire on the first pipeline run (without requiring a prior entropy phase to have run).

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
```

## Rerunning Phases

Phases are idempotent. Rerunning the pipeline skips already-completed phases unless source data has changed.

```bash
# Force re-run of a specific phase
dataraum run /path/to/data --phase statistics --force
```

To start completely fresh, delete the output directory and re-run.
