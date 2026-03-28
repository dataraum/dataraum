# CLI Reference

The `dataraum` CLI runs the metadata extraction pipeline and provides developer utilities. For interactive data exploration, querying, and source management, use the MCP server via Claude or another MCP client.

## Installation

The CLI is installed automatically when you install the package:

```bash
uv pip install -e .
```

## Commands

### `run` - Run the Pipeline

Execute the pipeline on data sources.

```bash
dataraum run [SOURCE] [OPTIONS]
```

**Arguments:**
- `SOURCE` - Path to a data file or directory (optional; uses registered sources when omitted)

**Options:**
- `-o, --output PATH` - Output directory for database files (default: `./pipeline_output`)
- `-n, --name TEXT` - Name for the data source (default: derived from path)
- `-p, --phase TEXT` - Run only this phase and its dependencies
- `-f, --force` - Force re-run of target phase, deleting previous results (requires `--phase`)
- `--contract TEXT` - Target contract name for entropy evaluation
- `-q, --quiet` - Suppress progress output
- `-v, --verbose` - Increase logging verbosity (`-v` = INFO, `-vv` = DEBUG)
- `--log-format TEXT` - Log output format: `console` (default) or `json`

**Examples:**

```bash
# Run full pipeline on a directory
dataraum run /path/to/data

# Run on a single file with custom output
dataraum run /path/to/file.csv --output ./my_output

# Run using already-registered sources
dataraum run --output ./output

# Run only up to the statistics phase
dataraum run /path/to/data --phase statistics

# Run with a specific contract
dataraum run /path/to/data --contract aggregation_safe
```

### Re-running a Single Phase

During development you often need to re-run a single phase after changing its code. By default, completed phases are skipped based on checkpoint records. The `--force` flag cleans up a phase's previous output and re-runs it.

```bash
# Re-run the statistics phase (deletes its output, then runs it fresh)
dataraum run /path/to/data --phase statistics --force

# Re-run semantic analysis
dataraum run /path/to/data --phase semantic -f
```

**Notes:**
- `--force` requires `--phase` — it only applies to the target phase, not its dependencies.
- Only the target phase's output is deleted; dependency phases keep their results.

### `dev` - Developer Utilities

Subcommand group for pipeline debugging.

```bash
dataraum dev phases [--reset PHASE] [-o OUTPUT_DIR]
dataraum dev context [OUTPUT_DIR]
```

- `dev phases` — List all pipeline phases and their dependencies
- `dev phases --reset PHASE` — Reset a specific phase (delete its data and checkpoint)
- `dev context` — Print the full metadata document that agents receive when generating SQL

## Output Files

After running the pipeline, the output directory contains:

| File | Description |
|------|-------------|
| `metadata.db` | SQLite database with all metadata (sources, tables, columns, relationships, etc.) |
| `data.duckdb` | DuckDB database with raw, typed, and quarantine tables |
