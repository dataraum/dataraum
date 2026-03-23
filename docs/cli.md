# CLI Reference

The `dataraum` command provides commands for running the metadata extraction pipeline, documenting domain knowledge, and querying your data.

## Installation

The CLI is installed automatically when you install the package:

```bash
uv pip install -e .
```

## Commands

### `run` - Run the Pipeline

Execute the pipeline on CSV data sources.

```bash
dataraum run SOURCE [OPTIONS]
```

**Arguments:**
- `SOURCE` - Path to a CSV file or directory containing CSV files (required)

**Options:**
- `-o, --output PATH` - Output directory for database files (default: `./pipeline_output`)
- `-n, --name TEXT` - Name for the data source (default: derived from path)
- `-p, --phase TEXT` - Run only this phase and its dependencies
- `-f, --force` - Force re-run of target phase, deleting previous results (requires `--phase`)
- `--contract TEXT` - Target contract name for gate evaluation
- `-q, --quiet` - Suppress progress output

**Examples:**

```bash
# Run full pipeline on a directory
dataraum run /path/to/csv/directory

# Run on a single file with custom output
dataraum run /path/to/file.csv --output ./my_output

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

### `tui` - Interactive Dashboard

Open the Textual TUI for interactive exploration of pipeline results. Provides five screens: Home (overview), Entropy (dimension drill-down), Contracts (compliance), Actions (resolution actions), and Query (natural language).

```bash
dataraum tui [OUTPUT_DIR]
```

**Arguments:**
- `OUTPUT_DIR` - Output directory containing pipeline databases (default: `./pipeline_output`)

### `sources` - Manage Data Sources

Subcommand group for discovering and registering data sources.

```bash
dataraum sources list [-o OUTPUT_DIR]
dataraum sources add NAME PATH [-o OUTPUT_DIR]
dataraum sources discover [PATH] [--no-recursive] [-o OUTPUT_DIR]
dataraum sources remove NAME [--purge] [-o OUTPUT_DIR]
```

**Examples:**

```bash
# Scan for data files
dataraum sources discover /path/to/data

# Register a CSV file
dataraum sources add sales /path/to/sales.csv

# List registered sources
dataraum sources list

# Remove a source (keep analysis results)
dataraum sources remove old_data
```

### `dev` - Developer Utilities

Subcommand group for pipeline debugging and maintenance.

```bash
dataraum dev phases [--reset PHASE] [-o OUTPUT_DIR]
dataraum dev inspect [OUTPUT_DIR] [--vertical VERTICAL]
dataraum dev reset [OUTPUT_DIR] [--force]
```

- `dev phases` — List all pipeline phases and their dependencies
- `dev phases --reset PHASE` — Reset a specific phase (delete its data and checkpoint)
- `dev inspect` — Inspect graph definitions, filter coverage, and execution context
- `dev reset` — Delete all database files from an output directory

### `query` - Natural Language Data Query

Ask questions about your data in natural language. The query engine generates SQL, executes it, and returns an answer with confidence levels.

```bash
dataraum query OUTPUT_DIR QUESTION
```

**Arguments:**
- `OUTPUT_DIR` - Output directory containing pipeline databases (required)
- `QUESTION` - Natural language question about the data (required)

**Example:**

```bash
dataraum query ./pipeline_output "What is the total revenue by month?"
```

**Output includes:**
- Natural language answer
- Generated SQL
- Result data
- Confidence level

## Output Files

After running the pipeline, the output directory contains:

| File | Description |
|------|-------------|
| `metadata.db` | SQLite database with all metadata (sources, tables, columns, relationships, etc.) |
| `data.duckdb` | DuckDB database with raw, typed, and quarantine tables |

## Configuration

The command name can be changed in `pyproject.toml`:

```toml
[project.scripts]
dataraum = "dataraum_context.cli:app"
```
