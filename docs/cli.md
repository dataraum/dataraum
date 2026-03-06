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
- `-g, --gate-mode {skip|fail}` - How to handle entropy gates (default: `skip`)
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

# Gate with contract evaluation (fail on violations)
dataraum run /path/to/data --gate-mode fail --contract aggregation_safe
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

### `fix` - Document Domain Knowledge

Review data quality actions and document domain knowledge through an interactive agent. Fixes are stored in a durable ledger and used as context when re-running semantic analysis.

```bash
dataraum fix [OUTPUT_DIR] [OPTIONS]
```

**Arguments:**
- `OUTPUT_DIR` - Output directory containing pipeline databases (default: `./pipeline_output`)

**Options:**
- `--rerun` - After documenting fixes, automatically re-run semantic + all downstream phases and show an entropy impact report

**Workflow:**

1. Run the pipeline: `dataraum run /path/to/data`
2. Document domain knowledge: `dataraum fix ./pipeline_output`
3. The agent shows `document_*` actions (e.g. undeclared units, unclear naming) and asks targeted questions
4. Your answers are interpreted and stored in the fix ledger
5. With `--rerun`, semantic analysis and all downstream phases re-run automatically, incorporating your fixes

**Examples:**

```bash
# Interactive fix session
dataraum fix ./pipeline_output

# Fix and immediately re-run the pipeline
dataraum fix --rerun ./pipeline_output
```

The `--rerun` flag handles the full re-run cycle: it snapshots entropy scores before cleaning, re-runs semantic + all downstream phases (enriched views, slicing, correlations, validation, graphs, entropy, etc.), and prints an entropy impact report showing which dimensions improved.

**Stale detection:** Even without `--rerun`, the semantic phase automatically detects when fixes are newer than its last run and re-executes instead of skipping.

### `status` - Show Pipeline Status

Display information about a completed or in-progress pipeline run.

```bash
dataraum status [OUTPUT_DIR]
```

**Arguments:**
- `OUTPUT_DIR` - Output directory containing pipeline databases (default: `./pipeline_output`)

**Output includes:**
- Source information (name, ID)
- Table counts by layer (raw, typed, quarantine)
- Total row and column counts
- Phase execution history with status, duration, and timestamps

**Example:**

```bash
dataraum status ./pipeline_output
```

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
