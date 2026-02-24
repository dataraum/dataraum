# DataRaum Context Engine

## Core Philosophy

This project prioritizes **correctness over speed**. We would rather have working code slowly than broken code quickly.

## Critical Rules - READ THESE FIRST

### Never Claim "Done" or "Production Ready" Until:
1. ALL tests pass (run the full test suite, not just the file you changed)
2. You have verified the actual output matches expected behavior
3. Type checking passes (if applicable)
4. Linting passes (if applicable)

If any of these fail, the task is NOT complete. Fix the issues before declaring success.

### The "Three Strikes" Rule for Debugging
If you've attempted the same fix 3 times without success:
1. STOP making changes
2. Explain what you've tried and what you observed
3. Form a hypothesis about the ROOT CAUSE (not just symptoms)
4. Ask for guidance or propose a fundamentally different approach

Do not continue making random changes hoping something works.

## Problem-Solving Standards

### Before Writing Any Code
- Understand the actual requirement, not what you assume it to be
- If the requirement is ambiguous, ask for clarification
- Consider edge cases upfront, not as an afterthought

### When Something Doesn't Work
1. **Read the actual error message** - quote it in your response
2. **Form a hypothesis** about WHY this error occurred
3. **Verify your hypothesis** before attempting a fix
4. **Make ONE targeted change** to test your hypothesis
5. **Observe the result** - did it confirm or refute your hypothesis?

Do NOT:
- Make multiple simultaneous changes
- Modify tests to make them pass (unless the test itself is wrong)
- Assume simple explanations for persistent problems
- Skip the hypothesis step

### Test Failures Are Information
When a test fails:
- The test is probably right and your code is wrong
- Understand WHAT the test expects and WHY
- Only modify the test if you can articulate why the test's expectation is incorrect

## Testing Standards

### Test Quality
- Each test should test ONE thing
- Test names should describe the expected behavior
- Tests should be independent - order should not matter
- Prefer many small, focused tests over few large tests

### When Tests Become Bloated
If you find yourself iterating heavily on tests:
1. STOP
2. Step back and understand what behavior you're actually testing
3. Delete the bloated test
4. Write a fresh, minimal test for that single behavior

### Test-Driven Debugging
When fixing a bug:
1. First write a failing test that reproduces the bug
2. Then fix the bug
3. Verify the test now passes
4. This proves you actually fixed the issue

## Code Quality

### Changes Should Be Minimal
- Prefer small, targeted changes over broad rewrites
- Each commit should do ONE thing
- If you're changing many files, question whether you're taking the right approach

### Avoid Premature Abstraction
- Write concrete code first
- Only abstract when you see actual duplication (rule of three)
- Simple, readable code beats clever, abstract code

## Definition of Done Checklist

Before declaring any task complete, verify:

- [ ] All existing tests still pass
- [ ] New functionality has tests
- [ ] Type checking passes
- [ ] Linting passes
- [ ] Code has been manually verified (if UI) or output checked (if logic)
- [ ] No console.log or debug statements left in code
- [ ] Error handling is in place
- [ ] Edge cases are handled

## Current Work - READ THIS FOR CONTEXT

**Before starting work, read these files to understand current state:**

| File | Purpose |
|------|---------|
| `docs/BACKLOG.md` | Task stack - what's done, what's next |
| `docs/PROGRESS.md` | Session log - recent work history |
| `docs/plans/cli-tui-plan.md` | Current plan: Textual TUI + MCP |
| `docs/ENTROPY_IMPLEMENTATION_PLAN.md` | Entropy system architecture |

**Current Focus (from BACKLOG.md):**
- CLI/TUI: Textual screens for status, entropy, contracts, query
- MCP Server: 6 tools (analyze, get_context, get_entropy, evaluate_contract, query, get_actions)

**Core Concept - Entropy:**
The key innovation is quantifying **uncertainty (entropy)** in data so LLMs can make deterministic decisions. See `entropy-management-framework.md` for the full spec.

---

## What Is This?

A Python library for extracting rich metadata context from data sources to power
AI-driven data analytics. The core idea: instead of giving AI tools to discover
metadata at runtime, we pre-compute comprehensive metadata and serve it as
structured context documents interpreted through domain ontologies.

## Quick Start for Claude Code

```bash
# Read these first
cat docs/ARCHITECTURE.md    # High-level design
cat docs/DATA_MODEL.md      # SQLAlchemy schema
cat docs/INTERFACES.md      # Module interfaces

# Check configuration
cat config/llm.yaml         # LLM provider settings
cat config/prompts/         # Customizable prompts

# Check existing prototypes (USE AS INSPIRATION)
ls prototypes/pattern_detection/helper/ # PyArrow + Pint pattern detection, check _convertCsvFile in ../main.py
ls prototypes/topology/                 # TDA-based relationship detection
ls prototypes/analytics-agents-ts       # Agents and prompts (inside prompts folder) for data-engineering and business-analysis
```

## Core Principles

1. **DuckDB for compute** - All data operations through DuckDB/PyArrow
2. **SQLAlchemy for metadata** - SQLite for dev, PostgreSQL for production
3. **Configuration as YAML** - Ontologies, patterns, rules, null values
4. **OSS-ready** - Clean interfaces, pip-installable, Apache-licensed core
5. **CLI-first** - Textual TUI for interactive use, MCP for LLM integration

## Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Data compute | DuckDB | Load, query, transform data |
| Data interchange | PyArrow | Efficient data transfer |
| Pattern detection | PyArrow + Pint | Type inference, unit detection |
| Topology | TDA (existing prototype) | Relationship detection |
| Metadata storage | SQLAlchemy | SQLite (dev) / PostgreSQL (prod) |
| CLI | Typer + Rich | Command-line interface |
| TUI | Textual | Interactive terminal UI |
| AI interface | MCP SDK | Tool definitions for AI |
| Python Runtime | Python 3.14t | Free-threading for true parallelism |

## Free-Threading (NO_GIL)

This project uses **Python 3.14 free-threaded build** for true CPU parallelism. The GIL is disabled, enabling:
- ~3.5x speedup on 4-core CPU-bound work via `ThreadPoolExecutor`
- Pipeline phases run in parallel without GIL contention

### Running with Free-Threading

```bash
# Run pipeline with GIL disabled
uv run python -Xgil=0 -m dataraum run /path/to/data

# Verify free-threading is enabled
python -c "import sys; print('Free-threading:', not sys._is_gil_enabled())"
```

### Architecture Notes

- **Single ConnectionManager** (`core/connections.py`) shared across modules
- **Pipeline in ThreadPoolExecutor**: Gets true parallelism from free-threading
- **DuckDB**: Read cursors are thread-safe; writes serialized via mutex

## Module Structure

```
src/dataraum/
├── analysis/       # Data analysis modules
│   ├── typing/         # Type inference, pattern detection
│   ├── statistics/     # Column profiling, distributions
│   ├── correlation/    # Numeric/categorical correlations
│   ├── relationships/  # Join detection, FK candidates
│   ├── semantic/       # LLM-powered semantic analysis
│   ├── temporal/       # Time series analysis
│   ├── slicing/        # Data slicing recommendations
│   ├── cycles/         # Business cycle detection
│   ├── validation/     # Data validation rules
│   └── quality_summary/# Quality report synthesis
├── entropy/        # Uncertainty quantification (core innovation)
│   ├── detectors/      # Entropy measurement per dimension
│   ├── context.py      # Entropy context builder
│   └── interpretation.py # LLM entropy interpretation
├── graphs/         # Calculation graphs, context assembly
├── pipeline/       # Pipeline orchestrator (18 phases)
├── sources/        # Data source loaders (CSV, Parquet)
├── storage/        # SQLAlchemy models
├── llm/            # LLM providers and prompts
├── core/           # Config, connections, utilities
├── cli/            # Textual TUI (TODO)
│   ├── main.py         # Typer app, command definitions
│   ├── app.py          # Textual DataraumApp
│   ├── screens/        # TUI screens
│   └── widgets/        # TUI widgets
└── mcp/            # MCP server (TODO)
    ├── server.py       # MCP tool definitions
    └── formatters.py   # LLM-optimized output
```

**Note:** SQLAlchemy DB models are co-located with business logic in `db_models.py` files within each module.

## Data Flow

```
Source (CSV/DB/API)
    ↓
[staging] Load as VARCHAR → raw_{table}
    ↓
[profiling] Statistical analysis → profiles, type_candidates
    ↓
[profiling] Type resolution → typed_{table}, quarantine_{table}
    ↓
[enrichment] LLM semantic analysis → roles, entities, relationships
    ↓
[enrichment] Topology (TDA) + temporal → additional metadata
    ↓
[quality] LLM rule generation + assessment → rules, scores
    ↓
[context] Assembly + LLM summary → ContextDocument (for AI)
         + LLM suggested queries
```

## LLM-Powered Features

The following features use LLM (configurable via `config/llm.yaml`):

| Feature | Description | Fallback |
|---------|-------------|----------|
| Semantic Analysis | Column roles, entity types, relationships | `config/semantic_overrides.yaml` |
| Quality Rules | Domain-specific rule generation | `config/rules/*.yaml` |
| Suggested Queries | Context-aware SQL queries | Skip |
| Context Summary | Natural language data overview | Skip |

When LLM is disabled, manual YAML configuration is required for semantic analysis.
Prompts are customizable in `config/prompts/`.

## Key Design Decisions

### VARCHAR-First Staging
Load all data as VARCHAR to preserve raw values. Type inference happens in
profiling, not during load. This prevents silent data loss.

### Quarantine Pattern
Failed type casts don't fail the pipeline. They go to quarantine tables for
human review. The workflow can checkpoint and wait for approval.

### Pre-computed Context
AI doesn't discover metadata. It receives a pre-assembled `ContextDocument`
with all relevant metadata already computed and interpreted through the
selected ontology.

### Minimal AI Tools
Only 6 MCP tools:
- `analyze` - Run pipeline on CSV/Parquet data
- `get_context` - Primary context retrieval
- `get_entropy` - Entropy analysis for tables/columns
- `evaluate_contract` - Data readiness evaluation
- `query` - Execute SQL with entropy awareness
- `get_actions` - Prioritized resolution actions

### Ontologies as Configuration
Domain ontologies (financial_reporting, marketing, etc.) are YAML configs that:
- Map column patterns to business terms
- Define computable metrics with formulas
- Provide domain-specific quality rules
- Guide semantic interpretation

## Existing Prototypes

### Pattern Detection (`prototypes/pattern_detection/helper`)
**REIMPLEMENT** take inspiration from the code. Use the yaml configurations for patterns and null values inside config for a reimplementation. Don't use the stats part.

```python
# Entry point
from helper.convert_csv import ChunkedArrayAnalyzer

csv_analyzer = ChunkedArrayAnalyzer(header)
converted_row = csv_analyzer.process_chunked_array(struct_array)
# Returns: PatternResult with type_candidates, detected_patterns, detected_unit, stats
```

Features:
- Regex-based pattern matching (dates, emails, UUIDs, etc.)
- Pint integration for unit detection (kg, m/s, USD, etc.)
- Type candidate scoring with confidence

### Topology Analysis (`prototypes/topology/`)
**REIMPLEMENT** - take inspiration from the code, remove string comparisons for ranking, focus on extracting the topology and related metrics as additional context for the semantic analysis.

```python
# Entry point
from core.topology_extractor import TableTopologyExtractor
from core.relationship_finder import TableRelationshipFinder

extractor = TableTopologyExtractor()
finder = TableRelationshipFinder()


topology = extractor.extract_topology(df)
relationships = finder.find_relationships(tables)

# Returns: TopologyGraph with relationships, confidence scores
```

Features:
- TDA-based structural analysis
- Semantic similarity integration
- FK candidate detection with evidence

### Topology Analysis (`prototypes/analytics-agents-ts/`)
**REIMPLEMENT** - take inspiration from the code, check the data-analysis prompts inside the prompts folder. Check the data-analysis schema inside the prompts/schemas folder. Follow the same pattern of system prompt, user prompt, JSON Schema as a tool. Only focus on the semantic meaning, skip the data quality part

Features:
- Analyse the semantics of the provided data schema.
- Summarize the business purpose.
- Dynamically create the prompts, with context injected.

## Implementation Guidelines

### Error Handling
```python
# Use Result type, not exceptions
from dataraum.core.models import Result

async def some_operation() -> Result[SomeOutput]:
    try:
        # ... do work ...
        return Result.ok(output, warnings=["minor issue"])
    except SomeExpectedError as e:
        return Result.fail(str(e))
```

### Database Connections
```python
# Use context managers
with manager.session_scope() as session:
    with manager.duckdb_cursor() as cursor:
        result = some_operation(cursor, session)
```

### Testing
- Unit tests for each module
- Integration tests with DuckDB in-memory + SQLite in-memory
- Property-based tests for pattern detection
- Mock LLM responses for deterministic testing
- Use pytest-asyncio for async tests

### Code Style
- Type hints on all functions
- Pydantic models for data classes
- No classes where functions suffice
- Docstrings with Args/Returns
- Max function length: ~50 lines

## Implementation Order (Historical Reference)

> **Note:** This section reflects the original plan. For current status, see `docs/BACKLOG.md`.
> Phases 1-4 are complete. Current work is CLI/TUI + MCP.

### Phase 1: Foundation ✅ COMPLETE
These must come first - everything else depends on them.

| Step | Module | Deliverables |
|------|--------|--------------|
| 1 | Storage | SQLAlchemy models, `llm_cache` table |
| 2 | Core/Config | Settings, YAML loaders, connection managers |

### Phase 2A: LLM Infrastructure ✅ COMPLETE

### Phase 2B: Data Pipeline ✅ COMPLETE

### Phase 3: Intelligent Enrichment ✅ COMPLETE

### Phase 4: Quality & Context ✅ COMPLETE
(Includes Entropy layer - see `docs/ENTROPY_IMPLEMENTATION_PLAN.md`)

### Phase 5: Interfaces & Orchestration ⏳ IN PROGRESS

| Step | Module | Status |
|------|--------|--------|
| Pipeline Orchestrator | `pipeline/` | ✅ 18 phases, CLI complete |
| CLI/TUI | `cli/` | ⏳ Next: Textual screens |
| MCP Server | `mcp/` | Pending (after TUI) |

### Testing Strategy

| Phase | Approach |
|-------|----------|
| 1-2 | Unit tests with SQLite in-memory |
| 3-4 | Mock LLM provider (no API calls) |
| 5-6 | Property-based tests, sample datasets |
| 7-9 | Integration tests: mock LLM + real DuckDB |
| 10-11 | Golden file tests for context documents |

## File Locations

| What | Where |
|------|-------|
| Architecture docs | `docs/` |
| Existing prototypes | `prototypes/` |
| Source code | `src/dataraum/` |
| Tests | `tests/` |
| Ontology configs | `config/ontologies/` |
| Pattern configs | `config/patterns/` |
| Quality rule configs | `config/rules/` |
| LLM prompts | `config/prompts/` |
| LLM config | `config/llm.yaml` |
| Null value lists | `config/null_values.yaml` |
| Semantic overrides | `config/semantic_overrides.yaml` |
| Example data | `examples/data/` |

## Dependencies

```toml
# pyproject.toml [project.dependencies]
dependencies = [
    "duckdb>=1.0.0",
    "pyarrow>=15.0.0",
    "pint>=0.23",
    "sqlalchemy>=2.0.0",
    "aiosqlite>=0.19.0",
    "pydantic>=2.0.0",
    "typer>=0.9.0",
    "rich>=13.0.0",
    "textual>=0.50.0",
    "mcp>=1.0.0",
    "pyyaml>=6.0.0",
]

# Optional: LLM providers
# pip install dataraum[anthropic]  # Claude
# pip install dataraum[openai]     # OpenAI
# pip install dataraum[llm]        # Both

# Optional: Synthetic data for privacy
# pip install dataraum[privacy]    # SDV

# Optional: Everything
# pip install dataraum[all]
```

## Quick Reference

### Run pipeline (CLI)
```bash
# Run on CSV data
dataraum run /path/to/data --output ./output

# Check status
dataraum status ./output

# View entropy dashboard
dataraum entropy ./output

# Evaluate contracts
dataraum contracts ./output

# Inspect graphs and context
dataraum inspect ./output

# List phases
dataraum phases
```

See `docs/CLI.md` for full CLI documentation.

### Run tests

This project uses `pytest-testmon` to only re-run tests affected by code changes. Follow these rules strictly:

**During development (after each code change):**
```bash
# Run only the specific unit test file(s) related to what you changed
uv run pytest tests/unit/path/to/test_file.py -v
```

**After finishing a feature or fix:**
```bash
# Run testmon across unit tests to catch regressions
uv run pytest --testmon tests/unit -q
```

**Before declaring a task done:**
```bash
# Run testmon across the full suite — this is fast when only a few things changed
uv run pytest --testmon tests -q
```

**Rules:**
- **NEVER run `pytest tests/ -v` (full suite without testmon)** — it takes 2+ minutes. The end-of-turn hook uses testmon to catch regressions automatically.
- **Only run specific integration test files** when you changed integration-level code (pipeline phases, DB models, loaders, graph agents). Example: `uv run pytest tests/integration/pipeline/test_import_phase.py -v`
- **Never run the entire `tests/integration/` directory** as a manual step — testmon in the hook handles this.
- Unit tests live in `tests/unit/` (~760 tests, ~13s). Integration tests live in `tests/integration/` (~300 tests, ~2min). Always prefer running unit tests during development.

### Start MCP server
```bash
dataraum-mcp
# or
python -m dataraum.mcp.server
```

### Run migration
```bash
python -m dataraum.storage.migrations up
```
