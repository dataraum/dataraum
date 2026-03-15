# Contributing

## Setup

```bash
# Clone the repository
git clone https://github.com/dataraum/dataraum-context.git
cd dataraum-context

# Install dependencies (requires uv and Python 3.14t free-threaded)
uv sync

# Verify installation
uv run dataraum --help
```

### Python 3.14 Free-Threaded

This project uses Python 3.14 with free-threading (no GIL). The pipeline uses `ThreadPoolExecutor` for true CPU parallelism.

```bash
# Verify free-threading
python -c "import sys; print('Free-threading:', not sys._is_gil_enabled())"
```

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `ANTHROPIC_API_KEY` | For LLM phases | Anthropic API key |
| `DATARAUM_OUTPUT_DIR` | For MCP server | Path to pipeline output directory |
| `PYTHON_GIL` | Recommended | Set to `0` to disable GIL |

## Running Tests

```bash
# Run affected unit tests (fast, uses testmon)
uv run pytest --testmon tests/unit -q

# Run a specific test file
uv run pytest tests/unit/path/to/test_file.py -v

# Run all tests before submitting (unit + integration)
uv run pytest --testmon tests -q

```

**Test structure:**
- `tests/unit/` — ~760 tests, ~13s. Run frequently during development.
- `tests/integration/` — ~300 tests, ~2min. Run when changing cross-module behavior.

## Code Quality

Automated quality gates run via hooks:

| Check | When | Tool |
|-------|------|------|
| Formatting | After every file edit | `ruff format` |
| Linting | End of turn | `ruff check` |
| Type checking | End of turn | `mypy` |
| Affected tests | End of turn | `pytest --testmon` |

```bash
# Manual runs
uv run ruff format src/ tests/
uv run ruff check src/ tests/
uv run mypy src/
```

## Code Patterns

### Error Handling

Use the `Result` type for operations that can fail — not exceptions:

```python
from dataraum.core.models import Result

async def some_operation() -> Result[SomeOutput]:
    try:
        return Result.ok(output, warnings=["minor issue"])
    except SomeExpectedError as e:
        return Result.fail(str(e))
```

### Database Access

Always use context managers for database connections:

```python
with manager.session_scope() as session:       # SQLAlchemy (metadata)
    with manager.duckdb_cursor() as cursor:     # DuckDB (data)
        result = some_operation(cursor, session)
```

### Data Models

Use Pydantic for all data classes. SQLAlchemy models live in `db_models.py` files co-located with their module:

```python
# Pydantic model (business logic)
class ValidationResult(BaseModel):
    status: ValidationStatus
    passed: bool
    message: str

# SQLAlchemy model (persistence, in db_models.py)
class ValidationResultRecord(Base):
    __tablename__ = "validation_results"
    result_id: Mapped[str] = mapped_column(primary_key=True)
    ...
```

### LLM Integration

LLM-powered features extend `LLMFeature` and use tool-based structured output:

```python
class MyAgent(LLMFeature):
    def __init__(self):
        super().__init__(feature_name="my_feature")

    async def analyze(self, schema):
        response = await self.call_llm(
            prompt_template="my_prompt",
            tool_schema=MyOutput.model_json_schema(),
            context={"schema": schema},
        )
```

### Pipeline Phases

New phases extend `BasePhase` and use the `@analysis_phase` decorator:

```python
from dataraum.pipeline.registry import analysis_phase
from dataraum.pipeline.phases.base import BasePhase

@analysis_phase
class MyPhase(BasePhase):
    @property
    def name(self) -> str:
        return "my_phase"

    @property
    def dependencies(self) -> list[str]:
        return ["semantic"]

    @property
    def requires_llm(self) -> bool:
        return False

    def run(self, session, duckdb_conn, source, tables) -> PhaseResult:
        # Phase implementation
        ...
```

Add the phase name to `config/pipeline.yaml` to activate it.

## Project Structure

```
src/dataraum/
├── analysis/          # Data analysis modules (typing, statistics, semantic, etc.)
├── entropy/           # Uncertainty quantification (detectors, contracts, actions)
├── graphs/            # Calculation graphs and context assembly
├── pipeline/          # Pipeline orchestrator and phase registry
├── sources/           # Data source loaders (CSV, Parquet)
├── storage/           # SQLAlchemy base, migrations
├── llm/               # LLM provider abstraction
├── core/              # Config, connections, utilities
├── cli/               # Typer CLI + Textual TUI
└── mcp/               # MCP server (6 tools)
```

Module documentation is in `docs/internals/`.

## Style Guidelines

- Type hints on all functions
- Pydantic models for data classes
- No classes where functions suffice
- Max function length: ~50 lines
- Prefer small, targeted changes over broad rewrites
- Only abstract when you see actual duplication (rule of three)
