# CLI-First UX Plan

> **Status (2026-02-17):** Largely implemented. CLI commands, TUI screens, and MCP server all exist. API was NOT removed (decision reversed in restructuring-plan.md §4.6 — kept as foundation for Web UI). Superseded by [interface-strategy.md](./interface-strategy.md) for future UI work.

## Goal
Streamline to a single CLI module with Textual TUI. Remove HTTP API. Add MCP server for LLM integration.
**Flatten project structure** to root level.

## Decisions Made
- **Remove FastAPI** - User will back up locally. Not needed for CLI/MCP workflow.
- **CLI commands launch Textual** - `dataraum entropy` opens TUI screen, `--no-tui` for raw output
- **MCP calls Python directly** - No HTTP layer, just function calls
- **Flatten to root** - Move `packages/dataraum-api/` contents to root level
- **Defer Jupyter** - Focus on CLI/MCP first. Jupyter is a future project.

---

## What Gets Removed

### FastAPI / api/ Module
Delete entirely (user backs up first):
```
src/dataraum/api/
├── main.py
├── deps.py
├── schemas.py
├── server.py
└── routers/
    ├── sources.py
    ├── tables.py
    ├── context.py
    ├── pipeline.py
    ├── query.py
    ├── entropy.py
    ├── contracts.py
    └── graphs.py
```

Also remove from `pyproject.toml`:
- `dataraum-api` entry point
- `fastapi`, `uvicorn` dependencies (unless needed elsewhere)

### Move to Archive
```
docs/ui/*.md → docs/archive/ui/
```

---

## What Gets Consolidated

### Plan Documents
Move to `docs/plans/`:
```
packages/dataraum-api/docs/plans/column-eligibility.md → docs/plans/column-eligibility.md
packages/dataraum-api/docs/plans/aggregation-framework-roadmap.md → docs/plans/aggregation-framework-roadmap.md
```

---

## New Architecture

### Flattened Project Structure

Move from `packages/dataraum-api/` to root:

```
dataraum-context/                 # Root
├── pyproject.toml               # Single project config
├── src/dataraum/                # Source code
│   ├── cli/                     # Merged CLI + TUI
│   │   ├── __init__.py
│   │   ├── main.py              # Typer app, command definitions
│   │   ├── app.py               # Textual DataraumApp
│   │   ├── screens/
│   │   │   ├── home.py          # Home screen (tables, overview)
│   │   │   ├── entropy.py       # Entropy dashboard
│   │   │   ├── table.py         # Table detail
│   │   │   ├── contracts.py     # Contract evaluation
│   │   │   └── query.py         # Query interface (NL input, streaming)
│   │   ├── widgets/
│   │   │   ├── table_tree.py    # Table list sidebar
│   │   │   └── entropy_summary.py
│   │   └── styles.tcss          # Textual CSS
│   ├── mcp/                     # MCP server (calls Python directly)
│   │   ├── __init__.py
│   │   ├── server.py
│   │   └── formatters.py
│   └── (other modules unchanged)
├── tests/                       # Tests at root
├── config/                      # Config at root
└── docs/                        # Docs at root
```

### Command Behavior

| Command | With TUI (default) | With `--no-tui` |
|---------|-------------------|-----------------|
| `dataraum run` | Rich progress (current) | Rich progress (no change) |
| `dataraum status` | Home screen with drill-down | Rich tables (current behavior) |
| `dataraum entropy` | Entropy dashboard screen | Rich tables (current behavior) |
| `dataraum contracts` | Contract screen with traffic lights | Rich tables (current behavior) |
| `dataraum inspect` | Interactive schema viewer | Rich tables (current behavior) |
| `dataraum query` | Query screen with history, streaming | Simple input → output |

### MCP Server

Calls library functions directly (no HTTP):

```python
# mcp/server.py
from dataraum.graphs.context import build_execution_context, format_context_for_prompt
from dataraum.entropy.views.dashboard_context import build_for_dashboard
from dataraum.entropy.contracts import evaluate_contract

@server.tool()
async def get_context(output_dir: str) -> str:
    """Get data context for AI analysis."""
    manager = ConnectionManager.for_directory(output_dir)
    with manager.session_scope() as session:
        source = get_source(session)
        context = build_execution_context(session, [t.table_id for t in source.tables])
        return format_context_for_prompt(context)

@server.tool()
async def get_entropy(output_dir: str, table_name: str | None = None) -> str:
    """Get entropy analysis."""
    ...
```

---

## Implementation Phases

### Phase 1: Flatten & Remove ✅ COMPLETED (2026-02-03)

**Flatten project structure:**
1. Move `packages/dataraum-api/src/dataraum/` → `src/dataraum/`
2. Move `packages/dataraum-api/tests/` → `tests/`
3. Move `packages/dataraum-api/config/` → `config/`
4. Merge `packages/dataraum-api/pyproject.toml` into root `pyproject.toml`
5. Delete empty `packages/` directory

**Remove API:**
1. User backs up `src/dataraum/api/` locally
2. Delete `api/` module
3. Delete `tests/api/` tests

**Consolidate docs:**
1. Move `docs/ui/*.md` → `docs/archive/ui/`
2. Move `packages/dataraum-api/docs/plans/*.md` → `docs/plans/`
3. Update `CLAUDE.md` and `docs/BACKLOG.md`

### Phase 2: CLI/TUI Integration
1. Add `textual>=0.50.0` dependency
2. Create `cli/` module structure (rename from `cli.py`)
3. Implement Textual screens:
   - `HomeScreen` - table tree, overview, entropy summary
   - `EntropyScreen` - entropy dashboard with drill-down
   - `TableScreen` - column details, semantic annotations
   - `ContractsScreen` - contract evaluation with traffic lights
   - `QueryScreen` - NL input, history, streaming response, SQL preview
4. Add `--no-tui` flag to commands
5. Test with `small_finance` data

### Phase 3: MCP Server
1. Add `mcp>=1.0.0` dependency
2. Create `mcp/` module
3. Implement tools:
   - `get_context` - formatted context document
   - `get_entropy` - entropy dashboard (optional drill-down)
   - `evaluate_contract` - contract evaluation
   - `query` - natural language query with SQL generation
4. Add `dataraum-mcp` entry point
5. Document Claude Desktop configuration

### Phase 4: Documentation
1. Update `CLAUDE.md` with new focus
2. Update `docs/BACKLOG.md` with new priorities
3. Update `docs/CLI.md` with TUI usage
4. Create MCP integration guide

---

## Files Changed

### Deleted
- `packages/` directory (entire monorepo structure)
- `src/dataraum/api/` (entire module, after flattening)
- `tests/api/` (API router tests)

### Moved (Flatten)
- `packages/dataraum-api/src/dataraum/` → `src/dataraum/`
- `packages/dataraum-api/tests/` → `tests/`
- `packages/dataraum-api/config/` → `config/`
- `packages/dataraum-api/pyproject.toml` → merge into root `pyproject.toml`

### Moved (Docs)
- `docs/ui/*.md` → `docs/archive/ui/`
- `packages/dataraum-api/docs/plans/*.md` → `docs/plans/`

### New (Future)
- `src/dataraum/cli/` (module, not single file)
  - `main.py`, `app.py`, `screens/`, `widgets/`, `styles.tcss`
- `src/dataraum/mcp/`
  - `server.py`, `formatters.py`

### Modified
- `pyproject.toml` (root)
  - Remove: `dataraum-api` script, `fastapi`, `uvicorn`
  - Add: `textual>=0.50.0`, `mcp>=1.0.0`, `dataraum-mcp` script
- `CLAUDE.md` - Update focus
- `docs/BACKLOG.md` - Update priorities

---

## Verification

### After Phase 1
```bash
# Verify structure is flattened
ls src/dataraum/  # Should exist
ls packages/      # Should not exist

# Verify api/ is gone
ls src/dataraum/api/  # Should fail

# Verify tests still pass
pytest tests/ -v
```

### After Phase 2
```bash
# Run pipeline
dataraum run tests/integration/fixtures/small_finance -o ./test_output

# Launch TUI (default)
dataraum status ./test_output
# Should open interactive Textual app

# Raw output
dataraum status ./test_output --no-tui
# Should show Rich tables (current behavior)

# Query with TUI
dataraum query ./test_output
# Should open query screen
```

### After Phase 3
```bash
# Start MCP server
python -m dataraum.mcp.server

# Or via entry point
dataraum-mcp

# Test with MCP inspector or Claude Desktop
```

---

## Decisions on Open Questions

1. **Query command**: Textual screen with history, suggestions, streaming response.
2. **API tests**: Deleted along with API module.

## Deferred

- **Pipeline TUI**: Keep `dataraum run` with Rich progress for now. TUI live progress can come later.
- **Jupyter/Python API**: Focus on CLI/MCP first.
