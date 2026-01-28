# UI/API Consolidation Plan

## Overview

This plan consolidates the UI/API layer for dataraum-context, integrating:
- **Python backend** (`dataraum`) with FastAPI
- **TypeScript prototype patterns** (analytics-agents-ts)
- **Web visualizer patterns** (calculation-graphs/web_visualizer)
- **Entropy system** already built
- **Query Agent with RAG-based query reuse** (core innovation)
- **MCP Server** (after UI learnings collected)

**Key decisions:**
- Package renamed to `dataraum` (completed)
- **End-to-end to Query Agent first** - validate core assumptions before Configuration Hub
- Query RAG approach over fresh SQL generation (stabilizes entropy faster)
- API grows organically as UI needs emerge

**Related:** [Query Agent Architecture](./query-agent-architecture.md) - detailed design for RAG-based query reuse

---

## Core Architectural Idea: Query RAG

**Problem:** Traditional LLM query agents generate SQL from scratch each time. Every query is "novel" from an entropy perspective—no learning accumulates.

**Solution:** Build a library of validated query patterns, search by semantic similarity, adapt existing queries rather than generating fresh.

```
User Question → Search Library → Adapt Match → Execute → Save/Update
                     ↓
              (no match? generate with entropy awareness)
```

**Why this matters:**
- Reused queries have **known entropy profiles** (validated assumptions, documented null handling)
- Entropy **stabilizes with reuse** instead of staying high
- Creates a flywheel: more usage → more validation → lower entropy → higher trust
- The graph visualization (from web_visualizer) serves dual purpose: human understanding AND query library seeding

**Existing modules to adapt:**
- `graphs/` - Pre-defined calculation graphs become query library seeds
- `web_visualizer` - Graph UI shows calculation lineage, enables "save as query"
- `analytics-agents-ts` - Prompt patterns for SQL generation, chart recommendations

See [Query Agent Architecture](./query-agent-architecture.md) for full design.

---

## Current Status (2026-01-23, Updated)

### Completed

- [x] **Restructure** - Moved to `packages/dataraum-api/src/dataraum/`
- [x] **FastAPI app skeleton** - `api/main.py` with lifespan management
- [x] **Core Pydantic schemas** - `api/schemas.py`
- [x] **Dependency injection** - `api/deps.py` using core ConnectionManager
- [x] **Source endpoints** - GET /sources, GET /sources/{id}, POST /sources
- [x] **Pipeline endpoints** - POST /sources/{id}/run, GET /sources/{id}/status
- [x] **SSE progress streaming** - GET /runs/{run_id}/stream (real-time updates)
- [x] **Table endpoints** - GET /tables, GET /tables/{id}, GET /columns/{id}
- [x] **Query endpoint** - POST /query (read-only SQL execution)
- [x] **Singleton execution** - Only one pipeline at a time
- [x] **Startup cleanup** - Marks interrupted runs on server start
- [x] **Free-threading support** - Python 3.14t with `-Xgil=0`
- [x] **API test suite** - 31 tests covering all implemented endpoints
- [x] **Unified Query Document System** - QueryDocument model for consistent storage/retrieval

### Recent: Unified Query Document System

Implemented a unified `QueryDocument` model that both Graph Agent and Query Agent use:

- **`query/document.py`** - `QueryDocument`, `SQLStep`, `QueryAssumptionData` dataclasses
- **`query/embeddings.py`** - `build_embedding_text()` with priority-based truncation (summary > steps > assumptions)
- **`query/library.py`** - `save()` now requires `QueryDocument`, `LibraryMatch.to_context()` for LLM injection
- **`query/db_models.py`** - Added `summary` and `steps_json` to `QueryLibraryEntry`
- **`graphs/agent.py`** - Fixed: `summary` now saved to DB (was generated but not persisted)
- **`query/models.py`** - Added required `summary` field to `QueryAnalysisOutput`
- **API schema** - `QueryLibrarySaveRequest` now requires `summary` field

Key design decisions:
- No backward compatibility - clean break, `summary` is required everywhere
- Embedding uses model's 256-token limit efficiently via priority truncation
- `LibraryMatch.to_context()` returns full document for RAG context injection

### In Progress

- [ ] **Context endpoint** - GET /context/{source_id} (needs GraphExecutionContext integration)
- [ ] **Entropy endpoint** - GET /entropy/{source_id} (needs to_dashboard_dict() integration)
- [ ] **Graphs endpoint** - GET /graphs, POST /graphs/{id}/execute (partial)

### Not Started

- [ ] Frontend (Phase 3-5)
- [ ] Configuration Hub (Phase 6)
- [ ] MCP Server (Phase 7)

---

## Part A: Remaining API Work

### Context Endpoint

```python
# api/routers/context.py
@router.get("/context/{source_id}")
def get_context(source_id: str, session: SessionDep) -> ContextResponse:
    """Return full context document for LLM consumption."""
    # Uses GraphExecutionContext from graphs/context.py
    pass
```

### Entropy Endpoint

```python
# api/routers/entropy.py
@router.get("/entropy/{source_id}")
def get_entropy_dashboard(source_id: str, session: SessionDep) -> EntropyDashboardResponse:
    """Return entropy dashboard data."""
    # Uses EntropyContext.to_dashboard_dict() from entropy/models.py
    pass
```

### Graphs Endpoint

```python
# api/routers/graphs.py
@router.get("/graphs")
def list_graphs(source_id: str, session: SessionDep) -> GraphListResponse:
    """List available transformation graphs."""
    pass

@router.post("/graphs/{graph_id}/execute")
def execute_graph(graph_id: str, request: GraphExecuteRequest) -> GraphExecuteResponse:
    """Execute a graph and return results."""
    pass
```

---

## Part B: MCP Server (Phase 7 - after UI)

**Rationale for building last:**
- UI iteration validates API contracts first
- Same data structures serve both REST and MCP
- Easier to debug HTTP than stdio-based tools
- Learnings from UI inform better tool design

### 4 Tools (per CLAUDE.md)

| Tool | Purpose | Wraps API |
|------|---------|-----------|
| `get_context` | Primary context retrieval | `GET /api/v1/context/{source_id}` |
| `query` | Execute read-only SQL | `POST /api/v1/query` |
| `get_metrics` | Available metrics for ontology | `GET /api/v1/graphs` + metadata |
| `annotate` | Update semantic annotation | `PUT /api/v1/columns/{id}/annotation` |

### File Structure

```
src/dataraum/mcp/
├── __init__.py
├── server.py         # Main MCP server
├── tools/
│   ├── context.py    # Wraps context router
│   ├── query.py      # Wraps query router
│   ├── metrics.py    # Wraps graphs + ontology
│   └── annotate.py   # Wraps annotation endpoint
└── formatting.py     # Output formatters (markdown, json)
```

### Entry Point

```python
# mcp/server.py
from mcp.server import Server

class DataRaumMCPServer:
    def __init__(self, output_dir: Path):
        self.server = Server("dataraum")
        self.manager = ConnectionManager(ConnectionConfig.for_directory(output_dir))
        self._register_tools()

# pyproject.toml already has:
# dataraum-mcp = "dataraum.mcp.server:main"  # TODO: implement
```

---

## Part C: Frontend Architecture

### Technology Stack

**Frontend:**
- **React 19** + TypeScript
- **XY Flow** for graphs
- **TanStack** suite (Query, Router, Form, Table, Virtual)
- **Zod** - schema validation
- **Tailwind CSS** + shadcn/ui
- **Vite** for build

**Backend (completed):**
- **FastAPI** with Pydantic V2
- **SQLAlchemy 2.0** sync (works well with free-threading)
- **uvicorn** with `-Xgil=0`

### Component Structure

**From web_visualizer patterns:**
1. **Expand/Collapse** - Chevron toggle for calculation breakdown
2. **Drill-Down Modals** - Full-screen overlay with sortable tables
3. **View Modes** - Sidebar navigation between different layouts
4. **Node Types** - TableNode, ColumnNode, FieldNode, MetricNode

**New Components:**
- EntropyDashboard (from `to_dashboard_dict()`)
- PipelineMonitor (SSE for real-time progress)
- ContextViewer (hierarchical tree + graph)
- QualityView (issues list)

### Data Layer

```typescript
// TanStack Query hooks
const queryKeys = {
  sources: ['sources'],
  source: (id: string) => ['sources', id],
  tables: (sourceId: string) => ['tables', { sourceId }],
  context: (sourceId: string) => ['context', sourceId],
  entropy: (sourceId: string) => ['entropy', sourceId],
  pipelineStatus: (sourceId: string) => ['pipeline', sourceId, 'status'],
};

// SSE for pipeline progress
const usePipelineStream = (runId: string) => {
  // Connect to GET /api/v1/runs/{runId}/stream
  // Parse SSE events: start, phase_complete, phase_failed, complete, error
};
```

---

## Part D: Configuration Hub (Phase 6)

**Purpose:** Enable human-in-the-loop refinement of automated analysis.

### Configuration Areas

| Area | UI Component | Backend |
|------|--------------|---------|
| **Ontology Selection** | Dropdown selector | `config/ontologies/*.yaml` |
| **Semantic Overrides** | Column → concept mapping editor | `PUT /api/v1/columns/{id}/annotation` |
| **Entropy Thresholds** | Slider controls | `config/entropy/thresholds.yaml` |
| **Quality Rules** | Toggle switches + severity dropdowns | `config/rules/*.yaml` |

---

## Part E: Project Structure (Current)

```
dataraum-context/
├── packages/
│   └── dataraum-api/                # Python backend
│       ├── src/dataraum/
│       │   ├── api/                 # FastAPI (DONE)
│       │   │   ├── main.py
│       │   │   ├── schemas.py
│       │   │   ├── deps.py
│       │   │   ├── server.py
│       │   │   └── routers/
│       │   │       ├── sources.py   # DONE
│       │   │       ├── pipeline.py  # DONE
│       │   │       ├── tables.py    # DONE
│       │   │       ├── query.py     # DONE
│       │   │       ├── context.py   # TODO: integrate
│       │   │       ├── entropy.py   # TODO: integrate
│       │   │       └── graphs.py    # TODO: integrate
│       │   ├── mcp/                 # TODO: MCP Server
│       │   ├── pipeline/
│       │   ├── entropy/
│       │   ├── graphs/
│       │   └── ...
│       ├── tests/
│       │   └── api/                 # 31 tests (DONE)
│       └── pyproject.toml
│
├── packages/web/                    # TODO: Frontend
│
├── config/
├── docs/
│   └── plans/
│       └── ui-api-consolidation.md  # This file
└── README.md
```

---

## Implementation Phases (Revised 2026-01-23)

**Strategy:** CLI-first validation of Query Agent and Contracts, then UI.

**Key Insight:** The CLI already calls library functions directly (not API). The query agent should follow the same pattern—library function first, with CLI and API as thin wrappers.

See [Query Agent Architecture](./query-agent-architecture.md) for details on:
- RAG-based query reuse
- Contract-based confidence levels (🟢🟡🟠🔴)
- Complete test flow with CLI commands

### Phase 1: API Foundation ✅ COMPLETE
- [x] FastAPI app skeleton with dependency injection
- [x] Core Pydantic schemas
- [x] Source endpoints (CRUD)
- [x] Pipeline endpoints (trigger, status, SSE stream)
- [x] Table/Column endpoints
- [x] Query endpoint (raw SQL)
- [x] Singleton execution + startup cleanup
- [x] Free-threading support
- [x] API test suite (31 tests)

### Phase 2: Contract Implementation ✅ COMPLETE
*Implement contracts to enable confidence-level responses*
- [x] Create `entropy/contracts.py` with full contract evaluation
- [x] Load contracts from `config/entropy/contracts.yaml` (fail-fast if missing)
- [x] Implement `evaluate_contract()` function
- [x] Implement `_calculate_confidence_level()` (GREEN/YELLOW/ORANGE/RED)
- [x] Add `dataraum contracts` CLI command
- [x] API endpoints: `GET /api/v1/contracts`, `GET /api/v1/contracts/{name}`, `GET /api/v1/contracts/{name}/evaluate`, `GET /api/v1/sources/{source_id}/contracts`
- [x] 26 tests covering all contract functionality

### Phase 3: Query Agent Core ✅ COMPLETE
*The core library that CLI/API/MCP all call*
- [x] Create `query/` module structure
- [x] Implement `answer_question()` library function
- [x] Create `QueryAgent` class extending LLMFeature
- [x] Create `query_analysis.yaml` prompt template
- [x] Assumption tracking with QueryAssumption model
- [x] Contract-based confidence levels in responses
- [x] Query library schema with `QueryDocument` model
- [x] Unified embedding with `build_embedding_text()` (priority: summary > steps > assumptions)
- [x] `QueryLibrary.save()` requires `QueryDocument`
- [x] `LibraryMatch.to_context()` for RAG context injection
- [x] Graph Agent: `summary` now persisted to DB
- [ ] Seed library with existing graph definitions

### Phase 4: Query Agent CLI ✅ COMPLETE
*CLI wrapper for fast iteration and testing*
- [x] Add `dataraum query "..."` command
- [x] Contract selection: `--contract NAME`
- [x] Auto-contract: `--auto-contract`
- [x] Show SQL: `--show-sql`
- [ ] Behavior modes: `--mode strict|balanced|lenient`
- [ ] Interactive REPL: `dataraum query --interactive`
- [ ] Save to library: `--save NAME`

### Phase 5: Query Agent API ⏳ IN PROGRESS
*HTTP wrapper for UI/external consumption*
- [x] `POST /api/v1/query/agent` endpoint
- [x] `GET /api/v1/query/library/{source_id}` (list saved queries)
- [x] `POST /api/v1/query/library/{source_id}` (save query - requires `summary`)
- [x] `POST /api/v1/query/library/{source_id}/search` (similarity search)
- [ ] Context endpoint integration (existing)
- [ ] Entropy endpoint integration (existing)

### Phase 6: Frontend Foundation
- [ ] Vite + React 19 + TypeScript scaffold
- [ ] API client generation from OpenAPI + Zod schemas
- [ ] TanStack setup (Query, Router, Form, Table, Virtual)
- [ ] Tailwind + shadcn/ui components

### Phase 7: Core UI
- [ ] Layout (Sidebar + MainContent)
- [ ] Source selector + Pipeline monitor (SSE)
- [ ] Schema browser (tables/columns)
- [ ] Entropy dashboard with traffic light indicators
- [ ] Contract compliance view

### Phase 8: Query Agent UI
- [ ] Query input with contract selector
- [ ] Confidence level display (traffic light)
- [ ] Assumption disclosure panel
- [ ] SQL preview and results table
- [ ] Save/update query workflow
- [ ] Query library browser

### Phase 9: Graph Visualization
- [ ] Adapt web_visualizer to live API
- [ ] MetricNode with entropy indicators
- [ ] Calculation breakdown (expand/collapse)
- [ ] Chart recommendations (from analytics-agents-ts patterns)

### Phase 10: Configuration Hub
*Only after validating Query Agent assumptions*
- [ ] Ontology selector
- [ ] Semantic overrides editor
- [ ] Entropy threshold configuration
- [ ] Quality rule toggles

### Phase 11: MCP Server
- [ ] MCP server skeleton
- [ ] `get_context` tool (wraps context API)
- [ ] `query` tool (wraps answer_question())
- [ ] `get_metrics` tool
- [ ] `annotate` tool
- [ ] Claude Desktop configuration docs

### Phase 12: Polish & Integration
- [ ] End-to-end testing (Playwright)
- [ ] Error boundaries
- [ ] Export functionality (CSV/JSON)
- [ ] Documentation
- [ ] Query library analytics (usage, validation rates)

---

## Test Flow Summary

The complete test flow validates the system end-to-end via CLI:

```bash
# 1. Import data
dataraum run ./data/financial --output ./output

# 2. Check contract compliance
dataraum contracts ./output
# → Shows which contracts pass/fail

# 3. Query with confidence levels
dataraum query "What was revenue?" -o ./output
# → 🟢 Answer with confidence level

dataraum query "What was revenue?" -o ./output --contract regulatory_reporting
# → 🔴 BLOCKED (shows why and how to fix)

# 4. Interactive exploration
dataraum query --interactive -o ./output
```

See [Query Agent Architecture](./query-agent-architecture.md) for detailed test scenarios.

---

## API Reference (Implemented)

### Sources
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/sources` | List sources with pagination |
| GET | `/api/v1/sources/{id}` | Get source details |
| POST | `/api/v1/sources` | Create new source |

### Pipeline
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/sources/{id}/run` | Trigger pipeline (returns run_id) |
| GET | `/api/v1/sources/{id}/status` | Get pipeline status |
| GET | `/api/v1/runs/{run_id}/stream` | SSE progress stream |

### Tables
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/tables` | List tables (filter by source_id) |
| GET | `/api/v1/tables/{id}` | Get table with columns |
| GET | `/api/v1/columns/{id}` | Get column details |

### Query
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/query` | Execute read-only SQL |

## API Reference (Planned)

### Query Agent
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/query/agent` | Answer question with Query Agent |
| GET | `/api/v1/query/library` | List saved queries |
| POST | `/api/v1/query/library` | Save query to library |
| GET | `/api/v1/query/library/{id}` | Get query details |

### Contracts
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/contracts` | List all contracts |
| GET | `/api/v1/contracts/{name}` | Get contract definition |
| GET | `/api/v1/contracts/{name}/evaluate` | Evaluate contract for source |

### Context & Entropy (Needs Integration)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/context/{source_id}` | Get execution context |
| GET | `/api/v1/entropy/{source_id}` | Get entropy dashboard data |
| GET | `/api/v1/entropy/{source_id}/columns/{column_id}` | Get column entropy |

---

## CLI Reference

### Existing Commands
| Command | Description |
|---------|-------------|
| `dataraum run SOURCE` | Import data and run pipeline |
| `dataraum status DIR` | Show pipeline status |
| `dataraum inspect DIR` | Show context and entropy |
| `dataraum phases` | List available phases |
| `dataraum reset DIR` | Delete databases |

### Planned Commands
| Command | Description |
|---------|-------------|
| `dataraum contracts DIR` | Evaluate all contracts |
| `dataraum contracts DIR --contract NAME` | Evaluate specific contract |
| `dataraum query "..." -o DIR` | Ask a question |
| `dataraum query "..." --contract NAME` | Query with specific contract |
| `dataraum query "..." --auto-contract` | Auto-select best contract |
| `dataraum query --interactive` | Interactive REPL mode |

---

## Running the API

```bash
# With free-threading (recommended)
uv run python -Xgil=0 -m dataraum.api.server

# Or via script (warns if GIL enabled)
uv run dataraum-api

# Environment variables
DATARAUM_OUTPUT_DIR=./pipeline_output  # Required
DATARAUM_API_HOST=127.0.0.1            # Default
DATARAUM_API_PORT=8000                 # Default
DATARAUM_API_RELOAD=false              # Set true for dev
```

---

## Critical Files

| File | Purpose |
|------|---------|
| `src/dataraum/api/main.py` | FastAPI app factory with lifespan |
| `src/dataraum/api/server.py` | Entry point with free-threading |
| `src/dataraum/api/routers/pipeline.py` | SSE streaming, singleton execution |
| `src/dataraum/graphs/context.py` | GraphExecutionContext - for context API |
| `src/dataraum/entropy/models.py` | EntropyContext.to_dashboard_dict() |
| `src/dataraum/pipeline/orchestrator.py` | Pipeline execution with run_id |
| `tests/api/conftest.py` | Test fixtures (test_client, seeded_source, seeded_tables) |
