# UI/API Consolidation Plan

## Overview

This plan consolidates the UI/API layer for dataraum-context, integrating:
- **Python backend** (renamed to `dataraum`) with FastAPI
- **TypeScript prototype patterns** (analytics-agents-ts)
- **Web visualizer patterns** (calculation-graphs/web_visualizer)
- **Entropy system** already built
- **MCP Server** (after UI learnings collected)

**Key decisions:**
- Rename `dataraum_context` → `dataraum` during migration
- Build UI first, MCP Server last (learn from UI iteration)
- API grows organically as UI needs emerge
- Configuration Hub as separate phase after core UI

**Two tasks identified:**
1. Phase Timing Metrics (smaller, can do first)
2. UI/API Consolidation (larger, this plan)

---

## Part A: Phase Timing Metrics (Prerequisite)

### Implementation

Add to `src/dataraum_context/pipeline/`:

```python
# metrics.py (new file)
@dataclass
class PhaseMetrics:
    phase_name: str
    start_time: float
    end_time: float | None = None
    tables_processed: int = 0
    rows_processed: int = 0
    llm_calls: int = 0
    llm_tokens_in: int = 0
    llm_tokens_out: int = 0
    timings: dict[str, float] = field(default_factory=dict)

    @property
    def duration_seconds(self) -> float:
        if self.end_time:
            return self.end_time - self.start_time
        return time.time() - self.start_time

@dataclass
class PipelineMetrics:
    source_id: str
    phases: list[PhaseMetrics] = field(default_factory=list)

    def summary(self) -> dict:
        return {
            "total_duration_s": sum(p.duration_seconds for p in self.phases),
            "phases": {p.phase_name: {...} for p in self.phases},
            "totals": {...}
        }
```

**Files to modify:**
- `src/dataraum_context/pipeline/orchestrator.py` - Collect metrics during execution
- `src/dataraum_context/pipeline/runner.py` - Pass metrics to CLI
- `src/dataraum_context/cli.py` - Display metrics summary

**Effort:** ~2-3 hours

---

## Part B: FastAPI Backend

### API Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/v1/sources` | GET, POST | List/create sources |
| `/api/v1/sources/{id}` | GET | Source details |
| `/api/v1/sources/{id}/run` | POST | Trigger pipeline |
| `/api/v1/sources/{id}/status` | GET | Pipeline status |
| `/api/v1/tables` | GET | List tables |
| `/api/v1/tables/{id}` | GET | Table with columns |
| `/api/v1/context/{source_id}` | GET | Full context document |
| `/api/v1/entropy/{source_id}` | GET | Entropy dashboard |
| `/api/v1/graphs` | GET | List transformation graphs |
| `/api/v1/graphs/{id}/execute` | POST | Execute graph |
| `/api/v1/query` | POST | Execute SQL (read-only) |

### File Structure

```
src/dataraum_context/api/
├── __init__.py
├── main.py           # FastAPI app factory
├── schemas.py        # Pydantic models
├── deps.py           # Dependency injection
└── routers/
    ├── sources.py
    ├── pipeline.py
    ├── tables.py
    ├── context.py
    ├── entropy.py
    ├── graphs.py
    └── query.py
```

### Key Implementation

```python
# api/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

def create_app() -> FastAPI:
    app = FastAPI(title="DataRaum Context API", version="1.0.0")
    app.add_middleware(CORSMiddleware, allow_origins=["*"], ...)

    app.include_router(sources.router, prefix="/api/v1")
    app.include_router(pipeline.router, prefix="/api/v1")
    # ... other routers

    return app
```

---

## Part C: MCP Server (Phase 7 - after UI)

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
packages/python/src/dataraum/mcp/
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

# pyproject.toml
[project.scripts]
dataraum-mcp = "dataraum.mcp.server:main"
```

---

## Part D: Frontend Architecture

### Technology Decision

**Recommendation: React** (initial implementation)
- Better Claude Code generation support
- Mature React Flow ecosystem
- More component library options
- Can migrate to Svelte later if desired

**Stack (use latest versions):**
- **React 19** + TypeScript
- **XY Flow** (latest) for graphs
- **TanStack** suite:
  - TanStack Query - server state
  - TanStack Router - type-safe routing
  - TanStack Form - form handling
  - TanStack Table - data tables
  - TanStack Virtual - virtualized lists
- **Zod** - schema validation (matches analytics-agents-ts pattern)
- **Tailwind CSS** + shadcn/ui
- **Vite** (latest) for build

**Python Stack (use latest versions):**
- **FastAPI** (latest) with Pydantic V2
- **SQLAlchemy 2.0** async
- **uvicorn** with ASGI

### Component Structure (from web_visualizer)

**Preserved Patterns:**
1. **Expand/Collapse** - Chevron toggle for calculation breakdown
2. **Drill-Down Modals** - Full-screen overlay with sortable tables
3. **View Modes** - Sidebar navigation between different layouts
4. **Node Types** - TableNode, ColumnNode, FieldNode, MetricNode

**New Components:**
- EntropyDashboard (from `to_dashboard_dict()`)
- PipelineMonitor (WebSocket for real-time)
- ContextViewer (hierarchical tree + graph)
- QualityView (issues list)

### Data Layer

```typescript
// TanStack Router + Query integration
// Routes define data requirements, queries fetch on navigation

// Zod schemas for API responses (mirrors Python Pydantic)
const SourceSchema = z.object({
  source_id: z.string(),
  name: z.string(),
  source_type: z.string(),
  // ...
});

// TanStack Query hooks
const queryKeys = {
  sources: ['sources'],
  source: (id: string) => ['sources', id],
  tables: (sourceId: string) => ['tables', { sourceId }],
  context: (sourceId: string) => ['context', sourceId],
  entropy: (sourceId: string) => ['entropy', sourceId],
  pipelineStatus: (sourceId: string) => ['pipeline', sourceId, 'status'],
};

// API client generated from OpenAPI + validated with Zod
// npx openapi-typescript http://localhost:8000/openapi.json -o src/api/types.ts
```

---

## Part D2: Configuration Hub (Phase 6)

**Purpose:** Enable human-in-the-loop refinement of automated analysis.

### Configuration Areas

| Area | UI Component | Backend |
|------|--------------|---------|
| **Ontology Selection** | Dropdown selector | `config/ontologies/*.yaml` |
| **Semantic Overrides** | Column → concept mapping editor | `PUT /api/v1/columns/{id}/annotation` |
| **Entropy Thresholds** | Slider controls | `config/entropy/thresholds.yaml` |
| **Quality Rules** | Toggle switches + severity dropdowns | `config/rules/*.yaml` |

### UI Components

```
ConfigurationHub/
├── OntologySelector.tsx      # Choose domain context
├── SemanticEditor.tsx        # Inline edit business terms
│   ├── ColumnMappingRow.tsx  # Single column config
│   └── BulkEditModal.tsx     # Multi-column updates
├── ThresholdEditor.tsx       # Entropy threshold sliders
└── QualityRulesPanel.tsx     # Enable/disable rules
```

### Interaction Pattern

1. User runs pipeline → sees entropy/quality issues
2. User opens Configuration Hub
3. User adjusts semantic mappings or thresholds
4. Changes saved via API
5. User can re-run affected phases (not full pipeline)
6. UI reflects updated analysis

---

## Part E: Project Structure

```
dataraum-context/
├── packages/
│   ├── python/                    # Move existing src/ here
│   │   ├── src/dataraum/          # RENAMED from dataraum_context
│   │   │   ├── api/              # NEW: FastAPI
│   │   │   ├── mcp/              # NEW: MCP Server (Phase 7)
│   │   │   └── ...               # Existing modules
│   │   ├── tests/
│   │   └── pyproject.toml        # Update package name to "dataraum"
│   │
│   └── web/                       # NEW: Frontend (standalone npm project)
│       ├── src/
│       │   ├── api/              # Generated client
│       │   ├── components/
│       │   ├── hooks/
│       │   └── views/
│       ├── package.json          # npm (not workspace)
│       └── vite.config.ts
│
├── config/                        # Shared configs (YAML)
├── docs/
└── README.md
```

**No monorepo tooling needed:**
- `packages/python/` → standalone uv project
- `packages/web/` → standalone npm project
- Coordination: just run API first, then frontend

**Migration steps for rename:**
1. `git mv src/dataraum_context packages/python/src/dataraum`
2. Update all imports (sed/find-replace)
3. Update pyproject.toml `name = "dataraum"`
4. Update CLI entry point to `dataraum`

**Build Tooling:** npm (for Node) + uv (for Python)

---

## Implementation Phases

### Phase 1: Foundation (Week 1)
- [ ] Implement Phase Timing Metrics (prerequisite)
- [ ] Restructure: move to `packages/` + rename `dataraum_context` → `dataraum`
- [ ] FastAPI app skeleton with dependency injection
- [ ] Core Pydantic schemas
- [ ] Source and pipeline endpoints

### Phase 2: Metadata API (Week 2)
- [ ] Table/Column endpoints
- [ ] Context document endpoint (uses GraphExecutionContext)
- [ ] Entropy dashboard endpoint (uses to_dashboard_dict())
- [ ] Query execution endpoint
- [ ] *Additional endpoints added organically as UI needs emerge*

### Phase 3: Frontend Foundation (Week 3)
- [ ] Vite + React 19 + TypeScript scaffold
- [ ] API client generation from OpenAPI + Zod schemas
- [ ] TanStack setup (Query, Router, Form, Table, Virtual)
- [ ] Tailwind + shadcn/ui components

### Phase 4: Core UI (Week 4)
- [ ] Layout (Sidebar + MainContent)
- [ ] Source selector
- [ ] Pipeline monitor with WebSocket
- [ ] Table browser (schema view)

### Phase 5: Advanced UI (Week 5)
- [ ] Entropy dashboard (heatmap, readiness indicators)
- [ ] Graph visualization (React Flow)
- [ ] Drill-down modals (from web_visualizer)
- [ ] Quality view

### Phase 6: Configuration Hub (Week 6)
- [ ] Ontology selector
- [ ] Semantic overrides editor (column → business_concept)
- [ ] Entropy threshold configuration
- [ ] Quality rule toggles

### Phase 7: MCP Server (Week 7)
*Built after UI learnings collected*
- [ ] MCP server skeleton
- [ ] `get_context` tool (wraps context API)
- [ ] `query` tool (wraps query API)
- [ ] `get_metrics` tool
- [ ] `annotate` tool
- [ ] Claude Desktop configuration docs

### Phase 8: Polish & Integration (Week 8)
- [ ] End-to-end testing (Playwright)
- [ ] Error boundaries
- [ ] Export functionality (CSV/JSON)
- [ ] Documentation

---

## Critical Files

| File | Purpose |
|------|---------|
| `src/dataraum_context/graphs/context.py` | GraphExecutionContext - core API data structure |
| `src/dataraum_context/entropy/models.py` | EntropyContext.to_dashboard_dict() for entropy API |
| `src/dataraum_context/pipeline/status.py` | PipelineStatus for monitoring |
| `src/dataraum_context/pipeline/orchestrator.py` | Add metrics collection |
| `prototypes/calculation-graphs/web_visualizer/src/components/` | UI patterns to preserve |
| `prototypes/analytics-agents-ts/prompts/` | Prompt patterns for LLM features |
| `config/entropy/thresholds.yaml` | Entropy threshold configuration |
| `config/ontologies/` | Domain ontology definitions |
| `config/semantic_overrides.yaml` | Manual semantic mappings |

---

## Verification

1. **API Testing:**
   - `pytest tests/api/` for endpoint tests
   - Swagger UI at `/docs` for manual testing

2. **MCP Testing:**
   - Claude Desktop integration test
   - `mcp dev` for local testing

3. **Frontend Testing:**
   - Playwright for E2E tests
   - Storybook for component isolation

4. **Integration:**
   - Run full pipeline, verify API serves correct data
   - Connect frontend to API, verify all views render
