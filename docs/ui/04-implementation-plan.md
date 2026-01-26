# Dataraum UI: Implementation Plan

## Overview

This document outlines an 8-week implementation plan for the dataraum UI, focusing on incremental delivery with working software at each phase.

**Key principle:** Each phase produces a deployable, testable increment.

---

## Technology Summary

| Component | Technology | Notes |
|-----------|------------|-------|
| Server | FastAPI + Jinja2 | Python 3.11+ |
| CSS | daisyUI + Tailwind | Build-time only |
| Hypermedia | HTMX 4.x | CDN or bundled |
| Client state | Alpine.js | CDN or bundled |
| Data format | Apache Arrow | Zero-copy from DuckDB |
| Tables | regular-table | ~10KB, FINOS |
| Charts | Vega-Lite + vega-loader-arrow | Declarative, Arrow-native |
| Graphs | Cytoscape.js | Schema visualization |
| Code editor | CodeMirror 6 | SQL/YAML modes |

---

## Phase 0: Foundation (Week 1)

### Goals
- Project structure established
- Build pipeline working
- Basic shell rendering

### Deliverables

```
dataraum-ui/
├── app/
│   ├── __init__.py
│   ├── main.py
│   ├── config.py
│   └── templates/
│       ├── base.html
│       └── index.html
├── static/
│   ├── css/
│   │   └── app.css          # Compiled
│   └── js/
│       ├── htmx.min.js
│       └── alpine.min.js
├── styles/
│   └── app.css              # Tailwind source
├── tailwind.config.js
├── package.json
├── pyproject.toml
└── README.md
```

### Tasks

1. **Project setup**
   - [ ] Create repository structure
   - [ ] Configure pyproject.toml with dependencies
   - [ ] Configure package.json for Tailwind build

2. **FastAPI shell**
   - [ ] Basic app with health endpoint
   - [ ] Jinja2 template configuration
   - [ ] Static file serving

3. **Tailwind + daisyUI build**
   - [ ] Configure tailwind.config.js with daisyUI
   - [ ] Create base styles
   - [ ] Add build script to package.json

4. **Base template**
   - [ ] Shell layout (header, sidebar, main, footer)
   - [ ] HTMX configuration
   - [ ] Alpine.js integration
   - [ ] Theme toggle (light/dark)

### Validation
- [ ] `uvicorn app.main:app` serves the shell
- [ ] `npm run build` compiles CSS
- [ ] Theme toggle works
- [ ] HTMX requests work (test with simple endpoint)

---

## Phase 1: Session & Messaging (Weeks 2-3)

### Goals
- Sessions can be created and persisted
- Messages can be sent and displayed
- Basic SSE streaming works

### Deliverables

```
app/
├── routes/
│   ├── sessions.py          # Session CRUD
│   └── messages.py          # Message handling
├── models/
│   ├── session.py
│   └── message.py
├── services/
│   └── session_store.py     # In-memory or SQLite
└── templates/
    ├── session.html
    └── messages/
        ├── user.html
        └── agent.html
```

### Tasks

1. **Session management**
   - [ ] Session model with ID, timestamps
   - [ ] In-memory session store (upgrade to Redis later)
   - [ ] Create/get/delete endpoints
   - [ ] Session persistence between requests

2. **Message handling**
   - [ ] Message model (role, content, artifacts, actions)
   - [ ] POST endpoint for user messages
   - [ ] Message templates (user, agent, system)
   - [ ] Message list rendering

3. **SSE streaming**
   - [ ] StreamingResponse setup
   - [ ] SSE event formatting
   - [ ] HTMX SSE consumption
   - [ ] Thinking indicator

4. **Basic agent integration**
   - [ ] Mock agent that echoes input
   - [ ] Streaming text chunks
   - [ ] Replace with real agent later

### Validation
- [ ] Create session, see welcome message
- [ ] Send message, see it rendered
- [ ] Agent response streams in chunks
- [ ] Refresh page, session persists

---

## Phase 2: Artifacts & Actions (Weeks 3-4)

### Goals
- Artifacts render correctly
- Actions are derived and executable
- Arrow data flows to frontend

### Deliverables

```
app/
├── models/
│   ├── artifact.py
│   └── action.py
├── services/
│   └── actions.py           # Action derivation
└── templates/
    ├── artifacts/
    │   ├── table.html
    │   ├── code.html
    │   └── error.html
    └── partials/
        ├── action_button.html
        └── actions.html

static/js/islands/
└── arrow-table.ts           # regular-table + Arrow
```

### Tasks

1. **Artifact system**
   - [ ] Artifact model (type, title, data, metadata)
   - [ ] Artifact serialization for templates
   - [ ] Artifact templates (table, code, error)

2. **Arrow table component**
   - [ ] regular-table web component wrapper
   - [ ] Arrow IPC loading
   - [ ] Virtual scrolling verification
   - [ ] Row selection events

3. **Arrow data endpoint**
   - [ ] `/sessions/{id}/artifacts/{aid}/arrow` endpoint
   - [ ] DuckDB → Arrow serialization
   - [ ] Content-Type: application/vnd.apache.arrow.stream
   - [ ] HTMX extension for Arrow responses

4. **Action system**
   - [ ] Action model (rel, href, style, confirm)
   - [ ] Action derivation service (mock)
   - [ ] Action button template
   - [ ] Action execution endpoint

### Validation
- [ ] Table artifact renders with virtual scroll
- [ ] 100K rows scroll smoothly
- [ ] Actions appear below artifacts
- [ ] Clicking action triggers request

---

## Phase 3: Charts & Entropy (Weeks 4-5)

### Goals
- Vega-Lite charts render from Arrow data
- Entropy report displays correctly
- Chart DSL works for agent

### Deliverables

```
app/
├── models/
│   └── chart.py             # Chart DSL
├── services/
│   └── entropy.py           # Entropy computation
└── templates/
    └── artifacts/
        ├── chart.html
        └── entropy.html

static/js/islands/
└── vega-chart.ts            # Vega-Lite + vega-loader-arrow
```

### Tasks

1. **Chart DSL**
   - [ ] DataraumChart dataclass
   - [ ] to_vega_lite() compilation
   - [ ] Altair validation
   - [ ] Supported chart types (bar, line, radar, heatmap)

2. **Vega chart component**
   - [ ] vega-embed web component wrapper
   - [ ] vega-loader-arrow integration
   - [ ] Arrow URL attribute
   - [ ] Click events for drill-down
   - [ ] Export (PNG, SVG)

3. **Entropy visualization**
   - [ ] Entropy report artifact type
   - [ ] Radar chart for dimensions
   - [ ] Score badges
   - [ ] Issues list with actions

4. **Chart + Arrow endpoint**
   - [ ] Same Arrow endpoint serves charts
   - [ ] Vega-Lite spec in artifact.data
   - [ ] Arrow URL in artifact metadata

### Validation
- [ ] Bar chart renders from Arrow data
- [ ] Radar chart shows entropy dimensions
- [ ] Chart click triggers HTMX request
- [ ] Export produces valid image

---

## Phase 4: Context Panel & Schema (Weeks 5-6)

### Goals
- Context panel shows live state
- Schema explorer is interactive
- Multi-target updates work

### Deliverables

```
app/
├── routes/
│   └── schema.py            # Schema endpoints
└── templates/
    ├── partials/
    │   ├── schema_tree.html
    │   └── entropy_summary.html
    └── context/
        ├── connection.html
        └── undo_stack.html

static/js/islands/
└── graph-viewer.ts          # Cytoscape.js
```

### Tasks

1. **Schema explorer**
   - [ ] Schema tree component
   - [ ] Table/column hierarchy
   - [ ] Click to analyze column
   - [ ] Lazy loading for large schemas

2. **Schema graph**
   - [ ] Cytoscape.js web component
   - [ ] Node/edge data from schema
   - [ ] Layout options (dagre, circle, cose)
   - [ ] Node click → column details

3. **Entropy summary**
   - [ ] Mini radar chart in context panel
   - [ ] Score badge with delta
   - [ ] Link to full report

4. **Multi-target updates**
   - [ ] `<hx-partial>` for context updates
   - [ ] SSE events update multiple targets
   - [ ] Test: action updates both canvas and context

### Validation
- [ ] Schema tree shows tables/columns
- [ ] Graph renders with relationships
- [ ] Entropy updates after fix
- [ ] Multi-target SSE works

---

## Phase 5: Mutations & Undo (Weeks 6-7)

### Goals
- Fix actions execute with preview
- Changes are tracked
- Undo works

### Deliverables

```
app/
├── models/
│   └── change.py            # Change tracking
├── services/
│   ├── mutations.py         # Execute mutations
│   └── undo.py              # Undo logic
└── templates/
    └── partials/
        ├── preview_dialog.html
        └── undo_stack.html
```

### Tasks

1. **Preview system**
   - [ ] Preview SQL generation
   - [ ] Affected row count
   - [ ] Preview dialog template
   - [ ] Cancel/Apply buttons

2. **Mutation execution**
   - [ ] Execute SQL safely
   - [ ] Record change with undo SQL
   - [ ] Update entropy after mutation
   - [ ] Stream result to UI

3. **Undo system**
   - [ ] Change stack in session
   - [ ] Undo endpoint
   - [ ] Undo SQL execution
   - [ ] UI updates after undo

4. **Undo UI**
   - [ ] Undo stack in context panel
   - [ ] "Undo" action after mutations
   - [ ] Change history view

### Validation
- [ ] Fix action shows preview
- [ ] Apply executes and shows result
- [ ] Undo reverts change
- [ ] Entropy reflects current state

---

## Phase 6: Code Editor & SQL (Week 7)

### Goals
- SQL editor works
- Run/Explain queries
- Results display as artifacts

### Deliverables

```
app/
├── routes/
│   └── query.py             # Query execution
└── templates/
    └── artifacts/
        └── editor.html

static/js/islands/
└── sql-editor.ts            # CodeMirror 6
```

### Tasks

1. **SQL editor component**
   - [ ] CodeMirror 6 web component
   - [ ] SQL syntax highlighting
   - [ ] Value binding for forms
   - [ ] Change events

2. **Query execution**
   - [ ] Run query endpoint
   - [ ] Read-only validation
   - [ ] Timeout handling
   - [ ] Result as table artifact

3. **Query explain**
   - [ ] Explain endpoint
   - [ ] Explain output formatting
   - [ ] Inline display

4. **Editor artifact**
   - [ ] Editor template with CodeMirror
   - [ ] Run/Explain buttons
   - [ ] Dirty state tracking

### Validation
- [ ] SQL editor has syntax highlighting
- [ ] Run produces table artifact
- [ ] Explain shows query plan
- [ ] Large results paginate

---

## Phase 7: Agent Integration (Week 8)

### Goals
- Real agent generates responses
- Tool calls work
- Chart DSL used by agent

### Deliverables

```
app/
├── services/
│   ├── agent.py             # Full agent
│   └── tools/
│       ├── entropy.py
│       ├── schema.py
│       └── query.py
└── prompts/
    ├── system.md
    └── chart_generation.md
```

### Tasks

1. **Agent service**
   - [ ] LLM client (Claude API)
   - [ ] System prompt
   - [ ] Tool definitions
   - [ ] Streaming response handling

2. **Tool implementations**
   - [ ] entropy_report tool
   - [ ] column_analysis tool
   - [ ] execute_query tool
   - [ ] suggest_fix tool

3. **Chart generation**
   - [ ] Chart DSL in tool response
   - [ ] Agent prompt for charts
   - [ ] Validation before rendering

4. **Action generation**
   - [ ] Agent suggests actions
   - [ ] Action derivation from tool results
   - [ ] Combined agent + rule-based actions

### Validation
- [ ] "Show me data quality" triggers tools
- [ ] Agent generates valid charts
- [ ] Actions appear after analysis
- [ ] Full conversation flow works

---

## Phase 8: Polish & Testing (Weeks 8-9)

### Goals
- Error handling is robust
- Performance is acceptable
- Tests provide confidence

### Tasks

1. **Error handling**
   - [ ] Global exception handler
   - [ ] Error artifact type
   - [ ] Graceful degradation
   - [ ] User-friendly messages

2. **Performance**
   - [ ] Lazy loading for large artifacts
   - [ ] Caching for entropy
   - [ ] Connection pooling
   - [ ] Arrow compression

3. **Testing**
   - [ ] Unit tests for action derivation
   - [ ] Integration tests for SSE
   - [ ] E2E tests with Playwright
   - [ ] Performance benchmarks

4. **Documentation**
   - [ ] API documentation
   - [ ] Deployment guide
   - [ ] User guide

### Validation
- [ ] No unhandled exceptions in UI
- [ ] 100K rows load < 2s
- [ ] Tests pass in CI
- [ ] Docs are complete

---

## Milestones Summary

| Week | Milestone | Demo |
|------|-----------|------|
| 1 | Shell renders | Page loads with theme toggle |
| 2-3 | Messages flow | Send message, see response |
| 3-4 | Artifacts display | Table with 100K rows scrolls |
| 4-5 | Charts work | Entropy radar renders |
| 5-6 | Context updates | Schema + entropy in sidebar |
| 6-7 | Mutations work | Fix action with undo |
| 7 | SQL editor | Run custom queries |
| 8 | Agent integrated | Full conversation with tools |
| 9 | Production ready | Deployed, tested, documented |

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| HTMX 4 not stable | Pin version, test thoroughly |
| Arrow performance | Benchmark early, optimize |
| Agent reliability | Fallback to rule-based actions |
| Scope creep | Strict phase boundaries |

---

## Dependencies

### Python
```toml
[project]
dependencies = [
    "fastapi>=0.109",
    "uvicorn[standard]",
    "jinja2",
    "python-multipart",
    "duckdb",
    "pyarrow",
    "altair",
    "httpx",
    "anthropic",
]
```

### JavaScript (build only)
```json
{
  "devDependencies": {
    "tailwindcss": "^4.0",
    "daisyui": "^5.0",
    "@tailwindcss/typography": "^0.5"
  }
}
```

### JavaScript (runtime, CDN or bundled)
- htmx.min.js (~14KB)
- alpine.min.js (~15KB)
- regular-table (~10KB)
- vega + vega-lite + vega-embed (~300KB)
- vega-loader-arrow (~5KB)
- cytoscape.min.js (~250KB)
- codemirror (~150KB with SQL)

---

## Next Steps

1. Start Phase 0: Create repository and project structure
2. Set up CI/CD pipeline
3. Create Linear project with epics for each phase
4. Begin implementation
