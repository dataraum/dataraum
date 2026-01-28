# Interface Strategy: Agent Testing, HTMX UI, MCP, Jupyter

## Overview

This plan covers the path from validated agents to multiple user interfaces, all sharing the same backend. It supersedes [ui-api-consolidation.md](../archive/ui-api-consolidation.md) (archived).

**Key decisions:**
- **HTMX + Jinja2 + daisyUI** for the web UI (JS Islands for complex widgets)
- **Content negotiation** on API endpoints (same route, different media types)
- **Same Python package** for API and HTMX templates (no separate frontend build)
- **UI first, then MCP and Jupyter** — the UI discovers the real API shape; MCP/Jupyter wrap proven endpoints
- **SDK-based integration tests** (library functions directly, not CLI)
- **API can be reshaped freely** — no backward compatibility constraints
- **CLI** already complete, stays as-is
- **Agent testing with BookSQL data** is the prerequisite for everything

**Phase sequencing rationale:**

With content negotiation, the UI IS the API — same endpoints, different
representations. Building the UI first means:
1. API shape gets driven by what the browser actually needs
2. SSE streaming, `<hx-partial>`, Arrow IPC — all proven before wrapping
3. MCP/Jupyter wrap battle-tested endpoints, not theoretical guesses
4. The library functions naturally support streaming (generator/async iterator);
   MCP collects into a final result, Jupyter blocks until done

**Related docs:**
- [query-agent-architecture.md](./query-agent-architecture.md) - RAG-based query reuse design (still active)
- [ENTROPY_IMPLEMENTATION_PLAN.md](../ENTROPY_IMPLEMENTATION_PLAN.md) - Entropy system architecture
- [docs/ui/](../ui/) - Detailed HTMX/JS Islands UI specification
- [BACKLOG.md](../BACKLOG.md) - Task tracking
- [PROGRESS.md](../PROGRESS.md) - Session log

---

## Current State (2026-01-28)

### What's Done

| Component | Status | Details |
|-----------|--------|---------|
| Pipeline | Complete | 18 phases, DAG orchestrator, checkpoint resume |
| Entropy | Complete | 6 detectors, compound risk, contracts, LLM interpretation |
| API | Complete | 8 routers: sources, tables, context, entropy, contracts, graphs, pipeline, query |
| CLI | Complete | 7 commands: run, status, inspect, reset, phases, contracts, query |
| Graph Agent | Implemented | SQL generation with entropy awareness, assumption tracking |
| Query Agent | Implemented | NL-to-SQL with RAG library, embeddings, contract confidence |
| Tests | 583 pass | Unit tests, API tests, entropy tests |

### What's NOT Validated

| Component | Gap |
|-----------|-----|
| Graph Agent | Never tested end-to-end with real data |
| Query Agent | Zero direct tests for `agent.py` (700 lines), `library.py` (500 lines), `embeddings.py` (350 lines) |
| Contract evaluation | Traffic light classification never fired against real entropy |
| RAG library | Save/search/reuse cycle untested with real queries |
| Pipeline + BookSQL | Pipeline has been run but agents have not been tested against the results |

### What's Not Started

| Component | Status |
|-----------|--------|
| Web UI | Detailed spec exists in `docs/ui/`, no code |
| MCP Server | Directory doesn't exist |
| Jupyter API | Not planned yet |

---

## Architecture: Shared Backend, Content Negotiation

### Library-First Design (Existing)

All interfaces call the same Python functions:

```
answer_question()     ──┐
build_context()        │
evaluate_contract()  ──┤── Core Library Functions
run_pipeline()         │
search_library()     ──┘
         │
         ├── CLI (typer commands)         [DONE]
         ├── API + HTMX (FastAPI routes)  [Phase 1]
         ├── MCP (tool handlers)          [Phase 2a]
         └── Jupyter (direct import)      [Phase 2b]
```

### Content Negotiation on API Endpoints

Same FastAPI routes serve both JSON and HTML. The API can be reshaped freely
to serve both — there are no external consumers to maintain compatibility for.

```python
@router.get("/tables/{table_id}")
def get_table(
    table_id: str,
    request: Request,
    session: SessionDep,
):
    table = load_table(session, table_id)
    actions = derive_actions(table)  # HATEOAS

    if wants_html(request):
        return templates.TemplateResponse(
            "tables/detail.html",
            {"table": table, "actions": actions},
        )
    return TableResponse.from_model(table, actions=actions)
```

Format selection mechanisms:
- `Accept: text/html` header (browser/HTMX default)
- `HX-Request: true` header (HTMX sends this automatically)
- `Accept: application/json` header (programmatic clients)
- URL suffix: `/tables/123.html` vs `/tables/123` (optional, explicit)

Endpoints may be restructured, merged, or split as the UI requires.

### HATEOAS Action Derivation

Every response includes valid next actions, derived server-side:

```python
def derive_actions(table: Table) -> list[Action]:
    actions = []
    entropy = get_table_entropy(table)

    if entropy and entropy.has_high_entropy_columns:
        actions.append(Action(
            label="View entropy issues",
            href=f"/entropy/table/{table.table_id}",
            method="GET",
            priority="primary",
        ))

    if table.layer == "typed":
        actions.append(Action(
            label="Query this table",
            href=f"/query/agent",
            method="POST",
            priority="secondary",
        ))

    return actions
```

### Arrow IPC for Data Transfer

DuckDB returns Arrow natively. For table/chart artifacts:

```python
@router.get("/artifacts/{artifact_id}/arrow")
def get_arrow_data(artifact_id: str, duckdb: DuckDBDep):
    result = duckdb.execute(artifact.sql)
    arrow_table = result.arrow()

    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, arrow_table.schema)
    writer.write_table(arrow_table)
    writer.close()

    return Response(
        content=sink.getvalue().to_pybytes(),
        media_type="application/vnd.apache.arrow.stream",
    )
```

Browser components (`<arrow-table>`, `<vega-chart>`) consume this directly.

---

## Test Data Strategy

### Primary: BookSQL (Symlinked)

Located at `examples/data/` (symlink to `../../testdata/booksql/Tables/`).

| Table | Rows | Purpose for Testing |
|-------|------|---------------------|
| Master_txn_table | 810K | Fact table: transactions with debit/credit, dates, FKs |
| customer_table | 50K | Dimension: addresses, multi-tenant |
| vendor_table | 100K | Dimension: addresses, multi-tenant |
| employee_table | 50K | Dimension: inconsistent date format (MM/DD/YYYY) |
| chart_of_account_OB | 2.4K | Dimension: GL accounts with types |
| product_service_table | 270 | Dimension: all services, no products |
| payment_method | 189 | Lookup: card types, cash, check |

**Known quality issues (good for entropy testing):**
- `"--"` as NULL everywhere (non-standard null representation)
- Date format mismatch: employee_table uses MM/DD/YYYY, others use YYYY-MM-DD
- 3 spurious index columns in Master_txn_table
- Balance columns always null in customer/vendor tables
- Composite FKs: (business_id + name) for all joins
- Sparse FK references (many `"--"` in customer_name, vendor_name)
- Soft-deleted accounts and employees in active dataset

**Cannot check into repo** (non-commercial license). Tests requiring this data
must be marked to skip if data is absent.

### Future: Synthetic Data Generation

NVIDIA DataDesigner (Apache 2.0, `pip install data-designer`) generates synthetic
data using LLMs + statistical samplers. Current limitations:
- Single-table focus (no multi-table FK consistency out of the box)
- Requires LLM API keys for generation

Potential use: generate targeted test fixtures with controlled entropy characteristics
(e.g., "30% nulls in column X, mixed date formats in column Y"). Evaluate after
Phase 0 reveals which test scenarios need synthetic data.

---

## Test Data Strategy

### Small Finance Fixtures
**Location:** `tests/integration/fixtures/small_finance/`

Committed to the repo, always available. 5 tables, ~730 rows total:

| Table | Rows | Key Columns |
|-------|------|-------------|
| `customers.csv` | 100 | Business Id, Customer name, Balance |
| `vendors.csv` | 50 | Business Id, Vendor name |
| `products.csv` | 30 | Product_Service, Rate |
| `payment_methods.csv` | 10 | Payment method |
| `transactions.csv` | 500 | Transaction date, Amount, Customer name, Vendor name |

**Temporal columns:** `Transaction date`, `Created date`, `Due date`

**Quality issues (for entropy testing):**
- `"--"` as null representation (Balance, Customer name, Vendor name)
- Mixed payment methods
- Nullable foreign key columns

**Use for:**
- Fast automated tests (CI pipeline)
- Mocked LLM tests
- Real end-to-end testing with LLM

### Configuration

The `config/pipeline.yaml` is pre-configured for small_finance:

```yaml
temporal:
  time_column: "Transaction date"
  time_grain: monthly
```

---

## Phase 0: Agent Validation

**Goal:** Validate graph agent and query agent end-to-end. Fix integration bugs
before building any new interface.

**Two-tier approach:**

1. **Mocked tests (Steps 0.1-0.5):** Use small_finance fixtures with mocked LLM.
   Run in CI, no API keys needed. Tests core logic and data flow.

2. **Real E2E tests (Step 0.6):** Run actual pipeline via CLI with real LLM.
   Generate databases in `data/` folder. Test agents with real dependencies.
   Requires API keys, not for CI.

**SDK-based:** Call library functions directly (`answer_question()`,
`build_execution_context()`, `evaluate_contract()`, etc.) — not through CLI or
API. The library-first design means SDK tests ARE the core integration tests.

### Step 0.1: Graph Agent Integration Tests

Create `tests/integration/test_graph_agent.py`:

1. **Context loading** - Call `build_execution_context()`, verify entropy
   scores are populated from real BookSQL analysis data (typing, statistics,
   semantic, relationships, correlations all present)

2. **SQL generation** - Load a transformation graph (e.g., "total amount by
   account type"), call graph agent's generate method, verify the SQL is
   valid DuckDB syntax

3. **Entropy-aware behavior** - Test all three modes against real entropy:
   - Strict: should block or warn on high-entropy columns
   - Balanced: should warn with assumptions
   - Lenient: should proceed with documented assumptions

4. **Assumption tracking** - Verify assumptions appear in execution metadata
   (e.g., "NULL values represented as '--' are excluded")

5. **SQL comments** - Verify entropy warnings appear as SQL comments

### Step 0.2: Query Agent Integration Tests

Create `tests/integration/test_query_agent.py`:

1. **End-to-end query** - Call `answer_question("How many transactions are there?")`
   against BookSQL. Verify: returns SQL, executes, result is plausible (~810K)

2. **Entropy-aware query** - Ask about revenue (amount column has no declared
   currency). Verify: assumptions list includes currency assumption

3. **Contract evaluation** - Query with different contracts:
   - `exploratory_analysis` should pass (GREEN/YELLOW)
   - `regulatory_reporting` should fail (RED) given BookSQL's entropy

4. **Auto-contract** - Verify `auto_contract=True` selects the strictest
   passing contract

5. **Query library cycle** - Save a query, search for similar, verify match
   is found with similarity > threshold

### Step 0.3: Embedding and Library Tests

Create `tests/integration/test_query_library.py`:

1. **Embedding generation** - Build embedding text from QueryDocument, verify
   non-empty and within token limit

2. **Vector search** - Save 3-5 queries with different topics, search with
   natural language, verify relevant query ranks highest

3. **Library persistence** - Save, retrieve by ID, verify all fields persisted

4. **Usage tracking** - Use a query, verify usage_count increments

### Step 0.4: Contract Evaluation Against Real Data

Create `tests/integration/test_contracts.py`:

1. **All 5 profiles** - Call `evaluate_contract()` for regulatory, executive,
   operational, ad-hoc, ML against BookSQL entropy. Verify classifications
   are sensible.

2. **Confidence levels** - Verify traffic light output for each contract:
   - BookSQL with `"--"` nulls and no currency should produce non-GREEN
     results for strict contracts

3. **Resolution hints** - Verify resolution suggestions are actionable
   (e.g., "declare null meaning for '--' values")

### Step 0.5: Mocked Test Suite (CI-Ready)

**Status:** ✅ COMPLETE

Created integration tests using small_finance fixtures with mocked LLM:

- `tests/integration/conftest.py` - Fixtures for harness, mock LLM, vectors DB
- `tests/integration/test_graph_agent.py` - Context loading, SQL generation
- `tests/integration/test_query_agent.py` - End-to-end with mocked LLM
- `tests/integration/test_query_library.py` - Embeddings, save/search cycle
- `tests/integration/test_contracts.py` - Contract evaluation against real entropy

**Test characteristics:**
- Run with `pytest tests/integration/ -v`
- No LLM API keys needed (mocked)
- Use small_finance data (committed, always available)
- ~50 tests, run in ~5 minutes

### Step 0.6: Real End-to-End Testing (Manual/Comprehensive)

**Goal:** Validate the full stack with real dependencies: actual LLM, full
pipeline execution, real entropy detection, real query generation.

**Approach:**

1. **Generate test databases:**
   ```bash
   # Create output directory (not committed, gitignored)
   mkdir -p data/small_finance_output

   # Run pipeline against small_finance
   # Config is pre-set for "Transaction date" column
   dataraum run packages/dataraum-api/tests/integration/fixtures/small_finance \
     --output data/small_finance_output

   # If phases fail, resume from checkpoint (just re-run)
   dataraum run packages/dataraum-api/tests/integration/fixtures/small_finance \
     --output data/small_finance_output
   ```

2. **Test agents against generated data:**
   ```bash
   # Query agent with real LLM
   dataraum query data/small_finance_output \
     "How many transactions are there?"

   dataraum query data/small_finance_output \
     "What is the total transaction amount?"

   # Check entropy and contracts
   dataraum contracts data/small_finance_output

   # Inspect context
   dataraum inspect data/small_finance_output
   ```

**Requirements:**
- LLM API keys configured (e.g., `ANTHROPIC_API_KEY`)
- Not for CI — manual testing before releases
- Uses checkpoints for recovery if phases fail

**Validation checklist:**
- [ ] Pipeline completes all 18 phases (or recovers via checkpoint)
- [ ] Entropy scores are populated for all columns
- [ ] Query agent generates valid SQL for simple questions
- [ ] Query agent reuses library entries on similar questions
- [ ] Contract evaluation produces sensible traffic lights
- [ ] Resolution hints are actionable

### Deliverables

**Mocked tests (Step 0.5):** ✅
- `tests/integration/conftest.py`
- `tests/integration/test_graph_agent.py`
- `tests/integration/test_query_agent.py`
- `tests/integration/test_query_library.py`
- `tests/integration/test_contracts.py`

**Real E2E (Step 0.6):**
- `data/` directory for generated databases (gitignored)
- Documentation for running real E2E tests
- Validation checklist in this plan

---

## Phase 1: HTMX UI Foundation

**Goal:** Conversational query interface with entropy dashboard, using HTMX
and JS Islands for complex widgets. This phase discovers the real API shape
that MCP and Jupyter will later wrap.

The UI drives API design because:
- Streaming patterns (SSE, `<hx-partial>`) define how library functions
  expose intermediate results
- Content negotiation forces each endpoint to have both a data model and
  a template — this surfaces missing fields and awkward data shapes
- HATEOAS action derivation validates the entropy → action → resolution flow
- Arrow IPC endpoints prove the binary data pipeline end-to-end

API endpoints will be freely reshaped during this phase to serve the UI.

### Tech Stack (from docs/ui/)

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Server | FastAPI + Jinja2 | Templates + API (same process) |
| CSS | daisyUI + Tailwind | Pre-built components, dark mode |
| Hypermedia | HTMX 4 | SSE, morphing, `<hx-partial>` |
| Client state | Alpine.js | Minimal declarative state |
| Tables | regular-table + Arrow JS | Virtual scroll, millions of rows |
| Charts | Vega-Lite + vega-loader-arrow | Declarative, Arrow-native |
| Graphs | Cytoscape.js | Schema/lineage visualization |
| Code | CodeMirror 6 | SQL editing |

### Project Structure (Same Package)

```
packages/dataraum-api/
├── src/dataraum/
│   ├── api/
│   │   ├── main.py              # Add Jinja2, static files, HTMX middleware
│   │   ├── routers/             # Routes gain content negotiation
│   │   ├── templates/           # Jinja2 templates
│   │   │   ├── base.html        # Shell: head, sidebar, main canvas, scripts
│   │   │   ├── partials/        # Reusable fragments (actions, messages)
│   │   │   ├── pages/           # Full page templates
│   │   │   │   ├── index.html   # Landing / source selector
│   │   │   │   └── workspace.html # Main workspace with conversation
│   │   │   ├── artifacts/       # Artifact type templates
│   │   │   │   ├── table.html   # <arrow-table> wrapper
│   │   │   │   ├── chart.html   # <vega-chart> wrapper
│   │   │   │   ├── entropy.html # Entropy report (radar + issues)
│   │   │   │   ├── code.html    # Syntax-highlighted code block
│   │   │   │   └── graph.html   # <graph-viewer> wrapper
│   │   │   └── components/      # Sidebar, context panel, input bar
│   │   └── static/
│   │       ├── css/
│   │       │   └── app.css      # Tailwind output (built once)
│   │       └── js/
│   │           └── islands/     # Web component definitions
│   │               ├── arrow-table.js
│   │               ├── vega-chart.js
│   │               ├── graph-viewer.js
│   │               └── sql-editor.js
│   ...
├── tailwind.config.js           # Tailwind + daisyUI config
└── package.json                 # Only for Tailwind build (not a Node app)
```

### Build Process

The only "frontend build" is Tailwind CSS compilation:

```bash
npx tailwindcss -i src/dataraum/api/static/css/input.css \
                -o src/dataraum/api/static/css/app.css --watch
```

JS Islands are vanilla ES modules loaded directly by the browser. No bundler.

### Step 1.1: Foundation

- Add Jinja2 environment to FastAPI app
- Mount static files directory
- Create base.html template with daisyUI layout
- Add HTMX middleware (detect `HX-Request` header)
- Add content negotiation helper to router dependencies
- Build Tailwind CSS with daisyUI

### Step 1.2: Conversation Interface

- Workspace page with conversation stream
- Input bar for natural language queries
- SSE streaming for agent responses (`<hx-partial>` multi-target updates)
- Message rendering (user messages, agent responses)
- Action buttons derived server-side (HATEOAS)

### Step 1.3: Arrow Table Island

- `<arrow-table>` web component using regular-table
- Arrow IPC endpoint for binary data transfer
- HTMX `beforeSwap` hook for binary response handling
- Virtual scroll for BookSQL's 810K transaction rows
- Column sorting (client-side, no server round-trip)

### Step 1.4: Entropy Dashboard

- Radar chart via `<vega-chart>` (Vega-Lite spec)
- Issues table with severity indicators
- Contract compliance traffic lights
- Drill-down from table -> column -> specific issues
- Resolution suggestions with action buttons

### Step 1.5: Query Agent UI

- Natural language input with contract selector dropdown
- Streaming response with confidence badge
- Assumptions panel (collapsible)
- SQL preview (CodeMirror, read-only)
- Result table (`<arrow-table>`)
- Save to library button

---

## Phase 2a: MCP Server

**Goal:** 4 MCP tools wrapping the battle-tested API and library functions
proven in Phase 1.

By this point, the UI has validated:
- How `answer_question()` streams intermediate results (MCP collects into final)
- What context format works for LLM consumption (proven in UI prompts)
- Which actions and response shapes are useful (MCP exposes the same)

### Tools

| Tool | Wraps | Input | Output |
|------|-------|-------|--------|
| `get_context` | `build_execution_context()` | source_id | Formatted context document (markdown) |
| `query` | `answer_question()` | question, source_id, contract? | QueryResult with confidence |
| `get_metrics` | Graph listing + entropy | source_id | Available metrics with entropy scores |
| `annotate` | Semantic annotation update | column_id, field, value | Updated annotation |

### File Structure

```
src/dataraum/mcp/
├── __init__.py
├── server.py           # MCP server with tool registration
└── formatters.py       # Output formatting for LLM consumption
```

### Entry Point

```python
# src/dataraum/mcp/server.py
from mcp.server import Server

server = Server("dataraum")

@server.tool()
async def get_context(source_id: str) -> str:
    """Get pre-computed data context for a source."""
    ctx = build_execution_context(session, source_id)
    return format_context_for_prompt(ctx)

@server.tool()
async def query(question: str, source_id: str, contract: str | None = None) -> str:
    """Answer a question about the data."""
    result = answer_question(question, manager, source_id, contract=contract)
    return result.format_for_llm()

@server.tool()
async def get_metrics(source_id: str) -> str:
    """List available metrics with entropy scores."""
    graphs = list_graphs(session, source_id)
    return format_metrics_for_llm(graphs)

@server.tool()
async def annotate(column_id: str, field: str, value: str) -> str:
    """Update semantic annotation for a column."""
    update_annotation(session, column_id, field, value)
    return f"Updated {field} for column {column_id}"
```

### Claude Desktop Configuration

```json
{
  "mcpServers": {
    "dataraum": {
      "command": "uv",
      "args": ["run", "python", "-m", "dataraum.mcp.server"],
      "env": {
        "DATARAUM_OUTPUT_DIR": "/path/to/pipeline/output"
      }
    }
  }
}
```

### Implementation Notes

- Use `mcp` SDK (already in dependencies)
- Server connects to existing ConnectionManager
- Output formatted as markdown (LLM-optimized, not JSON)
- `query` tool collects streaming result into final response
- `get_context` returns the same prompt text proven in UI agent workflow

---

## Phase 2b: Jupyter / Python API

**Goal:** Clean Python API for interactive use in notebooks and scripts,
wrapping the same endpoints and library functions proven in the UI.

### Public API

```python
from dataraum import Context

# Load analyzed data
ctx = Context("./output")

# Entropy
ctx.entropy.summary()                    # DataFrame: table x dimension scores
ctx.entropy.table("Master_txn_table")    # TableEntropyProfile
ctx.entropy.column("Master_txn_table", "amount")  # ColumnEntropyProfile

# Contracts
ctx.contracts.list()                     # Available contracts
ctx.contracts.evaluate("regulatory")     # ContractEvaluation with confidence
ctx.contracts.evaluate_all()             # All contracts with traffic lights

# Query
result = ctx.query("What was total revenue?")
result.data                              # pandas DataFrame
result.sql                               # Generated SQL
result.confidence                        # GREEN/YELLOW/ORANGE/RED
result.assumptions                       # List of assumptions made

# Query library
ctx.library.search("revenue")            # Similar queries
ctx.library.list()                       # All saved queries

# Context (for inspection)
ctx.context()                            # Full GraphExecutionContext
ctx.context_prompt()                     # Formatted prompt text
```

### File Structure

```
src/dataraum/notebook/
├── __init__.py          # Context class
├── entropy.py           # EntropyAccessor
├── contracts.py         # ContractAccessor
├── query.py             # QueryAccessor
└── library.py           # LibraryAccessor
```

### Implementation Notes

- Thin wrappers around the same library functions proven in UI
- Returns pandas DataFrames where appropriate (optional dependency)
- No async — synchronous wrappers for notebook ergonomics
- `query()` blocks until streaming result is collected (same as MCP)
- IPython repr methods for rich display in Jupyter (`_repr_html_`)
- Context class manages its own ConnectionManager

---

## Phase 3: UI Completion

### Step 3.1: Schema Explorer

- Cytoscape.js graph of table relationships
- Click node -> show table details (content negotiated)
- Entropy indicators on nodes (color-coded)
- Layout toggle (hierarchical / force-directed)

### Step 3.2: Pipeline Monitor

- SSE-driven progress display (existing SSE endpoint)
- Phase status badges (pending/running/complete/failed)
- Duration display per phase
- Log viewer for phase output

### Step 3.3: Context Panel

- Collapsible sidebar with:
  - Connection info
  - Schema tree (lazy-loaded)
  - Entropy summary (mini radar)
  - Recent queries
  - Undo stack (when mutations enabled)

### Step 3.4: SQL Editor

- CodeMirror 6 with SQL mode
- Run button -> execute against DuckDB (read-only)
- Result as `<arrow-table>` artifact
- Explain button -> show query plan

### Step 3.5: Mutation and Undo

- Server-tracked change history
- Preview before apply
- Undo via reverse SQL
- Audit trail

---

## Content Negotiation Details

### Request Detection

```python
def wants_html(request: Request) -> bool:
    """Check if client wants HTML (HTMX or browser)."""
    if request.headers.get("HX-Request"):
        return True
    accept = request.headers.get("accept", "")
    if "text/html" in accept and "application/json" not in accept:
        return True
    return False
```

### Response Pattern

Each router endpoint returns either JSON or HTML:

```python
@router.get("/entropy/{source_id}")
def get_entropy(source_id: str, request: Request, session: SessionDep):
    data = build_entropy_dashboard(session, source_id)
    actions = derive_entropy_actions(data)

    if wants_html(request):
        return templates.TemplateResponse(
            "artifacts/entropy.html",
            {"data": data, "actions": actions, "request": request},
        )
    return EntropyDashboardResponse(**data, _actions=actions)
```

### SSE Streaming (Agent Responses)

Agent responses stream as HTML fragments:

```
event: message
data: <article class="agent-message">Analyzing your question...</article>

event: artifact
data: <figure class="artifact-table">
data:   <arrow-table hx-get="/artifacts/abc/arrow" hx-trigger="load" hx-swap="none">
data:   </arrow-table>
data: </figure>

event: actions
data: <hx-partial hx-target="#actions-panel">
data:   <button hx-post="/query/library/save" class="btn btn-primary">Save query</button>
data: </hx-partial>

event: done
data: <div class="stream-complete"></div>
```

---

## JS Islands Integration

### How It Works

1. **Web components loaded once** in `base.html`:
   ```html
   <script type="module" src="/static/js/islands/arrow-table.js"></script>
   <script type="module" src="/static/js/islands/vega-chart.js"></script>
   ```

2. **HTMX brings in HTML** containing custom elements (via SSE or responses)

3. **Components self-initialize** when connected to DOM (`connectedCallback`)

4. **`hx-swap="none"`** — HTMX fetches data but component handles rendering

5. **Alpine.js bridges events** — component dispatches custom event, Alpine
   converts to HTMX request

### Binary Data Pattern

Arrow IPC binary data requires special handling:

```javascript
// In base.html
document.body.addEventListener('htmx:beforeSwap', (evt) => {
    const contentType = evt.detail.xhr.getResponseHeader('content-type');
    if (contentType?.includes('arrow')) {
        const component = evt.detail.target.closest('arrow-table');
        component.loadArrow(evt.detail.xhr.response);
        evt.detail.shouldSwap = false;
    }
});
```

---

## Success Criteria

### Phase 0 Complete When

- [ ] Graph agent generates valid SQL from BookSQL context
- [ ] Query agent answers "How many transactions?" with plausible result
- [ ] Contract evaluation produces different confidence levels for different contracts
- [ ] Query library save/search cycle works
- [ ] Integration test suite runs (with LLM mocks)
- [ ] No crashes on BookSQL's edge cases (-- nulls, mixed dates, sparse FKs)

### Phase 1 Complete When

- [ ] Browser loads workspace with HTMX + daisyUI layout
- [ ] Query input sends question -> SSE streams response -> result appears
- [ ] Arrow table renders BookSQL's 810K rows with virtual scroll
- [ ] Entropy dashboard shows radar chart + issues
- [ ] Content negotiation: same endpoint returns JSON or HTML
- [ ] API shape stable enough to wrap

### Phase 2 Complete When

- [ ] MCP `get_context` returns formatted context for BookSQL
- [ ] MCP `query` answers questions with confidence levels
- [ ] `from dataraum import Context` works in Jupyter
- [ ] `ctx.query("...")` returns results with pandas DataFrame
- [ ] `ctx.entropy.summary()` displays entropy table
- [ ] MCP and Jupyter use same endpoints/functions proven in UI

### Phase 3 Complete When

- [ ] Schema explorer shows BookSQL table relationships
- [ ] Pipeline monitor shows phase progress via SSE
- [ ] SQL editor executes queries and shows results
- [ ] Context panel updates on navigation

---

## Dependency Graph

```
Phase 0: Agent Validation (SDK tests)
    │
    ├── 0.1 Graph agent integration tests
    ├── 0.2 Query agent integration tests
    ├── 0.3 Query library tests
    ├── 0.4 Contract evaluation tests
    └── 0.5 Test suite codified
    │
Phase 1: HTMX UI Foundation
    │   Discovers the real API shape.
    │   Content negotiation, SSE streaming, Arrow IPC
    │   all proven with real browser usage.
    │
    ├── 1.1 FastAPI + Jinja2 + Tailwind + content negotiation
    ├── 1.2 Conversation interface + SSE
    ├── 1.3 Arrow table island
    ├── 1.4 Entropy dashboard
    └── 1.5 Query agent UI
    │
    ├─────────────────────────────────────┐
    │                                     │
Phase 2a: MCP Server               Phase 2b: Jupyter API
    │   Wraps proven endpoints            │   Wraps proven endpoints
    │                                     │
    ├── MCP tools (4)                     ├── Context class
    ├── Formatters                        ├── Accessors
    └── Claude Desktop config             └── IPython repr methods
    │                                     │
    └─────────────────┬───────────────────┘
                      │
Phase 3: UI Completion
    │
    ├── 3.1 Schema explorer (Cytoscape)
    ├── 3.2 Pipeline monitor
    ├── 3.3 Context panel
    ├── 3.4 SQL editor (CodeMirror)
    └── 3.5 Mutations + undo
```

---

## Endpoints (Will Be Reshaped by UI)

The current API endpoints are a starting point. The UI will reshape them
as needed — adding fields, merging routes, changing response structures.

### Current Endpoints

| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/api/v1/sources` | List sources |
| GET | `/api/v1/sources/{id}` | Source details |
| POST | `/api/v1/sources` | Create source |
| GET | `/api/v1/tables` | List tables |
| GET | `/api/v1/tables/{id}` | Table with columns |
| GET | `/api/v1/columns/{id}` | Column details |
| GET | `/api/v1/entropy/{source_id}` | Entropy dashboard |
| GET | `/api/v1/entropy/table/{table_id}` | Table entropy |
| GET | `/api/v1/contracts` | List contracts |
| GET | `/api/v1/contracts/{name}/evaluate` | Evaluate contract |
| GET | `/api/v1/context/{source_id}` | Execution context |
| POST | `/api/v1/query` | Execute raw SQL |
| POST | `/api/v1/query/agent` | NL query agent |
| GET | `/api/v1/query/library/{source_id}` | List saved queries |
| POST | `/api/v1/sources/{id}/run` | Trigger pipeline |
| GET | `/api/v1/runs/{id}/stream` | SSE pipeline progress |

### Expected New Endpoints (Phase 1)

| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/` | Workspace (full page) |
| GET | `/artifacts/{id}/arrow` | Arrow IPC binary |
| POST | `/sessions/{id}/messages` | User message -> SSE agent response |

All existing endpoints gain HTML responses via content negotiation.

---

## Synthetic Data: Future Evaluation

**NVIDIA DataDesigner** (`pip install data-designer`, Apache 2.0):
- LLM-powered synthetic data with dependency-aware field generation
- Currently single-table focus (multi-table FK consistency not built-in)
- Useful for: generating test fixtures with controlled entropy characteristics

**Evaluate after Phase 0** when we know which test scenarios need synthetic data.
Potential uses:
- High-entropy dataset (lots of nulls, mixed types, ambiguous joins)
- Low-entropy dataset (clean, well-typed, documented)
- Specific compound risk scenarios (currency + aggregation, nulls + aggregation)
- Regression test data that can be committed to repo (no licensing issues)

---

## Notes

- HTMX approach may need adjustment for specific interactions. If a widget proves
  awkward, it can be replaced with a richer JS island without rewriting the whole UI.
  React fallback remains possible per-component, not all-or-nothing.
- The BookSQL dataset cannot be committed to the repo. Integration tests should
  skip gracefully when data is missing.
- Slicing bug fixes are pending but orthogonal to agent testing.
