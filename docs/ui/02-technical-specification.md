# Dataraum UI: Technical Specification

## Technology Stack

### Core Choices

| Layer | Technology | Rationale |
|-------|------------|-----------|
| **Server** | FastAPI (Python) | Async, type hints, OpenAPI generation, same language as core library |
| **Templates** | Jinja2 | Standard, well-understood, no build step |
| **CSS** | daisyUI + Tailwind | Pre-built components, 30+ themes, dark mode, no runtime cost |
| **Hypermedia** | HTMX 4.x | Native SSE, morphing, `<hx-partial>`, explicit inheritance |
| **Client scripting** | Alpine.js | Minimal, declarative, complements HTMX |
| **Data format** | Apache Arrow | Zero-copy from DuckDB, columnar, efficient streaming |
| **Charts** | Vega-Lite + vega-loader-arrow | Declarative grammar, Arrow-native, LLM-friendly |
| **Tables** | regular-table + Arrow JS | Virtual DOM, async data model, ~10KB, FINOS-backed |
| **Graph visualization** | Cytoscape.js | Mature, handles large graphs, good layout algorithms |
| **Code editors** | CodeMirror 6 | Modern, extensible, good SQL/YAML modes |
| **Session storage** | Redis or SQLite | Redis for multi-instance, SQLite for single-user |
| **CLI** | Typer + Rich | Modern Python CLI with good formatting |

### What We're NOT Using

| Technology | Why Not |
|------------|---------|
| React/Vue/Svelte | Adds build complexity, separate state management, harder to unify with MCP |
| GraphQL | Over-engineering for our use case; REST + HATEOAS is simpler |
| WebSocket everywhere | SSE is sufficient, simpler, better for our streaming needs |
| Node.js runtime | Avoids polyglot complexity; Python serves everything, Tailwind is build-only |
| ECharts/Chart.js | Not declarative; Vega-Lite has formal grammar better suited for LLM generation |
| AG Grid/Handsontable | Heavy, commercial; regular-table is lighter and Arrow-native |

### Build Pipeline

```bash
# Development: Two terminals
Terminal 1: uvicorn main:app --reload
Terminal 2: npx @tailwindcss/cli -i ./styles/app.css -o ./static/css/app.css --watch

# Production: Static CSS, no Node.js runtime
npx @tailwindcss/cli -i ./styles/app.css -o ./static/css/app.css --minify
```

---

## Unified Arrow Data Format

DuckDB returns Arrow natively. Both tables and charts consume the same format:

```
┌─────────────────────────────────────────────────────────────────┐
│                         DuckDB Query                            │
│                              │                                  │
│                    Arrow IPC (zero-copy)                        │
│                              │                                  │
│                    ┌─────────▼─────────┐                        │
│                    │   ArrayBuffer     │                        │
│                    │   (in browser)    │                        │
│                    └─────────┬─────────┘                        │
│                              │                                  │
│              ┌───────────────┼───────────────┐                  │
│              │               │               │                  │
│              ▼               ▼               ▼                  │
│      ┌───────────────┐ ┌───────────┐ ┌─────────────────┐        │
│      │ regular-table │ │ Vega-Lite │ │  Export (CSV,   │        │
│      │  (data grid)  │ │  (charts) │ │  Parquet, etc.) │        │
│      │               │ │           │ │                 │        │
│      │ Arrow JS      │ │ vega-     │ │  Arrow JS       │        │
│      │ .slice()      │ │ loader-   │ │  .toArray()     │        │
│      │               │ │ arrow     │ │                 │        │
│      └───────────────┘ └───────────┘ └─────────────────┘        │
└─────────────────────────────────────────────────────────────────┘
```

**Benefits:**
- No JSON serialization/deserialization
- Columnar format matches analytical queries
- Same data feeds multiple visualizations
- Millions of rows feel instant

---

## HTMX 4 Features We'll Use

### 1. Native SSE Streaming

```html
<!-- Agent responses stream directly -->
<div hx-get="/session/{{ id }}/stream"
     hx-trigger="load"
     hx-swap="beforeend">
</div>
```

Server sends:
```
data: <article class="message">Analyzing...</article>

data: <article class="message">Found 3 issues...</article>

data: <hx-partial hx-target="#actions"><button class="btn btn-primary">Fix all</button></hx-partial>
```

### 2. `<hx-partial>` for Multi-Target Updates

```html
<!-- Response can update multiple targets -->
<hx-partial hx-target="#canvas" hx-swap="beforeend">
  <article class="message">Fix applied successfully.</article>
</hx-partial>

<hx-partial hx-target="#context-panel" hx-swap="innerHTML">
  <div class="stat"><div class="stat-value">0.72</div><div class="stat-desc">+0.07</div></div>
</hx-partial>

<hx-partial hx-target="#actions" hx-swap="innerHTML">
  <button class="btn" hx-post="/undo">Undo</button>
  <button class="btn btn-primary" hx-post="/continue">Continue</button>
</hx-partial>
```

### 3. Built-in Morphing

```html
<table class="table" hx-get="/results" 
       hx-trigger="every 5s"
       hx-swap="outerMorph">
  <!-- Rows update in-place, preserving selection state -->
</table>
```

### 4. Explicit Attribute Inheritance

```html
<main hx-target:inherited="#canvas"
      hx-swap:inherited="beforeend"
      hx-indicator:inherited="#loading">
  
  <!-- All children inherit these defaults -->
  <button class="btn" hx-post="/action/1">Action 1</button>
  <button class="btn" hx-post="/action/2">Action 2</button>
  
</main>
```

---

## Data Models

### Session

```python
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from enum import Enum

class MessageRole(Enum):
    USER = "user"
    AGENT = "agent"
    SYSTEM = "system"

@dataclass
class Message:
    id: str
    role: MessageRole
    content: str
    timestamp: datetime
    artifacts: list["Artifact"] = field(default_factory=list)
    actions: list["Action"] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

@dataclass
class Artifact:
    id: str
    type: str  # "table", "chart", "code", "graph", "error"
    title: str | None
    data: Any  # Type depends on artifact type
    metadata: dict[str, Any] = field(default_factory=dict)

@dataclass
class Session:
    id: str
    created_at: datetime
    updated_at: datetime
    
    # Connection context
    connection_string: str | None
    active_dataset: str | None
    
    # Conversation
    messages: list[Message] = field(default_factory=list)
    
    # State
    context: dict[str, Any] = field(default_factory=dict)
    
    # Undo stack
    changes: list["Change"] = field(default_factory=list)
    
    @property
    def has_uncommitted_changes(self) -> bool:
        return len(self.changes) > 0 and not self.changes[-1].committed
```

### Action

```python
from typing import Literal

@dataclass
class Action:
    id: str
    rel: str  # Semantic relationship
    label: str
    
    # How to execute (one of these)
    href: str | None = None
    method: str = "POST"
    prompt: str | None = None  # Inject as user message
    tool_call: dict | None = None  # Direct tool execution
    
    # Display
    description: str | None = None
    icon: str | None = None
    style: Literal["primary", "secondary", "destructive", "success"] = "secondary"
    group: str | None = None
    
    # State
    disabled: bool = False
    disabled_reason: str | None = None
    confirm: str | None = None  # Requires confirmation
    
    # HTMX hints
    hx_target: str | None = None
    hx_swap: str | None = None
    
    def to_html_attrs(self) -> dict[str, str]:
        """Convert to HTMX attributes for template rendering."""
        attrs = {}
        
        if self.href:
            attrs[f"hx-{self.method.lower()}"] = self.href
        
        if self.hx_target:
            attrs["hx-target"] = self.hx_target
        
        if self.hx_swap:
            attrs["hx-swap"] = self.hx_swap
        
        if self.confirm:
            attrs["hx-confirm"] = self.confirm
        
        if self.disabled:
            attrs["disabled"] = "disabled"
            if self.disabled_reason:
                attrs["title"] = self.disabled_reason
        
        return attrs
    
    def to_btn_class(self) -> str:
        """Convert style to daisyUI button class."""
        mapping = {
            "primary": "btn btn-primary",
            "secondary": "btn btn-ghost",
            "destructive": "btn btn-error",
            "success": "btn btn-success",
        }
        return mapping.get(self.style, "btn")
```

### Change (for Undo)

```python
@dataclass
class Change:
    id: str
    timestamp: datetime
    action_id: str
    action_label: str
    
    # What changed
    table: str
    operation: Literal["INSERT", "UPDATE", "DELETE", "DDL"]
    affected_rows: int
    
    # How to undo
    undo_sql: str | None
    undo_snapshot: str | None  # Path to backup if needed
    
    # State
    committed: bool = False
    undone: bool = False
```

---

## Chart DSL: Dataraum → Vega-Lite

AI agents generate a simplified DSL that compiles to Vega-Lite. This constrains the options to what we support while remaining LLM-friendly.

### Chart DSL Definition

```python
from dataclasses import dataclass
from typing import Literal
from enum import Enum

class ChartType(Enum):
    BAR = "bar"
    LINE = "line"
    AREA = "area"
    SCATTER = "scatter"
    RADAR = "radar"          # Custom (transformed to radial)
    HEATMAP = "heatmap"
    HISTOGRAM = "histogram"

@dataclass
class DataraumChart:
    """Simplified chart spec that compiles to Vega-Lite."""
    
    type: ChartType
    title: str | None = None
    
    # Data binding (column names from query result)
    x: str | None = None
    y: str | None = None
    color: str | None = None
    size: str | None = None
    
    # Aggregations
    x_aggregate: Literal["sum", "mean", "count", "min", "max"] | None = None
    y_aggregate: Literal["sum", "mean", "count", "min", "max"] | None = None
    
    # Time handling
    x_time_unit: Literal["year", "month", "day", "hour"] | None = None
    
    # Sorting
    sort: Literal["ascending", "descending", "-x", "-y"] | None = None
    
    # Radar chart specific
    dimensions: list[str] | None = None
    
    def to_vega_lite(self) -> dict:
        """Compile to Vega-Lite spec. Data loaded separately via Arrow."""
        spec = {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "title": self.title,
            "data": {"name": "source"},  # Named data source for Arrow loading
        }
        
        if self.type == ChartType.BAR:
            spec["mark"] = "bar"
            spec["encoding"] = {
                "x": self._encode_field(self.x, self.x_aggregate, self.x_time_unit),
                "y": self._encode_field(self.y, self.y_aggregate),
            }
            if self.color:
                spec["encoding"]["color"] = {"field": self.color, "type": "nominal"}
                
        elif self.type == ChartType.LINE:
            spec["mark"] = {"type": "line", "point": True}
            spec["encoding"] = {
                "x": self._encode_field(self.x, time_unit=self.x_time_unit),
                "y": self._encode_field(self.y, self.y_aggregate),
            }
            if self.color:
                spec["encoding"]["color"] = {"field": self.color, "type": "nominal"}
                
        elif self.type == ChartType.SCATTER:
            spec["mark"] = "point"
            spec["encoding"] = {
                "x": self._encode_field(self.x),
                "y": self._encode_field(self.y),
            }
            if self.color:
                spec["encoding"]["color"] = {"field": self.color, "type": "nominal"}
            if self.size:
                spec["encoding"]["size"] = {"field": self.size, "type": "quantitative"}
                
        elif self.type == ChartType.RADAR:
            spec = self._build_radar_spec()
            
        elif self.type == ChartType.HEATMAP:
            spec["mark"] = "rect"
            spec["encoding"] = {
                "x": {"field": self.x, "type": "nominal"},
                "y": {"field": self.y, "type": "nominal"},
                "color": {"field": self.color or "value", "type": "quantitative"},
            }
            
        elif self.type == ChartType.HISTOGRAM:
            spec["mark"] = "bar"
            spec["encoding"] = {
                "x": {"bin": True, "field": self.x, "type": "quantitative"},
                "y": {"aggregate": "count"},
            }
        
        return spec
    
    def _encode_field(self, field: str, aggregate: str = None, time_unit: str = None) -> dict:
        if not field:
            return {}
        encoding = {"field": field}
        
        if time_unit:
            encoding["type"] = "temporal"
            encoding["timeUnit"] = time_unit
        elif aggregate:
            encoding["type"] = "quantitative"
            encoding["aggregate"] = aggregate
        else:
            encoding["type"] = "quantitative"
            
        return encoding
    
    def _build_radar_spec(self) -> dict:
        """Build radar chart using layered arc marks."""
        return {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "title": self.title,
            "data": {"name": "source"},
            "layer": [
                {
                    "mark": {"type": "arc", "innerRadius": 20, "stroke": "#fff"},
                    "encoding": {
                        "theta": {"field": "dimension", "type": "nominal"},
                        "radius": {"field": self.y or "value", "type": "quantitative", "scale": {"zero": True}},
                        "color": {"field": "dimension", "type": "nominal"},
                    }
                },
                {
                    "mark": {"type": "text", "radiusOffset": 10},
                    "encoding": {
                        "theta": {"field": "dimension", "type": "nominal"},
                        "radius": {"field": self.y or "value", "type": "quantitative"},
                        "text": {"field": self.y or "value", "type": "quantitative", "format": ".2f"},
                    }
                }
            ]
        }
```

### Server-Side Validation with Altair

```python
import altair as alt
from pydantic import BaseModel, validator

class ChartRequest(BaseModel):
    type: ChartType
    x: str | None = None
    y: str | None = None
    color: str | None = None
    # ... other fields
    
    @validator('type')
    def validate_chart_type(cls, v):
        if v not in ChartType:
            raise ValueError(f"Unsupported chart type: {v}")
        return v

def compile_chart(spec: DataraumChart) -> dict:
    """Compile and validate chart spec using Altair."""
    vega_lite = spec.to_vega_lite()
    
    # Validate using Altair (catches schema errors)
    try:
        chart = alt.Chart.from_dict(vega_lite)
        chart.to_dict()  # This validates
    except Exception as e:
        raise ChartValidationError(f"Invalid chart spec: {e}")
    
    return vega_lite
```

### Agent Prompt for Chart Generation

```python
CHART_GENERATION_PROMPT = """
When visualizing data, generate a DataraumChart specification.

Available chart types:
- bar: Compare categories or values
- line: Show trends over time  
- area: Show cumulative trends
- scatter: Show correlations between two variables
- radar: Show multi-dimensional scores (like entropy dimensions)
- heatmap: Show density or correlation matrices
- histogram: Show distributions

Example for entropy radar:
{
  "type": "radar",
  "title": "Entropy by Dimension",
  "dimensions": ["completeness", "consistency", "accuracy", "timeliness", "uniqueness", "validity"],
  "y": "score"
}

Example for trend line:
{
  "type": "line",
  "title": "Monthly Revenue",
  "x": "order_date",
  "x_time_unit": "month",
  "y": "amount",
  "y_aggregate": "sum"
}

Always use column names from the query result. Keep specs minimal.
"""
```

---

## Table Component: regular-table + Arrow JS

### Arrow Table Web Component

```typescript
// arrow-table.ts
import { Table, tableFromIPC } from 'apache-arrow';
import 'regular-table';

class ArrowTable extends HTMLElement {
    private arrowTable: Table | null = null;
    private regularTable: HTMLElement | null = null;
    private sortedIndices: number[] | null = null;
    private selectedIndices: Set<number> = new Set();
    
    static get observedAttributes() {
        return ['height', 'selectable'];
    }
    
    connectedCallback() {
        // Create regular-table element
        this.regularTable = document.createElement('regular-table');
        this.regularTable.className = 'table table-zebra table-sm';
        this.appendChild(this.regularTable);
        
        // Apply height
        const height = this.getAttribute('height') || '400px';
        this.regularTable.style.height = height;
        
        // Setup selection if enabled
        if (this.hasAttribute('selectable')) {
            this.setupSelection();
        }
    }
    
    async loadFromUrl(url: string) {
        const response = await fetch(url);
        const buffer = await response.arrayBuffer();
        await this.loadArrow(buffer);
    }
    
    async loadArrow(buffer: ArrayBuffer | Uint8Array) {
        this.arrowTable = tableFromIPC(buffer);
        this.sortedIndices = null;
        this.setupDataListener();
    }
    
    private setupDataListener() {
        if (!this.arrowTable || !this.regularTable) return;
        
        const numRows = this.arrowTable.numRows;
        const numCols = this.arrowTable.numCols;
        const schema = this.arrowTable.schema;
        
        // Column headers from Arrow schema
        const columnHeaders = [schema.fields.map(f => f.name)];
        
        // Data listener - called only for visible viewport
        const dataListener = (x0: number, y0: number, x1: number, y1: number) => {
            const data: any[][] = [];
            
            for (let col = x0; col < Math.min(x1, numCols); col++) {
                const column = this.arrowTable!.getChildAt(col)!;
                const slice: any[] = [];
                
                for (let row = y0; row < Math.min(y1, numRows); row++) {
                    const actualRow = this.sortedIndices ? this.sortedIndices[row] : row;
                    slice.push(column.get(actualRow));
                }
                data.push(slice);
            }
            
            return {
                num_rows: numRows,
                num_columns: numCols,
                column_headers: columnHeaders.map(h => h.slice(x0, x1)),
                data,
            };
        };
        
        (this.regularTable as any).setDataListener(dataListener);
        (this.regularTable as any).draw();
        
        // Setup column header click for sorting
        this.setupSorting();
    }
    
    private setupSorting() {
        this.regularTable!.addEventListener('click', (e) => {
            const target = e.target as HTMLElement;
            if (target.tagName === 'TH') {
                const meta = (this.regularTable as any).getMeta(target);
                if (meta?.column_header) {
                    const columnName = meta.column_header[meta.column_header.length - 1];
                    this.sortBy(columnName);
                }
            }
        });
    }
    
    sortBy(column: string, direction: 'asc' | 'desc' = 'asc') {
        if (!this.arrowTable) return;
        
        const colIndex = this.arrowTable.schema.fields.findIndex(f => f.name === column);
        if (colIndex === -1) return;
        
        const columnData = this.arrowTable.getChildAt(colIndex)!;
        const indices = Array.from({ length: this.arrowTable.numRows }, (_, i) => i);
        
        indices.sort((a, b) => {
            const va = columnData.get(a);
            const vb = columnData.get(b);
            if (va == null) return 1;
            if (vb == null) return -1;
            const cmp = va < vb ? -1 : va > vb ? 1 : 0;
            return direction === 'asc' ? cmp : -cmp;
        });
        
        this.sortedIndices = indices;
        (this.regularTable as any).draw();
        
        this.dispatchEvent(new CustomEvent('sort', {
            detail: { column, direction },
            bubbles: true
        }));
    }
    
    private setupSelection() {
        this.regularTable!.addEventListener('click', (e) => {
            const target = e.target as HTMLElement;
            if (target.tagName === 'TD') {
                const meta = (this.regularTable as any).getMeta(target);
                if (meta?.y !== undefined) {
                    const actualRow = this.sortedIndices ? this.sortedIndices[meta.y] : meta.y;
                    
                    if (e.shiftKey) {
                        // Range select
                        this.selectedIndices.add(actualRow);
                    } else if (e.ctrlKey || e.metaKey) {
                        // Toggle select
                        if (this.selectedIndices.has(actualRow)) {
                            this.selectedIndices.delete(actualRow);
                        } else {
                            this.selectedIndices.add(actualRow);
                        }
                    } else {
                        // Single select
                        this.selectedIndices.clear();
                        this.selectedIndices.add(actualRow);
                    }
                    
                    this.dispatchEvent(new CustomEvent('row-select', {
                        detail: { 
                            index: actualRow, 
                            row: this.getRow(actualRow),
                            selected: Array.from(this.selectedIndices)
                        },
                        bubbles: true
                    }));
                    
                    (this.regularTable as any).draw();
                }
            }
        });
        
        // Style selected rows
        (this.regularTable as any).addStyleListener(() => {
            for (const td of this.regularTable!.querySelectorAll('td')) {
                const meta = (this.regularTable as any).getMeta(td);
                if (meta?.y !== undefined) {
                    const actualRow = this.sortedIndices ? this.sortedIndices[meta.y] : meta.y;
                    td.classList.toggle('bg-primary/20', this.selectedIndices.has(actualRow));
                }
            }
        });
    }
    
    getRow(index: number): Record<string, any> {
        if (!this.arrowTable) return {};
        const row: Record<string, any> = {};
        for (const field of this.arrowTable.schema.fields) {
            row[field.name] = this.arrowTable.getChild(field.name)!.get(index);
        }
        return row;
    }
    
    getSelectedRows(): Record<string, any>[] {
        return Array.from(this.selectedIndices).map(i => this.getRow(i));
    }
    
    get numRows(): number {
        return this.arrowTable?.numRows || 0;
    }
    
    get schema(): any {
        return this.arrowTable?.schema;
    }
}

customElements.define('arrow-table', ArrowTable);
```

### Server Endpoint for Arrow Data

```python
import pyarrow as pa
from fastapi import Response

@app.get("/sessions/{session_id}/query/{query_id}/arrow")
async def get_arrow_data(session_id: str, query_id: str):
    """Return query results as Arrow IPC stream."""
    result = await get_query_result(session_id, query_id)
    
    # DuckDB → Arrow (zero-copy)
    arrow_table = result.arrow()
    
    # Serialize to IPC stream format
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, arrow_table.schema)
    writer.write_table(arrow_table)
    writer.close()
    
    return Response(
        content=sink.getvalue().to_pybytes(),
        media_type="application/vnd.apache.arrow.stream",
        headers={
            "X-Arrow-Rows": str(arrow_table.num_rows),
            "X-Arrow-Columns": str(arrow_table.num_columns),
        }
    )
```

---

## Vega Chart Web Component

```typescript
// vega-chart.ts
import embed, { VisualizationSpec } from 'vega-embed';
import { read as arrowRead } from 'vega-loader-arrow';

class VegaChart extends HTMLElement {
    private view: any = null;
    
    static get observedAttributes() {
        return ['spec', 'arrow-url'];
    }
    
    async connectedCallback() {
        const container = document.createElement('div');
        container.style.width = '100%';
        container.style.height = this.getAttribute('height') || '300px';
        this.appendChild(container);
        
        await this.render(container);
    }
    
    private async render(container: HTMLElement) {
        const specAttr = this.getAttribute('spec');
        if (!specAttr) return;
        
        const spec: VisualizationSpec = JSON.parse(specAttr);
        const arrowUrl = this.getAttribute('arrow-url');
        
        // Load Arrow data if URL provided
        if (arrowUrl) {
            const response = await fetch(arrowUrl);
            const buffer = await response.arrayBuffer();
            const arrowData = arrowRead(buffer);
            
            // Inject Arrow data into spec
            spec.data = { values: arrowData };
        }
        
        // Render with vega-embed
        const result = await embed(container, spec, {
            actions: {
                export: true,
                source: false,
                compiled: false,
                editor: false,
            },
            theme: this.closest('[data-theme="dark"]') ? 'dark' : undefined,
        });
        
        this.view = result.view;
        
        // Handle click events for drill-down
        this.view.addEventListener('click', (event: any, item: any) => {
            if (item?.datum) {
                this.dispatchEvent(new CustomEvent('chart-click', {
                    detail: { datum: item.datum, event },
                    bubbles: true
                }));
            }
        });
    }
    
    async attributeChangedCallback(name: string, oldVal: string, newVal: string) {
        if (name === 'spec' && oldVal !== newVal && this.view) {
            // Re-render with new spec
            this.innerHTML = '';
            const container = document.createElement('div');
            container.style.width = '100%';
            container.style.height = this.getAttribute('height') || '300px';
            this.appendChild(container);
            await this.render(container);
        }
    }
    
    downloadImage(format: 'png' | 'svg' = 'png') {
        if (this.view) {
            this.view.toImageURL(format).then((url: string) => {
                const link = document.createElement('a');
                link.download = `chart.${format}`;
                link.href = url;
                link.click();
            });
        }
    }
}

customElements.define('vega-chart', VegaChart);
```

---

## Graph Viewer Component (Cytoscape)

```typescript
// graph-viewer.ts
import cytoscape from 'cytoscape';
import dagre from 'cytoscape-dagre';

cytoscape.use(dagre);

class GraphViewer extends HTMLElement {
    private cy: cytoscape.Core | null = null;
    
    static get observedAttributes() {
        return ['data', 'layout'];
    }
    
    connectedCallback() {
        const container = document.createElement('div');
        container.style.width = '100%';
        container.style.height = this.getAttribute('height') || '400px';
        this.appendChild(container);
        
        const data = JSON.parse(this.getAttribute('data') || '{"nodes":[],"edges":[]}');
        const layout = this.getAttribute('layout') || 'dagre';
        
        this.cy = cytoscape({
            container,
            elements: data,
            style: this.getDefaultStyles(),
            layout: { name: layout },
        });
        
        // Emit events for HTMX integration
        this.cy.on('tap', 'node', (evt) => {
            const node = evt.target;
            this.dispatchEvent(new CustomEvent('node-select', {
                detail: { id: node.id(), data: node.data() },
                bubbles: true
            }));
        });
        
        this.cy.on('tap', 'edge', (evt) => {
            const edge = evt.target;
            this.dispatchEvent(new CustomEvent('edge-select', {
                detail: { id: edge.id(), data: edge.data() },
                bubbles: true
            }));
        });
    }
    
    private getDefaultStyles(): cytoscape.Stylesheet[] {
        return [
            {
                selector: 'node',
                style: {
                    'background-color': 'oklch(var(--p))',  // daisyUI primary
                    'label': 'data(label)',
                    'color': 'oklch(var(--pc))',
                    'text-valign': 'center',
                    'text-halign': 'center',
                }
            },
            {
                selector: 'edge',
                style: {
                    'width': 2,
                    'line-color': 'oklch(var(--b3))',
                    'target-arrow-color': 'oklch(var(--b3))',
                    'target-arrow-shape': 'triangle',
                    'curve-style': 'bezier'
                }
            },
            {
                selector: '.highlighted',
                style: {
                    'background-color': 'oklch(var(--a))',  // daisyUI accent
                }
            },
            {
                selector: '.error',
                style: {
                    'background-color': 'oklch(var(--er))',  // daisyUI error
                }
            }
        ];
    }
    
    layout(name: string) {
        this.cy?.layout({ name }).run();
    }
    
    fit() {
        this.cy?.fit();
    }
}

customElements.define('graph-viewer', GraphViewer);
```

---

## SQL Editor Component (CodeMirror)

```typescript
// sql-editor.ts
import { EditorView, basicSetup } from 'codemirror';
import { sql, PostgreSQL } from '@codemirror/lang-sql';
import { oneDark } from '@codemirror/theme-one-dark';

class SqlEditor extends HTMLElement {
    private editor: EditorView | null = null;
    
    static get observedAttributes() {
        return ['value', 'language', 'readonly'];
    }
    
    connectedCallback() {
        const container = document.createElement('div');
        container.className = 'sql-editor-container';
        this.appendChild(container);
        
        const isDark = this.closest('[data-theme="dark"]') !== null;
        
        this.editor = new EditorView({
            doc: this.getAttribute('value') || '',
            extensions: [
                basicSetup,
                sql({ dialect: PostgreSQL }),
                ...(isDark ? [oneDark] : []),
                EditorView.updateListener.of((update) => {
                    if (update.docChanged) {
                        this.dispatchEvent(new CustomEvent('change', {
                            detail: { value: this.value },
                            bubbles: true
                        }));
                    }
                }),
            ],
            parent: container,
        });
    }
    
    get value(): string {
        return this.editor?.state.doc.toString() || '';
    }
    
    set value(v: string) {
        if (this.editor) {
            this.editor.dispatch({
                changes: { from: 0, to: this.editor.state.doc.length, insert: v }
            });
        }
    }
    
    get name(): string {
        return this.getAttribute('name') || 'content';
    }
}

customElements.define('sql-editor', SqlEditor);
```

---

## API Design

### Content Negotiation

```python
from fastapi import Request, Response
from fastapi.responses import HTMLResponse, JSONResponse

def negotiate_response(
    request: Request,
    template: str,
    data: dict,
    actions: list[Action]
) -> Response:
    accept = request.headers.get("accept", "text/html")
    
    if "text/html" in accept:
        return templates.TemplateResponse(
            template,
            {"request": request, **data, "actions": actions}
        )
    
    elif "application/json" in accept:
        return JSONResponse({
            **data,
            "_actions": [action_to_dict(a) for a in actions],
            "_links": {
                "self": str(request.url),
            }
        })
    
    elif "application/vnd.apache.arrow.stream" in accept:
        # Return Arrow for data endpoints
        return get_arrow_response(data)
    
    return templates.TemplateResponse(template, {"request": request, **data})
```

### Route Structure

```python
# Session management
POST   /sessions                      # Create new session
GET    /sessions/{id}                 # Get session state
DELETE /sessions/{id}                 # End session

# Conversation
POST   /sessions/{id}/messages        # Send message (user input)
GET    /sessions/{id}/stream          # SSE stream for agent responses
POST   /sessions/{id}/actions/{aid}   # Execute action

# Data (Arrow-native)
GET    /sessions/{id}/query/{qid}/arrow    # Get query results as Arrow
GET    /sessions/{id}/artifacts/{aid}/arrow # Get artifact data as Arrow

# Artifacts
GET    /sessions/{id}/artifacts/{aid}           # Get artifact (HTML)
GET    /sessions/{id}/artifacts/{aid}/expand    # Get expanded view

# Analysis
GET    /sessions/{id}/analyze/entropy           # Full entropy report
GET    /sessions/{id}/analyze/column/{name}     # Column analysis
GET    /sessions/{id}/schema                    # Schema explorer
GET    /sessions/{id}/schema/graph              # Schema as graph data

# Mutations
POST   /sessions/{id}/fix/{strategy}            # Apply fix
POST   /sessions/{id}/undo                      # Undo last change
POST   /sessions/{id}/commit                    # Commit changes
```

---

## SSE Streaming Protocol

```python
async def stream_agent_response(session: Session):
    """Generate SSE events for agent response."""
    
    async for chunk in agent.run_stream(session):
        
        if chunk.type == "thinking":
            html = render("partials/thinking.html", status=chunk.status)
            yield f"data: {html}\n\n"
        
        elif chunk.type == "text":
            html = render("partials/text_chunk.html", text=chunk.text)
            yield f"data: {html}\n\n"
        
        elif chunk.type == "artifact":
            html = render(f"artifacts/{chunk.artifact.type}.html", 
                         artifact=chunk.artifact,
                         session_id=session.id)
            yield f"data: {html}\n\n"
        
        elif chunk.type == "actions":
            html = render("partials/actions.html", actions=chunk.actions)
            yield f"data: <hx-partial hx-target=\"#actions\">{html}</hx-partial>\n\n"
        
        elif chunk.type == "context_update":
            html = render("partials/context.html", context=chunk.context)
            yield f"data: <hx-partial hx-target=\"#context-panel\">{html}</hx-partial>\n\n"
        
        elif chunk.type == "done":
            yield f"data: <div class=\"stream-complete\"></div>\n\n"
            break
```

---

## Security Considerations

### HTMX-Specific Security

```html
<!-- Escape all user content -->
{{ user_input | e }}

<!-- Use hx-ignore for any raw content -->
<div hx-ignore>
    {{ raw_content }}
</div>

<!-- CSRF protection -->
<meta name="htmx-config" content='{"csrf": true}'>
<input type="hidden" name="csrf_token" value="{{ csrf_token }}">
```

### SQL Injection Prevention

```python
async def execute_user_sql(sql: str, session: Session) -> QueryResult:
    # Validate SQL is read-only
    if not is_select_statement(sql):
        raise ValueError("Only SELECT statements allowed in query editor")
    
    # Execute with timeout and row limit
    return await session.connection.execute(
        sql,
        timeout=30,
        limit=10000
    )
```

---

## Performance Considerations

### Server-Side

- Use async everywhere (FastAPI, async database drivers)
- Cache entropy computations (expensive but deterministic)
- Stream Arrow data instead of JSON
- Use connection pooling

### Client-Side

- Arrow eliminates JSON parse overhead
- regular-table virtual DOM only renders visible rows
- Vega-Lite compiles specs once, updates data efficiently
- HTMX morph swaps preserve DOM state

### Caching

```python
@lru_cache(maxsize=100)
def compute_entropy_cached(dataset_hash: str, column: str) -> float:
    ...

@app.get("/sessions/{id}/schema")
async def get_schema(id: str, request: Request):
    schema = await get_schema_for_session(id)
    etag = compute_etag(schema)
    
    if request.headers.get("if-none-match") == etag:
        return Response(status_code=304)
    
    return Response(
        content=render("schema.html", schema=schema),
        headers={"ETag": etag}
    )
```
