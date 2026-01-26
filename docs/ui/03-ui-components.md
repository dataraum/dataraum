# Dataraum UI: Components & Screens

## Screen Philosophy

The dataraum UI is **one primary screen with contextual panels**, not multiple separate pages. Think VS Code or a modern IDE—the workspace adapts to context without full page navigation.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Header: Connection status, session info, mode toggles                       │
├──────────────────┬──────────────────────────────────────────────────────────┤
│                  │                                                          │
│  Context Panel   │  Main Canvas                                             │
│  (collapsible)   │                                                          │
│                  │  Conversation messages, artifacts, editors               │
│  - Schema        │  stream here in chronological order                      │
│  - Entropy       │                                                          │
│  - Quality       │                                                          │
│  - Active focus  │                                                          │
│                  │                                                          │
│                  ├──────────────────────────────────────────────────────────┤
│                  │  Input Bar: Message input + contextual actions           │
└──────────────────┴──────────────────────────────────────────────────────────┘
```

---

## Component Hierarchy

### Level 1: Shell

The outermost container that's always present.

```html
<!-- templates/base.html -->
<!DOCTYPE html>
<html lang="en" data-theme="corporate">
<head>
    <meta charset="utf-8">
    <title>dataraum{% block title %}{% endblock %}</title>
    <meta name="htmx-config" content='{
        "defaultSwap": "innerHTML",
        "globalViewTransitions": true
    }'>
    
    <!-- Tailwind + daisyUI (compiled) -->
    <link rel="stylesheet" href="/static/css/app.css">
    
    <!-- HTMX -->
    <script src="/static/js/htmx.min.js"></script>
    <script src="/static/js/alpine.min.js" defer></script>
</head>
<body class="min-h-screen bg-base-200" hx-boost:inherited="true">
    
    <header id="app-header" class="navbar bg-base-100 shadow-sm">
        {% include "partials/header.html" %}
    </header>
    
    <div id="app-layout" class="flex h-[calc(100vh-4rem)]">
        <aside id="context-panel" 
               x-data="{ collapsed: false }"
               :class="collapsed ? 'w-12' : 'w-80'"
               class="bg-base-100 border-r border-base-300 overflow-y-auto transition-all">
            {% block context %}{% endblock %}
        </aside>
        
        <main id="main-canvas" class="flex-1 flex flex-col overflow-hidden">
            <div id="conversation" class="flex-1 overflow-y-auto p-4 space-y-4">
                {% block canvas %}{% endblock %}
            </div>
            
            <footer id="input-bar" class="border-t border-base-300 p-4 bg-base-100">
                {% block input %}{% endblock %}
            </footer>
        </main>
    </div>
    
    <!-- JS Islands (loaded once, used everywhere) -->
    <script type="module" src="/static/js/islands/sql-editor.js"></script>
    <script type="module" src="/static/js/islands/graph-viewer.js"></script>
    <script type="module" src="/static/js/islands/vega-chart.js"></script>
    <script type="module" src="/static/js/islands/arrow-table.js"></script>
    
</body>
</html>
```

### Level 2: Session View

```html
<!-- templates/session.html -->
{% extends "base.html" %}

{% block title %} — {{ session.active_dataset or "New Session" }}{% endblock %}

{% block context %}
<div class="p-4 space-y-4">
    
    <!-- Connection Info -->
    <section class="card bg-base-200">
        <div class="card-body p-3">
            <h3 class="card-title text-sm">Connection</h3>
            <dl class="text-sm space-y-1">
                <div class="flex justify-between">
                    <dt class="text-base-content/60">Source</dt>
                    <dd class="font-mono text-xs">{{ session.connection_string | truncate(20) }}</dd>
                </div>
                <div class="flex justify-between">
                    <dt class="text-base-content/60">Dataset</dt>
                    <dd>{{ session.active_dataset or "None" }}</dd>
                </div>
            </dl>
        </div>
    </section>
    
    <!-- Schema Explorer -->
    <section class="card bg-base-200" 
             hx-get="/sessions/{{ session.id }}/schema"
             hx-trigger="load, schema-changed from:body"
             hx-swap="innerHTML">
        <div class="card-body p-3">
            <span class="loading loading-spinner loading-sm"></span>
            <span class="text-sm">Loading schema...</span>
        </div>
    </section>
    
    <!-- Entropy Summary -->
    <section class="card bg-base-200"
             hx-get="/sessions/{{ session.id }}/entropy/summary"
             hx-trigger="load, data-changed from:body"
             hx-swap="innerHTML">
        <div class="card-body p-3">
            <span class="loading loading-spinner loading-sm"></span>
            <span class="text-sm">Computing entropy...</span>
        </div>
    </section>
    
    <!-- Active Focus -->
    <section class="card bg-base-200" id="active-focus">
        <div class="card-body p-3">
            <h3 class="card-title text-sm">Focus</h3>
            <div id="focus-content" class="text-sm text-base-content/60">
                No active focus
            </div>
        </div>
    </section>
    
</div>
{% endblock %}

{% block canvas %}
<!-- Welcome message if new session -->
{% if not session.messages %}
<article class="chat chat-start">
    <div class="chat-bubble chat-bubble-primary">
        <p>Connected to <strong>{{ session.connection_string }}</strong></p>
        <p class="text-sm opacity-80 mt-1">Try: "What's the data quality like?" or "Show me the schema"</p>
    </div>
</article>
{% endif %}

<!-- Existing messages -->
{% for message in session.messages %}
    {% include "messages/" ~ message.role.value ~ ".html" %}
{% endfor %}
{% endblock %}

{% block input %}
<form hx-post="/sessions/{{ session.id }}/messages"
      hx-target="#conversation"
      hx-swap="beforeend"
      hx-indicator="#thinking"
      class="flex gap-2">
    
    <input type="text" 
           name="content" 
           placeholder="Ask about your data..."
           class="input input-bordered flex-1"
           autocomplete="off"
           autofocus>
    
    <button type="submit" class="btn btn-primary">
        Send
    </button>
    
    <div id="thinking" class="htmx-indicator">
        <span class="loading loading-dots loading-md"></span>
    </div>
</form>

<!-- Contextual Actions -->
<div id="actions" class="mt-2 flex gap-2 flex-wrap">
    <!-- Populated dynamically -->
</div>
{% endblock %}
```

---

## Message Components

### User Message

```html
<!-- templates/messages/user.html -->
<article class="chat chat-end">
    <div class="chat-bubble">
        {{ message.content }}
    </div>
</article>
```

### Agent Message

```html
<!-- templates/messages/agent.html -->
<article class="chat chat-start">
    <div class="chat-bubble chat-bubble-primary space-y-3">
        
        <!-- Text content -->
        {% if message.content %}
        <div class="prose prose-sm">
            {{ message.content | markdown }}
        </div>
        {% endif %}
        
        <!-- Artifacts -->
        {% for artifact in message.artifacts %}
            {% include "artifacts/" ~ artifact.type ~ ".html" %}
        {% endfor %}
        
    </div>
    
    <!-- Actions (outside bubble) -->
    {% if message.actions %}
    <div class="chat-footer mt-2 flex gap-2 flex-wrap">
        {% for action in message.actions %}
            {% include "partials/action_button.html" %}
        {% endfor %}
    </div>
    {% endif %}
</article>
```

### Thinking Indicator

```html
<!-- templates/partials/thinking.html -->
<article class="chat chat-start" id="thinking-indicator">
    <div class="chat-bubble chat-bubble-ghost">
        <span class="loading loading-dots loading-sm"></span>
        <span class="text-sm opacity-70">{{ status or "Thinking..." }}</span>
    </div>
</article>
```

---

## Artifact Components

### Table Artifact (Arrow-powered)

```html
<!-- templates/artifacts/table.html -->
<figure class="artifact-table card bg-base-200 overflow-hidden"
        x-data="{ selected: [], expanded: false }">
    
    {% if artifact.title %}
    <header class="card-body pb-2">
        <h4 class="card-title text-sm">{{ artifact.title }}</h4>
        <p class="text-xs text-base-content/60">
            {{ artifact.metadata.row_count | number }} rows × 
            {{ artifact.metadata.column_count }} columns
        </p>
    </header>
    {% endif %}
    
    <!-- Arrow-powered virtual table -->
    <arrow-table
        height="{{ 'auto' if artifact.metadata.row_count < 20 else '400px' }}"
        selectable
        hx-get="/sessions/{{ session_id }}/artifacts/{{ artifact.id }}/arrow"
        hx-trigger="load"
        hx-swap="none"
        @row-select="selected = $event.detail.selected"
        @sort="
            htmx.ajax('GET', '/sessions/{{ session_id }}/artifacts/{{ artifact.id }}/arrow?sort=' + $event.detail.column + '&dir=' + $event.detail.direction, {
                target: 'previous arrow-table',
                swap: 'none'
            })
        ">
    </arrow-table>
    
    <footer class="card-body pt-2 flex-row gap-2">
        <button class="btn btn-sm btn-ghost"
                x-show="selected.length > 0"
                hx-post="/sessions/{{ session_id }}/actions/analyze-rows"
                hx-vals="js:{ rows: document.querySelector('arrow-table').getSelectedRows() }"
                hx-target="#conversation"
                hx-swap="beforeend">
            Analyze (<span x-text="selected.length"></span>)
        </button>
        
        <button class="btn btn-sm btn-ghost"
                hx-get="/sessions/{{ session_id }}/artifacts/{{ artifact.id }}/export?format=csv">
            Export CSV
        </button>
        
        <button class="btn btn-sm btn-ghost"
                @click="expanded = !expanded"
                x-show="{{ artifact.metadata.row_count }} > 20">
            <span x-text="expanded ? 'Collapse' : 'Expand'"></span>
        </button>
    </footer>
</figure>

<script>
    // HTMX extension to handle Arrow responses
    document.body.addEventListener('htmx:beforeSwap', function(evt) {
        if (evt.detail.xhr.getResponseHeader('content-type')?.includes('arrow')) {
            const table = evt.detail.target.tagName === 'ARROW-TABLE' 
                ? evt.detail.target 
                : evt.detail.target.querySelector('arrow-table');
            if (table) {
                table.loadArrow(evt.detail.xhr.response);
            }
            evt.detail.shouldSwap = false;
        }
    });
</script>
```

### Chart Artifact (Vega-Lite)

```html
<!-- templates/artifacts/chart.html -->
<figure class="artifact-chart card bg-base-200"
        x-data="{ showData: false }">
    
    {% if artifact.title %}
    <header class="card-body pb-2">
        <h4 class="card-title text-sm">{{ artifact.title }}</h4>
    </header>
    {% endif %}
    
    <!-- Vega-Lite chart with Arrow data -->
    <vega-chart
        spec='{{ artifact.data.vega_lite_spec | tojson }}'
        arrow-url="/sessions/{{ session_id }}/artifacts/{{ artifact.id }}/arrow"
        height="300px"
        @chart-click="
            htmx.ajax('GET', '/sessions/{{ session_id }}/analyze/drill-down?datum=' + encodeURIComponent(JSON.stringify($event.detail.datum)), {
                target: '#conversation',
                swap: 'beforeend'
            })
        ">
    </vega-chart>
    
    <footer class="card-body pt-2 flex-row gap-2">
        <button class="btn btn-sm btn-ghost"
                @click="showData = !showData">
            <span x-text="showData ? 'Hide Data' : 'View Data'"></span>
        </button>
        
        <button class="btn btn-sm btn-ghost"
                onclick="this.closest('.artifact-chart').querySelector('vega-chart').downloadImage('png')">
            Download PNG
        </button>
        
        <button class="btn btn-sm btn-ghost"
                onclick="this.closest('.artifact-chart').querySelector('vega-chart').downloadImage('svg')">
            Download SVG
        </button>
    </footer>
    
    <!-- Expandable data view -->
    <div x-show="showData" x-collapse class="border-t border-base-300">
        <arrow-table
            height="200px"
            hx-get="/sessions/{{ session_id }}/artifacts/{{ artifact.id }}/arrow"
            hx-trigger="load"
            hx-swap="none">
        </arrow-table>
    </div>
</figure>
```

### Entropy Report Artifact

```html
<!-- templates/artifacts/entropy.html -->
<figure class="artifact-entropy card bg-base-200">
    
    <header class="card-body pb-2">
        <div class="flex items-center justify-between">
            <h4 class="card-title text-sm">{{ artifact.title or "Entropy Report" }}</h4>
            
            <!-- Overall score badge -->
            <div class="badge badge-lg 
                        {% if artifact.data.overall_score >= 0.8 %}badge-success
                        {% elif artifact.data.overall_score >= 0.6 %}badge-warning
                        {% else %}badge-error{% endif %}">
                {{ "%.0f"|format(artifact.data.overall_score * 100) }}%
            </div>
        </div>
    </header>
    
    <!-- Radar chart for dimension scores -->
    <vega-chart
        spec='{{ artifact.data.radar_spec | tojson }}'
        height="250px">
    </vega-chart>
    
    <!-- Dimension breakdown -->
    <div class="card-body pt-0">
        <div class="grid grid-cols-2 md:grid-cols-3 gap-2 text-sm">
            {% for dim in artifact.data.dimensions %}
            <div class="stat bg-base-300 rounded-lg p-2">
                <div class="stat-title text-xs">{{ dim.name | title }}</div>
                <div class="stat-value text-lg">{{ "%.0f"|format(dim.score * 100) }}%</div>
                {% if dim.delta %}
                <div class="stat-desc text-xs 
                            {% if dim.delta > 0 %}text-success{% else %}text-error{% endif %}">
                    {{ "%+.1f"|format(dim.delta * 100) }}%
                </div>
                {% endif %}
            </div>
            {% endfor %}
        </div>
    </div>
    
    <!-- Issues list -->
    {% if artifact.data.issues %}
    <div class="card-body pt-0">
        <h5 class="font-medium text-sm mb-2">Issues Found</h5>
        <ul class="space-y-1">
            {% for issue in artifact.data.issues[:5] %}
            <li class="flex items-center gap-2 text-sm">
                <span class="badge badge-sm badge-{{ issue.severity }}">{{ issue.severity }}</span>
                <span>{{ issue.message }}</span>
                {% if issue.action %}
                <button class="btn btn-xs btn-ghost"
                        hx-post="{{ issue.action.href }}"
                        hx-target="#conversation"
                        hx-swap="beforeend">
                    Fix
                </button>
                {% endif %}
            </li>
            {% endfor %}
        </ul>
        
        {% if artifact.data.issues | length > 5 %}
        <button class="btn btn-sm btn-ghost mt-2"
                hx-get="/sessions/{{ session_id }}/artifacts/{{ artifact.id }}/issues"
                hx-target="closest ul"
                hx-swap="innerHTML">
            Show all {{ artifact.data.issues | length }} issues
        </button>
        {% endif %}
    </div>
    {% endif %}
    
</figure>
```

### Code Artifact (Read-only)

```html
<!-- templates/artifacts/code.html -->
<figure class="artifact-code card bg-base-200">
    
    <header class="card-body pb-2 flex-row items-center gap-2">
        <span class="badge badge-ghost">{{ artifact.data.language }}</span>
        {% if artifact.title %}
        <h4 class="card-title text-sm">{{ artifact.title }}</h4>
        {% endif %}
    </header>
    
    <div class="mockup-code bg-base-300 rounded-none">
        <pre><code class="language-{{ artifact.data.language }}">{{ artifact.data.content }}</code></pre>
    </div>
    
    <footer class="card-body pt-2 flex-row gap-2">
        <button class="btn btn-sm btn-ghost"
                onclick="navigator.clipboard.writeText(`{{ artifact.data.content | e }}`)">
            Copy
        </button>
        
        {% if artifact.data.language == 'sql' %}
        <button class="btn btn-sm btn-primary"
                hx-post="/sessions/{{ session_id }}/actions/edit-sql"
                hx-vals='{"sql": {{ artifact.data.content | tojson }}}'
                hx-target="#conversation"
                hx-swap="beforeend">
            Edit & Run
        </button>
        {% endif %}
    </footer>
</figure>
```

### Editor Artifact (SQL/YAML)

```html
<!-- templates/artifacts/editor.html -->
<figure class="artifact-editor card bg-base-200"
        x-data="{ dirty: false, running: false }">
    
    <header class="card-body pb-2 flex-row items-center gap-2">
        <span class="badge badge-ghost">{{ artifact.data.language }}</span>
        <span class="badge badge-warning badge-sm" x-show="dirty">Modified</span>
    </header>
    
    <!-- CodeMirror component -->
    <sql-editor 
        name="content"
        value="{{ artifact.data.content }}"
        language="{{ artifact.data.language }}"
        @change="dirty = true"
        class="border-y border-base-300">
    </sql-editor>
    
    <footer class="card-body pt-2 flex-row gap-2">
        <button class="btn btn-sm btn-primary"
                hx-post="/sessions/{{ session_id }}/query/run"
                hx-include="closest .artifact-editor"
                hx-target="#conversation"
                hx-swap="beforeend"
                hx-indicator="closest .artifact-editor"
                @htmx:before-request="running = true"
                @htmx:after-request="running = false; dirty = false"
                :disabled="running">
            <span x-show="!running">Run</span>
            <span x-show="running" class="loading loading-spinner loading-xs"></span>
        </button>
        
        <button class="btn btn-sm btn-ghost"
                hx-post="/sessions/{{ session_id }}/query/explain"
                hx-include="closest .artifact-editor"
                hx-target="next .explain-output"
                hx-swap="innerHTML">
            Explain
        </button>
        
        <button class="btn btn-sm btn-ghost"
                @click="dirty = false" 
                x-show="dirty">
            Discard
        </button>
    </footer>
    
    <div class="explain-output"></div>
</figure>
```

### Graph Artifact (Schema/Lineage)

```html
<!-- templates/artifacts/graph.html -->
<figure class="artifact-graph card bg-base-200"
        x-data="{ 
            selectedNode: null,
            layout: 'dagre'
        }">
    
    {% if artifact.title %}
    <header class="card-body pb-2">
        <h4 class="card-title text-sm">{{ artifact.title }}</h4>
    </header>
    {% endif %}
    
    <!-- Cytoscape component -->
    <graph-viewer
        data='{{ artifact.data | tojson }}'
        :layout="layout"
        height="400px"
        @node-select="
            selectedNode = $event.detail;
            htmx.ajax('GET', '/sessions/{{ session_id }}/schema/node/' + $event.detail.id, {
                target: '#node-details',
                swap: 'innerHTML'
            })
        ">
    </graph-viewer>
    
    <footer class="card-body pt-2">
        <div class="btn-group">
            <button class="btn btn-sm" 
                    :class="layout === 'dagre' ? 'btn-active' : ''"
                    @click="layout = 'dagre'">
                Hierarchy
            </button>
            <button class="btn btn-sm"
                    :class="layout === 'circle' ? 'btn-active' : ''"
                    @click="layout = 'circle'">
                Circle
            </button>
            <button class="btn btn-sm"
                    :class="layout === 'cose' ? 'btn-active' : ''"
                    @click="layout = 'cose'">
                Force
            </button>
        </div>
        
        <button class="btn btn-sm btn-ghost ml-auto"
                @click="$refs.graph.fit()">
            Fit
        </button>
    </footer>
    
    <!-- Node details slide-out -->
    <aside class="drawer drawer-end" x-show="selectedNode">
        <div class="drawer-side">
            <div class="bg-base-200 p-4 w-80" id="node-details">
                <!-- Populated via HTMX -->
            </div>
        </div>
    </aside>
</figure>
```

### Error Artifact

```html
<!-- templates/artifacts/error.html -->
<figure class="artifact-error alert alert-error">
    <svg xmlns="http://www.w3.org/2000/svg" class="stroke-current shrink-0 h-6 w-6" fill="none" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z" />
    </svg>
    <div>
        <h3 class="font-bold">{{ artifact.title or "Error" }}</h3>
        <div class="text-sm">{{ artifact.data.message }}</div>
        
        {% if artifact.data.details %}
        <details class="mt-2">
            <summary class="cursor-pointer text-sm opacity-70">Details</summary>
            <pre class="text-xs mt-1 whitespace-pre-wrap">{{ artifact.data.details }}</pre>
        </details>
        {% endif %}
    </div>
</figure>
```

---

## Partial Components

### Action Button

```html
<!-- templates/partials/action_button.html -->
<button class="{{ action.to_btn_class() }} btn-sm"
        {% for key, val in action.to_html_attrs().items() %}
        {{ key }}="{{ val }}"
        {% endfor %}>
    {% if action.icon %}
    <span class="icon">{{ action.icon }}</span>
    {% endif %}
    {{ action.label }}
</button>
```

### Actions Panel

```html
<!-- templates/partials/actions.html -->
<div class="flex gap-2 flex-wrap">
    {% for action in actions %}
        {% include "partials/action_button.html" %}
    {% endfor %}
</div>
```

### Schema Tree

```html
<!-- templates/partials/schema_tree.html -->
<div class="menu menu-sm bg-base-200 rounded-lg">
    {% for table in schema.tables %}
    <li x-data="{ open: false }">
        <details>
            <summary class="font-medium">
                <span class="icon">📋</span>
                {{ table.name }}
                <span class="badge badge-ghost badge-xs">{{ table.row_count | number }}</span>
            </summary>
            <ul>
                {% for column in table.columns %}
                <li>
                    <a hx-get="/sessions/{{ session_id }}/analyze/column/{{ table.name }}/{{ column.name }}"
                       hx-target="#conversation"
                       hx-swap="beforeend"
                       class="text-xs">
                        <span class="opacity-50">{{ column.type }}</span>
                        {{ column.name }}
                        {% if column.nullable %}
                        <span class="badge badge-warning badge-xs">null</span>
                        {% endif %}
                    </a>
                </li>
                {% endfor %}
            </ul>
        </details>
    </li>
    {% endfor %}
</div>
```

### Entropy Summary Card

```html
<!-- templates/partials/entropy_summary.html -->
<div class="card-body p-3">
    <h3 class="card-title text-sm flex justify-between">
        Entropy
        <span class="badge badge-lg 
                    {% if entropy.score >= 0.8 %}badge-success
                    {% elif entropy.score >= 0.6 %}badge-warning
                    {% else %}badge-error{% endif %}">
            {{ "%.0f"|format(entropy.score * 100) }}%
        </span>
    </h3>
    
    <!-- Mini radar chart -->
    <vega-chart
        spec='{{ entropy.mini_radar_spec | tojson }}'
        height="120px">
    </vega-chart>
    
    <button class="btn btn-sm btn-ghost w-full"
            hx-get="/sessions/{{ session_id }}/analyze/entropy"
            hx-target="#conversation"
            hx-swap="beforeend">
        View Full Report
    </button>
</div>
```

---

## Context Panel Components

### Connection Status

```html
<!-- templates/context/connection.html -->
<section class="card bg-base-200">
    <div class="card-body p-3">
        <div class="flex items-center gap-2">
            <span class="badge badge-success badge-xs"></span>
            <span class="text-sm font-medium">Connected</span>
        </div>
        <p class="text-xs text-base-content/60 font-mono truncate" title="{{ connection_string }}">
            {{ connection_string }}
        </p>
        
        <div class="flex gap-1 mt-2">
            <button class="btn btn-xs btn-ghost"
                    hx-post="/sessions/{{ session_id }}/refresh"
                    hx-target="#schema-explorer">
                Refresh
            </button>
            <button class="btn btn-xs btn-ghost text-error"
                    hx-delete="/sessions/{{ session_id }}"
                    hx-confirm="Disconnect from this data source?">
                Disconnect
            </button>
        </div>
    </div>
</section>
```

### Undo Stack

```html
<!-- templates/context/undo_stack.html -->
<section class="card bg-base-200" x-show="changes.length > 0">
    <div class="card-body p-3">
        <h3 class="card-title text-sm">
            Changes
            <span class="badge badge-warning badge-sm">{{ changes | length }}</span>
        </h3>
        
        <ul class="text-xs space-y-1">
            {% for change in changes[-3:] | reverse %}
            <li class="flex items-center gap-2">
                <span class="badge badge-xs">{{ change.operation }}</span>
                <span class="truncate">{{ change.action_label }}</span>
                <span class="opacity-50">{{ change.affected_rows }} rows</span>
            </li>
            {% endfor %}
        </ul>
        
        <div class="flex gap-1 mt-2">
            <button class="btn btn-xs btn-warning"
                    hx-post="/sessions/{{ session_id }}/undo"
                    hx-target="#conversation"
                    hx-swap="beforeend">
                Undo Last
            </button>
            <button class="btn btn-xs btn-success"
                    hx-post="/sessions/{{ session_id }}/commit"
                    hx-confirm="Commit all changes? This cannot be undone.">
                Commit All
            </button>
        </div>
    </div>
</section>
```

---

## CSS Custom Properties (Tailwind Config)

```javascript
// tailwind.config.js
module.exports = {
  content: ["./templates/**/*.html"],
  theme: {
    extend: {},
  },
  plugins: [
    require("daisyui"),
    require("@tailwindcss/typography"),
  ],
  daisyui: {
    themes: [
      "corporate",   // Light theme for enterprise
      "business",    // Dark theme for enterprise
      "light",
      "dark",
    ],
    darkTheme: "business",
  },
}
```

```css
/* styles/app.css */
@import "tailwindcss";

/* Custom component styles */
@layer components {
  /* regular-table styling */
  regular-table {
    @apply w-full text-sm;
  }
  
  regular-table th {
    @apply bg-base-300 font-medium text-left px-3 py-2 cursor-pointer;
  }
  
  regular-table th:hover {
    @apply bg-base-content/10;
  }
  
  regular-table td {
    @apply px-3 py-2 border-b border-base-300;
  }
  
  regular-table tr:nth-child(even) td {
    @apply bg-base-200/50;
  }
  
  /* CodeMirror in editor */
  .sql-editor-container {
    @apply font-mono text-sm;
  }
  
  .sql-editor-container .cm-editor {
    @apply min-h-[100px];
  }
  
  /* Graph viewer */
  graph-viewer {
    @apply block w-full;
  }
  
  /* Vega chart container */
  vega-chart {
    @apply block w-full;
  }
  
  vega-chart .vega-embed {
    @apply w-full;
  }
}

/* HTMX loading states */
@layer utilities {
  .htmx-request .htmx-indicator {
    @apply opacity-100;
  }
  
  .htmx-indicator {
    @apply opacity-0 transition-opacity;
  }
}
```

---

## Theme Switching

```html
<!-- templates/partials/theme_toggle.html -->
<div class="dropdown dropdown-end">
    <label tabindex="0" class="btn btn-ghost btn-sm">
        <svg class="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" 
                  d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z"/>
        </svg>
    </label>
    <ul tabindex="0" class="dropdown-content menu p-2 shadow bg-base-200 rounded-box w-40">
        <li><a onclick="document.documentElement.setAttribute('data-theme', 'corporate')">Light</a></li>
        <li><a onclick="document.documentElement.setAttribute('data-theme', 'business')">Dark</a></li>
        <li><a onclick="document.documentElement.setAttribute('data-theme', 'light')">System Light</a></li>
        <li><a onclick="document.documentElement.setAttribute('data-theme', 'dark')">System Dark</a></li>
    </ul>
</div>
```
