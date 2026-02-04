# Dataraum UI: UX Refinements & Clarifications

## Purpose

This document reconciles user experience goals with the existing technical specification, adding clarity on:
1. Success metrics
2. Differentiated UI patterns for Data Quality vs Analytics
3. Confidence/uncertainty display
4. The drill-down interaction model
5. Agent tool contracts

---

## 1. Success Metrics

The UI exists to minimize these times:

| Metric | Target | How We Measure |
|--------|--------|----------------|
| **Time to understand data issues** | < 30 seconds from page load to comprehension | User can verbalize what's wrong |
| **Time to fix data issues** | < 2 minutes for common fixes | From seeing issue to "fixed" confirmation |
| **Time from question to decision** | < 5 minutes for typical queries | User has actionable insight |
| **Agent trust calibration** | User correctly identifies when to trust/verify | Confidence indicators match actual accuracy |

These metrics should drive every UI decision.

---

## 2. Interaction Model: 90% Buttons, 10% Text

**Principle:** Most user actions should be achievable via buttons. Text input is for:
- Open-ended questions
- Custom SQL
- Edge cases

### Button Hierarchy (HATEOAS-driven)

```
┌─────────────────────────────────────────────────────────────────┐
│ Analysis Result                                                 │
├─────────────────────────────────────────────────────────────────┤
│ Found 3 issues in orders table:                                 │
│ • customer_id: 12% null                                        │
│ • shipping_date: 8% invalid                                    │
│ • amount: 2 negative                                           │
├─────────────────────────────────────────────────────────────────┤
│ Primary Actions (most likely next step)                        │
│ [Fix All Issues]  [Export Report]                              │
├─────────────────────────────────────────────────────────────────┤
│ Secondary Actions (drill deeper)                               │
│ [Review customer_id]  [Review shipping_date]  [Review amount]  │
├─────────────────────────────────────────────────────────────────┤
│ Tertiary Actions (alternatives)                                │
│ [Write Custom SQL]  [Add Context]  [Analyze Further]           │
└─────────────────────────────────────────────────────────────────┘
```

### Action Styling (extends existing Action model)

```python
@dataclass
class Action:
    # ... existing fields ...
    
    # New: Priority for display ordering
    priority: Literal["primary", "secondary", "tertiary"] = "secondary"
    
    # New: Group for visual organization  
    group: str | None = None  # "fix", "analyze", "export", "navigate"
```

---

## 3. Confidence & Uncertainty Display

**Principle:** Never fake certainty. Users don't forgive overconfident AI.

### Confidence Levels

| Level | Visual | When to Use |
|-------|--------|-------------|
| **High** (>90%) | No indicator (default) | Deterministic computations, SQL results |
| **Medium** (70-90%) | Subtle indicator | Pattern detection, recommendations |
| **Low** (<70%) | Explicit warning | Inferences, predictions, ambiguous data |
| **Unknown** | "Unable to determine" | Insufficient data |

### Implementation

```html
<!-- templates/partials/confidence_badge.html -->
{% if confidence < 0.7 %}
<span class="badge badge-warning badge-sm" 
      title="Low confidence: {{ confidence_reason }}">
    ⚠️ {{ "%.0f"|format(confidence * 100) }}%
</span>
{% elif confidence < 0.9 %}
<span class="badge badge-ghost badge-sm opacity-60"
      title="Medium confidence">
    ~{{ "%.0f"|format(confidence * 100) }}%
</span>
{% endif %}
{# High confidence: no badge, don't clutter #}
```

### Where to Show Confidence

- **Show:** Agent recommendations, pattern detection, root cause analysis
- **Don't show:** Row counts, SQL query results, schema metadata (these are facts)

---

## 4. Differentiated UI: Data Quality vs Analytics

### Data Quality UI

Focus: **What's wrong, how bad, how to fix**

```
┌─────────────────────────────────────────────────────────────────┐
│ Data Quality Summary                                            │
├─────────────────────────────────────────────────────────────────┤
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ 🔴 Critical (2)                                             │ │
│ │ ├─ customer_id: 12% null values                            │ │
│ │ │  └─ [Expand] [Fix] [View Data]                          │ │
│ │ └─ amount: 2 negative values                               │ │
│ │    └─ [Expand] [Fix] [View Data]                          │ │
│ └─────────────────────────────────────────────────────────────┘ │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ 🟡 Warning (1)                                              │ │
│ │ └─ shipping_date: 8% invalid format                        │ │
│ │    └─ [Expand] [Fix] [View Data]                          │ │
│ └─────────────────────────────────────────────────────────────┘ │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ 🟢 Good (5 columns)                                         │ │
│ │ └─ [Show Details]                                          │ │
│ └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

**Key patterns:**
- **Ampel system (4 levels):** Critical (🔴), Warning (🟡), Info (🟠), Good (🟢)
- **Expandable text fields** - Not graphs. Text summaries with drill-down.
- **Inline actions** - Fix buttons at every level
- **Sample data access** - [View Data] opens popup table

### Expanded Issue View

```
┌─────────────────────────────────────────────────────────────────┐
│ 🔴 customer_id: 12% null values (847 rows)                     │
├─────────────────────────────────────────────────────────────────┤
│ Root Cause Analysis                                    ~85% ⚠️  │
│ • 92% are guest checkout orders (expected)                     │
│ • 8% are import errors from batch 2024-01-15                   │
├─────────────────────────────────────────────────────────────────┤
│ Affected Segments                                               │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ Product Group A: 234 nulls  [View] [Fix]                   │ │
│ │ Product Group B: 156 nulls  [View] [Fix]                   │ │
│ │ Product Group C: 457 nulls  [View] [Fix]                   │ │
│ └─────────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────────┤
│ Recommended Fixes                                               │
│ [Assign GUEST to checkout nulls]  ← Primary                    │
│ [Backfill from email]  [Write Custom SQL]                       │
├─────────────────────────────────────────────────────────────────┤
│ [Collapse] [Export Report]                                      │
└─────────────────────────────────────────────────────────────────┘
```

### Analytics UI

Focus: **Understand relationships, trace calculations, explore**

```
┌─────────────────────────────────────────────────────────────────┐
│ Revenue Analysis                                                │
├────────────────┬────────────────────────────────────────────────┤
│                │                                                │
│  Lineage       │  Result                                        │
│  Graph         │                                                │
│                │  Total Revenue: €1,234,567                     │
│  [Revenue]     │  ├─ Q1: €312,456                              │
│     │          │  ├─ Q2: €298,123                              │
│     ▼          │  ├─ Q3: €324,567                              │
│  [Orders]──┐   │  └─ Q4: €299,421                              │
│     │      │   │                                                │
│     │      ▼   │  [Drill: By Product] [Drill: By Region]       │
│     ▼   [FX]   │  [Show Chart] [Export]                        │
│  [Items]       │                                                │
│                │                                                │
└────────────────┴────────────────────────────────────────────────┘
```

**Key patterns:**
- **Lineage graph on left** - Shows calculation flow (Cytoscape)
- **Result panel on right** - Aggregated numbers with drill-down
- **Multi-level navigation** - Total → Quarter → Month → Day
- **Every number is clickable** - Drill into any aggregation

---

## 5. Drill-Down Pattern

### Hierarchy Levels

```
Level 0: Summary
    └─> "3 issues found"
    
Level 1: Categories  
    └─> Critical (2), Warning (1), Info (0), Good (5)
    
Level 2: Items
    └─> customer_id: 12% null, amount: 2 negative
    
Level 3: Segments/Slices
    └─> By product group, by date range, by source
    
Level 4: Sample Data
    └─> Actual rows via popup table
```

### Implementation Pattern

```html
<!-- templates/artifacts/dq_issue.html -->
<div class="collapse collapse-arrow" 
     x-data="{ expanded: false, loaded: false }"
     :class="{ 'collapse-open': expanded }">
    
    <!-- Summary (always visible) -->
    <div class="collapse-title flex items-center gap-2"
         @click="expanded = !expanded; if (!loaded) { $dispatch('load-details') }">
        <span class="badge badge-{{ severity }}">{{ severity_icon }}</span>
        <span class="font-medium">{{ column_name }}</span>
        <span class="text-base-content/60">{{ summary }}</span>
        <span class="ml-auto">{{ affected_rows | number }} rows</span>
    </div>
    
    <!-- Details (lazy loaded) -->
    <div class="collapse-content"
         hx-get="/sessions/{{ session_id }}/issues/{{ issue_id }}/details"
         hx-trigger="load-details from:closest div"
         hx-swap="innerHTML">
        <span class="loading loading-spinner loading-sm"></span>
    </div>
</div>
```

### Popup Tables

For Level 4 (sample data), use modal tables:

```html
<!-- templates/partials/data_popup.html -->
<dialog id="data-popup-{{ id }}" class="modal">
    <div class="modal-box max-w-4xl">
        <h3 class="font-bold text-lg">{{ title }}</h3>
        <arrow-table 
            src="/sessions/{{ session_id }}/artifacts/{{ artifact_id }}/arrow"
            max-rows="100">
        </arrow-table>
        <div class="modal-action">
            <button class="btn btn-sm"
                    hx-get="/sessions/{{ session_id }}/artifacts/{{ artifact_id }}/export/csv"
                    hx-swap="none">
                Export CSV
            </button>
            <form method="dialog">
                <button class="btn">Close</button>
            </form>
        </div>
    </div>
</dialog>

<!-- Trigger button -->
<button class="btn btn-xs btn-ghost"
        onclick="document.getElementById('data-popup-{{ id }}').showModal()">
    View Data
</button>
```

---

## 6. Agent Tool Contract

### Input: What the Agent Receives

```python
@dataclass
class AgentContext:
    # Session state
    session_id: str
    connection_info: ConnectionInfo
    active_dataset: str | None
    
    # Schema context
    schema: SchemaInfo
    
    # Conversation history
    messages: list[Message]
    
    # Current focus (what user is looking at)
    focus: Focus | None  # table, column, issue, etc.
    
    # Available actions (for context)
    available_actions: list[Action]
```

### Output: What the Agent Can Do

| Tool | Purpose | Output |
|------|---------|--------|
| `query_database` | Run SQL, get data | Arrow table artifact |
| `analyze_column` | Profile a column | DQ issue artifact with actions |
| `analyze_entropy` | Compute entropy scores | Entropy report artifact |
| `suggest_fix` | Recommend data fix | Fix action with preview SQL |
| `explain_calculation` | Show lineage | Graph artifact |
| `search_knowledge` | RAG over docs | Text with citations |
| `answer_question` | General Q&A about data | Text response |

### Agent Response Format

```python
@dataclass
class AgentResponse:
    # Text explanation
    content: str
    
    # Structured artifacts
    artifacts: list[Artifact]
    
    # Suggested next actions (HATEOAS)
    actions: list[Action]
    
    # Confidence in this response
    confidence: float  # 0.0 - 1.0
    confidence_reason: str | None
```

---

## 7. Updated Component List

### Core Components (from original spec)
- `<arrow-table>` - Data grids with Arrow
- `<vega-chart>` - Charts with Vega-Lite
- `<graph-viewer>` - Lineage/schema with Cytoscape
- `<sql-editor>` - CodeMirror SQL editing

### New/Refined Components

| Component | Purpose | Used In |
|-----------|---------|---------|
| `<dq-issue>` | Expandable issue with actions | DQ UI |
| `<severity-badge>` | Ampel indicator | DQ UI |
| `<confidence-badge>` | Uncertainty indicator | Agent responses |
| `<drill-down>` | Hierarchical navigation | Both |
| `<data-popup>` | Modal table viewer | Both |
| `<lineage-panel>` | Calculation graph | Analytics UI |
| `<action-bar>` | Grouped action buttons | Both |

---

## 8. Implementation Priority

### Phase 2A: DQ-Specific Components (Add to Week 4)

1. **`<dq-issue>` component**
   - Expandable collapse with severity badge
   - Lazy-load details endpoint
   - Inline action buttons

2. **Severity system**
   - 4-level Ampel badges
   - Consistent colors across UI
   - Sort/filter by severity

3. **Drill-down endpoints**
   - `/sessions/{id}/issues/{iid}/details`
   - `/sessions/{id}/issues/{iid}/segments`
   - `/sessions/{id}/issues/{iid}/sample`

### Phase 4A: Analytics-Specific Components (Add to Week 6)

1. **`<lineage-panel>` component**
   - Cytoscape graph on left
   - Result panel on right
   - Click-to-drill on nodes

2. **Calculation trace**
   - Store lineage metadata
   - Render as graph
   - Link to underlying queries

---

## 9. Open Questions (Updated)

1. **Confidence thresholds** - Are 70%/90% the right cutoffs?
2. **Drill-down depth limit** - How many levels before we stop?
3. **Lineage storage** - Do we track calculation graphs, or derive on demand?
4. **Mobile responsiveness** - Is this desktop-only for v1?

---

## Summary

| Aspect | Original Spec | This Refinement |
|--------|---------------|-----------------|
| Success metrics | Implicit | Explicit: time-to-understand, time-to-fix |
| Interaction model | Buttons exist | 90% buttons, prioritized hierarchy |
| Confidence display | Not addressed | 3-level system with badges |
| DQ vs Analytics | Same UI | Differentiated patterns |
| Drill-down | Artifacts + actions | Explicit hierarchy (5 levels) |
| Agent contract | Tools mentioned | Formal I/O specification |
