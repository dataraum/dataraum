# Query Agent Architecture: RAG-Based Query Reuse

> **Status (2026-02-17):** Design complete, implementation deferred. Graph/query agents are out of scope for the current restructuring — will be re-introduced after pipeline stabilization and testdata work.

## Core Insight

**Reusing validated queries stabilizes entropy faster than generating fresh SQL.**

Traditional approaches have the LLM generate SQL from scratch for each question. This means:
- Every query is "novel" from an entropy perspective
- No learning accumulates across sessions
- Same mistakes get repeated
- Users don't trust results

The DataRaum approach inverts this:
- Build a library of **validated query patterns**
- Search by semantic similarity when users ask questions
- Adapt existing patterns rather than generating from scratch
- Only generate new SQL for genuinely novel patterns
- Validated queries have **known entropy profiles**

---

## The Query RAG Flywheel

```
                    ┌─────────────────────┐
                    │  User asks question │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │  Search query library│
                    │  (semantic similarity)│
                    └──────────┬──────────┘
                               │
              ┌────────────────┴────────────────┐
              │                                 │
    ┌─────────▼─────────┐            ┌──────────▼──────────┐
    │  Match found:     │            │  No match:          │
    │  Adapt existing   │            │  Generate new SQL   │
    │  query            │            │  (with entropy      │
    └─────────┬─────────┘            │  awareness)         │
              │                      └──────────┬──────────┘
              │                                 │
              └────────────────┬────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │  Execute & validate │
                    │  (user feedback,    │
                    │  result inspection) │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │  Save/update library│
                    │  (entropy stabilizes│
                    │  with reuse)        │
                    └─────────────────────┘
```

### Why This Works

| Approach | Entropy Behavior |
|----------|------------------|
| **Generate CTEs each time** | Every query is novel → entropy stays high, no learning |
| **RAG of tested queries** | Reuse validated patterns → entropy drops with each use |

The flywheel effect:
1. More queries in library → better retrieval
2. Better retrieval → more reuse
3. More reuse → queries get validated/refined
4. Refined queries → lower entropy profiles
5. Lower entropy → higher user trust → more usage

---

## Query Library Schema

### QueryDocument Model

The core data structure for all queries in the library is a **QueryDocument**, which captures the complete semantic representation:

```python
@dataclass
class QueryDocument:
    """Complete semantic document for a query."""

    # Required: Plain English description (used for embedding)
    summary: str  # e.g., "Calculates monthly revenue by sales region"

    # Calculation steps with SQL
    steps: list[SQLStep]  # [{step_id, sql, description}]

    # Final executable SQL
    final_sql: str

    # Column mappings (abstract -> concrete)
    column_mappings: dict[str, str]

    # Assumptions made during generation
    assumptions: list[QueryAssumptionData]
```

### Embedding Strategy

The `build_embedding_text()` function creates searchable text from QueryDocuments:

```python
def build_embedding_text(
    summary: str,
    step_descriptions: list[str] | None = None,
    assumption_texts: list[str] | None = None,
    max_chars: int = 1000,  # ~256 tokens for all-MiniLM-L6-v2
) -> str:
    """Build embedding with priority-based truncation.

    Priority order:
    1. Summary (always included - primary semantic signal)
    2. Step descriptions (secondary - calculation methodology)
    3. Assumption texts (tertiary - ambiguity context)
    """
```

### Database Schema (QueryLibraryEntry)

```yaml
query_id: "revenue_by_region_monthly"  # UUID
source_id: "abc123..."

# Semantic content (from QueryDocument)
summary: "Calculates monthly revenue by sales region"
steps_json:
  - step_id: "filter"
    sql: "SELECT * FROM orders WHERE status = 'active'"
    description: "Filter to active customers only"
  - step_id: "aggregate"
    sql: "SELECT region, month, SUM(amount)..."
    description: "Sum amount by region and month"
final_sql: |
  SELECT region, DATE_TRUNC('month', order_date), SUM(amount)
  FROM typed_orders WHERE customer_status = 'active'
  GROUP BY 1, 2
column_mappings:
  revenue: "amount"
  region: "region"

# Assumptions with structured data
assumptions:
  - dimension: "semantic.units"
    target: "column:orders.amount"
    assumption: "Currency is EUR"
    basis: "system_default"
    confidence: 0.9
  - dimension: "value.nulls"
    target: "column:orders.region"
    assumption: "NULL regions excluded"
    basis: "inferred"
    confidence: 0.8

# Embedding for vector search
embedding_text: "Calculates monthly revenue by sales region Filter to active..."

# Usage tracking
usage_count: 47
last_used_at: "2026-01-22"
created_at: "2026-01-15"
```

### Library API

```python
# Save a query (requires QueryDocument)
library.save(
    source_id=source_id,
    document=QueryDocument(
        summary="Calculates revenue by region",
        steps=[SQLStep(...)],
        final_sql="SELECT ...",
        assumptions=[QueryAssumptionData(...)],
    ),
    original_question="What was revenue by region?",
)

# Find similar queries
match = library.find_similar(question, source_id, min_similarity=0.8)
if match:
    # Get full context for LLM injection
    context = match.to_context()
    # Returns: {query_id, summary, calculation_steps, sql, assumptions, similarity_score}
```

---

## The Graph View's Dual Purpose

The web_visualizer prototype shows calculation graphs: how metrics are built from the typed data layer through aggregations to final metrics.

**Purpose 1: Human Understanding**
- Visualize data lineage
- Understand how metrics are calculated
- See which columns contribute to which outputs
- Drill down into calculation steps

**Purpose 2: Query Library Seeding**
- Each graph IS a validated SQL pattern
- Graphs come with semantic tags (metric name, dimensions, business context)
- The visualization provides the "documentation" users need to trust the query
- Expand/collapse reveals calculation breakdown

```
┌─────────────────────────────────────────────────────────┐
│                    MetricNode                           │
│  ┌─────────────────────────────────────────────────┐   │
│  │  Monthly Revenue by Region                       │   │
│  │  €12.1M                                         │   │
│  │  ─────────────────────────────────              │   │
│  │  ▼ Expand calculation                           │   │
│  │    ├─ SUM(amount) from typed_orders            │   │
│  │    ├─ Filtered: customer_status = 'active'     │   │
│  │    ├─ Grouped: region, month                   │   │
│  │    └─ Entropy: 0.2 (ready)                     │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

---

## Existing Modules and Adaptation Needed

### `graphs/` Module (Current State)

The existing graphs module was designed for:
- Pre-defined calculation graphs based on industry-standard metrics
- Financial metrics (revenue, costs, margins, etc.)
- Graph execution with filter application
- Entropy-aware query behavior

**Adaptation needed:**
- Add semantic embeddings for RAG retrieval
- Store validated entropy profiles per graph
- Track usage and validation history
- Support graph composition (combining building blocks)

### `web_visualizer` Prototype (Current State)

Shows:
- MetricNode, FieldNode, AccountNode, TransactionNode
- Expand/collapse for calculation breakdown
- Drill-down modals with sortable tables
- Sidebar navigation between views

**Adaptation needed:**
- Connect to live API (currently uses mock data)
- Add entropy indicators to nodes
- Enable "save as query" workflow
- Support query refinement UI

### `analytics-agents-ts` Prompts (Current State)

Contains sophisticated prompt patterns for:
- SQL generation with DuckDB standards
- Multiple table outputs (executive, analyst, operational)
- Chart type recommendations
- Data quality consideration

**Adaptation needed:**
- Integrate with query library search
- Add "adapt existing query" mode
- Include entropy context in prompts
- Support iterative refinement workflow

---

## Query Agent Workflow

### Step 1: Question Understanding

User asks: "What was our revenue by region last quarter?"

Agent extracts:
- **Metric**: revenue
- **Dimensions**: region
- **Time filter**: last quarter
- **Implicit**: probably wants comparison, totals

### Step 2: Library Search

Search query library with:
```
metric:revenue dimensions:region temporal:quarterly
```

Results:
1. `revenue_by_region_monthly` (0.92 similarity) - adapt time grouping
2. `revenue_by_region_ytd` (0.85 similarity) - different time scope
3. `revenue_total_quarterly` (0.78 similarity) - missing region dimension

### Step 3: Adaptation or Generation

**If good match found:**
```sql
-- Adapted from: revenue_by_region_monthly (v3)
-- Change: DATE_TRUNC('quarter', ...) instead of 'month'
-- Entropy profile: inherited (0.2)
SELECT
  region,
  DATE_TRUNC('quarter', order_date) as quarter,
  SUM(amount) as revenue
FROM typed_orders
WHERE customer_status = 'active'
  AND order_date >= DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '3 months')
GROUP BY 1, 2
```

**If no match (novel pattern):**
Generate with full entropy awareness, flag for validation.

### Step 4: Execution with Entropy Context

Execute query and include:
- Assumptions made (inherited from base query + new ones)
- Entropy warnings if any dimensions are uncertain
- Confidence level based on entropy profile

### Step 5: Result Presentation

Following analytics-agents-ts patterns:
- Multiple table views (executive summary, detailed breakdown)
- Chart recommendations based on data shape
- Assumptions clearly stated
- Option to save as new query or update existing

### Step 6: Feedback Loop

User can:
- **Validate**: "This is correct" → increases confidence
- **Refine**: "Actually, exclude APAC" → creates variant
- **Report issue**: "Numbers don't match" → flags for review
- **Save**: Add to library with description

---

## End-to-End Validation Questions

Before building elaborate UI, validate these assumptions:

1. **Can semantic search find relevant queries?**
   - Test: Embed 50 sample queries, search with natural language
   - Success: >80% of questions find relevant match in top 3

2. **Does query reuse actually stabilize entropy?**
   - Test: Track entropy scores over time for reused vs. novel queries
   - Success: Reused queries show lower variance in results

3. **Can users understand the graph view?**
   - Test: Show calculation graph, ask users to explain what it does
   - Success: Users can correctly describe the calculation

4. **Does the RAG approach feel faster/better to users?**
   - Test: A/B test RAG vs. pure generation
   - Success: RAG queries have higher user satisfaction

---

## Implementation Phases

### Phase 1: API Integration (Current)
- Complete context/entropy/graphs endpoint integration
- Validate existing data structures work end-to-end
- Ensure `GraphExecutionContext` and `EntropyContext` are correct

### Phase 2: Query Library Foundation
- Design query library schema (extend existing graphs/)
- Add semantic embedding storage
- Implement basic similarity search
- Seed with existing graph definitions

### Phase 3: Query Agent MVP
- Implement search → adapt → execute flow
- Basic entropy-aware query generation
- Simple result presentation (tables only)
- Assumption tracking

### Phase 4: Graph Visualization
- Adapt web_visualizer to live API
- Add entropy indicators
- Enable drill-down into calculations
- Connect graph view to query library

### Phase 5: Interactive Refinement
- SQL refinement workflow
- Chart recommendations
- Save/update query workflow
- User feedback collection

### Phase 6: Configuration Hub
- Only after validating core assumptions
- Threshold tuning based on real usage patterns
- Semantic override UI
- Contract configuration

---

## Success Metrics

| Metric | Target | Why It Matters |
|--------|--------|----------------|
| Query reuse rate | >60% | Validates RAG approach |
| Avg entropy of executed queries | <0.4 | Shows stabilization |
| User trust (survey) | >4/5 | End goal |
| Time to answer | <10s for reused | UX target |
| Query library growth | +10/week | Flywheel working |

---

## Related Documents

- [ui-api-consolidation.md](./ui-api-consolidation.md) - Implementation status and phases
- [ENTROPY_QUERY_BEHAVIOR.md](../ENTROPY_QUERY_BEHAVIOR.md) - Agent response policies
- [ENTROPY_CONTRACTS.md](../ENTROPY_CONTRACTS.md) - Data readiness thresholds
- [ENTROPY_IMPLEMENTATION_PLAN.md](../ENTROPY_IMPLEMENTATION_PLAN.md) - Entropy system design
- [BACKLOG.md](../BACKLOG.md) - Task tracking

---

## Contract-Based Confidence Levels

### The Traffic Light Model

When a user queries with a contract, the response includes a **confidence level** based on how well the data meets the contract's entropy thresholds:

| Level | Color | Meaning | Agent Behavior |
|-------|-------|---------|----------------|
| 🟢 **GREEN** | Green | All thresholds pass | Answer confidently |
| 🟡 **YELLOW** | Yellow | Minor issues (within 20% of threshold) | Answer with minor caveats |
| 🟠 **ORANGE** | Orange | Significant issues (some dimensions exceed) | Answer with strong caveats, list issues |
| 🔴 **RED** | Red | Critical issues (blocking violations) | Refuse or require confirmation |

### Confidence Calculation

```python
def calculate_confidence_level(
    evaluation: ContractEvaluation,
) -> ConfidenceLevel:
    """Calculate traffic light confidence from contract evaluation."""

    if evaluation.is_compliant:
        # Check if any dimensions are close to threshold (within 20%)
        near_threshold = [
            v for v in evaluation.warnings
            if v.actual > v.max_allowed * 0.8
        ]
        if near_threshold:
            return ConfidenceLevel.YELLOW
        return ConfidenceLevel.GREEN

    # Non-compliant: check severity
    blocking = evaluation.get_blocking_violations()

    if blocking:
        # Has blocking violations
        if any(v.severity == "critical" for v in blocking):
            return ConfidenceLevel.RED
        return ConfidenceLevel.ORANGE

    # Non-blocking violations only
    return ConfidenceLevel.YELLOW
```

### Response Format with Confidence

```
🟢 Data Quality: GOOD for executive_dashboard

Revenue by region for Q3 2024:
| Region   | Revenue |
|----------|---------|
| EMEA     | €4.2M   |
| APAC     | €2.8M   |
| Americas | €5.1M   |

Total: €12.1M
```

```
🟠 Data Quality: ISSUES for executive_dashboard

Revenue by region for Q3 2024:
| Region   | Revenue |
|----------|---------|
| EMEA     | €4.2M   |
| APAC     | €2.8M   |
| Americas | €5.1M   |

Total: €12.1M

⚠️ Quality Issues:
- Currency: 45% uncertain (threshold: 20%) — assuming EUR
- Temporal: 35% uncertain (threshold: 30%) — assuming calendar quarter

To improve: Add currency field to orders table, or configure default currency.
```

```
🔴 Data Quality: BLOCKED for regulatory_reporting

I cannot provide a reliable answer for regulatory_reporting because:

❌ Currency entropy: 0.72 (max allowed: 0.10)
   - 40% of revenue records have no currency indicator

❌ Temporal alignment: 0.55 (max allowed: 0.10)
   - Mixed calendar/fiscal periods detected

To proceed:
1. Choose a different contract (e.g., exploratory_analysis)
2. Or resolve these issues first

Would you like me to answer with exploratory_analysis contract instead?
```

### Contract Selection in Queries

Users can specify a contract explicitly or let the system choose a default:

```bash
# Explicit contract
dataraum query "What was revenue?" --contract regulatory_reporting

# Default contract (exploratory_analysis)
dataraum query "What was revenue?"

# Ask system to recommend appropriate contract
dataraum query "What was revenue?" --auto-contract
```

With `--auto-contract`, the system evaluates all contracts and recommends the strictest one that passes:

```
📊 Contract Recommendation:

Your data quality supports:
  🟢 exploratory_analysis — PASS
  🟢 operational_analytics — PASS
  🟡 executive_dashboard — MARGINAL (1 warning)
  🔴 regulatory_reporting — FAIL (3 blocking issues)

Using: executive_dashboard (strictest passing contract)

[Answer follows...]
```

---

## Architecture: Library-First Design

### Core Principle

The query agent is implemented as a **library function** that both CLI and API call directly. This ensures:
- Single code path for all interfaces
- Faster iteration (no server restart)
- Easier testing and debugging

```
┌─────────────────────────────────────────────────┐
│                   User                          │
└─────────────────────┬───────────────────────────┘
                      │
        ┌─────────────┼─────────────┐
        ▼             ▼             ▼
   ┌─────────┐  ┌──────────┐  ┌─────────┐
   │   CLI   │  │   API    │  │   MCP   │
   │ dataraum│  │ FastAPI  │  │ Server  │
   └────┬────┘  └────┬─────┘  └────┬────┘
        │            │             │
        └────────────┼─────────────┘
                     ▼
        ┌─────────────────────────┐
        │     Query Agent         │
        │   (Library Function)    │
        │                         │
        │  answer_question()      │
        │  evaluate_contract()    │
        │  search_query_library() │
        └────────────┬────────────┘
                     │
        ┌────────────┼────────────┐
        ▼            ▼            ▼
   ┌─────────┐ ┌──────────┐ ┌──────────┐
   │ Entropy │ │  Query   │ │ Context  │
   │ Context │ │ Library  │ │ Builder  │
   └─────────┘ └──────────┘ └──────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │  ConnectionManager      │
        │  (metadata.db +         │
        │   data.duckdb)          │
        └─────────────────────────┘
```

### Core Library Function

```python
# src/dataraum/query/agent.py

@dataclass
class QueryResult:
    """Result from query agent."""

    # The answer
    answer: str                      # Natural language response
    sql: str | None                  # Generated/adapted SQL
    data: list[dict] | None          # Query results as records

    # Confidence and quality
    confidence_level: ConfidenceLevel  # GREEN/YELLOW/ORANGE/RED
    entropy_score: float               # Overall entropy for this query
    assumptions: list[QueryAssumption] # Assumptions made

    # Contract evaluation (if contract specified)
    contract: str | None               # Contract name used
    contract_evaluation: ContractEvaluation | None

    # Query library tracking
    source_query_id: str | None        # If adapted from library
    is_novel: bool                     # True if newly generated

    # Metadata
    execution_id: str
    executed_at: datetime


def answer_question(
    question: str,
    manager: ConnectionManager,
    source_id: str,
    *,
    contract: str | None = None,       # Explicit contract
    auto_contract: bool = False,       # Find best contract
    behavior_mode: str = "balanced",   # strict/balanced/lenient
    save_to_library: bool = True,      # Save successful queries
) -> QueryResult:
    """Answer a question using the query agent.

    This is the core library function called by CLI, API, and MCP.

    Flow:
    1. Build execution context with entropy
    2. Search query library for similar questions
    3. If match found: adapt existing query
    4. If no match: generate new SQL with entropy awareness
    5. Execute query
    6. Evaluate against contract (if specified)
    7. Format response with confidence level
    8. Optionally save to query library
    """
```

### File Structure

```
src/dataraum/
├── cli.py                    # Extend with query commands
├── query/                    # NEW: Query agent module
│   ├── __init__.py
│   ├── agent.py              # answer_question() - core function
│   ├── library.py            # Query library (store, search, adapt)
│   ├── confidence.py         # Confidence level calculation
│   ├── models.py             # QueryResult, QueryAssumption, etc.
│   └── db_models.py          # QueryLibraryEntry, QueryExecution
├── api/routers/
│   └── query.py              # Extend with /query/agent endpoint
├── entropy/
│   └── contracts.py          # Contract evaluation (implement from spec)
└── mcp/
    └── tools/
        └── query.py          # MCP tool wrapping answer_question()
```

---

## Test Flow

### Complete End-to-End Test Scenario

```bash
# ============================================
# PHASE 1: Data Import
# ============================================

# Import test dataset
dataraum run ./examples/data/financial --output ./test_output

# Verify import succeeded
dataraum status ./test_output
# Output:
#   Source: financial (abc123...)
#   Tables: 5 (raw: 5, typed: 5, quarantine: 0)
#   Rows: 10,234
#   Phases: 18 completed

# Inspect context and entropy
dataraum inspect ./test_output
# Output:
#   Entropy Summary:
#     Overall: 0.42 (INVESTIGATE)
#     Highest: semantic.units (0.65)
#     Compound Risks: 1 (currency + aggregation)

# ============================================
# PHASE 2: Contract Evaluation
# ============================================

# Check which contracts this data supports
dataraum contracts ./test_output
# Output:
#   Contract Compliance:
#   🟢 exploratory_analysis    — PASS (all thresholds met)
#   🟢 data_science            — PASS (all thresholds met)
#   🟡 operational_analytics   — MARGINAL (1 warning)
#   🟠 executive_dashboard     — ISSUES (2 violations)
#   🔴 regulatory_reporting    — BLOCKED (5 violations)

# Get details for a specific contract
dataraum contracts ./test_output --contract executive_dashboard
# Output:
#   Contract: executive_dashboard
#   Status: 🟠 NON-COMPLIANT
#
#   Violations:
#     ❌ semantic.units: 0.65 (max: 0.20)
#        Affected: revenue, costs, margin
#     ❌ semantic.temporal: 0.35 (max: 0.30)
#        Affected: order_date
#
#   Path to Compliance:
#     1. [LOW effort] Add currency to orders table
#        Expected reduction: -0.40 on semantic.units
#     2. [LOW effort] Configure fiscal calendar
#        Expected reduction: -0.10 on semantic.temporal

# ============================================
# PHASE 3: Query Agent Testing
# ============================================

# Simple query with default contract (exploratory)
dataraum query "How many orders are there?" -o ./test_output
# Output:
#   🟢 Data Quality: GOOD for exploratory_analysis
#
#   There are 5,234 orders in the dataset.

# Query with medium entropy
dataraum query "What was total revenue last month?" -o ./test_output
# Output:
#   🟢 Data Quality: GOOD for exploratory_analysis
#
#   Total revenue last month: €1,247,832
#
#   Assumptions:
#   - Currency: EUR (inferred from majority of records)
#   - Period: Calendar month (December 2025)

# Query with stricter contract
dataraum query "What was revenue by region?" -o ./test_output \
    --contract executive_dashboard
# Output:
#   🟠 Data Quality: ISSUES for executive_dashboard
#
#   Revenue by region:
#   | Region   | Revenue   |
#   |----------|-----------|
#   | EMEA     | €4,234,123|
#   | APAC     | €2,891,456|
#   | Americas | €5,102,789|
#
#   ⚠️ Quality Issues:
#   - Currency: 65% uncertain (threshold: 20%)
#     Assuming EUR based on system default
#
#   To improve confidence:
#   - Add currency field to orders table

# Query that would be blocked
dataraum query "What was revenue?" -o ./test_output \
    --contract regulatory_reporting
# Output:
#   🔴 Data Quality: BLOCKED for regulatory_reporting
#
#   Cannot provide answer for regulatory reporting because:
#
#   ❌ semantic.units: 0.65 (max: 0.10)
#   ❌ semantic.temporal: 0.35 (max: 0.10)
#   ❌ computational.aggregations: 0.25 (max: 0.10)
#
#   Options:
#   1. Use --contract exploratory_analysis (will show answer with caveats)
#   2. Resolve data quality issues first
#
#   Run `dataraum contracts ./test_output --contract regulatory_reporting`
#   to see path to compliance.

# Auto-contract selection
dataraum query "What was profit margin?" -o ./test_output --auto-contract
# Output:
#   📊 Contract: operational_analytics (auto-selected)
#
#   Your data supports up to operational_analytics
#   (executive_dashboard has 2 violations)
#
#   🟢 Data Quality: GOOD for operational_analytics
#
#   Profit margin: 23.4%
#
#   Calculation: (Revenue - Costs) / Revenue
#   Based on: 5,234 orders

# ============================================
# PHASE 4: Interactive Mode
# ============================================

dataraum query --interactive -o ./test_output
# Output:
#   DataRaum Query Agent
#   Type 'help' for commands, 'exit' to quit
#   Contract: exploratory_analysis (default)
#
#   > What tables are available?
#   🟢 Tables: orders, customers, products, regions, costs
#
#   > /contract executive_dashboard
#   Contract set to: executive_dashboard
#   ⚠️ Note: Current data has 2 violations for this contract
#
#   > What was revenue by product category?
#   🟠 Data Quality: ISSUES for executive_dashboard
#   [... response ...]
#
#   > /save "revenue_by_category"
#   Query saved to library as: revenue_by_category
#
#   > exit
```

### CLI Commands Summary

| Command | Purpose |
|---------|---------|
| `dataraum run SOURCE` | Import data, run pipeline |
| `dataraum status DIR` | Show pipeline status |
| `dataraum inspect DIR` | Show context and entropy |
| `dataraum contracts DIR` | Evaluate all contracts |
| `dataraum contracts DIR --contract NAME` | Evaluate specific contract |
| `dataraum query "..." -o DIR` | Ask a question |
| `dataraum query "..." --contract NAME` | Ask with specific contract |
| `dataraum query "..." --auto-contract` | Auto-select best contract |
| `dataraum query --interactive` | Interactive REPL mode |

### API Endpoints Summary

| Endpoint | Purpose |
|----------|---------|
| `POST /api/v1/query/agent` | Answer a question |
| `GET /api/v1/contracts` | List all contracts |
| `GET /api/v1/contracts/{name}` | Get contract definition |
| `GET /api/v1/contracts/{name}/evaluate` | Evaluate contract |
| `GET /api/v1/query/library` | List saved queries |
| `POST /api/v1/query/library` | Save query to library |
