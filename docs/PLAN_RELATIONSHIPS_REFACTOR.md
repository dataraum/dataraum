# Plan: Streamline Relationships Module & Context Assembly

## Goal

Create a focused relationships module that proposes join candidates based on **data characteristics only** (no name matching, no confusing metrics), and assemble rich context from multiple analysis modules to help the semantic agent make informed decisions.

## Part 1: Streamlined Relationships Module

### What to Keep

**Value Overlap Detection (`joins.py`):**
- Jaccard similarity (intersection / union)
- Containment (intersection / smaller set)
- Cardinality detection (one-to-one, one-to-many, etc.)

**Evaluation Metrics (`evaluator.py`):**
- Referential integrity (% of FK values with matching PK)
- Orphan count
- Join success rate
- Duplicate introduction detection (fan trap)

### What to Remove

- `topology.py` - Remove entirely (table-level persistence comparison is confusing)
- `column_feature_similarity` - Remove (statistical feature comparison between columns)
- ID pattern matching - Don't use name-based heuristics
- `topology_similarity` field from models

### Simplified Data Model

```python
class JoinCandidate:
    column1: str
    column2: str

    # Value overlap metrics
    value_overlap: float          # Jaccard or containment score
    cardinality: str              # one-to-one, one-to-many, etc.

    # Column characteristics (from statistics, not name matching)
    left_uniqueness: float        # distinct/total ratio
    right_uniqueness: float

    # Evaluation metrics (from evaluator.py)
    left_referential_integrity: float | None
    right_referential_integrity: float | None
    orphan_count: int | None

class RelationshipCandidate:
    table1: str
    table2: str
    join_candidates: list[JoinCandidate]

    # Relationship-level evaluation
    join_success_rate: float | None
    introduces_duplicates: bool | None
```

### Simplified Flow

```
find_join_columns()          # Value overlap via SQL
    ↓
add_column_statistics()      # Uniqueness ratios from sampled data
    ↓
evaluate_candidates()        # Referential integrity, orphans, duplicates
    ↓
RelationshipCandidate        # Clean output for context assembly
```

---

## Part 2: Context Assembly for Semantic Agent

The semantic agent needs **text context** combining multiple analysis results.

### Context Sources

| Source | Module | What it provides |
|--------|--------|------------------|
| Join Candidates | `relationships/` | Columns that could join between tables |
| Numeric Correlations | `correlation/numeric.py` | Which numeric columns move together |
| Categorical Associations | `correlation/categorical.py` | Which categorical columns are related |
| Functional Dependencies | `correlation/functional_dependency.py` | A → B relationships (composite keys) |
| Derived Columns | `correlation/derived_columns.py` | Computed column detection |
| Column Statistics | `statistics/` | Cardinality, distribution characteristics |

### Context Document Structure (Text for LLM)

```
## Table: orders
Columns: order_id, customer_id, order_date, total_amount, tax, status

### Column Characteristics
- order_id: high cardinality (0.99), likely identifier
- customer_id: medium cardinality (0.15), likely foreign key reference
- status: low cardinality (0.001), categorical with 5 values

### Intra-table Relationships
- total_amount and tax: strong correlation (r=0.92) - tax may be derived
- customer_id → status: no functional dependency (same customer has multiple statuses)

### Functional Dependencies
- order_id → customer_id (confidence: 1.0) - order_id determines customer
- order_id → order_date (confidence: 1.0)

---

## Table: customers
Columns: customer_id, name, city, state, zip_code

### Column Characteristics
- customer_id: high cardinality (0.98), likely primary key
- city: medium cardinality (0.05)
- state: low cardinality (0.01)

### Functional Dependencies
- customer_id → name (confidence: 1.0)
- zip_code → city (confidence: 0.97)
- zip_code → state (confidence: 0.99)

---

## Candidate Relationships Between Tables

### orders ↔ customers
Join candidates (by value overlap):

1. orders.customer_id ↔ customers.customer_id
   - Value overlap: 0.87
   - Cardinality: many-to-one
   - Left uniqueness: 0.15, Right uniqueness: 0.98
   - Referential integrity: 87% of orders have matching customer
   - Orphan orders: 1,342

2. orders.status ↔ customers.state
   - Value overlap: 0.02
   - Cardinality: many-to-many
   - (Low overlap suggests coincidental, not meaningful)

### orders ↔ products
...
```

### Context Assembly Function

```python
async def assemble_relationship_context(
    table_ids: list[str],
    session: AsyncSession,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> str:
    """Assemble text context for semantic agent.

    Combines:
    - Column statistics and characteristics
    - Intra-table correlations and dependencies
    - Cross-table join candidates

    Returns human-readable text for LLM consumption.
    """
    sections = []

    # Per-table context
    for table_id in table_ids:
        table_context = await _build_table_context(table_id, session)
        sections.append(table_context)

    # Cross-table relationship candidates
    relationship_context = await _build_relationship_context(
        table_ids, session, duckdb_conn
    )
    sections.append(relationship_context)

    return "\n\n---\n\n".join(sections)
```

---

## Part 3: Implementation Steps

### Phase 1: Clean up relationships module
1. Remove `topology.py` (or keep only for other uses, not relationships)
2. Simplify `finder.py` - just value overlap + column stats
3. Update models to remove topology fields
4. Update detector.py and evaluator.py

### Phase 2: Context assembly
1. Create `context/relationship_context.py`
2. Function to format table statistics as text
3. Function to format intra-table correlations/dependencies as text
4. Function to format join candidates as text
5. Main assembly function combining all

### Phase 3: Integration with semantic agent
1. Update semantic agent to receive assembled context
2. Agent uses context to confirm/reject relationship candidates
3. Agent can identify relationships the value overlap missed

---

## Questions for Review

1. Should we keep any topology analysis for other purposes (not relationships)?
2. What minimum value overlap threshold should we use? (currently 0.3)
3. Should the context include sample values for ambiguous cases?
4. How much detail in the text context? (balance between helpful and overwhelming)
