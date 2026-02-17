# Design: Composite Key Support & Denormalized Query Model

## Problem Statement

The current relationship model stores single-column foreign key references (`from_column_id` → `to_column_id`). In practice, many schemas use composite keys — a combination of 2+ columns that together form a unique identifier. Examples: `(company_id, fiscal_year)`, `(region, product_code, period)`.

Without composite key support, the system either misses these relationships entirely or detects individual column overlaps that score too low to be useful. This also blocks the planned denormalized query model, where a flat table is materialized for downstream text2sql and quality assessment.

## Current State — What Exists

### Data Model (`relationships/db_models.py`)

Each `Relationship` row stores exactly one column pair:

```
from_table_id → from_column_id
to_table_id   → to_column_id
```

There is no grouping mechanism. A composite FK like `(order_id, line_no)` → `(order_id, line_no)` would need to be represented as two separate `Relationship` rows, but nothing ties them together as a unit.

### Detection (`relationships/joins.py`)

`find_join_columns` compares columns pairwise — every column in table A against every column in table B. It computes Jaccard similarity and containment for each pair independently. There is no phase that groups co-occurring column pairs into a composite key.

### View Builder (`views/builder.py`)

`DimensionJoin` has singular `fact_fk_column` and `dim_pk_column` fields. The generated SQL emits `ON f."col1" = d."col2"` — a single join predicate. Composite keys would need `ON f.a = d.a AND f.b = d.b`.

### Pipeline Flow (`pipeline/base.py`)

```
import → typing → statistics → column_eligibility →
  relationships → semantic → enriched_views → slicing →
  slice_analysis → temporal_slice_analysis → entropy → ...
```

The `enriched_views` phase consumes confirmed relationships to build grain-preserving views. These views feed slicing, quality summary, and indirectly the query agent.

### Downstream Consumers

| Consumer | How it uses relationships | Impact |
|----------|--------------------------|--------|
| **Enriched Views** | Builds LEFT JOINs from confirmed rels | Must emit multi-column ON clauses |
| **Semantic Agent** | Receives candidates for LLM confirmation | Prompt needs composite key context |
| **Query Agent** | Uses `RelationshipContext` for SQL gen | Must know composite join conditions |
| **Relationship Entropy** | Evaluates RI metrics per-relationship | Must evaluate composite RI as unit |
| **Graph Topology** | Builds NetworkX graph from rels | Edges already table-level; no change needed |
| **Cross-Table Quality** | Correlation analysis across tables | Uses views; inherits from enriched_views |

## Proposed Design

### Principle: Relationship Group

Introduce a `relationship_group_id` that ties multiple single-column `Relationship` rows into one logical composite relationship. This is the minimal change that preserves backward compatibility — existing single-column relationships are just groups of size 1.

### Layer 1: Data Model Changes

**`relationships/db_models.py`** — Add `relationship_group_id`:

```python
class Relationship(Base):
    # ... existing fields ...
    
    # NEW: Groups composite key columns into one logical relationship
    # For single-column keys, group_id is the same as relationship_id.
    # For composite keys, multiple rows share the same group_id.
    relationship_group_id: Mapped[str] = mapped_column(
        String, nullable=False, default=lambda: str(uuid4())
    )
    
    # NEW: Position within composite key (0-based)
    # Enables ordered reconstruction: ON a.col0 = b.col0 AND a.col1 = b.col1
    key_position: Mapped[int] = mapped_column(default=0)
```

Migration: Backfill existing rows with `relationship_group_id = relationship_id`, `key_position = 0`.

**`relationships/models.py`** — Add `CompositeJoinCandidate`:

```python
class CompositeJoinCandidate(BaseModel):
    """A composite key relationship between two tables.
    
    Groups multiple JoinCandidate pairs that together form
    a composite foreign key.
    """
    column_pairs: list[tuple[str, str]]  # [(fk_col, pk_col), ...]
    composite_confidence: float  # Combined confidence score
    cardinality: str  # Evaluated on the composite key
    
    # Per-pair details (for transparency)
    pair_details: list[JoinCandidate] = Field(default_factory=list)
    
    # Composite evaluation metrics
    composite_referential_integrity: float | None = None
    composite_orphan_count: int | None = None
    composite_cardinality_verified: bool | None = None
```

### Layer 2: Detection — Composite Key Grouping

Add a new function in `relationships/finder.py` that runs after `find_join_columns`:

```python
def group_composite_keys(
    conn: duckdb.DuckDBPyConnection,
    table1_path: str,
    table2_path: str,
    join_candidates: list[dict],
) -> list[CompositeJoinCandidate]:
    """Group individual column matches into composite key candidates.
    
    Strategy:
    1. For each pair of JoinCandidates between the same two tables,
       check if (col_a, col_b) together is unique in the target table
       when neither col_a nor col_b alone is unique.
    2. Extend to 3+ columns using iterative expansion.
    3. Evaluate composite referential integrity as a unit.
    """
```

**Heuristic for detection:**

The key insight is: if `table2.col_x` and `table2.col_y` are each non-unique, but `(col_x, col_y)` together is unique, and both have Jaccard overlap with corresponding columns in table1, they likely form a composite key.

```sql
-- Check if combination is unique in target table
SELECT COUNT(*) = COUNT(DISTINCT ("col_x", "col_y"))
FROM target_table
WHERE "col_x" IS NOT NULL AND "col_y" IS NOT NULL
```

**Algorithm:**

1. Take all `JoinCandidate` pairs between two tables where `right_uniqueness < 1.0` (the target column alone is not a key).
2. Generate 2-column combinations. For each, check composite uniqueness in the target table.
3. If composite is unique, evaluate composite RI:
   ```sql
   SELECT COUNT(*) as total,
          COUNT(*) FILTER (WHERE t2."col_x" IS NOT NULL) as matched
   FROM source_table t1
   LEFT JOIN target_table t2 
     ON t1."fk_x" = t2."col_x" AND t1."fk_y" = t2."col_y"
   WHERE t1."fk_x" IS NOT NULL AND t1."fk_y" IS NOT NULL
   ```
4. If composite RI is high (>70%), emit a `CompositeJoinCandidate`.
5. Extend to 3-column combinations only if 2-column didn't produce a unique key.

**Complexity guard:** Cap at 4-column composites. Beyond that, the combinatorial explosion is not worth it, and such schemas are rare.

### Layer 3: Storage — Persisting Composite Relationships

In `relationships/detector.py`, the `_store_candidates` function changes to:

```python
# For composite keys, all column pairs share the same group_id
group_id = str(uuid4())
for position, (fk_col, pk_col) in enumerate(composite.column_pairs):
    db_rel = RelationshipDB(
        relationship_id=str(uuid4()),
        relationship_group_id=group_id,
        key_position=position,
        from_column_id=...,
        to_column_id=...,
        # ... other fields ...
    )
```

### Layer 4: Enriched Views — Multi-Column Joins

**`views/builder.py`** — Change `DimensionJoin`:

```python
@dataclass
class DimensionJoin:
    dim_table_name: str
    dim_duckdb_path: str
    # CHANGED: list of (fact_col, dim_col) pairs
    join_columns: list[tuple[str, str]]
    include_columns: list[str] = field(default_factory=list)
    relationship_group_id: str = ""
```

**SQL generation** changes from:

```sql
LEFT JOIN "dim" AS d ON f."fk_col" = d."pk_col"
```

to:

```sql
LEFT JOIN "dim" AS d ON f."fk_x" = d."pk_x" AND f."fk_y" = d."pk_y"
```

**`enriched_views_phase.py`** — `_find_dimension_joins` must group relationships by `relationship_group_id` before building `DimensionJoin` objects.

### Layer 5: Downstream Consumers

**Query Agent / `RelationshipContext`:**

```python
@dataclass
class RelationshipContext:
    from_table: str
    from_columns: list[str]   # was: from_column (str)
    to_table: str
    to_columns: list[str]     # was: to_column (str)
    relationship_type: str
    cardinality: str | None = None
    confidence: float = 0.0
    is_composite: bool = False
```

The prompt template for graph/query SQL generation needs to render composite keys as multi-column join conditions.

**Relationship Entropy:**

Must evaluate composite keys as a unit. The current detector processes each `Relationship` row independently — it needs to group by `relationship_group_id` and compute RI on the composite join before scoring.

**Semantic Agent (`utils.py`):**

`load_relationship_candidates_for_semantic` already groups by table pair. Within each table pair, it needs to sub-group by `relationship_group_id` and present composite candidates as a unit to the LLM.

## Impact Assessment

### Phases Affected

| Phase | Change | Risk |
|-------|--------|------|
| `relationships` | Add composite grouping after pairwise detection | Medium — new logic, but additive |
| `semantic` | Update prompt to handle composite candidates | Low — prompt change only |
| `enriched_views` | Multi-column ON clauses | Medium — SQL generation change |
| `entropy` | Group-level RI evaluation | Low — evaluation logic change |
| `graph_execution` | Composite keys in context | Low — context format change |
| `slicing` | No change (uses enriched views) | None |
| `quality_summary` | No change (uses enriched views) | None |
| `import`, `typing`, `statistics`, `temporal`, `correlations` | No change | None |

### Backward Compatibility

Single-column relationships continue to work unchanged — they're just groups of size 1. The `relationship_group_id` defaults to `relationship_id`, and `key_position` defaults to 0.

### Migration

One-time backfill migration:

```sql
UPDATE relationships 
SET relationship_group_id = relationship_id, 
    key_position = 0 
WHERE relationship_group_id IS NULL;
```

## Denormalized Query Model

With composite keys working, the denormalized query model becomes straightforward.

### Two-Table Strategy

Materialize two representations from the enriched view:

1. **Canonical (nested):** Preserves fact-table grain. 1:N and M:N sides are aggregated into `LIST(STRUCT(...))`. Used for quality assessment, grain-sensitive checks, referential integrity evaluation.

2. **Flat (exploded):** All joins are simple LEFT JOINs with dimension columns prefixed by source table name. Rows may multiply for 1:N relationships. Used for text2sql (LLMs work best against genuinely flat tables).

The `enriched_views` phase already produces the canonical form for N:1/1:1 joins. The flat version extends this by also including 1:N joins (accepting row multiplication) with appropriate column prefixing.

### Column Naming Convention

For the flat version:

```
{source_table}__{column_name}
```

This is already the convention in `build_enriched_view_sql`. It gives the LLM lineage context and avoids collisions.

### Delta Import Consideration

The `relationship_group_id` design is compatible with future delta imports:

- When new data arrives, relationship detection reruns on affected tables only.
- Existing groups are identified by matching column pairs — if the same composite key is re-detected, the group_id is reused.
- Group-level metadata (confidence, cardinality) is recomputed from current data.
- Enriched views are recreated (they're views, not materialized tables, so this is cheap).

The key is that the group_id provides a stable identity for a logical relationship across import cycles.

## Implementation Plan

### Phase 1: Data Model (Low risk, no behavior change)

1. Add `relationship_group_id` and `key_position` columns to `Relationship` model.
2. Write backfill migration.
3. Update `_store_candidates` to set `group_id = relationship_id` for single-column rels.
4. Update tests.

### Phase 2: Detection (Medium risk, new logic)

1. Add `group_composite_keys()` function in `finder.py`.
2. Wire it into `find_relationships()` — call after pairwise detection.
3. Add composite evaluation in `evaluator.py` (composite RI check).
4. Add tests with fixture data that has composite keys.

### Phase 3: Views & Downstream (Medium risk, SQL generation change)

1. Update `DimensionJoin` to multi-column.
2. Update `build_enriched_view_sql` for multi-column ON.
3. Update `_find_dimension_joins` to group by `relationship_group_id`.
4. Update `RelationshipContext` for query agent.
5. Update semantic agent prompt.
6. Update relationship entropy to group-evaluate.

### Phase 4: Flat Query Model (Low risk, additive)

1. Add a second view builder that produces the exploded flat table.
2. Wire into the query agent's context building.
3. Test with text2sql scenarios.

## Open Questions

1. **Junction tables for M:N:** The current system doesn't detect junction/bridge tables. Should composite key detection also identify tables whose PK is the composite of two FKs? This would enable M:N relationship modeling. Recommendation: defer to a separate feature, but the data model supports it (junction tables are just tables where the composite key is also a composite FK).

2. **Composite key cardinality limits:** Should we cap at 3-column composites for v1? The combinatorial cost of checking all N-column subsets grows fast. Recommendation: cap at 4, extend later if needed.

3. **Semantic agent confirmation:** Should composite keys be confirmed as a unit or per-column? Recommendation: as a unit — the LLM should see `(company_id, fiscal_year) → (company_id, fiscal_year)` not two separate confirmations.
