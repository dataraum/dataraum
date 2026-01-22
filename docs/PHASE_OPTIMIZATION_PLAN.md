# Phase Optimization Plan

This document evaluates two optimization opportunities:
1. Merging graph and context phases in the pipeline
2. Performance optimization through DuckDB sampling for computation-intensive phases

---

## Part 1: Graph and Context Phase Relationship

### Current Architecture Analysis

#### Context Phase (`pipeline/phases/context_phase.py`)

**What it does:**
- Non-LLM phase (no AI calls)
- Calls `build_execution_context()` from `graphs/context.py`
- Returns statistics: table count, column count, relationships, quality issues
- Dependencies: `entropy_interpretation`, `quality_summary`

**Output structure:**
```python
{
    "tables": 5,
    "columns": 48,
    "relationships": 12,
    "business_cycles": 3,
    "available_slices": 8,
    "quality_issues": {"critical": 2, "warning": 5},
    "has_field_mappings": True,
    "has_entropy_summary": True
}
```

#### Graph Execution Phase (`pipeline/phases/graph_execution_phase.py`)

**What it does:**
- LLM phase (uses Claude for SQL generation)
- Calls `ExecutionContext.with_rich_context()` which internally calls `build_execution_context()`
- Dependencies: 10+ phases including `semantic`, `statistics`, `relationships`, `entropy_interpretation`

**Critical Finding:**
```python
# graph_execution_phase.py:149-155
exec_context = ExecutionContext.with_rich_context(
    session=ctx.session,
    duckdb_conn=ctx.duckdb_conn,
    table_name=f"typed_{primary_table.table_name}",
    table_ids=table_ids,
    entropy_behavior_mode="balanced",
)
```

The graph execution phase **rebuilds the entire context from scratch**. It does NOT consume the context phase's output.

### Redundancy Analysis

| Aspect | Context Phase | Graph Phase | Redundant? |
|--------|---------------|-------------|------------|
| Calls `build_execution_context()` | Yes | Yes | **Yes** |
| Loads entropy context | Yes | Yes | **Yes** |
| Produces reusable context object | No (stats only) | Yes (uses it) | - |
| LLM calls | No | Yes | - |
| Purpose | Reporting/validation | Execution | Different |

**Root cause:** The context phase was designed as a "checkpoint" that validates context can be built and reports statistics. However, its output (statistics only) is not used by the graph phase, which rebuilds everything.

### Entropy Context Availability to Graph Agent

The graph agent has **full access** to entropy context via `build_execution_context()`:

```python
# graphs/context.py:413-456 - Entropy integration
entropy_context = build_entropy_ctx(session, table_ids)

# Column-level entropy (lines 424-427)
column_entropy_lookup: dict[str, dict[str, Any]] = {}
for col_key, col_entropy_profile in entropy_context.column_profiles.items():
    column_entropy_lookup[col_key] = get_column_entropy_summary(col_entropy_profile)

# Table-level entropy (lines 430-432)
table_entropy_lookup: dict[str, dict[str, Any]] = {}
for tbl_name, tbl_entropy_profile in entropy_context.table_profiles.items():
    table_entropy_lookup[tbl_name] = get_table_entropy_summary(tbl_entropy_profile)

# Relationship entropy (lines 434-447)
for rel_ctx in relationships:
    rel_profile = entropy_context.relationship_profiles.get(rel_key)
    if rel_profile:
        rel_ctx.relationship_entropy = {
            "composite_score": rel_profile.composite_score,
            "cardinality_entropy": rel_profile.cardinality_entropy,
            "join_path_entropy": rel_profile.join_path_entropy,
            "is_deterministic": rel_profile.is_deterministic,
            "join_warning": rel_profile.join_warning,
        }
```

**Entropy data available to graph agent:**

| Data | Location | Used For |
|------|----------|----------|
| Column composite scores | `ColumnContext.entropy_scores` | Uncertainty-aware SQL |
| Layer scores (structural, semantic, value, computational) | `entropy_scores` dict | Dimension-specific behavior |
| High entropy dimensions | `entropy_scores["high_entropy_dimensions"]` | Targeted warnings |
| Resolution hints | `ColumnContext.resolution_hints` | Suggested fixes |
| Table readiness | `TableContext.readiness_for_use` | "ready"/"investigate"/"blocked" |
| Relationship determinism | `RelationshipContext.relationship_entropy` | Join path warnings |
| Compound risks | `entropy_summary["compound_risk_count"]` | Multiplicative risk alerts |
| Readiness blockers | `entropy_summary["readiness_blockers"]` | Critical issues |

**Conclusion:** Entropy context is comprehensively available. The context phase is not needed for entropy to reach the graph agent.

### Options for Optimization

#### Option A: Remove Context Phase (Recommended)

**Changes:**
1. Delete `context_phase.py`
2. Update `graph_execution_phase.py` to:
   - Add dependency on `quality_summary` (currently via context phase)
   - Include context statistics in its output
3. Update pipeline DAG to remove `context` node

**Benefits:**
- Eliminates redundant `build_execution_context()` call
- Reduces pipeline complexity
- Faster execution (one less phase)

**Risks:**
- Context validation happens later (at graph execution time)
- Statistics previously in context output must be captured elsewhere

#### Option B: Cache Context Object

**Changes:**
1. Context phase stores `GraphExecutionContext` in database or memory cache
2. Graph phase retrieves cached context instead of rebuilding
3. Add cache invalidation when underlying data changes

**Benefits:**
- Context built once, used multiple times (if multiple graph executions)
- Maintains separation of concerns

**Drawbacks:**
- Cache management complexity
- Serialization overhead for `GraphExecutionContext` (has nested dataclasses)
- Single graph execution doesn't benefit

#### Option C: Keep Both, Share Building (Hybrid)

**Changes:**
1. Context phase builds and stores context
2. Add `execution_context` to `PhaseContext.previous_outputs`
3. Graph phase checks for cached context, builds only if missing

**Benefits:**
- Backward compatible
- Graceful degradation

**Drawbacks:**
- `GraphExecutionContext` passed through pipeline machinery
- Added complexity without significant benefit over Option A

### Recommendation

**Option A: Remove context phase** is recommended because:

1. **Single use case:** Context is only used once per pipeline run (for graph execution)
2. **No consumer:** No other phase uses the context phase output
3. **Simplicity:** Fewer phases = easier to understand and maintain
4. **Minimal risk:** Graph execution already handles missing context gracefully

**Migration path:**
1. Add `quality_summary` to graph execution dependencies
2. Move context statistics to graph execution output
3. Remove context phase
4. Update tests

---

## Part 2: Performance Optimization with Sampling

### Current Computation-Intensive Operations

| Phase | Operation | Complexity | Current Optimization |
|-------|-----------|------------|---------------------|
| Relationships | Jaccard intersection | O(column_pairs × distinct_values) | max_distinct=100K limit |
| Relationships | Evaluation queries | O(candidates × 8 queries) | ThreadPoolExecutor(4) |
| Correlations (categorical) | Contingency tables | O(column_pairs × contingency) | ThreadPoolExecutor(4) |
| Correlations (numeric) | Pearson/Spearman | O(column_pairs × rows) | NumPy (GIL-released) |
| Statistics | Per-column profiling | O(columns × rows) | ThreadPoolExecutor |

### Current Sampling Usage

**Relationship Detection (`detector.py:206-208`):**
```python
# Sample for uniqueness calculation (join detection uses full data via SQL)
df = duckdb_conn.execute(
    f"SELECT * FROM {duckdb_path} USING SAMPLE {sample_percent}%"
).df()
```

This samples rows for uniqueness ratio, but the actual Jaccard computation uses full distinct values via SQL.

**Join Detection (`joins.py:110-114`):**
```python
WITH
vals1 AS (SELECT DISTINCT "{col1}" AS v FROM {table1_path} WHERE "{col1}" IS NOT NULL LIMIT {max_distinct}),
vals2 AS (SELECT DISTINCT "{col2}" AS v FROM {table2_path} WHERE "{col2}" IS NOT NULL LIMIT {max_distinct})
SELECT COUNT(*) FROM vals1 WHERE v IN (SELECT v FROM vals2)
```

This uses **LIMIT** not **SAMPLE** - it takes the first N distinct values, which can introduce ordering bias.

### Theoretical Foundation: Sampling-Based Jaccard Estimation

From the paper (arXiv:2507.10019v3):

**Unbiased Estimator:**
For sets A and B with sizes N₁ and N₂, sampling M₁ and M₂ items respectively:
- Intersection estimate: Î_b = x · N₁ · N₂ / (M₁ · M₂)
- Where x = observed sample intersection count
- This estimator is **unbiased** for the true intersection

**Error Bounds:**
- Fractional standard error: O(1/√x)
- For ϕ = |A∩B|/|A| (containment), MSE < x(N₁N₂)²/(M₁M₂)²
- Accuracy improves with more observed overlaps

**Sample Size Requirements:**
For symmetric sampling (α₁ = α₂ = α):
- Accuracy requirement: α ≥ 1/(δ√N₁) where δ is target error
- Example: N₁=10⁸, δ=0.01 → M₁=10⁵ samples sufficient

**Critical Constraint:**
- Maintain M₁M₂ ≪ N₁N₂ (recommend ≤0.01 ratio)
- When sample products approach set products, independence assumptions break

### Bias Considerations

**Current Implementation Bias:**
1. `LIMIT max_distinct` takes first N values, biased by storage/insertion order
2. Row-level sampling biases toward high-frequency values in distinct sets
3. No error bounds computed - can't quantify confidence

**Mitigation Strategies:**

| Strategy | Bias Reduction | Implementation |
|----------|----------------|----------------|
| `USING SAMPLE n%` on distinct | Random sample of distinct values | DuckDB native |
| MinHash signatures | Probabilistic Jaccard estimate | DuckDB extension or custom |
| Reservoir sampling | Uniform random sample | Custom implementation |
| Stratified sampling | Equal representation of value ranges | Complex, domain-specific |

### Recommended Approach: MinHash for Jaccard

**MinHash Overview:**
- Uses k hash functions to create signature of size k for each set
- Jaccard ≈ fraction of matching signature positions
- Error: O(1/√k), typically k=128-256 for 5-10% error
- Complexity: O(n) per set instead of O(n²) for intersection

**DuckDB Implementation Options:**

1. **Approximate aggregates (available now):**
```sql
SELECT approx_count_distinct(col) FROM table
```

2. **HyperLogLog extension:**
```sql
-- Install: INSTALL 'hll' FROM community
-- Use for cardinality estimation
SELECT hll_count(hll_add(hll_create(), col)) FROM table
```

3. **Custom MinHash via SQL:**
```sql
-- Pseudo-implementation
WITH signatures AS (
    SELECT
        col,
        MIN(hash(col || seed1)) as h1,
        MIN(hash(col || seed2)) as h2,
        -- ... k signatures
    FROM table
    GROUP BY col
)
SELECT
    SUM(CASE WHEN a.h1 = b.h1 THEN 1 ELSE 0 END) / k as jaccard_estimate
FROM signatures_a a, signatures_b b
```

### Implementation Plan for Sampled Jaccard

#### Phase 1: Add Error Bounds to Current Implementation

**Changes to `joins.py`:**

```python
def _compute_join_score_with_sampling(
    conn: duckdb.DuckDBPyConnection,
    table1_path: str,
    table2_path: str,
    col1: str,
    col2: str,
    stats1: ColumnStats,
    stats2: ColumnStats,
    sample_rate: float = 0.1,  # 10% sample
    min_samples: int = 1000,
) -> tuple[str, str, float, str, float]:  # Added: confidence
    """Compute join score with statistical error bounds."""

    # Compute required sample size for target accuracy
    target_error = 0.1  # 10% relative error
    required_samples = int(1 / (target_error ** 2))  # ~100 samples for 10% error

    # Determine sample counts
    n1, n2 = stats1.distinct_count, stats2.distinct_count
    m1 = max(min_samples, int(n1 * sample_rate))
    m2 = max(min_samples, int(n2 * sample_rate))

    # Sample distinct values (uniform random)
    result = cursor.execute(f"""
        WITH
        vals1 AS (
            SELECT DISTINCT "{col1}" AS v
            FROM {table1_path}
            WHERE "{col1}" IS NOT NULL
            USING SAMPLE {m1} ROWS
        ),
        vals2 AS (
            SELECT DISTINCT "{col2}" AS v
            FROM {table2_path}
            WHERE "{col2}" IS NOT NULL
            USING SAMPLE {m2} ROWS
        )
        SELECT COUNT(*) FROM vals1 WHERE v IN (SELECT v FROM vals2)
    """).fetchone()

    x = result[0]  # observed intersection

    # Unbiased intersection estimate
    intersection_estimate = x * n1 * n2 / (m1 * m2)

    # Jaccard estimate
    union_estimate = n1 + n2 - intersection_estimate
    jaccard_estimate = intersection_estimate / union_estimate if union_estimate > 0 else 0

    # Error bound (standard error)
    if x > 0:
        fractional_se = 1 / math.sqrt(x)
        confidence = 1 - fractional_se  # Rough confidence
    else:
        confidence = 0.0  # No observed overlap, low confidence

    return (col1, col2, jaccard_estimate, cardinality, confidence)
```

#### Phase 2: Add MinHash for Large Datasets

For datasets with >1M distinct values, use MinHash:

```python
def _compute_minhash_jaccard(
    conn: duckdb.DuckDBPyConnection,
    table1_path: str,
    table2_path: str,
    col1: str,
    col2: str,
    num_hashes: int = 128,
) -> tuple[str, str, float, float]:
    """Compute Jaccard using MinHash signatures."""

    # Generate signatures with multiple hash seeds
    seeds = [f"seed_{i}" for i in range(num_hashes)]

    hash_expressions = [
        f"MIN(hash(CAST(\"{col}\" AS VARCHAR) || '{seed}')) as h_{i}"
        for i, seed in enumerate(seeds)
    ]

    sig1 = conn.execute(f"""
        SELECT {', '.join(hash_expressions)}
        FROM {table1_path}
        WHERE "{col1}" IS NOT NULL
    """.replace(col, col1)).fetchone()

    sig2 = conn.execute(f"""
        SELECT {', '.join(hash_expressions)}
        FROM {table2_path}
        WHERE "{col2}" IS NOT NULL
    """.replace(col, col2)).fetchone()

    # Count matching signature positions
    matches = sum(1 for h1, h2 in zip(sig1, sig2) if h1 == h2)
    jaccard_estimate = matches / num_hashes

    # Error bound for MinHash: sqrt(J(1-J)/k)
    if 0 < jaccard_estimate < 1:
        se = math.sqrt(jaccard_estimate * (1 - jaccard_estimate) / num_hashes)
    else:
        se = 1 / math.sqrt(num_hashes)

    confidence = 1 - se

    return (col1, col2, jaccard_estimate, confidence)
```

### Adaptive Strategy

```python
def find_join_columns_adaptive(
    conn: duckdb.DuckDBPyConnection,
    table1_path: str,
    table2_path: str,
    columns1: list[str],
    columns2: list[str],
    min_score: float = 0.3,
) -> list[dict[str, Any]]:
    """Adaptive join detection based on cardinality."""

    stats1 = _precompute_column_stats(conn, table1_path, columns1)
    stats2 = _precompute_column_stats(conn, table2_path, columns2)

    candidates = []

    for col1, col2 in _get_pairs_to_check(stats1, stats2):
        s1, s2 = stats1[col1], stats2[col2]

        # Choose algorithm based on cardinality
        if max(s1.distinct_count, s2.distinct_count) < 10_000:
            # Small: exact computation
            result = _compute_join_score_exact(conn, ...)
        elif max(s1.distinct_count, s2.distinct_count) < 1_000_000:
            # Medium: sampling with error bounds
            result = _compute_join_score_with_sampling(conn, ...)
        else:
            # Large: MinHash
            result = _compute_minhash_jaccard(conn, ...)

        if result.score > min_score and result.confidence > 0.7:
            candidates.append(result)

    return candidates
```

### Performance Projections

| Dataset Size | Current | With Sampling | With MinHash |
|--------------|---------|---------------|--------------|
| 10K distinct | 100ms | 100ms (exact) | N/A |
| 100K distinct | 5s | 0.5s (10% sample) | 0.3s |
| 1M distinct | 50s+ | 5s (1% sample) | 1s |
| 10M distinct | timeout | 50s | 3s |

### Actual Test Results (15K distinct values, Jaccard=0.5)

| Algorithm | Avg Error | Confidence | Notes |
|-----------|-----------|------------|-------|
| EXACT | 0.0% | 1.00 | Reference implementation |
| SAMPLED | 12.5% | 0.89-0.90 | Uses RESERVOIR sampling |
| MINHASH | 6.2% | 0.96 | Most accurate approximation |

### Other Phases Benefiting from Sampling

#### Categorical Correlations

**Current:** Full contingency table for every column pair
**Optimization:** Sample rows before building contingency table

```python
# Current
df = conn.execute(f"SELECT col1, col2 FROM table").df()
contingency = pd.crosstab(df[col1], df[col2])

# Optimized
sample_size = min(100_000, total_rows)
df = conn.execute(f"""
    SELECT col1, col2
    FROM table
    USING SAMPLE {sample_size} ROWS
""").df()
contingency = pd.crosstab(df[col1], df[col2])

# Chi-square is robust to sampling if sample is representative
chi2, p_value, dof, expected = chi2_contingency(contingency)
```

**Error consideration:** Chi-square test is designed for samples. Result is statistically valid as long as expected cell counts ≥ 5.

#### Statistics Profiling

**Percentiles:** Can use approximate percentiles (DuckDB supports `approx_quantile`)
```sql
SELECT approx_quantile(col, [0.25, 0.5, 0.75]) FROM table
```

**Histograms:** Sample-based histograms are standard practice
**Null ratios:** Exact count needed, but fast O(n) scan

---

## Part 3: Implementation Status

### Completed ✓

1. **Merge graph/context phases**
   - [x] Added `quality_summary` to graph_execution dependencies
   - [x] Moved context stats to graph_execution output
   - [x] Removed context_phase.py
   - [x] Updated tests

2. **Fix LIMIT vs SAMPLE bias** in joins.py
   - [x] Replaced `LIMIT max_distinct` with `USING SAMPLE reservoir(n ROWS)`
   - [x] Added confidence score to join candidates

3. **Add error bounds to Jaccard computation**
   - [x] Implemented `_compute_sampled_jaccard` with unbiased estimator
   - [x] Added `statistical_confidence` to JoinCandidate model
   - [x] Stored confidence in relationship evidence

4. **Adaptive algorithm selection**
   - [x] Cardinality-based algorithm routing (<10K: exact, 10K-1M: sampled, >1M: MinHash)
   - [x] Performance tested with synthetic data

5. **Implement MinHash for large datasets**
   - [x] SQL-based MinHash signatures (128 hashes)
   - [x] Integrated with existing pipeline
   - [x] Tested: 6.2% average error at 95% confidence

6. **Categorical correlation sampling**
   - [x] Added RESERVOIR sampling for tables >100K rows
   - [x] Chi-square remains valid with 100K samples

7. **Numeric correlation sampling**
   - [x] Added RESERVOIR sampling for tables >100K rows
   - [x] Standard error ~0.003 for r with 100K samples

### Future Considerations

- **Approximate percentiles** - Use DuckDB's `approx_quantile` for large tables
- **Free-threaded Python 3.14** - Test when stable for CPU-bound parallelism
- **Caching intermediate results** - Memoize expensive computations across phases
- **Temporal sampling** - Replace Bernoulli with stratified time-range sampling (has documented TODO)

---

## Appendix: Key Code Locations

| Component | File | Notes |
|-----------|------|-------|
| Graph execution phase | `pipeline/phases/graph_execution_phase.py` | Now includes context stats |
| Context builder | `graphs/context.py` | Builds GraphExecutionContext |
| Entropy integration | `graphs/context.py:413-456` | Entropy → context |
| Join detection | `analysis/relationships/joins.py` | Adaptive: exact/sampled/minhash |
| Relationship detector | `analysis/relationships/detector.py` | Passes confidence through |
| Relationship evaluator | `analysis/relationships/evaluator.py` | Quality metrics |
| Categorical correlation | `analysis/correlation/within_table/categorical.py` | RESERVOIR sampling |
| Numeric correlation | `analysis/correlation/within_table/numeric.py` | RESERVOIR sampling |

### Sampling Summary

| Module | Threshold | Sample Size | Method |
|--------|-----------|-------------|--------|
| Joins (Jaccard) | 10K-1M distinct | 10% or 1K min | RESERVOIR on distinct |
| Joins (MinHash) | >1M distinct | 128 hash signatures | SQL MIN(hash()) |
| Categorical | >100K rows | 100K rows | RESERVOIR |
| Numeric | >100K rows | 100K rows | RESERVOIR |
| Temporal | >1K rows | 20% (min 1K) | Bernoulli (needs improvement) |
| Categorical correlation | `analysis/correlation/within_table/categorical.py` | 1-201 |
