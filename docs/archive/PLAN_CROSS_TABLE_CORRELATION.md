# Plan: Cross-Table Correlation & Multicollinearity

## Goal
Add cross-table correlation analysis to the correlation module, including multicollinearity detection.
The analysis runs 2x:
1. Before semantic agent: on staging data + relationship candidates → context for semantic agent
2. After semantic agent: on confirmed relationships → context for downstream quality agents

## Architecture Decision
**All cross-table correlation/multicollinearity lives in `analysis/correlation/`**

Rationale:
- It's computing correlations (unified correlation matrix across tables)
- Multicollinearity is derived from that correlation matrix
- Pure algorithms are reusable for both per-table and cross-table analysis
- Cross-table runner uses existing relationships infrastructure for joins (doesn't rebuild it)

This means `enrichment/cross_table_multicollinearity.py` gets replaced by `correlation/cross_table.py`.

## Current State
- [x] `analysis/correlation/algorithms/` - pure computation functions (numeric, categorical, multicollinearity)
- [x] `analysis/correlation/cross_table.py` - cross-table correlation runner
- `analysis/correlation/` - per-table correlation analysis
- `analysis/relationships/` - relationship detection (topology, finder, joins)
- `enrichment/cross_table_multicollinearity.py` - to be replaced

## Tasks

### Task 0: Create pure algorithms in correlation/algorithms/ ✅
- [x] `algorithms/numeric.py` - pure Pearson/Spearman on numpy arrays (`compute_pairwise_correlations`)
- [x] `algorithms/categorical.py` - pure Cramér's V on contingency tables (`compute_cramers_v`, `build_contingency_table`)
- [x] `algorithms/multicollinearity.py` - pure VDP/condition index (`compute_multicollinearity`)

### Task 1: Create cross-table correlation runner ✅
- [x] Created `analysis/correlation/cross_table.py`
- [x] Uses existing relationships infrastructure (`analysis/relationships/`) for joins
- [x] Accepts explicit relationships (for flexibility)
- [x] Computes multicollinearity analysis (using algorithms/multicollinearity.py)
- [x] Returns rich context: dependency groups, join paths, quality issues

### Task 2: Update models for cross-table support ✅
- [x] Reused existing models from `analysis/relationships/models.py`
- [x] No duplication needed - CrossTableMulticollinearityAnalysis already exists

### Task 3: Integrate with semantic agent ✅
- [x] Updated prompt template with `correlation_context` variable
- [x] Added `_format_correlation_context()` method to SemanticAgent
- [x] Updated `analyze()` and `enrich_semantic()` to accept correlation context
- [x] Added cross-table numeric correlations (strong/very_strong, limited to top 10)
- [x] Added cross-table categorical associations (moderate/strong, limited to top 10)

### Task 4: Integrate with downstream quality ✅
- [x] Added `store_cross_table_analysis()` function
- [x] Added `compute_cross_table_multicollinearity()` convenience wrapper
- [x] Quality context already fetches from CrossTableMulticollinearityMetrics DB

### Task 5: Remove old implementation ✅
- [x] Updated imports in `dataflows/pipeline.py` and `scripts/run_staging_profiling.py`
- [x] Updated import in `tests/quality/test_multi_table_business_cycles.py`
- [ ] Old file `enrichment/cross_table_multicollinearity.py` can be deleted when ready
- [ ] Old test files in `tests/enrichment/` can be updated or removed

### Task 5b: Move cross-table models to correlation/models.py ✅
**Problem:** Cross-table correlation models were in `relationships/models.py` but are primarily used by correlation module.

**Models moved to `correlation/models.py`:**
- [x] `CrossTableNumericCorrelation`
- [x] `CrossTableCategoricalAssociation`
- [x] `CrossTableDependencyGroup`
- [x] `CrossTableMulticollinearityAnalysis`
- [x] `SingleRelationshipJoin` (used for join path context)
- [x] `EnrichedRelationship` (used for building joins)

**Kept in relationships/models.py:**
- `JoinCandidate`, `RelationshipCandidate`, `RelationshipDetectionResult` (relationship detection)
- `DependencyGroup` (within-table multicollinearity)

### Task 6: Use pure algorithms in cross-table analysis ✅
**Problem:** The pure algorithms created in Task 0 were not being used in cross-table analysis.

**Solution:** Updated `cross_table.py` to use all pure algorithms:
- [x] Use `compute_pairwise_correlations` instead of raw `np.corrcoef`
- [x] Add cross-table categorical associations using `compute_cramers_v`
- [x] Handle zero-variance columns (skip from correlation matrix)
- [x] Handle NaN values and masked arrays in correlation matrix
- [x] Use DuckDB `USING SAMPLE` for random data sampling
- [x] Update result model with `numeric_correlations` and `categorical_associations`
- [x] Update `_format_correlation_context()` to include new results

**Results from test data:**
- 30 numeric correlations (17 cross-table)
- 25 categorical associations (11 cross-table)
- Cross-table dependencies detected when data supports it

### Task 7: Test data validation ✅
**Status:** Existing finance CSV example works well with updated implementation.

**What works:**
- [x] `USING SAMPLE` for random sampling now implemented
- [x] Master_txn_table has good numeric columns (Amount, Quantity, Rate, Credit, Debit)
- [x] Cross-table correlations detected (17 cross-table out of 30 total)
- [x] Cross-table categorical associations detected (11 cross-table out of 25 total)
- [x] Multicollinearity groups detected (within-table Business ID dependencies)

**Future improvements (optional):**
- [ ] Create synthetic test dataset with explicit cross-table multicollinearity
- [ ] Add more diverse numeric columns to customer/vendor tables

### Task 8: Fix VDP group splitting for perfectly correlated columns ✅
**Problem:** When 4+ columns are perfectly correlated (r=1.0), the Belsley VDP algorithm spreads
variance across multiple near-zero eigenvalue dimensions. This caused:
1. Columns split into separate groups (e.g., 2 in one group, 2 in another)
2. Some perfectly correlated columns missed entirely (VDP < threshold)

**Example:** 4 Business Id columns from 4 tables (master_txn, customer, vendor, payment_method)
were all perfectly correlated (r=1.0), but only 2 were found in a dependency group.

**Solution:** Added two post-processing functions:

1. **`_merge_correlated_groups()`** - Expands VDP groups:
   - Expands each group to include ALL columns with r >= 0.95 correlation with any member
   - Merges groups that share columns or have highly correlated members

2. **`_find_correlation_clusters()`** - Finds additional groups:
   - Builds a correlation graph where edges connect columns with |r| >= threshold
   - Finds connected components (clusters) of highly correlated columns
   - Creates dependency groups for clusters not already covered by VDP

**Result:** All 4 Business Id columns now correctly appear in a single dependency group spanning
all 4 tables, plus Transaction ID (r=0.999 with Business Id). Additional correlation clusters
are detected even when VDP misses them due to eigenvalue/VDP thresholds.

**Debug mode:** Set `DEBUG_MULTICOLLINEARITY=1` environment variable to see raw VDP results
and merging output.

## Output Context (what downstream agents receive)

```python
CrossTableCorrelationResult:
  - table_ids: list[str]
  - table_names: list[str]
  - relationships_used: list[RelationshipInfo]

  # Numeric Correlations (NEW - from compute_pairwise_correlations)
  - numeric_correlations: list[CrossTableNumericCorrelation]
    - table1, column1: source column
    - table2, column2: target column
    - pearson_r, spearman_rho: correlation coefficients
    - strength: "weak" | "moderate" | "strong" | "very_strong"
    - is_cross_table: bool (whether columns are from different tables)

  # Categorical Associations (NEW - from compute_cramers_v)
  - categorical_associations: list[CrossTableCategoricalAssociation]
    - table1, column1: source column
    - table2, column2: target column
    - cramers_v: association strength
    - strength: "weak" | "moderate" | "strong"
    - is_cross_table: bool

  # Multicollinearity (existing)
  - overall_condition_index: float
  - overall_severity: "none" | "moderate" | "severe"
  - dependency_groups: list[DependencyGroup]
    - involved_columns: list[(table, column)]
    - join_paths: how columns are connected
    - variance_proportions: VDP values

  # Quality context
  - quality_issues: list[QualityIssue]
```

## Signals for Semantic Agent (What & Why)

### Purpose
Cross-table correlation provides **evidence-based context** for the semantic agent to make better decisions about:
- Entity identification (which columns represent the same business concept)
- Relationship validation (which join candidates are actually used)
- Data quality awareness (redundancy, derived columns)

### Three Signal Types

**1. Entity Identification (r ≈ 1.0)**
- Perfect correlations like `customer_table.Business Id <-> master_txn_table.business Id` indicate the same entity
- Helps semantic agent correctly identify foreign key relationships
- Example: All 4 Business Id columns across tables have r=1.0 → same entity, not just similar names

**2. Derived/Related Columns (r > 0.9)**
- Near-perfect correlations suggest derived or tightly coupled columns
- Example: `Transaction ID <-> Business Id` with r=0.999 suggests they increment together
- Helps semantic agent understand data model structure

**3. Categorical Associations (Cramér's V)**
- High V values indicate shared categorical structure
- Example: `Customer name <-> Created user` with V=1.0 suggests same person
- Helps semantic agent identify dimensional relationships

### When to Run Correlation

**Run 1: Before Semantic Agent**
- Input: Relationship candidates (detected joins)
- Purpose: Give semantic agent evidence for relationship decisions
- Filtered output: Only strong/very_strong correlations (limit noise)

**Run 2: After Semantic Agent (Quality Context)**
- Input: Confirmed relationships (semantic agent validated)
- Purpose: Detect data quality issues for downstream agents
- Full output: All correlations, multicollinearity groups, quality issues

### Noise Reduction

**Current filters:**
- Minimum |r| threshold (0.3 for numeric)
- Minimum Cramér's V threshold (0.1 for categorical)
- Only strong/very_strong shown to semantic agent (top 10)

**Potential future filters:**
- Skip ZIP codes (numeric but not meaningful for business correlation)
- Skip columns with high cardinality (UUID-like)
- Focus on columns with semantic meaning (detected entities)

## Investigation Required (Current Issues)

**Status: NOT DONE - Code produces output but correctness unverified**

### Issue 1: VDP Purpose Confusion
**Problem:** We're using VDP to *find* dependency groups, but we already *have* relationships from the relationship detection module. VDP is trying to rediscover relationships instead of enriching them.

**Question:** What should VDP actually tell us that we don't already know from relationships?
- Option A: VDP enriches known relationships with "these columns have multicollinearity issues"
- Option B: VDP finds *additional* dependencies not captured by join detection
- Option C: VDP is the wrong tool entirely for cross-table analysis

**Investigation needed:**
- [ ] Compare VDP groups vs relationship join columns - are they finding the same things?
- [ ] Understand what VDP adds beyond "these columns are correlated"
- [ ] Consider if we should just use correlation matrix directly without VDP

### Issue 2: Cramér's V Only Returns 1 Value (Single-Table)
**Problem:** Test output shows "Found 1 categorical associations (V >= 0.1)" for Master_txn_table which has many categorical columns. This seems wrong.

**Investigation needed:**
- [ ] Trace through `compute_categorical_associations` for single-table case
- [ ] Check what columns are being passed to the algorithm
- [ ] Verify the contingency table building is correct
- [ ] Check if filtering is too aggressive

### Issue 3: Data Loss Between Steps 6c and 6d
**Problem:** We build 19 enriched relationships but the cross-table analysis output seems limited.

**Investigation needed:**
- [ ] How many rows come back from the join query?
- [ ] How many columns are actually being analyzed?
- [ ] Where is data being filtered out?
- [ ] Is the sampling working correctly?

### Issue 4: Matrix Collection Unknown
**Problem:** We don't fully understand how the correlation matrix is being built from the joined data.

**Additional finding:** Section 6f "Debug - Direct correlation matrix" only queries ONE table (first_table), not the cross-table joined data. This debug section is misleading - it doesn't show what the cross-table analysis actually computes.

**Investigation needed:**
- [ ] Trace the exact SQL query being executed in cross-table analysis
- [ ] Log the actual cross-table correlation matrix (not single-table)
- [ ] Check which columns make it into the matrix
- [ ] Verify column ordering matches between matrix and metadata
- [ ] Check for off-by-one or index mapping errors

### Issue 5: Merging Approach May Be Wrong
**Problem:** Added `_merge_groups_by_correlation` and `_find_correlation_clusters` as post-processing to fix VDP splitting. But this might be papering over a deeper misunderstanding.

**Question:** If we need to merge VDP groups using correlation, why use VDP at all?

**Investigation needed:**
- [ ] Understand what VDP provides that raw correlation clustering doesn't
- [ ] Evaluate if post-processing undermines VDP's value
- [ ] Consider simpler alternatives

### Investigation Plan

**Step 1: Understand current data flow** ✅ DONE

```
4 tables (810K + 50K + 100K + 189 rows)
          ↓
20 join columns detected (6 relationship pairs)
          ↓
40 metadata queries (2 per join = SQLAlchemy column ID lookups, NOT data)
          ↓
1 JOIN query (master LEFT JOIN vendor LEFT JOIN customer LEFT JOIN payment_method)
          ↓ SAMPLE 10000 rows
10000 rows × 47 columns
          ↓ filter to numeric
14 numeric columns (from all 4 tables)
          ↓
ONE 14×14 correlation matrix
          ↓
ONE VDP analysis
```

**Findings:**
- The "40 queries" are metadata lookups, not data queries
- ONE joined dataset is used for correlation
- 14 numeric columns make it into the matrix
- All 4 Business Id columns have r=1.000000 (perfect correlation)
- VDP splits them across eigenvalue dimensions (mathematically correct)

**Step 2: Single-table Cramér's V** ✅ FIXED

**Bug found and fixed in `categorical.py`:**
- Function selected columns by distinct count only, ignored `resolved_type`
- `business Id` (BIGINT) was incorrectly included → V=1.0 with `Created user`

**Fix applied:** Added type filter `if col.resolved_type not in ("VARCHAR", "BOOLEAN"): continue`

**After fix:**
- 6 categorical columns pass filter (VARCHAR/BOOLEAN with 2-100 distinct)
- All Cramér's V values are < 0.025 (below 0.1 threshold)
- 0 associations returned = CORRECT behavior for this dataset
- The data just doesn't have strong categorical associations

**Step 3: VDP vs Relationships** ✅ DONE

**VDP raw output (with 4 perfectly correlated Business Id columns):**
- dim=10: Only Transaction ID has VDP > 0.5
- dim=11: Only master_txn.business Id has VDP > 0.5
- dim=12: Only vendor.Business Id has VDP > 0.5
- dim=13: customer.Business Id AND payment_method.Business Id both have VDP > 0.5

**Result**: VDP finds only 1 group with 2 columns. The other 2 Business Id columns are isolated in separate dimensions.

**After merging**: All 5 columns correctly grouped (4 Business Id + Transaction ID).

**Conclusion**: VDP mathematically distributes variance across dimensions. Our correlation-based merging fixes this, but then **we're using correlation to fix what VDP misses**.

**Step 4: Decide on purpose** ✅ RESOLVED

**Multicollinearity is a DATA QUALITY metric, not relationship discovery.**

Expert guidance on value:
- **Redundancy detection**: e.g., storing "age in years" AND "age in days"
- **Consistency/Integrity**: expected correlations vs. outliers indicating data entry errors
- **Efficiency**: identifying redundant storage

**Key insight**: The value isn't for a future regression model - it's about the inherent quality and structure of the data itself.

**Challenges with noisy real-world data:**
| Challenge | Impact | Best Practice |
|-----------|--------|---------------|
| Outliers | Inflate/deflate correlations | Clean data first (Z-score, IQR) |
| Missing values | Biased results | Impute or remove |
| Data types | VIF is for continuous only | Encode or use different metrics |
| Spurious correlations | False positives in large datasets | Apply domain knowledge |
| Non-linearity | Misses complex patterns | EDA, visualizations |

**Conclusion**: Multicollinearity should be:
1. Run AFTER data cleaning/type resolution
2. Used as a DATA QUALITY signal, not relationship discovery
3. Interpreted with domain context
4. Part of the quality assessment pipeline (after semantic agent)

**Remove the correlation-based merging** - it was trying to make VDP do relationship discovery, which is wrong. VDP should report what it finds; interpretation comes from domain knowledge.

## Changes Made (This Session)

1. ✅ **Removed `_find_correlation_clusters`** - was trying to do relationship discovery
2. ✅ **Removed `_merge_groups_by_correlation`** - was papering over VDP behavior
3. ✅ **Updated `_compute_multicollinearity_analysis`** to use raw VDP output
4. ✅ **Fixed Cramér's V bug** - added `resolved_type` filter in `categorical.py`

## Current Output (Verified)

```
Single-table (master_txn_table):
  - 12 numeric correlations (business Id ↔ Transaction ID: r=0.999)
  - 0 categorical associations (correct - no strong associations within table)
  - 3 derived columns (Credit = Quantity × Rate)
  - 186 functional dependencies

Cross-table (4 tables joined):
  - 42 numeric correlations (27 cross-table)
  - 110 categorical associations (70 cross-table)
  - 1 VDP dependency group: [master_txn.business Id, payment_method.Business Id]
  - Overall CI: 999.0 (severe) - indicates redundancy across tables
```

## Per-Relationship Evaluation Architecture (NEW)

### Problem with Current Approach

The current implementation builds ONE big matrix with ALL join candidates:
```
4 tables → 20 join columns → 1 JOIN query → ONE 14×14 matrix → ONE VDP analysis
```

This approach tries to **discover** relationships using VDP, but we already **have** relationship candidates from TDA + join detection. Using VDP on the global matrix:
- Mixes signals from multiple relationships
- Loses context about which relationship introduced which dependency
- Cannot tell us "this specific relationship is good/bad"

### New Approach: Evaluate, Don't Discover

**Key insight**: We should EVALUATE existing relationship candidates, not discover new ones.

```
Relationship Detection (TDA + joins)
          ↓
RelationshipCandidate list (table pairs with join columns)
          ↓
FOR EACH candidate:
    Join ONLY those 2 tables
    Compute quality metrics for that pair
    Store metrics ON the relationship
          ↓
Enriched relationships with quality scores
```

### Per-JoinCandidate Metrics (Column Pair Level)

These metrics evaluate the quality of a specific join column pair:

| Metric | Description | Good Value | Bad Sign |
|--------|-------------|------------|----------|
| **Referential Integrity (Left)** | % of FK values with matching PK | > 95% | < 80% = orphan records |
| **Referential Integrity (Right)** | % of PK values referenced | varies | very low = unused dimension |
| **Join Column Correlation** | Pearson/Spearman on matched rows | High (> 0.9) for numeric FKs | N/A |
| **Cardinality Verification** | Detected vs actual cardinality | Match | Mismatch = wrong detection |

Note: Type compatibility is not checked here - the relationship detection algorithms (TDA, join detection) already enforce strict type matching. TDA cannot work across different types.

**Referential Integrity Calculation:**
```sql
-- Left coverage (FK → PK match rate)
SELECT COUNT(*) FILTER (WHERE pk_table.pk_col IS NOT NULL) * 100.0 / COUNT(*)
FROM fk_table LEFT JOIN pk_table ON fk_table.fk_col = pk_table.pk_col

-- Right coverage (PK referenced rate)
SELECT COUNT(DISTINCT fk_table.fk_col) * 100.0 / COUNT(DISTINCT pk_table.pk_col)
FROM pk_table LEFT JOIN fk_table ON pk_table.pk_col = fk_table.fk_col
```

**Cardinality Verification:**
```sql
-- Check if "one-to-many" is actually 1:N
SELECT MAX(cnt) FROM (
    SELECT fk_col, COUNT(*) as cnt
    FROM fk_table GROUP BY fk_col
)
-- If MAX(cnt) = 1, it's 1:1 not 1:N
```

### Per-RelationshipCandidate Metrics (Table Pair Level)

These metrics evaluate the overall quality of joining two tables:

| Metric | Description | Good Value | Bad Sign |
|--------|-------------|------------|----------|
| **Join Success Rate** | % of rows from table1 that match | > 90% | < 50% = poor join |
| **Duplicate Introduction** | Does join multiply rows? | No duplicates | Multiplication = fan trap |
| **NULL Rate After Join** | % of NULLs in joined columns | Low | High = sparse relationship |
| **Cross-Table Correlation** | Correlation between non-join columns | Meaningful | Random = unrelated data |
| **Redundancy Score** | Condition Index after join | CI < 30 | CI > 30 = redundant columns |

**Join Success Rate:**
```sql
SELECT COUNT(*) FILTER (WHERE t2.pk_col IS NOT NULL) * 100.0 / COUNT(*)
FROM table1 t1 LEFT JOIN table2 t2 ON t1.fk_col = t2.pk_col
```

**Duplicate Introduction Check:**
```sql
-- Row count before vs after join
SELECT
    (SELECT COUNT(*) FROM table1) as before_count,
    (SELECT COUNT(*) FROM table1 t1
     LEFT JOIN table2 t2 ON t1.fk_col = t2.pk_col) as after_count
-- If after > before, duplicates introduced
```

**Redundancy Score (VDP applied PER-PAIR):**
```python
# Join only these 2 tables
joined_data = join(table1, table2, on=relationship.join_columns)

# Get numeric columns from both tables
numeric_cols = get_numeric_columns(joined_data)

# Compute correlation matrix for THIS pair only
corr_matrix = compute_correlation(joined_data[numeric_cols])

# Apply VDP to detect if joining introduces multicollinearity
vdp_result = compute_multicollinearity(corr_matrix)

# Store: CI and which columns are redundant
```

### Module Organization (Option A)

```
analysis/relationships/
├── detector.py          # TDA + join detection (existing)
├── evaluator.py         # NEW: per-relationship quality metrics
├── models.py            # JoinCandidate, RelationshipCandidate (extended)
├── db_models.py         # Relationship (evaluation stored in evidence JSON)
└── ...

analysis/correlation/
├── algorithms/          # Pure algorithms (VDP, Pearson, Cramér's V)
│   ├── numeric.py
│   ├── categorical.py
│   └── multicollinearity.py
├── numeric.py           # Within-table numeric correlations
├── categorical.py       # Within-table categorical associations
├── cross_table.py       # REPURPOSE: Quality-focused (post-confirmation, uses VDP)
└── models.py            # Within-table models only
```

**Separation of concerns:**
- `relationships/` owns the candidate lifecycle (detection → evaluation → confirmation)
- `correlation/` owns quality analysis on confirmed data (VDP, cross-table correlations)
- Pure algorithms in `correlation/algorithms/` can be imported by either if needed later

### Model Cleanup

**correlation/models.py - REMOVE (global matrix approach):**
```python
# DELETE these - part of global matrix approach we're removing
CrossTableNumericCorrelation      # → not needed
CrossTableCategoricalAssociation  # → not needed
CrossTableDependencyGroup         # → VDP moves to quality module
CrossTableMulticollinearityAnalysis  # → global analysis removed
EnrichedRelationship              # → used for global joins
SingleRelationshipJoin            # → used for global join paths
```

**correlation/models.py - KEEP (within-table analysis):**
```python
# KEEP these - valid for within-table analysis
NumericCorrelation
CategoricalAssociation
FunctionalDependency
DerivedColumn
CorrelationAnalysisResult
```

**relationships/db_models.py - REMOVE:**
```python
# DELETE - part of global matrix approach
CrossTableMulticollinearityMetrics
```

**relationships/db_models.py - KEEP:**
```python
# KEEP - stores confirmed relationships
Relationship  # evaluation metrics go in evidence JSON field
```

### Model Updates

**Extend JoinCandidate (relationships/models.py):**
```python
class JoinCandidate(BaseModel):
    column1: str
    column2: str
    confidence: float
    cardinality: str  # one-to-one, one-to-many, many-to-one

    # NEW: Evaluation metrics (populated by evaluator.py)
    left_referential_integrity: float | None = None  # 0-100%
    right_referential_integrity: float | None = None  # 0-100%
    orphan_count: int | None = None
    cardinality_verified: bool | None = None
```

**Extend RelationshipCandidate (relationships/models.py):**
```python
class RelationshipCandidate(BaseModel):
    table1: str
    table2: str
    confidence: float
    topology_similarity: float
    relationship_type: str
    join_candidates: list[JoinCandidate]

    # NEW: Evaluation metrics (populated by evaluator.py)
    join_success_rate: float | None = None  # 0-100%
    introduces_duplicates: bool | None = None
```

**Simplifications applied:**
- Removed `null_rate_after_join` - redundant with `join_success_rate`
- Removed `redundancy_condition_index` and `redundant_column_pairs` - VDP deferred to quality module

### Pipeline Flow

**Before Semantic Agent:**
1. Relationship detection (TDA + joins) → `RelationshipCandidate` list
2. **NEW: Per-relationship evaluation** → Enriched candidates with quality metrics
3. Semantic agent receives: candidates + quality evidence
4. Semantic agent confirms/rejects relationships with evidence

**After Semantic Agent:**
1. Confirmed relationships stored in `Relationship` table
2. Quality agents receive: confirmed relationships with quality metrics
3. Quality rules can reference relationship quality (e.g., "fail if join_success_rate < 80%")

**Quality Gate for Multi-Table Aggregations:**

This evaluation is particularly valuable as a **quality gate before aggregations across multiple tables**. When downstream processes want to:
- Build a denormalized view joining multiple tables
- Compute aggregates that span table boundaries
- Create materialized summaries combining data sources

The per-relationship quality metrics answer: "Is this join safe to use for aggregation?"

| Metric | Aggregation Risk |
|--------|-----------------|
| Low join_success_rate | Missing data in aggregates |
| introduces_duplicates = True | Inflated counts/sums (fan trap) |
| Low referential_integrity | Orphan records skewing results |
| cardinality_verified = False | Wrong join multiplicity |

### Implementation Tasks

**Phase 1: Model Cleanup**
- [ ] Remove from `correlation/models.py`: CrossTableNumericCorrelation, CrossTableCategoricalAssociation, CrossTableDependencyGroup, CrossTableMulticollinearityAnalysis, EnrichedRelationship, SingleRelationshipJoin
- [ ] Remove from `relationships/db_models.py`: CrossTableMulticollinearityMetrics
- [ ] Update `relationships/models.py`: Extend JoinCandidate and RelationshipCandidate with evaluation fields
- [ ] Update imports in files that used removed models

**Phase 2: Evaluator Implementation**
- [ ] Create `analysis/relationships/evaluator.py`
- [ ] Implement `evaluate_join_candidate()` - referential integrity, cardinality verification
- [ ] Implement `evaluate_relationship_candidate()` - join success rate, duplicate detection
- [ ] Add integration point in detector.py to call evaluator after detection

**Phase 3: Integration**
- [ ] Update semantic agent prompt to use per-relationship evaluation metrics
- [ ] Repurpose `correlation/cross_table.py` for quality-only analysis (post-confirmation, VDP)
- [ ] Update any downstream consumers of removed models

**Deferred to Quality Module:**
- VDP-based redundancy analysis (stays in correlation/ for quality context)
- Cross-table correlation analysis (post-confirmation only)

### Example Output

```
RelationshipCandidate:
  table1: "master_txn_table"
  table2: "customer_master"
  confidence: 0.85
  topology_similarity: 0.72
  relationship_type: "foreign_key"

  # Evaluation metrics
  join_success_rate: 94.2%
  introduces_duplicates: False

  join_candidates:
    - column1: "business_id"
      column2: "Business Id"
      confidence: 0.92
      cardinality: "many-to-one"
      left_referential_integrity: 94.2%
      right_referential_integrity: 100%
      orphan_count: 58
      cardinality_verified: True
```

### References

- [Dataiku Data Quality Metrics](https://knowledge.dataiku.com/latest/automation/data-quality/tutorial-data-quality-sql-metrics.html)
- [Metaplane Data Quality Tests](https://www.metaplane.dev/blog/how-to-set-up-data-quality-tests)
- [Tableau Cardinality and Referential Integrity](https://help.tableau.com/current/pro/desktop/en-us/cardinality_and_ri.htm)
- [Database Design Integrity Constraints](https://opentextbc.ca/dbdesign01/chapter/chapter-9-integrity-rules-and-constraints/)

## Principles
- Keep it lean and focused
- Use existing infrastructure (relationships module for joins)
- Use pure algorithms consistently (algorithms/ folder)
- Exact function copies when possible, no unnecessary reinterpretation
- Rich context output for both semantic and quality agents
- **NEW: Verify correctness before declaring done**
- **NEW: Understand the math, don't just make tests pass**
