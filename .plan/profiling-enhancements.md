# Profiling and Enrichment Enhancements

**Status:** Planning  
**Date:** 2025-11-28  
**Goal:** Enhance statistical profiling, temporal analysis, and add cross-column metrics before implementing quality and context layers

## Overview

Before proceeding to the quality and context aggregation layers, we need to enrich the metadata extracted during profiling and enrichment. The current implementation provides basic statistics, but missing advanced metrics that would significantly improve:

1. LLM semantic analysis quality (more signals to reason about)
2. Quality rule generation (better understanding of data characteristics)
3. Context document richness (more valuable insights for AI consumers)

## Architecture Notes

### Topology vs Correlation
The existing TDA topology analysis already finds relationships between columns using numerical topology analysis. This is **exploratory** and finds **possible** relationships based on structural similarity, not actual correlation values.

The correlation metrics proposed here are **complementary**:
- **TDA topology**: Structural similarity across tables (FK candidates, join paths)
- **Correlation metrics**: Mathematical correlation within a single table (how columns relate numerically/categorically)

Both provide different lenses on relationships and should coexist.

---

## Option 1: Enhanced Statistical Profiling

**Goal:** Add advanced statistical metrics to column profiles  
**Module:** `src/dataraum_context/profiling/statistical.py`  
**Dependencies:** DuckDB, scipy (optional for advanced stats)

### 1.1 Numeric Column Enhancements

#### Histogram Generation (Currently TODO)
```python
# Implementation in statistical.py
async def _compute_histogram(
    table_name: str,
    col_name: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    buckets: int = 20,
) -> list[HistogramBucket]:
    """Generate histogram using DuckDB's equi-width binning."""
```

**Query approach:**
```sql
-- DuckDB has built-in histogram function
SELECT 
    histogram("{col_name}") as hist
FROM {table_name}
WHERE TRY_CAST("{col_name}" AS DOUBLE) IS NOT NULL
```

**Storage:** Already exists in `ColumnProfile.histogram` and `DBColumnProfile.histogram`

#### Distribution Shape Metrics
```python
class DistributionStats(BaseModel):
    """Statistical measures of distribution shape."""
    skewness: float  # Measure of asymmetry
    kurtosis: float  # Measure of tail heaviness
    is_normal: bool  # Simple normality indicator
    cv: float        # Coefficient of variation (stddev/mean)
```

**Implementation:**
```sql
-- Skewness and kurtosis in DuckDB
SELECT
    skewness(TRY_CAST("{col_name}" AS DOUBLE)) as skew,
    kurtosis(TRY_CAST("{col_name}" AS DOUBLE)) as kurt,
    (STDDEV(TRY_CAST("{col_name}" AS DOUBLE)) / 
     AVG(TRY_CAST("{col_name}" AS DOUBLE))) as cv
FROM {table_name}
WHERE TRY_CAST("{col_name}" AS DOUBLE) IS NOT NULL
```

**Data Model Changes:**
- Add to `NumericStats` model in `core/models.py`
- Add to `ColumnProfile` in `storage/models.py`

#### Outlier Detection
```python
class OutlierStats(BaseModel):
    """Outlier detection results."""
    method: str  # 'iqr' or 'zscore'
    outlier_count: int
    outlier_ratio: float
    lower_fence: float
    upper_fence: float
    outlier_examples: list[float]  # Sample of outliers
```

**IQR Method (Interquartile Range):**
```sql
WITH quartiles AS (
    SELECT
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY val) as q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY val) as q3
    FROM (SELECT TRY_CAST("{col_name}" AS DOUBLE) as val FROM {table_name}) t
    WHERE val IS NOT NULL
),
bounds AS (
    SELECT
        q1 - 1.5 * (q3 - q1) as lower_fence,
        q3 + 1.5 * (q3 - q1) as upper_fence
    FROM quartiles
)
SELECT
    COUNT(*) as outlier_count,
    MIN(val) as min_outlier,
    MAX(val) as max_outlier
FROM (SELECT TRY_CAST("{col_name}" AS DOUBLE) as val FROM {table_name}) t
CROSS JOIN bounds
WHERE val < lower_fence OR val > upper_fence
```

**Data Model Changes:**
- Add `OutlierStats` model to `core/models.py`
- Add `outlier_stats` field to `NumericStats`
- Add `outlier_detection` JSON field to `DBColumnProfile`

#### Value Domain Analysis
```python
class ValueDomainStats(BaseModel):
    """Analysis of value domain characteristics."""
    zero_count: int
    zero_ratio: float
    negative_count: int
    negative_ratio: float
    positive_count: int
    positive_ratio: float
```

**Implementation:**
```sql
SELECT
    COUNT(CASE WHEN val = 0 THEN 1 END) as zero_count,
    COUNT(CASE WHEN val < 0 THEN 1 END) as negative_count,
    COUNT(CASE WHEN val > 0 THEN 1 END) as positive_count,
    COUNT(*) as total_count
FROM (SELECT TRY_CAST("{col_name}" AS DOUBLE) as val FROM {table_name}) t
WHERE val IS NOT NULL
```

**Data Model Changes:**
- Add `ValueDomainStats` to `core/models.py`
- Add to `NumericStats` model

### 1.2 String Column Enhancements

#### Character Set Analysis
```python
class CharacterSetStats(BaseModel):
    """Character set and encoding analysis."""
    charset_type: str  # 'ascii', 'unicode', 'mixed'
    has_unicode: bool
    has_special_chars: bool
    has_whitespace: bool
    avg_unicode_ratio: float
```

**Implementation:**
```sql
SELECT
    AVG(CASE WHEN regexp_matches("{col_name}", '^[\x00-\x7F]*$') 
        THEN 0 ELSE 1 END) as unicode_ratio,
    AVG(CASE WHEN regexp_matches("{col_name}", '.*\s.*') 
        THEN 1 ELSE 0 END) as whitespace_ratio,
    AVG(CASE WHEN regexp_matches("{col_name}", '.*[^a-zA-Z0-9\s].*') 
        THEN 1 ELSE 0 END) as special_char_ratio
FROM {table_name}
WHERE "{col_name}" IS NOT NULL
```

#### Case Distribution Analysis
```python
class CaseStats(BaseModel):
    """Text case distribution."""
    uppercase_count: int
    lowercase_count: int
    mixedcase_count: int
    titlecase_count: int
    uppercase_ratio: float
```

**Implementation:**
```sql
SELECT
    COUNT(CASE WHEN "{col_name}" = UPPER("{col_name}") THEN 1 END) as upper_count,
    COUNT(CASE WHEN "{col_name}" = LOWER("{col_name}") THEN 1 END) as lower_count,
    -- Titlecase: First char upper, rest can be anything
    COUNT(CASE WHEN SUBSTR("{col_name}", 1, 1) = UPPER(SUBSTR("{col_name}", 1, 1)) 
              AND "{col_name}" != UPPER("{col_name}") THEN 1 END) as title_count
FROM {table_name}
WHERE "{col_name}" IS NOT NULL
```

**Data Model Changes:**
- Add `CharacterSetStats` and `CaseStats` to `core/models.py`
- Add to `StringStats` model

#### Whitespace Pattern Analysis
```python
class WhitespaceStats(BaseModel):
    """Whitespace pattern analysis."""
    has_leading_ws_count: int
    has_trailing_ws_count: int
    has_internal_ws_count: int
    leading_ws_ratio: float
    trailing_ws_ratio: float
```

**Implementation:**
```sql
SELECT
    COUNT(CASE WHEN "{col_name}" != LTRIM("{col_name}") THEN 1 END) as leading_ws,
    COUNT(CASE WHEN "{col_name}" != RTRIM("{col_name}") THEN 1 END) as trailing_ws,
    COUNT(CASE WHEN POSITION(' ' IN TRIM("{col_name}")) > 0 THEN 1 END) as internal_ws
FROM {table_name}
WHERE "{col_name}" IS NOT NULL
```

### 1.3 Universal Column Metrics

These apply to all column types (numeric, string, date, etc.)

#### Entropy (Information Content)
```python
class EntropyStats(BaseModel):
    """Information-theoretic metrics."""
    shannon_entropy: float  # Bits of information
    normalized_entropy: float  # 0-1, relative to max possible
    entropy_category: str  # 'low', 'medium', 'high'
```

**Implementation:**
```sql
-- Shannon entropy: H = -Σ(p * log2(p))
WITH value_counts AS (
    SELECT 
        "{col_name}" as val,
        COUNT(*) as cnt,
        COUNT(*) * 1.0 / SUM(COUNT(*)) OVER () as probability
    FROM {table_name}
    WHERE "{col_name}" IS NOT NULL
    GROUP BY "{col_name}"
)
SELECT
    -SUM(probability * LOG2(probability)) as shannon_entropy,
    -SUM(probability * LOG2(probability)) / LOG2(COUNT(*)) as normalized_entropy
FROM value_counts
```

**Interpretation:**
- Low entropy (< 1 bit): Very repetitive, few distinct values
- High entropy (> 5 bits): Rich information, many distinct values
- Normalized entropy near 1.0: Maximum information density

**Data Model Changes:**
- Add `EntropyStats` to `core/models.py`
- Add `entropy_stats` to `ColumnProfile`

#### Uniqueness Analysis
```python
class UniquenessStats(BaseModel):
    """Uniqueness and key-worthiness metrics."""
    uniqueness_ratio: float  # Same as cardinality_ratio
    is_unique: bool  # All values unique (potential PK)
    is_near_unique: bool  # > 99% unique
    duplicate_count: int  # Number of duplicated values
    most_duplicated_value: Any
    most_duplicated_count: int
```

**Implementation:**
```sql
WITH duplicates AS (
    SELECT 
        "{col_name}",
        COUNT(*) as cnt
    FROM {table_name}
    WHERE "{col_name}" IS NOT NULL
    GROUP BY "{col_name}"
    HAVING COUNT(*) > 1
)
SELECT
    COUNT(*) as duplicate_value_count,
    MAX(cnt) as max_duplicate_count,
    ARG_MAX("{col_name}", cnt) as most_duplicated_value
FROM duplicates
```

**Data Model Changes:**
- Add `UniquenessStats` to `core/models.py`
- Extend `ColumnProfile` with uniqueness metrics

#### Sortedness and Monotonicity
```python
class OrderStats(BaseModel):
    """Order and sequence characteristics."""
    is_sorted: bool
    is_monotonic_increasing: bool
    is_monotonic_decreasing: bool
    sort_direction: str | None  # 'asc', 'desc', 'unsorted'
    inversions_ratio: float  # Measure of how "unsorted" it is
```

**Implementation:**
```sql
-- Check if sorted by comparing with sorted version
WITH ordered AS (
    SELECT 
        "{col_name}" as original,
        ROW_NUMBER() OVER (ORDER BY "{col_name}") as sorted_pos,
        ROW_NUMBER() OVER () as original_pos
    FROM {table_name}
    WHERE "{col_name}" IS NOT NULL
)
SELECT
    SUM(CASE WHEN sorted_pos != original_pos THEN 1 ELSE 0 END) as inversions,
    COUNT(*) as total
FROM ordered
```

**For monotonicity (numeric/date columns only):**
```sql
WITH pairs AS (
    SELECT
        "{col_name}" as curr,
        LAG("{col_name}") OVER (ORDER BY rowid) as prev
    FROM {table_name}
    WHERE "{col_name}" IS NOT NULL
)
SELECT
    COUNT(CASE WHEN curr >= prev THEN 1 END) as increasing_pairs,
    COUNT(CASE WHEN curr <= prev THEN 1 END) as decreasing_pairs,
    COUNT(*) as total_pairs
FROM pairs
WHERE prev IS NOT NULL
```

**Data Model Changes:**
- Add `OrderStats` to `core/models.py`
- Add to `ColumnProfile`

### 1.4 Implementation Plan for Statistical Enhancements

**Files to Modify:**
1. `src/dataraum_context/core/models.py`
   - Add new stats models: `DistributionStats`, `OutlierStats`, `ValueDomainStats`, `CharacterSetStats`, `CaseStats`, `WhitespaceStats`, `EntropyStats`, `UniquenessStats`, `OrderStats`
   - Extend `NumericStats` with new fields
   - Extend `StringStats` with new fields
   - Extend `ColumnProfile` with new fields

2. `src/dataraum_context/storage/models.py`
   - Add JSON columns to `ColumnProfile` for new stats

3. `src/dataraum_context/profiling/statistical.py`
   - Refactor `_profile_column()` to compute new metrics
   - Add helper functions for each metric category
   - Keep queries efficient (minimize table scans)

**Testing Strategy:**
- Unit tests for each metric calculation
- Golden file tests with known datasets
- Performance tests (should handle 1M+ rows)

**Migration:**
- Add new columns to `metadata.column_profiles` table
- Nullable to support gradual rollout

---

## Option 2: Cross-Column Correlation Analysis

**Goal:** Detect relationships between columns within a single table  
**Module:** New module `src/dataraum_context/profiling/correlation.py`  
**Dependencies:** DuckDB, scipy (for advanced correlation methods)

### 2.1 Numeric Correlation

#### Pearson Correlation
```python
class NumericCorrelation(BaseModel):
    """Pearson correlation between two numeric columns."""
    column1_id: str
    column2_id: str
    column1_name: str
    column2_name: str
    correlation_coefficient: float  # -1 to 1
    p_value: float | None  # Statistical significance
    correlation_strength: str  # 'none', 'weak', 'moderate', 'strong', 'very_strong'
    sample_size: int
```

**Implementation:**
```sql
-- DuckDB has built-in correlation function
SELECT
    CORR(
        TRY_CAST("{col1}" AS DOUBLE),
        TRY_CAST("{col2}" AS DOUBLE)
    ) as correlation
FROM {table_name}
WHERE 
    TRY_CAST("{col1}" AS DOUBLE) IS NOT NULL AND
    TRY_CAST("{col2}" AS DOUBLE) IS NOT NULL
```

**Interpretation:**
- |r| > 0.9: Very strong correlation
- |r| > 0.7: Strong correlation
- |r| > 0.5: Moderate correlation
- |r| > 0.3: Weak correlation
- |r| ≤ 0.3: Negligible correlation

#### Spearman Rank Correlation
For non-linear monotonic relationships:

```python
class SpearmanCorrelation(BaseModel):
    """Spearman rank correlation (handles non-linear)."""
    column1_id: str
    column2_id: str
    spearman_rho: float
    is_monotonic: bool  # rho > 0.8
```

**Implementation approach:**
- Rank values in each column
- Compute Pearson correlation on ranks
- Can be done in DuckDB with window functions

### 2.2 Categorical Association

#### Cramér's V (Chi-Square Based)
```python
class CategoricalAssociation(BaseModel):
    """Association between categorical columns."""
    column1_id: str
    column2_id: str
    cramers_v: float  # 0 to 1
    chi_square: float
    dof: int  # Degrees of freedom
    association_strength: str  # 'none', 'weak', 'moderate', 'strong'
```

**Implementation:**
```sql
-- Build contingency table
WITH contingency AS (
    SELECT
        "{col1}" as val1,
        "{col2}" as val2,
        COUNT(*) as observed
    FROM {table_name}
    WHERE "{col1}" IS NOT NULL AND "{col2}" IS NOT NULL
    GROUP BY "{col1}", "{col2}"
),
marginals AS (
    SELECT
        val1,
        val2,
        observed,
        SUM(observed) OVER (PARTITION BY val1) as row_total,
        SUM(observed) OVER (PARTITION BY val2) as col_total,
        SUM(observed) OVER () as grand_total
    FROM contingency
),
expected AS (
    SELECT
        val1,
        val2,
        observed,
        (row_total * col_total * 1.0 / grand_total) as expected,
        grand_total
    FROM marginals
)
SELECT
    SUM(POWER(observed - expected, 2) / expected) as chi_square,
    COUNT(DISTINCT val1) - 1 as rows_minus_1,
    COUNT(DISTINCT val2) - 1 as cols_minus_1,
    grand_total
FROM expected
```

Then compute Cramér's V:
```python
V = sqrt(chi_square / (n * min(rows-1, cols-1)))
```

**Interpretation:**
- V > 0.5: Strong association
- V > 0.3: Moderate association
- V > 0.1: Weak association
- V ≤ 0.1: Negligible association

### 2.3 Functional Dependencies

#### Exact Functional Dependencies
```python
class FunctionalDependency(BaseModel):
    """A → B (A determines B)."""
    determinant_columns: list[str]  # A
    dependent_column: str  # B
    confidence: float  # 1.0 for exact, < 1.0 for approximate
    exception_count: int
    example: dict[str, Any] | None  # Example of A → B
```

**Implementation:**
```sql
-- Check if col1 → col2 (each value of col1 maps to exactly one value of col2)
WITH mappings AS (
    SELECT
        "{col1}",
        COUNT(DISTINCT "{col2}") as distinct_col2_values
    FROM {table_name}
    WHERE "{col1}" IS NOT NULL AND "{col2}" IS NOT NULL
    GROUP BY "{col1}"
)
SELECT
    COUNT(CASE WHEN distinct_col2_values = 1 THEN 1 END) as valid_mappings,
    COUNT(CASE WHEN distinct_col2_values > 1 THEN 1 END) as violations,
    COUNT(*) as total_unique_col1_values
FROM mappings
```

**Confidence:**
```python
confidence = valid_mappings / total_unique_col1_values
```

If confidence = 1.0, it's an exact FD. If > 0.95, it's an approximate FD.

#### Multi-Column Functional Dependencies
Check if (col1, col2) → col3:

```sql
WITH combos AS (
    SELECT
        "{col1}",
        "{col2}",
        COUNT(DISTINCT "{col3}") as distinct_values
    FROM {table_name}
    WHERE "{col1}" IS NOT NULL AND "{col2}" IS NOT NULL AND "{col3}" IS NOT NULL
    GROUP BY "{col1}", "{col2}"
)
SELECT
    COUNT(CASE WHEN distinct_values = 1 THEN 1 END) as valid,
    COUNT(*) as total
FROM combos
```

### 2.4 Derived Column Detection

#### Simple Arithmetic Derivations
```python
class DerivedColumnCandidate(BaseModel):
    """Potential derived column."""
    derived_column: str
    source_columns: list[str]
    derivation_type: str  # 'sum', 'difference', 'product', 'ratio', 'concat'
    formula: str  # "col_a + col_b"
    match_rate: float  # How often the formula holds
```

**Implementation:**
Check if col3 = col1 + col2:
```sql
SELECT
    COUNT(CASE WHEN ABS(col3 - (col1 + col2)) < 0.01 THEN 1 END) as matches,
    COUNT(*) as total
FROM (
    SELECT
        TRY_CAST("{col1}" AS DOUBLE) as col1,
        TRY_CAST("{col2}" AS DOUBLE) as col2,
        TRY_CAST("{col3}" AS DOUBLE) as col3
    FROM {table_name}
) t
WHERE col1 IS NOT NULL AND col2 IS NOT NULL AND col3 IS NOT NULL
```

Try: sum, difference, product, ratio, min, max, avg

#### String Derivations
Check transformations:
- col2 = UPPER(col1)
- col2 = SUBSTR(col1, 1, 10)
- col2 = CONCAT(col1, '_suffix')

```sql
SELECT
    COUNT(CASE WHEN "{col2}" = UPPER("{col1}") THEN 1 END) as uppercase_matches,
    COUNT(CASE WHEN "{col2}" = LOWER("{col1}") THEN 1 END) as lowercase_matches,
    COUNT(*) as total
FROM {table_name}
WHERE "{col1}" IS NOT NULL AND "{col2}" IS NOT NULL
```

### 2.5 Data Model for Correlations

**Storage:**
```python
# New table in storage/models.py
class ColumnCorrelation(Base):
    __tablename__ = "column_correlations"
    
    correlation_id = Column(String, primary_key=True)
    table_id = Column(String, ForeignKey("tables.table_id"))
    
    # Columns involved
    column1_id = Column(String, ForeignKey("columns.column_id"))
    column2_id = Column(String, ForeignKey("columns.column_id"))
    
    # Correlation type and value
    correlation_type = Column(String)  # 'pearson', 'spearman', 'cramers_v', 'functional'
    correlation_value = Column(Float)
    correlation_strength = Column(String)
    
    # Additional metadata
    sample_size = Column(Integer)
    confidence = Column(Float)
    evidence = Column(JSON)
    
    computed_at = Column(DateTime)
```

**Core models:**
```python
# In core/models.py
class CorrelationResult(BaseModel):
    """Result of correlation analysis."""
    table_id: str
    numeric_correlations: list[NumericCorrelation]
    categorical_associations: list[CategoricalAssociation]
    functional_dependencies: list[FunctionalDependency]
    derived_candidates: list[DerivedColumnCandidate]
```

### 2.6 Implementation Plan for Correlation Analysis

**New Files:**
1. `src/dataraum_context/profiling/correlation.py`
   - `compute_correlations(table_id, session, duckdb_conn) -> Result[CorrelationResult]`
   - `_compute_numeric_correlations()`
   - `_compute_categorical_associations()`
   - `_detect_functional_dependencies()`
   - `_detect_derived_columns()`

**Modified Files:**
1. `src/dataraum_context/storage/models.py`
   - Add `ColumnCorrelation` table

2. `src/dataraum_context/core/models.py`
   - Add correlation models

3. `src/dataraum_context/profiling/profiler.py`
   - Add correlation step to profiling workflow

**Testing:**
- Test with known correlated datasets
- Test with categorical data
- Test functional dependency detection
- Performance test (O(n²) column pairs)

**Optimization:**
- Only compute correlations for column pairs that make sense
- Skip categorical columns with > 100 distinct values for chi-square
- Use sampling for large tables (> 1M rows)

---

## Option 3: Topological Quality Metrics

**Goal:** Assess quality of detected relationships  
**Module:** `src/dataraum_context/enrichment/topology.py`  
**Dependencies:** Existing TDA implementation

### 3.1 Relationship Quality Metrics

**Note:** User will provide specific ideas for topological quality metrics. Placeholder structure below.

```python
class RelationshipQuality(BaseModel):
    """Quality assessment of a detected relationship."""
    relationship_id: str
    
    # Value overlap metrics
    value_overlap_percentage: float
    orphan_count: int
    orphan_ratio: float
    
    # Referential integrity
    referential_integrity_score: float  # 0-1
    
    # Cardinality quality
    actual_cardinality: str
    cardinality_skew: float  # How skewed is the 1:N relationship?
    
    # Confidence metrics
    tda_confidence: float
    overall_confidence: float
```

### 3.2 Implementation Placeholder

**Files to Create/Modify:**
1. `src/dataraum_context/enrichment/topology.py`
   - Add `_assess_relationship_quality()` function
   - Compute value overlap analysis
   - Detect orphans and referential integrity issues

2. `src/dataraum_context/storage/models.py`
   - Add quality fields to `Relationship` table

**Note:** Detailed implementation will be defined once user provides specific topological quality metrics.

---

## Implementation Order

### Phase 1: Statistical Enhancements (Option 1)
**Priority:** HIGH  
**Estimated Effort:** 3-4 days

1. **Day 1:** Data model updates
   - Add new stats models to `core/models.py`
   - Update `storage/models.py` with new fields
   - Create migration for new columns

2. **Day 2:** Histogram and distribution metrics
   - Implement histogram generation
   - Add skewness, kurtosis, CV
   - Add outlier detection (IQR method)

3. **Day 3:** String and universal metrics
   - Character set and case analysis
   - Entropy calculation
   - Uniqueness and sortedness metrics

4. **Day 4:** Integration and testing
   - Update `_profile_column()` in `statistical.py`
   - Write comprehensive tests
   - Test with real datasets

### Phase 2: Correlation Analysis (Option 2)
**Priority:** HIGH  
**Estimated Effort:** 3-4 days

1. **Day 1:** Data model and infrastructure
   - Create `ColumnCorrelation` table
   - Add correlation models to `core/models.py`
   - Create `profiling/correlation.py` module

2. **Day 2:** Numeric correlation
   - Implement Pearson correlation
   - Implement Spearman correlation
   - Add correlation strength classification

3. **Day 3:** Categorical and functional dependencies
   - Implement Cramér's V
   - Detect exact and approximate FDs
   - Handle multi-column FDs

4. **Day 4:** Derived column detection and testing
   - Arithmetic derivation detection
   - String transformation detection
   - Integration tests and optimization

### Phase 3: Topological Quality Metrics (Option 3)
**Priority:** MEDIUM  
**Estimated Effort:** 2-3 days (pending user input)

1. **Day 1:** Design based on user requirements
   - Review user-provided topological quality ideas
   - Define metrics and implementation approach

2. **Day 2-3:** Implementation
   - Add relationship quality assessment
   - Integrate with existing TDA workflow
   - Testing

### Phase 4: Integration
**Priority:** HIGH  
**Estimated Effort:** 1-2 days

1. Update profiler orchestrator to run all analyses
2. Ensure efficient execution (minimize redundant table scans)
3. Add configuration for enabling/disabling metric groups
4. End-to-end testing

---

## Configuration

Add to `config/profiling.yaml`:
```yaml
statistical_profiling:
  compute_histogram: true
  histogram_buckets: 20
  
  compute_distribution_stats: true
  compute_outliers: true
  outlier_method: 'iqr'  # 'iqr' or 'zscore'
  
  compute_entropy: true
  compute_sortedness: true
  
correlation_analysis:
  enabled: true
  
  numeric_correlation:
    min_correlation: 0.3  # Only store if |r| > 0.3
    methods: ['pearson', 'spearman']
  
  categorical_association:
    max_distinct_values: 100  # Skip if cardinality too high
    min_cramers_v: 0.1
  
  functional_dependencies:
    min_confidence: 0.95
    check_multi_column: true
    max_columns_in_determinant: 3
  
  derived_columns:
    min_match_rate: 0.95
    check_arithmetic: true
    check_string_transforms: true

topology_quality:
  enabled: true
  compute_value_overlap: true
  detect_orphans: true
```

---

## Success Criteria

### Option 1: Statistical Enhancements
- ✅ Histograms generated for all numeric columns
- ✅ Entropy calculated for all columns
- ✅ Outliers detected with configurable methods
- ✅ Distribution shape metrics (skewness, kurtosis) computed
- ✅ String analysis includes character set and case distribution
- ✅ All tests pass
- ✅ Profile computation time remains reasonable (< 30s for 1M rows)

### Option 2: Correlation Analysis
- ✅ Pearson correlation computed for all numeric column pairs
- ✅ Cramér's V computed for categorical column pairs
- ✅ Functional dependencies detected with > 95% confidence
- ✅ Derived columns identified with examples
- ✅ Performance acceptable for tables with 50+ columns
- ✅ All tests pass

### Option 3: Topological Quality
- ✅ Relationship quality scores computed
- ✅ Orphan detection working
- ✅ Metrics align with user requirements
- ✅ All tests pass

---

## Benefits for Downstream Layers

### For LLM Semantic Analysis
With enhanced statistics, the LLM can:
- Identify key columns by looking at uniqueness and entropy
- Detect measure vs dimension by distribution shape
- Understand data quality from outlier rates
- Infer relationships from correlation patterns
- Recognize derived columns and hierarchies from FDs

### For Quality Layer
Enhanced metrics enable:
- Automatic generation of outlier detection rules
- Entropy-based completeness rules
- Correlation-based consistency rules
- Functional dependency validation rules
- Domain-specific rules based on distribution shape

### For Context Documents
Richer context includes:
- "This column has high entropy and unique values - likely a primary key"
- "Revenue and Cost are strongly correlated (r=0.95)"
- "Date column is monotonically increasing with no gaps"
- "Customer_Name appears to be UPPER(customer_name_raw)"
- "Order_Total = Sum(Line_Items.Amount) with 99.8% confidence"

---

## Notes

- **Performance:** All new metrics should use efficient SQL queries in DuckDB
- **Sampling:** For very large tables (> 10M rows), use sampling for expensive metrics
- **Configurability:** Users should be able to disable expensive metrics
- **Incremental:** Can implement metrics incrementally, don't need all at once
- **Testing:** Each metric needs unit tests with known expected values
