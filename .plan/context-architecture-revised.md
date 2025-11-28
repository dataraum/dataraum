# Context Architecture - Revised Plan

**Status:** Planning  
**Date:** 2025-11-28  
**Based on:** Prototype sketches in `prototypes/generated/`

## Executive Summary

After reviewing the prototype sketches, we need to **restructure our approach**. The prototypes reveal that the context engine should have **5 distinct analytical pillars** that feed into context generation:

1. **Statistical Context** - Profiles, distributions, correlations
2. **Topological Context** - TDA features, cycles, structural complexity
3. **Semantic Context** - Entity types, relationships, business rules
4. **Temporal Context** - Time patterns, gaps, seasonality, trends
5. **Quality Context** - **Synthesized from the other 4 using domain-specific rules**

**Critical insight:** Quality is not just "data validation"—it includes domain-specific quality laws like:
- Financial: Benford's Law, double-entry balance, trial balance
- Topological: Homological stability, cycle persistence
- Temporal: Distribution stability, trend break detection
- Statistical: VIF, outlier scoring, distribution tests

---

## Current State vs. Prototype Vision

### What We Have ✅

| Component | Status | Notes |
|-----------|--------|-------|
| Basic statistical profiling | ✅ Implemented | Counts, nulls, distributions, top-k values |
| Pattern detection | ✅ Implemented | Regex-based, Pint unit detection |
| Type inference | ✅ Implemented | Multi-strategy with confidence scoring |
| Semantic enrichment (LLM) | ✅ Implemented | Entity detection, relationships |
| TDA topology extraction | ✅ Implemented | Relationship finding via TDA |
| Temporal analysis | ✅ Implemented | Granularity, gaps, completeness |

### What's Missing ❌

| Component | Status | Priority |
|-----------|--------|----------|
| **Statistical quality metrics** | ❌ Missing | HIGH |
| - Benford's Law compliance | ❌ | Financial fraud detection |
| - Distribution stability (KS test) | ❌ | Change detection |
| - VIF (multicollinearity) | ❌ | Correlation quality |
| - Isolation Forest (anomalies) | ❌ | Outlier scoring |
| **Topological quality metrics** | ❌ Missing | HIGH |
| - Cycle persistence tracking | ❌ | Flow pattern stability |
| - Homological stability | ❌ | Structural consistency |
| - Component count validation | ❌ | Graph integrity |
| - Structural complexity bounds | ❌ | Anomaly detection |
| **Enhanced temporal metrics** | ❌ Missing | MEDIUM |
| - Seasonality strength | ❌ | Pattern quantification |
| - Trend break detection | ❌ | Change point analysis |
| - Update frequency scoring | ❌ | Pipeline health |
| **Domain-specific quality** | ❌ Missing | HIGH |
| - Financial rules (double-entry) | ❌ | Accounting validation |
| - Trial balance checks | ❌ | Financial integrity |
| - Sign convention validation | ❌ | Domain semantics |

---

## Revised Architecture: The Five Context Pillars

### Pillar 1: Statistical Context

**Purpose:** Comprehensive statistical characterization of data

**Current Implementation:** `profiling/statistical.py`

**Enhancements Needed:**

#### 1.1 Enhanced Profiling
- ✅ Basic stats (mean, median, std)
- ✅ Percentiles
- ❌ **Skewness and kurtosis** (distribution shape)
- ❌ **Coefficient of variation** (CV)
- ❌ **Histograms** (currently TODO)
- ❌ **Entropy** (information content)

#### 1.2 Correlation Analysis
- ❌ **Pearson correlation** (linear relationships)
- ❌ **Spearman correlation** (monotonic relationships)
- ❌ **Mutual information** (non-linear dependencies)
- ❌ **Cramér's V** (categorical associations)

Implementation note: Use DuckDB's `CORR()` function for efficiency.

#### 1.3 Outlier Detection
- ❌ **IQR method** (interquartile range)
- ❌ **Z-score method**
- ❌ **Isolation Forest** (ML-based anomaly scoring)

#### 1.4 Distribution Testing
- ❌ **Normality tests** (Shapiro-Wilk, Anderson-Darling)
- ❌ **Benford's Law** (first-digit distribution for fraud detection)
- ❌ **KS test** (distribution stability across periods)

**New Module:** `profiling/statistical_quality.py`

### Pillar 2: Topological Context

**Purpose:** Extract structural features using TDA

**Current Implementation:** `enrichment/topology.py` (basic TDA)

**Enhancements Needed:**

#### 2.1 Persistence Features
- ✅ Basic persistence computation (via TDA prototype)
- ❌ **Persistence diagram storage** (birth/death pairs)
- ❌ **Betti numbers** (H₀, H₁, H₂ counts)
- ❌ **Persistent entropy** (complexity measure)

#### 2.2 Cycle Analysis
- ✅ Basic relationship detection
- ❌ **Money flow cycles** (financial domain)
- ❌ **Cycle lifetimes** (persistence)
- ❌ **Cycle significance** (filter noise)

#### 2.3 Graph Metrics
- ❌ **Component count** (H₀ Betti number)
- ❌ **Centrality measures** (which tables/columns are central?)
- ❌ **Clustering coefficient**
- ❌ **Graph density**

#### 2.4 Topological Quality (NEW)
This is the **key missing piece** from the prototypes:

```python
class TopologicalQualityMetrics(BaseModel):
    """Quality metrics derived from topology."""
    
    # Stability metrics
    homological_stability: float  # Bottleneck distance between periods
    cycle_persistence_score: float  # How long do cycles last?
    component_stability: bool  # Expected component count?
    
    # Complexity metrics
    structural_complexity: int  # Sum of Betti numbers
    complexity_trend: str  # 'increasing', 'stable', 'decreasing'
    complexity_within_bounds: bool  # Historical norms
    
    # Anomaly detection
    anomalous_cycles: list[dict]  # Unexpected flow patterns
    orphaned_components: int  # Disconnected subgraphs
    
    # Context for LLM
    topology_description: str  # "3 connected components, 12 significant cycles"
    quality_warnings: list[str]  # "Unusual cycle detected in accounts payable"
```

**Implementation:**
- Use `gudhi.bottleneck_distance()` for stability
- Track Betti numbers over time
- Compare current topology to historical baselines

**New Module:** `enrichment/topological_quality.py`

### Pillar 3: Semantic Context

**Purpose:** Business meaning and domain mapping

**Current Implementation:** `enrichment/semantic.py` (LLM-based)

**Enhancements Needed:**

#### 3.1 Domain Mapping
- ✅ Entity type detection
- ❌ **Standard framework mapping** (IFRS, GAAP, CoA)
- ❌ **Business term dictionary**
- ❌ **Ontology alignment scoring**

#### 3.2 Relationship Extraction
- ✅ Basic relationship detection
- ❌ **Hierarchical relationships** (account trees)
- ❌ **Business rule inference** (constraints)
- ❌ **Functional dependency documentation**

#### 3.3 Semantic Quality
- ❌ **Schema completeness** (missing expected entities?)
- ❌ **Naming consistency** (similar columns, different names)
- ❌ **Business rule violations** (domain constraints)

**Note:** Most of this is already handled by LLM semantic analysis. Focus on validation and scoring.

### Pillar 4: Temporal Context

**Purpose:** Time-based patterns and quality

**Current Implementation:** `enrichment/temporal.py`

**Enhancements Needed:**

#### 4.1 Enhanced Pattern Detection
- ✅ Granularity detection
- ✅ Gap detection
- ❌ **Seasonality strength** (quantified, not just boolean)
- ❌ **Trend strength** (quantified slope)
- ❌ **Autocorrelation** (lag analysis)

Implementation using statsmodels:
```python
from statsmodels.tsa.seasonal import seasonal_decompose

decomposition = seasonal_decompose(time_series, model='additive')
seasonality_strength = 1 - (decomposition.resid.var() / time_series.var())
```

#### 4.2 Change Point Detection
- ❌ **Trend breaks** (CUSUM, Chow test)
- ❌ **Distribution shifts** (KS test across periods)
- ❌ **Velocity changes** (transaction frequency shifts)

Library: `ruptures` for change point detection

#### 4.3 Fiscal Calendar
- ❌ **Fiscal period alignment** (Q1-Q4, FY boundaries)
- ❌ **Business day analysis** (weekend/holiday patterns)
- ❌ **Period-end effects** (spikes near month/quarter end)

#### 4.4 Temporal Quality
- ✅ Completeness ratio
- ❌ **Update frequency score** (regularity of updates)
- ❌ **Data freshness** (time since last update)
- ❌ **Temporal consistency** (no time-traveling records)

**New Module:** `enrichment/temporal_quality.py`

### Pillar 5: Quality Context (SYNTHESIZED)

**Purpose:** Aggregate quality metrics from all other pillars + domain-specific rules

**Current Implementation:** ❌ Not yet implemented

**Architecture:**

```python
class QualityContext(BaseModel):
    """Comprehensive quality assessment."""
    
    # Dimension scores (standard DQ framework)
    completeness_score: float  # From statistical + temporal
    consistency_score: float  # From semantic + topological
    accuracy_score: float  # From domain-specific rules
    timeliness_score: float  # From temporal
    uniqueness_score: float  # From statistical
    
    overall_score: float  # Weighted average
    
    # Statistical quality
    statistical_quality: StatisticalQualityMetrics
    
    # Topological quality
    topological_quality: TopologicalQualityMetrics
    
    # Temporal quality
    temporal_quality: TemporalQualityMetrics
    
    # Domain-specific quality
    domain_quality: DomainQualityMetrics  # e.g., FinancialQualityMetrics
    
    # Issues and warnings
    critical_issues: list[QualityIssue]
    warnings: list[str]
    recommendations: list[str]
```

#### 5.1 Statistical Quality Metrics

```python
class StatisticalQualityMetrics(BaseModel):
    """Quality derived from statistical analysis."""
    
    # Distribution quality
    benford_compliance: BenfordTestResult | None  # For financial amounts
    distribution_stability: KSTestResult | None  # Across time periods
    outlier_score: float  # Isolation Forest anomaly score
    
    # Correlation quality
    vif_scores: dict[str, float]  # Multicollinearity detection
    correlation_stability: bool  # Correlations stable over time?
    
    # Type quality
    type_consistency_score: float  # Parse success rates
    pattern_match_quality: float  # Pattern detection confidence
```

Implementation:
```python
# Benford's Law (first digit distribution)
def check_benford_law(amounts: list[float]) -> BenfordTestResult:
    first_digits = [int(str(abs(x))[0]) for x in amounts if x != 0]
    observed = np.bincount(first_digits)[1:] / len(first_digits)
    expected = np.log10(1 + 1/np.arange(1, 10))  # Benford distribution
    
    chi2, p_value = scipy.stats.chisquare(observed, expected)
    
    return BenfordTestResult(
        chi_square=chi2,
        p_value=p_value,
        compliant=p_value > 0.05,
        interpretation="Follows Benford's Law" if p_value > 0.05 else "Anomalous distribution"
    )
```

#### 5.2 Domain-Specific Quality

Example for **Financial Domain**:

```python
class FinancialQualityMetrics(BaseModel):
    """Financial accounting-specific quality rules."""
    
    # Double-entry accounting
    double_entry_balanced: bool
    balance_difference: float
    balance_tolerance: float  # e.g., 0.01 for rounding
    
    # Trial balance
    trial_balance_check: bool
    accounting_equation_holds: bool  # Assets = Liabilities + Equity
    
    # Sign conventions
    sign_convention_compliance: float  # % of accounts with correct signs
    sign_violations: list[dict]  # Examples of violations
    
    # Consolidation (if multi-entity)
    intercompany_elimination_rate: float  # Should be 1.0
    orphaned_intercompany: int  # Unmatched IC transactions
    
    # Period integrity
    fiscal_period_complete: bool
    period_end_cutoff_clean: bool
```

Implementation:
```python
async def check_double_entry_balance(
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_name: str,
) -> FinancialQualityMetrics:
    """Check if debits = credits."""
    
    result = duckdb_conn.execute(f"""
        SELECT
            SUM(CASE WHEN account_type IN ('Asset', 'Expense') THEN amount ELSE 0 END) as debits,
            SUM(CASE WHEN account_type IN ('Liability', 'Equity', 'Revenue') THEN amount ELSE 0 END) as credits
        FROM {table_name}
    """).fetchone()
    
    debits, credits = result
    difference = abs(debits - credits)
    
    return {
        'double_entry_balanced': difference < 0.01,
        'balance_difference': difference,
        'balance_tolerance': 0.01
    }
```

#### 5.3 Quality Rule Generation (LLM)

The LLM should generate domain-specific quality rules based on:
- Detected entity types (e.g., "This is a financial ledger")
- Applied ontology (e.g., "financial_reporting")
- Statistical patterns (e.g., "Revenue is always positive")
- Topological patterns (e.g., "Each order must link to a customer")

```python
# LLM prompt for quality rule generation
async def llm_generate_quality_rules(
    semantic_context: SemanticEnrichmentResult,
    statistical_context: StatisticalContext,
    topological_context: TopologyEnrichmentResult,
    ontology: str,
) -> list[QualityRule]:
    """
    Generate quality rules using LLM with full context.
    
    The LLM receives:
    - Entity types and relationships (semantic)
    - Distribution characteristics (statistical)
    - Structural patterns (topological)
    - Domain ontology constraints
    
    Returns domain-appropriate quality rules.
    """
```

---

## Implementation Roadmap

### Phase 1: Statistical Quality Foundation (Week 1)

**Goal:** Add missing statistical quality metrics

**Tasks:**
1. Implement Benford's Law testing
2. Add distribution stability checks (KS test)
3. Implement VIF calculation for multicollinearity
4. Add Isolation Forest for anomaly scoring
5. Store results in `statistical_quality_metrics` table

**Deliverables:**
- `profiling/statistical_quality.py`
- Updated data models
- Tests with financial datasets

### Phase 2: Topological Quality Metrics (Week 2)

**Goal:** Extract quality signals from topology

**Tasks:**
1. Implement Betti number extraction and storage
2. Add homological stability tracking (bottleneck distance)
3. Implement cycle persistence analysis
4. Add structural complexity bounds checking
5. Create topological anomaly detection

**Deliverables:**
- `enrichment/topological_quality.py`
- Updated TDA pipeline
- Persistence diagram storage

### Phase 3: Enhanced Temporal Analysis (Week 2-3)

**Goal:** Quantified temporal patterns and quality

**Tasks:**
1. Add seasonality strength calculation
2. Implement trend break detection (change points)
3. Add update frequency scoring
4. Implement fiscal calendar alignment
5. Distribution stability across time periods

**Deliverables:**
- `enrichment/temporal_quality.py`
- Seasonal decomposition results
- Change point detection

### Phase 4: Domain-Specific Quality (Week 3-4)

**Goal:** Financial domain quality rules

**Tasks:**
1. Implement double-entry balance checks
2. Add trial balance validation
3. Create sign convention validation
4. Add intercompany elimination checks
5. Fiscal period integrity validation

**Deliverables:**
- `quality/domains/financial.py`
- Domain rule framework
- Pluggable domain modules

### Phase 5: Quality Context Synthesis (Week 4)

**Goal:** Aggregate all quality signals

**Tasks:**
1. Create unified `QualityContext` model
2. Implement dimension scoring (completeness, consistency, etc.)
3. Add quality issue aggregation
4. Create quality summary for LLM
5. Update LLM quality rule generation with full context

**Deliverables:**
- `quality/synthesis.py`
- Complete quality context in context document
- LLM-generated rules using all pillars

### Phase 6: Context Assembly Refactor (Week 5)

**Goal:** Clean separation of 5 context pillars

**Tasks:**
1. Refactor context assembly to use pillar structure
2. Update `ContextDocument` model
3. Ensure each pillar is independently queryable
4. Add pillar-specific LLM features
5. Update MCP tools to expose pillar contexts

**Deliverables:**
- `context/assembly_v2.py`
- Updated `ContextDocument` with 5 pillars
- Enhanced MCP tool definitions

---

## Data Model Changes

### New Tables

```sql
-- Statistical quality results
CREATE TABLE metadata.statistical_quality_metrics (
    metric_id UUID PRIMARY KEY,
    column_id UUID REFERENCES columns(column_id),
    computed_at TIMESTAMPTZ,
    
    -- Benford's Law (for numeric columns)
    benford_chi_square DOUBLE PRECISION,
    benford_p_value DOUBLE PRECISION,
    benford_compliant BOOLEAN,
    
    -- Distribution stability
    ks_statistic DOUBLE PRECISION,
    ks_p_value DOUBLE PRECISION,
    distribution_stable BOOLEAN,
    
    -- Anomaly detection
    isolation_forest_score DOUBLE PRECISION,
    is_anomalous BOOLEAN,
    
    -- Multicollinearity
    vif_score DOUBLE PRECISION
);

-- Topological quality results
CREATE TABLE metadata.topological_quality_metrics (
    metric_id UUID PRIMARY KEY,
    table_id UUID REFERENCES tables(table_id),
    computed_at TIMESTAMPTZ,
    
    -- Betti numbers
    betti_0 INTEGER,  -- Connected components
    betti_1 INTEGER,  -- Cycles
    betti_2 INTEGER,  -- Voids
    
    -- Persistence
    persistence_diagrams JSONB,  -- Birth/death pairs
    persistent_entropy DOUBLE PRECISION,
    
    -- Stability
    bottleneck_distance DOUBLE PRECISION,  -- vs previous period
    homologically_stable BOOLEAN,
    
    -- Complexity
    structural_complexity INTEGER,  -- Sum of Betti numbers
    complexity_within_bounds BOOLEAN,
    
    -- Anomalies
    anomalous_cycles JSONB,
    orphaned_components INTEGER
);

-- Domain-specific quality (extensible)
CREATE TABLE metadata.domain_quality_metrics (
    metric_id UUID PRIMARY KEY,
    table_id UUID REFERENCES tables(table_id),
    domain VARCHAR,  -- 'financial', 'marketing', etc.
    computed_at TIMESTAMPTZ,
    
    -- Generic storage for domain-specific metrics
    metrics JSONB,
    
    -- Overall domain compliance
    domain_compliance_score DOUBLE PRECISION,
    violations JSONB
);

-- Quality context (aggregated)
CREATE TABLE metadata.quality_contexts (
    context_id UUID PRIMARY KEY,
    source_id UUID REFERENCES sources(source_id),
    computed_at TIMESTAMPTZ,
    
    -- DQ dimensions
    completeness_score DOUBLE PRECISION,
    consistency_score DOUBLE PRECISION,
    accuracy_score DOUBLE PRECISION,
    timeliness_score DOUBLE PRECISION,
    uniqueness_score DOUBLE PRECISION,
    
    overall_score DOUBLE PRECISION,
    
    -- Issues
    critical_issues JSONB,
    warnings JSONB,
    recommendations JSONB
);
```

---

## Configuration Updates

```yaml
# config/quality.yaml

statistical_quality:
  benford_law:
    enabled: true
    apply_to: ["amount", "revenue", "balance"]  # Column patterns
    significance_level: 0.05
  
  distribution_stability:
    enabled: true
    window_size: "30 days"
    significance_level: 0.01
  
  anomaly_detection:
    enabled: true
    contamination: 0.05  # Expected % of anomalies
  
  multicollinearity:
    enabled: true
    vif_threshold: 10

topological_quality:
  enabled: true
  
  stability_tracking:
    enabled: true
    bottleneck_threshold: 0.2
  
  complexity_bounds:
    enabled: true
    historical_window: "90 days"
    sigma_threshold: 2  # Standard deviations
  
  cycle_detection:
    enabled: true
    min_persistence: 0.1  # Filter noise

temporal_quality:
  seasonality:
    enabled: true
    model: "additive"  # or "multiplicative"
  
  change_points:
    enabled: true
    method: "pelt"  # or "binseg", "bottomup"
    penalty: 10
  
  freshness:
    enabled: true
    sla_hours: 24

domain_quality:
  financial:
    enabled: true
    
    double_entry:
      tolerance: 0.01
    
    trial_balance:
      check_accounting_equation: true
    
    sign_conventions:
      asset: "debit"
      liability: "credit"
      equity: "credit"
      revenue: "credit"
      expense: "debit"
```

---

## Success Metrics

### Phase 1 Success Criteria
- ✅ Benford's Law detects anomalies in test financial dataset
- ✅ KS test identifies distribution shifts across periods
- ✅ VIF correctly identifies correlated columns
- ✅ Isolation Forest scores match manual anomaly labels

### Phase 2 Success Criteria
- ✅ Betti numbers extracted and stored
- ✅ Bottleneck distance computed between periods
- ✅ Topological anomalies flagged correctly
- ✅ Cycle persistence tracked over time

### Phase 3 Success Criteria
- ✅ Seasonality strength quantified (0-1 scale)
- ✅ Trend breaks detected at known change points
- ✅ Update frequency scored accurately
- ✅ Fiscal calendar alignment validated

### Phase 4 Success Criteria
- ✅ Double-entry balance verified
- ✅ Trial balance validation works
- ✅ Sign conventions checked
- ✅ Domain rules extensible to other domains

### Phase 5 Success Criteria
- ✅ Quality context aggregates all pillars
- ✅ Dimension scores align with manual assessment
- ✅ LLM generates domain-appropriate rules
- ✅ Quality warnings actionable

### Phase 6 Success Criteria
- ✅ Context document has 5 distinct pillars
- ✅ Each pillar independently queryable
- ✅ MCP tools expose pillar contexts
- ✅ LLM consumes richer context effectively

---

## Next Steps

1. **Review this plan** - Does it align with your vision from the prototypes?

2. **Prioritize domains** - Should we start with financial domain quality, or make it more generic?

3. **Validate approach** - Any concerns about the 5-pillar architecture?

4. **Choose starting point** - Which phase should we implement first?

My recommendation: **Start with Phase 1 (Statistical Quality)** since it's foundational and feeds into quality rule generation. Then Phase 4 (Financial Domain Quality) to prove out the domain-specific approach. Then backfill topological and temporal quality.
