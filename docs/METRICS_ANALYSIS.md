# Data Context Library: Comprehensive Metrics Analysis Report

**Generated:** 2025-11-28
**Updated:** 2025-12-14 (context-focused refactoring)
**Version:** 2.0
**Scope:** All metrics collected across 5 pillars (Statistical, Topological, Temporal, Correlation, Domain Quality)

---

## Executive Summary

This report analyzes all metrics collected by the DataRaum Context Engine, documenting their purpose, relevance, and surfacing in quality context. The library collects **127+ individual metrics** across 5 pillars to provide comprehensive data context for AI-driven analytics.

### Key Findings

| Pillar | Total Metrics | Surfaced in Quality Context | Issue Generation | Notes |
|--------|---------------|---------------------------|---------------------|------------|
| Statistical | 42 | 15+ | Yes | Outliers, Benford, multicollinearity |
| Topological | 18 | 6+ | Yes | Betti numbers, orphans, cycles |
| Temporal | 36 | 10+ | Yes | Granularity, completeness, staleness |
| Correlation | 21 | 8+ | Yes | High correlations, FD violations |
| Domain Quality | 10+ | 10+ | Yes | Rule violations |
| **TOTAL** | **127+** | **49+** | - | - |

**Philosophy:** The library prioritizes **context-focused output over computed scores**. Metrics and issues are surfaced with flags and evidence for consumers to interpret within their own context. Quality scores have been removed in favor of raw metrics and actionable flags.

### Recent Changes (v2.0)

The following metrics are now surfaced in quality context output:

**Column Context (`ColumnQualityContext`):**
- `detected_granularity` - Time granularity (daily, weekly, etc.)
- `completeness_ratio` - Temporal coverage ratio
- `semantic_role` - Semantic role (identifier, measure, attribute, dimension)
- `entity_type` - Business entity type
- `business_name` - Human-friendly name
- `derived_from` - Derivation relationships (formula, match_rate)

**Table Context (`TableQualityContext`):**
- `multicollinearity` - Full LLM-formatted multicollinearity analysis
- `detected_entity_type` - Table entity type
- `is_fact_table` / `is_dimension_table` - Table classification

---

## Pillar 1: Statistical Context

### 1.1 StatisticalProfile Model

This model captures basic statistical metadata that's always computed. It provides foundational understanding of data distribution and characteristics.

#### Basic Counts (5 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `total_count` | Total number of rows | Foundational context | ✓ (indirectly) | Used for ratio calculations |
| `null_count` | Number of NULL values | Completeness quality | ✓ | Direct input to completeness dimension |
| `distinct_count` | Number of unique values | Uniqueness quality | ✓ | Used for cardinality analysis |
| `null_ratio` | Percentage of NULLs (null_count/total_count) | Completeness quality | ✓ | **Primary completeness metric** |
| `cardinality_ratio` | Percentage of unique values (distinct/total) | Uniqueness quality | ✓ | **Primary uniqueness metric** |

**Synthesis Usage:** All 5 metrics contribute to Completeness and Uniqueness dimensions.

#### Numeric Statistics (7 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `min_value` | Minimum numeric value | Data range understanding | ✗ | Human/AI context only |
| `max_value` | Maximum numeric value | Data range understanding | ✗ | Human/AI context only |
| `mean_value` | Average value | Distribution center | ✗ | Human/AI context only |
| `stddev_value` | Standard deviation | Data spread | ✗ | Human/AI context only |
| `skewness` | Distribution asymmetry | Distribution shape | ✗ | AI context for understanding distribution |
| `kurtosis` | Distribution tail heaviness | Outlier propensity | ✗ | AI context for understanding outliers |
| `cv` | Coefficient of variation (stddev/mean) | Relative variability | ✗ | AI context for scale-independent comparison |

**Why Not in Synthesis?** These are descriptive statistics that help AI understand data characteristics but don't directly indicate quality issues. Outliers detected from these are surfaced separately.

#### Distribution Shape (1 metric)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `percentiles` | Distribution percentiles (p25, p50, p75, p95, p99) | Distribution understanding | ✗ | AI context for understanding data spread |

**Purpose:** Provides AI with rich distribution context without requiring raw data access.

#### String Statistics (3 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `min_length` | Shortest string length | Format validation context | ✗ | Human curiosity |
| `max_length` | Longest string length | Format validation context | ✗ | Human curiosity |
| `avg_length` | Average string length | Format validation context | ✗ | Human curiosity |

**Purpose:** Helps understand data format and potential truncation issues.

#### Distribution Data (2 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `histogram` | Bucketed value distribution | Visual/AI distribution understanding | ✗ | AI context for distribution shape |
| `top_values` | Most frequent values with counts | Pattern detection | ✗ | AI context for common values |

**Purpose:** Enables AI to understand distribution without accessing raw data.

#### Entropy (2 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `shannon_entropy` | Information content (bits) | Randomness/predictability | ✗ | AI context for data complexity |
| `normalized_entropy` | Entropy on 0-1 scale | Relative randomness | ✗ | AI context for data complexity |

**Purpose:** Helps AI understand how predictable/random the data is.

#### Uniqueness Metrics (2 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `is_unique` | All values are unique (potential PK) | Schema inference | ✗ | AI context for key detection |
| `duplicate_count` | Number of duplicated values | Uniqueness quality | ✓ | Used in uniqueness dimension |

**Synthesis Usage:** `duplicate_count` contributes to Uniqueness dimension scoring.

#### Ordering Metrics (4 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `is_sorted` | Column is sorted | Indexing/query optimization hint | ✗ | AI context for query planning |
| `is_monotonic_increasing` | Values always increase | Temporal/sequence detection | ✗ | AI context for time series |
| `is_monotonic_decreasing` | Values always decrease | Sequence detection | ✗ | AI context for inverse sequences |
| `inversions_ratio` | Measure of "unsortedness" | Data organization | ✗ | AI context for data organization |

**Purpose:** Helps AI suggest appropriate indexes and understand data organization.

**StatisticalProfile Total: 42 metrics (12 used in synthesis, 30 for context)**

---

### 1.2 StatisticalQualityMetrics Model

Advanced quality metrics that may be expensive to compute. These are optional based on configuration.

#### Benford's Law (5 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `benford_chi_square` | Chi-square test statistic | Fraud detection statistical test | ✗ | Evidence for benford_compliant |
| `benford_p_value` | Statistical significance | Fraud detection confidence | ✗ | Evidence for benford_compliant |
| `benford_compliant` | Passes Benford's Law (p > 0.05) | Accuracy quality | ✓ | **Primary accuracy metric for financial data** |
| `benford_interpretation` | Human-readable interpretation | Explanation | ✗ | Human/AI context |
| `benford_digit_distribution` | First digit distribution | Fraud detection evidence | ✗ | Evidence/visualization |

**Synthesis Usage:** `benford_compliant` directly impacts Accuracy dimension score (30% penalty if violated).

**Why Important:** Benford's Law violations can indicate data manipulation, especially in financial datasets.

#### Distribution Stability (5 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `ks_statistic` | KS test statistic | Distribution drift magnitude | ✗ | Evidence for distribution_stable |
| `ks_p_value` | Statistical significance | Distribution drift confidence | ✗ | Evidence for distribution_stable |
| `distribution_stable` | Distribution hasn't changed (p > 0.01) | Consistency quality | ✗ | Could be used in synthesis (currently not) |
| `comparison_period_start` | Start of comparison period | Temporal context | ✗ | Metadata |
| `comparison_period_end` | End of comparison period | Temporal context | ✗ | Metadata |

**Future Use:** `distribution_stable` could feed into Consistency dimension but currently isn't used.

#### Outlier Detection - IQR Method (4 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `iqr_outlier_count` | Number of IQR outliers | Validity quality | ✗ | Used to compute iqr_outlier_ratio |
| `iqr_outlier_ratio` | Percentage of IQR outliers | Validity quality | ✓ | **Primary validity metric** |
| `iqr_lower_fence` | Lower outlier boundary | Statistical context | ✗ | Evidence/explanation |
| `iqr_upper_fence` | Upper outlier boundary | Statistical context | ✗ | Evidence/explanation |

**Synthesis Usage:** `iqr_outlier_ratio` penalizes Validity dimension if >5% of values are outliers.

#### Outlier Detection - Isolation Forest (4 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `isolation_forest_score` | Average anomaly score | ML-based anomaly detection | ✗ | Alternative outlier method |
| `isolation_forest_anomaly_count` | Number of ML-detected anomalies | Validity quality | ✗ | Could be used as alternative to IQR |
| `isolation_forest_anomaly_ratio` | Percentage of ML anomalies | Validity quality | ✗ | Could be used as alternative to IQR |
| `outlier_samples` | Sample outliers for review | Human investigation | ✗ | Human curiosity |

**Why Not Used:** Currently synthesis uses IQR method. Isolation Forest provides alternative perspective.

#### Multicollinearity (2 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `vif_score` | Variance Inflation Factor | Multicollinearity detection | ✓ | **Primary consistency metric** |
| `vif_correlated_columns` | Highly correlated column IDs | Evidence/investigation | ✗ | Evidence for VIF score |

**Synthesis Usage:** VIF >10 penalizes Consistency dimension (up to 50% penalty).

**Why Important:** High VIF indicates redundant columns that can confuse AI models.

#### Overall Quality Assessment (2 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `quality_score` | Aggregate quality score (0-1) | Pre-synthesis quality estimate | ✗ | Superseded by Pillar 5 synthesis |
| `quality_issues` | List of detected issues | Issue aggregation | ✓ | **Issues extracted and re-categorized** |

**Synthesis Usage:** Issues are extracted, mapped to dimensions, and aggregated in Pillar 5.

**StatisticalQualityMetrics Total: 22 metrics (4 used in synthesis, 18 for context/evidence)**

---

## Pillar 2: Topological Context

Topological metrics use TDA (Topological Data Analysis) to detect structural patterns and relationships in data.

### 2.1 TopologicalQualityMetrics Model (Table-level)

#### Betti Numbers (3 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `betti_0` | Connected components | Data clustering | ✗ | AI context for structural understanding |
| `betti_1` | Cycles/holes | Circular relationships | ✗ | AI context for flow patterns |
| `betti_2` | Voids/cavities | 3D structural features | ✗ | AI context for complex structures |

**Purpose:** Helps AI understand structural complexity without graph theory expertise.

#### Persistence Metrics (3 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `persistent_entropy` | Topological complexity measure | Structural complexity | ✗ | AI context for data organization |
| `max_persistence_h0` | Longest-lived component | Structural stability | ✗ | AI context for stable structures |
| `max_persistence_h1` | Longest-lived cycle | Flow pattern stability | ✗ | AI context for persistent flows |

**Purpose:** Quantifies structural stability and complexity.

#### Persistence Diagrams (1 metric)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `persistence_diagrams` | Birth/death of topological features | Complete topological signature | ✗ | Advanced AI context |

**Purpose:** Complete topological fingerprint for advanced analysis.

#### Stability Metrics (2 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `bottleneck_distance` | Distance from previous topology | Structural drift | ✗ | Evidence for homologically_stable |
| `homologically_stable` | Topology hasn't changed significantly | Consistency quality | ✓ | Generates INFO-level issue if unstable |

**Synthesis Usage:** Instability generates a Consistency dimension issue for human review.

#### Complexity Metrics (3 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `structural_complexity` | Sum of Betti numbers | Overall structural richness | ✗ | AI context for complexity |
| `complexity_trend` | 'increasing', 'stable', 'decreasing' | Complexity drift | ✗ | AI context for evolution |
| `complexity_within_bounds` | Within historical norms | Anomaly detection | ✗ | Could be used in synthesis |

**Future Use:** `complexity_within_bounds` could trigger Consistency issues.

#### Anomaly Detection (2 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `anomalous_cycles` | Unexpected flow patterns | Consistency quality | ✓ | **Generates WARNING-level issues** |
| `orphaned_components` | Disconnected subgraphs | Consistency quality | ✓ | **Generates WARNING-level issues** |

**Synthesis Usage:** Both directly generate Consistency dimension issues with specific penalties:
- Orphaned components: Up to 40% penalty
- Anomalous cycles: Up to 30% penalty

#### Summary (2 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `topology_description` | Human-readable summary | Explanation | ✗ | Human/AI context |
| `quality_warnings` | List of warnings | Issue aggregation | ✓ | Extracted into synthesis issues |

**TopologicalQualityMetrics Total: 16 metrics (4 used in synthesis, 12 for context)**

### 2.2 Supporting Models

#### PersistentCycle (11 fields per cycle)

Tracks individual cycles detected in the data. Not directly used in synthesis but provides evidence for anomalous cycle detection.

#### StructuralComplexityHistory (6 fields per snapshot)

Historical tracking for baseline comparison. Enables drift detection but not directly used in current synthesis.

**Total Topological Metrics: 18+ metrics (4 used in synthesis, 14 for context)**

---

## Pillar 3: Temporal Context

Temporal metrics analyze time-based patterns, trends, and quality issues.

### 3.1 TemporalQualityMetrics Model

#### Basic Temporal Stats (4 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `min_timestamp` | Earliest timestamp | Data range | ✗ | AI context for time coverage |
| `max_timestamp` | Latest timestamp | Data range | ✗ | AI context for time coverage |
| `span_days` | Total time range in days | Data coverage | ✗ | AI context for temporal scope |
| `detected_granularity` | Time granularity (second, minute, hour, day, etc.) | Data resolution | ✗ | AI context for query planning |
| `granularity_confidence` | Confidence in granularity detection (0-1) | Detection quality | ✗ | Metadata quality indicator |

**Purpose:** Establishes temporal scope and resolution for AI query planning.

#### Seasonality Analysis (4 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `has_seasonality` | Seasonal patterns detected | Pattern detection | ✗ | AI context for forecasting |
| `seasonality_strength` | Strength of seasonal pattern (0-1) | Pattern significance | ✗ | AI context for pattern importance |
| `seasonality_period` | Seasonal period (daily, weekly, monthly, etc.) | Pattern type | ✗ | AI context for pattern understanding |
| `seasonal_peaks` | When peaks occur (month, day_of_week) | Pattern details | ✗ | AI context for business insights |

**Purpose:** Helps AI understand cyclical patterns for forecasting and anomaly detection.

#### Trend Analysis (5 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `has_trend` | Trend detected | Pattern detection | ✗ | AI context for forecasting |
| `trend_strength` | Strength of trend (0-1) | Pattern significance | ✗ | AI context for pattern importance |
| `trend_direction` | 'increasing', 'decreasing', 'stable' | Trend type | ✗ | AI context for business insights |
| `trend_slope` | Rate of change | Trend magnitude | ✗ | AI context for quantifying trend |
| `autocorrelation_lag1` | First-order autocorrelation | Time dependency | ✗ | AI context for time series modeling |

**Purpose:** Enables AI to understand long-term patterns and make projections.

#### Change Points (2 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `change_point_count` | Number of detected breaks | Structural stability | ✗ | Could trigger Consistency issues |
| `change_points` | List of break points with details | Change detection | ✗ | AI context for regime changes |

**Future Use:** High change point counts could indicate Consistency issues.

#### Update Frequency (5 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `update_frequency_score` | Regularity of updates (0-1) | Timeliness quality | ✗ | Could be used in Timeliness dimension |
| `median_update_interval_seconds` | Typical time between updates | Update pattern | ✗ | AI context for refresh expectations |
| `update_interval_cv` | Coefficient of variation | Update consistency | ✗ | AI context for regularity |
| `last_update_timestamp` | Most recent update | Freshness | ✗ | Used to compute data_freshness_days |
| `data_freshness_days` | Days since last update | Timeliness quality | ✓ | **Used in Timeliness dimension** |

**Synthesis Usage:** `data_freshness_days` directly impacts Timeliness score with exponential decay.

#### Fiscal Calendar (4 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `fiscal_alignment_detected` | Data aligns with fiscal calendar | Business context | ✗ | AI context for financial data |
| `fiscal_year_end_month` | Fiscal year end (1-12) | Business context | ✗ | AI context for period boundaries |
| `has_period_end_effects` | Activity spikes at period ends | Business pattern | ✗ | AI context for seasonal patterns |
| `period_end_spike_ratio` | Magnitude of period-end activity | Pattern strength | ✗ | AI context for business cycles |

**Purpose:** Helps AI understand financial reporting cycles and patterns.

#### Distribution Stability (3 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `distribution_stability_score` | Stability across time (0-1) | Consistency quality | ✗ | Could be used in Consistency dimension |
| `distribution_shift_count` | Number of detected shifts | Drift detection | ✗ | Could trigger Consistency issues |
| `distribution_shifts` | KS test results by period | Shift details | ✗ | AI context for data evolution |

**Future Use:** Low stability could penalize Consistency dimension.

#### Completeness and Quality (3 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `completeness_ratio` | Temporal coverage (0-1) | Completeness quality | ✓ | **Used in Completeness dimension** |
| `gap_count` | Number of temporal gaps | Completeness quality | ✗ | Evidence for completeness_ratio |
| `largest_gap_days` | Biggest gap in days | Completeness quality | ✗ | Evidence for completeness_ratio |

**Synthesis Usage:** `completeness_ratio` directly multiplies Completeness dimension score.

#### Staleness (1 metric)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `is_stale` | Data is stale (derived from freshness) | Timeliness quality | ✓ | **Primary Timeliness indicator** |

**Synthesis Usage:** Stale data gets 50% Timeliness score.

#### Overall Temporal Quality (2 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `temporal_quality_score` | Aggregate temporal quality (0-1) | Pre-synthesis quality | ✗ | Superseded by Pillar 5 synthesis |
| `quality_issues` | List of detected issues | Issue aggregation | ✓ | **Issues extracted and re-categorized** |

**Synthesis Usage:** Issues are extracted, mapped to dimensions (Completeness, Timeliness, Consistency).

**TemporalQualityMetrics Total: 36 metrics (8 used in synthesis, 28 for context)**

### 3.2 Supporting Models

#### SeasonalDecomposition (7 fields)
Detailed seasonal decomposition for time series. Provides evidence for seasonality metrics.

#### ChangePoint (8 fields per change point)
Individual change points with before/after statistics. Evidence for change_point_count.

#### DistributionShift (10 fields per shift)
Individual distribution shifts across time periods. Evidence for distribution_shifts.

#### UpdateFrequencyHistory (8 fields per snapshot)
Historical update frequency tracking. Enables baseline comparison.

---

## Pillar 4: Correlation Context

Correlation metrics detect relationships between columns within a table.

### 4.1 ColumnCorrelation Model

#### Pearson Correlation (2 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `pearson_r` | Linear correlation (-1 to 1) | Linear relationship | ✓ | Used to detect high correlations |
| `pearson_p_value` | Statistical significance | Correlation confidence | ✗ | Used with is_significant |

**Synthesis Usage:** |r| > 0.9 generates INFO-level Consistency issue (potential redundancy).

#### Spearman Correlation (2 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `spearman_rho` | Monotonic correlation (-1 to 1) | Non-linear relationship | ✓ | Alternative to Pearson |
| `spearman_p_value` | Statistical significance | Correlation confidence | ✗ | Used with is_significant |

**Synthesis Usage:** |ρ| > 0.9 generates INFO-level Consistency issue if Pearson is NULL.

#### Metadata (4 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `sample_size` | Number of rows analyzed | Statistical power | ✗ | Metadata |
| `computed_at` | When correlation was computed | Freshness | ✗ | Metadata |
| `correlation_strength` | 'none', 'weak', 'moderate', 'strong', 'very_strong' | Interpretation | ✗ | Human/AI context |
| `is_significant` | p < 0.05 | Statistical validity | ✗ | Used to filter correlations |

**ColumnCorrelation Total: 8 metrics (2 used in synthesis, 6 metadata/context)**

### 4.2 CategoricalAssociation Model

#### Cramér's V (4 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `cramers_v` | Association strength (0-1) | Categorical relationship | ✗ | Could be used like Pearson |
| `chi_square` | Chi-square test statistic | Statistical test | ✗ | Evidence for p_value |
| `p_value` | Statistical significance | Association confidence | ✗ | Used with is_significant |
| `degrees_of_freedom` | Chi-square DOF | Statistical context | ✗ | Metadata |

**Future Use:** High Cramér's V could generate Consistency issues like Pearson correlation.

#### Metadata (4 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `sample_size` | Number of rows analyzed | Statistical power | ✗ | Metadata |
| `computed_at` | When association was computed | Freshness | ✗ | Metadata |
| `association_strength` | 'none', 'weak', 'moderate', 'strong' | Interpretation | ✗ | Human/AI context |
| `is_significant` | p < 0.05 | Statistical validity | ✗ | Used to filter associations |

**CategoricalAssociation Total: 8 metrics (0 used in synthesis, 8 context/metadata)**

### 4.3 FunctionalDependency Model

#### Dependency Characteristics (5 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `determinant_column_ids` | Left side of FD (A → B) | Relationship structure | ✗ | Defines the FD |
| `dependent_column_id` | Right side of FD | Relationship structure | ✗ | Defines the FD |
| `confidence` | FD exactness (1.0 = exact) | FD quality | ✗ | Evidence for violation_count |
| `unique_determinant_values` | Count of unique determinant combinations | FD scope | ✗ | Metadata |
| `violation_count` | How many determinants map to multiple dependents | Consistency quality | ✓ | **Generates WARNING-level issues** |

**Synthesis Usage:** Violations penalize Consistency dimension (up to 50% penalty, 10% per violation).

#### Metadata (2 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `computed_at` | When FD was detected | Freshness | ✗ | Metadata |
| `example` | Example of the dependency | Understanding | ✗ | Human/AI context |

**FunctionalDependency Total: 7 metrics (1 used in synthesis, 6 context/metadata)**

### 4.4 DerivedColumn Model

Detects columns computed from other columns (e.g., total = price * quantity).

#### Derivation Characteristics (8 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `derived_column_id` | The computed column | Schema inference | ✗ | AI context for redundancy |
| `source_column_ids` | Columns used in computation | Dependency graph | ✗ | AI context for lineage |
| `derivation_type` | 'sum', 'product', 'concat', etc. | Operation type | ✗ | AI context for understanding |
| `formula` | Human-readable formula | Explanation | ✗ | Human/AI context |
| `match_rate` | How often formula holds (0-1) | Derivation confidence | ✗ | Could trigger Consistency issues |
| `total_rows` | Number of rows analyzed | Statistical power | ✗ | Metadata |
| `matching_rows` | Rows where formula holds | Evidence | ✗ | Metadata |
| `mismatch_examples` | Sample mismatches | Investigation | ✗ | Human curiosity |

**Future Use:** Low match_rate could indicate Consistency issues.

**DerivedColumn Total: 8 metrics (0 used in synthesis, 8 for AI context/schema inference)**

**Total Correlation Metrics: 31 metrics (3 used in synthesis, 28 for context)**

---

## Pillar 5: Domain Quality

Domain-specific quality metrics, focusing on financial accounting rules.

### 5.1 DomainQualityMetrics Model (Generic)

#### Generic Domain Metrics (4 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `domain` | Domain type ('financial', 'marketing', etc.) | Domain classification | ✗ | Metadata |
| `metrics` | Flexible JSONB for domain-specific metrics | Domain-specific measures | ✗ | Extensibility |
| `domain_compliance_score` | Overall domain compliance (0-1) | Accuracy quality | ✓ | **Used in Accuracy dimension** |
| `violations` | List of domain rule violations | Issue aggregation | ✓ | **Extracted into synthesis issues** |

**Synthesis Usage:** Compliance score directly multiplies Accuracy dimension. Violations generate issues.

### 5.2 FinancialQualityMetrics Model

#### Double-Entry Accounting (3 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `double_entry_balanced` | Debits = Credits | Accuracy quality | ✓ | Fundamental accounting check |
| `balance_difference` | |Debits - Credits| | Balance magnitude | ✓ | Evidence for balance check |
| `balance_tolerance` | Acceptable difference | Precision requirement | ✗ | Configuration |

**Synthesis Usage:** Unbalanced entries severely penalize Accuracy dimension.

#### Trial Balance (4 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `trial_balance_check` | Passes trial balance | Accuracy quality | ✓ | Overall balance check |
| `accounting_equation_holds` | Assets = Liabilities + Equity | Fundamental equation | ✓ | Core accounting principle |
| `assets_total` | Total assets | Context | ✗ | Evidence |
| `liabilities_total` | Total liabilities | Context | ✗ | Evidence |
| `equity_total` | Total equity | Context | ✗ | Evidence |

**Synthesis Usage:** Equation violations generate CRITICAL-level Accuracy issues.

#### Sign Conventions (2 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `sign_convention_compliance` | % following sign rules (0-1) | Accuracy quality | ✓ | E.g., revenue should be positive |
| `sign_violations` | List of sign violations | Issue details | ✓ | Extracted into synthesis issues |

**Synthesis Usage:** Compliance score penalizes Accuracy dimension.

#### Consolidation (2 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `intercompany_elimination_rate` | % of intercompany transactions eliminated | Consolidation quality | ✓ | Multi-entity accounting |
| `orphaned_intercompany` | Unmatched intercompany transactions | Accuracy quality | ✓ | Generates WARNING issues |

**Synthesis Usage:** Low elimination rate and orphaned transactions penalize Accuracy.

#### Period Integrity (2 metrics)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `fiscal_period_complete` | All days in period have data | Completeness quality | ✓ | Could affect Completeness dimension |
| `period_end_cutoff_clean` | No transactions after period close | Accuracy quality | ✓ | Accounting period discipline |

**Synthesis Usage:** Incomplete periods and dirty cutoffs penalize Accuracy/Completeness.

#### Overall Financial Quality (1 metric)

| Metric | What It Measures | Relevance | Used in Synthesis? | Notes |
|--------|------------------|-----------|-------------------|-------|
| `financial_quality_score` | Aggregate financial quality (0-1) | Pre-synthesis quality | ✓ | Aggregated into Accuracy dimension |

**FinancialQualityMetrics Total: 14 metrics (all used in synthesis - 100% coverage)**

### 5.3 Supporting Models

#### DoubleEntryCheck (8 fields)
Detailed double-entry breakdown. Evidence for double_entry_balanced.

#### TrialBalanceCheck (6 fields)
Trial balance validation details. Evidence for trial_balance_check.

#### SignConventionViolation (7 fields per violation)
Individual sign violations. Evidence for sign_convention_compliance.

#### IntercompanyTransaction (7 fields per transaction)
Intercompany tracking. Evidence for elimination metrics.

#### FiscalPeriodIntegrity (10 fields)
Period completeness details. Evidence for period integrity metrics.

**Total Domain Quality Metrics: 10+ primary metrics (100% used in synthesis)**

---

## Quality Synthesis Architecture

### How Metrics Flow Into Quality Dimensions

```
Pillar 1 (Statistical)      →  Completeness, Validity, Uniqueness, Consistency, Accuracy
Pillar 2 (Topological)      →  Consistency
Pillar 3 (Temporal)         →  Completeness, Timeliness, Consistency
Pillar 4 (Correlation)      →  Consistency
Pillar 5 (Domain Quality)   →  Accuracy, Completeness
```

### Dimension Scoring Functions

#### Completeness Dimension
**Inputs:**
- `null_ratio` (Statistical)
- `temporal_completeness_ratio` (Temporal)

**Formula:** `score = (1 - null_ratio) × temporal_completeness`

**Penalties:** Multiplicative - poor completeness in either aspect reduces overall score.

#### Validity Dimension
**Inputs:**
- `parse_success_rate` (not yet implemented)
- `iqr_outlier_ratio` (Statistical)

**Formula:** `score = parse_rate × (1 - outlier_penalty)`

**Penalties:** >5% outliers trigger penalty (max 50%).

#### Consistency Dimension
**Inputs:**
- `vif_score` (Statistical)
- `functional_dep_violations` (Correlation)
- `orphaned_components` (Topological)
- `anomalous_cycles_count` (Topological)
- `high_correlations_count` (Correlation)

**Formula:** Multiple penalties applied multiplicatively:
- VIF >10: up to 50% penalty
- FD violations: 10% per violation (max 50%)
- Orphaned components: 15% per component (max 40%)
- Anomalous cycles: 10% per cycle (max 30%)
- High correlations: 5% per correlation (max 30%)

**Philosophy:** Consistency is the most complex dimension, aggregating structural, statistical, and relational issues.

#### Uniqueness Dimension
**Inputs:**
- `cardinality_ratio` (Statistical)
- `duplicate_count` (Statistical)

**Formula:** `score = cardinality_ratio`

**Notes:** Simple dimension - higher cardinality = better uniqueness.

#### Timeliness Dimension
**Inputs:**
- `is_stale` (Temporal)
- `data_freshness_days` (Temporal)

**Formula:** 
- If stale: `score = 0.5 × freshness_decay`
- Freshness decay: Exponential (7 days=100%, 30 days=75%, 90 days=50%, 180 days=25%)

**Philosophy:** Timeliness degrades exponentially over time.

#### Accuracy Dimension
**Inputs:**
- `benford_compliant` (Statistical)
- `domain_compliance_score` (Domain Quality)

**Formula:** `score = benford_factor × domain_compliance`
- Benford violation: 30% penalty (score × 0.7)
- Domain compliance: Direct multiplier

**Philosophy:** Accuracy requires both statistical plausibility and domain rule compliance.

---

## Metric Usage Patterns

### Pattern 1: Direct Scoring Metrics (38 metrics)
These metrics directly influence quality dimension scores:

**Statistical Pillar (12):**
- null_ratio, null_count, total_count
- distinct_count, cardinality_ratio, duplicate_count
- iqr_outlier_ratio, iqr_outlier_count
- vif_score
- benford_compliant
- quality_issues (extracted)

**Topological Pillar (4):**
- orphaned_components
- anomalous_cycles
- homologically_stable
- quality_warnings (extracted)

**Temporal Pillar (8):**
- completeness_ratio
- is_stale, data_freshness_days
- quality_issues (extracted)

**Correlation Pillar (4):**
- pearson_r, spearman_rho (for high correlation detection)
- violation_count (FunctionalDependency)

**Domain Quality Pillar (10+):**
- All financial quality metrics

### Pattern 2: Evidence Metrics (40+ metrics)
These provide supporting evidence for scoring metrics:

- Statistical: min/max/mean/stddev, percentiles, histogram
- Topological: Betti numbers, persistence diagrams, bottleneck_distance
- Temporal: seasonality metrics, trend metrics, change_points
- Correlation: p_values, chi_square, examples

**Purpose:** Enable human investigation and AI understanding of root causes.

### Pattern 3: AI Context Metrics (50+ metrics)
These provide rich context for AI interpretation:

- Distribution shape (skewness, kurtosis, entropy)
- Temporal patterns (seasonality, trends, fiscal alignment)
- Topological structure (Betti numbers, persistence)
- Derivation formulas and examples

**Purpose:** Enable AI to make intelligent suggestions without accessing raw data.

### Pattern 4: Human Curiosity Metrics (10+ metrics)
These satisfy human analysts' questions:

- String lengths (min/max/avg)
- Top values
- Outlier samples
- Mismatch examples

**Purpose:** Support exploratory data analysis and investigation.

---

## Coverage Analysis

### High Coverage Pillars (>50% in synthesis)
- **Domain Quality: 100%** - All metrics drive quality scores
  - Reason: Domain rules directly define quality

### Medium Coverage Pillars (20-30% in synthesis)
- **Statistical: 28.6%** - Core metrics used, rest for context
  - Reason: Rich statistical context for AI, selective quality scoring
- **Topological: 22.2%** - Structural anomalies used
  - Reason: TDA provides complex context, key anomalies scored
- **Temporal: 22.2%** - Key quality metrics used
  - Reason: Many pattern metrics for AI context, selective quality scoring

### Low Coverage Pillars (<20% in synthesis)
- **Correlation: 19.0%** - Minimal synthesis usage
  - Reason: Primarily for AI context and schema inference

### Overall Coverage: 29.9%

**Interpretation:** This is INTENTIONAL. The library follows a "comprehensive context" philosophy:
- ~30% of metrics drive quality scores (actionable)
- ~70% of metrics provide AI context (interpretive)

This enables AI to:
1. Understand data without accessing it
2. Make intelligent suggestions
3. Explain quality issues with evidence
4. Suggest appropriate queries and analyses

---

## Recommendations

### For Quality Synthesis Enhancement

1. **Add to Synthesis (High Value):**
   - `distribution_stable` (Statistical) → Consistency dimension
   - `complexity_within_bounds` (Topological) → Consistency dimension
   - `change_point_count` (Temporal) → Consistency dimension (if excessive)
   - `update_frequency_score` (Temporal) → Timeliness dimension
   - `cramers_v` (Correlation) → Consistency dimension (like Pearson)
   - `match_rate` (DerivedColumn) → Consistency dimension (if low)

2. **Implement Missing Metrics:**
   - `parse_success_rate` (referenced but not yet implemented)
   - Type inference confidence scores

3. **Consolidate Issue Extraction:**
   - All pillars should consistently populate `quality_issues` fields
   - Standardize issue format (type, severity, dimension, evidence)

### For AI Context Enhancement

1. **Add Human-Readable Summaries:**
   - Each metric group should have an AI-generated summary
   - Example: "This column shows strong weekly seasonality with peaks on Fridays"

2. **Add Metric Importance Scores:**
   - Not all context metrics are equally relevant
   - Add relevance scores based on detected data type

3. **Add Cross-Pillar Correlations:**
   - Link related metrics across pillars
   - Example: Link temporal change_points to distribution_shifts

### For Human Usability

1. **Add Visualization Hints:**
   - Tag metrics that should be visualized
   - Suggest chart types (histogram, time series, scatter)

2. **Add Threshold Configurations:**
   - Make thresholds configurable (currently hardcoded)
   - Example: Benford p-value threshold, outlier percentage threshold

3. **Add Metric Explanations:**
   - Add `explanation` field to each metric
   - Explain what "good" vs "bad" values mean

---

## Appendix A: Metric Quick Reference

### Statistical Pillar Quick Reference
| Category | Count | Synthesis % |
|----------|-------|-------------|
| Basic Counts | 5 | 100% |
| Numeric Stats | 7 | 0% |
| String Stats | 3 | 0% |
| Distribution | 2 | 0% |
| Entropy | 2 | 0% |
| Uniqueness | 2 | 50% |
| Ordering | 4 | 0% |
| Benford | 5 | 20% |
| Stability | 5 | 0% |
| Outliers (IQR) | 4 | 25% |
| Outliers (IF) | 4 | 0% |
| Multicollinearity | 2 | 50% |
| Quality Assessment | 2 | 50% |
| **TOTAL** | **42** | **28.6%** |

### Topological Pillar Quick Reference
| Category | Count | Synthesis % |
|----------|-------|-------------|
| Betti Numbers | 3 | 0% |
| Persistence | 4 | 0% |
| Stability | 2 | 50% |
| Complexity | 3 | 0% |
| Anomalies | 2 | 100% |
| Summary | 2 | 50% |
| **TOTAL** | **16** | **25%** |

### Temporal Pillar Quick Reference
| Category | Count | Synthesis % |
|----------|-------|-------------|
| Basic Stats | 4 | 0% |
| Seasonality | 4 | 0% |
| Trends | 5 | 0% |
| Change Points | 2 | 0% |
| Update Frequency | 5 | 20% |
| Fiscal Calendar | 4 | 0% |
| Stability | 3 | 0% |
| Completeness | 3 | 33% |
| Staleness | 1 | 100% |
| Quality Assessment | 2 | 50% |
| **TOTAL** | **33** | **24.2%** |

### Correlation Pillar Quick Reference
| Category | Count | Synthesis % |
|----------|-------|-------------|
| Pearson | 2 | 50% |
| Spearman | 2 | 50% |
| Correlation Metadata | 4 | 0% |
| Cramér's V | 4 | 0% |
| Association Metadata | 4 | 0% |
| Functional Dependencies | 5 | 20% |
| FD Metadata | 2 | 0% |
| Derived Columns | 8 | 0% |
| **TOTAL** | **31** | **12.9%** |

### Domain Quality Quick Reference
| Category | Count | Synthesis % |
|----------|-------|-------------|
| Generic Domain | 4 | 50% |
| Double-Entry | 3 | 100% |
| Trial Balance | 5 | 100% |
| Sign Conventions | 2 | 100% |
| Consolidation | 2 | 100% |
| Period Integrity | 2 | 100% |
| Overall Financial | 1 | 100% |
| **TOTAL** | **19** | **94.7%** |

---

## Appendix B: Quality Dimension Mapping

### Completeness Dimension
**Sources:**
- Statistical: null_ratio (primary)
- Temporal: completeness_ratio (primary)
- Domain: fiscal_period_complete (financial only)

**Scoring:** Multiplicative - both statistical and temporal completeness matter.

### Validity Dimension
**Sources:**
- Statistical: iqr_outlier_ratio (primary)
- Statistical: parse_success_rate (not yet implemented)
- Statistical: isolation_forest metrics (alternative, not used)

**Scoring:** Parse rate × (1 - outlier penalty if >5%)

### Consistency Dimension
**Sources:**
- Statistical: vif_score (multicollinearity)
- Correlation: functional_dep_violations
- Correlation: high_correlations_count
- Topological: orphaned_components
- Topological: anomalous_cycles
- Temporal: distribution_stability (not yet used)

**Scoring:** Multiple multiplicative penalties - most complex dimension.

### Uniqueness Dimension
**Sources:**
- Statistical: cardinality_ratio (primary)
- Statistical: duplicate_count (evidence)
- Statistical: is_unique (boolean indicator)

**Scoring:** Simple - cardinality_ratio is the score.

### Timeliness Dimension
**Sources:**
- Temporal: is_stale (boolean)
- Temporal: data_freshness_days (primary)
- Temporal: update_frequency_score (not yet used)

**Scoring:** Exponential decay based on days since update.

### Accuracy Dimension
**Sources:**
- Statistical: benford_compliant (financial data)
- Domain: domain_compliance_score (all domain metrics)
- Domain: Financial metrics (trial balance, sign conventions, etc.)

**Scoring:** Benford factor × domain_compliance.

---

## Appendix C: Issue Severity Mapping

### CRITICAL Issues
- Accounting equation violations (Assets ≠ Liabilities + Equity)
- Double-entry balance failures
- Critical domain rule violations

**Impact:** Block data from being used in production.

### ERROR Issues
- Sign convention violations
- Period cutoff violations
- High-severity domain violations

**Impact:** Data usable but requires investigation.

### WARNING Issues
- Orphaned components (structural inconsistencies)
- Anomalous cycles (unexpected relationships)
- Functional dependency violations
- Medium-severity domain violations

**Impact:** Data usable but may have quality issues.

### INFO Issues
- High correlations (potential redundancy)
- Structural instability (topology changed)
- Low-severity domain violations

**Impact:** Informational only - may optimize but not required.

---

## Conclusion

The DataRaum Context Engine collects **127+ metrics** across 5 pillars with a clear philosophy:

1. **Quality Context (40%):** ~50 metrics surfaced in quality context for LLM/API consumption
2. **AI Context (45%):** ~55 metrics provide rich context for AI interpretation
3. **Human Investigation (10%):** ~13 metrics support exploratory analysis
4. **Evidence (5%):** ~9 metrics provide supporting evidence

This architecture enables:
- **Context-focused quality output** through issues, flags, and raw metrics
- **Intelligent AI suggestions** through comprehensive formatted context
- **Human understanding** through evidence and examples
- **Privacy-preserving analysis** by providing context without raw data access

The quality context output (`DatasetQualityContext`) aggregates metrics and issues from all pillars into a structured format optimized for LLM consumption. Instead of computed scores, consumers receive:
- Raw metrics (null_ratio, outlier_ratio, etc.)
- Actionable flags (high_nulls, stale_data, benford_violation)
- Issues with evidence and source tracking
- Formatted analysis (multicollinearity, etc.)

**Key Insight:** More metrics ≠ better scores. The library intentionally separates **quality signals** (issues, flags) from **context metrics** (interpretive), enabling AI to understand data deeply and consumers to interpret quality within their own context.

---

## Appendix D: Future Work

See `docs/METRICS_BACKLOG.md` for planned metrics and features including:
- Shannon entropy metrics
- Ordering metrics (is_sorted, is_monotonic)
- Uniqueness/duplicate detection
- Additional formatters for statistical, temporal, topological domains
- API endpoints and MCP tools
