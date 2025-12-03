# Quality Metrics - Revised for Unknown Data Sources

**Date**: 2025-12-03
**Context**: Reconsidering through data quality transparency lens
**Goal**: Give LLM enough signals to filter/trust data from unknown sources

## The User's Critical Insight

**My Initial Mistake**: Evaluated metrics only for "query generation value"

**The Reality**:
- We receive data of **unknown quality**
- Users need **transparency** on quality issues
- LLM needs to **filter/trust** data accordingly
- But we want a **focused set** of metrics (not kitchen sink)

---

## Data Quality Dimensions (Industry Standard)

For unknown data sources, we need to assess:

| Dimension | Question | Impact on Analysis |
|-----------|----------|-------------------|
| **Completeness** | How much data is missing? | Missing values bias results |
| **Consistency** | Do business rules hold? | Violations indicate process failures |
| **Accuracy** | Are values plausible? | Outliers/anomalies distort aggregates |
| **Validity** | Do values match expectations? | Invalid data corrupts analysis |
| **Timeliness** | Is data current and complete? | Gaps/delays reduce reliability |
| **Integrity** | Are relationships intact? | Broken links lose context |

---

## Revised Metric Evaluation (Quality Transparency Lens)

### ✅ STRONG KEEP - High Quality Signal

#### 1. Benford's Law ⭐⭐⭐
**Quality Dimension**: Accuracy + Fraud Detection

**What it detects**: Data fabrication, manipulation, or unnatural generation
- Expected: Natural amounts follow logarithmic first-digit distribution
- Violation: "These amounts might be fabricated"

**LLM Filtering Decision**:
```
❌ CRITICAL: "revenue" column failed Benford's Law (p=0.001)
   → LLM: "Warning: Revenue data may be fabricated. Consider excluding from financial analysis."
```

**Transparency Value**: ⭐⭐⭐ Users need to know if data is suspicious
**Dependency**: scipy (core dependency) ✅
**Verdict**: **KEEP** - Essential fraud/manipulation detector

---

#### 2. Functional Dependencies ⭐⭐⭐
**Quality Dimension**: Consistency + Integrity

**What it detects**: Business rule violations
- Expected: `customer_id → email` (one ID = one email)
- Violation: "Customer has multiple emails" (data quality issue)

**LLM Filtering Decision**:
```
⚠️ WARNING: customer_id → email rule violated in 15% of rows
   → LLM: "Customer data has inconsistencies. Use with caution for customer-level analysis."
```

**Transparency Value**: ⭐⭐⭐ Critical for understanding data reliability
**Dependency**: None (pure SQL) ✅
**Verdict**: **KEEP** - Detects data quality issues directly

---

#### 3. IQR Outlier Detection ⭐⭐⭐
**Quality Dimension**: Accuracy + Validity

**What it detects**: Statistical anomalies that might be errors
- Expected: Values within reasonable range
- Violation: "5% of amounts are extreme outliers"

**LLM Filtering Decision**:
```
⚠️ OUTLIERS: 50 transactions (5%) are statistical outliers
   Samples: $1,000,000 when average is $500
   → LLM: "Consider excluding outliers from trend analysis or investigate separately."
```

**Transparency Value**: ⭐⭐⭐ Users need to see anomalous data
**Dependency**: None (DuckDB + scipy) ✅
**Verdict**: **KEEP** - Simple, effective quality indicator

---

#### 4. Derived Column Validation ⭐⭐
**Quality Dimension**: Integrity + Accuracy

**What it detects**: Calculation errors, data entry mistakes
- Expected: `total = price * quantity`
- Violation: "10% of rows have incorrect totals"

**LLM Filtering Decision**:
```
⚠️ CALCULATION ERROR: total ≠ price * quantity in 50 rows
   → LLM: "Data contains calculation errors. Recalculate totals in queries."
```

**Transparency Value**: ⭐⭐ Good for catching data entry errors
**Dependency**: None (pure SQL) ✅
**Verdict**: **KEEP** - Detects real quality issues

---

### 🤔 RECONSIDER - Marginal Value for Unknown Data

#### 5. Isolation Forest ⭐⭐
**Quality Dimension**: Accuracy (Anomaly Detection)

**What it detects**: Complex multivariate anomalies
- Better than IQR for subtle patterns
- ML-based, adapts to data distribution

**LLM Filtering Decision**:
```
⚠️ ANOMALIES: 25 records flagged as anomalous by ML model
   → LLM: "These records show unusual patterns. Investigate before using."
```

**Arguments FOR keeping**:
- ✅ Better anomaly detection than IQR
- ✅ Unknown data might have subtle issues
- ✅ Provides anomaly scores (not just binary)

**Arguments AGAINST keeping**:
- ❌ Heavy dependency (scikit-learn ~50MB)
- ❌ Single-column analysis (misses multivariate issues)
- ❌ Marginal improvement over IQR for simple cases
- ❌ Graceful degradation already implemented (optional)

**Revised Verdict**: **KEEP BUT OPTIONAL**
- Make sklearn truly optional (already is)
- Document that it's "enhanced quality detection"
- Falls back to IQR if sklearn not installed
- **Value**: Medium quality signal, worth the optional dependency

---

### ❌ REMOVE - Low Quality Signal

#### 6. VIF (Variance Inflation Factor) ⭐
**Quality Dimension**: ??? (Not really a quality dimension)

**What it detects**: Redundant information (multicollinearity)
- Measures: "Column A predicts Column B"
- Example: "TotalRevenue and Revenue are highly correlated"

**Quality Signal**: ❌ Weak
- Redundancy ≠ Bad quality
- Having multiple related columns is normal (revenue, cost, profit)
- Doesn't indicate errors, fraud, or problems

**LLM Filtering Decision**:
```
ℹ️ INFO: revenue and total_revenue have VIF > 10 (redundant)
   → LLM: "Both columns contain similar information"
   → Impact: None - doesn't affect trustworthiness
```

**Transparency Value**: ⭐ Low - users don't care about statistical redundancy
**Dependency**: scikit-learn (heavy) ❌
**Integration**: Not even wired up ❌

**Verdict**: **REMOVE**
- Wrong tool - measures redundancy, not quality
- Orphaned code, never integrated
- No actionable quality signal
- Save ~150 lines + remove from tests

---

### ❌ STUB - Not Implemented

#### 7. KS Test (Distribution Stability) ⭐⭐ (If implemented)
**Quality Dimension**: Timeliness + Consistency

**What it WOULD detect**: Sudden data distribution changes
- Expected: Revenue distribution stable over time
- Violation: "Recent data has different distribution"

**Potential LLM Filtering Decision**:
```
⚠️ DRIFT: Revenue distribution changed significantly in last week
   → LLM: "Recent data may have quality issues. Verify before using in trends."
```

**Current Status**: Just returns `None` (stub)

**Arguments FOR implementing**:
- ✅ Detects temporal data quality degradation
- ✅ Important for time-series analysis
- ✅ No heavy dependencies (scipy)

**Arguments AGAINST**:
- ❌ Not implemented (just a stub)
- ❌ Requires temporal column detection (dependency)
- ❌ Better handled by temporal enrichment module

**Verdict**: **REMOVE STUB FOR NOW**
- Delete the stub function (~30 lines)
- If we want this, implement properly later
- Temporal enrichment already handles time-based quality

---

## Gaps in Quality Coverage

### What We're Missing for Unknown Data Quality

#### 🔴 Gap 1: Referential Integrity
**Missing**: FK validation across tables
- **Need**: Detect orphaned records
- **Example**: `order.customer_id` points to non-existent customer
- **LLM Signal**: "10% of orders have invalid customer references"
- **Priority**: HIGH - critical for join quality

#### 🔴 Gap 2: Completeness Metrics
**Status**: Partially covered (null_ratio in StatisticalProfile)
**Missing**:
- Required field validation
- Cross-column completeness ("if A filled, B must be filled")
- Temporal completeness gaps (handled by temporal enrichment)

#### 🟡 Gap 3: Domain-Specific Validation
**Status**: Partially covered (financial.py has sign conventions)
**Missing**:
- Configurable value range checks
- Format validation (beyond patterns)
- Cross-field validation rules

#### 🟡 Gap 4: Duplicate Detection
**Status**: Basic (distinct_count in profiles)
**Missing**:
- Fuzzy duplicate detection
- Composite key uniqueness

---

## Focused Quality Metric Set - Revised Recommendation

### ✅ Core Metrics (Must Have)

| Metric | Quality Dimension | Dependency | Lines | Value |
|--------|------------------|------------|-------|-------|
| **Benford's Law** | Accuracy/Fraud | scipy | ~80 | ⭐⭐⭐ |
| **Functional Dependencies** | Consistency | None | ~130 | ⭐⭐⭐ |
| **IQR Outliers** | Validity | scipy | ~110 | ⭐⭐⭐ |
| **Derived Column Validation** | Integrity | None | ~180 | ⭐⭐ |
| **Null/Completeness** | Completeness | None | (exists) | ⭐⭐⭐ |

**Total**: ~500 lines, no heavy dependencies

### ⚠️ Optional Enhanced Metrics

| Metric | Quality Dimension | Dependency | Lines | Value |
|--------|------------------|------------|-------|-------|
| **Isolation Forest** | Accuracy | sklearn | ~80 | ⭐⭐ |

**Condition**: Only if sklearn installed (graceful degradation)
**Rationale**: Better anomaly detection, worth optional dependency

### ❌ Remove

| Metric | Reason | Lines Saved |
|--------|--------|-------------|
| **VIF** | Measures redundancy, not quality | ~150 |
| **KS Test Stub** | Not implemented, temporal does this | ~30 |

**Total savings**: ~180 lines

### 📝 Add (Future)

| Metric | Priority | Effort | Value |
|--------|----------|--------|-------|
| **Referential Integrity** | HIGH | Medium | ⭐⭐⭐ |
| **Required Field Validation** | MEDIUM | Low | ⭐⭐ |
| **Domain Value Ranges** | LOW | Medium | ⭐⭐ |

---

## LLM Context - What Quality Signals Enable

### Scenario: Unknown CSV from Client

**Without Quality Metrics**:
```
LLM: "Here's your revenue analysis based on the data provided."
```

**With Quality Metrics**:
```
LLM: "⚠️ Data Quality Issues Detected:

1. CRITICAL: Revenue column failed Benford's Law (p=0.001)
   - First digit distribution is unnatural
   - Possible data fabrication or manual entry
   - Recommendation: Verify source before using for financial analysis

2. WARNING: 150 transactions (5%) are statistical outliers
   - Values range from $100K to $5M when average is $10K
   - Recommendation: Review these separately or exclude from aggregates

3. INFO: customer_id → email rule violated in 12% of rows
   - Some customers have multiple email addresses
   - Recommendation: Use latest email or deduplicate customers

Based on these quality issues, I'll provide analysis WITH CAVEATS..."
```

**Impact**: LLM can make informed decisions about data trustworthiness

---

## Revised Recommendations

### 🎯 Final Decision Matrix

| Metric | Keep? | Reason |
|--------|-------|--------|
| Benford's Law | ✅ YES | Critical fraud detection |
| IQR Outliers | ✅ YES | Essential validity check |
| Isolation Forest | ✅ YES (optional) | Enhanced detection, graceful degradation |
| Functional Dependencies | ✅ YES | Business rule validation |
| Derived Column Validation | ✅ YES | Calculation integrity |
| Null/Completeness | ✅ YES | Already have |
| **VIF** | ❌ **REMOVE** | Not a quality signal |
| **KS Test Stub** | ❌ **REMOVE** | Not implemented |

### 📊 Impact Summary

**If we remove VIF + KS stub**:
- Lines removed: ~180
- Dependencies simplified: No change (sklearn already optional)
- Quality coverage: No loss (they don't assess quality)
- Focused set: ✅ Better (removes noise)

**If we keep Isolation Forest (optional)**:
- Quality signal: Improved anomaly detection
- Dependency: sklearn (already optional, graceful degradation)
- Trade-off: Worth it for unknown data sources

---

## Implementation Strategy

### Phase 1: Cleanup (This Sprint)
1. ✅ Remove VIF function and test (~150 lines)
2. ✅ Remove KS test stub (~30 lines)
3. ✅ Keep Isolation Forest but ensure sklearn is truly optional
4. ✅ Document quality metrics in user-facing docs

### Phase 2: Fill Gaps (Next Sprint)
1. Add referential integrity checks
2. Enhance completeness metrics
3. Add configurable domain validation

### Phase 3: Context Assembly
1. Include quality metrics in ContextDocument
2. Format for LLM consumption
3. Add quality-based filtering suggestions

---

## Questions Back to You

1. **Isolation Forest**: Keep as optional enhancement? Or remove to simplify?
   - My lean: Keep (already graceful, useful for unknown data)

2. **Priority for referential integrity**? This seems critical for unknown data
   - My lean: High priority gap to fill

3. **Quality API vs Query Generation**: Should we separate these concerns?
   - Quality API: All metrics visible
   - Query Generation: Only use functional deps, derived columns, relationships

**My revised take**: You're right - for unknown data sources, we need robust quality assessment. Remove VIF (not quality-related), keep the rest including Isolation Forest (optional). Focus on transparency and LLM filtering decisions.

What do you think?
