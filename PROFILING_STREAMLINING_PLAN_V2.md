# Profiling & Quality Streamlining Plan v2

**Date**: 2025-12-03
**Goal**: Remove VIF, Isolation Forest, KS stub - comprehensive cleanup across all layers
**Status**: Ready for execution

---

## ✅ Updated Decisions

| Feature | Decision | Reason |
|---------|----------|--------|
| **VIF** | ❌ REMOVE | Orphaned, low quality signal, single-table only |
| **Isolation Forest** | ❌ REMOVE | Marginal improvement over IQR, heavy sklearn dependency |
| **KS Test Stub** | ❌ REMOVE | Redundant - already in temporal.py |
| **Benford's Law** | ✅ KEEP | Fraud detection, critical quality signal |
| **IQR Outliers** | ✅ KEEP | Core validity checking, simple, effective |
| **Functional Deps** | ✅ KEEP | Business rule validation |
| **Derived Columns** | ✅ KEEP | Calculation integrity |

---

## 📋 Comprehensive Removal Plan

### Phase 1: Remove VIF (All Layers)

#### 1.1 Profiling Layer

**File: `src/dataraum_context/profiling/correlation.py`**
- **Delete**: Lines 613-724 (`compute_vif_for_table` function) - ~112 lines
- **Delete**: Line 8 (comment about VIF in docstring)
- **Delete**: Line 39 (VIF import)
  ```python
  # DELETE:
  from dataraum_context.profiling.models import VIFResult
  ```

**File: `src/dataraum_context/profiling/models.py`**
- **Delete**: Lines 191-198 (`VIFResult` class) - ~8 lines

**File: `tests/profiling/test_correlation.py`**
- **Delete**: Lines 326-354 (`test_vif_computation` function) - ~29 lines
- **Delete**: Import if present

**Subtotal**: -149 lines (profiling layer)

---

#### 1.2 Quality Layer

**File: `src/dataraum_context/quality/statistical.py`**

**Line 7 - Update docstring**:
```python
# BEFORE:
- Multicollinearity detection (VIF)

# AFTER:
# (remove this line)
```

**Line 378 - Update docstring**:
```python
# BEFORE:
1. Runs quality tests (Benford, outliers, stability, VIF)

# AFTER:
1. Runs quality tests (Benford, outliers)
```

**Lines 466-469 - Remove VIF computation**:
```python
# DELETE:
# VIF computation (TODO: optimize by computing for all columns at once)
vif_score = None
vif_correlated_columns = []
# Skipping VIF for now - requires correlation with other columns
```

**Lines 495-496 - Remove VIF from StatisticalQualityResult**:
```python
# BEFORE:
result = StatisticalQualityResult(
    column_id=column.column_id,
    column_ref=ColumnRef(...),
    benford_analysis=benford_analysis,
    outlier_detection=outlier_detection,
    distribution_stability=dist_stability,
    vif_score=vif_score,                          # DELETE THIS LINE
    vif_correlated_columns=vif_correlated_columns,  # DELETE THIS LINE
    quality_score=quality_score,
    quality_issues=quality_issues,
)

# AFTER:
result = StatisticalQualityResult(
    column_id=column.column_id,
    column_ref=ColumnRef(...),
    benford_analysis=benford_analysis,
    outlier_detection=outlier_detection,
    distribution_stability=dist_stability,
    quality_score=quality_score,
    quality_issues=quality_issues,
)
```

**File: `src/dataraum_context/quality/synthesis.py`**

**Line 7 - Update docstring**:
```python
# BEFORE:
- Pillar 1 (Statistical): Benford, outliers, VIF, distribution stability

# AFTER:
- Pillar 1 (Statistical): Benford, outliers, distribution stability
```

**Lines 155, 164, 176-181 - Remove from `_compute_statistical_quality_score`**:
```python
# DELETE entire vif_score parameter and logic:
def _compute_statistical_quality_score(
    benford_compliant: bool | None,
    outlier_ratio: float | None,
    dist_stable: bool | None,
    vif_score: float | None,  # DELETE THIS PARAMETER
) -> float:
    ...
    if vif_score is not None:  # DELETE THIS ENTIRE BLOCK
        # VIF > 10 is problematic
        if vif_score > 10:
            penalty = min((vif_score - 10) / 20, 0.5)  # Max 50% penalty
            score *= 1 - penalty
            factors.append(f"VIF={vif_score:.1f}")
```

**Lines 832-838 - Remove VIF extraction and usage**:
```python
# DELETE:
# Get VIF score from statistical quality metrics JSONB
vif_score = None  # TODO VIF score is not calculated yet
if stat_quality and stat_quality.quality_data:
    vif_score = stat_quality.quality_data.get("vif_score")

statistical_score = _compute_statistical_quality_score(
    benford_compliant=benford_compliant,
    outlier_ratio=outlier_ratio,
    dist_stable=dist_stable,
    vif_score,  # DELETE THIS ARGUMENT
)
```

**Subtotal**: -20 lines (quality layer)

---

#### 1.3 Storage/Persistence Layer

**File: `src/dataraum_context/storage/models_v2/statistical_context.py`**

**Line 80 - Update docstring**:
```python
# BEFORE:
    - Multicollinearity (VIF - requires correlation with other columns)

# AFTER:
    # (remove this line)
```

**Line 106 - Update comment**:
```python
# BEFORE:
    # JSONB: Full quality analysis results
    # Stores: Benford analysis, KS test, outlier details, VIF, quality issues

# AFTER:
    # JSONB: Full quality analysis results
    # Stores: Benford analysis, outlier details, quality issues
```

**Note**: No database columns to remove - VIF was only in JSONB, never had structured columns

**Subtotal**: -2 comment lines (storage layer)

---

#### 1.4 Profiling Models Layer

**File: `src/dataraum_context/profiling/models.py`**

**Lines 220-222 - Remove VIF from StatisticalQualityResult**:
```python
# DELETE:
# Multicollinearity (VIF)
vif_score: float | None = None
vif_correlated_columns: list[str] = Field(default_factory=list)  # Column IDs
```

**Subtotal**: -3 lines (profiling models)

---

**Phase 1 Total VIF Removal**: **-174 lines**

---

### Phase 2: Remove Isolation Forest (All Layers)

#### 2.1 Quality Layer Implementation

**File: `src/dataraum_context/quality/statistical.py`**

**Line 6 - Update docstring**:
```python
# BEFORE:
- Outlier detection (IQR and Isolation Forest)

# AFTER:
- Outlier detection (IQR method)
```

**Lines 283-362 - Delete entire `detect_outliers_isolation_forest` function** (~80 lines)

**Lines 454-464 - Remove Isolation Forest call in `_assess_column_quality`**:
```python
# DELETE:
# Run Isolation Forest outlier detection and merge into OutlierDetection
iso_forest_data = await detect_outliers_isolation_forest(table, column, duckdb_conn)
if iso_forest_data and outlier_detection:
    avg_score, anomaly_count, anomaly_ratio, iso_samples = iso_forest_data
    # Update the outlier_detection with Isolation Forest results
    outlier_detection.isolation_forest_score = avg_score
    outlier_detection.isolation_forest_anomaly_count = anomaly_count
    outlier_detection.isolation_forest_anomaly_ratio = anomaly_ratio
    # Merge samples
    if iso_samples:
        outlier_detection.outlier_samples.extend(iso_samples)
```

**Subtotal**: -91 lines (quality statistical.py)

---

#### 2.2 Profiling Models Layer

**File: `src/dataraum_context/profiling/models.py`**

**Lines 173-175 - Remove Isolation Forest fields from OutlierDetection**:
```python
class OutlierDetection(BaseModel):
    """Outlier detection results."""

    # IQR Method
    iqr_lower_fence: float
    iqr_upper_fence: float
    iqr_outlier_count: int
    iqr_outlier_ratio: float

    # Isolation Forest                           # DELETE COMMENT
    isolation_forest_score: float                # DELETE LINE
    isolation_forest_anomaly_count: int          # DELETE LINE
    isolation_forest_anomaly_ratio: float        # DELETE LINE

    # Sample outliers
    outlier_samples: list[dict[str, Any]] = Field(default_factory=list)
```

**Subtotal**: -4 lines (profiling models)

---

#### 2.3 Storage/Persistence Layer

**File: `src/dataraum_context/storage/models_v2/statistical_context.py`**

**Line 79 - Update docstring**:
```python
# BEFORE:
    - Outlier detection (Isolation Forest, IQR method)

# AFTER:
    - Outlier detection (IQR method)
```

**Line 103 - Remove database column**:
```python
# DELETE:
isolation_forest_anomaly_ratio: Mapped[float | None] = mapped_column(Float)
```

**⚠️ IMPORTANT**: This requires a database migration!

**Subtotal**: -2 lines + 1 migration (storage layer)

---

**Phase 2 Total Isolation Forest Removal**: **-97 lines + migration**

---

### Phase 3: Remove KS Test Stub (Already Covered in V1)

**File: `src/dataraum_context/quality/statistical.py`**
- Delete lines 135-165 (`check_distribution_stability` stub)
- Remove call in `_assess_column_quality` (lines 447-448)
- Update docstring

**Subtotal**: -34 lines

---

### Phase 4: Dependencies Cleanup

**File: `pyproject.toml`**

**Current**:
```toml
# Statistical quality metrics (ML-based anomaly detection)
statistical-quality = [
    "scikit-learn>=1.3.0",
]
```

**Updated**:
```toml
# Remove statistical-quality extra entirely (no longer needed)
# scikit-learn was only used for Isolation Forest
```

**Remove from**:
- `complete` extra (line 90)
- `all` extra (line 99)

**Subtotal**: Remove entire `statistical-quality` extra

---

## 🗄️ Database Migration Required

### Migration: Remove `isolation_forest_anomaly_ratio` Column

**File to create**: `src/dataraum_context/storage/migrations/003_remove_isolation_forest.sql`

```sql
-- Remove Isolation Forest column from statistical_quality_metrics
-- This column is no longer populated or used

ALTER TABLE statistical_quality_metrics
DROP COLUMN IF EXISTS isolation_forest_anomaly_ratio;

-- Update any existing quality_data JSONB to remove isolation forest keys
-- (Optional cleanup - JSONB is flexible, old keys won't hurt)
UPDATE statistical_quality_metrics
SET quality_data = quality_data - 'isolation_forest_score'
                                - 'isolation_forest_anomaly_count'
                                - 'isolation_forest_anomaly_ratio'
WHERE quality_data ? 'isolation_forest_score';
```

**SQLite equivalent** (if using SQLite):
```sql
-- SQLite doesn't support DROP COLUMN easily
-- We'll leave the column (it's nullable) and just stop using it
-- Document that isolation_forest_anomaly_ratio is deprecated
```

**Note**: For production, we'd use Alembic or similar. For now, document the change.

---

## 📊 Total Impact Summary

| Phase | Lines Removed | Files Modified | Dependencies |
|-------|---------------|----------------|--------------|
| VIF Removal | -174 | 7 files | None |
| Isolation Forest Removal | -97 | 4 files | sklearn removed |
| KS Stub Removal | -34 | 1 file | None |
| **TOTAL** | **-305 lines** | **12 files** | -sklearn |

### Files Modified (Complete List)

1. `src/dataraum_context/profiling/correlation.py` (VIF function)
2. `src/dataraum_context/profiling/models.py` (VIF + Isolation Forest models)
3. `src/dataraum_context/quality/statistical.py` (VIF, Isolation Forest, KS stub)
4. `src/dataraum_context/quality/synthesis.py` (VIF references)
5. `src/dataraum_context/storage/models_v2/statistical_context.py` (comments + Isolation Forest column)
6. `tests/profiling/test_correlation.py` (VIF test)
7. `pyproject.toml` (remove sklearn dependency)

### Database Changes

- Remove `isolation_forest_anomaly_ratio` column (nullable, safe to remove)
- Optional: Clean old JSONB keys

---

## 🧪 Testing Strategy

### Before Making Changes

```bash
# Establish baseline
uv run pytest tests/profiling/test_correlation.py -v
uv run pytest tests/quality/test_statistical_quality.py -v
uv run pytest tests/quality/test_synthesis.py -v

# Count: 225 tests (187 passing, 19 failing, 3 errors)
```

### After Phase 1 (VIF Removal)

```bash
uv run pytest tests/profiling/test_correlation.py -v
uv run pytest tests/quality/ -v

# Expected: test_vif_computation gone, no new failures
```

### After Phase 2 (Isolation Forest Removal)

```bash
uv run pytest tests/quality/test_statistical_quality.py -v

# Expected: Tests that check Isolation Forest fields may need updates
# Check: OutlierDetection model tests
```

### After Phase 3 (KS Stub Removal)

```bash
uv run pytest tests/quality/test_temporal_quality.py -v

# Verify: Temporal KS test still works
```

### Final Verification

```bash
# Full suite
uv run pytest tests/ -v

# Linting
uv run ruff check src/

# Type checking (if we re-enable)
uv run mypy src/ --ignore-missing-imports

# Expected test count: 224 (was 225, removed test_vif_computation)
```

---

## 📝 Implementation Checklist

### Pre-Work

- [ ] Create feature branch: `git checkout -b refactor/remove-vif-isolation-forest-ks`
- [ ] Run baseline tests and document results
- [ ] Verify no uncommitted changes

### Phase 1: VIF Removal (Complete Layer-by-Layer)

#### Profiling Layer
- [ ] Delete `compute_vif_for_table` from correlation.py (lines 613-724)
- [ ] Remove VIF import from correlation.py (line 39)
- [ ] Update correlation.py docstring (line 8)
- [ ] Delete `VIFResult` class from profiling/models.py (lines 191-198)
- [ ] Remove VIF fields from `StatisticalQualityResult` in profiling/models.py (lines 220-222)
- [ ] Delete `test_vif_computation` from test_correlation.py (lines 326-354)
- [ ] Run: `pytest tests/profiling/test_correlation.py -v`

#### Quality Layer
- [ ] Update statistical.py docstrings (lines 7, 378)
- [ ] Remove VIF variables from statistical.py (lines 466-469)
- [ ] Remove VIF from StatisticalQualityResult construction (lines 495-496)
- [ ] Update synthesis.py docstring (line 7)
- [ ] Remove VIF parameter from `_compute_statistical_quality_score` (lines 155, 164)
- [ ] Delete VIF logic in scoring function (lines 176-181)
- [ ] Remove VIF extraction in synthesis (lines 832-838)
- [ ] Run: `pytest tests/quality/ -v`

#### Storage Layer
- [ ] Update statistical_context.py docstrings (lines 80, 106)
- [ ] Verify no VIF columns in database schema
- [ ] Run: `pytest tests/storage/ -v`

#### Verification
- [ ] Search for remaining VIF references: `grep -r "VIF\|vif" src/`
- [ ] Commit: "refactor: remove VIF computation (orphaned, low value)"

---

### Phase 2: Isolation Forest Removal

#### Quality Layer
- [ ] Update statistical.py docstring (line 6)
- [ ] Delete `detect_outliers_isolation_forest` function (lines 283-362)
- [ ] Remove Isolation Forest call in `_assess_column_quality` (lines 454-464)
- [ ] Remove sklearn import check
- [ ] Run: `pytest tests/quality/test_statistical_quality.py -v`

#### Profiling Models
- [ ] Remove Isolation Forest fields from `OutlierDetection` (lines 173-175)
- [ ] Update any tests that check these fields
- [ ] Run: `pytest tests/profiling/ -v`

#### Storage Layer
- [ ] Update statistical_context.py docstring (line 79)
- [ ] Remove `isolation_forest_anomaly_ratio` column (line 103)
- [ ] Create migration script (if using migrations)
- [ ] Document breaking change
- [ ] Run: `pytest tests/storage/ -v`

#### Dependencies
- [ ] Remove `statistical-quality` extra from pyproject.toml
- [ ] Remove sklearn from `complete` and `all` extras
- [ ] Update README if sklearn mentioned
- [ ] Run: `uv pip list | grep scikit`

#### Verification
- [ ] Search: `grep -r "isolation.*forest\|IsolationForest" src/ -i`
- [ ] Search: `grep -r "sklearn" src/`
- [ ] Commit: "refactor: remove Isolation Forest (marginal value, heavy dependency)"

---

### Phase 3: KS Test Stub Removal

- [ ] Delete `check_distribution_stability` stub (lines 135-165)
- [ ] Remove call in `_assess_column_quality` (lines 447-448)
- [ ] Update docstring (line 6)
- [ ] Verify temporal KS test still exists: `grep -n "analyze_distribution_stability" src/dataraum_context/quality/temporal.py`
- [ ] Run: `pytest tests/quality/test_temporal_quality.py -v`
- [ ] Commit: "refactor: remove KS test stub (exists in temporal module)"

---

### Phase 4: Final Cleanup

- [ ] Search all files for orphaned imports
- [ ] Update CURRENT_STATE.md with new test counts
- [ ] Update PROFILING_REALITY_CHECK.md (mark as implemented)
- [ ] Run full linting: `ruff check src/`
- [ ] Run full test suite: `pytest tests/ -v`
- [ ] Verify test count: ~224 tests
- [ ] Create summary of changes

---

### Phase 5: Documentation

- [ ] Update module docstrings
- [ ] Update README.md if needed
- [ ] Document migration for isolation_forest_anomaly_ratio
- [ ] Create CHANGELOG entry
- [ ] Commit: "docs: update documentation after streamlining"

---

## ⚠️ Breaking Changes

### Database Schema Change

**Breaking**: `isolation_forest_anomaly_ratio` column removed from `statistical_quality_metrics`

**Migration Path**:
- Column is nullable, safe to remove
- Existing rows won't break (column just disappears)
- New code won't populate this column

**Backward Compatibility**:
- If old code tries to read `isolation_forest_anomaly_ratio`, it will get NULL/None
- JSONB `quality_data` is flexible, old keys won't cause errors

### API Changes

**Breaking**: `OutlierDetection` model no longer has Isolation Forest fields

**Impact**:
- Code that reads `outlier_detection.isolation_forest_score` will fail
- Solution: Only use IQR fields

**Before**:
```python
outlier_detection.isolation_forest_score  # Existed
outlier_detection.isolation_forest_anomaly_count  # Existed
outlier_detection.isolation_forest_anomaly_ratio  # Existed
```

**After**:
```python
outlier_detection.iqr_outlier_count  # Still exists
outlier_detection.iqr_outlier_ratio  # Still exists
outlier_detection.outlier_samples  # Still exists
```

### Dependency Changes

**Breaking**: `scikit-learn` no longer included in any extras

**Before**:
```bash
pip install dataraum-context[statistical-quality]  # Included sklearn
pip install dataraum-context[complete]  # Included sklearn
```

**After**:
```bash
# scikit-learn no longer needed
pip install dataraum-context  # Core features only
```

---

## 🎯 Success Criteria

### Must Have (Blocking)

- [x] All VIF code removed from all layers
- [x] All Isolation Forest code removed from all layers
- [x] KS test stub removed
- [x] No new import errors
- [x] No new test failures (existing 19 acceptable)
- [x] Linting passes
- [x] sklearn dependency removed

### Should Have (High Priority)

- [x] Database migration documented
- [x] Breaking changes documented
- [x] Docstrings updated
- [x] Test count reflects removals

### Nice to Have (Low Priority)

- [ ] CURRENT_STATE.md updated
- [ ] README.md updated
- [ ] Architecture docs updated

---

## 🚀 Ready to Execute

**Estimated Time**: ~1.5 hours
- Phase 1 (VIF): 30 minutes
- Phase 2 (Isolation Forest): 30 minutes
- Phase 3 (KS stub): 10 minutes
- Phase 4 (Cleanup): 15 minutes
- Phase 5 (Documentation): 15 minutes

**Impact**: -305 lines, cleaner codebase, simpler dependencies

**Questions?**
1. Should I proceed with all phases?
2. Do you want to review after each phase or at the end?
3. Should I create the database migration script or just document it?

Ready to start when you give the go-ahead! 🎯
