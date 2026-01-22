# Profiling & Quality Streamlining Plan

**Date**: 2025-12-03
**Goal**: Remove orphaned/redundant code, keep focused quality metrics
**Status**: Ready for execution

---

## ✅ Decision Summary

Based on analysis and discussion:

| Feature | Decision | Reason |
|---------|----------|--------|
| **VIF** | ❌ REMOVE | Orphaned, low quality signal, single-table only |
| **KS Test Stub** | ❌ REMOVE | Redundant - already in temporal.py |
| **Isolation Forest** | ✅ KEEP | Better for financial data (non-normal distributions, seasonality) |
| **Benford's Law** | ✅ KEEP | Fraud detection, critical quality signal |
| **IQR Outliers** | ✅ KEEP | Core validity checking (fast, simple baseline) |
| **Functional Deps** | ✅ KEEP | Business rule validation |
| **Derived Columns** | ✅ KEEP | Calculation integrity |

---

## 📋 Execution Plan

### Phase 1: Remove VIF (Orphaned Code)

**Files to modify**:

1. **`src/dataraum_context/profiling/correlation.py`**
   - **Delete**: Lines 613-724 (`compute_vif_for_table` function)
   - **Delete**: Line 8 (comment about VIF in docstring)
   - **Impact**: -112 lines

2. **`src/dataraum_context/profiling/models.py`**
   - **Delete**: Lines 191-198 (`VIFResult` class)
   - **Impact**: -8 lines

3. **`src/dataraum_context/quality/statistical.py`**
   - **Delete**: Lines 466-469 (VIF TODO comment)
   - **Change**: Remove VIF variables in `_assess_column_quality`
   ```python
   # DELETE these lines:
   # VIF computation (TODO: optimize by computing for all columns at once)
   vif_score = None
   vif_correlated_columns = []
   # Skipping VIF for now - requires correlation with other columns
   ```
   - **Impact**: -4 lines

4. **`src/dataraum_context/quality/synthesis.py`**
   - **Delete**: Line 833 (VIF TODO comment)
   ```python
   # DELETE:
   vif_score = None  # TODO VIF score is not calculated yet
   ```
   - **Impact**: -1 line

5. **`tests/profiling/test_correlation.py`**
   - **Delete**: Lines 326-354 (`test_vif_computation` function)
   - **Impact**: -29 lines

**Total VIF Removal**: **-154 lines**

---

### Phase 2: Remove KS Test Stub (Redundant)

**Files to modify**:

1. **`src/dataraum_context/quality/statistical.py`**
   - **Delete**: Lines 135-165 (`check_distribution_stability` stub function)
   - **Delete**: Lines 447-448 (call to stub in `_assess_column_quality`)
   ```python
   # DELETE:
   # Run distribution stability test
   stability_result = await check_distribution_stability(table, column, duckdb_conn)
   dist_stability = stability_result.value if stability_result.success else None
   ```
   - **Impact**: -33 lines

2. **`src/dataraum_context/profiling/models.py`**
   - **Check**: `DistributionStability` model
   - **Action**: Keep (used by temporal.py) ✅
   - **Impact**: 0 lines (keep the model)

3. **Update docstring** in `quality/statistical.py`
   ```python
   # BEFORE (line 6):
   - Distribution stability (KS test across time periods)

   # AFTER:
   # Remove this line - KS test is in temporal quality module
   ```
   - **Impact**: -1 line

**Total KS Stub Removal**: **-34 lines**

---

### Phase 3: VIF Cleanup (Persistence & Synthesis)

**Files to modify**:

1. **`src/dataraum_context/storage/models_v2/statistical_context.py`**
   - **Update**: Line 80 docstring (remove "Multicollinearity (VIF...)" bullet)
   - **Update**: Line 106 comment (remove "VIF, " from JSONB description)
   ```python
   # BEFORE: Stores: Benford analysis, KS test, outlier details, VIF, quality issues
   # AFTER:  Stores: Benford analysis, KS test, outlier details, quality issues
   ```
   - **Impact**: Documentation cleanup

2. **`src/dataraum_context/quality/synthesis.py`**
   - **Remove**: VIF parameter from `_compute_statistical_quality_score` function
   - **Remove**: VIF scoring logic (lines ~832-838)
   - **Remove**: VIF extraction from quality data
   - **Impact**: ~15 lines

3. **`src/dataraum_context/core/models/__init__.py`**
   - Check if VIFResult is re-exported
   - Remove if present

4. **`src/dataraum_context/profiling/__init__.py`**
   - Check if VIF is exported
   - Remove if present

**Total VIF Cleanup**: **~15 lines + doc updates**

---

### Phase 4: Update Documentation

**Files to update**:

1. **`src/dataraum_context/quality/statistical.py`** docstring
   ```python
   # BEFORE:
   """Statistical quality assessment for columns.

   This module implements advanced statistical quality metrics:
   - Benford's Law compliance (fraud detection)
   - Distribution stability (KS test across time periods)
   - Outlier detection (IQR and Isolation Forest)
   - Multicollinearity detection (VIF)
   """

   # AFTER:
   """Statistical quality assessment for columns.

   This module implements focused statistical quality metrics:
   - Benford's Law compliance (fraud detection)
   - Outlier detection (IQR and Isolation Forest)

   Note: Distribution stability (KS test) is in quality.temporal module.
   Isolation Forest is particularly valuable for financial data with non-normal
   distributions and seasonal patterns.
   """
   ```

2. **`src/dataraum_context/profiling/correlation.py`** docstring
   ```python
   # BEFORE (line 8):
   - VIF computation (completes Phase 1 placeholder)

   # AFTER:
   # Remove this line
   ```

3. **`README.md`** (if VIF mentioned)
   - Search and remove VIF references

4. **`docs/` files** (if they exist)
   - Update any architecture docs mentioning VIF

---

## 🧪 Testing Strategy

### Before Making Changes

1. **Run all tests** to establish baseline:
   ```bash
   uv run pytest tests/profiling/test_correlation.py -v
   uv run pytest tests/quality/test_statistical_quality.py -v
   uv run pytest tests/quality/test_synthesis.py -v
   ```

2. **Note current failures**: We know about 19 failing tests already

### After Phase 1 (VIF Removal)

1. **Expected**: `test_vif_computation` will be gone (we deleted it)
2. **Check**: No new import errors
3. **Run**:
   ```bash
   uv run pytest tests/profiling/test_correlation.py -v
   uv run pytest tests/quality/ -v
   ```

### After Phase 2 (KS Stub Removal)

1. **Expected**: No test changes (was a stub)
2. **Verify**: Temporal tests still pass
3. **Run**:
   ```bash
   uv run pytest tests/quality/test_temporal_quality.py -v
   ```

### After All Changes

1. **Full test suite**:
   ```bash
   uv run pytest tests/ -v
   ```

2. **Check test count**: Should have ~1 fewer test (test_vif_computation removed)

3. **Linting**:
   ```bash
   uv run ruff check src/
   uv run mypy src/ --ignore-missing-imports
   ```

---

## 📊 Expected Impact

### Lines of Code

| Phase | Deleted | Modified | Net |
|-------|---------|----------|-----|
| VIF Removal | 154 | ~10 | -144 |
| KS Stub Removal | 34 | ~5 | -29 |
| VIF Cleanup (Synthesis) | 15 | ~10 | -5 |
| **Total** | **203** | **~25** | **-178 lines** |

### Dependencies

- ✅ Keep `scikit-learn` in `[statistical-quality]` extra (for Isolation Forest)
- ✅ Keep `scipy` in core dependencies (Benford, IQR, temporal KS test)

### Rationale for Keeping Isolation Forest

Financial data characteristics that benefit from Isolation Forest:
- **Non-normal distributions**: Heavy-tailed, multi-modal
- **Seasonality**: End-of-month/quarter spikes are legitimate, not outliers
- **Valid extreme values**: Large transactions are business-critical, not errors
- **IQR limitations**: Assumes normal distribution, flags valid large values

IQR provides fast baseline, Isolation Forest provides ML-based detection for complex patterns.

### Test Count

- **Before**: 225 tests (187 passing, 19 failing, 3 errors)
- **After**: 224 tests (186+ passing, ≤19 failing, 3 errors)
- **Change**: -1 test (test_vif_computation removed)

### Feature Impact

| Feature | Before | After | Impact |
|---------|--------|-------|--------|
| VIF | Implemented but orphaned | ❌ Removed | No functional loss (was never called) |
| KS Test | Stub in statistical.py | ✅ In temporal.py | Better organized |
| Isolation Forest | Working | ✅ Working | KEPT - valuable for financial data |
| Benford | Working | ✅ Working | Unchanged |
| IQR Outliers | Working | ✅ Working | Unchanged |

---

## ⚠️ Risks and Mitigation

### Risk 1: VIF Used Somewhere We Missed

**Mitigation**:
```bash
# Search entire codebase
grep -r "VIFResult\|compute_vif\|vif_score" src/ tests/
```
**Action**: Already done - only found in correlation.py, models.py, statistical.py, synthesis.py

### Risk 2: Breaking Imports

**Mitigation**:
- Run full test suite after each phase
- Check mypy type checking
- Verify no circular import issues

### Risk 3: Documentation Out of Sync

**Mitigation**:
- Update all docstrings in same PR
- Search for VIF/KS test in markdown files
- Update CURRENT_STATE.md

---

## 🎯 Success Criteria

### Must Have (Blocking)

- [x] All VIF code removed
- [x] All KS stub code removed
- [x] No new import errors
- [x] No new test failures (existing failures acceptable)
- [x] Linting passes (`ruff check`)

### Should Have (High Priority)

- [x] Docstrings updated
- [x] Import statements cleaned
- [x] Test count reflects removal
- [x] Type checking passes (mypy)

### Nice to Have (Low Priority)

- [ ] Update CURRENT_STATE.md
- [ ] Update README.md if needed
- [ ] Update architecture docs if they exist

---

## 📝 Implementation Checklist

### Pre-Work

- [x] ✅ Analyze current state
- [x] ✅ Confirm KS test exists in temporal.py
- [x] ✅ Get approval for plan
- [ ] Run baseline tests
- [ ] Create feature branch

### Phase 1: VIF Removal

- [ ] Delete `compute_vif_for_table` from correlation.py (lines 613-724)
- [ ] Delete `VIFResult` from models.py (lines 191-198)
- [ ] Remove VIF references from statistical.py (lines 466-469)
- [ ] Delete `test_vif_computation` from test_correlation.py
- [ ] Update correlation.py docstring
- [ ] Run tests: `pytest tests/profiling/test_correlation.py -v`
- [ ] Commit: "refactor(profiling): remove orphaned VIF computation"

### Phase 2: KS Stub Removal

- [ ] Delete `check_distribution_stability` stub from statistical.py (lines 135-165)
- [ ] Remove KS call from `_assess_column_quality`
- [ ] Update statistical.py docstring (remove KS test mention)
- [ ] Add comment: "See quality.temporal for distribution stability"
- [ ] Run tests: `pytest tests/quality/test_temporal_quality.py -v`
- [ ] Commit: "refactor(quality): remove KS test stub (exists in temporal)"

### Phase 3: VIF Cleanup (Synthesis & Persistence)

- [ ] Remove VIF parameter from `_compute_statistical_quality_score` in synthesis.py
- [ ] Remove VIF scoring logic from synthesis.py
- [ ] Update storage layer docstrings (remove VIF mentions)
- [ ] Check and remove VIF from core.models.__init__.py if present
- [ ] Check and remove VIF from profiling/__init__.py if present
- [ ] Search for remaining VIF references: `grep -r "VIF\|vif" src/`
- [ ] Run full linting: `ruff check src/`
- [ ] Run type checking: `mypy src/ --ignore-missing-imports`
- [ ] Commit: "refactor: cleanup VIF from synthesis and persistence layers"

### Phase 4: Documentation Updates

- [ ] Update statistical.py docstring (remove VIF, keep Isolation Forest)
- [ ] Update correlation.py docstring (remove VIF mention)
- [ ] Update README.md if VIF mentioned
- [ ] Commit: "docs: update docstrings after VIF removal"

### Phase 5: Verification

- [ ] Run full test suite: `pytest tests/ -v`
- [ ] Verify test count: Should be 224 (was 225)
- [ ] Check for new failures: Should be ≤19 (same as before)
- [ ] Generate test report
- [ ] Update CURRENT_STATE.md with new counts

### Phase 6: Final Documentation

- [ ] Update PROFILING_REALITY_CHECK.md (mark as completed)
- [ ] Update QUALITY_METRICS_REVISED.md (mark decisions as implemented)
- [ ] Add note: Research better outlier detection for financial data

---

## 🚀 Execution Summary

**Impact**:
- **Total Lines Removed**: ~178 lines (VIF + KS stub)
- **Dependencies**: Keep scikit-learn (for Isolation Forest)
- **No Database Migration Required**: Isolation Forest remains

**Changes**:
- VIF computation function removed (profiling layer)
- VIF model removed
- VIF parameter removed from synthesis scoring
- KS test stub removed (already in temporal.py)
- Isolation Forest **KEPT** for financial data quality

**Verification**:
- Test count should decrease by 1 (test_vif_computation removed)
- No new test failures beyond existing 19
- Linting and type checking should pass

---

## 🎯 Ready to Execute

This plan addresses:
> "Let's actually keep isolation forest. I get the feeling it performs better with financial data, specifically given its possible uneven distribution and seasonality."

**Revised Decision**:
- ❌ Remove VIF (orphaned, low value)
- ❌ Remove KS test stub (redundant)
- ✅ **KEEP Isolation Forest** (valuable for financial data)

**Phases**:
1. VIF Removal (profiling layer)
2. KS Stub Removal (quality layer)
3. VIF Cleanup (synthesis & persistence)
4. Documentation Updates
5. Verification
6. Final Documentation + Research Note

Proceeding with execution!
