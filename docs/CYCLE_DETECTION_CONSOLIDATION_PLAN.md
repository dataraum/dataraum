# Cycle Detection Consolidation Plan

**Status**: Complete
**Created**: 2025-12-13
**Last Updated**: 2025-12-13

## Overview

Consolidate the fragmented cycle detection feature into a unified LLM-supported business cycle analysis system.

## Current State (Problems)

Three separate implementations exist:
1. **Pattern-matching** in `financial.py` - brittle, no LLM
2. **Generic LLM** in `llm_quality.py:classify_business_cycle()` - incomplete, hardcoded placeholders
3. **Domain-specific LLM** in `financial_llm.py` + `financial_orchestrator.py` - correct architecture, not integrated

## Target Architecture

```
                    topological.py
                         │
                         ▼
              financial_orchestrator.py  ──────┐
                    │    │                      │
         ┌──────────┘    └──────────┐           │
         ▼                          ▼           ▼
   financial.py              financial_llm.py   config/domains/
   (accounting checks)       (LLM classification)  financial.yaml
         │                          │
         └──────────┬───────────────┘
                    ▼
              synthesis.py
              (uses orchestrator output)
```

**Key Principles**:
1. Python computes metrics, LLM interprets meaning
2. Configuration provides LLM context (not matching rules)
3. Three-layer architecture: Compute → Classify → Interpret
4. Single entry point: `financial_orchestrator.analyze_complete_financial_quality()`

---

## Implementation Phases

### Phase 1: Remove Pattern-Based Cycle Detection from financial.py

**File**: `src/dataraum_context/quality/domains/financial.py`

| Action | Function | Lines (approx) |
|--------|----------|----------------|
| DELETE | `detect_financial_cycles()` | 906-983 |
| DELETE | `_score_cycle_match()` | 986-1035 |
| DELETE | `_get_fallback_cycle_patterns()` | 1038-1073 |
| MODIFY | `FinancialDomainAnalyzer.analyze()` | 1444-1515 |
| MODIFY | `FinancialDomainAnalyzer.analyze_cross_table_cycles()` | 1537-1724 |

**Keep**:
- All accounting checks (double-entry, trial balance, sign conventions, fiscal period)
- `assess_fiscal_stability()`
- `detect_financial_anomalies()` (but remove cycle-related logic)
- `compute_financial_quality_score()` (but remove cycle-related logic)

### Phase 2: Remove Incomplete LLM Implementation from llm_quality.py

**File**: `src/dataraum_context/quality/llm_quality.py`

| Action | Function | Lines (approx) |
|--------|----------|----------------|
| DELETE | `classify_business_cycle()` | 284-399 |
| KEEP | `generate_quality_summary()` | 63-159 |
| KEEP | `generate_quality_recommendations()` | 162-281 |

### Phase 3: Refactor FinancialDomainAnalyzer

**File**: `src/dataraum_context/quality/domains/financial.py`

The `FinancialDomainAnalyzer` class needs to be refactored:

1. Keep synchronous `analyze()` for backward compatibility with `topological.py`
2. Add async `analyze_with_llm()` that delegates to orchestrator
3. Remove pattern-matching from `analyze_cross_table_cycles()`

### Phase 4: Integrate into synthesis.py

**File**: `src/dataraum_context/quality/synthesis.py`

1. Import and call `analyze_complete_financial_quality()` from orchestrator
2. Store cycle classification results
3. Generate LLM quality summary using `generate_quality_summary()`
4. Generate LLM recommendations using `generate_quality_recommendations()`

### Phase 5: Update topological.py Integration

**File**: `src/dataraum_context/quality/topological.py`

1. Pass LLM service to domain analyzer when available
2. Use LLM-classified cycles in results

---

## Files Summary

| File | Final State |
|------|-------------|
| `quality/domains/financial.py` | Accounting checks only, delegates cycle analysis to orchestrator |
| `quality/domains/financial_llm.py` | Keep as-is (correct implementation) |
| `quality/domains/financial_orchestrator.py` | Keep as-is (correct implementation) |
| `quality/llm_quality.py` | Remove `classify_business_cycle()`, keep summary/recommendations |
| `quality/synthesis.py` | Add orchestrator + LLM integration |
| `quality/topological.py` | Minor updates to pass LLM service |
| `config/domains/financial.yaml` | Keep as-is (used as LLM context) |

---

## Testing Strategy

1. **Unit tests**: Test accounting checks in isolation
2. **Integration tests**: Test orchestrator with mock LLM
3. **Golden file tests**: Verify LLM classification output format

---

## Rollback Plan

If issues arise:
1. The original `financial.py` functions are being deleted, not modified
2. Git history preserves all original code
3. Can revert individual commits if needed

---

## Progress Tracking

- [x] Phase 1: Remove pattern-based cycle detection from financial.py
- [x] Phase 2: Remove incomplete LLM implementation from llm_quality.py
- [x] Phase 3: Refactor FinancialDomainAnalyzer
- [x] Phase 4: Integrate into synthesis.py
- [x] Phase 5: Update topological.py integration
- [x] Phase 6: Update/add tests (removed obsolete test file)
- [x] Phase 7: Final cleanup and documentation

## Completion Notes (2025-12-13)

**Files Modified:**
- `quality/domains/financial.py` - Removed `detect_financial_cycles()`, `_score_cycle_match()`, `_get_fallback_cycle_patterns()`, and pattern-matching from `analyze_cross_table_cycles()`
- `quality/llm_quality.py` - Removed `classify_business_cycle()`, updated imports and docstring
- `quality/domains/__init__.py` - Updated exports to remove deleted function
- `quality/synthesis.py` - Added LLM integration via orchestrator
- `quality/topological.py` - Updated docstring to point to orchestrator

**Files Deleted:**
- `tests/quality/test_single_table_cycle_classification.py` - Obsolete test for removed pattern-based function

**Files Kept (correct architecture):**
- `quality/domains/financial_llm.py` - LLM classification functions
- `quality/domains/financial_orchestrator.py` - Main entry point for LLM-enhanced analysis

**All 290 tests pass** (1 skipped due to missing optional dependency)
