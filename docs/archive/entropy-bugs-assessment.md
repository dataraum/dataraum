# Entropy Bug/Improvement Assessment

Assessed against `data/metadata.db` from small_finance fixture run on `refactor/streamline` branch (2026-02-10).

## Priority Table

| # | Bug | Severity | Effort | Priority | Status |
|---|-----|----------|--------|----------|--------|
| 7 | Table actions all generic "review" | High | Medium | P1 | **Backlogged** → `docs/specs/entropy.md` roadmap. Root cause: `DimensionalSummaryAgent` output has no structured actions, `entropy_phase.py:942` wraps text in hardcoded "review". Fix overlaps with quality context pipeline redesign. |
| 1 | naming_clarity always 0.2 | Medium | Low | P2 | **Fixed** — added `score_fully_documented: 0.0` tier |
| 2 | Units ignores currency context | Medium | Medium | P2 | **Backlogged** → `docs/specs/semantic.md` roadmap (cross-column unit detection) |
| 6 | Dimensional not in column interpretations | Not a bug | — | — | Scores ARE included via semantic layer average. But audit revealed deeper issue: quality context (issues, recommendations, grades) gets lost in pipeline. See `docs/specs/entropy.md` roadmap "Fix quality context pipeline". |
| 8 | Action parameters always empty | Medium | Low | P2 | **Backlogged** → `docs/specs/entropy.md` roadmap. `ResolutionActionOutput` Pydantic schema has no `parameters` field — LLM is never asked for them. Also `to_dict()` doesn't serialize parameters. Fix with interpretation pipeline redesign. |
| 11 | Table summary only dimensional | Medium | Medium | P2 | **Backlogged** → `docs/specs/entropy.md` roadmap. Table-level entropy objects skipped in interpretation phase (`column_id is None: continue`). Only `DimensionalSummaryAgent` produces table interpretations. No aggregation of column-level nulls/outliers/types into table summary. Part of interpretation pipeline redesign. |
| 12 | Compound risks misclassify columns | Medium | Medium | P2 | **Fixed** — `_get_dimension_score()` fell back to layer average for missing dimensions, causing false compound risks. Now returns 0.0 for missing dimensions. |
| 13 | Contract summaries not business-focused | Medium | Low | P2 | **Partially fixed** — removed layer-level fallback bug in `contracts.py` `_get_dimension_score()` and `_find_affected_columns()` (same pattern as Bug #12). Business-focused violation text is a larger redesign — backlogged. |
| 4 | Benford not computed | Low | High | P3 | **Backlogged** → `docs/specs/entropy.md` roadmap. Benford IS computed in statistics/quality.py, just not wired into entropy. |
| 5 | Outliers on ID columns | Low | Low | P3 | **Fixed** — skip columns with semantic_role `key`/`foreign_key` |
| 3 | Null score formula | Medium | Low | P2 | **Fixed** — removed 2x multiplier, score = null_ratio directly |
| 9 | Evidence missing column name | Low | Low | P3 | **Backlogged** → `docs/specs/entropy.md` roadmap. DB schema is fine (`column_id` FK + `target` has `"column:table.col"`). But evidence JSON itself is not self-identifying — no column/table name inside. Minor readability issue for debugging/TUI/LLM context. |
| 10 | Type decision duplicates | Not a bug | — | — | Intentional: TypeDecision/TypeCandidate are copied from raw→typed columns during `resolve_types()`. Entropy phase queries typed column IDs exclusively (`entropy_phase.py:128,162,170`), so copies are required. **Backlogged** → `docs/specs/typing.md` roadmap (eliminate copy via `raw_column_id` FK). |

---

## Detailed Assessment

### 1. `semantic.business_meaning.naming_clarity` — All scores 0.2

**Confirmed bug.** The scoring in `entropy/detectors/semantic/business_meaning.py` has three tiers:
- `1.0` = no description
- `0.6` = description only
- `0.2` = description + (business_name OR entity_type)

There is no `0.0` tier. When a column has description + business_name + entity_type (all 52 do), the best possible score is still `0.2`. The config `thresholds.yaml` defines `score_documented: 0.2`.

**Evidence:** All 52 business_meaning objects have score=0.2, evidence shows `has_description: true`, `has_business_name: true`, `has_entity_type: true`.

**Fix:** Add a `score_fully_documented: 0.0` tier for when all three (description, business_name, entity_type) are present. Keep `0.2` for partial documentation (description + one of the two). The ontology_bonus (0.1) should be additive on top of this.

**Impact:** Inflates all composite scores by 0.03-0.06, makes all columns show "naming clarity" as an issue when it isn't.

---

### 2. `semantic.units.unit_declaration` — Currency columns wrongly flagged (score=0.8)

**Confirmed bug.** All 6 measure columns (Amount, Open balance, Quantity, Rate, Credit, Debit) score 0.8 with `unit_status: "missing"`. The detector (`entropy/detectors/semantic/unit_entropy.py`) only checks for Pint-detected units (explicit `$`, `EUR` symbols in values). It doesn't consider:
- That `business_concept` = `transaction_amount`, `debit`, `credit` implies monetary
- That the table has an `Account type` dimension column indicating accounting context
- That the ontology maps these to financial concepts

**Evidence:** All 6 units objects show `detected_unit: null`, `unit_confidence: 0.0`, `unit_status: "missing"`. Semantic annotations confirm these columns have monetary `business_concept` values.

**Fix:** In `unit_entropy.py`, check `business_concept` and `entity_type` from semantic annotations. If the concept is monetary (from ontology), reduce score to `score_inferred: 0.2` or similar. Also consider that Quantity and Rate may legitimately need units (not monetary).

---

### 3. `value.nulls` — Score 1.0 for ratio 0.69

**Not a bug.** The formula is `score = min(1.0, null_ratio * 2.0)` (multiplier from `thresholds.yaml`). Anything above 50% null hits score 1.0. This is by design — 50%+ nulls represents maximum uncertainty.

Null score distribution from metadata:
- `score=0.0088, ratio=0.0044` (Customer name)
- `score=0.4185, ratio=0.2093` (Credit)
- `score=0.4229, ratio=0.2115` (Rate)
- `score=0.7973, ratio=0.3987` (Quantity)
- `score=1.0, ratio=0.6878` (Product_Service)
- `score=1.0, ratio=0.7907` (Debit)
- `score=1.0, ratio=0.9387` (Misc)
- `score=1.0, ratio=0.9973` (Vendor name)

The "Umsatzsteuerschluessel" column is from a different dataset, not the small_finance fixture.

---

### 4. `value.outliers` — Benford always 0/NULL

**Confirmed gap.** The `statistical_profiles` table has NO Benford-related columns. Benford analysis is not computed in the statistics phase. The outlier detector only uses `outlier_ratio`, `outlier_count`, `outlier_impact`, `iqr_lower_fence`, `iqr_upper_fence`.

**Fix options:**
1. Add Benford analysis to the statistics phase and wire into outlier detector
2. Remove Benford references from entropy docs/specs if not planned

---

### 5. Outliers don't make sense for categorical number columns (IDs)

**Confirmed design issue.** 48/52 outlier objects have score=0.0, so minimal current impact. But BIGINT columns like `Business Id` and `Transaction ID` get outlier analysis applied even though outliers are meaningless for identifiers.

**Fix:** The outlier detector should skip columns with `semantic_role` in (`key`, `foreign_key`).

---

### 6. `semantic.dimensional` — Not in column-level entropy interpretations

**Confirmed.** Dimensional entropy objects exist (5 `column_quality`, 2 `cross_column_patterns`, 1 `overall_score`). Table-level interpretations mention dimensional, but column-level interpretations do NOT include dimensional entropy in their composite scores.

**Evidence:** `dimensional.column_quality` objects exist for Rate (0.28), Credit (0.30), Business Id (0.05), Account name (0.28), Account Full Name (0.28). These scores are not reflected in the respective column interpretations.

**Fix:** The entropy interpretation builder should include `dimensional.column_quality` in column-level composite scores where applicable.

---

### 7. Table-level actions — All generic "review" with empty parameters

**Confirmed bug.** Both table-level interpretations have 5 actions each, ALL identical: `action=review, priority=medium, effort=low, params={}`. The explanations are decent but the actions are useless.

**Evidence:**
- `master_txn_table`: score=0.0, readiness=ready, 5x "review" actions
- `chart_of_account_ob`: score=0.325, readiness=investigate, 5x "review" actions

**Root cause:** The entropy interpretation prompt doesn't give the LLM enough guidance on table-level action generation. No examples of good table-level actions.

**Fix:**
1. Improve table-level interpretation prompt with specific action examples
2. Add action templates for common table-level issues (naming inconsistencies, relationship gaps, missing constraints)
3. Populate `parameters` with dimension references

---

### 8. Column-level actions — `parameters` always `{}`

**Confirmed.** ALL resolution actions across ALL 54 interpretations have `parameters: {}`. The `expected_impact` and `description` fields ARE populated with useful text, but `parameters` is always empty.

**Evidence sample:**
- `add_unit_declaration`: params={}, expected_impact="Reduces semantic.units.unit_declaration from 0.8 to 0.0"
- `standardize_column_naming`: params={}, expected_impact="Reduces semantic.business_meaning.naming_clarity from 0.2 to 0.0"

**Fix:** Instruct the LLM to populate parameters with actionable values (e.g., `{"suggested_unit": "USD"}`, `{"suggested_name": "transaction_id"}`).

---

### 9. Table evidence — only column ID, no column name

**Not a bug.** The `entropy_interpretations` table has both `column_name` and `column_id` columns, and column names ARE present. The `entropy_objects` table references `column_id` as a FK without a denormalized name — that's standard relational design (join to get the name).

---

### 10. Type decisions "duplicates"

**Not a bug.** 63 type decisions for raw layer + 52 for typed layer = 115 total. No duplicates per `column_id`. Each layer's columns are separate entities in the `columns` table (different `column_id` values), so each gets its own type decision.

---

### 11. Table-level summary only includes dimensional entropy

**Confirmed gap.** Table-level interpretations exist for only 2 tables and only cover `dimensional` entropy. Other dimensions (nulls, outliers, types, relations) are computed per-column but not aggregated to table level.

**Fix:** The table interpretation builder should aggregate all column-level entropy dimensions into a table summary (e.g., "3/15 columns have high null entropy, 0/15 have type issues, 2 relationship quality concerns").

---

### 12. Compound risks — Incorrect temporal classification

**Confirmed.** Compound risks show `semantic.temporal + value.nulls` for "Quantity" and "Debit" — describing them as timestamp columns with null values. These are NOT timestamp columns (Quantity is `item_quantity`/measure, Debit is `debit_amount`/measure).

**Root cause:** The compound risk detector cross-references temporal entropy with null entropy without verifying that the column actually has a temporal role.

---

### 13. Contract LLM summary not business-focused

**Confirmed by user.** The LLM produces statistically-oriented summaries ("statistically unreliable distribution of column") instead of business-context summaries. This is a prompt engineering issue in the contract evaluation prompts.

**Fix:** Update contract evaluation prompts to emphasize business context over statistical descriptions. Include ontology concepts and business names in the prompt context.

---

## Metadata Summary (small_finance fixture)

| Dimension | Sub-dimension | Count | Avg Score | Min | Max |
|-----------|--------------|-------|-----------|-----|-----|
| business_meaning | naming_clarity | 52 | 0.200 | 0.2 | 0.2 |
| nulls | null_ratio | 52 | 0.109 | 0.0 | 1.0 |
| outliers | outlier_rate | 52 | 0.037 | 0.0 | 0.733 |
| types | type_fidelity | 52 | 0.000 | 0.0 | 0.0 |
| relations | relationship_quality | 22 | 0.358 | 0.3 | 0.939 |
| relations | join_path_determinism | 17 | 0.100 | 0.1 | 0.1 |
| units | unit_declaration | 6 | 0.800 | 0.8 | 0.8 |
| dimensional | column_quality | 5 | 0.238 | 0.05 | 0.3 |
| temporal | time_role | 4 | 0.100 | 0.1 | 0.1 |
| dimensional | cross_column_patterns | 2 | 0.500 | 0.5 | 0.5 |
| derived_values | formula_match | 1 | 0.000 | 0.0 | 0.0 |
| dimensional | overall_score | 1 | 0.325 | 0.325 | 0.325 |

**Total entropy objects:** 266
**Total interpretations:** 54 (52 column-level + 2 table-level)
