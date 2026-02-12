# Plan: Consolidate Entropy Resolution Actions into Taxonomy

## Problem
The entropy system has resolution actions from **two sources**:

### Source 1: Hardcoded in Detectors (23 actions)
- Semantic duplicates (`declare_null_meaning` vs `document_null_meaning`)
- Inconsistent naming (`declare_*`, `add_*`, `document_*` for similar operations)
- No clear categorization by who executes them (user/agent/collaborative)

### Source 2: LLM-Generated Dynamically
The `EntropyInterpreter` (in `interpretation.py`) generates contextual actions like:
- `document_account_structure`
- `cross_validate_tax_rates`

These come from the LLM because `ResolutionActionOutput.action` is a free-form string (line 208).
The prompt only gives examples but doesn't constrain to a fixed set.

## Current Actions Inventory

| File | Current Action | Category |
|------|----------------|----------|
| null_semantics.py:97 | `declare_null_meaning` | Document |
| null_semantics.py:113 | `filter_nulls` | Transform |
| null_semantics.py:125 | `impute_values` | Transform |
| outliers.py:137 | `winsorize` | Transform |
| outliers.py:153 | `exclude_outliers` | Transform |
| outliers.py:166 | `investigate_outliers` | Investigate |
| derived_values.py:129 | `declare_formula` | Document |
| derived_values.py:143 | `verify_formula` | Investigate |
| derived_values.py:155 | `investigate_mismatches` | Investigate |
| relationship_entropy.py:155 | `confirm_relationship` | Document |
| relationship_entropy.py:170 | `fix_referential_integrity` | Transform |
| temporal_entropy.py:119 | `mark_timestamp` | Document |
| temporal_entropy.py:133 | `resolve_temporal_mismatch` | Transform |
| types.py:85 | `override_type` | Document |
| types.py:100 | `quarantine_values` | Transform |
| business_meaning.py:155 | `add_description` | Document |
| business_meaning.py:170 | `add_business_name` | Document |
| business_meaning.py:184 | `add_entity_type` | Document |
| relations.py:120 | `declare_relationship` | Document |
| relations.py:130 | `declare_preferred_path` | Document |
| unit_entropy.py:104 | `declare_unit` | Document |
| dimensional_entropy.py:320 | `document_business_rule` | Document |
| dimensional_entropy.py:331 | `add_constraint` | Create |
| dimensional_entropy.py:383 | `document_all_patterns` | Document |
| entropy_phase.py:865 | `review_quality_findings` | Investigate |
| entropy_phase.py:876 | `address_quality_issues` | Transform |
| test_interpretation.py:100 | `document_null_meaning` | Document (test) |

## Proposed Action Taxonomy

### Three Action Categories by Executor

| Category | Prefix | Executor | Description |
|----------|--------|----------|-------------|
| **Document** | `document_*` | User alone | User provides knowledge/decisions that only they have |
| **Investigate** | `investigate_*` | Agent with user review | Agent analyzes, user reviews findings |
| **Transform** | `transform_*` | Agent alone | Agent applies data transformation |

### Consolidated Action Types

| New Action | Old Actions | Category | Description |
|------------|-------------|----------|-------------|
| `document_null_semantics` | `declare_null_meaning`, `document_null_meaning` | Document | User documents what null values mean |
| `document_unit` | `declare_unit` | Document | User declares the unit of measurement |
| `document_formula` | `declare_formula` | Document | User documents derived column formula |
| `document_type_override` | `override_type` | Document | User overrides inferred type |
| `document_relationship` | `declare_relationship`, `confirm_relationship` | Document | User confirms/declares a relationship |
| `document_join_path` | `declare_preferred_path` | Document | User specifies preferred join path |
| `document_timestamp_role` | `mark_timestamp` | Document | User marks column as timestamp |
| `document_business_name` | `add_business_name` | Document | User provides business name |
| `document_description` | `add_description` | Document | User provides column description |
| `document_entity_type` | `add_entity_type` | Document | User declares entity type |
| `document_business_rule` | `document_business_rule`, `document_all_patterns` | Document | User documents business rules |
| `create_constraint` | `add_constraint` | Document | User creates DB constraint |
| `investigate_outliers` | `investigate_outliers` | Investigate | Agent finds outliers, user reviews |
| `investigate_formula_mismatches` | `verify_formula`, `investigate_mismatches` | Investigate | Agent checks formula, user reviews |
| `investigate_quality_issues` | `review_quality_findings`, `address_quality_issues` | Investigate | Agent summarizes issues, user reviews |
| `transform_filter_nulls` | `filter_nulls` | Transform | Agent excludes null rows |
| `transform_impute_values` | `impute_values` | Transform | Agent fills missing values |
| `transform_winsorize` | `winsorize` | Transform | Agent caps outliers |
| `transform_exclude_outliers` | `exclude_outliers` | Transform | Agent excludes outlier rows |
| `transform_quarantine_values` | `quarantine_values` | Transform | Agent moves bad values to quarantine |
| `transform_fix_referential_integrity` | `fix_referential_integrity` | Transform | Agent fixes RI violations |
| `transform_resolve_temporal_mismatch` | `resolve_temporal_mismatch` | Transform | Agent resolves time mismatches |

## Changes Required

### 1. Detectors to Update

**[null_semantics.py](src/dataraum/entropy/detectors/value/null_semantics.py)**
- L97: `declare_null_meaning` → `document_null_semantics`
- L113: `filter_nulls` → `transform_filter_nulls`
- L125: `impute_values` → `transform_impute_values`

**[outliers.py](src/dataraum/entropy/detectors/value/outliers.py)**
- L137: `winsorize` → `transform_winsorize`
- L153: `exclude_outliers` → `transform_exclude_outliers`
- L166: `investigate_outliers` → keep as is (already fits pattern)

**[derived_values.py](src/dataraum/entropy/detectors/computational/derived_values.py)**
- L129: `declare_formula` → `document_formula`
- L143: `verify_formula` → `investigate_formula_mismatches`
- L155: `investigate_mismatches` → merge into `investigate_formula_mismatches`

**[relationship_entropy.py](src/dataraum/entropy/detectors/structural/relationship_entropy.py)**
- L155: `confirm_relationship` → `document_relationship`
- L170: `fix_referential_integrity` → `transform_fix_referential_integrity`

**[temporal_entropy.py](src/dataraum/entropy/detectors/semantic/temporal_entropy.py)**
- L119: `mark_timestamp` → `document_timestamp_role`
- L133: `resolve_temporal_mismatch` → `transform_resolve_temporal_mismatch`

**[types.py](src/dataraum/entropy/detectors/structural/types.py)**
- L85: `override_type` → `document_type_override`
- L100: `quarantine_values` → `transform_quarantine_values`

**[business_meaning.py](src/dataraum/entropy/detectors/semantic/business_meaning.py)**
- L155: `add_description` → `document_description`
- L170: `add_business_name` → `document_business_name`
- L184: `add_entity_type` → `document_entity_type`

**[relations.py](src/dataraum/entropy/detectors/structural/relations.py)**
- L120: `declare_relationship` → `document_relationship`
- L130: `declare_preferred_path` → `document_join_path`

**[unit_entropy.py](src/dataraum/entropy/detectors/semantic/unit_entropy.py)**
- L104: `declare_unit` → `document_unit`

**[dimensional_entropy.py](src/dataraum/entropy/detectors/semantic/dimensional_entropy.py)**
- L320, L519: `document_business_rule` → keep as is
- L331, L530: `add_constraint` → `create_constraint`
- L383, L582: `document_all_patterns` → merge into `document_business_rule` with parameters

### 2. entropy_phase.py Consolidation

**[entropy_phase.py](src/dataraum/pipeline/phases/entropy_phase.py#L862-L884)**

Replace two separate resolution options with single consolidated one:
```python
resolution_options=[
    ResolutionOption(
        action="investigate_quality_issues",
        parameters={
            "column_name": col_name,
            "key_findings": all_key_findings,
            "quality_issues": all_quality_issues,
            "recommendations": all_recommendations,
        },
        expected_entropy_reduction=entropy_score_val * 0.6,
        effort="medium",
        description=f"Review {len(all_quality_issues)} quality issues and {len(all_recommendations)} recommendations for {col_name}",
    ),
],
```

Remove TODO comment on L861.

### 3. Test Updates

**[test_interpretation.py](tests/unit/entropy/test_interpretation.py)**
- L100: `document_null_meaning` → `document_null_semantics`

### 4. Guide LLM-Generated Actions with Prefix Rules

The LLM can generate contextual actions (like `document_account_structure`) but must follow the prefix convention.

**[interpretation.py](src/dataraum/entropy/interpretation.py)**

Add a validator to `ResolutionActionOutput.action` to enforce prefix rules:

```python
from pydantic import field_validator

VALID_ACTION_PREFIXES = ("document_", "investigate_", "transform_", "create_")

class ResolutionActionOutput(BaseModel):
    """Pydantic model for resolution action in LLM tool output."""

    action: str = Field(
        description="Action identifier. Must start with: document_, investigate_, transform_, or create_"
    )

    @field_validator("action")
    @classmethod
    def validate_action_prefix(cls, v: str) -> str:
        if not v.startswith(VALID_ACTION_PREFIXES):
            raise ValueError(
                f"Action '{v}' must start with one of: {VALID_ACTION_PREFIXES}"
            )
        return v
    # ... rest unchanged
```

**[config/system/prompts/entropy_interpretation.yaml](config/system/prompts/entropy_interpretation.yaml)**

Update the `<resolution_guidelines>` section to explain the prefix convention:

```yaml
<resolution_guidelines>
When suggesting resolutions:
- Prioritize low-effort, high-impact actions
- Be specific about what metadata needs to be added
- Consider cascade effects (fixing one thing may help others)
- For each action, explain what entropy dimension it addresses

ACTION NAMING CONVENTION - all actions MUST use one of these prefixes:

document_* (User provides knowledge/decisions):
  Standard: document_null_semantics, document_unit, document_formula,
            document_type_override, document_relationship, document_join_path,
            document_timestamp_role, document_business_name, document_description,
            document_entity_type, document_business_rule
  Contextual: document_{domain_specific} (e.g., document_account_structure)

investigate_* (Agent analyzes, user reviews findings):
  Standard: investigate_outliers, investigate_formula_mismatches,
            investigate_quality_issues
  Contextual: investigate_{domain_specific} (e.g., investigate_tax_discrepancies)

transform_* (Agent applies data transformation):
  Standard: transform_filter_nulls, transform_impute_values, transform_winsorize,
            transform_exclude_outliers, transform_quarantine_values,
            transform_fix_referential_integrity, transform_resolve_temporal_mismatch
  Contextual: transform_{domain_specific} (e.g., transform_normalize_currency)

create_* (User creates artifacts):
  Standard: create_constraint
  Contextual: create_{domain_specific} (e.g., create_validation_rule)
</resolution_guidelines>
```

## Verification

1. Run all entropy tests: `pytest tests/unit/entropy/ -v`
2. Run pipeline tests: `pytest tests/ -k entropy -v`
3. Grep for old action names to ensure none remain: `grep -r "action=\"declare_\|action=\"add_\|action=\"mark_" src/`
4. Run full test suite: `pytest tests/ -v`
5. Test LLM interpretation to verify it uses new action taxonomy

