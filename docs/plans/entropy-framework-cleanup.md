# Plan: Entropy Framework Cleanup & Extension

## Status

| Phase | Description | Status |
|-------|-------------|--------|
| Phase A | Fix Critical Agent Issues | ✅ COMPLETE |
| Phase B | Add Missing Detectors | ✅ COMPLETE |
| Phase C | Enhance Existing Detectors | ✅ COMPLETE |
| Phase D | Fix Configuration | ✅ COMPLETE |
| Phase E | Enrich Output | ✅ COMPLETE |
| Phase F | Architecture Refactor | 🔲 PENDING |

### Completed Work Summary

**Phase A - Critical Agent Fixes:**
- Fixed `query/agent.py` line 409: was using `execution_context` instead of checking `entropy_summary`
- Changed confidence default from GREEN to YELLOW when entropy unavailable
- Changed `entropy_score: float = 0.0` to `entropy_score: float | None = None`
- Fixed prompt defaults in `query_analysis.yaml` and `graph_sql_generation.yaml`

**Phase B - New Detectors (3 created):**
- `UnitEntropyDetector` (`semantic.units.unit_declaration`) - uses `typing.detected_unit`, `typing.unit_confidence`
- `TemporalEntropyDetector` (`semantic.temporal.time_role`) - uses `semantic.semantic_role`, `typing.data_type`
- `RelationshipEntropyDetector` (`structural.relations.relationship_quality`) - uses actual `JoinCandidate` metrics from `Relationship.evidence`

**Phase C - Detector Enhancement:**
- Enhanced `BusinessMeaningDetector` with confidence weighting and ontology bonus

**Phase D - Configuration Fixes:**
- Fixed `join_path` config keys (`score_single` → `score_deterministic`)
- Removed dead compound risks (`value.ranges`, `computational.filters`, `computational.aggregations`)
- Added fail-fast behavior in `config.py` and `compound_risk.py`
- Updated `thresholds.yaml` with new detector configs
- Updated `contracts.yaml` to remove non-existent dimensions

**Phase E - Output Enrichment:**
- Updated prompt templates to reflect actual available dimensions
- Removed references to non-existent `computational.aggregations`

---

## Executive Summary

The entropy framework has solid architecture (detectors, configuration, compound risks, LLM interpretation) but contains bugs:
- Hallucinated values computed without actual data
- Dead configuration that doesn't match code
- Silent fallbacks that obscure failures
- Missing connections between available data and entropy outputs

This plan takes a **bottom-up approach**: fix/extend detectors first, then clean configuration to match.

---

## Part 1: Data Inventory (What's Actually Available)

### Currently Available Analysis Data

| Analysis Module | Data Available | Currently Used By Detector? |
|-----------------|----------------|----------------------------|
| `typing` | `parse_success_rate`, `detected_unit`, `unit_confidence` | `TypeFidelityDetector` (only parse_success_rate) |
| `statistics` | `null_ratio`, `iqr_outlier_ratio`, distributions | `NullRatioDetector`, `OutlierRateDetector` |
| `semantic` | `business_description`, `business_name`, `entity_type`, `semantic_role`, `time_column` | `BusinessMeaningDetector` (only description/name/entity) |
| `relationships` | `confidence`, `cardinality`, `detection_method`, `is_confirmed` | `JoinPathDeterminismDetector` (only path ambiguity) |
| `correlation` | `derived_columns`, `match_rate`, `formula` | `DerivedValueDetector` |

### JoinCandidate Evaluation Metrics (Computed & Stored)

From `analysis/relationships/models.py`:
```python
left_referential_integrity: float | None   # 0-100% FK values with matching PK
right_referential_integrity: float | None  # 0-100% PK values referenced
orphan_count: int | None                   # FK values with no matching PK
cardinality_verified: bool | None          # Whether cardinality matches actual
```

**Verified**: These metrics ARE computed by `evaluator.py` when `evaluate=True` (default).
They're stored in the `Relationship.evidence` JSON column (detector.py:161-168).

The `RelationshipEntropyDetector` can read these directly from the database.

### TypeCandidate Unit Data (Available but Unused)

From `analysis/typing/db_models.py`:
```python
detected_unit: str | None      # e.g., "kg", "USD"
unit_confidence: float | None  # 0-1.0
```

---

## Part 2: Current Detectors Analysis

### Existing Detectors (6 total)

| Detector | Dimension Path | Data Source | Formula | Issues |
|----------|----------------|-------------|---------|--------|
| `TypeFidelityDetector` | `structural.types.type_fidelity` | `typing.parse_success_rate` | `1.0 - parse_success_rate` | None |
| `JoinPathDeterminismDetector` | `structural.relations.join_path_determinism` | `relationships[]` | orphan=0.9, deterministic=0.1, ambiguous=0.7 | Config key mismatch |
| `NullRatioDetector` | `value.nulls.null_ratio` | `statistics.null_ratio` | `min(1.0, ratio * multiplier)` | None |
| `OutlierRateDetector` | `value.outliers.outlier_rate` | `statistics.iqr_outlier_ratio` | `min(1.0, ratio * multiplier)` | None |
| `BusinessMeaningDetector` | `semantic.business_meaning.naming_clarity` | `semantic.business_description` | missing=1.0, partial=0.6, documented=0.2 | Provisional (documented) |
| `DerivedValueDetector` | `computational.derived_values.formula_match` | `correlation.derived_columns` | `1.0 - match_rate` | None |

### Hallucinated Code (No Detector)

`context.py:_compute_relationship_entropy()` fills `RelationshipEntropyProfile` with:
- `cardinality_entropy = 1.0 - confidence` (made up)
- `join_path_entropy = 0.2/0.1/0.5` based on detection_method string (hardcoded)
- `referential_integrity_entropy = 0.5 - confidence` (made up)
- `semantic_clarity_entropy = 0.2 or 0.7` (binary, no analysis)

These values are **NOT** computed from actual data like orphan counts or referential integrity percentages.

---

## Part 3: Gap Analysis

### Dimensions Without Detectors

| Config Dimension | Used In | Available Data | Action |
|------------------|---------|----------------|--------|
| `semantic.units` | compound_risks, contracts | `typing.detected_unit`, `typing.unit_confidence` | **Add UnitEntropyDetector** |
| `semantic.temporal` | compound_risks, contracts | `semantic.semantic_role=timestamp`, `semantic.time_column` | **Add TemporalEntropyDetector** |
| `value.ranges` | compound_risks | Could use `statistics.min`, `statistics.max` | Remove (low value) |
| `computational.aggregations` | compound_risks, contracts | Would need to detect if column is aggregated | Remove (not computable) |
| `computational.filters` | compound_risks | Would need query analysis | Remove (not computable) |

### RelationshipEntropyProfile vs JoinPathDeterminismDetector

The `RelationshipEntropyProfile` model has 4 fields:
1. `cardinality_entropy` - Could use `JoinCandidate.cardinality_verified`
2. `join_path_entropy` - Could use `JoinPathDeterminismDetector` output
3. `referential_integrity_entropy` - Could use `JoinCandidate.left_referential_integrity`, `orphan_count`
4. `semantic_clarity_entropy` - Could use `Relationship.is_confirmed`, `relationship_type`

**Solution**: Create a `RelationshipEntropyDetector` that computes these from actual data and populates `RelationshipEntropyProfile`.

---

## Part 4: Implementation Plan

### Phase 1: Add Missing Detectors

#### 1.1 UnitEntropyDetector (NEW)

**File**: `packages/dataraum-api/src/dataraum/entropy/detectors/semantic/unit_entropy.py`

```python
class UnitEntropyDetector(EntropyDetector):
    """Detector for unit declaration uncertainty.

    Measures whether numeric columns have declared units.
    Source: typing.detected_unit, typing.unit_confidence
    """

    detector_id = "unit_entropy"
    layer = "semantic"
    dimension = "units"
    sub_dimension = "unit_declaration"
    required_analyses = ["typing", "semantic"]

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        typing = context.get_analysis("typing", {})
        semantic = context.get_analysis("semantic", {})

        # Only applies to numeric columns (measures)
        semantic_role = semantic.get("semantic_role")
        if semantic_role != "measure":
            return []  # N/A for non-measures

        detected_unit = typing.get("detected_unit")
        unit_confidence = typing.get("unit_confidence", 0.0)

        if not detected_unit:
            score = config.get("score_no_unit", 0.8)  # No unit on measure = high entropy
        elif unit_confidence < 0.5:
            score = config.get("score_low_confidence", 0.5)
        else:
            score = config.get("score_declared", 0.1)  # Unit declared = low entropy

        # Resolution options
        if score > 0.3:
            options = [ResolutionOption(
                action="declare_unit",
                parameters={"column": context.column_name},
                expected_entropy_reduction=score * 0.8,
                effort="low",
                description="Declare the unit for this measure column",
            )]

        return [self.create_entropy_object(context, score, evidence, options)]
```

**Config** (`thresholds.yaml`):
```yaml
unit_entropy:
  score_no_unit: 0.8       # Measure without unit
  score_low_confidence: 0.5
  score_declared: 0.1
  reduction_declare_unit: 0.8
```

#### 1.2 TemporalEntropyDetector (NEW)

**File**: `packages/dataraum-api/src/dataraum/entropy/detectors/semantic/temporal_entropy.py`

```python
class TemporalEntropyDetector(EntropyDetector):
    """Detector for temporal column uncertainty.

    Measures whether timestamp columns are properly identified and analyzed.
    Source: semantic.semantic_role, semantic.time_column
    """

    detector_id = "temporal_entropy"
    layer = "semantic"
    dimension = "temporal"
    sub_dimension = "time_role"
    required_analyses = ["semantic", "typing"]

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        semantic = context.get_analysis("semantic", {})
        typing = context.get_analysis("typing", {})

        semantic_role = semantic.get("semantic_role")
        data_type = typing.get("data_type", "")

        # Check if column is date/time type but not marked as timestamp
        is_datetime_type = any(t in data_type.upper() for t in ["DATE", "TIME", "TIMESTAMP"])
        is_marked_timestamp = semantic_role == "timestamp"

        if is_datetime_type and not is_marked_timestamp:
            score = config.get("score_unmarked", 0.6)  # Date column not marked as timestamp
        elif not is_datetime_type and is_marked_timestamp:
            score = config.get("score_mismatch", 0.8)  # Marked timestamp but not date type
        elif is_datetime_type and is_marked_timestamp:
            score = config.get("score_aligned", 0.1)  # Properly identified
        else:
            return []  # N/A

        return [self.create_entropy_object(context, score, evidence, options)]
```

#### 1.3 RelationshipEntropyDetector (NEW - replaces hallucinated code)

**File**: `packages/dataraum-api/src/dataraum/entropy/detectors/structural/relationship_entropy.py`

```python
class RelationshipEntropyDetector(EntropyDetector):
    """Detector for relationship-level entropy.

    Computes entropy from actual relationship metrics:
    - Referential integrity (orphan ratio)
    - Cardinality verification
    - Semantic clarity (is relationship type known, is it confirmed)

    Source: relationships.Relationship, relationships.JoinCandidate
    """

    detector_id = "relationship_entropy"
    layer = "structural"
    dimension = "relations"
    sub_dimension = "relationship_quality"
    required_analyses = ["relationships"]

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        relationships = context.get_analysis("relationships", [])

        objects = []
        for rel in relationships:
            # Compute referential integrity entropy from actual orphan data
            ri = rel.get("left_referential_integrity")
            if ri is not None:
                ri_entropy = 1.0 - (ri / 100.0)  # 100% integrity = 0 entropy
            else:
                ri_entropy = config.get("score_unknown_ri", 0.5)  # Unknown = medium

            # Compute cardinality entropy from verification
            cardinality_verified = rel.get("cardinality_verified")
            if cardinality_verified is True:
                card_entropy = 0.1
            elif cardinality_verified is False:
                card_entropy = 0.7  # Cardinality mismatch
            else:
                card_entropy = config.get("score_unverified_cardinality", 0.4)

            # Compute semantic clarity from confirmation status
            is_confirmed = rel.get("is_confirmed", False)
            rel_type = rel.get("relationship_type", "unknown")
            if is_confirmed and rel_type != "unknown":
                semantic_entropy = 0.1
            elif rel_type != "unknown":
                semantic_entropy = 0.3
            else:
                semantic_entropy = 0.7

            # Create EntropyObject for this relationship
            score = max(ri_entropy, card_entropy, semantic_entropy)
            objects.append(self.create_entropy_object(
                context, score,
                evidence=[{"ri_entropy": ri_entropy, "card_entropy": card_entropy, "semantic_entropy": semantic_entropy}],
                resolution_options=...,
            ))

        return objects
```

### Phase 2: Wire RelationshipEntropyDetector to RelationshipEntropyProfile

**File**: `packages/dataraum-api/src/dataraum/entropy/context.py`

Replace `_compute_relationship_entropy()` with:

```python
def _build_relationship_profiles_from_detector(
    entropy_objects: list[EntropyObject],
    relationships: list[Relationship],
) -> dict[str, RelationshipEntropyProfile]:
    """Build RelationshipEntropyProfile from RelationshipEntropyDetector output.

    The detector produces EntropyObjects with evidence containing:
    - ri_entropy: referential integrity entropy
    - card_entropy: cardinality entropy
    - semantic_entropy: semantic clarity entropy

    We use these to populate the profile fields.
    """
    profiles = {}

    for rel, obj in zip(relationships, entropy_objects):
        evidence = obj.evidence[0] if obj.evidence else {}

        profile = RelationshipEntropyProfile(
            from_table=rel.from_table,
            from_column=rel.from_column,
            to_table=rel.to_table,
            to_column=rel.to_column,
            # Populate from detector evidence
            referential_integrity_entropy=evidence.get("ri_entropy", 0.0),
            cardinality_entropy=evidence.get("card_entropy", 0.0),
            semantic_clarity_entropy=evidence.get("semantic_entropy", 0.0),
            join_path_entropy=0.0,  # Comes from JoinPathDeterminismDetector
        )
        profile.calculate_composite()
        profiles[profile.relationship_key] = profile

    return profiles
```

### Phase 3: Fix Configuration

#### 3.1 Fix join_path Config Mismatch

**File**: `config/entropy/thresholds.yaml`

```yaml
# BEFORE (keys don't match code):
join_path:
  score_single: 0.1       # NEVER READ
  score_few: 0.4          # NEVER READ

# AFTER (matches detector code):
join_path:
  score_orphan: 0.9
  score_deterministic: 0.1
  score_ambiguous: 0.7
  reduction_declare_relationship: 0.8
  reduction_declare_preferred_path: 0.5
```

#### 3.2 Add New Detector Configs

```yaml
# Unit entropy detector
unit_entropy:
  score_no_unit: 0.8
  score_low_confidence: 0.5
  score_declared: 0.1
  reduction_declare_unit: 0.8

# Temporal entropy detector
temporal_entropy:
  score_unmarked: 0.6
  score_mismatch: 0.8
  score_aligned: 0.1
  reduction_mark_timestamp: 0.6

# Relationship entropy detector
relationship_entropy:
  score_unknown_ri: 0.5
  score_unverified_cardinality: 0.4
  reduction_verify_relationship: 0.6
```

#### 3.3 Update Compound Risks to Use Valid Dimensions

```yaml
compound_risks:
  # Units + aggregations (now valid with UnitEntropyDetector)
  units_derived:
    dimensions:
      - semantic.units
      - computational.derived_values
    threshold: 0.5
    multiplier: 2.0
    risk_level: critical
    impact_template: >
      Numeric measures without declared units are being used in formulas.
      Results may be incorrect if units don't match.

  # Temporal + nulls
  temporal_nulls:
    dimensions:
      - semantic.temporal
      - value.nulls
    threshold: 0.5
    multiplier: 1.5
    risk_level: high
    impact_template: >
      Timestamp columns have null values.
      Time-based analysis may have gaps.

  # Types + derived (existing, valid)
  types_derived:
    dimensions:
      - structural.types
      - computational.derived_values
    threshold: 0.6
    multiplier: 1.5
    risk_level: high

  # Relationship quality + derived
  relations_derived:
    dimensions:
      - structural.relations
      - computational.derived_values
    threshold: 0.5
    multiplier: 1.5
    risk_level: high
    impact_template: >
      Relationships with integrity issues are used in formulas.
```

#### 3.4 Update Contracts to Use Valid Dimensions

**File**: `config/entropy/contracts.yaml`

```yaml
dimension_thresholds:
  structural.types: 0.3
  structural.relations: 0.5
  semantic.business_meaning: 0.5
  semantic.units: 0.5           # Now valid with UnitEntropyDetector
  semantic.temporal: 0.5         # Now valid with TemporalEntropyDetector
  value.nulls: 0.5
  value.outliers: 0.5
  computational.derived_values: 0.5
```

### Phase 4: Fail Fast Behavior

#### 4.1 config.py - Remove Silent Fallbacks

```python
# effort_factor() - raise on unknown level
def effort_factor(self, effort: str) -> float:
    if effort not in self.effort_factors:
        raise KeyError(f"Unknown effort level '{effort}'. Valid: {list(self.effort_factors.keys())}")
    return self.effort_factors[effort]

# load_entropy_config() - fail on missing file
if not config_path.exists():
    raise FileNotFoundError(f"Required entropy config not found: {config_path}")
```

#### 4.2 compound_risk.py - Remove Hardcoded Fallback

```python
def _load_hardcoded_defaults(self) -> None:
    raise FileNotFoundError(
        "Compound risk config not found. Add compound_risks section to thresholds.yaml"
    )
```

#### 4.3 models.py - Require Explicit Thresholds

```python
def is_high_entropy(self, threshold: float) -> bool:  # No default
    return self.score >= threshold

def is_critical(self, threshold: float) -> bool:  # No default
    return self.score >= threshold
```

### Phase 5: Enrich CLI Output

#### 5.1 Fix API high_priority_resolutions

```python
# entropy.py line 95
high_priority_resolutions=dashboard.get("top_resolution_hints", []),
```

#### 5.2 Return Full Resolution Details

```python
# entropy.py line 205
resolution_hints=[{
    "action": r.action,
    "description": r.description,
    "expected_reduction": round(r.expected_entropy_reduction, 2),
    "effort": r.effort,
} for r in hints[:5]]
```

#### 5.3 Contracts CLI - Show Recommendations

After violations, show top 5 resolution actions with effort/impact.

#### 5.4 Query CLI - Show Quick Wins

For RED confidence, show resolution recommendations.

---

## Part 5: Files to Modify

### New Files

| File | Purpose |
|------|---------|
| `entropy/detectors/semantic/unit_entropy.py` | UnitEntropyDetector |
| `entropy/detectors/semantic/temporal_entropy.py` | TemporalEntropyDetector |
| `entropy/detectors/structural/relationship_entropy.py` | RelationshipEntropyDetector |

### Modified Files

| File | Changes |
|------|---------|
| `entropy/detectors/__init__.py` | Register new detectors |
| `entropy/detectors/semantic/__init__.py` | Export new detectors |
| `entropy/detectors/structural/__init__.py` | Export new detector |
| `entropy/detectors/semantic/business_meaning.py` | Use confidence and business_concept |
| `entropy/context.py` | Replace `_compute_relationship_entropy()` with detector-based approach |
| `entropy/config.py` | Remove silent fallbacks |
| `entropy/models.py` | Remove default thresholds |
| `entropy/compound_risk.py` | Remove hardcoded fallback |
| `config/entropy/thresholds.yaml` | Fix join_path keys, add new detector configs, update compound risks |
| `config/entropy/contracts.yaml` | Update dimension paths |
| `config/prompts/query_analysis.yaml` | Fix misleading "No data quality warnings" default |
| `config/prompts/graph_sql_generation.yaml` | Fix misleading entropy warnings default |
| `config/prompts/entropy_interpretation.yaml` | Update layer descriptions to match actual detectors |
| `config/prompts/entropy_query_interpretation.yaml` | Update layer descriptions to match actual detectors |
| `query/agent.py` | Fix wrong variable (line 409), fix GREEN default, use None for unknown entropy |
| `api/routers/entropy.py` | Fix resolution output |
| `cli.py` | Add resolution recommendations |

---

## Part 6: What to Remove

| Item | Reason |
|------|--------|
| `_compute_relationship_entropy()` | Replaced by `RelationshipEntropyDetector` |
| `value.ranges` compound risk | No data source, low value |
| `computational.aggregations` | Not computable from current analysis |
| `computational.filters` | Not computable from current analysis |
| Hardcoded fallbacks | Violates fail-fast principle |
| Dead config keys (`score_single`, `score_few`, etc.) | Never read by code |

---

## Part 7: Implementation Order

### Phase A: Fix Critical Agent Issues ✅ COMPLETE
1. ✅ **Fix query/agent.py line 409** - use correct entropy_context variable
2. ✅ **Fix query/agent.py confidence default** - YELLOW when entropy unavailable
3. ✅ **Fix query/agent.py entropy_score** - use None instead of 0.0
4. ✅ **Fix prompt defaults** - "Data quality assessment not available" instead of "No warnings"

### Phase B: Add Missing Detectors ✅ COMPLETE
5. ✅ **Add UnitEntropyDetector** - uses typing.detected_unit
6. ✅ **Add TemporalEntropyDetector** - uses semantic.semantic_role
7. ✅ **Add RelationshipEntropyDetector** - uses JoinCandidate evaluation metrics
8. ⏭️ **Update context.py** - wire detector to RelationshipEntropyProfile (deferred to Phase F)

### Phase C: Enhance Existing Detectors ✅ COMPLETE
9. ✅ **Enhance BusinessMeaningDetector** - use confidence and business_concept

### Phase D: Fix Configuration ✅ COMPLETE
10. ✅ **Fix thresholds.yaml** - align config with detectors
11. ✅ **Fix contracts.yaml** - use valid dimension paths
12. ✅ **Remove fallbacks** - fail-fast behavior

### Phase E: Enrich Output ✅ COMPLETE
13. ✅ **Update prompts** - remove references to non-existent dimensions
14. ⏭️ **Fix API resolution details** - (deferred to Phase F architecture refactor)

### Phase F: Architecture Refactor 🔲 PENDING
15. **Simplify data model** - EntropyMeasurement as single detector output
16. **EntropyContext as single source** - computed views, no duplicate profiles
17. **Remove duplicate classes** - ColumnEntropyProfile, TableEntropyProfile, etc.
18. **Update all consumers** - query agent, graph context, API endpoints

---

## Part 8: Verification

1. **Tests pass**: `pytest tests/entropy/ -v`
2. **Pipeline runs**: `uv run dataraum run examples/data/ -o ./test_out`
3. **New detectors fire**: Check logs for UnitEntropyDetector, TemporalEntropyDetector, RelationshipEntropyDetector
4. **Contracts work**: `uv run dataraum contracts ./test_out --contract exploratory_analysis`
5. **Query shows recommendations**: `uv run dataraum query "test" -o ./test_out`
6. **Config errors fail fast**: Remove a required config key, verify error
7. **RelationshipEntropyProfile populated**: Check profiles have actual computed values

---

## Part 9: Agent Context Issues (CRITICAL)

### Issue 1: Query Agent Uses Wrong Variable for Entropy Warnings

**Location**: `query/agent.py` line 409

```python
if entropy_context:  # Checks entropy_context
    entropy_warnings = format_entropy_for_prompt(execution_context)  # BUG: Uses execution_context!
```

**Fix**: Use `entropy_context` or pass entropy data to execution_context properly.

### Issue 2: Query Agent Defaults to GREEN When Entropy Fails

**Location**: `query/agent.py` lines 173-199

```python
confidence_level = ConfidenceLevel.GREEN  # Default
if entropy_context:
    # ... contract evaluation
# No else - stays GREEN when entropy unavailable
```

**Fix**: Default to YELLOW or RED when entropy_context is None:
```python
if entropy_context is None:
    confidence_level = ConfidenceLevel.YELLOW
    logger.warning("Entropy context unavailable - data quality unknown")
```

### Issue 3: Entropy Score is 0.0 Instead of None When Unknown

**Location**: `query/agent.py` lines 311-316

```python
entropy_score = 0.0  # Hallucinated "zero entropy"
if entropy_context:
    # ... compute actual score
```

**Fix**: Use `None` as sentinel for unknown:
```python
entropy_score: float | None = None
if entropy_context:
    scores = [p.composite_score for p in entropy_context.column_profiles.values()]
    entropy_score = sum(scores) / len(scores) if scores else None
```

### Issue 4: Prompt Default "No Data Quality Warnings" is Misleading

**Location**: `config/prompts/query_analysis.yaml` line 160-161

```yaml
entropy_warnings:
  default: "No data quality warnings."  # MISLEADING when entropy not computed
```

**Fix**: Change to:
```yaml
entropy_warnings:
  default: "Data quality assessment not available - proceed with caution."
```

Same fix needed in `config/prompts/graph_sql_generation.yaml`.

### Issue 5: Entropy Interpretation Prompts Mention Non-Existent Dimensions

**Location**: `config/prompts/entropy_interpretation.yaml`, `config/prompts/entropy_query_interpretation.yaml`

Both prompts describe the entropy layers as:
```yaml
- value: Nulls, outliers, ranges (is the data complete and reasonable?)
- computational: Derived values, aggregations (can we compute reliably with it?)
```

But we're removing `value.ranges` and there's no `computational.aggregations` detector.

**Fix**: Update layer descriptions to match actual detectors:
```yaml
- value: Nulls, outliers (is the data complete and reasonable?)
- computational: Derived values (can we compute reliably with it?)
```

---

## Part 10: Entropy Framework Agents Analysis

### EntropyInterpreter (`interpretation.py`)
- **Purpose**: LLM-powered interpretation of entropy metrics
- **Input**: Consumes detector output (raw_metrics, high_entropy_dimensions, compound_risks)
- **Output**: Human-readable assumptions, resolution actions, explanations
- **Status**: NO CHANGES NEEDED - better detectors → better interpretations

### Query Refinement (`query_refinement.py`)
- **Purpose**: Query-specific entropy interpretation
- **Input**: Calls EntropyInterpreter with query context
- **Output**: Interpretations focused on columns in the query
- **Status**: NO CHANGES NEEDED - uses EntropyInterpreter

Both agents are still necessary and valuable. They don't produce entropy metrics - they CONSUME them from detectors and generate human-readable interpretations.

---

## Part 12: BusinessMeaningDetector Enhancement

### Currently Unused Data

The semantic agent produces rich data that the detector collects but doesn't use:

| Field | Available | Currently Used |
|-------|-----------|----------------|
| `business_description` | Yes | Yes (presence only) |
| `business_name` | Yes | Yes (presence only) |
| `entity_type` | Yes | Yes (presence only) |
| `confidence` | Yes | **NO** - collected but ignored |
| `business_concept` | Yes | **NO** - collected but ignored |
| `semantic_role` | Yes | **NO** - collected but ignored |

### Enhancement: Use Confidence and Business Concept

**File**: `entropy/detectors/semantic/business_meaning.py`

```python
def detect(self, context: DetectorContext) -> list[EntropyObject]:
    # Current: binary presence check
    # Enhanced: use confidence as weight

    confidence = semantic.get("confidence", 0.5)
    business_concept = semantic.get("business_concept")

    # Base score (current logic)
    if not has_description:
        base_score = score_missing
    elif not has_business_name and not has_entity_type:
        base_score = score_partial
    else:
        base_score = score_documented

    # NEW: Confidence adjustment
    # Low LLM confidence = higher entropy even if metadata present
    confidence_factor = 1.0 - (confidence * config.get("confidence_weight", 0.3))

    # NEW: Ontology bonus
    # Having business_concept = ontology alignment = lower entropy
    ontology_bonus = config.get("ontology_bonus", 0.1) if business_concept else 0.0

    score = (base_score * confidence_factor) - ontology_bonus
    score = max(0.0, min(1.0, score))
```

**Config** (`thresholds.yaml`):
```yaml
business_meaning:
  # Existing
  score_missing: 1.0
  score_partial: 0.6
  score_documented: 0.2
  # New
  confidence_weight: 0.3      # How much LLM confidence affects score
  ontology_bonus: 0.1         # Reduction if business_concept present
  min_confidence: 0.5         # Below this, treat as speculative
```

---

## Part 13: Verification Steps (Updated)

```bash
# Run specific phases (faster testing)
uv run dataraum run examples/data/ -o ./test_out --phase entropy
uv run dataraum run examples/data/ -o ./test_out --phase entropy_interpretation

# Test detectors in isolation
pytest tests/entropy/detectors/ -v -k "unit_entropy or temporal_entropy or relationship_entropy"

# Test full pipeline
uv run dataraum run examples/data/ -o ./test_out

# Verify contracts CLI shows recommendations
uv run dataraum contracts ./test_out --contract exploratory_analysis

# Verify query CLI with entropy context
uv run dataraum query "total revenue" -o ./test_out

# Check that YELLOW confidence is returned when entropy unavailable
uv run dataraum query "test" -o ./fresh_output_no_entropy  # No entropy phase run
```

---

## Part 14: Future Work (Out of Scope)

| Item | Reason |
|------|--------|
| `AggregationEntropyDetector` | Would need query analysis, not currently available |
| `FilterEntropyDetector` | Would need query analysis, not currently available |
| `RangeEntropyDetector` | Low value, could use statistics.min/max but unclear use case |

---

## Part 15: Architecture Refactor (Phase F) - Code Analysis

### Current Architecture (From Source Code Review)

#### 1. Detectors (`entropy/detectors/`)

Each detector produces `list[EntropyObject]` with:

```python
@dataclass
class EntropyObject:
    # Identity
    layer: str                          # "structural", "semantic", "value", "computational"
    dimension: str                      # "types", "business_meaning", "nulls", etc.
    sub_dimension: str                  # "type_fidelity", "naming_clarity", etc.
    target: str                         # "column:orders.amount"
    detector_id: str

    # Measurement
    score: float                        # 0.0-1.0
    confidence: float                   # How confident in this score

    # Evidence - RAW FACTS from analysis modules
    evidence: list[dict]                # e.g., [{"raw_metrics": {...}, "score_components": {...}}]

    # Resolution options - GENERIC ACTION TEMPLATES
    resolution_options: list[ResolutionOption]

    # Context (mostly empty in detector output)
    llm_context: LLMContext             # Rarely populated by detectors
    human_context: HumanContext         # Rarely populated by detectors
```

**Example from BusinessMeaningDetector:**
```python
evidence = [{
    "raw_metrics": {
        "description": "Total order amount",
        "has_description": True,
        "business_name": "Order Total",
        "semantic_confidence": 0.85,
    },
    "score_components": {
        "base_score": 0.2,
        "confidence_factor": 1.05,
        "ontology_bonus": 0.1,
    }
}]

resolution_options = [
    ResolutionOption(
        action="add_description",
        parameters={"column": "amount", "table": "orders"},
        expected_entropy_reduction=0.8,
        effort="low",
        description="Add a business description for this column",
    )
]
```

**Key insight:** Detectors DO produce resolution options, but they're **generic templates** (action + parameters + effort), not contextual recommendations.

#### 2. EntropyContext (`entropy/models.py` + `entropy/context.py`)

```python
@dataclass
class EntropyContext:
    # Per-column aggregates
    column_profiles: dict[str, ColumnEntropyProfile]    # Key: "table.column"

    # Per-table aggregates
    table_profiles: dict[str, TableEntropyProfile]      # Key: "table_name"

    # Per-relationship
    relationship_profiles: dict[str, RelationshipEntropyProfile]

    # Cross-dimension risks
    compound_risks: list[CompoundRisk]

    # LLM interpretations (optional)
    column_interpretations: dict[str, EntropyInterpretation]
```

**ColumnEntropyProfile stores BOTH raw objects AND aggregates:**
```python
@dataclass
class ColumnEntropyProfile:
    # Aggregated scores (computed from objects)
    structural_entropy: float           # Average of structural detectors
    semantic_entropy: float
    value_entropy: float
    computational_entropy: float
    composite_score: float              # Weighted average of above

    # Dimension breakdown
    dimension_scores: dict[str, float]  # Full dimension paths → scores

    # Raw objects (for evidence access)
    entropy_objects: list[EntropyObject]  # ← EVIDENCE LIVES HERE

    # Derived data
    high_entropy_dimensions: list[str]
    top_resolution_hints: list[ResolutionOption]

    # LLM interpretation (optional)
    interpretation: EntropyInterpretation | None
```

**How context is built (`_load_entropy_from_db`):**
1. Load `EntropyObjectRecord` from DB for table_ids
2. Group by column_id
3. Build `dimension_scores` dict from records
4. Calculate layer averages (structural, semantic, etc.)
5. Create `ColumnEntropyProfile` with all data
6. Aggregate columns into `TableEntropyProfile`

#### 3. Interpreter (`entropy/interpretation.py`)

**Input:**
```python
@dataclass
class InterpretationInput:
    table_name: str
    column_name: str
    detected_type: str
    business_description: str | None

    # Aggregated scores
    composite_score: float
    structural_entropy: float
    semantic_entropy: float
    value_entropy: float
    computational_entropy: float

    # From detectors (PARTIAL - loses some evidence during aggregation)
    raw_metrics: dict[str, Any]         # Subset of evidence
    high_entropy_dimensions: list[str]
    compound_risks: list[CompoundRisk]
```

**Output:**
```python
@dataclass
class EntropyInterpretation:
    # LLM-generated contextual content
    assumptions: list[Assumption]        # What we're assuming about the data
    resolution_actions: list[ResolutionAction]  # Contextual recommendations
    explanation: str                     # Human-readable summary
```

**Key insight:** Interpreter receives `raw_metrics` but this is **built during context.py:_build_interpretations()**, not directly from detector evidence. Some evidence may be lost.

#### 4. Contract Evaluation (`entropy/contracts.py`)

```python
def evaluate_contract(entropy_context: EntropyContext, contract_name: str):
    for dimension, max_score in contract.dimension_thresholds.items():
        # Gets score from column profiles
        actual_score = _get_dimension_score(entropy_context, dimension)
        # → Looks up profile.dimension_scores[dimension] or layer score
```

**Data flow for contracts:**
- Uses `EntropyContext.column_profiles[col].dimension_scores`
- Falls back to layer-level scores (structural_entropy, etc.)
- Produces `ContractEvaluation` with `ConfidenceLevel` (GREEN/YELLOW/ORANGE/RED)

#### 5. Query Agent (`query/agent.py`)

**ISSUE: Entropy built TWICE:**
```python
# Line 154-161: Build execution context (calls build_entropy_context internally)
execution_context = build_execution_context(session, table_ids, duckdb_conn)

# Line 164-171: Build entropy context AGAIN
entropy_context = build_entropy_context(session, table_ids)
```

**Usage:**
- `entropy_context` → Contract evaluation, entropy_score calculation
- `execution_context.entropy_summary` → Prompt formatting (via `format_entropy_for_prompt`)

#### 6. GraphExecutionContext (`graphs/context.py`)

**Calls `build_entropy_context()` internally (line 423) and COPIES to dicts:**
```python
entropy_context = build_entropy_ctx(session, table_ids)

# Copy to dict lookups (line 426-433)
column_entropy_lookup: dict[str, dict[str, Any]] = {}
for col_key, col_entropy_profile in entropy_context.column_profiles.items():
    column_entropy_lookup[col_key] = get_column_entropy_summary(col_entropy_profile)

# Store as summary dict (line 451-457)
entropy_summary_dict = {
    "overall_readiness": entropy_context.overall_readiness,
    "high_entropy_count": entropy_context.high_entropy_count,
    # ...
}
```

**GraphExecutionContext stores dicts, not EntropyContext:**
```python
@dataclass
class GraphExecutionContext:
    entropy_summary: dict[str, Any] | None = None  # ← Dict copy, not reference

@dataclass
class ColumnContext:
    entropy_scores: dict[str, Any] | None = None   # ← Dict copy, not reference
```

---

### Problems Identified

| Problem | Location | Impact |
|---------|----------|--------|
| **Double entropy build** | `query/agent.py:154-171` | Wasted DB queries, potential inconsistency |
| **Dict copying** | `graphs/context.py:426-457` | Loses type safety, fragments access |
| **Evidence partially lost** | `context.py:_build_interpretations()` | Interpreter doesn't get full detector evidence |
| **Duplicate aggregation** | `ColumnEntropyProfile` + `TableEntropyProfile` | Redundant computation, complex data model |
| **LLMContext/HumanContext unused** | `EntropyObject` | Detectors don't populate these, adds complexity |

---

### Proposed Architecture

#### Keep: EntropyObject (Rename to EntropyMeasurement)

Detectors produce measurements with:
- `score`, `evidence`, `resolution_options` - **KEEP** (these are valuable)
- `LLMContext`, `HumanContext` - **REMOVE** (unused by detectors, Interpreter generates these)

```python
@dataclass
class EntropyMeasurement:
    # Identity
    dimension_path: str         # "structural.types.type_fidelity"
    target: str                 # "column:orders.amount"
    detector_id: str

    # Measurement
    score: float                # 0.0-1.0
    confidence: float           # 0.0-1.0

    # Evidence - FULL RAW FACTS
    evidence: list[dict]

    # Generic resolution templates
    resolution_options: list[ResolutionOption]
```

#### Keep: ResolutionOption (unchanged)

```python
@dataclass
class ResolutionOption:
    action: str                         # "add_description", "declare_unit"
    parameters: dict[str, Any]          # {"column": "amount", "table": "orders"}
    expected_entropy_reduction: float   # 0.8
    effort: str                         # "low", "medium", "high"
    description: str                    # Generic description
```

#### Simplify: EntropyContext (Data Only)

`EntropyContext` provides **data access only** - no thresholds, no policy decisions.

```python
@dataclass
class EntropyContext:
    """Raw entropy data. No policy/thresholds - that's ContractEvaluation's job."""

    measurements: list[EntropyMeasurement]
    interpretations: dict[str, EntropyInterpretation] = field(default_factory=dict)

    # ─────────────────────────────────────────────────────────────
    # Score Access (used by contract evaluation)
    # ─────────────────────────────────────────────────────────────

    def get_dimension_score(self, dimension_path: str) -> float:
        """Average score for a dimension across all targets."""

    def get_compound_risks(self) -> list[CompoundRisk]:
        """Compute compound risks from measurements.

        Risk definitions come from config (not thresholds - those are in contract).
        """

    # ─────────────────────────────────────────────────────────────
    # Evidence Access (used by interpreter)
    # ─────────────────────────────────────────────────────────────

    def get_measurements_for_target(self, target: str) -> list[EntropyMeasurement]:
        """All measurements for a target, with full evidence.

        Interpreter needs evidence to generate contextual assumptions:
        e.g., "detected_unit=None, semantic_role=measure" → "Assuming USD"
        """

    # ─────────────────────────────────────────────────────────────
    # Serialization (used by API)
    # ─────────────────────────────────────────────────────────────

    def to_dict(self) -> dict[str, Any]:
        """Raw data for API. Shape evolves with UI needs."""
```

#### Enhance: ContractEvaluation (Policy + Thresholds)

Threshold-aware methods belong on `ContractEvaluation` because "high entropy" is relative to the contract:
- `executive_dashboard`: > 0.3 is high (strict)
- `exploratory_analysis`: > 0.7 is high (lenient)

```python
@dataclass
class ContractEvaluation:
    """Result of applying a contract's policy to entropy data."""

    # Existing fields
    contract_name: str
    contract_display_name: str
    is_compliant: bool
    confidence_level: ConfidenceLevel
    overall_score: float
    dimension_scores: dict[str, float]
    violations: list[Violation]
    warnings: list[Violation]

    # ─────────────────────────────────────────────────────────────
    # Threshold-aware methods (use THIS contract's thresholds)
    # ─────────────────────────────────────────────────────────────

    def get_high_entropy_dimensions(self) -> list[str]:
        """Dimensions exceeding this contract's thresholds."""

    def get_overall_readiness(self) -> str:
        """'ready', 'investigate', or 'blocked' per this contract."""

    def get_readiness_blockers(self) -> list[str]:
        """Targets blocking readiness per this contract's thresholds."""

    def has_critical_risks(self) -> bool:
        """Whether compound risks violate this contract."""
```

**Separation of concerns:**
| Class | Responsibility |
|-------|----------------|
| `EntropyContext` | Raw data + evidence access |
| `ContractEvaluation` | Policy applied to data (threshold-aware) |

#### New Folder: `entropy/context/`

Organize context-related code into a dedicated folder:

```
entropy/
├── context/                        # NEW - Context module
│   ├── __init__.py                 # Exports EntropyContext, EntropyMeasurement, build_entropy_context
│   ├── models.py                   # EntropyMeasurement, CompoundRisk, CompoundRiskDefinition
│   ├── entropy_context.py          # EntropyContext class with all view methods
│   ├── builder.py                  # build_entropy_context (loads from DB, runs detectors)
│   └── formatting.py               # format_entropy_for_prompt, format_for_dashboard
│
├── interpretation.py               # EntropyInterpreter (LLM-powered, stays separate)
├── contracts.py                    # Contract evaluation (uses EntropyContext)
├── config.py                       # EntropyConfig (threshold config)
├── db_models.py                    # EntropyObjectRecord (DB storage)
│
├── detectors/                      # Unchanged
│   ├── base.py                     # EntropyDetector, DetectorRegistry
│   ├── structural/
│   ├── semantic/
│   ├── value/
│   └── computational/
│
└── compound_risk.py                # REMOVE - logic moves to EntropyContext.get_compound_risks()
```

**Migration:**
- `models.py` → Split: measurement classes to `context/models.py`, keep `ResolutionOption` in root
- `context.py` → Split: builder to `context/builder.py`, formatting to `context/formatting.py`
- `compound_risk.py` → Delete: logic becomes `EntropyContext.get_compound_risks()` method

#### Remove: Profile Classes

- ~~ColumnEntropyProfile~~ → `EntropyContext.get_column_score()` + `get_measurements_for_target()`
- ~~TableEntropyProfile~~ → `EntropyContext` computed views
- ~~RelationshipEntropyProfile~~ → Measurements with `target="relationship:..."`

#### Keep: Interpreter (Essential)

```python
class EntropyInterpreter:
    def interpret_batch(
        self,
        session: Session,
        entropy_context: EntropyContext,  # Now passes full context
        targets: list[str],               # Which targets to interpret
        query: str | None = None,         # Optional query context
    ) -> Result[dict[str, EntropyInterpretation]]:
        """
        For each target:
        1. Get all measurements via entropy_context.get_measurements_for_target()
        2. Get all evidence via entropy_context.get_all_evidence_for_target()
        3. Pass to LLM with query context
        4. Generate contextual assumptions and resolutions
        """
```

**Interpreter receives FULL evidence** because it accesses `EntropyMeasurement.evidence` directly.

#### Update: GraphExecutionContext

```python
@dataclass
class GraphExecutionContext:
    # Reference to entropy, not copy
    entropy_context: EntropyContext | None = None

    # Remove: entropy_summary dict
    # Remove: column_entropy_lookup dict
    # Remove: table_entropy_lookup dict

    # Access via methods
    def get_column_entropy_score(self, table: str, column: str) -> float | None:
        if self.entropy_context:
            return self.entropy_context.get_column_score(table, column)
        return None
```

#### Update: Query Agent

```python
def analyze(self, ...):
    # Build execution context (includes entropy_context reference)
    execution_context = build_execution_context(session, table_ids, duckdb_conn)

    # Use the SAME entropy context for everything
    entropy_context = execution_context.entropy_context

    # Contract evaluation
    if entropy_context:
        contract_evaluation = evaluate_contract(entropy_context, contract)
        confidence_level = contract_evaluation.confidence_level

    # Prompt formatting uses same context
    entropy_warnings = format_entropy_for_prompt(entropy_context)
```

---

### Data Flow Summary

```
┌──────────────────────────────────────────────────────────────────────┐
│                           Detectors                                   │
│  TypeFidelityDetector, BusinessMeaningDetector, NullRatioDetector... │
└──────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                     list[EntropyMeasurement]
                     (score + evidence + resolution_options)
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    EntropyContext (Data Only)                         │
│                                                                       │
│  Storage:                                                             │
│    measurements: list[EntropyMeasurement]                             │
│    interpretations: dict[str, EntropyInterpretation]                 │
│                                                                       │
│  Data Access (no thresholds):                                         │
│    get_dimension_score(path) → float                                 │
│    get_compound_risks() → list[CompoundRisk]                         │
│    get_measurements_for_target(target) → list[EntropyMeasurement]    │
│    to_dict() → dict                                                   │
└──────────────────────────────────────────────────────────────────────┘
            │                       │                       │
            ▼                       ▼                       ▼
┌─────────────────────┐    ┌─────────────────┐    ┌─────────────────────┐
│ Contract Evaluation │    │ EntropyInter-   │    │ GraphExecution-     │
│                     │    │ preter          │    │ Context             │
│ evaluate_contract() │    │                 │    │                     │
│ applies thresholds  │    │ get_measurements│    │ entropy_context ref │
│ from contract       │    │ _for_target()   │    │                     │
└─────────────────────┘    │ → full evidence │    └─────────────────────┘
            │               └─────────────────┘              │
            ▼                       │                        ▼
┌─────────────────────┐             ▼               Prompt Formatting
│ ContractEvaluation  │    EntropyInterpretation   (uses evaluation)
│ (Policy Applied)    │    (assumptions,
│                     │     resolutions)
│ Threshold-aware:    │
│ get_high_entropy_   │
│   dimensions()      │
│ get_overall_        │
│   readiness()       │
│ has_critical_risks()│
│ confidence_level    │
└─────────────────────┘
```

**Separation of concerns:**
- `EntropyContext` = raw data + evidence (no policy)
- `ContractEvaluation` = policy applied (threshold-aware methods)
- Thresholds are defined by the contract, not hardcoded

---

### Files to Modify (Phase F)

#### New Files (context folder)

| File | Purpose |
|------|---------|
| `entropy/context/__init__.py` | Export public API: `EntropyContext`, `EntropyMeasurement`, `build_entropy_context` |
| `entropy/context/models.py` | `EntropyMeasurement`, `CompoundRisk`, `CompoundRiskDefinition` |
| `entropy/context/entropy_context.py` | `EntropyContext` class with all view methods |
| `entropy/context/builder.py` | `build_entropy_context()` - loads from DB, runs detectors |
| `entropy/context/formatting.py` | `format_entropy_for_prompt()`, `format_for_dashboard()` |

#### Modified Files

| File | Changes |
|------|---------|
| `entropy/models.py` | Keep only `ResolutionOption`, remove profile classes |
| `entropy/detectors/base.py` | Update `create_entropy_object()` → `create_measurement()` |
| `entropy/detectors/*.py` | Update all 9 detectors to return `EntropyMeasurement` |
| `entropy/interpretation.py` | Accept `EntropyContext`, access measurements directly for full evidence |
| `entropy/contracts.py` | Add threshold-aware methods to `ContractEvaluation`: `get_high_entropy_dimensions()`, `get_overall_readiness()`, `get_readiness_blockers()`, `has_critical_risks()` |
| `graphs/context.py` | Store `entropy_context: EntropyContext` reference, remove dict copies |
| `query/agent.py` | Use `execution_context.entropy_context`, remove duplicate build |
| `api/routers/entropy.py` | Build responses from `EntropyContext.to_dict()` + `ContractEvaluation` |
| `api/schemas.py` | Simplify entropy response schemas |

#### Deleted Files

| File | Reason |
|------|--------|
| `entropy/context.py` | Split into `context/builder.py` + `context/formatting.py` |
| `entropy/compound_risk.py` | Logic moved to `EntropyContext.get_compound_risks()` |

### Verification (Phase F)

```bash
# Detectors produce valid measurements
pytest tests/entropy/detectors/ -v

# Context views compute correctly
pytest tests/entropy/test_context.py -v

# Interpreter gets full evidence
pytest tests/entropy/test_interpretation.py -v

# Contracts evaluate correctly
pytest tests/entropy/test_contracts.py -v

# Query agent uses single context
pytest tests/query/test_agent.py -v

# Full pipeline
uv run dataraum run examples/data/ -o ./test_out
uv run dataraum query "total revenue" -o ./test_out
```

### Migration Notes

1. **DB schema unchanged** - EntropyObjectRecord stays the same
2. **API may need updates** - Some response shapes may change
3. **Backward compat** - Can provide adapter methods during transition
