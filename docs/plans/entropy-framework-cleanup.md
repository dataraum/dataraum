# Plan: Entropy Framework Cleanup & Extension

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

### Phase A: Fix Critical Agent Issues (High Priority)
1. **Fix query/agent.py line 409** - use correct entropy_context variable
2. **Fix query/agent.py confidence default** - YELLOW when entropy unavailable
3. **Fix query/agent.py entropy_score** - use None instead of 0.0
4. **Fix prompt defaults** - "Data quality assessment not available" instead of "No warnings"

### Phase B: Add Missing Detectors
5. **Add UnitEntropyDetector** - uses typing.detected_unit
6. **Add TemporalEntropyDetector** - uses semantic.semantic_role
7. **Add RelationshipEntropyDetector** - uses JoinCandidate evaluation metrics
8. **Update context.py** - wire detector to RelationshipEntropyProfile

### Phase C: Enhance Existing Detectors
9. **Enhance BusinessMeaningDetector** - use confidence and business_concept

### Phase D: Fix Configuration
10. **Fix thresholds.yaml** - align config with detectors
11. **Fix contracts.yaml** - use valid dimension paths
12. **Remove fallbacks** - fail-fast behavior

### Phase E: Enrich Output
13. **Enrich CLI** - show resolution recommendations
14. **Fix API** - return full resolution details

### Phase F: Cleanup
15. **Remove dead code** - compound risks for non-existent dimensions, hallucinated entropy

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
