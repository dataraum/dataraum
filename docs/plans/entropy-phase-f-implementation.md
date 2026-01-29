# Phase F: Entropy Framework Architecture Refactor - Implementation Plan

## Executive Summary

Phase F completes the entropy framework cleanup by simplifying the data model and establishing clear separation of concerns. The key changes:

1. **EntropyMeasurement** replaces EntropyObject as the single detector output
2. **EntropyContext** becomes data-only with generic target-based access (no profile classes)
3. **ContractEvaluation** owns all threshold-aware methods (policy separated from data)
4. **Typed tables only** - all entropy operations filter to `Table.layer == "typed"`
5. **No backward compatibility** - callers are adapted directly
6. **No detector-specific methods** - generic access via target, consumers extract what they need

## Current Problems

### 1. Duplicate Context Building
**Location**: `query/agent.py:154-171`
```python
# Builds execution_context (which internally builds entropy_context)
execution_context = build_execution_context(session, typed_table_ids, duckdb_conn)

# Then builds entropy_context AGAIN
entropy_context = build_entropy_context(session, table_ids)  # BUG: Uses all table_ids!
```
- Double DB queries
- Uses ALL table_ids instead of typed_table_ids for entropy

### 2. Dict Copying Instead of References
**Location**: `graphs/context.py:426-457`
```python
# Copies to dict instead of keeping reference
column_entropy_lookup: dict[str, dict[str, Any]] = {}
for col_key, col_entropy_profile in entropy_context.column_profiles.items():
    column_entropy_lookup[col_key] = get_column_entropy_summary(col_entropy_profile)
```
- Loses type safety
- Creates stale copies
- Fragments access patterns

### 3. Evidence Partially Lost
**Location**: `context.py:_build_interpretations()`
- InterpretationInput receives subset of evidence
- Interpreter doesn't get full detector evidence

### 4. Redundant Profile Classes
**Location**: `models.py`
- `ColumnEntropyProfile` (155-228)
- `TableEntropyProfile` (230-313)
- `RelationshipEntropyProfile` (315-354)
- All store aggregated scores that could be computed views

### 5. LLMContext/HumanContext Unused
**Location**: `models.py:33-76`, `EntropyObject:132-133`
- Detectors rarely populate these
- Interpreter generates contextual content
- Adds complexity without value

### 6. Typed Tables Not Enforced
**Locations**:
- `api/routers/entropy.py:41-43` - Gets ALL tables
- `query/agent.py:165-168` - Uses wrong table_ids for entropy
- `graphs/context.py:423` - Doesn't filter

---

## Target Architecture

### Data Flow

```
┌──────────────────────────────────────────────────────────────────────┐
│                           Detectors                                   │
│  Each detector produces measurements for its dimension_path:          │
│  - TypeFidelityDetector → "structural.types.type_fidelity"           │
│  - RelationshipEntropyDetector → 4 measurements:                      │
│      "structural.relations.cardinality"                               │
│      "structural.relations.join_path"                                 │
│      "structural.relations.referential_integrity"                     │
│      "structural.relations.semantic_clarity"                          │
└──────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                     list[EntropyMeasurement]
                     - dimension_path: "layer.dimension.sub_dimension"
                     - target: "column:table.col" or "relationship:a.x->b.y"
                     - score, evidence, resolution_options
                                    │
                    ┌───────────────┴───────────────┐
                    │       DB Persistence          │
                    │   EntropyObjectRecord         │
                    └───────────────┬───────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    EntropyContext (Data Only)                         │
│                                                                       │
│  Storage:                                                             │
│    measurements: list[EntropyMeasurement]                             │
│    interpretations: dict[str, EntropyInterpretation]                 │
│                                                                       │
│  Generic Access (NO detector-specific methods):                       │
│    get_targets_by_type(type) → list[str]                             │
│    get_measurements_for_target(target) → list[EntropyMeasurement]    │
│    get_dimension_scores(target) → dict[str, float]                   │
│    get_composite_score(target) → float                               │
│    get_compound_risks() → list[CompoundRisk]                         │
│    to_dict() → dict                                                   │
└──────────────────────────────────────────────────────────────────────┘
            │                       │                       │
            ▼                       ▼                       ▼
┌─────────────────────┐    ┌─────────────────┐    ┌─────────────────────┐
│ Contract Evaluation │    │ EntropyInter-   │    │ GraphExecution-     │
│ (Policy Applied)    │    │ preter          │    │ Context             │
│                     │    │                 │    │                     │
│ Uses dimension_     │    │ get_measurements│    │ entropy_context ref │
│ scores + contract   │    │ _for_target()   │    │ (not dict copy)     │
│ thresholds          │    │ → full evidence │    │                     │
│                     │    └─────────────────┘    │ Extracts what it    │
│ Threshold-aware:    │             │             │ needs from          │
│ - is_compliant      │             ▼             │ dimension_scores()  │
│ - confidence_level  │    EntropyInterpretation  └─────────────────────┘
│ - violations        │
│ - get_high_entropy_ │
│   dimensions()      │
│ - get_readiness()   │
└─────────────────────┘
```

### Key Principles

1. **EntropyContext** = raw data + generic access (NO thresholds, NO detector-specific methods)
2. **ContractEvaluation** = policy applied (threshold-aware)
3. **Single source of truth** - measurements only, no profile classes
4. **Reference, not copy** - GraphExecutionContext holds EntropyContext reference
5. **Typed tables only** - enforced at build time (Table.layer == "typed")
6. **Consumers extract** - relationship entropy, layer scores computed from dimension_scores by consumer

---

## Implementation Phases

### Phase F.1: Create Context Module Structure

**New folder**: `entropy/context/`

```
entropy/
├── context/                        # NEW
│   ├── __init__.py                 # Exports: EntropyContext, EntropyMeasurement, build_entropy_context
│   ├── models.py                   # EntropyMeasurement only (CompoundRisk stays in models.py)
│   ├── entropy_context.py          # EntropyContext class with generic access methods
│   ├── builder.py                  # build_entropy_context() - loads from DB, filters typed tables
│   └── formatting.py               # format_entropy_for_prompt(), format_for_dashboard()
```

#### F.1.1: `context/models.py`

```python
@dataclass
class EntropyMeasurement:
    """Single entropy measurement from a detector.

    Simplified from EntropyObject:
    - Removed LLMContext/HumanContext (Interpreter generates these)
    - dimension_path combines layer.dimension.sub_dimension

    This is the ONLY data class for entropy measurements. No profile classes.
    """

    # Identity
    measurement_id: str = field(default_factory=lambda: str(uuid4()))
    dimension_path: str = ""  # "structural.types.type_fidelity"
    target: str = ""  # "column:orders.amount" or "relationship:orders.id->customers.id"
    detector_id: str = ""

    # Measurement
    score: float = 0.0  # 0.0 = deterministic, 1.0 = maximum uncertainty
    confidence: float = 1.0  # How confident in this score

    # Evidence - FULL detector output
    evidence: list[dict[str, Any]] = field(default_factory=list)

    # Resolution options - generic templates
    resolution_options: list[ResolutionOption] = field(default_factory=list)

    # Metadata
    computed_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    source_analysis_ids: list[str] = field(default_factory=list)

    @property
    def layer(self) -> str:
        """Extract layer from dimension_path (structural, semantic, value, computational)."""
        return self.dimension_path.split(".")[0] if self.dimension_path else ""

    @property
    def target_type(self) -> str:
        """Return 'column', 'table', or 'relationship'."""
        return self.target.split(":")[0] if ":" in self.target else "unknown"

    @property
    def target_key(self) -> str:
        """Return the target identifier without type prefix."""
        return self.target.split(":", 1)[1] if ":" in self.target else self.target


# NOTE: No RelationshipMetrics class - consumers extract what they need
# from dimension_scores using get_dimension_scores(target)
```

#### F.1.2: `context/entropy_context.py`

```python
@dataclass
class EntropyContext:
    """Raw entropy data with generic access methods.

    NO thresholds - that's ContractEvaluation's job.
    NO detector-specific methods - consumers extract what they need.
    """

    # Primary storage
    measurements: list[EntropyMeasurement] = field(default_factory=list)
    interpretations: dict[str, EntropyInterpretation] = field(default_factory=dict)

    # Metadata
    computed_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    table_names: list[str] = field(default_factory=list)  # Typed tables included

    # ─────────────────────────────────────────────────────────────
    # Discovery
    # ─────────────────────────────────────────────────────────────

    def get_targets_by_type(self, target_type: str) -> list[str]:
        """Get all targets of a type ('column', 'relationship', 'table').

        Returns full target strings, e.g., ['column:orders.amount', ...]
        """
        prefix = f"{target_type}:"
        return sorted(set(m.target for m in self.measurements
                         if m.target.startswith(prefix)))

    # ─────────────────────────────────────────────────────────────
    # Generic Access (works for ANY target type)
    # ─────────────────────────────────────────────────────────────

    def get_measurements_for_target(self, target: str) -> list[EntropyMeasurement]:
        """All measurements for any target, with full evidence."""
        return [m for m in self.measurements if m.target == target]

    def get_dimension_scores(self, target: str) -> dict[str, float]:
        """All dimension scores for any target.

        Returns: {"structural.types.type_fidelity": 0.3, "semantic.units.unit_declaration": 0.8, ...}
        """
        measurements = self.get_measurements_for_target(target)
        return {m.dimension_path: m.score for m in measurements}

    def get_composite_score(self, target: str) -> float:
        """Weighted composite score for any target."""
        measurements = self.get_measurements_for_target(target)
        if not measurements:
            return 0.0
        return self._compute_composite_score(measurements)

    # ─────────────────────────────────────────────────────────────
    # Compound Risks (computed from measurements)
    # ─────────────────────────────────────────────────────────────

    def get_compound_risks(self) -> list[CompoundRisk]:
        """Compute compound risks from measurements.

        Uses risk definitions from config (dimensions + threshold + multiplier).
        """
        from dataraum.entropy.config import get_entropy_config
        config = get_entropy_config()

        risks = []
        for target in self.get_targets_by_type("column"):
            dimension_scores = self.get_dimension_scores(target)

            for risk_def in config.compound_risk_definitions:
                # Check if all dimensions exceed threshold
                if all(dimension_scores.get(d, 0.0) >= risk_def.threshold
                       for d in risk_def.dimensions):
                    risk = CompoundRisk.from_scores(
                        target=target,
                        dimensions=risk_def.dimensions,
                        scores=dimension_scores,
                        risk_level=risk_def.risk_level,
                        impact=risk_def.impact_template,
                        multiplier=risk_def.multiplier,
                    )
                    risks.append(risk)
        return risks

    # ─────────────────────────────────────────────────────────────
    # Serialization
    # ─────────────────────────────────────────────────────────────

    def to_dict(self) -> dict[str, Any]:
        """Serialize for API responses."""
        # Group by target type
        result: dict[str, Any] = {
            "computed_at": self.computed_at.isoformat(),
            "table_names": self.table_names,
            "targets": {},
            "compound_risks": [
                {
                    "risk_id": r.risk_id,
                    "target": r.target,
                    "dimensions": r.dimensions,
                    "risk_level": r.risk_level,
                    "combined_score": r.combined_score,
                }
                for r in self.get_compound_risks()
            ],
        }

        # Add all targets with their dimension scores
        all_targets = set(m.target for m in self.measurements)
        for target in sorted(all_targets):
            # Extract key for interpretation lookup
            target_key = target.split(":", 1)[1] if ":" in target else target
            result["targets"][target] = {
                "composite_score": self.get_composite_score(target),
                "dimension_scores": self.get_dimension_scores(target),
                "interpretation": (
                    self.interpretations[target_key].to_dashboard_dict()
                    if target_key in self.interpretations else None
                ),
            }

        return result

    # ─────────────────────────────────────────────────────────────
    # Internal helpers
    # ─────────────────────────────────────────────────────────────

    def _compute_composite_score(self, measurements: list[EntropyMeasurement]) -> float:
        """Compute weighted composite from measurements."""
        from dataraum.entropy.config import get_entropy_config
        config = get_entropy_config()
        weights = config.composite_weights

        layer_scores = {}
        for layer in ["structural", "semantic", "value", "computational"]:
            layer_measurements = [m for m in measurements if m.layer == layer]
            if layer_measurements:
                layer_scores[layer] = sum(m.score for m in layer_measurements) / len(layer_measurements)
            else:
                layer_scores[layer] = 0.0

        return (
            layer_scores["structural"] * weights["structural"]
            + layer_scores["semantic"] * weights["semantic"]
            + layer_scores["value"] * weights["value"]
            + layer_scores["computational"] * weights["computational"]
        )
```

#### F.1.3: `context/builder.py`

```python
def build_entropy_context(
    session: Session,
    table_ids: list[str],
    *,
    interpreter: EntropyInterpreter | None = None,
) -> EntropyContext:
    """Build entropy context for the given tables.

    IMPORTANT: Only processes typed tables (layer == "typed").

    Args:
        session: SQLAlchemy session
        table_ids: List of table IDs (will be filtered to typed only)
        interpreter: Optional for LLM interpretation

    Returns:
        EntropyContext with measurements loaded from DB

    Raises:
        ValueError: If no entropy data exists
    """
    from dataraum.storage import Table

    # CRITICAL: Filter to typed tables only
    typed_stmt = select(Table).where(
        Table.table_id.in_(table_ids),
        Table.layer == "typed",
    )
    typed_tables = session.execute(typed_stmt).scalars().all()
    typed_table_ids = [t.table_id for t in typed_tables]
    typed_table_names = [t.table_name for t in typed_tables]

    if not typed_table_ids:
        return EntropyContext(table_names=[])

    # Load measurements from DB
    measurements = _load_measurements_from_db(session, typed_table_ids)
    if not measurements:
        raise ValueError(
            "No entropy data found. Run the entropy pipeline phase first: "
            "`dataraum run <source> --phase entropy`"
        )

    context = EntropyContext(
        measurements=measurements,
        table_names=typed_table_names,
    )

    # Load relationship measurements
    relationship_measurements = _load_relationship_measurements(session, typed_table_ids)
    context.measurements.extend(relationship_measurements)

    # Build interpretations if interpreter provided
    if interpreter is not None:
        _build_interpretations(session, context, interpreter)

    return context


def _load_measurements_from_db(
    session: Session,
    table_ids: list[str],
) -> list[EntropyMeasurement]:
    """Load entropy measurements from database."""
    from dataraum.entropy.db_models import EntropyObjectRecord
    from dataraum.storage import Column, Table

    # Load entropy records
    stmt = select(EntropyObjectRecord).where(EntropyObjectRecord.table_id.in_(table_ids))
    records = session.execute(stmt).scalars().all()

    # Load table/column info for target keys
    tables = session.execute(select(Table).where(Table.table_id.in_(table_ids))).scalars().all()
    table_map = {t.table_id: t.table_name for t in tables}

    columns = session.execute(select(Column).where(Column.table_id.in_(table_ids))).scalars().all()
    column_map = {c.column_id: (table_map.get(c.table_id, ""), c.column_name) for c in columns}

    measurements = []
    for record in records:
        # Build target key
        if record.column_id:
            table_name, col_name = column_map.get(record.column_id, ("", ""))
            target = f"column:{table_name}.{col_name}"
        else:
            target = f"table:{table_map.get(record.table_id, record.table_id)}"

        # Build dimension path
        dimension_path = f"{record.layer}.{record.dimension}.{record.sub_dimension}"

        # Convert resolution options
        resolution_options = []
        if record.resolution_options:
            for opt in record.resolution_options:
                resolution_options.append(ResolutionOption(
                    action=opt.get("action", ""),
                    parameters=opt.get("parameters", {}),
                    expected_entropy_reduction=opt.get("expected_entropy_reduction", 0.0),
                    effort=opt.get("effort", "medium"),
                    description=opt.get("description", ""),
                    cascade_dimensions=opt.get("cascade_dimensions", []),
                ))

        measurements.append(EntropyMeasurement(
            measurement_id=record.object_id,
            dimension_path=dimension_path,
            target=target,
            detector_id=record.detector_id,
            score=record.score,
            confidence=record.confidence,
            evidence=record.evidence or [],
            resolution_options=resolution_options,
            computed_at=record.computed_at,
            source_analysis_ids=record.source_analysis_ids or [],
        ))

    return measurements
```

---

### Phase F.2: Update Detectors

#### F.2.1: Update `detectors/base.py`

Change `create_entropy_object()` to `create_measurement()`:

```python
def create_measurement(
    self,
    context: DetectorContext,
    score: float,
    evidence: list[dict[str, Any]],
    resolution_options: list[ResolutionOption] | None = None,
    confidence: float = 1.0,
) -> EntropyMeasurement:
    """Create an entropy measurement for this detector's dimension.

    Args:
        context: Detector context with target info
        score: Entropy score (0.0-1.0)
        evidence: Raw facts from analysis
        resolution_options: Suggested fixes
        confidence: Confidence in this score

    Returns:
        EntropyMeasurement ready for persistence
    """
    return EntropyMeasurement(
        dimension_path=f"{self.layer}.{self.dimension}.{self.sub_dimension}",
        target=context.target_ref,
        detector_id=self.detector_id,
        score=score,
        confidence=confidence,
        evidence=evidence,
        resolution_options=resolution_options or [],
        source_analysis_ids=context.source_analysis_ids,
    )
```

#### F.2.2: Update All 11 Detectors

Each detector changes from:
```python
return [self.create_entropy_object(context, score, evidence, resolution_options)]
```

To:
```python
return [self.create_measurement(context, score, evidence, resolution_options)]
```

Files to update:
- `structural/types.py`
- `structural/relations.py`
- `structural/relationship_entropy.py`
- `semantic/business_meaning.py`
- `semantic/unit_entropy.py`
- `semantic/temporal_entropy.py`
- `value/null_semantics.py`
- `value/outliers.py`
- `computational/derived_values.py`

---

### Phase F.3: Update ContractEvaluation

Add threshold-aware methods to `contracts.py`:

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

    # Reference to entropy context (for threshold-aware methods)
    _entropy_context: EntropyContext | None = field(default=None, repr=False)
    _contract: ContractProfile | None = field(default=None, repr=False)

    def get_high_entropy_dimensions(self) -> list[str]:
        """Dimensions exceeding THIS contract's thresholds."""
        if not self._contract:
            return []
        return [
            dim for dim, score in self.dimension_scores.items()
            if score >= self._contract.dimension_thresholds.get(dim, 1.0)
        ]

    def get_overall_readiness(self) -> str:
        """'ready', 'investigate', or 'blocked' per THIS contract."""
        if self.confidence_level == ConfidenceLevel.RED:
            return "blocked"
        elif self.confidence_level in (ConfidenceLevel.ORANGE, ConfidenceLevel.YELLOW):
            return "investigate"
        return "ready"

    def get_readiness_blockers(self) -> list[str]:
        """Targets blocking readiness per THIS contract's thresholds."""
        return [v.target for v in self.violations if v.is_blocking]

    def has_critical_risks(self) -> bool:
        """Whether compound risks violate THIS contract."""
        if not self._entropy_context:
            return False
        for risk in self._entropy_context.get_compound_risks():
            if risk.risk_level == "critical":
                return True
        return False
```

---

### Phase F.4: Update Consumers

#### F.4.1: Update `query/agent.py`

```python
def analyze(self, ...):
    # Filter to typed tables
    typed_table_ids = [
        t.table_id for t in session.execute(
            select(Table).where(
                Table.table_id.in_(table_ids),
                Table.layer == "typed",
            )
        ).scalars().all()
    ]

    if not typed_table_ids:
        return Result.fail("No typed tables found. Run the typing phase first.")

    # Build execution context ONCE (includes entropy_context)
    execution_context = build_execution_context(
        session=session,
        table_ids=typed_table_ids,
        duckdb_conn=duckdb_conn,
    )

    # Use entropy_context from execution_context (NO SEPARATE BUILD)
    entropy_context = execution_context.entropy_context

    # ... rest of method uses entropy_context ...
```

#### F.4.2: Update `graphs/context.py`

```python
@dataclass
class GraphExecutionContext:
    # ... existing fields ...

    # Store reference to EntropyContext (NOT dict copy)
    entropy_context: EntropyContext | None = None

    # REMOVE: entropy_summary dict - use entropy_context directly
    # REMOVE: column entropy dicts - use entropy_context methods


def build_execution_context(...) -> GraphExecutionContext:
    # ... existing code ...

    # Build entropy context (already filters to typed internally)
    entropy_context = build_entropy_ctx(session, table_ids)

    # Update relationship contexts with entropy data
    # Consumer extracts what it needs using generic API
    for rel_ctx in relationships:
        target = f"relationship:{rel_ctx.from_table}.{rel_ctx.from_column}->{rel_ctx.to_table}.{rel_ctx.to_column}"
        dim_scores = entropy_context.get_dimension_scores(target)

        if dim_scores:  # Has entropy data for this relationship
            composite = entropy_context.get_composite_score(target)
            rel_ctx.relationship_entropy = {
                "composite_score": composite,
                "cardinality_entropy": dim_scores.get("structural.relations.cardinality", 0.0),
                "join_path_entropy": dim_scores.get("structural.relations.join_path", 0.0),
                "referential_integrity_entropy": dim_scores.get("structural.relations.referential_integrity", 0.0),
                "semantic_clarity_entropy": dim_scores.get("structural.relations.semantic_clarity", 0.0),
                "is_deterministic": composite < 0.5,
                "join_warning": f"Join has entropy {composite:.2f}" if composite >= 0.5 else None,
            }

    # Update column contexts with entropy data
    for table_ctx in table_contexts:
        for col_ctx in table_ctx.columns:
            target = f"column:{table_ctx.table_name}.{col_ctx.column_name}"
            col_ctx.entropy_scores = {
                "composite_score": entropy_context.get_composite_score(target),
                "dimension_scores": entropy_context.get_dimension_scores(target),
                # Consumer computes layer breakdown if needed from dimension_scores
            }

    return GraphExecutionContext(
        # ... other fields ...
        entropy_context=entropy_context,  # Store reference
    )
```

#### F.4.3: Update `api/routers/entropy.py`

```python
@router.get("/entropy/{source_id}", response_model=EntropyDashboardResponse)
def get_entropy_dashboard(source_id: str, session: SessionDep):
    # Get TYPED tables only
    tables_stmt = select(Table).where(
        Table.source_id == source_id,
        Table.layer == "typed",  # CRITICAL: Filter to typed
    )
    tables = list(session.execute(tables_stmt).scalars().all())

    if not tables:
        return EntropyDashboardResponse(...)

    table_ids = [t.table_id for t in tables]

    # Build entropy context
    entropy_context = build_entropy_context(session, table_ids)

    # Use to_dict() for API response
    return entropy_context.to_dict()
```

---

### Phase F.5: Update Interpretation

#### F.5.1: Update `interpretation.py`

```python
def interpret_batch(
    self,
    session: Session,
    entropy_context: EntropyContext,
    targets: list[str] | None = None,
    query: str | None = None,
) -> Result[dict[str, EntropyInterpretation]]:
    """Interpret entropy for multiple targets.

    Args:
        session: SQLAlchemy session
        entropy_context: Full context with measurements
        targets: Specific targets to interpret (None = all columns)
        query: Optional query for context-aware interpretation

    Returns:
        Dict mapping target keys to interpretations
    """
    if targets is None:
        # Use generic discovery - get all column targets
        targets = entropy_context.get_targets_by_type("column")

    # Build inputs with FULL evidence
    inputs = []
    for target in targets:
        measurements = entropy_context.get_measurements_for_target(target)
        if not measurements:
            continue

        # Full evidence from all measurements
        all_evidence = []
        for m in measurements:
            all_evidence.extend(m.evidence)

        inputs.append(InterpretationInput(
            target=target,
            composite_score=entropy_context.get_composite_score(target),
            dimension_scores=entropy_context.get_dimension_scores(target),
            measurements=measurements,  # Full measurements with evidence
            all_evidence=all_evidence,
        ))

    # ... LLM call ...
```

---

### Phase F.6: Remove Deprecated Code

#### Files to Delete

1. `entropy/compound_risk.py` - Logic moved to `EntropyContext.get_compound_risks()`

#### Classes to Remove from `models.py`

1. `LLMContext` - Unused by detectors
2. `HumanContext` - Unused by detectors
3. `ColumnEntropyProfile` - Use `get_dimension_scores(target)` + `get_composite_score(target)`
4. `TableEntropyProfile` - Consumer aggregates from column scores
5. `RelationshipEntropyProfile` - Consumer extracts from `get_dimension_scores(target)`

#### Keep in `models.py`

1. `ResolutionOption` - Essential
2. `CompoundRisk` - Essential (used by EntropyContext)
3. `CompoundRiskDefinition` - Config loading
4. `ResolutionCascade` - Resolution prioritization

#### Update `context.py`

- Move to `context/builder.py`
- Delete `_compute_relationship_entropy()` - replaced by detector + computed views
- Simplify `_build_interpretations()` - uses EntropyContext directly

---

## Implementation Order

### Step 1: Create Context Module (Non-Breaking)
- Create `entropy/context/` folder
- Add `models.py` with `EntropyMeasurement` only
- Add `entropy_context.py` with `EntropyContext` (generic access methods)
- Add `builder.py` with typed table filtering
- Add `formatting.py` with prompt helpers

### Step 2: Update Detectors (Non-Breaking)
- Add `create_measurement()` to base.py (keep `create_entropy_object()` as alias)
- Update detectors one by one to use new method

### Step 3: Update DB Models
- Ensure `EntropyObjectRecord` can serialize `EntropyMeasurement`

### Step 4: Update Consumers
- Update `graphs/context.py` to use new EntropyContext
- Update `query/agent.py` to use single context
- Update `api/routers/entropy.py` to filter typed tables

### Step 5: Update Contracts
- Add threshold-aware methods to `ContractEvaluation`

### Step 6: Update Interpretation
- Modify to use `EntropyContext.get_measurements_for_target()`

### Step 7: Remove Deprecated Code
- Delete old profile classes from `models.py`
- Delete `compound_risk.py`
- Clean up imports

---

## Testing Strategy

### Unit Tests

```bash
# Test new context module
pytest tests/entropy/context/ -v

# Test detectors produce EntropyMeasurement
pytest tests/entropy/detectors/ -v

# Test EntropyContext computed views
pytest tests/entropy/test_entropy_context.py -v

# Test contract evaluation with new context
pytest tests/entropy/test_contracts.py -v
```

### Integration Tests

```bash
# Full pipeline with typed table filtering
uv run dataraum run examples/data/ -o ./test_out

# Verify only typed tables processed
uv run dataraum query "total revenue" -o ./test_out

# Verify contracts work
uv run dataraum contracts ./test_out --contract exploratory_analysis
```

### Verification Checklist

- [ ] Only typed tables have entropy computed
- [ ] `get_composite_score(target)` returns correct weighted average
- [ ] `get_dimension_scores(target)` returns all dimension paths and scores
- [ ] Compound risks computed from measurements
- [ ] Relationship entropy extracted via `get_dimension_scores("relationship:...")`
- [ ] GraphExecutionContext stores reference, not dict copy
- [ ] Query agent builds context once
- [ ] API returns only typed table entropy
- [ ] Interpreter receives full evidence via `get_measurements_for_target()`
- [ ] Contract evaluation uses context methods

---

## Files Summary

### New Files (6)

| File | Purpose |
|------|---------|
| `entropy/context/__init__.py` | Public exports |
| `entropy/context/models.py` | EntropyMeasurement only |
| `entropy/context/entropy_context.py` | EntropyContext with generic access methods |
| `entropy/context/builder.py` | build_entropy_context() with typed table filtering |
| `entropy/context/formatting.py` | Prompt formatting helpers |
| `tests/entropy/context/test_entropy_context.py` | Unit tests |

### Modified Files (15)

| File | Changes |
|------|---------|
| `entropy/models.py` | Remove profile classes, keep ResolutionOption/CompoundRisk |
| `entropy/detectors/base.py` | Add create_measurement() |
| `entropy/detectors/structural/types.py` | Use create_measurement() |
| `entropy/detectors/structural/relations.py` | Use create_measurement() |
| `entropy/detectors/structural/relationship_entropy.py` | Use create_measurement() |
| `entropy/detectors/semantic/business_meaning.py` | Use create_measurement() |
| `entropy/detectors/semantic/unit_entropy.py` | Use create_measurement() |
| `entropy/detectors/semantic/temporal_entropy.py` | Use create_measurement() |
| `entropy/detectors/value/null_semantics.py` | Use create_measurement() |
| `entropy/detectors/value/outliers.py` | Use create_measurement() |
| `entropy/detectors/computational/derived_values.py` | Use create_measurement() |
| `entropy/contracts.py` | Add threshold-aware methods |
| `entropy/interpretation.py` | Use EntropyContext directly |
| `graphs/context.py` | Store entropy_context reference |
| `query/agent.py` | Single context, typed tables |
| `api/routers/entropy.py` | Filter typed tables |

### Deleted Files (2)

| File | Reason |
|------|--------|
| `entropy/context.py` | Replaced by `context/` folder |
| `entropy/compound_risk.py` | Logic in EntropyContext |

---

## Risk Mitigation

### Risk: Breaking API Responses
**Mitigation**: `EntropyContext.to_dict()` produces compatible shape

### Risk: Missing Entropy Data
**Mitigation**: `build_entropy_context()` raises clear error if no data

### Risk: Performance Regression
**Mitigation**: Computed views are simple operations; compound risks cached

### Risk: Interpreter Loses Evidence
**Mitigation**: `get_measurements_for_target()` returns full measurements
