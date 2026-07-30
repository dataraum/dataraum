"""Transformation graph models.

Metric graphs:
    - Output: scalar/series values with units
    - Steps: extractions, formulas, aggregations
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

# =============================================================================
# Enums
# =============================================================================


class GraphSource(StrEnum):
    """Source of the graph definition."""

    SYSTEM = "system"  # Built-in system graphs
    USER = "user"  # User-defined graphs
    LLM = "llm"  # LLM-generated graphs
    TEACH = "teach"  # Created via teach(type="metric")


class StepType(StrEnum):
    """Type of graph step."""

    EXTRACT = "extract"  # Pull data from source
    CONSTANT = "constant"  # Fixed or parameterized value
    FORMULA = "formula"  # Calculate derived value


class OutputType(StrEnum):
    """Type of graph output."""

    SCALAR = "scalar"  # Single value
    SERIES = "series"  # Time series or array
    TABLE = "table"  # Multi-column result


# =============================================================================
# Graph Definition Models
# =============================================================================


@dataclass
class GraphMetadata:
    """Metadata about a transformation graph."""

    name: str
    description: str
    category: str  # working_capital, profitability, liquidity, ...
    source: GraphSource
    created_by: str | None = None
    created_at: str | None = None
    tags: list[str] = field(default_factory=list)
    inspiration_snippet_id: str | None = None  # For snippet promotion via teach


@dataclass
class ParameterDef:
    """Definition of a user-configurable parameter."""

    name: str
    param_type: str  # integer, float, date, boolean, string
    default: Any
    description: str | None = None
    options: list[Any] | None = None  # For enum-like parameters
    # A DECLARED marker naming the rule a parameter's value belongs to (DAT-732):
    # ``period_grain`` for ``days_in_period``, NULL for a plain constant. Declared in the
    # vertical YAML (a DATA marker, never inferred by name in engine code) and persisted
    # to the ``metric_parameters`` typed home / ``og_metric_parameters`` as graph
    # structure. It is NOT read by the resolver at runtime — ``period_resolver`` keys its
    # override on the parameter NAME today, not this marker (see MetricParameterDerivation).
    derivation: str | None = None  # MetricParameterDerivation value, or None


@dataclass
class StepSource:
    """Data source for an extract step."""

    table: str | None = None  # Concrete table name
    column: str | None = None  # Concrete column name
    standard_field: str | None = None  # Abstract field (resolved by schema mapping)
    statement: str | None = None  # balance_sheet, income_statement
    # The DECLARED row restriction this extract measures over (DAT-838). Before it,
    # an extract carried only field+statement+aggregation, so "count the rows WHERE
    # status is reconciled" was unsayable and the model wrote the closest expressible
    # thing — `count(reconciliation_status)`, a count of a dimension column, which
    # counts every row and makes any rate ≈1.0 by construction while its declared
    # `0 <= value <= 1` check passes. Rates, shares, ratios-of-subset and conditional
    # counts are all that shape, so this is a large fraction of what a practitioner
    # means by "metric".
    #
    # It is INTENT, in business terms ("status is reconciled"), never SQL and never a
    # column/value pair: at frame time the real columns and their values are not known
    # yet — grounding a value to a column that actually carries it is the semantic
    # phase's business. The authoring path serves this line to the grounding agent,
    # which turns it into verified predicates over SERVED values
    # (`ExtractGroundingOutput.where` + the `filter_members` it must declare), and
    # `validate_grounding_basis` refuses a grounding that dropped it.
    #
    # `""` (never None) means "no restriction" — the same stated-attribute convention
    # `statement` and `ConceptGroundingBasis.filter` use, so the LLM-facing contract
    # needs no optional field (constrained decoding budgets those; DAT-807).
    predicate: str = ""


@dataclass
class StepValidation:
    """A declared post-execution check on a step's value (DAT-616).

    From the catalogue's per-extract ``validation:`` block, e.g.
    ``{condition: "value > 0", severity: "error", message: "Revenue must be positive"}``.
    Enforced by :func:`dataraum.graphs.verifier.verify_execution` against the
    executed value — execution-pass is not validation. Before DAT-616 the loader
    dropped this block on the floor; it is now parsed into this model.
    """

    condition: str  # comparison over `value`, e.g. "value > 0" / "value >= 0"
    severity: str = "error"
    message: str = ""


@dataclass
class GraphStep:
    """A single step in a transformation graph."""

    step_id: str
    step_type: StepType

    # For extract steps
    source: StepSource | None = None
    aggregation: str | None = None  # sum, avg, min, max, count, count_distinct, end_of_period

    # For constant steps
    value: Any | None = None
    parameter: str | None = None  # Reference to a parameter

    # For formula steps
    expression: str | None = None

    # Dependencies
    depends_on: list[str] = field(default_factory=list)

    # Output marker
    output_step: bool = False

    # Declared post-execution checks (catalogue `validation:` block, DAT-616)
    validations: list[StepValidation] = field(default_factory=list)


@dataclass
class OutputDef:
    """Definition of graph output."""

    output_type: OutputType
    metric_id: str | None = None
    unit: str | None = None  # days, currency, ratio, count, percentage
    decimal_places: int | None = None


@dataclass
class InterpretationRange:
    """Interpretation range for metric values."""

    min_value: float
    max_value: float
    label: str
    description: str


@dataclass
class Interpretation:
    """Interpretation rules for metric output."""

    ranges: list[InterpretationRange] = field(default_factory=list)


@dataclass
class TransformationGraph:
    """A metric transformation graph."""

    graph_id: str
    version: str

    metadata: GraphMetadata
    output: OutputDef
    steps: dict[str, GraphStep]

    # Optional
    parameters: list[ParameterDef] = field(default_factory=list)
    interpretation: Interpretation | None = None

    def get_output_step(self) -> GraphStep | None:
        """Get the final output step."""
        for step in self.steps.values():
            if step.output_step:
                return step
        return None


# =============================================================================
# Execution Result Models
# =============================================================================


@dataclass
class StepResult:
    """Result of executing a single step."""

    step_id: str

    # Value (polymorphic based on step type)
    value_scalar: float | None = None
    value_boolean: bool | None = None
    value_string: str | None = None
    value_list: list[Any] | None = None

    # Trace information
    inputs_used: dict[str, Any] = field(default_factory=dict)
    source_query: str | None = None

    @property
    def value(self) -> Any:
        """Get the value in its native type."""
        if self.value_scalar is not None:
            return self.value_scalar
        if self.value_boolean is not None:
            return self.value_boolean
        if self.value_string is not None:
            return self.value_string
        if self.value_list is not None:
            return self.value_list
        return None


class AssumptionBasis(StrEnum):
    """Basis for an assumption made during query execution."""

    SYSTEM_DEFAULT = "system_default"  # Default from system configuration
    INFERRED = "inferred"  # Inferred from context or data patterns
    USER_SPECIFIED = "user_specified"  # Explicitly set by user


@dataclass
class QueryAssumption:
    """An assumption made during query execution due to data entropy.

    Tracks assumptions the agent makes when data has uncertainty,
    allowing them to be reviewed, corrected, or promoted to permanent rules.
    """

    assumption_id: str
    execution_id: str

    # What was assumed
    dimension: str  # e.g., "semantic.units", "structural.relations"
    target: str  # e.g., "column:orders.amount", "relationship:orders->customers"
    assumption: str  # Human-readable: "Currency is EUR"

    # Basis for assumption
    basis: AssumptionBasis
    confidence: float  # 0.0 to 1.0

    @classmethod
    def create(
        cls,
        execution_id: str,
        dimension: str,
        target: str,
        assumption: str,
        basis: AssumptionBasis,
        confidence: float,
    ) -> QueryAssumption:
        """Create a new assumption with generated ID."""
        return cls(
            assumption_id=str(uuid4()),
            execution_id=execution_id,
            dimension=dimension,
            target=target,
            assumption=assumption,
            basis=basis,
            confidence=confidence,
        )


@dataclass
class GraphExecution:
    """Result of executing a transformation graph."""

    execution_id: str
    graph_id: str
    source: GraphSource

    # Step results (used internally for snippet saving)
    step_results: list[StepResult] = field(default_factory=list)

    # Output
    output_value: Any = None
    output_interpretation: str | None = None

    # DAT-616: the single self-contained CTE statement executed (steps composed +
    # final_sql) — the metric's executable artifact, mirroring the answer agent's
    # composed grid SQL. Ephemeral like output_value (durable knowledge = snippets).
    composed_sql: str | None = None

    # Assumptions made during execution (populated from LLM output)
    assumptions: list[QueryAssumption] = field(default_factory=list)

    # Declared-expectation violations from the verifier (DAT-699): the metric
    # EXECUTED; these surface on the artifact as visible state_reason flags
    # (execute-and-flag) — never a reason the number was refused.
    verification_flags: list[str] = field(default_factory=list)

    @classmethod
    def create(cls, graph: TransformationGraph) -> GraphExecution:
        """Create a new execution for a graph."""
        return cls(
            execution_id=str(uuid4()),
            graph_id=graph.graph_id,
            source=graph.metadata.source,
        )


# =============================================================================
# Pydantic models for the LLM structured outputs
# =============================================================================


class GraphAssumptionOutput(BaseModel):
    """An assumption made during graph SQL generation."""

    dimension: str = Field(description="Entropy dimension (e.g., 'semantic.units', 'value.nulls')")
    target: str = Field(description="What the assumption applies to (e.g., 'column:orders.amount')")
    assumption: str = Field(description="Human-readable assumption (e.g., 'Currency is EUR')")
    # Typed with the existing AssumptionBasis enum (contract v2, DAT-727): an
    # off-vocabulary basis is a ValidationError → the schema-repair turn fixes it,
    # instead of the old silent string map-with-INFERRED-fallback in the agent.
    basis: AssumptionBasis = Field(
        description="Basis for assumption: 'system_default', 'inferred', or 'user_specified'"
    )
    confidence: float = Field(
        ge=0.0, le=1.0, description="Confidence in this assumption (0.0 to 1.0)"
    )


class FilterMember(BaseModel):
    """One dimension member a grounding's WHERE predicates select (DAT-787).

    The typed ``(column, value)`` pair the operating-model graph's ``filtered_by``
    edge un-nests into a ``DimMember`` vertex — the VALUE analog of
    ``filter_columns`` (which types only the columns a filter touches, never which
    values it selects). Same enforced-at-authoring discipline as the rest of
    contract v2: the model declares it, and parsing the rendered SQL as a *source*
    of the value stays forbidden — the value comes from the model's own committed
    selection, cross-checked (never sourced) against the SQL.

    Enforced at save (``validate_grounding_basis``): ``column`` must be one of this
    concept's ``filter_columns``; ``value`` must be a SERVED value where a complete
    served value-set exists for the column (the served representation is the
    equality anchor — ``'posted'`` is not ``'Posted'``; no casing/whitespace
    folding); and ``value`` must appear in the rendered ``where`` predicates (a
    lexical cross-check, VALIDATOR-only). The graph emits a member + edge only when
    ``column`` resolves to a REFERENCED dimension axis (DAT-788 role-separated
    identity); a filter on a non-dimension or folded column contributes no member —
    ``where[]`` stays lossless on the grounding vertex regardless.

    Frozen at authoring: an existing grounding carries no ``filter_members`` until
    it is re-authored on a later run (honest absence — no edges, never a
    retroactively-improved row). No backfill; a dedicated update surface and the
    full-axis (all-members, not just referenced) enumeration are out of scope
    (DAT-864).
    """

    model_config = ConfigDict(extra="forbid")

    column: str = Field(
        description="The relation column the predicate filters on — one of this concept's "
        "filter_columns, a bare served name, no table qualifier.",
    )
    value: str = Field(
        description="The exact served value the predicate selects on that column, verbatim "
        "from its Value set (the equality anchor). ONE member per selected value: an IN-list "
        "of three values is three filter_members.",
    )


class ConceptGroundingBasis(BaseModel):
    """How ONE concept grounds to the relation's columns — provenance contract v2 (DAT-727).

    The typed substrate of the operating-model graph's ``uses`` edge (ADR-0021):
    the model ENUMERATES every relation column its grounding touches, by role,
    under the same evidence-first discipline as the ``grounding`` field. This is
    strictly typed, ENFORCED data — validated at save against the served
    relation schema plus a completeness cross-check of the emitted SQL parts,
    with a repair turn on violation (``validate_grounding_basis``); parsing the
    rendered SQL as a *source* of these names is forbidden by design (the parse
    is at most a validator). ``extra="forbid"`` keeps the v1 free-form keys
    (``column``, ``resolution``) from silently surviving — this is a clean
    contract cut, no backfill (pre-v2 rows simply yield no ``uses`` edges).

    No ``resolution`` field: the v1 key was the per-concept twin of the
    provenance-level ``field_resolution`` DAT-781 deleted as write-only —
    self-reported, read by nothing. ``filter`` stays: it IS the value→concept
    decision ``_build_prior_context`` feeds back (DAT-616) and the cockpit
    answer agent reuses. ``filter_members`` (DAT-787) types the VALUE side of
    that decision — the operating-model graph's ``filtered_by`` substrate,
    un-nested exactly as ``uses`` un-nests the column arrays.
    """

    model_config = ConfigDict(extra="forbid")

    measure_columns: list[str] = Field(
        description="Every relation column the select_expr reads for this concept — bare "
        "column names exactly as served in the schema, no table qualifier. [] only when "
        "the extract reads no column for it (e.g. COUNT(*)).",
    )
    filter_columns: list[str] = Field(
        description="Every relation column the where predicates filter on for this concept — "
        "bare served column names, no qualifier. [] when the concept needs no filter.",
    )
    filter: str = Field(
        description="The exact served values the filter selects (the value→concept decision), "
        'e.g. "account_type IN (\'revenue\')". "" when unfiltered.',
    )
    filter_members: list[FilterMember] = Field(
        description="The specific dimension members the where predicates select — one "
        "{column, value} per selected value (an IN-list of N values is N members). [] when "
        "the concept needs no filter, or filters only on a non-dimension column. Each column "
        "must be one of filter_columns; each value must be a served value (DAT-787). The graph "
        "turns each into a DimMember + filtered_by edge when the column is a dimension axis.",
    )


class ConceptGroundingEntry(BaseModel):
    """ONE concept's grounding record — the list element of ``column_mappings_basis``.

    A list of ``{concept, basis}`` entries, not a ``{concept: basis}`` map:
    constrained decoding requires ``additionalProperties: false`` on every object,
    which FORBIDS an open map outright (DAT-807). The map shape is restored at the
    persistence boundary, so ``HealthySnippetProvenance`` — and therefore the
    ``og_uses`` element view and the cockpit — see the unchanged stored shape.
    """

    model_config = ConfigDict(extra="forbid")

    concept: str = Field(
        description="The business concept this entry grounds, named verbatim from the "
        "graph specification."
    )
    basis: ConceptGroundingBasis = Field(
        description="How this concept grounds to the relation's columns."
    )


class GraphProvenanceOutput(BaseModel):
    """Provenance of how the LLM grounded business concepts to SQL."""

    column_mappings_basis: list[ConceptGroundingEntry] = Field(
        description="Per-concept grounding record, one entry per concept — enumerates ALL "
        "relation columns each grounding touches, by role (see ConceptGroundingBasis). "
        "Enforced against the served schema at save. One entry per concept: do NOT "
        "repeat a concept. [] only in the fall-loud case.",
    )

    @model_validator(mode="after")
    def _concepts_are_unique(self) -> GraphProvenanceOutput:
        """No concept may appear twice — the list must round-trip to a map.

        The map shape this replaced (DAT-807) made duplicates impossible by
        construction. A list does not: two entries for one concept would both
        pass ``validate_grounding_basis`` (it iterates the list, so completeness
        is checked over their union) and then silently collapse to the last one
        at the persistence boundary — dropping ``uses`` edges validation had
        just certified. Reject it here instead, where the contract-repair turn
        can still fix it.
        """
        seen = [e.concept for e in self.column_mappings_basis]
        duplicates = sorted({c for c in seen if seen.count(c) > 1})
        if duplicates:
            raise ValueError(
                f"column_mappings_basis repeats {duplicates} — emit exactly one "
                "entry per concept, merging its columns"
            )
        return self

    # No free-text reasoning field: the former `llm_reasoning` was written into the
    # snippet provenance blob and read by nothing (DAT-603 consumer audit) — output
    # tokens are serial-decode latency, so an unread sentence per call is pure cost.
    # `field_resolution` (direct/inferred) was the same class of write-only field —
    # emitted by the LLM, stamped into provenance, read by nothing persisted
    # (DAT-781). Contract v2 (DAT-727) keeps it deleted — its per-concept twin
    # (`resolution`) is likewise not typed here.


class SnippetFailureMode(StrEnum):
    """Closed vocabulary of retained-failure causes (DAT-543 / DAT-727).

    The failed half of the sql_snippets provenance contract — one value per
    actual writer path in ``GraphAgent``:

    - ``EXECUTION_FAILED`` — the authored SQL failed to run.
    - ``VERIFIER_REJECTED`` — the SQL ran clean but the DAT-616 verifier rejected
      the VALUE (e.g. negative against ``value >= 0``, or NULL "no support").
    - ``PROVENANCE_INVALID`` — the grounding's column enumeration stayed in
      violation of contract v2 after its repair turn (DAT-727): the SQL may be
      fine, but the operating-model graph cannot ground ``uses`` edges on an
      unenforced enumeration.
    - ``DISJOINT_COLLISION`` — the extract is byte-identical (modulo SQL syntax
      noise) to the one grounded for a concept this one is ``disjoint_with``
      (DAT-709). Disjoint concepts cannot select the same rows, so at most one
      of the two groundings can be right and nothing here can tell which: the
      SQL may execute and verify perfectly and still be the wrong concept's.
      Written by the cross-concept guard, never by a single grounding call —
      it is the ONE failure mode no per-concept check can reach.
    """

    EXECUTION_FAILED = "execution_failed"
    VERIFIER_REJECTED = "verifier_rejected"
    PROVENANCE_INVALID = "provenance_invalid"
    DISJOINT_COLLISION = "disjoint_collision"


class SnippetAssumption(BaseModel):
    """One persisted assumption record inside a snippet's provenance blob.

    The DAT-631 confidence gate reads these back from provenance so a metric
    ASSEMBLED from cache still surfaces its weakest grounding's confidence.
    """

    model_config = ConfigDict(extra="forbid")

    # The authored assumption's DIMENSION (DAT-887) — the kind of judgment it records
    # ('scope.validity', 'period.binding', …). Persisted because it is what makes the
    # grading surface FILTERABLE: an eval attributing period error must be able to
    # select the period-binding assumptions without pattern-matching prose. Defaults to
    # "" rather than being optional — every writer supplies it from the authored
    # GraphAssumptionOutput, and pre-DAT-887 rows simply carry no dimension.
    dimension: str = ""
    assumption: str
    basis: AssumptionBasis
    confidence: float = Field(ge=0.0, le=1.0)


class HealthySnippetProvenance(BaseModel):
    """The ENTIRE provenance payload of a healthy sql_snippets row (DAT-727).

    Built exclusively by ``GraphAgent._build_snippet_provenance`` — the engine's
    only sql_snippets writer — and un-nested by the operating-model graph's
    ``og_uses`` element view, so the persisted shape IS this model's
    ``model_dump``: ``{column_mappings_basis: {concept: {measure_columns[],
    filter_columns[], filter, filter_members: [{column, value}]}},
    assumptions: [{assumption, basis, confidence}]}``.

    Deliberately still a MAP, while the LLM-facing ``GraphProvenanceOutput`` is a
    LIST of ``{concept, basis}`` entries (DAT-807 — constrained decoding forbids an
    open map). The writer converts, so the STRUCTURE ``og_uses`` un-nests and the
    cockpit reads is unchanged.

    One VALUE-level change: an unfiltered concept now stores ``filter: ""`` where
    it stored ``filter: null``, because ``ConceptGroundingBasis`` is shared
    between the wire and this payload and the wire model states every attribute.
    No engine or cockpit code branches on that distinction (``og_uses`` un-nests
    the COLUMN arrays, not this string; the cockpit passes it through as opaque
    context into the answer agent's prompt), so it is a rendering difference —
    but it IS a difference, and a reader that treats "" and null differently
    would see it. No backfill: pre-DAT-807 rows keep their nulls.
    """

    model_config = ConfigDict(extra="forbid")

    column_mappings_basis: dict[str, ConceptGroundingBasis] = Field(default_factory=dict)
    assumptions: list[SnippetAssumption] = Field(default_factory=list)


class NoSupportClass(StrEnum):
    """Evidence-backed cause classes for a no-support rejection (DAT-658).

    ``verify_execution`` can only MEASURE "aggregated to NULL" — it is
    structurally blind to the cause (DAT-699: no context, no connection, no
    value sets). The cause IS determinable where the execution outcome and the
    served evidence are both in scope, so ``classify_no_support`` (agent.py)
    computes one of these deterministically — a COUNT probe over the extract's
    own clause parts plus the served value-set reference, never an LLM — and
    the class travels WITH the rejection: the verifier's message, the retained
    row, and the next authoring's guidance all name one measured cause instead
    of enumerating the possibility space.

    - ``PREDICATE_MATCHED_NO_ROWS`` — the filter selected zero rows, and
      absence of the concept could NOT be evidenced (a declared value IS
      served, or no complete enumeration screens the filter columns — the
      known un-screened gap, classified honestly as revisable). A different
      predicate over the served values may ground it.
    - ``OPERAND_ALL_NULL`` — rows matched the filter, but the aggregated
      operand is NULL over every one of them (the one-sided two-operand
      extract, DAT-699's second case). Revisable: row-guarded NULL-safe
      combination of the operands.
    - ``CONCEPT_ABSENT`` — every value the grounding declared for the concept
      is missing from its column's COMPLETE served enumeration: the served
      data carries no value for the concept's family, so no predicate over
      the served values can ever select it. NON-REVISABLE — re-authoring SQL
      against it is churn by construction; only new data changes the verdict.
    - ``COMPOSITION_ABSTAINED`` — the SYSTEM withheld composition (DAT-893:
      e.g. no reporting instant could be placed on the served relation); the
      rejected SQL is the fall-loud ``SELECT NULL`` the model never authored,
      so the NULL says nothing about the grounding itself.
    """

    PREDICATE_MATCHED_NO_ROWS = "predicate_matched_no_rows"
    OPERAND_ALL_NULL = "operand_all_null"
    CONCEPT_ABSENT = "concept_absent"
    COMPOSITION_ABSTAINED = "composition_abstained"


@dataclass(frozen=True)
class NoSupportFinding:
    """One NULL-aggregate step's classified cause + the evidence that backs it.

    The in-memory carrier between ``classify_no_support`` (which does the IO),
    ``verify_execution`` (which names the class in its rejection — data in, no
    IO), and ``_save_failed_snippet`` (which persists the pair onto
    ``FailedSnippetProvenance``). Always constructed together — a class without
    its evidence cannot exist, mirroring the persisted model's validator.
    """

    reason_class: NoSupportClass
    evidence: str


class FailedSnippetProvenance(BaseModel):
    """The ENTIRE provenance payload of a retained-failure sql_snippets row (DAT-543).

    Read back by ``_build_prior_context`` (the exact prior SQL + why it was
    rejected) and by the cockpit's ungroundable-node detail. Carries no column
    enumeration — a failed grounding yields no ``uses`` edges by construction.

    ``no_support_class`` / ``no_support_evidence`` (DAT-658) type the CAUSE of a
    no-support rejection, always as a pair — the validator below is the
    chokepoint (the additivity status/verdict/reason discipline; provenance is
    JSON, so no DB CHECK can repeat it). DAT-893's ``composition_abstain``
    field is FOLDED into this vocabulary (``no_support_class:
    composition_abstained``, evidence = the abstain reason): a nullable abstain
    string BESIDE a typed cause class would give every consumer two parallel
    reads for the one question "why did this grounding fail", and each engine
    reader is dict-keyed (no model round-trip on read), so the fold is a clean
    cut — pre-fold rows keep their old key, read by nothing.
    """

    model_config = ConfigDict(extra="forbid")

    failure_mode: SnippetFailureMode
    failure_reason: str
    no_support_class: NoSupportClass | None = None
    no_support_evidence: str | None = None

    @model_validator(mode="after")
    def _class_is_evidence_backed(self) -> FailedSnippetProvenance:
        """A class requires its evidence, and both qualify ONLY the verifier's rejection.

        The mode invariant generalizes DAT-893's ("the abstain accompanies only
        the verifier's rejection"): every class in the vocabulary explains a
        no-support NULL, which only the verifier gate produces — on any other
        mode the grounding failed for its own reason, and a cause class there
        would contradict the mode and misdirect the retry guidance.
        """
        if self.no_support_class is not None:
            if not self.no_support_evidence:
                raise ValueError(
                    f"no_support_class {self.no_support_class!r} must carry its evidence — "
                    "a class is a measured finding, never an assertion"
                )
            if self.failure_mode is not SnippetFailureMode.VERIFIER_REJECTED:
                raise ValueError(
                    "no_support_class qualifies the verifier's no-support rejection only; "
                    f"mode {self.failure_mode!r} failed for its own reason"
                )
        elif self.no_support_evidence is not None:
            raise ValueError("no_support_evidence without a class is not a finding")
        return self


class ValueSearchInput(BaseModel):
    """Input for the grounding agent's bounded catalog search (DAT-699).

    High-cardinality discriminators (above the complete-enumeration bound) are
    served as size + sample, never enumerated — the exact values live behind
    THIS tool. The agent resolves them by substring search and grounds its
    IN-list on the results instead of guessing an ILIKE predicate or falling
    loud on a concept whose values it was never shown (observed live: concepts
    present by name in a several-hundred-value column were unreachable).
    """

    table: str = Field(description="Table name exactly as shown in <data_schema>.")
    column: str = Field(description="Column name exactly as shown in <data_schema>.")
    pattern: str = Field(
        description="Case-insensitive substring to search the column's distinct "
        "values for — a fragment of the concept's likely name or a synonym. "
        "Plain text, not a SQL pattern."
    )


class ExtractGroundingOutput(BaseModel):
    """The structured output for grounding ONE extract leaf to SQL (DAT-603).

    The authoring path grounds exactly one EXTRACT per call (DAT-646), so the
    output is one SQL statement — not the retired full-graph shape (steps[] with
    model-chosen step_ids + final_sql). The caller binds the SQL to the graph's
    own leaf id, which structurally removes the step-id-paraphrase failure class
    (DAT-664: Sonnet 5 echoing `revenue` back as `revenue_extract` silently
    skipped snippet persistence).

    Every field here has a named consumer (DAT-603 schema audit), and the
    consumer can be the MODEL ITSELF at generation time: `grounding` is emitted
    FIRST, deliberately — it forces the model to commit to the served evidence
    before writing SQL (field order is generation order). The original audit
    cut the old `summary` field as "decorative" by counting only downstream
    readers and missed this consumer class; don't repeat that. Downstream:
    the CLAUSE PARTS (`relation`, `where`, `select_expr` — DAT-671
    parts-at-source: the parts ARE the artifact; the fused statement is
    rendered ONCE by `compose_extract_sql` and persisted alongside them, and
    the cockpit drill builder composes every variant from the parts without
    ever parsing SQL); `description` is the snippet's human line (cockpit
    reuse KB, compose-path descriptions); `assumptions` feed the DAT-631
    confidence gate + the answer UI; `provenance` feeds prior_context
    (DAT-616) and the confidence gate — its `column_mappings_basis` is THE
    per-concept grounding record (a flat `column_mappings` duplicate was
    removed 2026-07-03: the prompt stopped teaching it in DAT-636 and it had
    been silently empty since), and since contract v2 (DAT-727) also the
    operating-model graph's `uses` substrate: a typed enumeration of every
    relation column the grounding touches, ENFORCED at save against the
    served schema (`validate_grounding_basis` — membership + completeness,
    one repair turn, fall-loud on a still-invalid output).
    """

    grounding: str = Field(
        description="FIRST, commit to the evidence: which column grounds the concept, and "
        "the EXACT served values you selected from its Value set (or the complete "
        "discriminator you filter on) — e.g. \"volume via category = 'primary' "
        '(complete 5-value set)". Name the value-set entries verbatim; if you cannot, '
        "the concept is not grounded — fall loud instead of writing SQL around it."
    )
    relation: str = Field(
        description="The ONE relation the extract reads — an enriched view or table name "
        'verbatim from the provided schema, never invented. "" ONLY in the fall-loud '
        "case (the concept cannot be grounded).",
    )
    where: list[str] = Field(
        description="Row filters as standalone SQL predicate texts over the relation's "
        "columns, AND-composed by the system. Every literal must be verified against the "
        "served Value sets. A predicate may contain subqueries (e.g. an IN over a "
        "reference table). Empty when the concept needs no filter.",
    )
    select_expr: str = Field(
        description="The scalar value expression over the relation's columns using the "
        "step's aggregation, WITHOUT an alias (the system renders it AS value) — e.g. "
        '"SUM(gross) - SUM(fees)". In the fall-loud case: "NULL".',
    )
    description: str = Field(
        description="One short line: what this extract computes and how it is filtered, "
        "e.g. 'Total volume: SUM(amount) where category IN (Primary)'."
    )
    assumptions: list[GraphAssumptionOutput] = Field(
        description="Assumptions made due to data uncertainty during SQL generation; [] when none",
    )
    provenance: GraphProvenanceOutput = Field(
        description="How the concept was grounded to concrete columns. Required for a real "
        "grounding (non-null relation): column_mappings_basis must enumerate EVERY relation "
        "column the select_expr/where touch, by role, using served names verbatim — it is "
        "validated against the served schema. In the fall-loud case emit an empty "
        "column_mappings_basis.",
    )
