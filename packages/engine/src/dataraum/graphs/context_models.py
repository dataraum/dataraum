"""Served-context data model (DAT-869 split of ``graphs/context.py``).

The dataclasses the GraphAgent's served context is assembled into. Pure data —
no reads, no rendering: ``context_reads`` populates these from the operating-
model property graph plus the typed rows, and ``context_format`` renders them
for the grounding prompt.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dataraum.analysis.cycles.health import HealthReport
    from dataraum.graphs.field_mapping import ColumnMeaning

# =============================================================================
# Context Models
# =============================================================================


@dataclass
class ColumnContext:
    """One column's STRUCTURAL + quality facts for the served context (DAT-734).

    Business semantics (meaning, unit source, temporal behaviour prose) are NOT
    here — their one home is the column-meanings feed (``field_mappings``,
    DAT-769), served in its own prompt block. This carries what that feed does
    not: physical type, role, the graph-resolved materialization/anchor, value
    enumeration, ranges, derivations, and quality/readiness flags.
    """

    column_id: str
    column_name: str
    table_name: str

    # Type info
    data_type: str | None = None
    semantic_role: str | None = None  # key, measure, dimension, timestamp, etc.

    # Graph-served semantics (og_columns, DAT-734): the resolved materializes_as
    # verdict ('flow' | 'stock' — witness posterior over concept prior) and the
    # measure's anchor event-time axis (witness axis over declared anchor).
    materialization: str | None = None
    anchor_time_axis: str | None = None
    # The resolved storage convention of a monetary measure (DAT-875):
    # 'natural_balance' (each account family's direction already applied) or
    # 'ledger_signed' (one raw ledger direction for all families, so credit-normal
    # accounts read negative). NULL = undetermined; the author must see no fact
    # rather than a guess. Served because a metric extract that SUMs a stored
    # balance cannot otherwise know whether the magnitude it returns is signed.
    stored_sign: str | None = None

    # Statistical metrics
    null_ratio: float | None = None
    cardinality_ratio: float | None = None

    # Value enumeration (DAT-616): the freq-ordered value-set the SQL agent
    # grounds metric predicates in, instead of improvising an ILIKE filter.
    # `top_values` is [{value, count, percentage}] capped at the profiler's
    # top_k; it is the COMPLETE enumeration iff `distinct_count <= len(top_values)`.
    distinct_count: int | None = None
    top_values: list[dict[str, Any]] = field(default_factory=list)

    # DAT-616: measure range/sign — grounds signed measures (a min < 0 tells the agent
    # the column carries negatives, e.g. debit/credit, so a bare SUM may not be the metric).
    numeric_min: float | None = None
    numeric_max: float | None = None

    # Temporal metrics
    is_stale: bool | None = None
    detected_granularity: str | None = None

    # Temporal bounds (from TemporalColumnProfile)
    min_timestamp: str | None = None
    max_timestamp: str | None = None
    # Coverage window + worst discontinuity — promoted from the temporal profile
    # (DAT-783) so the agent knows a time axis's span and whether it's gappy.
    span_days: float | None = None
    largest_gap_days: float | None = None

    # Derived column info from correlation analysis
    is_derived: bool = False
    derived_formula: str | None = None  # e.g., "quantity * unit_price"

    # Quality flags
    flags: list[str] = field(default_factory=list)

    # Entropy scores (from entropy layer)
    entropy_scores: dict[str, Any] | None = None  # Layer scores and composite


@dataclass
class TableContext:
    """Context for a single table."""

    table_id: str
    table_name: str
    duckdb_name: str | None = None  # Actual DuckDB table name (e.g., "sales_csv__orders")
    row_count: int | None = None
    column_count: int = 0

    # Classification
    table_role: str | None = None  # TableRole: fact | periodic_snapshot | dimension
    entity_type: str | None = None

    # From TableEntity
    table_description: str | None = None
    grain_columns: list[str] = field(default_factory=list)
    # DAT-565: all event-time axes — [{"column", "aspect", "note"}, ...].
    time_columns: list[dict[str, Any]] = field(default_factory=list)
    # DAT-565: recurring identities (would-be FKs) — [{"column", "note"}, ...].
    identity_columns: list[dict[str, Any]] = field(default_factory=list)

    # Columns
    columns: list[ColumnContext] = field(default_factory=list)


@dataclass
class RelationshipContext:
    """One FK edge, served from the graph's ``refs`` relation (og_references).

    Conformed-dimension fact↔fact pairs are excluded by the element view's own
    typing (DAT-756) — they surface as :class:`ConformedDimensionContext`
    instead, never as an FK.
    """

    from_table: str
    from_column: str
    to_table: str
    to_column: str
    relationship_type: str
    cardinality: str | None = None
    confidence: float = 0.0
    # The relationships confirmation vocabulary (unconfirmed | judge | user |
    # keeper) — the agent's fall-loud gate for membership-subquery blueprints.
    confirmation_source: str | None = None

    # DAT-616: joining on this edge fans out (one row matches many) → SUMming an
    # additive measure across the join double-counts. The second silent-wrong vector.
    introduces_duplicates: bool | None = None


@dataclass
class SliceContext:
    """Available slice dimension for filtering/grouping.

    ``interest`` is the cataloguing agent's absolute judgment ('primary' /
    'supporting'), None when it never judged this row. ``relevance`` is the
    MEASURED score in [0, 1] (coverage x evenness — ``slicing/relevance.py``),
    None when the column had no statistical profile to measure. The pair
    replaced the ordinal ``priority`` + its 1000 floor (DAT-879).
    """

    column_name: str
    table_name: str
    interest: str | None = None
    relevance: float | None = None
    value_count: int = 0  # Measured COUNT(DISTINCT) on this axis
    business_context: str | None = None  # e.g., "Regional breakdown"
    distinct_values: list[str] = field(default_factory=list)  # Actual categorical values


@dataclass
class DriverContext:
    """One measure's driver ranking, served to the GraphAgent (DAT-616).

    The engine GraphAgent loaded NO drivers before this — the asymmetry the cockpit
    answer agent never had (`<drivers>`, DAT-548). `interesting_slices` are the actual
    dimension VALUES that move the measure (value + signed effect + support) — a
    high-signal HINT for which values carry data, NOT the complete value-set (recall<1;
    the value-set is `top_values`). `target_type` grounds the aggregation (flow→SUM,
    stock→end-of-period, ratio). Mirrors the cockpit `projectDriverRanking`.

    `status`/`abstain_reason` (DAT-859) carry the persisted abstention pair verbatim
    (plain strings — the DB row's own vocabulary, not the drivers module's enum):
    `_append_drivers` is the ONE read-side convention point that skips a non-
    "measured" ranking, so it never renders as prompt content; this dataclass still
    carries it (loaded from every row) for that check to read.
    """

    measure_label: str
    target_type: str  # flow | stock | ratio; "" when abstained with no resolved type
    grain: str  # row | entity
    entity: str | None = None
    status: str = "measured"  # measured | abstained (DAT-859)
    abstain_reason: str | None = None
    ranked_dimensions: list[dict[str, Any]] = field(default_factory=list)  # [{dimension, gain}]
    interesting_slices: list[dict[str, Any]] = field(
        default_factory=list
    )  # [{dimension, value, effect, support}]
    secondary_dimensions: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class CycleStageContext:
    """A stage within a business cycle."""

    stage_name: str
    stage_order: int
    indicator_column: str | None = None
    indicator_values: list[str] = field(default_factory=list)
    completion_rate: float | None = None


@dataclass
class EntityFlowContext:
    """An entity flowing through a business cycle."""

    entity_type: str  # "customer", "vendor"
    entity_column: str  # "customer_id"
    entity_table: str  # "customers"
    fact_table: str | None = None
    relationship_type: str | None = None


@dataclass
class BusinessCycleContext:
    """Detected business cycle with full metadata."""

    cycle_name: str
    cycle_type: str  # e.g., "order_to_cash", "procure_to_pay"
    # Direction axis (DAT-856): the declared family + resolved direction. Both None for
    # a non-family cycle; both set for a family cycle (a decided label, or 'undetermined'
    # — the honest detected-but-undirected state, rendered as such, never a guessed label).
    family: str | None = None
    direction: str | None = None
    tables_involved: list[str] = field(default_factory=list)
    completion_rate: float | None = None  # What % of cycles complete
    description: str | None = None
    business_value: str = "medium"
    confidence: float = 0.0
    stages: list[CycleStageContext] = field(default_factory=list)
    entity_flows: list[EntityFlowContext] = field(default_factory=list)
    # Bare parts (DAT-733): the status column + its table kept SEPARATE, not
    # pre-combined — the default validity-scope resolver renders a bare-column
    # predicate over the grounding's relation, and the narrative re-qualifies it
    # (``<status_table>.<status_column>``) for reading.
    status_table: str | None = None
    status_column: str | None = None
    # The served value that marks a cycle complete (the scope's right-hand side).
    completion_value: str | None = None

    # Volume metrics (from DetectedBusinessCycle)
    total_records: int | None = None
    completed_cycles: int | None = None
    evidence: list[str] = field(default_factory=list)


@dataclass
class ValidationContext:
    """Result of a validation check."""

    validation_id: str
    status: str  # passed, failed, skipped, error
    severity: str  # info, warning, error, critical
    passed: bool
    message: str
    details: dict[str, Any] | None = None  # recomputed verdict: deviation/magnitude/tolerance


@dataclass
class EnrichedViewContext:
    """A pre-built enriched view joining fact + dimension tables."""

    view_name: str
    fact_table: str
    dimension_columns: list[str] = field(default_factory=list)
    is_grain_verified: bool = False
    # Base dimension TABLES the view derives from (og_derived_from, DAT-734) —
    # the graph's derived_from edges served as structure alongside the joined
    # column names above.
    dimension_tables: list[str] = field(default_factory=list)


# =============================================================================
# Graph-served structure (DAT-734 — the operating-model property graph read)
# =============================================================================


@dataclass
class ReportingCalendarContext:
    """The workspace's reporting calendar, as served to the authoring prompt (DAT-887).

    ``fiscal_year_start_month`` is 1–12 (1 = January = a calendar year). ``source`` is
    ``'declared'`` when the workspace declared one and ``'default'`` when the
    calendar-year default was stamped in its absence — served, not collapsed, so the
    author can tell an assumed calendar from a declared one and caveat accordingly.
    """

    fiscal_year_start_month: int
    source: str


@dataclass
class GroundingUseContext:
    """One column a grounding touches (the ``uses`` edge, provenance contract v2)."""

    column_name: str
    table_name: str
    role: str  # 'measure' | 'filter'


@dataclass
class GroundingContext:
    """One reified grounding commitment (a ``grounding_node``, DAT-727).

    The N-ary fact served AS STRUCTURE: the relation it reads, the filter, the
    value expression, and the columns it ``uses`` — never recovered from SQL
    text. A retained failure is served discriminated (``failed`` + mode/reason):
    "why is this concept ungrounded?" is part of the served knowledge.
    """

    snippet_id: str
    concept: str
    relation: str | None
    select_expr: str | None
    where: list[str] = field(default_factory=list)
    statement: str | None = None
    aggregation: str | None = None
    description: str | None = None
    failed: bool = False
    failure_mode: str | None = None
    failure_reason: str | None = None
    uses: list[GroundingUseContext] = field(default_factory=list)


@dataclass
class ConceptReconciliation:
    """One ``reconciles_with`` assertion on a concept, with its evaluation.

    The landed shape (owner-ruled) derives concept-grain SELF-LOOPS for
    multi-grounding tie-out (``partner == concept``); seed/declared rows may
    name a distinct partner concept.

    The fields below the assertion carry what the last promoted run OBSERVED
    when it executed both sides (DAT-739). They are folded from that run's
    per-pair rows: ``evaluated_pairs`` of ``pairs`` produced comparable numbers,
    and the delta reported is the WIDEST divergence among them — the pair that
    puts the assertion most in question. ``status is None`` means the assertion
    has not been evaluated yet (no promoted run carries a row for it), which is
    a different statement from "evaluated and found consistent" and must be
    rendered as one.

    ``observed_delta`` NEVER implies a failure on its own: with no declared
    ``tolerance`` there is no band to have missed, and ``verdict`` says exactly
    that (``no_tolerance_declared``).
    """

    partner: str
    tolerance: float | None = None
    #: ``'evaluated'`` | ``'abstained'`` | ``None`` (never evaluated).
    status: str | None = None
    verdict: str | None = None
    #: Why no pair could be compared — set only when NO pair was evaluated and
    #: every abstention agreed on the reason.
    abstain_reason: str | None = None
    observed_delta: float | None = None
    relative_delta: float | None = None
    pairs: int = 0
    evaluated_pairs: int = 0


@dataclass
class ConceptAdditivity:
    """How one concept's measurement behaves on ONE axis (DAT-857/868).

    The served projection of a ``metric_axis_additivity`` row, reached from the
    concept through the graph's ``has_additivity`` edge (DAT-671 R4). Only
    MEASURE targets reach here: the edge exists because a measure's
    ``target_key`` IS the concept (``standard_field``), whereas a ``metric``
    target keys on a formula ``graph_id`` that has no concept vertex.

    ``axis_key`` is a served column name, or ``'*'`` for the CLASS row covering
    every axis of its kind; a concrete key REFINES the class row. ``status``
    decides how to read the rest — a ``classified`` row carries a ``verdict``
    (plus the doctrine ``reason`` when the verdict is not ``additive``), an
    ``abstained`` row carries only ``abstain_reason``. The vocabularies are
    ``dataraum.graphs.additivity``'s; nothing here re-judges them.

    ``bucket_grain`` is the axis's observed cadence (time axes only): the finest
    bucket the data supports, ``None`` for no claim.
    """

    axis_kind: str  # 'time' | 'categorical'
    axis_key: str  # a served column name, or '*' (the class row)
    status: str  # 'classified' | 'abstained'
    verdict: str | None = None
    reason: str | None = None
    abstain_reason: str | None = None
    bucket_grain: str | None = None


@dataclass
class ConceptContext:
    """One vocabulary concept with its graph neighbourhood (DAT-734).

    The traversal core: definition (typed ``concepts`` row + ontology garnish),
    ``part_of`` subconcepts/parents (+ bounded transitive ancestry),
    ``disjoint_with``, ``reconciles_with``, the concept's groundings
    (``grounded_by`` → ``uses``) — multi-grounding served first-class — and the
    per-axis additivity verdicts of the last promoted run (``has_additivity``).
    """

    name: str
    kind: str | None = None
    description: str | None = None
    indicators: list[str] = field(default_factory=list)
    exclude_patterns: list[str] = field(default_factory=list)
    part_of_children: list[str] = field(default_factory=list)  # subconcepts (1-hop)
    part_of_parents: list[str] = field(default_factory=list)  # 1-hop targets
    part_of_ancestry: list[str] = field(default_factory=list)  # transitive, depth 2..4
    disjoint_with: list[str] = field(default_factory=list)
    reconciles_with: list[ConceptReconciliation] = field(default_factory=list)
    groundings: list[GroundingContext] = field(default_factory=list)
    additivity: list[ConceptAdditivity] = field(default_factory=list)


@dataclass
class ConformedDimensionContext:
    """Two facts sharing a dimension AXIS (og_conformed_dimension, DAT-756).

    The alignable drill-across surface, served as structure: both facts expose
    the same resolved (dimension table, attribute) identity. Unordered pair —
    one row per axis-sharing pair, not per direction.
    """

    table_a: str
    table_b: str
    dimension_table: str
    attribute: str | None = None
    #: The ``conformed_group`` both cells carry — the IDENTITY a drill-across merges
    #: on (DAT-809). Carried rather than dropped so a consumer groups by the served
    #: identity instead of re-deriving one from the label, which drifts (DAT-800).
    conformed_group: str | None = None
    #: Who asserted the underlying structure. Only CONFIRMED pairings reach here.
    confirmation_source: str | None = None
    #: The column each fact joins on. Unlike ``conformed_group`` (which embeds a
    #: table uuid) these are writable in SQL, so they are what the served render
    #: shows — the identity stays for grouping and never reaches a prompt.
    role_a: str | None = None
    role_b: str | None = None


@dataclass
class GraphExecutionContext:
    """Complete context for graph execution (DAT-734 — graph-shaped).

    The GraphAgent's served knowledge: the physical relations, the
    operating-model graph's structure (concepts + groundings, references,
    conformed axes), and the typed knowledge sections with no graph element yet
    (value sets ride the columns; drivers, business cycles, validation results
    are their own rows; conventions ride their own prompt slot).
    """

    # Tables and their metadata (incl. per-column value sets + readiness flags)
    tables: list[TableContext] = field(default_factory=list)

    # FK edges from the graph's refs relation (og_references — conformed pairs
    # excluded by the element view's typing, DAT-756).
    relationships: list[RelationshipContext] = field(default_factory=list)

    # Available slice dimensions (from slicing analysis) — the CURATED subset.
    available_slices: list[SliceContext] = field(default_factory=list)
    # What that curation left out, in one sentence (DAT-879/DAT-622); "" when it
    # served everything. The renderer must print this wherever it prints the
    # slices — a curated list without its own caveat is the silent cap again.
    slice_catalog_note: str = ""

    # Driver rankings per measure (DAT-616): which dims/values move each measure +
    # target_type. The engine GraphAgent served none before — the cockpit/engine
    # asymmetry this closes.
    drivers: list[DriverContext] = field(default_factory=list)

    # Business cycles (from cycles analysis)
    business_cycles: list[BusinessCycleContext] = field(default_factory=list)

    # Cycle health (from cycles health computation)
    cycle_health: HealthReport | None = None

    # Validation results (from validation analysis)
    validations: list[ValidationContext] = field(default_factory=list)

    # DAT-853 abstention at the SECTION grain: "operating-model run absent —
    # cycles/validations never analyzed" and "graph unreachable — refs not
    # readable" must stay distinguishable from genuinely-empty results.
    # format_served_context renders an explicit not-analyzed stub for the False
    # cases instead of omitting the section (a served document that looks
    # byte-identical either way is the silent-substitute defect). Defaults are
    # False — absence is assumed until the builder proves otherwise.
    operating_model_analyzed: bool = False
    graph_readable: bool = False

    # Enriched views (pre-joined fact + dimension tables)
    enriched_views: list[EnrichedViewContext] = field(default_factory=list)

    # The traversal core (DAT-734): each vocabulary concept with its part_of /
    # disjoint_with / reconciles_with neighbourhood and its groundings
    # (grounded_by → uses), read from the operating-model property graph.
    concepts: list[ConceptContext] = field(default_factory=list)

    # Conformed dimension axes (og_conformed_dimension, DAT-756) — served as
    # structure: which facts drill across on which shared (dim table, attribute).
    conformed_dimensions: list[ConformedDimensionContext] = field(default_factory=list)

    # Column meaning feed (meaning + measurement facts, DAT-769) for metrics
    field_mappings: list[ColumnMeaning] = field(default_factory=list)

    # Vertical conventions for the extraction consumer (DAT-645): verbatim,
    # LLM-facing domain guidance (e.g. the sign/natural-balance rule) the SQL
    # agent applies when authoring a measure. Opaque to the engine — see
    # OntologyConvention. Empty string when the vertical declares none.
    conventions: str = ""

    # The workspace's reporting calendar (DAT-887 / DAT-730): the fiscal-year start
    # month and whether it was DECLARED or defaulted. Served because a point-in-time
    # extract's period is bound at composition to the last fiscal close — the author
    # must be able to see which window its value will be as-of rather than infer one
    # (and, seeing it, must not pin the period axis itself). None when the read
    # surface serves no calendar: absence is served as absence, never as a
    # fabricated calendar year.
    reporting_calendar: ReportingCalendarContext | None = None


# Value-set serving contract (DAT-621) — shared by the assembler that FETCHES a
# value set and the formatter that RENDERS it, so both agree on what "complete"
# and "not a partition" mean. Split out of the single context module by DAT-869.
# The "reasonable top" (DAT-621): a categorical dimension at/below this distinct count is
# enumerated COMPLETELY (via a live DISTINCT at context-build, since the profiler only stores
# the top-K); above it the column is not an aggregation partition (free-text / high-card id)
# and is served size+sample, never enumerated. Set from the measured dimension distribution
# (median 27, then a 40k tail; the number is insensitive in [100,500]).
_VALUE_SET_COMPLETE_MAX = 200
# Roles whose values are never a metric-grounding predicate (keys fan out; measures are
# aggregated, not filtered; time axes are handled by the temporal blueprints).
_NON_CATEGORICAL_ROLES = {"key", "measure", "timestamp", "time", "identifier"}

__all__ = [
    "BusinessCycleContext",
    "ColumnContext",
    "ConceptContext",
    "ConceptReconciliation",
    "ConformedDimensionContext",
    "CycleStageContext",
    "DriverContext",
    "EnrichedViewContext",
    "EntityFlowContext",
    "GraphExecutionContext",
    "GroundingContext",
    "GroundingUseContext",
    "RelationshipContext",
    "SliceContext",
    "TableContext",
    "ValidationContext",
]
