"""SQLAlchemy models for semantic analysis.

Contains database models for semantic annotations and entity detection.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum.storage import Base

if TYPE_CHECKING:
    from dataraum.storage import Column, Table


class ConceptKind(StrEnum):
    """The ontological kind of a concept (DAT-728).

    The typed vocabulary axis the flat ``config_overlay`` JSON lacked. Distinct
    from a column's ``semantic_role`` (a data-detected per-column property):
    ``kind`` is the concept's own type, authored in the vertical and seeded into
    the ``concepts`` table.

    - ``MEASURE`` — is aggregated (revenue, debit, account_balance).
    - ``ENTITY`` — names a table's grain (account, entity, customer).
    - ``DIMENSION`` — slices measures (fiscal_period, region, account_type).
    - ``UNIT`` — units other measures (currency).
    """

    MEASURE = "measure"
    ENTITY = "entity"
    DIMENSION = "dimension"
    UNIT = "unit"


class TableRole(StrEnum):
    """The table's role in the operating model (DAT-728).

    Replaces the two booleans (``is_fact_table`` / ``is_dimension_table``, which
    carried a single bit — a dimension was just ``not fact``). ``PeriodicSnapshot``
    is now a first-class subtype (``trial_balance``-shaped: a time column sits in
    the grain, so a ``COUNT`` re-states the same population each period and is
    non-additive across time) — persisted here instead of re-derived on demand.

    ``BridgeTable`` (the m:n resolver) is deliberately absent: it needs a detection
    signal and an eval fixture that don't exist yet (DAT-747), so shipping the enum
    value unpopulated would be dead vocabulary.
    """

    FACT = "fact"
    PERIODIC_SNAPSHOT = "periodic_snapshot"
    DIMENSION = "dimension"


class ConceptEdgePredicate(StrEnum):
    """The typed relation a concept edge asserts (DAT-729).

    The operating-model graph's *vocabulary* edges — concept → concept, distinct
    from the physical ``references`` / ``has_dimension`` edges over tables/columns.

    - ``PART_OF`` — mereological composition, DIRECTED: the source concept is a
      component that rolls up into the target (``accounts_payable`` part_of
      ``current_liabilities``). Its transitive closure (all ancestors of a concept)
      is walked by the bounded recursive CTE, never a PGQ path quantifier.
    - ``DISJOINT_WITH`` — SYMMETRIC: no instance classifies as both (an account is
      an asset xor a liability). Derived from a convention's ``concept_groups``
      partition — concepts in different groups of one convention are disjoint.
    - ``RECONCILES_WITH`` — SYMMETRIC: two measures/groundings must tie out within a
      ``tolerance`` (trial-balance ↔ general-ledger). Declared, or witnessed by the
      aggregation-lineage reconciliation, or free from a concept's two groundings.

    Symmetric predicates are materialized in BOTH directions (see
    :class:`ConceptEdge`) so a directed PGQ ``MATCH`` from either endpoint finds them.
    """

    PART_OF = "part_of"
    DISJOINT_WITH = "disjoint_with"
    RECONCILES_WITH = "reconciles_with"


def derive_table_role(
    is_fact: bool,
    grain_columns: Sequence[str],
    time_column_names: Sequence[str],
) -> TableRole:
    """Classify a table's role from the LLM's fact/dimension bit + its grain.

    The LLM answers one question (fact vs dimension); the PeriodicSnapshot subtype
    is structural, not asked: a fact whose grain contains a time column re-states
    the same population each period. Non-fact → ``DIMENSION``; fact with a time
    column in its grain → ``PERIODIC_SNAPSHOT``; otherwise ``FACT``.
    """
    if not is_fact:
        return TableRole.DIMENSION
    if set(grain_columns) & set(time_column_names):
        return TableRole.PERIODIC_SNAPSHOT
    return TableRole.FACT


class Concept(Base):
    """The workspace's typed concept vocabulary — one home (DAT-728).

    Replaces the opaque ``config_overlay(type='concept')`` JSON + the runtime read
    of the shipped ``ontology.yaml`` as the single home for a workspace's concept
    vocabulary (config→DB). The shipped vertical is the *seed* (normalized into
    typed rows at connect); the pipeline enriches these rows; ``frame`` writes
    declared/edited rows through the same table (the cockpit's Drizzle mirror).

    **Identity contract — NOT run-versioned.** A concept is a stable node keyed by
    ``(vertical, name)``; ``concept_id`` is a workspace-stable surrogate minted
    once at seed, NOT a fresh per-run uuid. The run-versioned groundings
    (:class:`ColumnConcept`) reference a concept by its stable ``(vertical, name)``
    key — the run axis lives on the grounding, never on the concept node. Edits
    supersede rather than collide: an edit writes a new row and stamps the prior
    ``superseded_at``; the partial-unique index keeps at most one *active* row per
    ``(vertical, name)`` so a head-free read is unambiguous (no
    ``MultipleResultsFound``). Workspace identity is the ``ws_<id>`` schema itself,
    as with ``config_overlay`` — no ``workspace_id`` column.
    """

    __tablename__ = "concepts"
    __table_args__ = (
        # At most one ACTIVE row per (vertical, name); superseded history rows are
        # exempt (superseded_at IS NOT NULL). Postgres partial unique index — the
        # deterministic-single-active-row guarantee the head-free reads rely on.
        Index(
            "uq_concept_active",
            "vertical",
            "name",
            unique=True,
            postgresql_where=text("superseded_at IS NULL"),
            sqlite_where=text("superseded_at IS NULL"),
        ),
    )

    concept_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    vertical: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    kind: Mapped[str] = mapped_column(String, nullable=False)  # ConceptKind

    description: Mapped[str | None] = mapped_column(Text)
    indicators: Mapped[list[str] | None] = mapped_column(JSON)
    exclude_patterns: Mapped[list[str] | None] = mapped_column(JSON)
    unit_from_concept: Mapped[str | None] = mapped_column(String)

    # Lifecycle: workspace-persistent with supersession (NULL superseded_at = active).
    source: Mapped[str | None] = mapped_column(String)  # 'seed' | 'frame' | 'teach'
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    superseded_at: Mapped[datetime | None] = mapped_column(DateTime)


class ConceptEdge(Base):
    """A typed edge between two concepts — the operating-model graph's vocabulary (DAT-729).

    The concept-level relations ``part_of`` / ``disjoint_with`` / ``reconciles_with``
    (:class:`ConceptEdgePredicate`), promoted to typed rows the same way concepts were
    (config→DB): the shipped vertical seeds them, the pipeline derives more, ``frame``
    authors them for novel datasets (P13). Bound into the property graph as the
    ``concept_edge`` edge over the ``og_concepts`` vertex.

    **Same identity contract as :class:`Concept` (P3) — NOT run-versioned.** An edge
    is a stable node keyed over ACTIVE rows by
    ``(vertical, predicate, from_concept, to_concept)`` (the ``uq_concept_edge_active``
    partial-unique index); an edit supersedes (stamp ``superseded_at`` + insert a new
    active row) rather than colliding. Endpoints are the concepts' stable
    ``name`` within ``vertical`` — NEVER ``concept_id`` (a per-seed surrogate the
    grounding contract already forbids keying on). The ``og_concept_edges`` element
    view resolves those names to the active concepts' ids for the PGQ vertex binding,
    so a superseded/absent endpoint drops the edge from the graph automatically.

    Rename caveat (future): keying endpoints by ``name`` means a concept RENAME (a new
    ``(vertical, name)`` identity, not a supersede-in-place) would orphan its edges —
    they'd silently vanish from the graph. No rename path exists today; when ``frame``
    adds one (P13) it must re-point or supersede the affected edges, not just the concept.

    Symmetric predicates (``disjoint_with``, ``reconciles_with``) are stored in BOTH
    directions — the graph is directed and PG19 SQL/PGQ ``MATCH`` is directed, so two
    rows let a walk from either endpoint enumerate the relation. ``part_of`` is
    directed (one row, source → target). ``tolerance`` is set only for
    ``reconciles_with`` (the tie-out band); NULL otherwise.
    """

    __tablename__ = "concept_edges"
    __table_args__ = (
        # At most one ACTIVE edge per (vertical, predicate, from, to) — the concept-
        # edge analogue of Concept.uq_concept_active, so a head-free read of the
        # active graph is unambiguous and the seed's ON CONFLICT DO NOTHING is
        # race-safe against a concurrent seed / frame write.
        Index(
            "uq_concept_edge_active",
            "vertical",
            "predicate",
            "from_concept",
            "to_concept",
            unique=True,
            postgresql_where=text("superseded_at IS NULL"),
            sqlite_where=text("superseded_at IS NULL"),
        ),
    )

    edge_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    vertical: Mapped[str] = mapped_column(String, nullable=False)
    predicate: Mapped[str] = mapped_column(String, nullable=False)  # ConceptEdgePredicate
    # Endpoints are concept NAMES within `vertical` (the stable (vertical, name) key),
    # NOT concept_id — the element view joins them to the active concepts for the graph.
    from_concept: Mapped[str] = mapped_column(String, nullable=False)
    to_concept: Mapped[str] = mapped_column(String, nullable=False)
    # reconciles_with tie-out band (e.g. 0.01 = 1%); NULL for part_of / disjoint_with.
    tolerance: Mapped[float | None] = mapped_column(Float)

    # Lifecycle: workspace-persistent with supersession (mirrors Concept).
    source: Mapped[str | None] = mapped_column(String)  # 'seed' | 'derived' | 'frame'
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    superseded_at: Mapped[datetime | None] = mapped_column(DateTime)


class SemanticAnnotation(Base):
    """Semantic annotations for columns.

    Stores LLM-generated or manually-provided semantic metadata
    including business terms, roles, and ontology mappings.
    """

    __tablename__ = "semantic_annotations"
    # One annotation per column PER RUN (DAT-413): the snapshot version axis widens
    # this from ``column_id`` to ``(column_id, run_id)`` so two coexisting runs'
    # rows for the same column don't collide. The promoted head names which run is
    # current; readers head-resolve rather than assume one row per column.
    __table_args__ = (
        UniqueConstraint("column_id", "run_id", name="uq_column_semantic_annotation"),
    )

    annotation_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    column_id: Mapped[str] = mapped_column(ForeignKey("columns.column_id"), nullable=False)
    # Snapshot version axis (DAT-413): the run that wrote this row.
    run_id: Mapped[str] = mapped_column(String, nullable=False)

    # Classification
    semantic_role: Mapped[str | None] = mapped_column(
        String
    )  # 'identifier', 'measure', 'attribute', 'dimension'
    entity_type: Mapped[str | None] = mapped_column(
        String
    )  # 'customer', 'product', 'transaction', etc.

    # Business terms
    business_name: Mapped[str | None] = mapped_column(String)
    business_description: Mapped[str | None] = mapped_column(Text)

    # Object-grain stock/flow witness (ADR-0009 / DAT-445): the column agent's
    # INDEPENDENT per-column read ('stock'/'flow'/'unsure') + its confidence,
    # decidable from one column's name + values, written by semantic_per_column.
    # The ontology-derived ``temporal_behavior`` it is pooled against is
    # catalogue-grain and now lives on ``ColumnConcept`` (DAT-637) — owned by the
    # table agent, never duplicated here.
    temporal_behavior_claim: Mapped[str | None] = mapped_column(String)
    temporal_behavior_claim_confidence: Mapped[float | None] = mapped_column(Float)

    # Resolved null-marker tokens (ADR-0009 / DAT-457): the rejected tokens the
    # null_semantics adjudication resolved to is-null (pooled posterior past
    # threshold). Written by the resolved-layer pass inside the terminal detect
    # (dataraum.entropy.resolve), updating THIS run's annotation. The query agent
    # treats these as NULL in generated SQL. None = not resolved / no rejects.
    null_tokens: Mapped[list[str] | None] = mapped_column(JSON)

    # Provenance
    annotation_source: Mapped[str | None] = mapped_column(
        String
    )  # 'llm', 'manual', 'config_override'
    annotated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    annotated_by: Mapped[str | None] = mapped_column(String)
    confidence: Mapped[float | None] = mapped_column(Float)

    # Relationships
    column: Mapped[Column] = relationship(back_populates="semantic_annotation")


class ColumnConcept(Base):
    """Catalogue-grain per-column semantics, owned by the table agent (DAT-637).

    The home for column attributes that CANNOT be decided from a single table —
    they need the composed catalogue (cross-cutting ontology concepts, the
    confirmed relationship catalogue, the enriched fact×dimension views). They are
    authored ONLY by ``semantic_per_table`` (begin_session) and sealed under the
    workspace **catalogue head**, never by the object-grain per-column agent.

    Single ownership, no copy-forward: these fields were physically removed from
    ``SemanticAnnotation`` (object-grain, add_source generation head). A reader
    resolves them through :func:`load_column_concepts`, which DEMANDS the catalogue
    run — there is no unscoped read, so object-grain code (add_source ``detect``)
    cannot reach catalogue-grain semantics by construction.

    Fields:
        meaning: the column's business-model characterization in the context of
            the composed catalogue — free text, ambiguity expressible (DAT-769:
            "the per-entity, per-period total of incoming movements — the inflow
            column of a periodic statement" is a complete answer where a
            categorical binding forced a coin-flip; examples here are
            deliberately vertical-agnostic — verticals live in config, never in
            engine contracts). Transported as CONTEXT to LLM consumers
            (metric grounding feed, cycles, validation); never a decision surface.
            Replaces the retired single-slot ``business_concept`` binding — the
            precise-word mapping was ill-posed for multi-facet columns and no
            consumer branched on it.
        temporal_behavior: the resolved stock/flow ('additive' / 'point_in_time')
            for this column — data-determined (DAT-657): the resolved-layer pass
            writes the LLM claim reconciled with the data-grounded structural
            witness.
        temporal_behavior_contested: set by the resolved-layer pass when the LLM's
            ``temporal_behavior_claim`` and the data-grounded structural witness
            pool to a non-trivial conflict.
        unit_source_column: the column (possibly ``table.column`` via a confirmed
            FK) that defines this measure's unit.
        derived_formula_hypothesis / _confidence: the arithmetic this column
            should obey — operands may span a JOINED table (the derived_value
            session detector grades it over enriched_views), so it is reasoned
            from the relationship catalogue, not one table.
    """

    __tablename__ = "column_concepts"
    # One row per column PER catalogue run (DAT-637): mirrors SemanticAnnotation's
    # run-versioned grain so coexisting begin_session runs don't collide; the
    # catalogue head names the current run.
    __table_args__ = (UniqueConstraint("column_id", "run_id", name="uq_column_concept"),)

    concept_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    column_id: Mapped[str] = mapped_column(ForeignKey("columns.column_id"), nullable=False)
    # Snapshot version axis: the begin_session (catalogue head) run that wrote this row.
    run_id: Mapped[str] = mapped_column(String, nullable=False)

    meaning: Mapped[str | None] = mapped_column(Text)
    temporal_behavior: Mapped[str | None] = mapped_column(String)
    temporal_behavior_contested: Mapped[bool | None] = mapped_column(Boolean)
    unit_source_column: Mapped[str | None] = mapped_column(String)
    derived_formula_hypothesis: Mapped[str | None] = mapped_column(String)
    derived_formula_confidence: Mapped[float | None] = mapped_column(Float)

    # Provenance
    annotation_source: Mapped[str | None] = mapped_column(String)
    annotated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    annotated_by: Mapped[str | None] = mapped_column(String)
    confidence: Mapped[float | None] = mapped_column(Float)


class TableEntity(Base):
    """Entity detection at table level.

    Identifies the type of entity represented by the table
    and classifies it as fact/dimension table with grain analysis.
    """

    __tablename__ = "table_entities"
    # One entity classification per table PER RUN (DAT-408/413). TableEntity is
    # run-versioned and coexists across runs; this constraint (mirroring
    # ``uq_column_semantic_annotation``) makes "one row per ``(table_id, run_id)``"
    # a DB guarantee so the run-scoped readers can trust it.
    __table_args__ = (UniqueConstraint("table_id", "run_id", name="uq_table_entity_table_run"),)
    entity_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)
    # Snapshot version axis (DAT-413): the run that wrote this row.
    run_id: Mapped[str] = mapped_column(String, nullable=False)

    detected_entity_type: Mapped[str] = mapped_column(
        String, nullable=False
    )  # 'customer', 'order', 'product', etc.
    description: Mapped[str | None] = mapped_column(Text)

    # Grain analysis
    grain_columns: Mapped[dict[str, Any] | None] = mapped_column(
        JSON
    )  # List of column IDs that define grain
    # The table's operating-model role (DAT-728): fact | periodic_snapshot |
    # dimension (see :class:`TableRole`). Replaces the two booleans; the
    # PeriodicSnapshot subtype is derived from grain∩time at classification and
    # feeds the additivity COUNT rule. Nullable: an unclassified stub has no role.
    table_role: Mapped[str | None] = mapped_column(String)
    # DAT-565: all event-time axes (multi-temporal) and recurring identity columns,
    # each carrying a one-line note. JSON list[dict]; run-versioned like the rest.
    #   time_columns:     [{"column": str, "aspect": str, "note": str}, ...]
    #   identity_columns: [{"column": str, "note": str}, ...]
    time_columns: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON)
    identity_columns: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON)

    # Provenance
    detection_source: Mapped[str | None] = mapped_column(String)  # 'llm', 'heuristic', 'manual'
    detected_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Relationships
    table: Mapped[Table] = relationship(back_populates="entity_detections")


__all__ = [
    "Concept",
    "ConceptEdge",
    "ConceptEdgePredicate",
    "ConceptKind",
    "SemanticAnnotation",
    "TableEntity",
    "TableRole",
    "derive_table_role",
]
