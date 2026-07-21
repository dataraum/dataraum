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
    CheckConstraint,
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

from dataraum.analysis.catalogue.models import MEANING_STATUSES
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


# Closed-vocabulary CHECK values (DAT-802 enum-standard sweep), each derived from
# its single-home enum above so the CHECK and the enum can never drift (the
# DAT-784 pattern). Sorted for a deterministic CHECK string in the offline DDL dump.
_CONCEPT_KIND_VALUES: tuple[str, ...] = tuple(sorted(v.value for v in ConceptKind))
_TABLE_ROLE_VALUES: tuple[str, ...] = tuple(sorted(v.value for v in TableRole))
_CONCEPT_EDGE_PREDICATE_VALUES: tuple[str, ...] = tuple(
    sorted(v.value for v in ConceptEdgePredicate)
)


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
        # Closed-vocabulary enforcement (DAT-802): derived from ConceptKind, the
        # single home (see ``concept_store.py``'s seed validation against
        # ``_VALID_KINDS`` and the cockpit's independently-typed TS mirror,
        # ``write-surface.ts`` / ``concept-write.ts``'s ``CONCEPT_KINDS`` — two
        # app-level guards converging on one physical column with no DB backstop
        # until now).
        CheckConstraint(
            "kind IN (" + ", ".join(f"'{v}'" for v in _CONCEPT_KIND_VALUES) + ")",
            name="kind",
        ),
        # Lifecycle-source vocabulary (DAT-802): the two LIVE writers — 'seed'
        # (``concept_store.py:72``, engine) and 'frame' (cockpit
        # ``concept-write.ts:84``'s ``writeConcept()``, a real Drizzle INSERT
        # wired into the ``frame`` tool at ``tools/frame.ts:405`` and exercised
        # by ``concept-write.integration.test.ts`` — not a generated view, not
        # aspirational). NOT 'teach': DAT-728 (Done) explicitly retired the
        # ``concept`` teach type. Deliberately narrower than ``ConceptEdge.source``
        # (2 live writers here vs. 1 there) — don't copy that CHECK's value list
        # onto this column; each column's set is its own writers, not a shared
        # template.
        CheckConstraint("source IS NULL OR source IN ('seed', 'frame')", name="source"),
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
    # Closed vocab: see ck_concepts_source — 'seed' | 'frame' are the two live writers.
    source: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    superseded_at: Mapped[datetime | None] = mapped_column(DateTime)


class ConceptEdge(Base):
    """A typed edge between two concepts — the operating-model graph's vocabulary (DAT-729).

    The concept-level relations ``part_of`` / ``disjoint_with`` / ``reconciles_with``
    (:class:`ConceptEdgePredicate`), promoted to typed rows the same way concepts were
    (config→DB): the shipped vertical seeds them, the pipeline derives more, ``frame``
    authors them for novel datasets. Bound into the property graph as the
    ``concept_edge`` edge over the ``og_concepts`` vertex.

    **Same identity contract as :class:`Concept` — NOT run-versioned.** An edge
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
    adds one it must re-point or supersede the affected edges, not just the concept.

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
        # Closed-vocabulary enforcement (DAT-802): derived from ConceptEdgePredicate,
        # the single home. ``concept_edge_store.py`` (seed) writes
        # PART_OF/DISJOINT_WITH; RECONCILES_WITH is emitted by the derived
        # producer ``reconciles_with.py`` (DAT-727: aggregation-lineage witness
        # + multi-grounding, concept-grain self-loops).
        CheckConstraint(
            "predicate IN (" + ", ".join(f"'{v}'" for v in _CONCEPT_EDGE_PREDICATE_VALUES) + ")",
            name="predicate",
        ),
        # Lifecycle-source vocabulary (DAT-802): every admitted value has a live
        # writer — 'seed' (``concept_edge_store.py``, the vertical's declared
        # edges) and 'derived' (``reconciles_with.py``, DAT-727: the
        # aggregation-lineage-witness + multi-grounding reconciles_with
        # self-loops, reconciled at the end of the metrics phase). 'frame'
        # (the cockpit's authoring path) stays OUT until that writer exists — a
        # CHECK admitting a value no writer produces is the exact defect the
        # DAT-802 sweep fixed (the DAT-772 ``relationship_type`` finding);
        # widening is one line + a re-dump in the PR that adds the writer.
        CheckConstraint("source IS NULL OR source IN ('derived', 'seed')", name="source"),
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
    # Closed vocab: see ck_concept_edges_source — 'seed' + 'derived' are the live writers.
    source: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    superseded_at: Mapped[datetime | None] = mapped_column(DateTime)


class WorkspaceSettings(Base):
    """The workspace's bound active vertical — the one home DAT-848 was missing.

    A workspace's concept vocabulary (:class:`Concept` / :class:`ConceptEdge`, keyed
    ``(vertical, name)``) is bound to exactly ONE vertical. The engine had no fact
    recording which, so a run launched with the wrong ``--vertical`` seeded a second
    vertical's rows and every unscoped reader served the union — permanent
    cross-vertical contamination (DAT-848). This single-row table is that fact.

    **Binding.** ``active_vertical`` is set the first time a run resolves a
    NON-placeholder vertical (``require_active_vertical`` in ``concept_store.py``,
    the resolve gate); a later run whose vertical differs fails LOUD there rather
    than seeding beside it. Placeholder runs (``_adhoc``) declare no domain — they
    never bind and are never checked. Changing a bound workspace's vertical is a
    deliberate, explicit operation (re-run after the change), NOT a per-run override
    — the gate refuses to silently repurpose a finance workspace as marketing.

    **Scoping.** The concept read surface — the ``__READ__.concepts`` /
    ``__READ__.concept_edges`` views (``storage/read_views.py``), which feed both
    ``og_concepts`` / ``og_concept_edges`` (the property graph) and the cockpit's
    Drizzle mirror — filters to ``active_vertical``. So a Concept row that a wrong
    ``--vertical`` (or the eval's wild-vertical stand-in) left under a DIFFERENT
    vertical is present in the base table but never SERVED. Frame writes and the
    seed still land on the raw ``concepts`` table directly; only the read surface is
    scoped.

    **Identity.** Workspace identity IS the ``ws_<id>`` schema (no ``workspace_id``
    column, as :class:`Concept`). Singleton: ``pin`` is a boolean PK checked
    ``= TRUE``, so at most one row exists per schema. The row exists iff a real
    vertical is bound; an unbound workspace has zero rows → the scoping subquery
    resolves to NULL and the read views ``COALESCE`` it to the placeholder ``_adhoc``
    (the no-domain vocabulary ``frame`` writes when the user names no vertical), so an
    unbound workspace serves its ``_adhoc`` concepts, not nothing.
    """

    __tablename__ = "workspace_settings"
    __table_args__ = (
        # Singleton guard: pin is the boolean PK restricted to TRUE, so the table
        # holds at most one row. The bind's INSERT ... ON CONFLICT (pin) DO NOTHING
        # (``require_active_vertical``) is thereby race-safe against a concurrent
        # first run (Temporal at-least-once) — the second INSERT is a no-op, and
        # both callers re-read the winner's value to check their own vertical.
        CheckConstraint("pin = TRUE", name="pin"),
    )

    pin: Mapped[bool] = mapped_column(Boolean, primary_key=True, default=True)
    # The workspace's bound vertical (shipped, framed, or placeholder name). Open
    # vocabulary (framed verticals are user-declared), so no CHECK on its value —
    # but NOT NULL with no default: a row exists only when a vertical is bound, and
    # the gate always supplies one, so an omitting writer fails loud (DAT-802 v4:
    # no permissive default on a load-bearing scalar).
    active_vertical: Mapped[str] = mapped_column(String, nullable=False)
    bound_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )


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
        # Closed-vocabulary enforcement (DAT-802): the ONLY value any writer
        # produces today is 'llm' (semantic/processor.py, the sole writer). The
        # 'manual' / 'config_override' values below were aspirational — no manual
        # or config-override write path exists (DAT-772 audit's doc-drift finding);
        # narrowed to reality rather than encoded from the stale comment.
        CheckConstraint(
            "annotation_source IS NULL OR annotation_source IN ('llm')", name="annotation_source"
        ),
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

    # Provenance. Closed vocab: see ck_semantic_annotations_annotation_source —
    # 'llm' is the only value any writer produces today.
    annotation_source: Mapped[str | None] = mapped_column(String)
    annotated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    annotated_by: Mapped[str | None] = mapped_column(String)
    confidence: Mapped[float | None] = mapped_column(Float)

    # Relationships
    column: Mapped[Column] = relationship(back_populates="semantic_annotation")


class ColumnConcept(Base):
    """Catalogue-grain per-column semantics, owned by the catalogue agent (DAT-637/823).

    The home for column attributes that CANNOT be decided from a single table —
    they need the composed catalogue (cross-cutting ontology concepts, the
    confirmed relationship catalogue, the enriched fact×dimension views, the
    resolved slice axes). They are authored ONLY by ``catalogue_semantics``
    (begin_session, after enriched_views + slicing — DAT-823 moved authoring off
    the structural per-table judge) and sealed under the workspace **catalogue
    head**, never by the object-grain per-column agent.

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
        meaning_status: the agent's persisted determination (DAT-823, W2-A
            persisted-status precedent): 'determined' = the composed evidence
            settles the meaning; 'ambiguous' = declared ignorance WITH a meaning
            present — the meaning text states what is undetermined and what
            would settle it (the DAT-769 contract licenses this). NULL on rows
            written before the column existed (no backfill) and on rows without
            a meaning (a coverage gap is not a judgment). No consumer branches
            on it — it is queryable state, not a decision surface.
        temporal_behavior: the resolved stock/flow ('additive' / 'point_in_time')
            for this column — data-determined (DAT-657): the resolved-layer pass
            writes the LLM claim reconciled with the data-grounded structural
            witness. This verdict is authoritative on its own — DAT-786 removed
            the parallel "contested" doubt flag; a disagreement between the LLM
            claim and the structural witness is logged at the resolve site, not
            persisted downstream.
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
    __table_args__ = (
        UniqueConstraint("column_id", "run_id", name="uq_column_concept"),
        # Closed-vocabulary enforcement (DAT-802): the ONLY value any writer
        # produces today is 'llm' (``catalogue_semantics``, the sole authoring
        # path — this table's own docstring: "authored ONLY by catalogue_semantics").
        CheckConstraint(
            "annotation_source IS NULL OR annotation_source IN ('llm')",
            name="annotation_source",
        ),
        # Persisted-determination vocabulary (DAT-823): NULL-or-IN, derived from
        # its single home ``catalogue.models.MEANING_STATUSES`` (the W2-A
        # ``abstain_reason`` pattern — explicit IS NULL arm, values sorted for a
        # deterministic offline DDL dump).
        CheckConstraint(
            "meaning_status IS NULL OR meaning_status IN ("
            + ", ".join(f"'{v}'" for v in sorted(MEANING_STATUSES))
            + ")",
            name="meaning_status",
        ),
    )

    concept_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    column_id: Mapped[str] = mapped_column(ForeignKey("columns.column_id"), nullable=False)
    # Snapshot version axis: the begin_session (catalogue head) run that wrote this row.
    run_id: Mapped[str] = mapped_column(String, nullable=False)

    meaning: Mapped[str | None] = mapped_column(Text)
    # Closed vocab: see ck_column_concepts_meaning_status (DAT-823). Old runs NULL.
    meaning_status: Mapped[str | None] = mapped_column(String)
    temporal_behavior: Mapped[str | None] = mapped_column(String)
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
    """Table classification — structural stub, then the business reading (DAT-823).

    Written in two steps within one begin_session run: ``semantic_per_table``
    INSERTs the STRUCTURAL stub (table_role, grain, time/identity columns) with
    ``detected_entity_type``/``description`` NULL; ``catalogue_semantics``
    UPDATEs the same ``(table_id, run_id)`` row with the business reading once
    the composed catalogue (confirmed relationships, enriched views, slice
    axes) exists to argue it from. The NULL window is within-run only — nothing
    between the two phases reads either column (verified at the rebalance); a
    NULL that survives the run is declared ignorance (the catalogue turn could
    not name the entity), not an error.
    """

    __tablename__ = "table_entities"
    # One entity classification per table PER RUN (DAT-408/413). TableEntity is
    # run-versioned and coexists across runs; this constraint (mirroring
    # ``uq_column_semantic_annotation``) makes "one row per ``(table_id, run_id)``"
    # a DB guarantee so the run-scoped readers can trust it.
    __table_args__ = (
        UniqueConstraint("table_id", "run_id", name="uq_table_entity_table_run"),
        # Closed-vocabulary enforcement (DAT-802): the ONLY value any writer
        # produces today is 'llm' (semantic/processor.py, the sole writer).
        # 'heuristic' / 'manual' were aspirational — no such write path exists
        # (DAT-772 audit's doc-drift finding); narrowed to reality.
        CheckConstraint(
            "detection_source IS NULL OR detection_source IN ('llm')", name="detection_source"
        ),
        # Table-role vocabulary (DAT-802): derived from TableRole, the single home
        # (see :func:`derive_table_role`, the sole writer via
        # ``semantic/agent.py``).
        CheckConstraint(
            "table_role IS NULL OR table_role IN ("
            + ", ".join(f"'{v}'" for v in _TABLE_ROLE_VALUES)
            + ")",
            name="table_role",
        ),
    )
    entity_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)
    # Snapshot version axis (DAT-413): the run that wrote this row.
    run_id: Mapped[str] = mapped_column(String, nullable=False)

    # 'customer', 'order', 'product', etc. Nullable (DAT-823): the structural
    # stub has no business reading yet — the documented unclassified-stub
    # nullability pattern (table_role / grain_columns below).
    detected_entity_type: Mapped[str | None] = mapped_column(String)
    description: Mapped[str | None] = mapped_column(Text)

    # Grain analysis. A bare JSON list of column NAMES that uniquely identify
    # each row (DAT-775) — NOT a ``{"columns": [...]}`` wrapper. The wrapper
    # shape was an unenforced convention: one reader (``cycles/context.py``)
    # joined the persisted value directly, so a wrapped dict rendered its sole
    # key ("columns") as the grain in the cycle-detection prompt instead of the
    # real columns. Nullable: an unclassified stub has no grain.
    grain_columns: Mapped[list[str] | None] = mapped_column(JSON)
    # The table's operating-model role (DAT-728): fact | periodic_snapshot |
    # dimension (see :class:`TableRole`). Replaces the two booleans; the
    # PeriodicSnapshot subtype is derived from grain∩time at classification and
    # feeds the additivity COUNT rule. Nullable: an unclassified stub has no role.
    table_role: Mapped[str | None] = mapped_column(String)
    # DAT-565: all date axes (multi-temporal) and recurring identity columns, each
    # carrying a one-line note. JSON list[dict]; run-versioned like the rest. The
    # event/attribute rule + single anchor are enforced at save by the TimeColumn
    # submodel + TableEntityOutput validator (DAT-780) — this JSON interior is the
    # typed home, so no scalar column / CheckConstraint carries the vocabulary.
    #   time_columns:     [{"column": str, "aspect": str, "role": "event"|"attribute",
    #                       "is_anchor": bool, "note": str}, ...]
    #   identity_columns: [{"column": str, "note": str}, ...]
    time_columns: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON)
    identity_columns: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON)

    # Provenance. Closed vocab: see ck_table_entities_detection_source — 'llm' is
    # the only value any writer produces today.
    detection_source: Mapped[str | None] = mapped_column(String)
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
    "WorkspaceSettings",
    "derive_table_role",
]
