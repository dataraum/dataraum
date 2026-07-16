"""SQLAlchemy model for dimension-hierarchy discovery (DAT-537).

The JSON interiors carry a two-layer contract (DAT-779/784): a strict Pydantic
submodel validates the shape at every writer (a ``CheckConstraint`` cannot reach
into a JSON array/object), and the scalar closed-vocabulary column ``role_verdict``
additionally gets a DB ``CheckConstraint`` (the DAT-781 two-layer standard).
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.analysis.hierarchies.stats import RoleVerdict
from dataraum.storage import Base

# The closed vocabulary of ``role_verdict``, derived from the single source of
# truth — the stack-v4 verdict enum (DAT-784). Sorted for a deterministic CHECK
# string in the offline DDL dump. DIRT is part of the enum's vocabulary though it
# is never persisted as its own row (a DIRT pair is merged into its alias group),
# so the column only ever holds ``role`` / ``value_systematic`` / ``abstain`` or
# NULL — the CHECK enforces the full enum domain as the forward-safe backstop.
_ROLE_VERDICT_VALUES: tuple[str, ...] = tuple(sorted(v.value for v in RoleVerdict))


class HierarchyMember(BaseModel):
    """One member column of a dimension-hierarchy structure's ordered ``members``.

    Persisted as one element of the ``dimension_hierarchies.members`` JSON array.
    **``level`` is the sole carrier of drill-down direction** — array position is
    incidental and MUST NOT be read by any consumer (DAT-779, the bug this closes):

        level 0 = the COARSEST level; increasing level = FINER.

    So a ``state → city → zip`` drill path stores ``state`` at level 0, ``city`` at
    1, ``zip`` at 2. For ``alias`` / ``role`` structures the members are a peer set
    with no coarse/fine axis; ``level`` is then a stable ordinal only
    (canonical / sorted-first = 0) and carries no drill meaning.
    """

    model_config = ConfigDict(extra="forbid")

    column_name: str
    # The catalog SliceDefinition's underlying column (provenance); "" when the
    # enriched-view column has no catalog row, or a manual teach could not resolve it.
    column_id: str
    # Null-aware d2 distinct count (DAT-761 null lane); None on a manual teach
    # (no measured scan).
    distinct_count: int | None
    level: int = Field(ge=0)


class RoleEvidence(BaseModel):
    """The stack-v4 role-check evidence for one near-copy pair (DAT-784).

    Persisted as ``dimension_hierarchies.role_evidence`` JSON on rows whose kind was
    decided by the role check (``kind='role'``, or a ``kind='alias'`` surfaced from a
    VALUE_SYSTEMATIC / ABSTAIN verdict). Mirrors ``stats.RoleResult`` plus the
    disagreement rate (formerly conflated into ``score``) so nothing the verdict
    object carries is lost — DAT-762's conform/role judge consumes exactly this.
    NULL on rows with no role check (genuine drilldown / alias group / manual teach).
    """

    model_config = ConfigDict(extra="forbid")

    t1_p: float  # best (smallest) membership p across contexts
    t1_context: str | None  # the context column that produced t1_p
    t2_p: float  # value-concentration p (dis vs B)
    k_disagree: int  # disagreement count |{A ≠ B}|
    alpha: float  # the Bonferroni-corrected threshold both tests were held to
    disagree_rate: float  # fraction of rows where A ≠ B (was overloaded into score)


class DimensionHierarchy(Base):
    """One drill-down chain, alias group or role pair over a fact's enriched view, per run.

    The deterministic FD pass (DAT-537, stack v4 since DAT-761) writes one row
    per discovered structure. Three kinds share the table via the ``kind``
    discriminator:

    - ``kind='drilldown'``: an ordered drill-down hierarchy. ``members`` carries the
      levels ordered by each member's ``level`` (see :class:`HierarchyMember` — the
      ONE place the direction is defined), e.g. ``state → city → zip``;
      ``canonical_label`` renders the same order.
    - ``kind='alias'``: a 1:1 redundant-axis group (bidirectional ``g3 ≈ 0``).
      ``members`` lists the equivalent columns; ``canonical_label`` is the chosen
      canonical axis name. A near-copy the role check could not decide surfaces
      as an alias with ``needs_confirmation=True`` (never silently merged) —
      ``role_verdict`` = value_systematic / abstain and ``role_evidence`` carries why.
    - ``kind='role'`` (DAT-761): a role-playing near-copy pair (bill-to ⇄ pay-to)
      whose disagreement set is membership-systematic — the two columns are the
      SAME domain in different roles: kept as separate axes, never merged, never
      stacked as levels. ``role_verdict='role'`` with the full ``role_evidence``
      (p-values, discriminating context column, disagreement count + rate).

    Run-versioned like ``MeasureAggregationLineage`` (DAT-491): form-(a) writer —
    one row per ``(signature, run_id)``, UPSERTed, so a Temporal success-redelivery
    (same ``run_id``, deterministic g3) converges in place and prior runs' rows
    coexist. ``signature`` is the run-grain dedup key:
    ``"{kind}:{table_id}:" + "|".join(sorted(member column_names))``.

    Sealed under the begin_session ``(catalog, "catalog")`` head: it is on
    ``read_views._CATALOG_GRAIN``, so ``current_dimension_hierarchies`` resolves
    the promoted run and ``session_promote_to_latest`` flips it with the rest of
    the catalog — no separate promote.
    """

    __tablename__ = "dimension_hierarchies"
    __table_args__ = (
        UniqueConstraint("signature", "run_id", name="uq_dimension_hierarchy_signature_run"),
        Index("idx_dimension_hierarchies_table", "table_id"),
        Index("idx_dimension_hierarchies_run", "run_id"),
        # Closed-vocabulary enforcement (DAT-784, the DAT-781 two-layer standard):
        # the producer sets ``role_verdict`` from the ``RoleVerdict`` enum (layer 1);
        # the CHECK — derived from that same enum so the two can never drift — is the
        # DB-enforced backstop (layer 2). NULL passes (non-role-check rows).
        CheckConstraint(
            "role_verdict IN (" + ", ".join(f"'{v}'" for v in _ROLE_VERDICT_VALUES) + ")",
            name="role_verdict",
        ),
        # Structure-kind vocabulary (DAT-802 enum-standard sweep): the three
        # discriminator values ``_hierarchy_row`` (processor.py) ever writes — see
        # the class docstring. Sibling of ``role_verdict`` in the same DAT-784
        # commit era that the CHECK pass left uncovered (no Python enum exists for
        # this one; hand-typed inline, matching the ``relationship_type`` precedent).
        CheckConstraint("kind IN ('drilldown', 'alias', 'role')", name="kind"),
        # Detection-source vocabulary (DAT-802): 'g3' (auto-discovered, the
        # default) or 'manual' (a teach add/alias assertion) — the only two values
        # ``processor.py`` ever writes.
        CheckConstraint("detection_source IN ('g3', 'manual')", name="detection_source"),
    )

    hierarchy_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    # Snapshot version axis (DAT-448): the begin_session run that derived this.
    run_id: Mapped[str] = mapped_column(String, nullable=False)
    # The fact whose grain-verified enriched view the chain was computed over
    # (cross-table levels surface for free on the denormalized view).
    table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)

    kind: Mapped[str] = mapped_column(String, nullable=False)  # 'drilldown' | 'alias' | 'role'

    # Ordered member columns, each validated by :class:`HierarchyMember` at write
    # (the two-layer standard for a JSON interior — a CHECK cannot reach inside).
    # Each entry: {column_name, column_id, distinct_count, level}. ORDER IS CARRIED
    # BY ``level`` (see :class:`HierarchyMember`), NOT by array position (DAT-779).
    # ``column_id`` is the catalog SliceDefinition's underlying column
    # (provenance); ``column_name`` is the enriched-view column the g3 pass measured
    # (may be an FK-prefixed dim col) and is the member identity.
    members: Mapped[list[dict[str, object]]] = mapped_column(JSON, nullable=False)
    canonical_label: Mapped[str] = mapped_column(String, nullable=False)

    # Run-grain dedup key (see class docstring); the UNIQUE rides on it because a
    # JSON ``members`` column cannot be a conflict target.
    signature: Mapped[str] = mapped_column(String, nullable=False)

    # The g3 evidence, kind-INVARIANT (DAT-784): the highest edge g3 in a drilldown
    # chain (the weakest link; max over edges), or the bidirectional g3 for an alias
    # group. Lower = stronger (0 = every edge is an exact FD; a manual teach asserts
    # 0). NULL on a role-check-derived row (kind='role', or a value_systematic /
    # abstain alias) — those have no g3; their disagreement rate lives in
    # ``role_evidence.disagree_rate``, never here (that overload was the bug).
    g3: Mapped[float | None] = mapped_column(Float, nullable=True)

    # The stack-v4 role-check outcome (DAT-784), from the ``RoleVerdict`` enum:
    # 'role' | 'value_systematic' | 'abstain'. NULL on rows with no role check
    # (genuine drilldown / alias group / manual teach). Closed vocab: see the CHECK.
    role_verdict: Mapped[str | None] = mapped_column(String, nullable=True)
    # The role-check evidence, validated by :class:`RoleEvidence` at write (the
    # two-layer JSON standard). NULL whenever ``role_verdict`` is NULL. DAT-762's
    # conform/role judge consumes this; this task only PERSISTS it.
    role_evidence: Mapped[dict[str, object] | None] = mapped_column(JSON, nullable=True)

    # 'g3' (auto-discovered) | 'manual' (a teach add/alias assertion).
    detection_source: Mapped[str] = mapped_column(String, nullable=False, default="g3")
    # Low-support / borderline edges are surfaced for confirmation, not silently
    # auto-asserted (DAT-537 guard). A manual teach clears it.
    needs_confirmation: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
