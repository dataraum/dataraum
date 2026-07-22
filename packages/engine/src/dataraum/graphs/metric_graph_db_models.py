"""SQLAlchemy models for the metric DAG typed home (DAT-732).

The metric transformation graphs — until now parsed from vertical YAML ⊕ ``metric``
teach overlays into in-memory :class:`~dataraum.graphs.models.TransformationGraph`
objects on every run — get a persistent, queryable, graph-projectable home. Three
tables reify the DAG structure so the operating-model property graph (ADR-0021) can
carry ``Metric`` vertices and walk ``metric → concept → grounding → column``:

* :class:`Metric` — one node per declared metric (``graph_id``).
* :class:`MetricParameter` — one node per user-configurable parameter, carrying the
  DECLARED default (typed metadata) and an optional ``derivation`` marker naming the
  rule that overrides it at runtime (``days_in_period`` → ``period_grain``). The
  data-derived override stays a RESOLVER computation
  (:mod:`dataraum.graphs.period_resolver`), NEVER a stored value — the owner-ruled
  split between declared metadata (here) and observed value (the resolver).
* :class:`MetricDerivesFrom` — one edge per (metric, extracted concept): the metric
  derives from the concepts its EXTRACT leaves ground. This is the ``derives_from``
  edge the property graph projects (metric → concept), collapsing the extract layer
  so the walk to groundings is one hop.

**Identity contract — NOT run-versioned (the DAT-728 pattern).** A declared metric is
a stable node, not a per-run measurement: it changes only when the DECLARATION changes
(a ``metric`` teach / future ``frame`` edit), never when a run recomputes data. So the
axis is declaration-versioned exactly like :class:`~dataraum.analysis.semantic.db_models.Concept`
— a stable ``(vertical, graph_id[, …])`` key, ``superseded_at`` the sole lifecycle axis,
seeded ``INSERT … ON CONFLICT DO NOTHING`` so a re-run is a no-op and a supersede is
never clobbered. This is deliberately NOT the run-versioned axis of
:class:`~dataraum.graphs.additivity_db_models.MetricAdditivity` (a verdict RECOMPUTED
every operating_model run from live ``temporal_behavior``); the DAG STRUCTURE is not
recomputed from data, so keying it on ``run_id`` would be wrong. Workspace identity is
the ``ws_<id>`` schema itself (no ``workspace_id`` column), and the read surface scopes
to the workspace's single bound ``active_vertical`` (``_VERTICAL_SCOPED`` in
``storage/read_views.py``), so a wrong ``--vertical`` cannot leak another vertical's
metrics into the graph.

YAML remains the SEED and still carries the executable formulas (this table persists
the graph-walkable STRUCTURE, not the formula expressions — those stay in the
vertical's ``metrics/**`` files, parsed into ``TransformationGraph`` for execution).
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from uuid import uuid4

from sqlalchemy import JSON, CheckConstraint, DateTime, Index, String, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage import Base


class MetricParameterDerivation(StrEnum):
    """How a metric parameter's runtime value is DERIVED when the user provides none.

    The typed home of the declared-default ↔ observed-override split (DAT-732). A
    parameter carries its DECLARED default as metadata; ``derivation`` names the RULE
    that computes an override from the data at runtime (never a stored value):

    - ``PERIOD_GRAIN`` — resolved from the flow's observed accumulation window over
      the period-grain ladder (:mod:`dataraum.graphs.period_resolver`, DAT-785). The
      first and only instance today: ``days_in_period``. NULL ⇒ no derivation, the
      declared default stands unless the caller overrides it.
    """

    PERIOD_GRAIN = "period_grain"


# Closed-vocabulary CHECK values, each derived from its single-home enum so the CHECK
# and the enum can never drift (the DAT-802 / DAT-784 pattern). Sorted for a
# deterministic CHECK string in the offline DDL dump.
_PARAMETER_DERIVATION_VALUES: tuple[str, ...] = tuple(
    sorted(v.value for v in MetricParameterDerivation)
)

# Lifecycle-source vocabulary: only 'seed' has a live writer today
# (:func:`dataraum.graphs.metric_store.ensure_metrics_seeded` — the shipped-vertical
# ⊕ teach-overlay declared set). 'frame' (the cockpit's authoring path) stays OUT
# until that writer exists — a CHECK admitting a value no writer produces is the exact
# DAT-802 defect; widening is one line + a re-dump when the writer lands.
_METRIC_SOURCE_VALUES: tuple[str, ...] = ("seed",)


class Metric(Base):
    """One declared metric node — the metric-DAG vertex (DAT-732).

    Keyed by the stable ``(vertical, graph_id)`` identity; ``metric_id`` is a
    workspace-stable surrogate minted once at seed (the PGQ vertex KEY), NOT a per-run
    uuid. Carries the metric's declared output metadata (unit / output_type /
    category) so a graph consumer reads the node's shape without re-parsing YAML. Edits
    supersede rather than collide (``superseded_at`` + the ``uq_metric_active`` partial
    unique index keeps one active row per ``(vertical, graph_id)``), so a head-free read
    is deterministic. Bound into the property graph as the ``metric_node`` vertex over
    ``og_metrics``.
    """

    __tablename__ = "metrics"
    __table_args__ = (
        # At most one ACTIVE row per (vertical, graph_id); superseded history rows are
        # exempt. Postgres/SQLite partial unique index — the deterministic single-active-
        # row guarantee the head-free reads (and the seed's ON CONFLICT) rely on.
        Index(
            "uq_metric_active",
            "vertical",
            "graph_id",
            unique=True,
            postgresql_where=text("superseded_at IS NULL"),
            sqlite_where=text("superseded_at IS NULL"),
        ),
        CheckConstraint(
            "source IS NULL OR source IN ("
            + ", ".join(f"'{v}'" for v in _METRIC_SOURCE_VALUES)
            + ")",
            name="source",
        ),
    )

    metric_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    vertical: Mapped[str] = mapped_column(String, nullable=False)
    graph_id: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    category: Mapped[str | None] = mapped_column(String)
    unit: Mapped[str | None] = mapped_column(String)
    output_type: Mapped[str | None] = mapped_column(
        String
    )  # OutputType value (scalar|series|table)
    version: Mapped[str | None] = mapped_column(String)

    # Lifecycle: workspace-persistent with supersession (NULL superseded_at = active).
    # Closed vocab: see ck_metrics_source — 'seed' is the one live writer.
    source: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    superseded_at: Mapped[datetime | None] = mapped_column(DateTime)


class MetricParameter(Base):
    """One user-configurable parameter node of a metric (DAT-732).

    The typed home of :class:`~dataraum.graphs.models.ParameterDef` — until now
    persisted nowhere. Keyed by the stable ``(vertical, graph_id, name)`` identity;
    ``parameter_id`` is the PGQ vertex KEY surrogate. ``default_value`` / ``options``
    are JSON so a numeric default (``days_in_period`` = 30) round-trips typed, not as a
    string. ``derivation`` (:class:`MetricParameterDerivation`) marks a parameter whose
    runtime value is computed by a rule when the caller provides none — NULL for a plain
    constant. Bound into the property graph as the ``parameter_node`` vertex over
    ``og_metric_parameters``, reachable from its metric via the ``has_parameter`` edge.

    The runtime merge point (``GraphAgent._resolve_parameters``) reads the DECLARED
    default from HERE, not from the raw parsed graph — this table is the authority.
    """

    __tablename__ = "metric_parameters"
    __table_args__ = (
        Index(
            "uq_metric_parameter_active",
            "vertical",
            "graph_id",
            "name",
            unique=True,
            postgresql_where=text("superseded_at IS NULL"),
            sqlite_where=text("superseded_at IS NULL"),
        ),
        # Derivation vocabulary (DAT-732): NULL-or-IN, meaningful only on a parameter
        # whose value is rule-derived. The same NULL-until-a-writer discipline as
        # Concept.ordering — never inferred, declared in the seed (the vertical YAML).
        CheckConstraint(
            "derivation IS NULL OR derivation IN ("
            + ", ".join(f"'{v}'" for v in _PARAMETER_DERIVATION_VALUES)
            + ")",
            name="derivation",
        ),
        CheckConstraint(
            "source IS NULL OR source IN ("
            + ", ".join(f"'{v}'" for v in _METRIC_SOURCE_VALUES)
            + ")",
            name="source",
        ),
    )

    parameter_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    vertical: Mapped[str] = mapped_column(String, nullable=False)
    graph_id: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    param_type: Mapped[str] = mapped_column(
        String, nullable=False
    )  # integer|float|date|bool|string
    default_value: Mapped[object | None] = mapped_column(JSON)  # typed default (JSON-preserved)
    options: Mapped[object | None] = mapped_column(JSON)  # enum-like choices, or NULL
    description: Mapped[str | None] = mapped_column(Text)
    # The derivation rule that overrides the declared default at runtime, or NULL.
    # Closed vocab: see ck_metric_parameters_derivation.
    derivation: Mapped[str | None] = mapped_column(String)  # MetricParameterDerivation

    source: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    superseded_at: Mapped[datetime | None] = mapped_column(DateTime)


class MetricDerivesFrom(Base):
    """One ``derives_from`` edge: a metric derives from an extracted concept (DAT-732).

    Reifies the metric-DAG's extract layer at concept grain — one row per
    (metric, distinct extract ``standard_field``). ``concept_name`` is the concept's
    stable name within ``vertical`` (the extract's ``standard_field``), NEVER a
    ``concept_id`` surrogate: the ``og_derives_from`` element view resolves it to the
    ACTIVE :class:`~dataraum.analysis.semantic.db_models.Concept` for the PGQ vertex
    binding — the same ``(vertical, name)`` INNER-JOIN discipline ``og_grounded_by`` /
    ``og_concept_edges`` use, so an edge whose concept is superseded/absent simply drops
    from the graph (it never dangles). This makes the walk ``metric → derives_from →
    concept → grounded_by → grounding → uses → column`` one PGQ MATCH.

    metric → METRIC edges are deliberately absent: the ``TransformationGraph`` model has
    no cross-graph reference (a formula step's ``depends_on`` is intra-graph, and the
    working-capital metrics inline their sub-metrics as internal formula steps rather
    than referencing another ``graph_id``). A metric→metric edge would need a cross-graph
    reference in the model first; none exists, so none is projected.
    """

    __tablename__ = "metric_derives_from"
    __table_args__ = (
        Index(
            "uq_metric_derives_from_active",
            "vertical",
            "graph_id",
            "concept_name",
            unique=True,
            postgresql_where=text("superseded_at IS NULL"),
            sqlite_where=text("superseded_at IS NULL"),
        ),
    )

    edge_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    vertical: Mapped[str] = mapped_column(String, nullable=False)
    graph_id: Mapped[str] = mapped_column(String, nullable=False)
    concept_name: Mapped[str] = mapped_column(String, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    superseded_at: Mapped[datetime | None] = mapped_column(DateTime)
