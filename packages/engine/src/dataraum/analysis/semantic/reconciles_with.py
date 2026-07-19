"""Derived ``reconciles_with`` concept edges (DAT-727 part c).

The two live producers of the reconciliation assertion, run at the end of the
metrics phase, both writing CONCEPT-GRAIN self-loop rows into ``concept_edges``
(``source='derived'``):

1. **Aggregation-lineage witness** — a measure column whose events→measure
   rollup RECONCILED (a ``measure_aggregation_lineage`` row at the pinned
   catalogue run) has two data-proven computations of one quantity: the
   measure reading and the event aggregation. Every concept grounded on that
   measure column (a healthy grounding whose enumerated ``measure_columns``
   resolve to it) must tie out against the rollup → self-loop on that concept.
2. **Multi-grounding** — a concept holding ≥2 healthy groundings (e.g.
   ``account_balance`` across trial_balance / balance_sheet) has two committed
   computations that must tie out → self-loop on that concept.

Why concept-grain self-loops and NOT a Grounding→Grounding edge (the DAT-727
design fork, ruled 2026-07-19):

- **The witness's event side is not a Grounding.** The witness reconciles a
  measure column against an event-table rollup (``convention_sql``) — the
  GraphAgent is the only sql_snippets writer and nothing mints a snippet for
  that rollup, so a grounding-grain edge would have no event endpoint to bind.
  The concept-grain assertion ("this concept's computations must tie out")
  represents it faithfully.
- **The grounding-pair grain is already derivable — one home.** Which concrete
  groundings pair up falls out of the operating-model graph's ``grounded_by``
  fan-out (all healthy groundings of the concept, one 2-hop MATCH); a
  materialized Grounding→Grounding edge would duplicate derivable information.

The self-loop is an ASSERTION, not a pairing — ``uq_concept_edge_active``
holding it to ONE active row per concept is correct (both producers firing on
one concept still mean one assertion). ``tolerance`` stays NULL: no vertical
declares a per-concept reconciliation band today (validation-spec tolerances
are a different mechanism); when one does, the seed carries it.

**Lifecycle.** Rows follow the ConceptEdge supersession contract: each run
inserts missing assertions (``insert_if_absent`` on the active-row partial
index — race-safe, never clobbers) and supersedes ``derived`` self-loops whose
support vanished (a witness gone on re-run, a grounding decayed to failed).
Seed rows are NEVER touched — the reconcile is scoped ``source='derived'``.

**In-run reads (ADR-0008).** This runs inside the operating-model session, so
it never reads ``current_*`` views: groundings are the un-versioned
sql_snippets rows, the witness is pinned to the catalogue run, and the served
relation→column resolution mirrors ``og_uses``' (property_graph.py) over the
same typed substrate — enriched relation → the view's own served columns
(matched by ``column_id`` or ``source_column_id``), typed relation → its
columns by ``table_name``/``duckdb_path``.
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime

from sqlalchemy import or_, select, text
from sqlalchemy.orm import Session

from dataraum.analysis.lineage.db_models import MeasureAggregationLineage
from dataraum.analysis.semantic.db_models import Concept, ConceptEdge, ConceptEdgePredicate
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.core.logging import get_logger
from dataraum.query.snippet_models import SQLSnippetRecord
from dataraum.storage.models import Column, Table
from dataraum.storage.upsert import insert_if_absent

logger = get_logger(__name__)

_EDGE_INDEX_ELEMENTS = ["vertical", "predicate", "from_concept", "to_concept"]
_RECONCILES = ConceptEdgePredicate.RECONCILES_WITH.value


def derive_reconciles_with(
    session: Session,
    *,
    vertical: str,
    catalogue_run_id: str | None,
) -> tuple[int, int]:
    """Reconcile the ``derived`` reconciles_with self-loop set for one vertical.

    Args:
        session: Workspace session (in-run — no ``current_*`` reads).
        vertical: The workspace's vertical; concept names resolve within it.
        catalogue_run_id: The pinned begin_session run whose aggregation-lineage
            witnesses count (the same pin the phase's other catalogue reads
            use). ``None`` — no pinned catalogue run — means no witness
            evidence exists; only the multi-grounding producer applies.

    Returns:
        ``(inserted, superseded)`` — rows added for newly supported assertions,
        and ``derived`` rows retired because their support vanished.
    """
    active_concepts = {
        name
        for (name,) in session.execute(
            select(Concept.name).where(
                Concept.vertical == vertical, Concept.superseded_at.is_(None)
            )
        )
    }
    healthy = list(
        session.execute(
            select(SQLSnippetRecord).where(
                SQLSnippetRecord.snippet_type == "extract",
                SQLSnippetRecord.source.like("graph:%"),
                SQLSnippetRecord.failure_count == 0,
            )
        ).scalars()
    )

    expected: set[str] = set()

    # Producer 2 (multi-grounding): ≥2 healthy groundings of one concept.
    counts = Counter(s.standard_field for s in healthy if s.standard_field)
    expected.update(f for f, n in counts.items() if n >= 2 and f in active_concepts)

    # Producer 1 (witness): a healthy grounding measuring a witnessed column.
    witnessed_ids: set[str] = set()
    if catalogue_run_id is not None:
        witnessed_ids = {
            column_id
            for (column_id,) in session.execute(
                select(MeasureAggregationLineage.measure_column_id).where(
                    MeasureAggregationLineage.run_id == catalogue_run_id
                )
            )
        }
    if witnessed_ids:
        for snippet in healthy:
            field = snippet.standard_field
            if not field or field not in active_concepts or field in expected:
                continue
            if _measures_witnessed_column(session, snippet, witnessed_ids):
                expected.add(field)

    inserted = 0
    if expected:
        inserted = insert_if_absent(
            session,
            ConceptEdge,
            [
                {
                    "vertical": vertical,
                    "predicate": _RECONCILES,
                    "from_concept": name,
                    "to_concept": name,
                    "source": "derived",
                }
                for name in sorted(expected)
            ],
            index_elements=_EDGE_INDEX_ELEMENTS,
            index_where=text("superseded_at IS NULL"),
        )

    # Retire derived self-loops whose support vanished. Scope: THIS producer's
    # rows only — source='derived' self-loops of this vertical; seed rows (and
    # any future non-self-loop derived writer's rows) are never touched.
    superseded = 0
    stale = session.execute(
        select(ConceptEdge).where(
            ConceptEdge.vertical == vertical,
            ConceptEdge.predicate == _RECONCILES,
            ConceptEdge.source == "derived",
            ConceptEdge.from_concept == ConceptEdge.to_concept,
            ConceptEdge.superseded_at.is_(None),
            # Empty expected set → NOT IN () renders true → every derived
            # self-loop is stale (all support vanished), which is the intent.
            ConceptEdge.from_concept.not_in(sorted(expected)),
        )
    ).scalars()
    now = datetime.now(UTC)
    for edge in stale:
        edge.superseded_at = now
        superseded += 1

    if inserted or superseded:
        logger.info(
            "reconciles_with_derived",
            vertical=vertical,
            asserted=len(expected),
            inserted=inserted,
            superseded=superseded,
        )
    return inserted, superseded


def _measures_witnessed_column(
    session: Session,
    snippet: SQLSnippetRecord,
    witnessed_ids: set[str],
) -> bool:
    """Does this grounding's enumerated measure read a witnessed column?

    Resolves the snippet's served relation to its column set exactly like
    ``og_uses`` (property_graph.py) — the enforced contract-v2 names against
    the SERVED relation — then matches the witness's TYPED measure_column_id
    directly (typed relation) or through ``source_column_id`` (an enriched
    view's f.* passthrough of the measure). Pre-v2 rows carry no
    ``measure_columns`` arrays and resolve to nothing (clean cut, no backfill).
    """
    parts = snippet.parts or {}
    relations = parts.get("from") or []
    relation = relations[0] if relations else None
    if not relation:
        return False
    basis = (snippet.provenance or {}).get("column_mappings_basis") or {}
    measure_names = {
        name
        for entry in basis.values()
        if isinstance(entry, dict)
        for name in entry.get("measure_columns") or []
    }
    if not measure_names:
        return False

    view_table_ids = [
        vt_id
        for (vt_id,) in session.execute(
            select(EnrichedView.view_table_id).where(
                EnrichedView.view_name == relation, EnrichedView.view_table_id.is_not(None)
            )
        )
    ]
    if view_table_ids:
        served = session.execute(
            select(Column.column_id, Column.source_column_id).where(
                Column.table_id.in_(view_table_ids),
                Column.column_name.in_(measure_names),
            )
        ).all()
        return any(
            column_id in witnessed_ids or source_id in witnessed_ids
            for column_id, source_id in served
        )

    typed = session.execute(
        select(Column.column_id)
        .join(Table, Table.table_id == Column.table_id)
        .where(
            Table.layer == "typed",
            or_(Table.table_name == relation, Table.duckdb_path == relation),
            Column.column_name.in_(measure_names),
        )
    ).scalars()
    return any(column_id in witnessed_ids for column_id in typed)


__all__ = ["derive_reconciles_with"]
