"""Shared utility functions for semantic analysis."""

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.relationships.surrogate import is_surrogate_column
from dataraum.analysis.semantic.db_models import ColumnConcept, SemanticAnnotation
from dataraum.analysis.statistics.models import ColumnProfile
from dataraum.analysis.typing.db_models import TypeCandidate
from dataraum.storage import Column, Table


def truncate_sample_value(value: Any, *, max_chars: int) -> Any:
    """Bound one served corpus value's rendered length.

    DAT-671 prompt-content bounds policy: cap DATA VALUES, never authored
    metadata prose. Only a ``str`` value longer than ``max_chars`` is
    shortened; anything else (a number, a bool, an already-short string)
    passes through UNCHANGED — a JSON ``sample_values`` array must keep a
    numeric column's samples typed as numbers, not stringify them. A
    prose-line caller (the catalogue/cycles context builders) interpolates
    the result with an f-string, which stringifies either way, so the same
    function serves both.

    This is the ONE home for what used to be four independently drifting
    100-char constants (``catalogue/context.py: _SAMPLE_MAX_CHARS``,
    ``cycles/context.py: _SAMPLE_VALUE_MAX_CHARS``, and the semantic agents'
    own ``_truncate_sample``, duplicated in both ``column_agent.py`` and
    ``agent.py``) — every caller now reads
    ``llm/config.yaml: privacy.max_sample_value_chars`` instead of a local
    number.
    """
    if isinstance(value, str) and len(value) > max_chars:
        return value[:max_chars] + "..."
    return value


def prompt_samples(
    profiles: list[ColumnProfile], *, limit: int, max_chars: int
) -> dict[tuple[str, str], list[Any]]:
    """Per-column value samples for a prompt, count- AND length-capped (DAT-890/DAT-671).

    The profiler stores ``top_k_values`` (200) per column for downstream
    analysis; ``limit`` is what may reach a PROMPT
    (``llm/config.yaml: privacy.max_sample_values``, 10). The two are different
    budgets and the gap is not decoration: serving the profiler's full 200 put
    8,722 raw values into one ``column_annotation`` prompt — 88% of its bytes
    and 45% digits — against the ~700 the cap allows. Every other prompt
    builder applies this cap (``analysis/catalogue/context.py`` renders
    ``top_values[:limit]``); the deleted ``llm/privacy.py`` sampler skipped it,
    which is how the two semantic agents alone shipped 20x their budget.
    ``max_chars`` (``privacy.max_sample_value_chars``) bounds each individual
    value's length via :func:`truncate_sample_value` — the count cap alone
    does not stop one pathologically long value from dominating the budget.

    Returns ``{(table_name, column_name): values}``. A column with no stored
    top values yields an empty list — absence stays visible as absence.
    """
    return {
        (p.column_ref.table_name, p.column_ref.column_name): [
            truncate_sample_value(vc.value, max_chars=max_chars)
            for vc in (p.top_values or [])[:limit]
        ]
        for p in profiles
    }


def load_column_concepts(
    session: Session,
    table_ids: list[str],
    catalogue_run_id: str,
) -> dict[str, ColumnConcept]:
    """Catalogue-grain per-column semantics for ``table_ids`` at the catalogue head run.

    The ONLY reader of :class:`ColumnConcept` (DAT-637). ``catalogue_run_id`` is
    **mandatory** — the catalogue-grain fields (meaning, ontology hints,
    temporal_behavior, unit_source_column, derived_formula hypothesis) live only
    under the begin_session catalogue head, so a caller MUST hold that run to read
    them. Object-grain code (add_source ``detect``) has no catalogue run and so
    cannot reach these by construction — the cross-grain read is unexpressible.

    Returns ``{column_id: ColumnConcept}`` for the run; columns the table agent did
    not bind are simply absent.
    """
    if not table_ids:
        return {}
    stmt = (
        select(ColumnConcept)
        .join(Column, ColumnConcept.column_id == Column.column_id)
        .where(Column.table_id.in_(table_ids), ColumnConcept.run_id == catalogue_run_id)
    )
    return {cc.column_id: cc for cc in session.execute(stmt).scalars()}


def load_table_mappings(
    session: Session,
    table_ids: list[str],
) -> dict[str, str]:
    """Load mapping of table_name -> table_id.

    Args:
        session: Database session
        table_ids: List of table IDs to load mappings for

    Returns:
        Dictionary mapping table_name to table_id
    """
    stmt = select(Table.table_name, Table.table_id).where(Table.table_id.in_(table_ids))
    result = session.execute(stmt)
    return dict(result.tuples().all())


def load_column_mappings(
    session: Session,
    table_ids: list[str],
) -> dict[tuple[str, str], str]:
    """Load mapping of (table_name, column_name) -> column_id.

    Args:
        session: Database session
        table_ids: List of table IDs to load mappings for

    Returns:
        Dictionary mapping (table_name, column_name) tuples to column_id
    """
    stmt = (
        select(Table.table_name, Column.column_name, Column.column_id)
        .join(Column)
        .where(Table.table_id.in_(table_ids))
    )
    result = session.execute(stmt)
    return {(table_name, col_name): col_id for table_name, col_name, col_id in result.all()}


def load_persisted_annotations(
    session: Session,
    table_ids: list[str],
) -> list[dict[str, Any]]:
    """Load persisted per-column semantic annotations for the given tables.

    The per-table synthesis phase reads these as read-only context — the
    OBJECT-grain column annotations the per-column agent produced (role, entity
    label, term, the stock/flow claim). It does NOT include catalogue-grain ``meaning``:
    that is catalogue-grain and AUTHORED by the table agent itself (DAT-637), so
    feeding it back would be the dual-ownership we removed. Returns one dict per
    annotated column, ordered by table then column.

    Args:
        session: Database session.
        table_ids: Table IDs whose columns' annotations to load.

    Returns:
        List of ``{table_name, column_name, column_id, semantic_role, entity_type,
        confidence, temporal_behavior_claim, detected_unit}`` dicts, ordered by
        table then column. ``detected_unit`` is the value-carried unit the typing
        phase parsed from the column's VALUES (DAT-647) — fed so the table agent
        can record a measure's unit resolution instead of treating it as unknown.
    """
    stmt = (
        select(
            Table.table_name,
            Column.column_name,
            Column.column_id,
            SemanticAnnotation.semantic_role,
            SemanticAnnotation.entity_type,
            SemanticAnnotation.confidence,
            SemanticAnnotation.temporal_behavior_claim,
        )
        .join(Column, SemanticAnnotation.column_id == Column.column_id)
        .join(Table, Column.table_id == Table.table_id)
        .where(Table.table_id.in_(table_ids))
        .order_by(Table.table_name, Column.column_position)
    )
    # Mint-owned surrogate join keys are excluded HERE rather than left to the
    # INNER JOIN above (DAT-878). Today the join already hides them — a surrogate
    # never gets a SemanticAnnotation, because SemanticAgent._load_profiles skips
    # it — but that makes this prompt incidentally shielded by a filter in another
    # module rather than structurally surrogate-free. If annotations ever cover
    # every column, the shield disappears silently. Rows are join tuples, not
    # Column ORM objects, so this calls the predicate directly (see
    # analysis/served_columns.py).
    rows = [r for r in session.execute(stmt).all() if not is_surrogate_column(r.column_name)]

    # Value-carried unit per column (DAT-647): the CURRENT type candidate's
    # detected_unit. TypeCandidate accumulates across runs (a re-type / teach
    # re-run leaves prior runs' rows in place), so we take the MOST RECENT run's
    # best candidate — mirroring load_typing's run_id=None "most recent" semantics
    # (the promoted re-run after a teach cycle). Ordering by detected_at first
    # avoids a stale prior run's higher-confidence candidate leaking a stale unit.
    # Bulk-loaded once, merged by column_id.
    column_ids = [row.column_id for row in rows]
    detected_units: dict[str, str | None] = {}
    if column_ids:
        unit_rows = session.execute(
            select(TypeCandidate.column_id, TypeCandidate.detected_unit)
            .where(TypeCandidate.column_id.in_(column_ids))
            .order_by(TypeCandidate.detected_at.desc(), TypeCandidate.confidence.desc())
        ).all()
        for column_id, detected_unit in unit_rows:
            detected_units.setdefault(column_id, detected_unit)

    return [
        {
            "table_name": row.table_name,
            "column_name": row.column_name,
            "column_id": row.column_id,
            "semantic_role": row.semantic_role,
            "entity_type": row.entity_type,
            "confidence": row.confidence,
            "temporal_behavior_claim": row.temporal_behavior_claim,
            "detected_unit": detected_units.get(row.column_id),
        }
        for row in rows
    ]
