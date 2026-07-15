"""Column meaning feed for graph execution (DAT-769).

The grounding context every metric/cycle prompt transports: each column's
LLM-authored business MEANING (free prose, ambiguity expressible) plus the
deterministic measurement facts from their own homes — the aggregation-lineage
reconciliation (the strongest semantic fact we own about a measure, DAT-759),
the unit source, temporal behavior, and the object-grain role garnish. The
vertical ontology grounds the system as SERVED CONTEXT (the semantic agent
authors meanings with the full ontology in-prompt; the SQL/metric agent gets
the concept vocabulary separately) — never as tokens attached to columns.

This replaced the ``business_concept → column`` mapping table (DAT-769): the
single categorical binding was ill-posed for multi-facet columns and no
consumer ever branched on it — every reader rendered it into prompt prose. The
feed now renders honest meaning instead of a forced label; the reading agent
resolves ontology ``standard_field`` references in-context from the meanings.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import select

from dataraum.analysis.lineage.db_models import MeasureAggregationLineage
from dataraum.analysis.semantic.db_models import ColumnConcept, SemanticAnnotation
from dataraum.storage import Column, Table

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@dataclass
class ColumnMeaning:
    """One column's transported semantics: authored meaning + measured facts."""

    column_id: str
    column_name: str
    table_name: str
    meaning: str
    unit_source_column: str | None = None
    temporal_behavior: str | None = None
    semantic_role: str | None = None
    entity_type: str | None = None
    # Aggregation-lineage reconciliation (DAT-759) — deterministic, data-grounded.
    lineage_pattern: str | None = None  # per_period | cumulative
    lineage_convention: str | None = None  # e.g. '"quantity"' or '"gross" - "fees"'
    lineage_event_table: str | None = None
    lineage_match_rate: float | None = None


def load_column_meanings(
    session: Session,
    table_ids: list[str],
    *,
    catalogue_run_id: str | None = None,
) -> list[ColumnMeaning]:
    """Load the per-column meaning feed from ``ColumnConcept`` (DAT-637/769).

    ``meaning`` is catalogue-grain: authored by the table agent and sealed under
    the begin_session catalogue head. ``catalogue_run_id`` scopes the read to that
    single run (fail-closed: ``None`` ⇒ empty feed, never a cross-run read). The
    object-grain role/entity_type garnish is outer-joined from
    ``SemanticAnnotation``; the reconciliation facts from
    ``MeasureAggregationLineage`` under the same catalogue run.

    Args:
        session: Database session.
        table_ids: Table IDs to load the feed for.
        catalogue_run_id: The begin_session catalogue head run the meanings live
            under (``base_runs.relationship_run_id``).

    Returns:
        One :class:`ColumnMeaning` per meaning-carrying column, in
        (table, column) order.
    """
    if not table_ids or not catalogue_run_id:
        return []

    stmt = (
        select(ColumnConcept, Column, Table, SemanticAnnotation)
        .join(Column, ColumnConcept.column_id == Column.column_id)
        .join(Table, Column.table_id == Table.table_id)
        .outerjoin(SemanticAnnotation, SemanticAnnotation.column_id == Column.column_id)
        .where(
            Table.table_id.in_(table_ids),
            ColumnConcept.run_id == catalogue_run_id,
            ColumnConcept.meaning.isnot(None),
        )
        # The SemanticAnnotation garnish (role/entity_type) can fan out across runs;
        # order so the dedup below keeps a deterministic row, not a DB-arbitrary one.
        .order_by(Table.table_name, Column.column_name, SemanticAnnotation.run_id.desc())
    )

    lineage_by_column: dict[str, tuple[str, str, str | None, float]] = {}
    lineage_stmt = (
        select(MeasureAggregationLineage, Table.table_name, Table.layer)
        .outerjoin(Table, MeasureAggregationLineage.event_table_id == Table.table_id)
        .where(
            MeasureAggregationLineage.measure_table_id.in_(table_ids),
            MeasureAggregationLineage.run_id == catalogue_run_id,
        )
    )
    for lineage, event_table_name, event_layer in session.execute(lineage_stmt).all():
        lineage_by_column[lineage.measure_column_id] = (
            lineage.pattern,
            lineage.convention_sql,
            # Same layer-qualified vocabulary as the feed's own table headers, so
            # the one grounding document never names a table two different ways.
            f"{event_layer}_{event_table_name}" if event_table_name else None,
            lineage.match_rate,
        )

    out: list[ColumnMeaning] = []
    seen: set[str] = set()  # column_id — the annotation outerjoin can fan out
    for concept_row, column, table, annotation in session.execute(stmt).all():
        if column.column_id in seen or not concept_row.meaning:
            continue
        seen.add(column.column_id)
        lineage = lineage_by_column.get(column.column_id)
        out.append(
            ColumnMeaning(
                column_id=column.column_id,
                column_name=column.column_name,
                table_name=f"{table.layer}_{table.table_name}",
                meaning=concept_row.meaning,
                unit_source_column=concept_row.unit_source_column,
                temporal_behavior=concept_row.temporal_behavior,
                semantic_role=annotation.semantic_role if annotation else None,
                entity_type=annotation.entity_type if annotation else None,
                lineage_pattern=lineage[0] if lineage else None,
                lineage_convention=lineage[1] if lineage else None,
                lineage_event_table=lineage[2] if lineage else None,
                lineage_match_rate=lineage[3] if lineage else None,
            )
        )
    return out


def format_meanings_for_prompt(meanings: list[ColumnMeaning]) -> str:
    """Render the meaning feed for LLM prompts (metric grounding, cycles).

    Meaning first (the honest characterization the agent reasons over), then the
    deterministic measurement facts, then the non-authoritative ontology hints —
    labeled as related concepts, never as bindings.
    """
    if not meanings:
        return "No column meanings available for this catalogue run."

    lines = ["## COLUMN MEANINGS", ""]
    lines.append("Each column's business meaning, characterized in the context of the whole")
    lines.append("catalogue, with measured facts where they exist.")
    lines.append("")

    current_table = None
    for m in meanings:
        if m.table_name != current_table:
            current_table = m.table_name
            lines.append(f"### {m.table_name}")
        facts: list[str] = []
        if m.semantic_role:
            facts.append(f"role={m.semantic_role}")
        if m.entity_type:
            facts.append(f"entity={m.entity_type}")
        if m.lineage_pattern and m.lineage_convention:
            match = (
                f", match {m.lineage_match_rate:.0%}" if m.lineage_match_rate is not None else ""
            )
            facts.append(
                f"reconciles {m.lineage_pattern} as {m.lineage_convention}"
                + (f" from {m.lineage_event_table}" if m.lineage_event_table else "")
                + match
            )
        if m.temporal_behavior:
            facts.append(f"temporal={m.temporal_behavior}")
        if m.unit_source_column:
            facts.append(f"unit from {m.unit_source_column}")
        fact_str = f" [{'; '.join(facts)}]" if facts else ""
        lines.append(f"- `{m.column_name}`{fact_str}: {m.meaning}")
    lines.append("")
    lines.append(f"Total: {len(meanings)} columns with authored meanings.")
    return "\n".join(lines)


__all__ = [
    "ColumnMeaning",
    "load_column_meanings",
    "format_meanings_for_prompt",
]
