"""Field mapping for graph execution.

Maps business concepts (e.g., 'revenue', 'accounts_receivable') to concrete
columns in the dataset based on semantic annotations.

This enables metrics that use `standard_field` references to resolve to
actual column names based on the LLM-detected business_concept mappings.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.db_models import ColumnConcept, SemanticAnnotation
from dataraum.storage import Column, Table

if TYPE_CHECKING:
    pass


@dataclass
class ColumnCandidate:
    """A column that matches an ontology term."""

    column_id: str
    column_name: str
    table_name: str
    confidence: float = 0.5
    semantic_role: str | None = None
    entity_type: str | None = None


@dataclass
class FieldMappings:
    """Collection of business concept → column mappings."""

    # Maps business_concept → list of matching columns
    mappings: dict[str, list[ColumnCandidate]] = field(default_factory=dict)

    # Tracks which tables were scanned
    table_ids: list[str] = field(default_factory=list)

    def get_all_columns(self, business_concept: str) -> list[ColumnCandidate]:
        """Get all matching columns for a business concept.

        Args:
            business_concept: Standard field name (e.g., 'revenue')

        Returns:
            List of matching columns, sorted by confidence
        """
        candidates = self.mappings.get(business_concept, [])
        return sorted(candidates, key=lambda c: c.confidence, reverse=True)

    @property
    def available_concepts(self) -> list[str]:
        """List all business concepts that have mappings."""
        return [concept for concept, cols in self.mappings.items() if cols]

    @property
    def total_mappings(self) -> int:
        """Total number of column mappings across all terms."""
        return sum(len(cols) for cols in self.mappings.values())


def load_semantic_mappings(
    session: Session,
    table_ids: list[str],
    *,
    catalogue_run_id: str | None = None,
) -> FieldMappings:
    """Load business_concept → column mappings from ``ColumnConcept`` (DAT-637).

    ``business_concept`` is catalogue-grain: authored by the table agent and
    sealed under the begin_session catalogue head. ``catalogue_run_id`` scopes the
    read to that single run (fail-closed: ``None`` ⇒ no mappings, never a cross-run
    read). The object-grain role/entity_type garnish is outer-joined from
    ``SemanticAnnotation`` — a descriptive hint on the candidate, not the
    grounding decision (which is the concept binding itself).

    Args:
        session: Database session.
        table_ids: Table IDs to load mappings for.
        catalogue_run_id: The begin_session catalogue head run the concepts live
            under (``base_runs.relationship_run_id``).

    Returns:
        FieldMappings with business_concept → column mappings.
    """
    if not table_ids or not catalogue_run_id:
        return FieldMappings(table_ids=table_ids)

    stmt = (
        select(ColumnConcept, Column, Table, SemanticAnnotation)
        .join(Column, ColumnConcept.column_id == Column.column_id)
        .join(Table, Column.table_id == Table.table_id)
        .outerjoin(SemanticAnnotation, SemanticAnnotation.column_id == Column.column_id)
        .where(
            Table.table_id.in_(table_ids),
            ColumnConcept.run_id == catalogue_run_id,
            ColumnConcept.business_concept.isnot(None),
        )
        # The SemanticAnnotation garnish (role/entity_type) can fan out across runs;
        # order so the dedup below keeps a deterministic row, not a DB-arbitrary one.
        .order_by(SemanticAnnotation.run_id.desc())
    )

    mappings: dict[str, list[ColumnCandidate]] = {}
    seen: set[tuple[str, str]] = set()  # (concept, column_id) — outerjoin can fan out

    for concept_row, column, table, annotation in session.execute(stmt).all():
        concept = concept_row.business_concept
        if not concept or (concept, column.column_id) in seen:
            continue
        seen.add((concept, column.column_id))
        mappings.setdefault(concept, []).append(
            ColumnCandidate(
                column_id=column.column_id,
                column_name=column.column_name,
                table_name=f"{table.layer}_{table.table_name}",
                # A concept binding is authoritative, not probabilistic — the table
                # agent does not author a per-binding confidence, so this is the
                # constant fallback by design (not a missing feature).
                confidence=concept_row.confidence or 0.5,
                semantic_role=annotation.semantic_role if annotation else None,
                entity_type=annotation.entity_type if annotation else None,
            )
        )

    return FieldMappings(mappings=mappings, table_ids=table_ids)


def format_mappings_for_prompt(field_mappings: FieldMappings) -> str:
    """Format field mappings for inclusion in LLM prompts.

    Creates a human-readable representation of the available field mappings
    for the graph agent to use when resolving standard_field references.

    Args:
        field_mappings: The field mappings to format

    Returns:
        Formatted string for prompt context
    """
    if not field_mappings.available_concepts:
        return "No semantic field mappings available. Standard field references cannot be resolved."

    lines = ["## Semantic Field Mappings", ""]
    lines.append("The following business concepts have been mapped to concrete columns:")
    lines.append("")

    for concept in sorted(field_mappings.available_concepts):
        candidates = field_mappings.get_all_columns(concept)
        if len(candidates) == 1:
            c = candidates[0]
            lines.append(
                f"- **{concept}** → `{c.table_name}.{c.column_name}` (confidence: {c.confidence:.2f})"
            )
        else:
            lines.append(f"- **{concept}** (multiple candidates):")
            for c in candidates[:3]:  # Show top 3
                lines.append(
                    f"  - `{c.table_name}.{c.column_name}` (confidence: {c.confidence:.2f})"
                )
            if len(candidates) > 3:
                lines.append(f"  - ... and {len(candidates) - 3} more")

    lines.append("")
    lines.append(
        f"Total mappings: {field_mappings.total_mappings} columns across {len(field_mappings.available_concepts)} business concepts"
    )

    return "\n".join(lines)


__all__ = [
    "ColumnCandidate",
    "FieldMappings",
    "load_semantic_mappings",
    "format_mappings_for_prompt",
]
