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

from dataraum.analysis.semantic.db_models import SemanticAnnotation
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

    def get_column(self, business_concept: str) -> ColumnCandidate | None:
        """Get best matching column for a business concept.

        Args:
            business_concept: Standard field name (e.g., 'revenue')

        Returns:
            Best matching column or None if no match
        """
        candidates = self.mappings.get(business_concept, [])
        if not candidates:
            return None
        # Return highest confidence match
        return max(candidates, key=lambda c: c.confidence)

    def get_column_name(self, business_concept: str) -> str | None:
        """Get column name for a business concept.

        Args:
            business_concept: Standard field name (e.g., 'revenue')

        Returns:
            Column name or None if no match
        """
        candidate = self.get_column(business_concept)
        return candidate.column_name if candidate else None

    def get_all_columns(self, business_concept: str) -> list[ColumnCandidate]:
        """Get all matching columns for a business concept.

        Args:
            business_concept: Standard field name (e.g., 'revenue')

        Returns:
            List of matching columns, sorted by confidence
        """
        candidates = self.mappings.get(business_concept, [])
        return sorted(candidates, key=lambda c: c.confidence, reverse=True)

    def has_mapping(self, business_concept: str) -> bool:
        """Check if a business concept has any mappings.

        Args:
            business_concept: Standard field name

        Returns:
            True if at least one column maps to this concept
        """
        return bool(self.mappings.get(business_concept))

    @property
    def available_concepts(self) -> list[str]:
        """List all business concepts that have mappings."""
        return [concept for concept, cols in self.mappings.items() if cols]

    @property
    def available_terms(self) -> list[str]:
        """Alias for available_concepts (backward compatibility)."""
        return self.available_concepts

    @property
    def total_mappings(self) -> int:
        """Total number of column mappings across all terms."""
        return sum(len(cols) for cols in self.mappings.values())


def load_semantic_mappings(
    session: Session,
    table_ids: list[str],
) -> FieldMappings:
    """Load business_concept → column mappings from semantic annotations.

    Queries the semantic_annotations table for all columns in the specified
    tables that have a business_concept set, and groups them by concept.

    Args:
        session: Database session
        table_ids: Table IDs to load mappings for

    Returns:
        FieldMappings with business_concept → column mappings
    """
    if not table_ids:
        return FieldMappings(table_ids=[])

    # Query semantic annotations with business_concept set
    stmt = (
        select(SemanticAnnotation, Column, Table)
        .join(Column, SemanticAnnotation.column_id == Column.column_id)
        .join(Table, Column.table_id == Table.table_id)
        .where(
            Table.table_id.in_(table_ids),
            SemanticAnnotation.business_concept.isnot(None),
        )
    )

    result = session.execute(stmt)
    rows = result.all()

    mappings: dict[str, list[ColumnCandidate]] = {}

    for annotation, column, table in rows:
        concept = annotation.business_concept
        if not concept:
            continue

        if concept not in mappings:
            mappings[concept] = []

        candidate = ColumnCandidate(
            column_id=column.column_id,
            column_name=column.column_name,
            table_name=table.table_name,
            confidence=annotation.confidence or 0.5,
            semantic_role=annotation.semantic_role,
            entity_type=annotation.entity_type,
        )
        mappings[concept].append(candidate)

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


def can_execute_metric(
    field_mappings: FieldMappings,
    required_fields: list[str],
) -> tuple[bool, list[str]]:
    """Check if a metric can be executed with available field mappings.

    Args:
        field_mappings: Available field mappings
        required_fields: List of standard_field names required by the metric

    Returns:
        Tuple of (can_execute, missing_fields)
    """
    missing = [field for field in required_fields if not field_mappings.has_mapping(field)]
    return len(missing) == 0, missing


def resolve_metric_fields(
    field_mappings: FieldMappings,
    field_references: list[str],
) -> dict[str, str]:
    """Resolve standard field references to concrete column names.

    Args:
        field_mappings: Available field mappings
        field_references: List of standard_field names to resolve

    Returns:
        Dict mapping standard_field → column_name (only for resolvable fields)
    """
    resolved = {}
    for field_ref in field_references:
        column_name = field_mappings.get_column_name(field_ref)
        if column_name:
            resolved[field_ref] = column_name
    return resolved


__all__ = [
    "ColumnCandidate",
    "FieldMappings",
    "load_semantic_mappings",
    "format_mappings_for_prompt",
    "can_execute_metric",
    "resolve_metric_fields",
]
