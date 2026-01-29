"""Entropy repository for loading entropy data with typed table enforcement.

Layer 1 of the entropy framework - provides data access with validation.
All entropy operations should go through this repository to ensure
typed table filtering is consistently applied.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.entropy.db_models import EntropyObjectRecord
from dataraum.entropy.models import (
    EntropyObject,
    HumanContext,
    LLMContext,
    ResolutionOption,
)
from dataraum.storage import Column, Table

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)


class EntropyRepository:
    """Repository for entropy data access with typed table enforcement.

    All entropy loading should go through this repository to ensure:
    - Typed table filtering is consistently applied
    - EntropyObjectRecords are properly converted to EntropyObjects
    - Column/table metadata is correctly joined
    """

    def __init__(self, session: Session) -> None:
        """Initialize repository.

        Args:
            session: SQLAlchemy session for database access
        """
        self.session = session

    def get_typed_table_ids(self, table_ids: list[str]) -> list[str]:
        """Filter table IDs to only include typed tables.

        Args:
            table_ids: List of table IDs to filter

        Returns:
            List of table IDs where layer == "typed"
        """
        if not table_ids:
            return []

        stmt = select(Table.table_id).where(
            Table.table_id.in_(table_ids),
            Table.layer == "typed",
        )
        result = self.session.execute(stmt).scalars().all()
        return list(result)

    def get_typed_table_ids_for_source(self, source_id: str) -> list[str]:
        """Get all typed table IDs for a source.

        Args:
            source_id: Source ID to get tables for

        Returns:
            List of typed table IDs for this source
        """
        stmt = select(Table.table_id).where(
            Table.source_id == source_id,
            Table.layer == "typed",
        )
        result = self.session.execute(stmt).scalars().all()
        return list(result)

    def load_for_tables(
        self,
        table_ids: list[str],
        *,
        enforce_typed: bool = True,
    ) -> list[EntropyObject]:
        """Load entropy objects for the given tables.

        Args:
            table_ids: List of table IDs to load entropy for
            enforce_typed: If True, validates all tables have layer="typed"
                and filters to only typed tables. Default True.

        Returns:
            List of EntropyObject instances with full data

        Raises:
            ValueError: If enforce_typed=True and no typed tables found
        """
        if not table_ids:
            return []

        # Filter to typed tables if enforcing
        if enforce_typed:
            typed_ids = self.get_typed_table_ids(table_ids)
            if not typed_ids:
                logger.warning(
                    f"No typed tables found among {len(table_ids)} table IDs. "
                    "Run the typing phase first."
                )
                return []
            table_ids = typed_ids

        # Load entropy records
        stmt = select(EntropyObjectRecord).where(EntropyObjectRecord.table_id.in_(table_ids))
        records = self.session.execute(stmt).scalars().all()

        if not records:
            logger.debug(f"No entropy objects found for {len(table_ids)} tables")
            return []

        # Convert records to EntropyObjects
        return [self._record_to_object(r) for r in records]

    def load_for_columns(
        self,
        column_ids: list[str],
        *,
        enforce_typed: bool = True,
    ) -> list[EntropyObject]:
        """Load entropy objects for specific columns.

        Args:
            column_ids: List of column IDs to load entropy for
            enforce_typed: If True, filters to columns in typed tables only

        Returns:
            List of EntropyObject instances
        """
        if not column_ids:
            return []

        if enforce_typed:
            # Get columns that are in typed tables
            typed_column_stmt = (
                select(Column.column_id)
                .join(Table, Column.table_id == Table.table_id)
                .where(
                    Column.column_id.in_(column_ids),
                    Table.layer == "typed",
                )
            )
            typed_column_ids = list(self.session.execute(typed_column_stmt).scalars().all())
            if not typed_column_ids:
                return []
            column_ids = typed_column_ids

        stmt = select(EntropyObjectRecord).where(EntropyObjectRecord.column_id.in_(column_ids))
        records = self.session.execute(stmt).scalars().all()

        return [self._record_to_object(r) for r in records]

    def load_by_dimension(
        self,
        table_ids: list[str],
        layer: str | None = None,
        dimension: str | None = None,
        *,
        enforce_typed: bool = True,
    ) -> list[EntropyObject]:
        """Load entropy objects filtered by dimension.

        Args:
            table_ids: List of table IDs to load from
            layer: Optional layer filter (structural, semantic, value, computational)
            dimension: Optional dimension filter
            enforce_typed: If True, filters to typed tables only

        Returns:
            List of EntropyObject instances matching filters
        """
        if not table_ids:
            return []

        if enforce_typed:
            table_ids = self.get_typed_table_ids(table_ids)
            if not table_ids:
                return []

        stmt = select(EntropyObjectRecord).where(EntropyObjectRecord.table_id.in_(table_ids))

        if layer is not None:
            stmt = stmt.where(EntropyObjectRecord.layer == layer)
        if dimension is not None:
            stmt = stmt.where(EntropyObjectRecord.dimension == dimension)

        records = self.session.execute(stmt).scalars().all()
        return [self._record_to_object(r) for r in records]

    def get_table_column_mapping(
        self,
        table_ids: list[str],
    ) -> tuple[dict[str, Table], dict[str, Column]]:
        """Load table and column metadata for context building.

        Args:
            table_ids: List of table IDs

        Returns:
            Tuple of (table_map, column_map) where keys are IDs
        """
        tables = (
            self.session.execute(select(Table).where(Table.table_id.in_(table_ids))).scalars().all()
        )
        table_map = {t.table_id: t for t in tables}

        columns = (
            self.session.execute(select(Column).where(Column.table_id.in_(table_ids)))
            .scalars()
            .all()
        )
        column_map = {c.column_id: c for c in columns}

        return table_map, column_map

    def _record_to_object(self, record: EntropyObjectRecord) -> EntropyObject:
        """Convert a database record to an EntropyObject.

        Args:
            record: EntropyObjectRecord from database

        Returns:
            EntropyObject with all fields populated
        """
        # Parse resolution options
        resolution_options: list[ResolutionOption] = []
        if record.resolution_options:
            for opt_dict in record.resolution_options:
                resolution_options.append(
                    ResolutionOption(
                        action=opt_dict.get("action", ""),
                        parameters=opt_dict.get("parameters", {}),
                        expected_entropy_reduction=opt_dict.get("expected_entropy_reduction", 0.0),
                        effort=opt_dict.get("effort", "medium"),
                        description=opt_dict.get("description", ""),
                        cascade_dimensions=opt_dict.get("cascade_dimensions", []),
                    )
                )

        # Parse LLM context
        llm_ctx = LLMContext()
        if record.llm_context:
            llm_ctx = LLMContext(
                description=record.llm_context.get("description", ""),
                query_impact=record.llm_context.get("query_impact", ""),
                best_guess=record.llm_context.get("best_guess"),
                best_guess_confidence=record.llm_context.get("best_guess_confidence", 0.0),
                assumption_if_unresolved=record.llm_context.get("assumption_if_unresolved"),
                filter_recommendation=record.llm_context.get("filter_recommendation"),
                aggregation_recommendation=record.llm_context.get("aggregation_recommendation"),
                join_recommendation=record.llm_context.get("join_recommendation"),
                warning=record.llm_context.get("warning"),
                caveat_template=record.llm_context.get("caveat_template"),
            )

        # Parse human context
        human_ctx = HumanContext()
        if record.human_context:
            human_ctx = HumanContext(
                severity=record.human_context.get("severity", "medium"),
                category=record.human_context.get("category", ""),
                message=record.human_context.get("message", ""),
                recommendation=record.human_context.get("recommendation", ""),
                icon=record.human_context.get("icon"),
                color=record.human_context.get("color"),
            )

        # Parse evidence
        evidence: list[dict[str, Any]] = []
        if record.evidence:
            if isinstance(record.evidence, list):
                evidence = record.evidence
            elif isinstance(record.evidence, dict):
                evidence = [record.evidence]

        return EntropyObject(
            object_id=record.object_id,
            layer=record.layer,
            dimension=record.dimension,
            sub_dimension=record.sub_dimension,
            target=record.target,
            score=record.score,
            confidence=record.confidence,
            evidence=evidence,
            resolution_options=resolution_options,
            llm_context=llm_ctx,
            human_context=human_ctx,
            computed_at=record.computed_at,
            source_analysis_ids=record.source_analysis_ids or [],
            detector_id=record.detector_id,
        )


def load_entropy_objects(
    session: Session,
    table_ids: list[str],
    *,
    enforce_typed: bool = True,
) -> list[EntropyObject]:
    """Convenience function to load entropy objects for tables.

    Args:
        session: SQLAlchemy session
        table_ids: List of table IDs
        enforce_typed: If True, filters to typed tables only

    Returns:
        List of EntropyObject instances
    """
    repo = EntropyRepository(session)
    return repo.load_for_tables(table_ids, enforce_typed=enforce_typed)
