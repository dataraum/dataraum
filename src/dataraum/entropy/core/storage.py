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
    ResolutionOption,
)
from dataraum.storage import Table

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
                        effort=opt_dict.get("effort", "medium"),
                        description=opt_dict.get("description", ""),
                    )
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
            evidence=evidence,
            resolution_options=resolution_options,
            computed_at=record.computed_at,
            source_analysis_ids=record.source_analysis_ids or [],
            detector_id=record.detector_id,
        )
