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
from dataraum.entropy.models import EntropyObject
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
        current_run_id: str | None = None,
        resolve_runs: bool = False,
    ) -> list[EntropyObject]:
        """Load entropy objects for the given tables, run-resolved when asked.

        With ``current_run_id`` set or ``resolve_runs=True``, rows are RESOLVED
        per ``(target, detector_id)`` instead of loaded blindly: the in-flight
        run's rows win (they are not promoted yet during their own detect),
        then rows under the promoted catalog head, then rows under the promoted
        table detect heads (plus legacy unstamped rows). At query time there is
        no in-flight run — ``resolve_runs=True`` resolves to the promoted
        catalog head first. With neither, every row loads — the pre-DAT-491
        behavior, when no detector lived on both the add_source and session
        detect paths and a blind load was safe. A session-detect re-adjudication
        (e.g. temporal_behavior's third witness) therefore supersedes the
        add_source rows for the same target instead of losing to a max-score
        dedup downstream.

        Args:
            table_ids: List of table IDs to load entropy for
            enforce_typed: If True, validates all tables have layer="typed"
                and filters to only typed tables. Default True.
            current_run_id: The in-flight detect run whose rows take precedence.
            resolve_runs: Resolve to the promoted catalog head (query-time path).

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
        records = list(self.session.execute(stmt).scalars().all())

        if not records:
            logger.debug(f"No entropy objects found for {len(table_ids)} tables")
            return []

        if current_run_id is not None or resolve_runs:
            records = self._resolve_runs(records, table_ids, current_run_id)

        # Convert records to EntropyObjects
        return [self._record_to_object(r) for r in records]

    def _resolve_runs(
        self,
        records: list[EntropyObjectRecord],
        table_ids: list[str],
        current_run_id: str | None,
    ) -> list[EntropyObjectRecord]:
        """Per ``(target, detector_id)``: current run > catalog head > table heads/legacy."""
        from dataraum.storage.snapshot_head import (
            GENERATION_STAGE,
            catalog_head_target,
            head_run_id,
        )

        catalog_head = head_run_id(self.session, catalog_head_target(), "catalog")
        table_heads = {
            rid
            for tid in table_ids
            if (rid := head_run_id(self.session, f"table:{tid}", GENERATION_STAGE)) is not None
        }

        def rank(record: EntropyObjectRecord) -> int | None:
            # None-guarded: at query time the in-flight slot is vacant, and a
            # legacy unstamped row (run_id None) must not match it and outrank
            # the catalog head.
            if current_run_id is not None and record.run_id == current_run_id:
                return 0
            if catalog_head is not None and record.run_id == catalog_head:
                return 1
            if record.run_id in table_heads or record.run_id is None:
                return 2
            return None  # superseded run — not a head, not in flight

        by_key: dict[tuple[str, str], tuple[int, list[EntropyObjectRecord]]] = {}
        for record in records:
            r = rank(record)
            if r is None:
                continue
            key = (record.target, record.detector_id)
            best = by_key.get(key)
            if best is None or r < best[0]:
                by_key[key] = (r, [record])
            elif r == best[0]:
                best[1].append(record)
        return [record for _, rows in by_key.values() for record in rows]

    def _record_to_object(self, record: EntropyObjectRecord) -> EntropyObject:
        """Convert a database record to an EntropyObject.

        Args:
            record: EntropyObjectRecord from database

        Returns:
            EntropyObject with all fields populated
        """
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
            computed_at=record.computed_at,
            source_analysis_ids=record.source_analysis_ids or [],
            detector_id=record.detector_id,
        )
