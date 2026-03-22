"""Business pattern filter for entropy gate scores.

Discounts scores for findings matched by ``confirm_expected_pattern``
DataFix records.  Score discount: ``score *= (1 - filter_confidence)``
when confidence >= 0.8.

Pattern classification is done externally (MCP ``apply_fix`` with
``confirm_expected_pattern`` action).  This module only reads applied
DataFix records and applies the discount at gate time.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from dataraum.core.logging import get_logger

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from dataraum.entropy.db_models import EntropyObjectRecord

logger = get_logger(__name__)

CONFIDENCE_THRESHOLD = 0.8


def apply_pattern_filter(
    session: Session,
    source_id: str,
    records: list[EntropyObjectRecord],
) -> list[EntropyObjectRecord]:
    """Discount scores for findings confirmed as expected business patterns.

    Reads ``confirm_expected_pattern`` DataFix records and sets
    ``filter_confidence = 1.0`` on matching entropy records.  The score
    discount (``score *= (1 - confidence)``) is applied in-place.

    Args:
        session: SQLAlchemy session (caller manages commit).
        source_id: Source ID for DataFix lookup.
        records: Loaded ``EntropyObjectRecord`` rows.

    Returns:
        The same list with scores discounted where appropriate.
    """
    # Filter to candidates: score > 0 and not already classified
    candidates = [r for r in records if r.score > 0 and r.filter_confidence is None]
    if not candidates:
        return records

    # Apply DataFix overrides
    _apply_datafix_overrides(session, source_id, candidates)

    # Discount scores for high-confidence classifications
    for record in records:
        if (
            record.filter_confidence is not None
            and record.filter_confidence >= CONFIDENCE_THRESHOLD
        ):
            record.score = round(record.score * (1 - record.filter_confidence), 4)

    return records


def _apply_datafix_overrides(
    session: Session,
    source_id: str,
    candidates: list[EntropyObjectRecord],
) -> None:
    """Apply confirm_expected_pattern DataFix records to matching entropy records.

    Sets ``filter_confidence = 1.0`` (full discount) on matched records.
    """
    from dataraum.pipeline.fixes.models import DataFix

    fixes = list(
        session.execute(
            select(DataFix).where(
                DataFix.source_id == source_id,
                DataFix.action == "confirm_expected_pattern",
                DataFix.status == "applied",
            )
        )
        .scalars()
        .all()
    )

    if not fixes:
        return

    # Build lookup: (table_name, column_name | None) -> fix
    fix_lookup: dict[tuple[str, str | None], DataFix] = {}
    for fix in fixes:
        fix_lookup[(fix.table_name, fix.column_name)] = fix

    for record in candidates:
        table_name, column_name = _parse_target(record.target)
        matched = fix_lookup.get((table_name, column_name)) or fix_lookup.get(
            (table_name, None),
        )
        if matched is not None:
            record.filter_confidence = 1.0
            params = matched.payload.get("parameters", {}) if matched.payload else {}
            record.expected_business_pattern = params.get("pattern_type", matched.action)
            record.business_rule = params.get("description", matched.description or matched.action)


def _parse_target(target: str) -> tuple[str, str | None]:
    """Parse 'column:table.col' or 'table:name' into (table_name, column_name | None)."""
    if ":" not in target:
        return target, None
    scope, ref = target.split(":", 1)
    if scope == "column" and "." in ref:
        parts = ref.split(".", 1)
        return parts[0], parts[1]
    return ref, None
