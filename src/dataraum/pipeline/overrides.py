"""Postprocess overrides — apply config patches before gate measurement.

Extracted from semantic_phase.py so overrides can be applied at gate time
rather than requiring a full semantic re-run. Called by the scheduler
before every gate measurement (idempotent).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from dataraum.core.logging import get_logger

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = get_logger(__name__)


def apply_postprocess_overrides(
    session: Session,
    source_id: str,
    config_root: object,
) -> None:
    """Apply all pending config overrides before gate measurement.

    Reads the semantic phase config and applies any overrides to DB records.
    Safe to call repeatedly — patches are idempotent (set fields to target
    values, skip if already matching).

    Args:
        session: SQLAlchemy session.
        source_id: Source being processed.
        config_root: Path to config root directory.
    """
    from pathlib import Path

    from dataraum.core.config import load_phase_config
    from dataraum.entropy.config import clear_entropy_config_cache
    from dataraum.storage import Table

    config_root_path = Path(str(config_root))

    # Load semantic phase config for overrides
    semantic_config = load_phase_config("semantic", config_root=config_root_path)

    # Get typed table_ids for this source
    tables = (
        session.execute(
            select(Table).where(
                Table.source_id == source_id,
                Table.layer == "typed",
            )
        )
        .scalars()
        .all()
    )
    table_ids = [t.table_id for t in tables]

    if not table_ids:
        return

    # Apply semantic annotation overrides
    apply_semantic_overrides(session, semantic_config, table_ids)

    # Apply relationship confirmations
    apply_relationship_confirmations(session, semantic_config, table_ids)

    # Clear entropy config cache so detectors read fresh thresholds
    clear_entropy_config_cache()


def apply_semantic_overrides(
    session: Session,
    config: dict,  # type: ignore[type-arg]
    table_ids: list[str],
) -> None:
    """Apply semantic overrides from config.

    Reads all override sections under ``overrides`` in the semantic
    phase config (``semantic_roles``, ``units``, ``business_meaning``).
    Each section maps ``"table.column"`` to a dict of field values
    that are patched onto the existing SemanticAnnotation.
    """
    from dataraum.analysis.semantic.db_models import SemanticAnnotation
    from dataraum.storage import Column, Table

    overrides = config.get("overrides", {})
    if not isinstance(overrides, dict):
        return

    # Merge all override sections into a single col_ref -> fields dict.
    merged: dict[str, dict[str, object]] = {}
    for section in overrides.values():
        if not isinstance(section, dict):
            continue
        for col_ref, values in section.items():
            if not isinstance(values, dict):
                continue
            merged.setdefault(col_ref, {}).update(values)

    if not merged:
        return

    # Build column lookup: "table.column" -> column_id
    cols = session.execute(
        select(Column, Table.table_name)
        .join(Table, Column.table_id == Table.table_id)
        .where(Column.table_id.in_(table_ids))
    ).all()
    col_lookup: dict[str, str] = {}
    for col, tbl_name in cols:
        col_lookup[f"{tbl_name}.{col.column_name}"] = col.column_id

    for col_ref, field_values in merged.items():
        col_id = col_lookup.get(col_ref)
        if col_id is None:
            logger.debug("semantic_override_skip", column=col_ref, reason="not found")
            continue

        annotation = session.execute(
            select(SemanticAnnotation).where(SemanticAnnotation.column_id == col_id)
        ).scalar_one_or_none()
        if annotation is None:
            logger.debug("semantic_override_skip", column=col_ref, reason="no annotation")
            continue

        changed = False
        for field_name, value in field_values.items():
            if hasattr(annotation, field_name) and getattr(annotation, field_name) != value:
                setattr(annotation, field_name, value)
                changed = True

        if changed:
            annotation.annotation_source = "config_override"
            logger.info("semantic_override_applied", column=col_ref)

    session.flush()


def apply_relationship_confirmations(
    session: Session,
    config: dict,  # type: ignore[type-arg]
    table_ids: list[str],
) -> None:
    """Apply confirmed_relationships overrides from the semantic phase config.

    Sets is_confirmed=True and patches field values onto matching
    Relationship records. Keys are ``"from_table->to_table"``.
    """
    from datetime import UTC, datetime

    from dataraum.analysis.relationships.db_models import Relationship
    from dataraum.storage import Table

    overrides = config.get("overrides", {})
    if not isinstance(overrides, dict):
        return

    confirmed = overrides.get("confirmed_relationships", {})
    if not isinstance(confirmed, dict) or not confirmed:
        return

    # Build table name lookup
    tables = session.execute(select(Table).where(Table.table_id.in_(table_ids))).scalars().all()
    name_to_id = {t.table_name: t.table_id for t in tables}

    for key, field_values in confirmed.items():
        if not isinstance(field_values, dict):
            continue
        if "->" not in key:
            continue
        from_name, to_name = key.split("->", 1)
        from_tid = name_to_id.get(from_name)
        to_tid = name_to_id.get(to_name)
        if not from_tid or not to_tid:
            logger.debug("relationship_confirm_skip", key=key, reason="table not found")
            continue

        rels = (
            session.execute(
                select(Relationship).where(
                    Relationship.from_table_id == from_tid,
                    Relationship.to_table_id == to_tid,
                )
            )
            .scalars()
            .all()
        )
        if not rels:
            logger.debug("relationship_confirm_skip", key=key, reason="no relationship")
            continue

        for rel in rels:
            for field_name, value in field_values.items():
                if hasattr(rel, field_name) and getattr(rel, field_name) != value:
                    setattr(rel, field_name, value)

            if not rel.is_confirmed:
                rel.is_confirmed = True
                rel.confirmed_at = datetime.now(UTC)
                rel.confirmed_by = "config_override"
                logger.info("relationship_confirm_applied", key=key)

    session.flush()
