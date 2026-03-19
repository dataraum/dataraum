"""Fix interpreters — generic handlers for config and metadata fixes.

Each interpreter knows how to apply one target type. They are stateless —
all context comes from the FixDocument and the runtime connections.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dataraum.core.logging import get_logger
from dataraum.pipeline.fixes import apply_config_yaml
from dataraum.pipeline.fixes.models import DataFix, FixDocument

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = get_logger(__name__)


class ConfigInterpreter:
    """Apply config fixes by writing to YAML files.

    Payload shape:
        config_path: str — relative path within config root
        key_path: list[str] — nested key path
        operation: str — "set", "append", "merge", "remove"
        value: Any — the value to write
        reason: str (optional) — human-readable reason
    """

    def apply(self, doc: FixDocument, config_root: Path) -> None:
        """Apply a config fix to a YAML file.

        Args:
            doc: Fix document with config payload.
            config_root: Absolute path to config directory.

        Raises:
            KeyError: If required payload fields are missing.
            ValueError: If the operation is invalid.
        """
        config_path = doc.payload["config_path"]
        key_path = doc.payload["key_path"]
        apply_config_yaml(
            config_root,
            config_path=config_path,
            operation=doc.payload["operation"],
            key_path=key_path,
            value=doc.payload.get("value"),
        )
        logger.info(
            "config_fix_applied",
            action=doc.action,
            config_path=config_path,
            key_path=key_path,
        )


class MetadataInterpreter:
    """Apply metadata fixes by updating ORM model fields.

    Payload shape:
        model: str — ORM model name (e.g. "SemanticAnnotation")
        field_updates: dict[str, Any] — fields to set on the model instance

    The interpreter resolves the target row by (table_name, column_name)
    and applies field_updates via setattr.
    """

    def apply(self, doc: FixDocument, session: Session) -> None:
        """Apply a metadata fix to an ORM model.

        For markers (no ``model`` in payload), the DataFix record itself
        is the fix — nothing to patch on an ORM model.  This covers
        acceptance markers and DataFix-only fixes (e.g. document_join_path).

        For model-based fixes, resolves the target row, applies field
        updates, and runs model-specific side effects (e.g. setting
        ``is_confirmed`` on Relationship records).

        Args:
            doc: Fix document with metadata payload.
            session: SQLAlchemy session for the metadata DB.

        Raises:
            KeyError: If required payload fields are missing.
            ValueError: If the model is unknown or the target row is not found.
        """
        if "model" not in doc.payload:
            # Marker — the persisted DataFix record is the fix.
            # Gate queries DataFix for acceptance; detectors query
            # DataFix for join preferences and documented patterns.
            logger.info(
                "metadata_marker_recorded",
                action=doc.action,
                table=doc.table_name,
                column=doc.column_name,
            )
            return

        model_name = doc.payload["model"]
        field_updates = doc.payload["field_updates"]

        resolver = self._get_resolver(model_name)
        instance = resolver(session, doc.table_name, doc.column_name)

        if instance is None:
            raise ValueError(f"No {model_name} found for {doc.table_name}.{doc.column_name}")

        applied_fields: list[str] = []
        for field_name, value in field_updates.items():
            if not hasattr(instance, field_name):
                # Skip resolution hints (e.g. from_table, to_table) that
                # are schema fields but not model attributes.
                continue
            setattr(instance, field_name, value)
            applied_fields.append(field_name)

        if not applied_fields:
            logger.warning(
                "metadata_fix_no_fields_applied",
                action=doc.action,
                model=model_name,
                table=doc.table_name,
                column=doc.column_name,
                skipped_keys=list(field_updates),
            )

        # Model-specific side effects
        _apply_model_defaults(model_name, instance)

        session.flush()
        logger.info(
            "metadata_fix_applied",
            action=doc.action,
            model=model_name,
            table=doc.table_name,
            column=doc.column_name,
            fields=applied_fields,
        )

    def _get_resolver(self, model_name: str) -> Any:
        """Get the resolver function for a model name."""
        resolver = _MODEL_RESOLVERS.get(model_name)
        if resolver is None:
            raise ValueError(f"Unknown metadata model: {model_name!r}")
        return resolver


# ---------------------------------------------------------------------------
# Model resolvers
# ---------------------------------------------------------------------------


def _resolve_semantic_annotation(session: Session, table_name: str, column_name: str | None) -> Any:
    """Find a SemanticAnnotation by (table_name, column_name)."""
    from sqlalchemy import select

    from dataraum.analysis.semantic.db_models import SemanticAnnotation
    from dataraum.storage import Column, Table

    if column_name is None:
        raise ValueError("SemanticAnnotation requires a column_name")

    col = session.execute(
        select(Column)
        .join(Table, Column.table_id == Table.table_id)
        .where(
            Table.table_name == f"typed_{table_name}",
            Column.column_name == column_name,
        )
    ).scalar_one_or_none()

    if col is None:
        col = session.execute(
            select(Column)
            .join(Table, Column.table_id == Table.table_id)
            .where(
                Table.table_name == table_name,
                Table.layer == "typed",
                Column.column_name == column_name,
            )
        ).scalar_one_or_none()

    if col is None:
        return None

    return session.execute(
        select(SemanticAnnotation).where(SemanticAnnotation.column_id == col.column_id)
    ).scalar_one_or_none()


def _resolve_relationship(session: Session, table_name: str, column_name: str | None) -> Any:
    """Find a Relationship by (table_name, column_name).

    Checks both from_column_id and to_column_id so that relationships
    are found regardless of which side the column is on.
    """
    from sqlalchemy import or_, select

    from dataraum.analysis.relationships.db_models import Relationship
    from dataraum.storage import Column, Table

    if column_name is None:
        raise ValueError("Relationship resolution requires a column_name")

    col = session.execute(
        select(Column)
        .join(Table, Column.table_id == Table.table_id)
        .where(
            Table.table_name == f"typed_{table_name}",
            Column.column_name == column_name,
        )
    ).scalar_one_or_none()

    if col is None:
        return None

    return session.execute(
        select(Relationship).where(
            or_(
                Relationship.from_column_id == col.column_id,
                Relationship.to_column_id == col.column_id,
            )
        )
    ).scalar_one_or_none()


# Module-level resolver registry — populated at import time (no lazy init)
# to avoid write-write races under free-threading (Python 3.14t).
_MODEL_RESOLVERS: dict[str, Any] = {
    "SemanticAnnotation": _resolve_semantic_annotation,
    "Relationship": _resolve_relationship,
}


def _apply_model_defaults(model_name: str, instance: Any) -> None:
    """Apply standard side-effect fields after user-provided updates.

    Always unconditional so that replayed fixes update the audit trail.
    """
    if model_name == "SemanticAnnotation":
        instance.annotation_source = "fix_system"
    elif model_name == "Relationship":
        instance.is_confirmed = True
        instance.confirmed_at = datetime.now(UTC)
        instance.confirmed_by = "fix_system"


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


def apply_fix_document(
    doc: FixDocument,
    *,
    config_root: Path | None = None,
    session: Session | None = None,
) -> None:
    """Apply a fix document using the appropriate interpreter.

    Routes to ConfigInterpreter or MetadataInterpreter based on doc.target.

    Args:
        doc: The fix document to apply.
        config_root: Required for config fixes.
        session: Required for metadata fixes.

    Raises:
        ValueError: If the required connection for the target is not provided.
    """
    if doc.target == "config":
        if config_root is None:
            raise ValueError("config_root required for config fixes")
        ConfigInterpreter().apply(doc, config_root)

    elif doc.target == "metadata":
        if session is None:
            raise ValueError("session required for metadata fixes")
        MetadataInterpreter().apply(doc, session)

    else:
        raise ValueError(f"Unknown fix target: {doc.target!r}")


def apply_and_persist(
    source_id: str,
    documents: list[FixDocument],
    *,
    session: Session,
    config_root: Path | None = None,
) -> list[DataFix]:
    """Apply a list of fix documents and persist them to the DB.

    Applies documents in ordinal order. On failure, marks the failed
    document and stops — subsequent documents are not applied.

    Args:
        source_id: The source these fixes apply to.
        documents: Ordered list of fix documents to apply.
        session: SQLAlchemy session (used for metadata fixes and persistence).
        config_root: Required if any document targets config.

    Returns:
        List of persisted DataFix records.
    """
    sorted_docs = sorted(documents, key=lambda d: d.ordinal)
    records: list[DataFix] = []

    for doc in sorted_docs:
        record = DataFix.from_document(source_id, doc)
        session.add(record)

        try:
            apply_fix_document(
                doc,
                config_root=config_root,
                session=session,
            )
            record.status = "applied"
            record.applied_at = datetime.now(UTC)
        except Exception as e:
            record.status = "failed"
            record.error_message = str(e)
            session.flush()
            records.append(record)
            logger.error(
                "fix_document_failed",
                fix_id=doc.fix_id,
                action=doc.action,
                error=str(e),
            )
            break  # Stop on first failure

        session.flush()
        records.append(record)

    return records
