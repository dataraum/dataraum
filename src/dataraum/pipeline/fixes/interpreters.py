"""Fix interpreters — three generic handlers for config, metadata, and data fixes.

Each interpreter knows how to apply one target type. They are stateless —
all context comes from the FixDocument and the runtime connections.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dataraum.core.logging import get_logger
from dataraum.pipeline.fixes import apply_config_yaml
from dataraum.pipeline.fixes.models import DataFix, FixDocument

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

logger = get_logger(__name__)

# SQL patterns that data fixes must not contain
_DANGEROUS_SQL = re.compile(
    r"\b(DROP\s+TABLE|DELETE\s+FROM\s+(?:raw_|typed_)|TRUNCATE)",
    re.IGNORECASE,
)


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

        Args:
            doc: Fix document with metadata payload.
            session: SQLAlchemy session for the metadata DB.

        Raises:
            KeyError: If required payload fields are missing.
            ValueError: If the model is unknown or the target row is not found.
        """
        model_name = doc.payload["model"]
        field_updates = doc.payload["field_updates"]

        resolver = self._get_resolver(model_name)
        instance = resolver(session, doc.table_name, doc.column_name)

        if instance is None:
            raise ValueError(f"No {model_name} found for {doc.table_name}.{doc.column_name}")

        for field_name, value in field_updates.items():
            if not hasattr(instance, field_name):
                raise ValueError(f"{model_name} has no field '{field_name}'")
            setattr(instance, field_name, value)

        session.flush()
        logger.info(
            "metadata_fix_applied",
            action=doc.action,
            model=model_name,
            table=doc.table_name,
            column=doc.column_name,
            fields=list(field_updates.keys()),
        )

    def _get_resolver(self, model_name: str) -> Any:
        """Get the resolver function for a model name."""
        resolver = _MODEL_RESOLVERS.get(model_name)
        if resolver is None:
            raise ValueError(f"Unknown metadata model: {model_name!r}")
        return resolver


class DataInterpreter:
    """Apply data fixes by executing SQL against DuckDB.

    Payload shape:
        sql: str — DuckDB SQL to execute

    Validates SQL with EXPLAIN before execution. Blocks destructive
    operations (DROP TABLE, DELETE FROM raw_/typed_, TRUNCATE).
    """

    def validate(self, doc: FixDocument, conn: duckdb.DuckDBPyConnection) -> list[str]:
        """Validate a data fix without executing it.

        Returns:
            List of validation error messages (empty if valid).
        """
        errors: list[str] = []
        sql = doc.payload.get("sql", "")

        if not sql.strip():
            errors.append("SQL is empty")
            return errors

        # Check for dangerous operations
        match = _DANGEROUS_SQL.search(sql)
        if match:
            errors.append(f"Dangerous operation not allowed: {match.group()}")
            return errors

        # Try EXPLAIN on SELECT statements only.
        for stmt in _split_statements(sql):
            stmt = stmt.strip()
            if not stmt:
                continue
            upper = stmt.upper().lstrip()
            if upper.startswith(("ALTER", "UPDATE", "INSERT", "CREATE")):
                continue
            try:
                conn.execute(f"EXPLAIN {stmt}")
            except Exception as e:
                errors.append(f"SQL validation failed: {e}")

        return errors

    def apply(self, doc: FixDocument, conn: duckdb.DuckDBPyConnection) -> None:
        """Apply a data fix by executing SQL.

        Args:
            doc: Fix document with SQL payload.
            conn: DuckDB connection.

        Raises:
            ValueError: If SQL is invalid or contains dangerous operations.
        """
        errors = self.validate(doc, conn)
        if errors:
            raise ValueError(f"Data fix validation failed: {'; '.join(errors)}")

        sql = doc.payload["sql"]
        for stmt in _split_statements(sql):
            stmt = stmt.strip()
            if stmt:
                conn.execute(stmt)

        logger.info(
            "data_fix_applied",
            action=doc.action,
            table=doc.table_name,
            column=doc.column_name,
        )


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


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def _split_statements(sql: str) -> list[str]:
    """Split SQL into individual statements on semicolons."""
    return [s.strip() for s in sql.split(";") if s.strip()]


def apply_fix_document(
    doc: FixDocument,
    *,
    config_root: Path | None = None,
    session: Session | None = None,
    duckdb_conn: duckdb.DuckDBPyConnection | None = None,
) -> None:
    """Apply a fix document using the appropriate interpreter.

    Routes to ConfigInterpreter, MetadataInterpreter, or DataInterpreter
    based on doc.target.

    Args:
        doc: The fix document to apply.
        config_root: Required for config fixes.
        session: Required for metadata fixes.
        duckdb_conn: Required for data fixes.

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

    elif doc.target == "data":
        if duckdb_conn is None:
            raise ValueError("duckdb_conn required for data fixes")
        DataInterpreter().apply(doc, duckdb_conn)

    else:
        raise ValueError(f"Unknown fix target: {doc.target!r}")


def apply_and_persist(
    source_id: str,
    documents: list[FixDocument],
    *,
    session: Session,
    config_root: Path | None = None,
    duckdb_conn: duckdb.DuckDBPyConnection | None = None,
) -> list[DataFix]:
    """Apply a list of fix documents and persist them to the DB.

    Applies documents in ordinal order. On failure, marks the failed
    document and stops — subsequent documents are not applied.

    Args:
        source_id: The source these fixes apply to.
        documents: Ordered list of fix documents to apply.
        session: SQLAlchemy session (used for metadata fixes and persistence).
        config_root: Required if any document targets config.
        duckdb_conn: Required if any document targets data.

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
                duckdb_conn=duckdb_conn,
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
