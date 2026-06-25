"""Base classes for data loaders."""

from __future__ import annotations

import re
import unicodedata
from abc import ABC, abstractmethod
from enum import StrEnum

from dataraum.core.models import Result, SourceConfig


def normalize_column_name(header: str, position: int = 0) -> str:
    """Normalize a CSV column header to a clean SQL identifier.

    Transforms: lowercase, whitespace→underscore, strip diacritics,
    remove punctuation like -,&, collapse multiple underscores,
    strip leading/trailing underscores.

    Args:
        header: Original column header string.
        position: Column position (used as fallback if name empties out).

    Returns:
        Normalized column name safe for use as a SQL identifier.
    """
    name = header.strip().lower()
    # Strip diacritics (NFD decomposition, drop combining marks)
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    # Whitespace → underscore
    name = re.sub(r"\s+", "_", name)
    # Remove problematic punctuation
    name = re.sub(r"[-,&/]", "", name)
    # Keep only alphanumeric and underscores
    name = re.sub(r"[^a-z0-9_]", "", name)
    # Collapse multiple underscores
    name = re.sub(r"_+", "_", name)
    # Strip leading/trailing underscores
    name = name.strip("_")
    # Guard: prefix if starts with digit
    if name and name[0].isdigit():
        name = f"c_{name}"
    # Guard: empty result
    if not name:
        name = f"column_{position}"
    return name


def _sanitize_table_stem(name: str) -> str:
    """Sanitize a logical table stem before ``workspace_table_name`` (DAT-639).

    The pre-pass a file's table name goes through: strip any extension, fold
    ``- . space`` to ``_``, guard a non-letter lead with ``t_``, lowercase. The
    result is then run through ``workspace_table_name`` for the final narrow,
    workspace-unique identifier. Kept distinct from ``sanitize_identifier`` only
    because the ``t_`` lead-guard predates it and its exact output is asserted by
    callers' tests; composing the two is the single canonical derivation.
    """
    if "." in name:
        name = name.rsplit(".", 1)[0]
    name = name.replace("-", "_").replace(" ", "_").replace(".", "_")
    if name and not (name[0].isalpha() or name[0] == "_"):
        name = f"t_{name}"
    return name.lower()


def raw_table_name_for_uri(source_uri: str) -> str:
    """The NARROW, workspace-unique raw table name a file URI loads into (DAT-639).

    The SINGLE source of truth for a file source's physical raw-table name: the
    URI stem, sanitized, run through ``workspace_table_name`` (no source prefix —
    the per-workspace DuckLake catalog is the namespace). Both the loaders (which
    CREATE the table) and the import phase's pre-flight collision guard derive the
    name through this one function, so the guard can never disagree with the name
    the loader will actually write.
    """
    from dataraum.core.duckdb_naming import workspace_table_name
    from dataraum.core.uri import uri_stem

    return workspace_table_name(_sanitize_table_stem(uri_stem(source_uri)))


class TypeSystemStrength(StrEnum):
    """Classification of source type system strength."""

    UNTYPED = "untyped"  # CSV, JSON - no inherent types
    WEAK = "weak"  # SQLite, Excel - advisory types
    STRONG = "strong"  # PostgreSQL, Parquet - enforced types


class ColumnInfo:
    """Column information from source."""

    def __init__(
        self,
        name: str,
        position: int,
        source_type: str | None = None,
        nullable: bool = True,
        sample_values: list[str] | None = None,
        original_name: str | None = None,
    ):
        self.name = name
        self.position = position
        self.source_type = source_type
        self.nullable = nullable
        self.sample_values = sample_values or []
        self.original_name = original_name


class LoaderBase(ABC):
    """Base class for all data loaders.

    Each loader handles a specific source type and knows its type system strength.
    """

    @abstractmethod
    def get_schema(
        self,
        source_config: SourceConfig,
    ) -> Result[list[ColumnInfo]]:
        """Get source schema information.

        Args:
            source_config: Source configuration

        Returns:
            Result containing list of ColumnInfo or error
        """
        pass

    def _sanitize_table_name(self, name: str) -> str:
        """Sanitize a logical table name for SQL — see :func:`_sanitize_table_stem`."""
        return _sanitize_table_stem(name)
