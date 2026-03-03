"""Base classes for data loaders."""

from __future__ import annotations

import re
import unicodedata
from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING

import duckdb
from sqlalchemy.orm import Session

from dataraum.core.models import Result, SourceConfig

if TYPE_CHECKING:
    from dataraum.sources.csv.models import StagingResult


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


class TypeSystemStrength(str, Enum):
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

    @property
    @abstractmethod
    def type_system_strength(self) -> TypeSystemStrength:
        """Classify the source's type system strength."""
        pass

    @abstractmethod
    def load(
        self,
        source_config: SourceConfig,
        duckdb_conn: duckdb.DuckDBPyConnection,
        session: Session,
    ) -> Result[StagingResult]:
        """Load data from source into DuckDB.

        Args:
            source_config: Source configuration
            duckdb_conn: DuckDB connection
            session: SQLAlchemy session for metadata

        Returns:
            Result containing StagingResult or error
        """
        pass

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
        """Sanitize table name for use in SQL.

        Args:
            name: Original table name

        Returns:
            Sanitized table name
        """
        # Remove extension
        if "." in name:
            name = name.rsplit(".", 1)[0]

        # Replace invalid characters
        name = name.replace("-", "_").replace(" ", "_").replace(".", "_")

        # Ensure it starts with a letter or underscore
        if name and not (name[0].isalpha() or name[0] == "_"):
            name = f"t_{name}"

        return name.lower()
