"""Base classes for data loaders."""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING

import duckdb
from sqlalchemy.orm import Session

from dataraum_context.core.models import Result, SourceConfig

if TYPE_CHECKING:
    from dataraum_context.sources.csv.models import StagingResult


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
    ):
        self.name = name
        self.position = position
        self.source_type = source_type
        self.nullable = nullable
        self.sample_values = sample_values or []


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

    def _get_layer_prefix(self) -> str:
        """Get the table layer prefix based on type system strength.

        Returns:
            'raw' for untyped/weak, 'typed' for strong
        """
        if self.type_system_strength == TypeSystemStrength.STRONG:
            return "typed"
        return "raw"
