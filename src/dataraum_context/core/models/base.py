"""Base models and types used across all modules.

This module contains the fundamental types that don't belong to any specific
domain module (staging, profiling, enrichment, etc.).
"""

from __future__ import annotations

from collections.abc import Callable
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class Result[T](BaseModel):
    """Result type for operations that can fail.

    Use this instead of exceptions for expected failures.
    Exceptions are reserved for unexpected/programming errors.
    """

    success: bool
    value: T | None = None
    error: str | None = None
    warnings: list[str] = Field(default_factory=list)

    @classmethod
    def ok(cls, value: T, warnings: list[str] | None = None) -> Result[T]:
        """Create a successful result."""
        return cls(success=True, value=value, warnings=warnings or [])

    @classmethod
    def fail(cls, error: str) -> Result[T]:
        """Create a failed result."""
        return cls(success=False, error=error)

    def unwrap(self) -> T:
        """Get the value or raise if failed."""
        if not self.success:
            raise ValueError(f"Result failed: {self.error}")
        assert self.value is not None
        return self.value

    def map(self, fn: Callable[[T], Any]) -> Result[Any]:
        """Transform the value if successful."""
        if self.success and self.value is not None:
            return Result.ok(fn(self.value), self.warnings)
        return self


# === Enums ===


class DataType(str, Enum):
    """Supported data types."""

    VARCHAR = "VARCHAR"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    DOUBLE = "DOUBLE"
    DECIMAL = "DECIMAL"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMPTZ = "TIMESTAMPTZ"
    TIME = "TIME"
    INTERVAL = "INTERVAL"
    JSON = "JSON"
    BLOB = "BLOB"


class SemanticRole(str, Enum):
    """Semantic role of a column."""

    MEASURE = "measure"  # Numeric value to aggregate
    DIMENSION = "dimension"  # Categorical grouping
    KEY = "key"  # Primary/business key
    FOREIGN_KEY = "foreign_key"  # Reference to another table
    ATTRIBUTE = "attribute"  # Descriptive non-key column
    TIMESTAMP = "timestamp"  # Time dimension
    UNKNOWN = "unknown"


class RelationshipType(str, Enum):
    """Type of relationship between tables."""

    FOREIGN_KEY = "foreign_key"
    HIERARCHY = "hierarchy"
    CORRELATION = "correlation"
    SEMANTIC = "semantic"


class Cardinality(str, Enum):
    """Relationship cardinality."""

    ONE_TO_ONE = "1:1"
    ONE_TO_MANY = "1:n"
    MANY_TO_MANY = "n:m"


class QualitySeverity(str, Enum):
    """Severity of quality issues."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class DecisionSource(str, Enum):
    """Source of a decision (type, annotation, etc)."""

    AUTO = "auto"  # Automatic heuristic
    LLM = "llm"  # LLM-generated
    MANUAL = "manual"  # Human-provided
    OVERRIDE = "override"  # Manual override of LLM
    RULE = "rule"  # Rule-based
    ONTOLOGY = "ontology"  # From ontology config
    DEFAULT = "default"  # From default config


# === Identifiers ===


class ColumnRef(BaseModel):
    """Reference to a column by name."""

    table_name: str
    column_name: str

    def __str__(self) -> str:
        return f"{self.table_name}.{self.column_name}"

    def __hash__(self) -> int:
        return hash((self.table_name, self.column_name))


class TableRef(BaseModel):
    """Reference to a table by name."""

    table_name: str
    schema_name: str | None = None

    def __str__(self) -> str:
        if self.schema_name:
            return f"{self.schema_name}.{self.table_name}"
        return self.table_name


# === Staging Models ===


class SourceConfig(BaseModel):
    """Configuration for a data source."""

    name: str
    source_type: str  # 'csv', 'parquet', 'postgres', 'duckdb', 'api'

    # For files
    path: str | None = None
    file_pattern: str | None = None

    # For databases
    connection_string: str | None = None
    schema_name: str | None = None  # Renamed from 'schema' to avoid shadowing BaseModel.schema
    tables: list[str] | None = None

    # Options
    sample_size: int | None = None
