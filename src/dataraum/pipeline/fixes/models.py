"""Fix document models.

FixDocument is the universal fix format — what agents write and interpreters apply.
DataFix is the ORM model that persists fix documents across pipeline re-runs.

A single user-facing fix action produces one or more FixDocuments (composite fix).
Each document targets exactly one interpreter (config, metadata, or data).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

if TYPE_CHECKING:
    from pydantic import BaseModel

from sqlalchemy import DateTime, ForeignKey, Integer, String
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage import Base

# ---------------------------------------------------------------------------
# In-memory models (API surface)
# ---------------------------------------------------------------------------


@dataclass
class FixDocument:
    """A single atomic fix operation targeting one interpreter.

    Agents produce these by filling in a FixSchema. Each document targets
    exactly one interpreter (config, metadata, or data). A composite fix
    action produces an ordered list of FixDocuments.

    Args:
        target: Which interpreter handles this: "config", "metadata", or "data".
        action: Human-readable action name, e.g. "declare_unit".
        table_name: Scoping — which table this fix applies to.
        column_name: Scoping — which column (None for table-scoped fixes).
        dimension: Which entropy dimension this addresses.
        payload: Target-specific data. Shape determined by the fix schema.
        description: Human-readable summary of what this fix does.
        fix_id: Unique identifier (auto-generated).
        ordinal: Execution order within a composite fix (0-indexed).
    """

    target: str  # "config" | "metadata" | "data"
    action: str
    table_name: str
    column_name: str | None
    dimension: str
    payload: dict[str, Any]
    description: str = ""
    fix_id: str = field(default_factory=lambda: str(uuid4()))
    ordinal: int = 0

    def __post_init__(self) -> None:
        if self.target not in ("config", "metadata", "data"):
            msg = f"target must be 'config', 'metadata', or 'data', got {self.target!r}"
            raise ValueError(msg)


@dataclass
class FixSchemaField:
    """A single field in a fix schema.

    Tells the agent what value to provide and how to validate it.
    """

    type: str  # "string" | "float" | "int" | "bool" | "enum" | "regex" | "duckdb_sql"
    required: bool = True
    description: str = ""
    default: Any = None
    examples: list[str] | None = None
    enum_values: list[str] | None = None


@dataclass
class FixSchema:
    """Schema for a valid fix document.

    Exposed by detectors to tell agents what fix documents they can write.
    The agent fills in the fields to produce a FixDocument payload.

    Args:
        action: Action name this schema produces.
        target: Which interpreter: "config", "metadata", or "data".
        description: What this fix does.
        fields: Required/optional fields the agent must provide.
        config_path: For config target — relative YAML path.
        key_path: For config target — nested key path.
        operation: For config target — set/append/merge.
        model: For metadata target — ORM model name.
        templates: For data target — named SQL templates with {placeholders}.
        requires_rerun: Phase to re-run after applying (None = no re-run).
        guidance: LLM guidance for agent Q&A.
        key_template: Format string for merge key suffix from params
            (e.g. "{from_table}->{to_table}"). When None, use
            affected_column as key suffix.
    """

    action: str
    target: str  # "config" | "metadata" | "data"
    description: str = ""
    fields: dict[str, FixSchemaField] = field(default_factory=dict)

    # Config target specifics
    config_path: str | None = None
    key_path: list[str] | None = None
    operation: str | None = None

    # Metadata target specifics
    model: str | None = None

    # Data target specifics
    templates: dict[str, str] | None = None

    # Routing and lifecycle
    requires_rerun: str | None = None
    guidance: str = ""
    key_template: str | None = None
    routing: str | None = None  # "preprocess" or "postprocess"
    gate: str | None = None  # gate where this fix is offered

    def _build_pydantic_model(self) -> type[BaseModel] | None:
        """Build a dynamic Pydantic model from self.fields.

        Returns None if there are no fields defined.
        """
        if not self.fields:
            return None

        from enum import Enum

        from pydantic import Field as PydanticField
        from pydantic import create_model

        _TYPE_MAP: dict[str, type] = {
            "string": str,
            "float": float,
            "int": int,
            "bool": bool,
            "enum": str,
            "regex": str,
            "duckdb_sql": str,
        }

        model_fields: dict[str, Any] = {}
        for name, f in self.fields.items():
            field_type: type = _TYPE_MAP.get(f.type, str)

            # For enum fields, create a real Enum so Pydantic emits "enum" in JSON schema
            if f.enum_values:
                field_type = Enum(name, {v: v for v in f.enum_values}, type=str)  # type: ignore[misc,no-redef]

            field_kwargs: dict[str, Any] = {}
            if f.description:
                field_kwargs["description"] = f.description

            if f.required and f.default is None:
                model_fields[name] = (field_type, PydanticField(..., **field_kwargs))
            else:
                model_fields[name] = (field_type, PydanticField(default=f.default, **field_kwargs))

        return create_model("FixParameters", **model_fields)

    def validate_payload(self, payload: dict[str, Any]) -> list[str]:
        """Validate a payload against this schema using Pydantic.

        Returns:
            List of validation error messages (empty if valid).
        """
        model = self._build_pydantic_model()
        if model is None:
            return []

        from pydantic import ValidationError

        try:
            model.model_validate(payload)
            return []
        except ValidationError as e:
            return [err["msg"] for err in e.errors()]


# ---------------------------------------------------------------------------
# ORM model (persistence — survives --force re-runs)
# ---------------------------------------------------------------------------


class DataFix(Base):
    """Persisted fix record.

    Stores fix documents so the data_fixes phase can replay them on
    pipeline re-runs. Scoped by table_name/column_name (not column_id)
    to survive --force re-runs that regenerate typed-layer IDs.
    """

    __tablename__ = "data_fixes"

    fix_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    source_id: Mapped[str] = mapped_column(ForeignKey("sources.source_id"), nullable=False)

    # What this fix does
    action: Mapped[str] = mapped_column(String, nullable=False)
    target: Mapped[str] = mapped_column(String, nullable=False)  # config|metadata|data
    dimension: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str] = mapped_column(String, nullable=False, default="")

    # Scope (denormalized for --force resilience)
    table_name: Mapped[str] = mapped_column(String, nullable=False)
    column_name: Mapped[str | None] = mapped_column(String, nullable=True)

    # The actual fix content — interpreter reads this based on target
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)

    # Lifecycle
    status: Mapped[str] = mapped_column(String, nullable=False, default="pending")
    error_message: Mapped[str | None] = mapped_column(String, nullable=True)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    applied_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    @classmethod
    def from_document(cls, source_id: str, doc: FixDocument) -> DataFix:
        """Create a DataFix record from an in-memory FixDocument."""
        return cls(
            fix_id=doc.fix_id,
            source_id=source_id,
            action=doc.action,
            target=doc.target,
            dimension=doc.dimension,
            description=doc.description,
            table_name=doc.table_name,
            column_name=doc.column_name,
            payload=doc.payload,
            ordinal=doc.ordinal,
        )

    def to_document(self) -> FixDocument:
        """Convert back to an in-memory FixDocument."""
        return FixDocument(
            fix_id=self.fix_id,
            target=self.target,
            action=self.action,
            table_name=self.table_name,
            column_name=self.column_name,
            dimension=self.dimension,
            payload=self.payload,
            description=self.description,
            ordinal=self.ordinal,
        )
