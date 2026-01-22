"""Pydantic models for semantic analysis.

Contains data structures for semantic annotations, entity detection,
relationships, and enrichment results.

Includes tool-friendly models for LLM structured output via Anthropic tool use.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from dataraum.core.models.base import (
    ColumnRef,
    DecisionSource,
    RelationshipType,
    SemanticRole,
)

# =============================================================================
# Tool Output Models - Used as Pydantic tools for LLM structured output
# =============================================================================


class ColumnSemanticOutput(BaseModel):
    """Semantic annotation for a single database column.

    The LLM uses this model to describe the meaning and role of each column
    in the analyzed schema.
    """

    column_name: str = Field(
        description="Exact column name from the provided schema. Must match exactly."
    )

    semantic_role: Literal[
        "key", "foreign_key", "measure", "dimension", "timestamp", "attribute"
    ] = Field(
        description=(
            "Structural role of the column: "
            "'key' = primary identifier (unique, non-null); "
            "'foreign_key' = references another table's key column; "
            "'measure' = numeric value for aggregation (sum, avg, count); "
            "'dimension' = categorical attribute for grouping/filtering; "
            "'timestamp' = date or datetime for time-based analysis; "
            "'attribute' = descriptive field not used for aggregation or grouping"
        )
    )

    entity_type: str = Field(
        description=(
            "What real-world entity this column represents. Examples: "
            "'customer_id', 'product_name', 'order_date', 'transaction_amount', "
            "'account_code', 'invoice_number'. Be specific to the domain."
        )
    )

    business_term: str = Field(
        description=(
            "Human-readable business name for this column. Convert technical names "
            "to natural language. Examples: 'Customer ID' → 'Customer Identifier', "
            "'txn_amt' → 'Transaction Amount', 'cust_nm' → 'Customer Name'"
        )
    )

    business_concept: str | None = Field(
        default=None,
        description=(
            "Standard domain concept from the provided ontology concept list. "
            "Use the EXACT concept name from the list (e.g., 'revenue', 'accounts_receivable', "
            "'fiscal_period'). Match based on semantic meaning, not just column name patterns. "
            "Set to null ONLY if no concept in the list applies to this column."
        ),
    )

    description: str = Field(
        description=(
            "One sentence describing what this column contains and how it's used. "
            "Be specific to the business context."
        )
    )

    confidence: float = Field(
        ge=0.0,
        le=1.0,
        description="Confidence in this analysis (0.0-1.0). Use higher values when certain.",
    )


class TableSemanticOutput(BaseModel):
    """Semantic annotation for a database table.

    Describes the table's purpose, structure, and the semantic meaning
    of all its columns.
    """

    table_name: str = Field(description="Exact table name from the provided schema.")

    entity_type: str = Field(
        description=(
            "What real-world entity this table represents. Examples: 'customers', "
            "'orders', 'products', 'transactions', 'invoices', 'payments'"
        )
    )

    description: str = Field(
        description="One sentence describing the table's purpose in the business domain."
    )

    is_fact_table: bool = Field(
        description=(
            "True if this is a fact table (contains transactions, events, or measurements). "
            "False if this is a dimension table (contains reference/lookup data)."
        )
    )

    grain: list[str] = Field(
        description=(
            "Column names that define the unique grain (primary key) of the table. "
            "These columns together uniquely identify each row."
        )
    )

    time_column: str | None = Field(
        default=None,
        description="Primary timestamp column for time-based analysis, if the table has one.",
    )

    columns: list[ColumnSemanticOutput] = Field(
        description="Semantic annotations for each column in this table."
    )


class RelationshipOutput(BaseModel):
    """A detected relationship between two tables.

    Describes how tables are connected through foreign key or hierarchical
    relationships.
    """

    from_table: str = Field(description="Source table name containing the foreign key.")

    from_column: str = Field(
        description="Column in the source table that references another table."
    )

    to_table: str = Field(description="Target table name being referenced.")

    to_column: str = Field(
        description="Column in the target table being referenced (usually a key)."
    )

    relationship_type: Literal["foreign_key", "hierarchy"] = Field(
        description=(
            "'foreign_key' = standard FK relationship; "
            "'hierarchy' = parent-child relationship within same entity"
        )
    )

    cardinality: Literal["one_to_one", "one_to_many", "many_to_one", "many_to_many"] = Field(
        description=(
            "Relationship cardinality: 'many_to_one' = many source rows reference one target row "
            "(most common for FKs); 'one_to_many' = one source row has many target rows; "
            "'one_to_one' = exact match; 'many_to_many' = requires junction table"
        )
    )

    confidence: float = Field(
        ge=0.0,
        le=1.0,
        description="Confidence in this relationship (0.0-1.0).",
    )

    reasoning: str = Field(
        description="Brief explanation of why this relationship exists, based on column names and data patterns."
    )


class SemanticAnalysisOutput(BaseModel):
    """Complete semantic analysis result for a database schema.

    This is the top-level tool output containing all tables, columns,
    and relationships analyzed.
    """

    tables: list[TableSemanticOutput] = Field(
        description="Semantic annotations for each table in the schema."
    )

    relationships: list[RelationshipOutput] = Field(
        default_factory=list,
        description=(
            "Relationships between tables. Evaluate the pre-computed candidates "
            "and include only confirmed relationships. Add any additional "
            "relationships you detect that weren't in the candidates."
        ),
    )

    summary: str = Field(
        default="",
        description=(
            "2-3 sentence overview of the schema structure, main entities, and business domain."
        ),
    )


# =============================================================================
# Internal Models - Used for storage and processing after LLM output
# =============================================================================


class SemanticAnnotation(BaseModel):
    """Semantic annotation for a column (LLM-generated or manual)."""

    column_id: str
    column_ref: ColumnRef

    semantic_role: SemanticRole
    entity_type: str | None = None
    business_name: str | None = None
    business_description: str | None = None  # LLM-generated description

    # Business concept mapping - maps to standard domain concepts
    # from the active ontology (e.g., 'accounts_receivable', 'revenue', 'fiscal_period')
    business_concept: str | None = None

    annotation_source: DecisionSource
    annotated_by: str | None = None  # e.g., 'claude-sonnet-4-20250514' or 'user@example.com'
    confidence: float


class EntityDetection(BaseModel):
    """Entity type detection for a table."""

    table_id: str
    table_name: str

    entity_type: str
    description: str | None = None  # LLM-generated table description
    confidence: float
    evidence: dict[str, Any] = Field(default_factory=dict)

    grain_columns: list[str] = Field(default_factory=list)
    is_fact_table: bool = False
    is_dimension_table: bool = False
    time_column: str | None = None  # Primary time column


class Relationship(BaseModel):
    """A detected relationship between tables."""

    relationship_id: str

    from_table: str
    from_column: str
    to_table: str
    to_column: str

    relationship_type: RelationshipType
    cardinality: str | None = None  # Using Cardinality from base

    confidence: float
    detection_method: str
    evidence: dict[str, Any] = Field(default_factory=dict)

    is_confirmed: bool = False


class SemanticEnrichmentResult(BaseModel):
    """Result of semantic enrichment operation."""

    annotations: list[SemanticAnnotation] = Field(default_factory=list)
    entity_detections: list[EntityDetection] = Field(default_factory=list)
    relationships: list[Relationship] = Field(default_factory=list)
    source: str = "llm"  # 'llm', 'manual', 'override'


__all__ = [
    # Tool output models for LLM structured output
    "ColumnSemanticOutput",
    "TableSemanticOutput",
    "RelationshipOutput",
    "SemanticAnalysisOutput",
    # Internal models for storage and processing
    "SemanticAnnotation",
    "EntityDetection",
    "Relationship",
    "SemanticEnrichmentResult",
]
