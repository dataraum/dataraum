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

    semantic_role: Literal["key", "measure", "dimension", "timestamp", "attribute"] = Field(
        description=(
            "Structural role of the column, judged from THIS table alone: "
            "'key' = primary identifier (unique, non-null); "
            "'measure' = numeric value for aggregation (sum, avg, count); "
            "'dimension' = categorical attribute for grouping/filtering; "
            "'timestamp' = date or datetime for time-based analysis; "
            "'attribute' = descriptive field not used for aggregation or grouping. "
            "Do NOT classify foreign keys — whether a column references another "
            "table is decided later from the confirmed relationship catalogue, not "
            "from one table."
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

    description: str = Field(
        description=(
            "One sentence describing what this column contains and how it's used. "
            "Be specific to the business context."
        )
    )

    confidence: float = Field(
        ge=0.0,
        le=1.0,
        description=(
            "Confidence in this annotation (0.0-1.0). Reflects how clearly the "
            "COLUMN NAME communicates its meaning. High (0.85-1.0) for self-documenting "
            "names like 'vendor_id'. Moderate (0.6-0.8) for recognizable abbreviations "
            "like 'amt'. Low (0.2-0.4) for random or encoded names like 'xq_v7kl' — "
            "even if you can infer meaning from the data values."
        ),
    )

    temporal_behavior_claim: Literal["stock", "flow", "unsure"] = Field(
        description=(
            "Does this column hold a STOCK or a FLOW? A 'stock' is a carried-forward "
            "point-in-time level — a balance, position, headcount, or status that "
            "persists until changed and must NOT be summed across periods (summing "
            "balances double-counts). A 'flow' is a per-period movement — a transaction "
            "amount, payment, sale, or change that accumulates over time and IS "
            "summable. Judge from the business meaning of the name, the semantic role, "
            "and representative values; a periodic snapshot reported each period (e.g. a "
            "trial-balance line) is still a stock. Use 'unsure' when the column is not a "
            "numeric measure, or when name, role, and values give no clear stock/flow "
            "signal — do not guess. This is an INDEPENDENT read: report what the column "
            "actually looks like even if it disagrees with the expected concept."
        )
    )

    temporal_behavior_claim_confidence: float = Field(
        ge=0.0,
        le=1.0,
        description=(
            "Confidence (0.0–1.0) in temporal_behavior_claim. High (0.85–1.0) for "
            "unambiguous domain language (balance, payment, revenue, closing_position). "
            "Moderate (0.6–0.8) when the role implies it but the name is ambiguous. Low "
            "(0.2–0.4) for a weak signal. Set this low whenever you answer 'unsure'."
        ),
    )


class KeyColumnPair(BaseModel):
    """One ADDITIONAL component of a composite key (DAT-277)."""

    from_column: str = Field(description="Column in the source table.")
    to_column: str = Field(description="Matching column in the target table.")


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

    key_columns: list[KeyColumnPair] = Field(
        default_factory=list,
        description=(
            "ADDITIONAL key columns beyond (from_column, to_column), making the key "
            "COMPOSITE. Leave EMPTY for a normal single-column relationship. Use this "
            "for a fan-trap edge whose single-column join OVER-COUNTS but whose "
            "multi-column key joins cleanly — typically a shared tenant/scope column "
            "present in both tables. The full key is (from_column, to_column) plus "
            "these. Only confirm a composite when the candidate evidence shows it "
            "RESOLVES the fan-out (composite cardinality many-to-one / one-to-one, "
            "not many-to-many)."
        ),
    )

    relationship_type: Literal["foreign_key", "hierarchy"] = Field(
        description=(
            "'foreign_key' = standard FK relationship; "
            "'hierarchy' = parent-child relationship within same entity"
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


class TimeColumn(BaseModel):
    """One event-time axis of a table (DAT-565 multi-temporal).

    A denormalized table commonly has several — each a distinct temporal lens
    (order vs ship vs delivery). Carries a one-line note so downstream
    formatters and the answer agent can pick the right lens per question."""

    column: str = Field(description="Exact column name from the provided schema.")
    aspect: str = Field(
        description=(
            "The temporal aspect this column records, as a short lowercase label "
            "(e.g. 'order', 'ship', 'delivery', 'payment') — distinguishes one "
            "event date from another on the same row."
        )
    )
    note: str = Field(description="One sentence describing what this time column represents.")


class IdentityColumn(BaseModel):
    """A recurring real-world identity in a table (DAT-565), distinct from grain.

    A would-be foreign key: high-cardinality, recurs across rows, functionally
    determines other columns. May be a NON-grain column (an FK pointing
    elsewhere), so it is not derivable from ``grain`` — and the grain may be a
    surrogate row key that identifies nothing. Consumed by driver discovery
    (DAT-563) to cluster measurements by entity."""

    column: str = Field(description="Exact column name from the provided schema.")
    note: str = Field(
        description=("One sentence: what entity this identifies and how it recurs across rows.")
    )


class TableEntityOutput(BaseModel):
    """Entity-level classification for a single table (per-table tier)."""

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

    time_columns: list[TimeColumn] = Field(
        default_factory=list,
        description=(
            "EVERY column recording WHEN a row's event occurred (booking/transaction/"
            "observation date) — each a distinct temporal lens the analysis can "
            "segment by (e.g. order_date, ship_date, delivery_date). A denormalized "
            "table commonly has several; emit all. Exclude attribute dates such as "
            "due_date or valid_until, and record metadata like created_at. Empty if "
            "the table has no event-time column."
        ),
    )

    identity_columns: list[IdentityColumn] = Field(
        default_factory=list,
        description=(
            "Recurring real-world identities — high-cardinality columns that recur "
            "across rows and identify a real entity (a customer, account, vehicle), "
            "i.e. would-be foreign keys. Distinct from grain: an identity may be a "
            "non-grain column, and the grain may be a surrogate row key that "
            "identifies nothing. Empty if the table has none."
        ),
    )


class ColumnConceptOutput(BaseModel):
    """Catalogue-grain semantics the table agent authors for ONE column (DAT-637).

    These need the composed catalogue — the cross-cutting ontology, the confirmed
    relationships, the joined views — so they are decided here, never by the
    object-grain per-column agent. Only emit a column that carries at least one of
    these; columns with none are omitted.
    """

    table_name: str = Field(description="Exact table name from the provided schema.")
    column_name: str = Field(description="Exact column name from the provided schema.")

    business_concept: str | None = Field(
        default=None,
        description=(
            "The EXACT ontology concept this column GROUNDS (e.g. 'revenue', "
            "'accounts_receivable'), or null. Bind ONLY a genuine discriminator the "
            "agent can filter or aggregate on. NEVER bind a concept to a near-constant "
            "column (one value ≥90% — a status flag like a 99%-true boolean is not a "
            "discriminator). When a concept has no genuine column in this catalogue "
            "(e.g. revenue is SUM of an amount filtered by an account-class value, not "
            "any single column), leave it null — it grounds via value-sets, honestly."
        ),
    )
    unit_source_column: str | None = Field(
        default=None,
        description=(
            "The column defining this measure's unit: a same-table column name, or "
            "'table_name.column_name' reachable via a CONFIRMED relationship, or "
            "'dimensionless' for ratios/rates/indices. Null when there is no concrete "
            "unit column — never guess."
        ),
    )
    derived_formula_hypothesis: str | None = Field(
        default=None,
        description=(
            "If this column reads as COMPUTED, the arithmetic it should obey: exactly "
            "two column names joined by one of + - * / . Operands may be in a JOINED "
            "table reachable via a confirmed relationship (use 'table.column' for a "
            "joined operand) — the derived-value check runs over the enriched view. "
            "Null when the column does not read as derived."
        ),
    )
    derived_formula_confidence: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Confidence (0.0–1.0) in derived_formula_hypothesis; 0.0 when null.",
    )


class TableSynthesisOutput(BaseModel):
    """Per-table synthesis tool output: entities, relationships, column concepts.

    The per-table (catalogue-grain) tier. It classifies tables, confirms
    relationships, AND authors the catalogue-grain per-column semantics
    (``column_concepts``, DAT-637) that the object-grain per-column agent cannot
    decide. The object-grain column annotations (role, term, stock/flow claim) are
    produced by the per-column phase and provided here only as read-only context.
    """

    tables: list[TableEntityOutput] = Field(
        description="Entity classification for each table in the schema."
    )

    relationships: list[RelationshipOutput] = Field(
        default_factory=list,
        description=(
            "Relationships between tables. Evaluate the pre-computed candidates "
            "and include only confirmed relationships. Add any additional "
            "relationships you detect that weren't in the candidates."
        ),
    )

    column_concepts: list[ColumnConceptOutput] = Field(
        default_factory=list,
        description=(
            "Catalogue-grain per-column semantics (ontology concept, unit source, "
            "derived-formula hypothesis). Emit only columns carrying at least one; "
            "omit the rest."
        ),
    )


# =============================================================================
# Per-column annotation output (DAT-362: now the authoritative per-column phase output)
# =============================================================================


class TableColumnAnnotation(BaseModel):
    """Column annotations for a single table (per-column phase output)."""

    table_name: str = Field(description="Exact table name from the provided schema.")
    columns: list[ColumnSemanticOutput] = Field(
        description="Semantic annotations for each column in this table."
    )


class ColumnAnnotationOutput(BaseModel):
    """Output from the per-column annotation phase.

    Contains column-level annotations only — no relationships or
    table-level entity classification. Those are handled by semantic_per_table.
    """

    tables: list[TableColumnAnnotation] = Field(description="Column annotations grouped by table.")


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

    # Temporal behavior from ontology: 'additive' or 'point_in_time'
    temporal_behavior: str | None = None

    # Cross-column unit inference: column name that defines the unit for this measure
    unit_source_column: str | None = None

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

    grain_columns: list[str] = Field(default_factory=list)
    is_fact_table: bool = False
    is_dimension_table: bool = False
    time_columns: list[TimeColumn] = Field(default_factory=list)  # all event-time axes (DAT-565)
    identity_columns: list[IdentityColumn] = Field(default_factory=list)  # recurring identities


class Relationship(BaseModel):
    """A detected relationship between tables."""

    relationship_id: str

    from_table: str
    from_column: str
    to_table: str
    to_column: str

    # ADDITIONAL composite-key components beyond (from_column, to_column), the
    # LLM's confirmed composite (DAT-277). Empty = single-column. When present,
    # the full key is (from_column, to_column) plus these pairs; the processor
    # persists the whole set as ONE relationship group.
    key_columns: list[tuple[str, str]] = Field(default_factory=list)

    relationship_type: RelationshipType
    cardinality: str | None = None  # Using Cardinality from base

    confidence: float
    detection_method: str
    evidence: dict[str, Any] = Field(default_factory=dict)


class SemanticEnrichmentResult(BaseModel):
    """Result of semantic enrichment operation."""

    annotations: list[SemanticAnnotation] = Field(default_factory=list)
    entity_detections: list[EntityDetection] = Field(default_factory=list)
    relationships: list[Relationship] = Field(default_factory=list)
    # Catalogue-grain per-column semantics authored by the table agent (DAT-637),
    # persisted as ``ColumnConcept`` rows under the catalogue head.
    column_concepts: list[ColumnConceptOutput] = Field(default_factory=list)
    source: str = "llm"  # 'llm', 'manual', 'override'


__all__ = [
    # Tool output models for LLM structured output
    "ColumnSemanticOutput",
    "RelationshipOutput",
    # Per-table synthesis output (DAT-362 Option B)
    "TableEntityOutput",
    "ColumnConceptOutput",
    "TableSynthesisOutput",
    # Per-column annotation output
    "TableColumnAnnotation",
    "ColumnAnnotationOutput",
    # Internal models for storage and processing
    "SemanticAnnotation",
    "EntityDetection",
    "Relationship",
    "SemanticEnrichmentResult",
]
