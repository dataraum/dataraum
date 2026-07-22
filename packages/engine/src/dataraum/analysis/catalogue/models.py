"""Output contract for the catalogue_semantics phase (DAT-823).

Strict, no optionals (feedback-llm-schema-no-optionals): every field is required;
absence is expressed through the ``""`` sentinel the persist path normalizes to
NULL, exactly as the per-table tier did before the rebalance moved authoring here.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

# The persisted ``ColumnConcept.meaning_status`` vocabulary — single home
# (the W2-A ``entropy.models`` precedent: the typed module owns the values, the
# CHECK in ``semantic/db_models.py`` derives from this tuple). Sorted for a
# deterministic CHECK string in the offline DDL dump. Mirrors the
# ``determination`` Literal below; ``test_catalogue_models.py`` pins the two
# against each other.
MEANING_STATUSES: tuple[str, ...] = ("ambiguous", "determined")

MeaningStatus = Literal["determined", "ambiguous"]


class ColumnConceptOutput(BaseModel):
    """Catalogue-grain semantics the catalogue agent authors for ONE column.

    Moved from the per-table tier (DAT-823 rebalance of DAT-637/769): these
    fields need the composed catalogue — the cross-cutting ontology, the
    CONFIRMED relationship catalogue with its measured evidence, the enriched
    views, the resolved slice axes — so they are authored at the catalogue
    horizon, after structure settles, never by the object-grain per-column agent
    and no longer by the structural per-table judge. Emit an entry for EVERY
    column: ``meaning`` is the load-bearing context surface (DAT-769 — meaning
    transported as context, never a categorical binding); the other fields only
    where they apply.
    """

    table_name: str = Field(description="Exact table name from the provided schema.")
    column_name: str = Field(description="Exact column name from the provided schema.")

    meaning: str = Field(
        description=(
            "The column's business-model characterization in the context of its table "
            "and the composed catalogue: what one row's value represents (its grain), "
            "and the role it plays in the business process or statement. One to three "
            "sentences of honest prose — ambiguity is expressible and welcome; never "
            "force a single label onto a multi-facet column. This is transported as "
            "context to downstream analysts, not parsed."
        ),
    )
    determination: MeaningStatus = Field(
        description=(
            "Whether the composed evidence SETTLES this column's meaning. "
            "'determined' = the meaning states what the column is. 'ambiguous' = "
            "declared ignorance WITH a meaning present: the meaning text must state "
            "what remains undetermined and what evidence would settle it — never an "
            "empty shrug, and never a guessed single label over unsettled evidence."
        ),
    )
    unit_source_column: str = Field(
        description=(
            "The column defining this measure's unit: a same-table column name, or "
            "'table_name.column_name' reachable via a CONFIRMED relationship, or "
            "'dimensionless' for ratios/rates/indices. \"\" when there is no concrete "
            "unit column — never guess."
        ),
    )
    derived_formula_hypothesis: str = Field(
        description=(
            "If this column reads as COMPUTED, the arithmetic it should obey: exactly "
            "two column names joined by one of + - * / . Operands may be in a JOINED "
            "table reachable via a confirmed relationship (use 'table.column' for a "
            "joined operand) — the derived-value check runs over the enriched view. "
            '"" when the column does not read as derived.'
        ),
    )
    derived_formula_confidence: float = Field(
        ge=0.0,
        le=1.0,
        description=(
            'Confidence (0.0-1.0) in derived_formula_hypothesis; 0.0 when the hypothesis is "".'
        ),
    )


class TableReadingOutput(BaseModel):
    """One table's business reading, authored at the catalogue horizon (DAT-823)."""

    table_name: str = Field(description="Exact table name from the provided schema.")
    entity_type: str = Field(
        description=(
            "What real-world entity this table represents. Examples: 'customers', "
            "'orders', 'products', 'transactions', 'shipments', 'appointments'. "
            "Argue it from the composed evidence — the confirmed references, the "
            "shared axes, the value samples on the join chain — not the table name "
            "alone."
        )
    )
    description: str = Field(
        description="One sentence describing the table's purpose in the business domain."
    )


class CatalogueSemanticsOutput(BaseModel):
    """The catalogue_semantics phase's structured output (DAT-823).

    Both fields are REQUIRED and blanket: one ``table_readings`` entry per table,
    one ``column_concepts`` entry per column. Returning an empty list for a
    non-empty catalogue is an error, not a shortcut — coverage gaps are retried
    scoped and then surface as loud partial warnings (DAT-769), and zero
    meaningful column rows fails the run (DAT-768).
    """

    table_readings: list[TableReadingOutput] = Field(
        description=(
            "The business reading for EVERY table in the catalogue — exactly one entry per table."
        )
    )
    column_concepts: list[ColumnConceptOutput] = Field(
        description=(
            "Catalogue-grain per-column semantics (meaning + determination, unit "
            "source, derived-formula hypothesis) — an entry for EVERY column. Every "
            "column has a meaning (a noise or unidentifiable column's honest meaning "
            "is saying exactly that, as 'ambiguous')."
        )
    )


__all__ = [
    "MEANING_STATUSES",
    "CatalogueSemanticsOutput",
    "ColumnConceptOutput",
    "MeaningStatus",
    "TableReadingOutput",
]
