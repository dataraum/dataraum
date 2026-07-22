"""Models for business cycle detection.

These models represent the output of business cycle analysis -
detected cycles, their stages, entity flows, and metrics.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field


class CycleStage(BaseModel):
    """A stage within a business cycle."""

    stage_name: str  # e.g., "Invoice Created", "Payment Received"
    stage_order: int  # Position in the cycle (1, 2, 3...)

    # How this stage is identified in the data
    indicator_column: str | None = None  # Column that indicates this stage
    indicator_values: list[str] = Field(default_factory=list)  # Values that mean this stage


class EntityFlow(BaseModel):
    """An entity that flows through a business cycle."""

    entity_type: str  # e.g., "customer", "vendor", "product"
    entity_column: str  # Column that identifies the entity
    entity_table: str  # Table containing entity master data

    # How entity connects to transaction/fact table
    fact_table: str | None = None
    fact_column: str | None = None


class DetectedCycle(BaseModel):
    """A detected business cycle."""

    cycle_id: str
    cycle_name: str  # e.g., "Accounts Receivable Cycle", "Order-to-Cash"
    cycle_type: str  # e.g., "ar_cycle", "ap_cycle", "revenue_cycle" (LLM output)

    # Canonical mapping to vocabulary
    canonical_type: str | None = None  # Mapped to vocabulary key (e.g., "accounts_receivable")
    is_known_type: bool = False  # True if cycle_type matches vocabulary

    # Direction axis (DAT-856): the resolved family + direction. Both None for a
    # non-family cycle; both set for a family cycle (a decided label, or the
    # 'undetermined' sentinel). Resolved from the judge's output at parse time by
    # ``config.resolve_cycle_identity`` — the sole producer of the pair.
    family: str | None = None
    direction: str | None = None

    description: str  # LLM-generated description of what this cycle represents
    business_value: str = "medium"  # "high", "medium", "low"

    # Structure
    stages: list[CycleStage] = Field(default_factory=list)
    entity_flows: list[EntityFlow] = Field(default_factory=list)

    # Tables involved
    tables_involved: list[str] = Field(default_factory=list)

    # Status/completion tracking
    status_column: str | None = None  # Column that tracks cycle completion
    status_table: str | None = None
    completion_value: str | None = None  # Value that indicates cycle complete (e.g., "Paid")

    # Metrics
    total_records: int | None = None
    completed_cycles: int | None = None
    completion_rate: float | None = None

    # Confidence
    confidence: float = 0.0  # How confident are we this cycle exists
    evidence: list[str] = Field(default_factory=list)  # What evidence supports this


class BusinessCycleAnalysis(BaseModel):
    """Complete business cycle analysis for a dataset."""

    analysis_id: str
    analyzed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    # Scope
    tables_analyzed: list[str] = Field(default_factory=list)
    total_columns: int = 0
    total_relationships: int = 0

    # Detected cycles
    cycles: list[DetectedCycle] = Field(default_factory=list)

    # Summary metrics
    total_cycles_detected: int = 0
    high_value_cycles: int = 0
    overall_cycle_health: float = 0.0  # 0-1 score

    # LLM interpretation
    business_summary: str = ""  # Overall description of the business model
    detected_processes: list[str] = Field(default_factory=list)
    data_quality_observations: list[str] = Field(default_factory=list)
    recommendations: list[str] = Field(default_factory=list)

    # Metadata
    llm_model: str | None = None
    analysis_duration_seconds: float | None = None

    # Raw context (for debugging/transparency)
    context_provided: dict[str, Any] = Field(default_factory=dict)


# =============================================================================
# Pydantic output models for the business_cycles structured output
#
# Flat schema (max depth 2): stages and entity flows are top-level lists that
# reference cycles by name; _parse_output groups them back into DetectedCycle.
#
# EVERY field is REQUIRED (DAT-807). An optional field is a modelling mistake —
# either the model must state the attribute or the attribute should not exist —
# and under constrained decoding each optional also spends one of the request's
# 24 optional-parameter slots (an ``X | None`` renders as an anyOf, so it spends
# a union slot too). Not-applicable is expressed by a DOCUMENTED EMPTY VALUE
# ("" / []), never by omission.
#
# The completion MEASUREMENT is the one tri-state here, and it is carried by an
# EXPLICIT discriminator (``measured``), not by absence and not by a sentinel
# value. Both alternatives were wrong in their own way:
#   * ``X | None`` puts the meaning in an omitted key — precisely the implicit
#     semantics this slice removes everywhere else. It happens to be
#     load-bearing rather than dead, which makes it worse, not better.
#   * an out-of-domain numeric sentinel (-1) is unsafe in a way "" is not: ""
#     is falsy, so a missed boundary degrades harmlessly, whereas -1 is a valid
#     float that reads as a genuine measurement — one missed normalization
#     silently reports a cycle as -100% complete.
# With ``measured`` there is no sentinel to miss and no absence to interpret: a
# model that cannot measure must SAY so. The three numbers are then 0 and
# explicitly meaningless, and ``_parse_output`` normalizes them to None on the
# domain model, so the PERSISTED shape (nullable columns, NULL when unmeasured)
# is unchanged and every downstream reader — the artifact lifecycle, health
# scoring, graph context, the cockpit — is untouched.
# =============================================================================


class CycleSummaryOutput(BaseModel):
    """Flat cycle summary — no nested objects."""

    cycle_name: str = Field(description="Descriptive name, e.g., 'Order Fulfillment Cycle'")
    cycle_type: str = Field(
        description=(
            "Type identifier in snake_case. Use a key from the KNOWN BUSINESS CYCLE TYPES "
            "vocabulary (provided in context) when the cycle matches a known type. "
            "For cycles not in the vocabulary, use a descriptive snake_case identifier "
            "(e.g., order_fulfillment, incident_resolution, employee_onboarding). "
            "Do NOT use generic labels like 'custom' or 'other'."
        )
    )
    family: str = Field(
        description=(
            "The declared cycle FAMILY this cycle belongs to, chosen from the CYCLE "
            "FAMILIES list in DOMAIN KNOWLEDGE — a family groups cycle types that differ "
            'ONLY in direction. "" when this cycle is not a member of any declared family '
            "(all cycles outside a declared family)."
        )
    )
    direction: str = Field(
        description=(
            "For a family cycle: which declared direction the served evidence decides — "
            'one of the family\'s declared direction labels, or "undetermined" when the '
            "served evidence does not decide the axis. undetermined is the HONEST answer "
            "(detected the family, could not direction it); never guess a direction to "
            'avoid it. "" when family is "" (not a family cycle).'
        )
    )
    description: str = Field(description="What this cycle represents in the business")
    business_value: str = Field(description="Business importance: high, medium, or low")
    status_column: str = Field(
        description='Column tracking cycle completion; "" when the cycle has none'
    )
    status_table: str = Field(
        description='Table containing the status column; "" when there is no status column'
    )
    completion_value: str = Field(
        description="Value indicating cycle complete, e.g., 'Paid'; \"\" when not applicable"
    )
    tables_involved: list[str] = Field(description="All tables involved in this cycle")
    measured: bool = Field(
        description=(
            "Whether you could actually MEASURE this cycle's completion from the "
            "served data. true when the three numbers below are real measurements; "
            "false when no completion signal could be derived — say so here rather "
            "than inventing numbers. Detecting a cycle and measuring it are separate "
            "claims: an honest 'detected but not measured' is a valid, useful answer."
        )
    )
    total_records: int = Field(
        description="Total records in cycle. 0 and MEANINGLESS when measured is false."
    )
    completed_cycles: int = Field(
        description="Number of completed cycles. 0 and MEANINGLESS when measured is false."
    )
    completion_rate: float = Field(
        description=(
            "Completion rate as decimal (0.0-1.0). For transactional cycles, compute "
            "from status column value counts (e.g., paid/total). For non-transactional "
            "cycles (reporting, reconciliation), derive from the closest available "
            "signal: posting ratio, balance ratio, period coverage, or similar metric. "
            "0.0 and MEANINGLESS when measured is false — a cycle you could not "
            "measure is NOT a cycle that is 0% complete."
        ),
    )
    confidence: float = Field(description="Confidence in this detection (0.0-1.0)")
    evidence: list[str] = Field(description="Evidence supporting this cycle detection")


class StageEntryOutput(BaseModel):
    """Flat stage — references a cycle by name."""

    cycle_name: str = Field(description="Name of the cycle this stage belongs to")
    stage_name: str = Field(description="Name of this stage, e.g., 'Order Shipped'")
    stage_order: int = Field(description="Position in cycle (1, 2, 3...)")
    indicator_column: str = Field(
        description='Column that indicates this stage; "" when the stage has no indicator'
    )
    indicator_value: str = Field(
        description='Value that means this stage (one row per value); "" when none'
    )


class EntityFlowEntryOutput(BaseModel):
    """Flat entity flow — references a cycle by name."""

    cycle_name: str = Field(description="Name of the cycle this entity participates in")
    entity_type: str = Field(description="Type of entity, e.g., 'customer', 'vendor'")
    entity_column: str = Field(description="Column that identifies the entity")
    entity_table: str = Field(description="Table containing entity data")
    fact_table: str = Field(
        description='Related fact/transaction table; "" when the entity has no fact table'
    )
    fact_column: str = Field(description='Column in the fact table; "" when there is none')


class BusinessCycleAnalysisOutput(BaseModel):
    """Complete business_cycles structured output.

    Flat schema: cycles, stages, and entity_flows are separate top-level lists.
    Stages and entity_flows reference their parent cycle via cycle_name.
    """

    cycles: list[CycleSummaryOutput] = Field(
        description="Detected business cycles (one per cycle); [] when none were detected"
    )
    stages: list[StageEntryOutput] = Field(
        description="Cycle stages (one row per stage per cycle, referencing cycle_name); [] if none",
    )
    entity_flows: list[EntityFlowEntryOutput] = Field(
        description="Entity flows (one row per entity per cycle, referencing cycle_name); [] if none",
    )
    business_summary: str = Field(
        description="Overall interpretation of the business model and its cycles"
    )
    detected_processes: list[str] = Field(
        description="Business processes identified, e.g., 'Order-to-Cash'; [] when none",
    )
    data_quality_observations: list[str] = Field(
        description="Data quality issues noticed during analysis; [] when none",
    )
    recommendations: list[str] = Field(
        description="Suggestions for improving data completeness or cycle tracking; [] when none",
    )
