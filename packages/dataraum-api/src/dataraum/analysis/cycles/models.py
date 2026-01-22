"""Models for business cycle detection.

These models represent the output of business cycle analysis -
detected cycles, their stages, entity flows, and metrics.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class CycleStage(BaseModel):
    """A stage within a business cycle."""

    stage_name: str  # e.g., "Invoice Created", "Payment Received"
    stage_order: int  # Position in the cycle (1, 2, 3...)

    # How this stage is identified in the data
    indicator_column: str | None = None  # Column that indicates this stage
    indicator_table: str | None = None
    indicator_values: list[str] = Field(default_factory=list)  # Values that mean this stage

    # Metrics for this stage
    record_count: int | None = None
    completion_rate: float | None = None  # % that progress to next stage


class EntityFlow(BaseModel):
    """An entity that flows through a business cycle."""

    entity_type: str  # e.g., "customer", "vendor", "product"
    entity_column: str  # Column that identifies the entity
    entity_table: str  # Table containing entity master data

    # How entity connects to transaction/fact table
    fact_table: str | None = None
    fact_column: str | None = None
    relationship_type: str | None = None  # "foreign_key", "semantic_match"


class DetectedCycle(BaseModel):
    """A detected business cycle."""

    cycle_id: str
    cycle_name: str  # e.g., "Accounts Receivable Cycle", "Order-to-Cash"
    cycle_type: str  # e.g., "ar_cycle", "ap_cycle", "revenue_cycle" (LLM output)

    # Canonical mapping to vocabulary
    canonical_type: str | None = None  # Mapped to vocabulary key (e.g., "accounts_receivable")
    is_known_type: bool = False  # True if cycle_type matches vocabulary

    description: str  # LLM-generated description of what this cycle represents
    business_value: str = "medium"  # "high", "medium", "low"

    # Structure
    stages: list[CycleStage] = Field(default_factory=list)
    entity_flows: list[EntityFlow] = Field(default_factory=list)

    # Tables and columns involved
    tables_involved: list[str] = Field(default_factory=list)
    key_columns: dict[str, list[str]] = Field(default_factory=dict)  # table -> columns

    # Status/completion tracking
    status_column: str | None = None  # Column that tracks cycle completion
    status_table: str | None = None
    completion_value: str | None = None  # Value that indicates cycle complete (e.g., "Paid")

    # Metrics
    total_records: int | None = None
    completed_cycles: int | None = None
    completion_rate: float | None = None
    avg_cycle_time_days: float | None = None

    # Confidence
    confidence: float = 0.0  # How confident are we this cycle exists
    evidence: list[str] = Field(default_factory=list)  # What evidence supports this


class BusinessCycleAnalysis(BaseModel):
    """Complete business cycle analysis for a dataset."""

    analysis_id: str
    analyzed_at: datetime = Field(default_factory=datetime.utcnow)

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
    tool_calls_made: list[dict[str, Any]] = Field(default_factory=list)


# =============================================================================
# Pydantic output models for LLM tool use (submit_analysis)
# =============================================================================


class CycleStageOutput(BaseModel):
    """Stage output for LLM tool."""

    stage_name: str = Field(description="Name of this stage, e.g., 'Invoice Created'")
    stage_order: int = Field(description="Position in cycle (1, 2, 3...)")
    indicator_column: str | None = Field(
        default=None, description="Column that indicates this stage"
    )
    indicator_values: list[str] = Field(
        default_factory=list, description="Values that mean this stage"
    )


class EntityFlowOutput(BaseModel):
    """Entity flow output for LLM tool."""

    entity_type: str = Field(description="Type of entity, e.g., 'customer', 'vendor'")
    entity_column: str = Field(description="Column that identifies the entity")
    entity_table: str = Field(description="Table containing entity data")
    fact_table: str | None = Field(default=None, description="Related fact/transaction table")
    fact_column: str | None = Field(default=None, description="Column in fact table")


class DetectedCycleOutput(BaseModel):
    """Detected cycle output for LLM tool."""

    cycle_name: str = Field(description="Descriptive name, e.g., 'Accounts Receivable Cycle'")
    cycle_type: str = Field(
        description="Type identifier: ar_cycle, ap_cycle, revenue_cycle, expense_cycle, inventory_cycle, payroll_cycle, or custom"
    )
    description: str = Field(description="What this cycle represents in the business")
    business_value: str = Field(
        default="medium", description="Business importance: high, medium, or low"
    )

    # Structure
    entity_flows: list[EntityFlowOutput] = Field(
        default_factory=list, description="Entities that flow through this cycle"
    )
    stages: list[CycleStageOutput] = Field(
        default_factory=list, description="Stages in the cycle progression"
    )

    # Status tracking
    status_column: str | None = Field(default=None, description="Column tracking cycle completion")
    status_table: str | None = Field(default=None, description="Table containing status column")
    completion_value: str | None = Field(
        default=None, description="Value indicating cycle complete, e.g., 'Paid'"
    )

    # Tables
    tables_involved: list[str] = Field(
        default_factory=list, description="All tables involved in this cycle"
    )

    # Metrics (from tool calls)
    total_records: int | None = Field(default=None, description="Total records in cycle")
    completed_cycles: int | None = Field(default=None, description="Number of completed cycles")
    completion_rate: float | None = Field(
        default=None, description="Completion rate as decimal (0.0-1.0)"
    )

    # Confidence
    confidence: float = Field(default=0.5, description="Confidence in this detection (0.0-1.0)")
    evidence: list[str] = Field(
        default_factory=list, description="Evidence supporting this cycle detection"
    )


class BusinessCycleAnalysisOutput(BaseModel):
    """Complete analysis output for submit_analysis tool.

    This is the structured output the LLM must provide when done analyzing.
    """

    cycles: list[DetectedCycleOutput] = Field(description="List of detected business cycles")
    business_summary: str = Field(
        description="Overall interpretation of the business model and its cycles"
    )
    detected_processes: list[str] = Field(
        default_factory=list,
        description="List of business processes identified, e.g., 'Order-to-Cash', 'Procure-to-Pay'",
    )
    data_quality_observations: list[str] = Field(
        default_factory=list,
        description="Data quality issues noticed during analysis",
    )
    recommendations: list[str] = Field(
        default_factory=list,
        description="Suggestions for improving data completeness or cycle tracking",
    )
