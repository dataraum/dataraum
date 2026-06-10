"""Pydantic models for aggregation-lineage discovery (DAT-491)."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

# The reconciliation patterns. ``per_period`` ⇒ the measure IS each period's
# movement (a flow); ``cumulative`` ⇒ the measure carries forward (a stock).
PATTERN_PER_PERIOD = "per_period"
PATTERN_CUMULATIVE = "cumulative"


class LineageCandidate(BaseModel):
    """One LLM-proposed events→measure rollup hypothesis.

    The LLM proposes ONLY the hypothesis: which measure column plausibly
    aggregates which event table, under what value expression and filter —
    the semantic judgment a name can't settle and the data adjudicates.
    Everything mechanical about the alignment (entity key, event date column,
    header join, period bridge) is derived in the processor from the
    relationship catalog and temporal metadata the engine already verified —
    never proposed, never guessed.
    """

    measure_table: str = Field(description="table_name of the table holding the measure column")
    measure_column: str = Field(description="the measure column being explained")
    event_table: str = Field(description="table_name of the event-level table")
    event_value_sql: str = Field(
        description=(
            "SQL expression over event-table columns whose per-period SUM should "
            "reproduce the measure (its movement). Signed conventions matter: e.g. "
            '\'"debit" - "credit"\' for debit-normal measures, \'"credit" - "debit"\' '
            "for credit-normal ones, or plain '\"amount\"'. Double-quote column names."
        )
    )
    event_filter_sql: str | None = Field(
        default=None,
        description=(
            "optional WHERE condition selecting the effective events, "
            "e.g. '\"status\" = ''posted'''"
        ),
    )
    rationale: str = Field(description="one sentence: why this rollup plausibly exists")


class AggregationLineageProposals(BaseModel):
    """The LLM tool output: zero or more candidates (empty = nothing plausible)."""

    candidates: list[LineageCandidate] = Field(default_factory=list)


class CandidateDisposal(BaseModel):
    """The deterministic verdict on one candidate (only reconciled ones persist)."""

    pattern: Literal["per_period", "cumulative"]
    match_rate: float = Field(ge=0.0, le=1.0)
    r_flow_median: float
    r_stock_median: float
    n_entities: int
    n_entities_fired: int
