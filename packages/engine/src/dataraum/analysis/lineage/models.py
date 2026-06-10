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

    The LLM proposes WHERE a lineage might exist and HOW to align the two
    tables (key, period bridge, value expression); it does NOT decide stock vs
    flow — the deterministic reconciliation statistic disposes every candidate.
    All ``*_sql`` fields are DuckDB SQL expressions over the named table only,
    with column names double-quoted.
    """

    measure_table: str = Field(description="table_name of the table holding the measure column")
    measure_duckdb_path: str = Field(description="exact duckdb_path of the measure table")
    measure_column: str = Field(description="the measure column being explained")
    event_table: str = Field(description="table_name of the event-level table")
    event_duckdb_path: str = Field(description="exact duckdb_path of the event table")
    event_value_sql: str = Field(
        description=(
            "SQL expression over event-table columns whose per-period SUM should "
            'reproduce the measure (its movement), e.g. \'"debit" - "credit"\' or \'"amount"\''
        )
    )
    measure_key_sql: str = Field(
        description="SQL expression on the measure table identifying the entity, e.g. '\"account_id\"'"
    )
    event_key_sql: str = Field(
        description="SQL expression on the event table producing the SAME entity key"
    )
    measure_period_sql: str = Field(
        description=(
            "SQL expression on the measure table producing a comparable period key, "
            "e.g. '\"period\"'"
        )
    )
    event_period_sql: str = Field(
        description=(
            "SQL expression on the event table producing the SAME period key (the "
            "period bridge), e.g. 'strftime(\"date\", ''%Y-%m'')'"
        )
    )
    event_filter_sql: str | None = Field(
        default=None,
        description="optional WHERE condition on the event table, e.g. '\"status\" = ''posted'''",
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
