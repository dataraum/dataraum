"""Pydantic models for aggregation-lineage discovery (DAT-491).

The verdict shape only — discovery is deterministic arithmetic over the slice
substrate (no LLM proposal models since the slice-substrate rework)."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

# The reconciliation patterns. ``per_period`` ⇒ the measure IS each period's
# movement (a flow); ``cumulative`` ⇒ the measure carries forward (a stock).
PATTERN_PER_PERIOD = "per_period"
PATTERN_CUMULATIVE = "cumulative"


class CandidateDisposal(BaseModel):
    """The deterministic verdict on one candidate (only reconciled ones persist)."""

    pattern: Literal["per_period", "cumulative"]
    match_rate: float = Field(ge=0.0, le=1.0)
    r_flow_median: float
    r_stock_median: float
    n_entities: int
    n_entities_fired: int
