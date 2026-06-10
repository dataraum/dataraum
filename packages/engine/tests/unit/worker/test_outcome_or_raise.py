"""The activity retry choke point: ``PhaseActivities._outcome_or_raise``.

A FAILED phase is normally a deterministic, non-retryable ``PhaseFailed``. The
exception is a *transient* provider failure (an LLM 429 / 5xx / connection
error tagged by ``format_api_error``): that raises a retryable error so
Temporal re-runs the activity with backoff instead of surfacing on attempt 1.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from temporalio.exceptions import ApplicationError

from dataraum.llm.providers.base import (
    PERMANENT_ERROR_KIND,
    TRANSIENT_ERROR_KIND,
    format_api_error,
)
from dataraum.pipeline.base import PhaseStatus
from dataraum.worker.activities import PhaseActivities
from dataraum.worker.activity import PhaseRun
from dataraum.worker.workflows import _RETRY


def _acts() -> PhaseActivities:
    # ``_outcome_or_raise`` never touches the manager.
    return PhaseActivities(MagicMock())


def _failed(error: str) -> PhaseRun:
    return PhaseRun(status=PhaseStatus.FAILED.value, error=error)


def test_transient_failure_is_retryable() -> None:
    run = _failed(format_api_error("Anthropic", TRANSIENT_ERROR_KIND, "429 rate limit"))
    with pytest.raises(ApplicationError) as ei:
        _acts()._outcome_or_raise(run, "slicing")
    assert ei.value.non_retryable is False
    assert ei.value.type == "TransientPhaseFailure"
    # The retryable type must be absent from the policy's non-retryable list,
    # else Temporal would still refuse to retry it.
    assert "TransientPhaseFailure" not in (_RETRY.non_retryable_error_types or ())


def test_permanent_provider_failure_stays_non_retryable() -> None:
    run = _failed(format_api_error("Anthropic", PERMANENT_ERROR_KIND, "401 unauthorized"))
    with pytest.raises(ApplicationError) as ei:
        _acts()._outcome_or_raise(run, "semantic_per_table")
    assert ei.value.non_retryable is True
    assert ei.value.type == "PhaseFailed"
    assert "PhaseFailed" in (_RETRY.non_retryable_error_types or ())


def test_deterministic_failure_stays_non_retryable() -> None:
    # A non-provider failure (no transient tag) is deterministic → PhaseFailed.
    run = _failed("No typed tables found. Run typing phase first.")
    with pytest.raises(ApplicationError) as ei:
        _acts()._outcome_or_raise(run, "relationships")
    assert ei.value.non_retryable is True
    assert ei.value.type == "PhaseFailed"


def test_completed_run_returns_outcome() -> None:
    run = PhaseRun(status=PhaseStatus.COMPLETED.value, summary="2 drift summaries")
    outcome = _acts()._outcome_or_raise(run, "slicing")
    assert outcome.status == PhaseStatus.COMPLETED.value
    assert outcome.summary == "2 drift summaries"
