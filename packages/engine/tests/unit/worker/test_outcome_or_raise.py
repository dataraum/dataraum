"""The activity retry choke point: ``PhaseActivities`` failure classification.

Retryability rides the exception *type* (DAT-503): a transient
:class:`TransientProviderError` raised out of the phase body becomes the
retryable ``TransientPhaseFailure``; a deterministic FAILED ``PhaseRun`` and a
permanent provider failure become the non-retryable ``PhaseFailed``. The worker
classifies at this boundary — not by parsing a substring of the error string.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from temporalio.exceptions import ApplicationError

from dataraum.llm.providers.base import (
    PermanentProviderError,
    TransientProviderError,
)
from dataraum.pipeline.base import PhaseStatus
from dataraum.worker.activities import PhaseActivities, _provider_app_error
from dataraum.worker.activity import PhaseRun
from dataraum.worker.contracts import RunRef, SessionScopedInput
from dataraum.worker.workflows import _LLM_RETRY, _RETRY


def _acts() -> PhaseActivities:
    # ``_outcome_or_raise`` never touches the manager.
    return PhaseActivities(MagicMock())


def _failed(error: str) -> PhaseRun:
    return PhaseRun(status=PhaseStatus.FAILED.value, error=error)


def _identity() -> RunRef:
    return RunRef(workspace_id="ws", run_id="run")


# --- _provider_app_error: the typed-exception -> Temporal-error translation ---


def test_transient_provider_error_maps_to_retryable() -> None:
    err = _provider_app_error(TransientProviderError("429 rate limit"))
    assert isinstance(err, ApplicationError)
    assert err.type == "TransientPhaseFailure"
    assert err.non_retryable is False
    assert "429 rate limit" in str(err)
    # The retryable type must be absent from BOTH policies' non-retryable lists,
    # else Temporal would still refuse to retry it.
    assert "TransientPhaseFailure" not in (_RETRY.non_retryable_error_types or ())
    assert "TransientPhaseFailure" not in (_LLM_RETRY.non_retryable_error_types or ())


def test_permanent_provider_error_maps_to_non_retryable() -> None:
    err = _provider_app_error(PermanentProviderError("401 unauthorized"))
    assert err.type == "PhaseFailed"
    assert err.non_retryable is True
    assert "PhaseFailed" in (_LLM_RETRY.non_retryable_error_types or ())


# --- _outcome_or_raise: deterministic FAILED is permanent ---


def test_deterministic_failure_stays_non_retryable() -> None:
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


# --- the propagation seam: a typed ProviderError out of the phase body ---


def test_transient_provider_error_propagates_as_retryable(monkeypatch: pytest.MonkeyPatch) -> None:
    # A transient failure raised out of run_session_phase becomes retryable.
    def boom(*_a: object, **_k: object) -> PhaseRun:
        raise TransientProviderError("Anthropic 429 rate limit")

    monkeypatch.setattr("dataraum.worker.activities.run_session_phase", boom)
    with pytest.raises(ApplicationError) as ei:
        _acts().run_relationships(
            SessionScopedInput(run=_identity(), table_ids=["t1"], vertical="finance")
        )
    assert ei.value.type == "TransientPhaseFailure"
    assert ei.value.non_retryable is False


def test_permanent_provider_error_propagates_as_non_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(*_a: object, **_k: object) -> PhaseRun:
        raise PermanentProviderError("Anthropic 401 unauthorized. Check your ANTHROPIC_API_KEY.")

    monkeypatch.setattr("dataraum.worker.activities.run_session_phase", boom)
    with pytest.raises(ApplicationError) as ei:
        _acts().run_semantic_per_table(
            SessionScopedInput(run=_identity(), table_ids=["t1"], vertical="finance")
        )
    assert ei.value.type == "PhaseFailed"
    assert ei.value.non_retryable is True
    assert "ANTHROPIC_API_KEY" in str(ei.value)


def test_simulated_429_then_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Simulated-429 retry at the run_session_phase / classification seam (DAT-503).

    Attempt 1 raises a transient 429 → the activity raises the retryable
    ``TransientPhaseFailure`` (Temporal would re-run it). Attempt 2 succeeds →
    the activity returns the COMPLETED outcome. This proves the retryable
    classification end-to-end without a Temporal test server (which stalls CI).
    """
    calls = {"n": 0}

    def flaky(*_a: object, **_k: object) -> PhaseRun:
        calls["n"] += 1
        if calls["n"] == 1:
            raise TransientProviderError("Anthropic 429 rate limit")
        return PhaseRun(status=PhaseStatus.COMPLETED.value, summary="ok on retry")

    monkeypatch.setattr("dataraum.worker.activities.run_session_phase", flaky)
    acts = _acts()
    payload = SessionScopedInput(run=_identity(), table_ids=["t1"], vertical="finance")

    # Attempt 1: retryable failure.
    with pytest.raises(ApplicationError) as ei:
        acts.run_relationships(payload)
    assert ei.value.type == "TransientPhaseFailure"

    # Attempt 2 (Temporal's retry): success.
    outcome = acts.run_relationships(payload)
    assert outcome.status == PhaseStatus.COMPLETED.value
    assert outcome.summary == "ok on retry"
    assert calls["n"] == 2
