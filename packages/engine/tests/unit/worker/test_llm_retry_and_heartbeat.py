"""LLM retry policy + metrics heartbeat (DAT-503).

The LLM-calling activities get a backoff-shaped retry distinct from the default,
and the long-running ``metrics`` activity heartbeats from a background pulser so
worker death is detected at the short heartbeat window, not the 10-minute
start-to-close timeout.
"""

from __future__ import annotations

import threading
import time
from datetime import timedelta

from dataraum.worker.activities import _heartbeat_pulse
from dataraum.worker.workflows import (
    _HEARTBEAT_TIMEOUT,
    _LLM_PHASES,
    _LLM_RETRY,
    _RETRY,
    _retry_for,
)


def test_llm_retry_backs_off_to_at_least_60s_and_allows_more_attempts() -> None:
    # A real upstream outage must be ridden out across the LLM's Retry-After
    # windows, not given up after 5 fast tries.
    assert _LLM_RETRY.maximum_interval is not None
    assert _LLM_RETRY.maximum_interval >= timedelta(seconds=60)
    assert (_LLM_RETRY.maximum_attempts or 0) >= 8
    # A permanent failure must still short-circuit on both policies.
    assert "PhaseFailed" in (_LLM_RETRY.non_retryable_error_types or ())
    assert "PhaseFailed" in (_RETRY.non_retryable_error_types or ())
    # A transient failure must stay retryable.
    assert "TransientPhaseFailure" not in (_LLM_RETRY.non_retryable_error_types or ())


def test_retry_for_picks_llm_policy_only_for_llm_phases() -> None:
    for phase in (
        "semantic_per_column",
        "semantic_per_table",
        "slicing",
        "enriched_views",
        "validation",
        "business_cycles",
        "metrics",
    ):
        assert phase in _LLM_PHASES
        assert _retry_for(phase) is _LLM_RETRY
    for phase in (
        "relationships",
        "aggregation_lineage",
        "slicing_view",
        "slice_analysis",
        "temporal_slice_analysis",
        "correlations",
    ):
        assert _retry_for(phase) is _RETRY


def test_heartbeat_timeout_is_above_pulse_cadence() -> None:
    # A slow LLM wave between pulses must not trip the heartbeat timeout.
    assert _HEARTBEAT_TIMEOUT > timedelta(seconds=15)


def test_heartbeat_pulse_starts_and_stops_a_thread() -> None:
    # Outside an activity context activity.heartbeat() raises RuntimeError, which
    # the pulser swallows — so the context manager is a clean no-op-safe wrapper
    # that always tears its thread down. Use a tiny interval to exercise a beat.
    before = threading.active_count()
    with _heartbeat_pulse(interval=0.01):
        time.sleep(0.05)
    # Give the daemon a moment to exit after stop.set().
    time.sleep(0.05)
    assert threading.active_count() <= before
