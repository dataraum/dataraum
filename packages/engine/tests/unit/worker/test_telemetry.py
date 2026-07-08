"""Telemetry bootstrap gating (ADR-0019 / DAT-705/707).

The contract under test: ``OTEL_EXPORTER_OTLP_ENDPOINT`` is the single on/off
switch — unset/empty constructs nothing (the worker runs exactly as before);
set yields the tracer + logger providers carrying the worker's service
identity, with the structlog→OTel bridge armed.
"""

from __future__ import annotations

import pytest
from opentelemetry import trace
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk.trace import TracerProvider

import dataraum.core.logging as logging_module
from dataraum.core.settings import Settings
from dataraum.worker.telemetry import init_telemetry


def test_off_when_endpoint_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    assert init_telemetry(Settings()) is None  # type: ignore[call-arg]
    assert logging_module._otel_logger is None


def test_off_when_endpoint_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    # Compose interpolation can hand the var through as "" — that means off,
    # never a half-configured exporter.
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
    assert init_telemetry(Settings()) is None  # type: ignore[call-arg]
    assert logging_module._otel_logger is None


def test_providers_when_endpoint_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
    # Intercept the GLOBAL provider registration: opentelemetry treats
    # set_tracer_provider as once-per-process (an override only warns and is
    # ignored), so letting the test mutate real process-global state could
    # leak into any later test that reads trace.get_tracer_provider().
    # telemetry.py calls through this same `opentelemetry.trace` module object,
    # so patching the attribute here intercepts it; monkeypatch restores after.
    registered: list[trace.TracerProvider] = []
    monkeypatch.setattr(trace, "set_tracer_provider", registered.append)

    try:
        telemetry = init_telemetry(Settings())  # type: ignore[call-arg]
        assert telemetry is not None
        assert isinstance(telemetry.tracer_provider, TracerProvider)
        assert registered == [telemetry.tracer_provider]  # the process-global provider
        assert isinstance(telemetry.logger_provider, LoggerProvider)
        # The structlog→OTel bridge is armed against this provider's pipeline.
        assert logging_module._otel_logger is not None
        # Both signals carry the same service identity (Loki's service_name
        # label must match the Tempo spans for trace↔logs click-through).
        for provider in (telemetry.tracer_provider, telemetry.logger_provider):
            attrs = provider.resource.attributes
            assert attrs["service.name"] == "dataraum-engine-worker"
            # conftest pins DATARAUM_WORKSPACE_ID=test — per-workspace identity.
            assert attrs["service.instance.id"] == "test"
        telemetry.shutdown()
    finally:
        # enable_otel_logging set module state; never leak it into other tests.
        logging_module._otel_logger = None
