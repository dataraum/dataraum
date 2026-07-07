"""OpenTelemetry bootstrap for the worker process (ADR-0019 / DAT-705).

Tracing only — metrics and log shipping land with DAT-706/707. The single
on/off switch is ``OTEL_EXPORTER_OTLP_ENDPOINT`` (the OTLP vendor seam per
ADR-0019): unset or empty means the worker runs exactly as before and nothing
here is constructed.
"""

from __future__ import annotations

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from dataraum.core.logging import get_logger
from dataraum.core.settings import Settings

logger = get_logger(__name__)


def init_telemetry(settings: Settings) -> TracerProvider | None:
    """Install the global tracer provider when an OTLP endpoint is configured.

    Returns the provider — the caller owns ``shutdown()`` so buffered spans
    flush on worker exit — or ``None`` when telemetry is off. The exporter is
    constructed without an explicit endpoint: it resolves
    ``OTEL_EXPORTER_OTLP_ENDPOINT`` itself per the OTLP spec (base URL +
    ``/v1/traces``), so the Settings field only gates construction and the
    URL semantics stay the SDK's.

    Args:
        settings: The validated process settings.
    """
    if not settings.otel_exporter_otlp_endpoint:
        return None
    provider = TracerProvider(
        resource=Resource.create(
            {
                "service.name": "dataraum-engine-worker",
                # One worker container per workspace (DAT-505) — the workspace
                # id distinguishes their spans in a multi-workspace stack.
                "service.instance.id": settings.dataraum_workspace_id,
            }
        )
    )
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    logger.info("telemetry_enabled", otlp_endpoint=settings.otel_exporter_otlp_endpoint)
    return provider
