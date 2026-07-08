"""OpenTelemetry bootstrap for the worker process (ADR-0019 / DAT-705/707).

Traces and logs — metrics are cockpit-side (DAT-706). The single on/off switch
is ``OTEL_EXPORTER_OTLP_ENDPOINT`` (the OTLP vendor seam per ADR-0019): unset
or empty means the worker runs exactly as before and nothing here is
constructed.
"""

from __future__ import annotations

from dataclasses import dataclass

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from dataraum.core.logging import enable_otel_logging, get_logger
from dataraum.core.settings import Settings

logger = get_logger(__name__)


@dataclass
class TelemetryHandles:
    """The providers whose buffered telemetry the caller flushes on exit."""

    tracer_provider: TracerProvider
    logger_provider: LoggerProvider

    def shutdown(self) -> None:
        """Flush and stop both exporter pipelines (worker exit path)."""
        self.logger_provider.shutdown()
        self.tracer_provider.shutdown()


def init_telemetry(settings: Settings) -> TelemetryHandles | None:
    """Install the tracer provider and arm log shipping when OTLP is configured.

    Returns the provider handles — the caller owns ``shutdown()`` so buffered
    spans and log records flush on worker exit — or ``None`` when telemetry is
    off. The exporters are constructed without an explicit endpoint: they
    resolve ``OTEL_EXPORTER_OTLP_ENDPOINT`` themselves per the OTLP spec (base
    URL + ``/v1/traces`` / ``/v1/logs``), so the Settings field only gates
    construction and the URL semantics stay the SDK's.

    Args:
        settings: The validated process settings.
    """
    if not settings.otel_exporter_otlp_endpoint:
        return None
    resource = Resource.create(
        {
            "service.name": "dataraum-engine-worker",
            # One worker container per workspace (DAT-505) — the workspace
            # id distinguishes their telemetry in a multi-workspace stack.
            "service.instance.id": settings.dataraum_workspace_id,
        }
    )
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(tracer_provider)
    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter()))
    enable_otel_logging(logger_provider)
    # Emitted after arming, so this line is the first record shipped to Loki —
    # its presence there is the boot-time proof the logs pipeline works.
    logger.info("telemetry_enabled", otlp_endpoint=settings.otel_exporter_otlp_endpoint)
    return TelemetryHandles(tracer_provider=tracer_provider, logger_provider=logger_provider)
