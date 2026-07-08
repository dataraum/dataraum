"""Tests for pipeline base types and phase registry."""

from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.pipeline_config import load_phase_declarations
from dataraum.pipeline.registry import get_registry


class TestPhaseRegistry:
    """Tests for the phase registry."""

    def test_registry_not_empty(self):
        registry = get_registry()
        assert len(registry) > 0

    def test_all_phases_have_names(self):
        registry = get_registry()
        for name, cls in registry.items():
            instance = cls()
            assert instance.name == name
            assert isinstance(instance.name, str)

    def test_all_declared_phases_have_descriptions(self):
        declarations = load_phase_declarations()
        for name, decl in declarations.items():
            assert decl.description, f"Phase {name!r} has no description"
            assert isinstance(decl.description, str)


class TestBasePhaseProperties:
    """Tests for BasePhase runtime behavior."""

    def test_run_measures_duration(self):
        """BasePhase.run() sets duration_seconds on the result."""

        class SlowPhase(BasePhase):
            name = "slow"

            def _run(self, ctx: PhaseContext) -> PhaseResult:
                return PhaseResult.success(records_processed=1)

        from unittest.mock import MagicMock

        ctx = MagicMock(spec=PhaseContext)
        result = SlowPhase().run(ctx)
        assert result.duration_seconds > 0

    def test_run_measures_duration_on_failure(self):
        """BasePhase.run() sets duration even when _run raises."""

        class CrashPhase(BasePhase):
            name = "crash"

            def _run(self, ctx: PhaseContext) -> PhaseResult:
                raise RuntimeError("boom")

        from unittest.mock import MagicMock

        ctx = MagicMock(spec=PhaseContext)
        result = CrashPhase().run(ctx)
        assert result.status.value == "failed"
        assert result.duration_seconds > 0


class TestPhaseSpan:
    """BasePhase.run() wraps the phase body in one span (DAT-706)."""

    def _capture(self, monkeypatch):
        """Route the module tracer through an in-memory exporter."""
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
            InMemorySpanExporter,
        )

        import dataraum.pipeline.phases.base as module

        exporter = InMemorySpanExporter()
        tracer_provider = TracerProvider()
        tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
        monkeypatch.setattr(module, "tracer", tracer_provider.get_tracer("test"))
        return exporter

    def _ctx(self, run_id: str | None):
        from unittest.mock import MagicMock

        ctx = MagicMock(spec=PhaseContext)
        ctx.run_id = run_id
        return ctx

    def test_success_span_names_phase_and_run(self, monkeypatch):
        class OkPhase(BasePhase):
            name = "ok"

            def _run(self, ctx: PhaseContext) -> PhaseResult:
                return PhaseResult.success(records_processed=1)

        exporter = self._capture(monkeypatch)
        result = OkPhase().run(self._ctx("run-1"))

        assert result.status.value == "completed"
        (span,) = exporter.get_finished_spans()
        assert span.name == "phase ok"
        assert span.attributes["dataraum.phase"] == "ok"
        assert span.attributes["dataraum.run_id"] == "run-1"
        # Span brackets the same wall-clock window as duration_seconds.
        assert (span.end_time - span.start_time) / 1e9 >= result.duration_seconds

    def test_unset_run_id_omits_attribute(self, monkeypatch):
        class OkPhase(BasePhase):
            name = "ok"

            def _run(self, ctx: PhaseContext) -> PhaseResult:
                return PhaseResult.success()

        exporter = self._capture(monkeypatch)
        OkPhase().run(self._ctx(None))

        (span,) = exporter.get_finished_spans()
        assert "dataraum.run_id" not in span.attributes

    def test_failed_result_marks_span_error(self, monkeypatch):
        from opentelemetry.trace import StatusCode

        class CrashPhase(BasePhase):
            name = "crash"

            def _run(self, ctx: PhaseContext) -> PhaseResult:
                raise RuntimeError("boom")

        exporter = self._capture(monkeypatch)
        result = CrashPhase().run(self._ctx("run-1"))

        assert result.status.value == "failed"
        (span,) = exporter.get_finished_spans()
        assert span.status.status_code == StatusCode.ERROR
        assert "RuntimeError: boom" in span.status.description

    def test_provider_error_propagates_and_marks_span(self, monkeypatch):
        import pytest
        from opentelemetry.trace import StatusCode

        from dataraum.llm.providers.base import TransientProviderError

        class LlmPhase(BasePhase):
            name = "llm"

            def _run(self, ctx: PhaseContext) -> PhaseResult:
                raise TransientProviderError("rate limited")

        exporter = self._capture(monkeypatch)
        with pytest.raises(TransientProviderError):
            LlmPhase().run(self._ctx("run-1"))

        (span,) = exporter.get_finished_spans()
        assert span.status.status_code == StatusCode.ERROR
        assert any(e.name == "exception" for e in span.events)
