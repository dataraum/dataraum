"""Pipeline scheduler — generator-based reactive execution loop.

Runs phases in dependency order with concurrent execution for independent phases.
Post-step detectors run after each phase completes.
"""

from __future__ import annotations

from collections.abc import Callable, Generator, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.pipeline.base import Phase, PhaseContext, PhaseResult, PhaseStatus
from dataraum.pipeline.db_models import PhaseLog
from dataraum.pipeline.events import EventType, PipelineEvent

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


@dataclass
class PipelineResult:
    """Final result returned by the scheduler generator."""

    success: bool
    phases_completed: list[str]
    phases_failed: list[str]
    phases_skipped: list[str]
    phases_blocked: list[str]  # PENDING phases blocked by failed dependencies
    final_scores: dict[str, float]  # dimension_path -> score
    error: str | None = None


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------


class PipelineScheduler:
    """Generator-based reactive pipeline scheduler.

    Yields PipelineEvent objects for each lifecycle event.  Phases in
    the same dependency wave run concurrently when a session_factory
    is available.
    """

    def __init__(
        self,
        phases: Mapping[str, Phase],
        source_id: str,
        run_id: str,
        session: Session,
        duckdb_conn: Any,
        contract_thresholds: dict[str, float] | None = None,
        phase_configs: dict[str, dict[str, Any]] | None = None,
        runtime_config: dict[str, Any] | None = None,
        session_factory: Callable[[], Any] | None = None,
        manager: Any | None = None,
    ) -> None:
        # Validate that all declared dependencies reference known phases
        for name, phase in phases.items():
            unknown = [d for d in phase.dependencies if d not in phases]
            if unknown:
                raise ValueError(f"Phase {name!r} declares unknown dependencies: {unknown}")

        self.phases = phases
        self.source_id = source_id
        self.run_id = run_id
        self.session = session
        self.duckdb_conn = duckdb_conn
        self.contract_thresholds = contract_thresholds or {}
        self._phase_configs = phase_configs or {}
        self._runtime_config = runtime_config or {}
        self.session_factory = session_factory
        self.manager = manager
        # Internal state
        self._state: dict[str, PhaseStatus] = dict.fromkeys(phases, PhaseStatus.PENDING)
        self._step = 0

        # Validate analysis coverage: warn if detectors require analyses
        # that no phase produces
        self._validate_analysis_coverage()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> Generator[PipelineEvent, None, PipelineResult]:
        """Execute the pipeline as a generator.

        Phases in the same dependency wave run concurrently when a
        session_factory is available.

        Yields:
            PipelineEvent for each lifecycle event.

        Returns:
            PipelineResult summarising the run.
        """
        total = len(self.phases)
        yield self._event(EventType.PIPELINE_STARTED, total=total)

        while ready := self._ready_phases():
            # 1. Check should_skip on main thread
            to_run: list[str] = []
            for phase_name in ready:
                phase = self.phases[phase_name]
                ctx = self._build_context(phase_name)
                skip_reason = phase.should_skip(ctx)
                if skip_reason:
                    self._state[phase_name] = PhaseStatus.SKIPPED
                    self._write_phase_log(phase_name, "skipped", error=skip_reason)
                    yield self._event(
                        EventType.PHASE_SKIPPED,
                        phase=phase_name,
                        total=total,
                        message=skip_reason,
                    )
                else:
                    to_run.append(phase_name)

            if not to_run:
                continue

            # 2. Execute phases
            use_parallel = len(to_run) > 1 and self.session_factory is not None
            if use_parallel:
                yield from self._run_parallel(to_run, total)
            else:
                yield from self._run_sequential(to_run, total)

        yield self._event(EventType.PIPELINE_COMPLETED, total=total)

        return PipelineResult(
            success=not self._phases_with_status(PhaseStatus.FAILED),
            phases_completed=self._phases_with_status(PhaseStatus.COMPLETED),
            phases_failed=self._phases_with_status(PhaseStatus.FAILED),
            phases_skipped=self._phases_with_status(PhaseStatus.SKIPPED),
            phases_blocked=self._phases_with_status(PhaseStatus.PENDING),
            final_scores={},
        )

    def _run_phase(self, phase_name: str) -> tuple[PhaseResult, datetime]:
        """Execute a single phase, optionally with its own session.

        When session_factory is available, creates a per-phase session that
        auto-commits on exit (safe for threaded use). Falls back to the
        shared session when session_factory is None (unit tests).

        Returns:
            Tuple of (PhaseResult, started_at timestamp).
        """
        phase = self.phases[phase_name]
        started_at = datetime.now(UTC)
        logger.info("phase.start", phase=phase_name)

        if self.session_factory and self.manager:
            with (
                self.session_factory() as phase_session,
                self.manager.duckdb_cursor() as phase_cursor,
            ):
                config: dict[str, Any] = {}
                config.update(self._phase_configs.get(phase_name, {}))
                config.update(self._runtime_config)
                ctx = PhaseContext(
                    session=phase_session,
                    duckdb_conn=phase_cursor,
                    source_id=self.source_id,
                    config=config,
                    session_factory=self.session_factory,
                    manager=self.manager,
                )
                result = phase.run(ctx)
        else:
            ctx = self._build_context(phase_name)
            result = phase.run(ctx)

        logger.info(
            "phase.done",
            phase=phase_name,
            status=result.status.value,
            duration=result.duration_seconds,
        )

        return result, started_at

    def _record_phase(
        self, phase_name: str, result: PhaseResult, started_at: datetime, total: int
    ) -> Generator[PipelineEvent]:
        """Record phase result: update state, write log, run post-step detectors, yield events."""
        if result.status == PhaseStatus.FAILED:
            self._state[phase_name] = PhaseStatus.FAILED
            self._write_phase_log(
                phase_name,
                "failed",
                started_at=started_at,
                duration=result.duration_seconds,
                error=result.error,
            )
            yield self._event(
                EventType.PHASE_FAILED,
                phase=phase_name,
                total=total,
                error=result.error or "",
                duration_seconds=result.duration_seconds,
            )
            return

        self._state[phase_name] = PhaseStatus.COMPLETED
        self._write_phase_log(
            phase_name,
            "completed",
            started_at=started_at,
            duration=result.duration_seconds,
            outputs=result.outputs or None,
        )

        # Run post-step detectors declared in pipeline.yaml
        self._run_post_step_detectors(phase_name)

        yield self._event(
            EventType.PHASE_COMPLETED,
            phase=phase_name,
            total=total,
            duration_seconds=result.duration_seconds,
            records_processed=result.records_processed,
            records_created=result.records_created,
            warnings=result.warnings,
            outputs=result.outputs,
            summary=result.summary,
        )

    def _run_sequential(self, phase_names: list[str], total: int) -> Generator[PipelineEvent]:
        """Run phases sequentially, yield events."""
        for phase_name in phase_names:
            yield self._event(EventType.PHASE_STARTED, phase=phase_name, total=total)
            result, started_at = self._run_phase(phase_name)
            yield from self._record_phase(phase_name, result, started_at, total)

    def _run_parallel(self, phase_names: list[str], total: int) -> Generator[PipelineEvent]:
        """Run phases concurrently via ThreadPoolExecutor.

        PHASE_STARTED events are yielded for all phases before execution.
        Results are recorded on the main thread as they complete.
        """
        # Yield STARTED for all phases in this wave
        for phase_name in phase_names:
            yield self._event(EventType.PHASE_STARTED, phase=phase_name, total=total)

        max_workers = min(len(phase_names), 4)
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(self._run_phase, name): name for name in phase_names}
            for future in as_completed(futures):
                phase_name = futures[future]
                try:
                    result, started_at = future.result()
                except Exception as exc:
                    # Phase raised an unhandled exception
                    result = PhaseResult.failed(str(exc))
                    started_at = datetime.now(UTC)
                yield from self._record_phase(phase_name, result, started_at, total)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _event(self, event_type: EventType, **kwargs: Any) -> PipelineEvent:
        """Create a PipelineEvent, auto-incrementing the step counter."""
        self._step += 1
        return PipelineEvent(event_type=event_type, step=self._step, **kwargs)

    def _phases_with_status(self, status: PhaseStatus) -> list[str]:
        """Return phase names matching the given status."""
        return [n for n, s in self._state.items() if s == status]

    def _ready_phases(self) -> list[str]:
        """Return PENDING phases whose dependencies are all completed or skipped.

        Results are sorted by dependency order (phases with fewer unresolved
        dependencies first) for deterministic execution.
        """
        ready = []
        for name, status in self._state.items():
            if status != PhaseStatus.PENDING:
                continue
            phase = self.phases[name]
            deps_satisfied = all(
                self._state.get(dep) in (PhaseStatus.COMPLETED, PhaseStatus.SKIPPED)
                for dep in phase.dependencies
            )
            if deps_satisfied:
                ready.append(name)

        # Sort by number of dependencies (topological-ish) for determinism
        ready.sort(key=lambda n: len(self.phases[n].dependencies))
        return ready

    def _build_context(self, phase_name: str = "") -> PhaseContext:
        """Build a PhaseContext from current state."""
        config: dict[str, Any] = {}
        if phase_name:
            config.update(self._phase_configs.get(phase_name, {}))
        config.update(self._runtime_config)
        return PhaseContext(
            session=self.session,
            duckdb_conn=self.duckdb_conn,
            source_id=self.source_id,
            config=config,
            session_factory=self.session_factory,
            manager=self.manager,
        )

    def _write_phase_log(
        self,
        phase_name: str,
        status: str,
        *,
        started_at: datetime | None = None,
        duration: float = 0.0,
        error: str | None = None,
        outputs: dict[str, Any] | None = None,
    ) -> None:
        """Write a PhaseLog record."""
        now = datetime.now(UTC)
        log = PhaseLog(
            run_id=self.run_id,
            source_id=self.source_id,
            phase_name=phase_name,
            status=status,
            started_at=started_at or now,
            completed_at=now,
            duration_seconds=duration,
            error=error,
            outputs=outputs,
        )
        self.session.add(log)
        self.session.commit()

    def _validate_analysis_coverage(self) -> None:
        """Warn if detectors require analyses that no phase produces."""
        from dataraum.entropy.detectors.base import get_default_registry
        from dataraum.entropy.dimensions import AnalysisKey

        all_produced: set[AnalysisKey] = set()
        for phase in self.phases.values():
            all_produced.update(phase.produces_analyses)

        detector_registry = get_default_registry()
        for detector in detector_registry.get_all_detectors():
            missing = [str(a) for a in detector.required_analyses if a not in all_produced]
            if missing:
                logger.warning(
                    "detector_analysis_gap",
                    detector=detector.detector_id,
                    missing=missing,
                    message=(
                        f"Detector '{detector.detector_id}' requires "
                        f"[{', '.join(missing)}] but no configured phase "
                        f"produces them — it will never run"
                    ),
                )

    def _run_post_step_detectors(self, phase_name: str) -> None:
        """Run detectors declared as post-steps for this phase.

        Called after a phase completes successfully on the main thread.
        Uses a fresh session from ``session_factory`` so detectors see
        data committed by the per-phase session. Falls back to
        ``self.session`` when no factory is available (unit tests).

        The session_factory context manager auto-commits on exit;
        self.session relies on the caller (scheduler loop) to commit.
        """
        from dataraum.entropy.engine import run_detector_post_step

        phase = self.phases[phase_name]
        if not phase.detectors:
            return

        if self.session_factory and self.manager:
            with (
                self.session_factory() as detector_session,
                self.manager.duckdb_cursor() as detector_cursor,
            ):
                for detector_id in phase.detectors:
                    run_detector_post_step(
                        detector_session, self.source_id, detector_id, detector_cursor
                    )
        else:
            for detector_id in phase.detectors:
                run_detector_post_step(self.session, self.source_id, detector_id, self.duckdb_conn)
