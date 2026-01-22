"""Pipeline orchestrator.

Runs phases in dependency order with parallel execution where possible.

Uses ThreadPoolExecutor for true parallel execution of CPU-bound phases.
Each phase runs in its own thread with its own SQLAlchemy session.
With free-threaded Python (python3.14t), this enables real parallelism.
"""

from __future__ import annotations

import time
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError, as_completed
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum_context.core.connections import ConnectionManager
from dataraum_context.core.logging import get_logger
from dataraum_context.pipeline.base import (
    PIPELINE_DAG,
    Phase,
    PhaseContext,
    PhaseResult,
    PhaseStatus,
    get_all_dependencies,
    get_phase_definition,
)
from dataraum_context.pipeline.db_models import PhaseCheckpoint, PipelineRun

logger = get_logger(__name__)


@dataclass
class PipelineConfig:
    """Configuration for pipeline execution."""

    max_parallel: int = 4
    fail_fast: bool = True
    skip_completed: bool = True
    skip_llm_phases: bool = False  # Skip phases that require LLM


@dataclass
class Pipeline:
    """Pipeline orchestrator.

    Manages phase execution with:
    - Dependency resolution
    - Parallel execution of independent phases
    - Checkpoint-based resume
    - Progress tracking
    """

    phases: dict[str, Phase] = field(default_factory=dict)
    config: PipelineConfig = field(default_factory=PipelineConfig)

    # Runtime state
    _completed: set[str] = field(default_factory=set)
    _running: set[str] = field(default_factory=set)
    _failed: set[str] = field(default_factory=set)
    _skipped: set[str] = field(default_factory=set)
    _outputs: dict[str, dict[str, Any]] = field(default_factory=dict)

    def register(self, phase: Phase) -> None:
        """Register a phase implementation."""
        self.phases[phase.name] = phase

    def get_phases_to_run(self, target_phase: str | None = None) -> list[str]:
        """Get phases to run in dependency order.

        Args:
            target_phase: If set, only run this phase and its dependencies.
                         If None, run all phases.

        Returns:
            List of phase names in execution order.
        """
        if target_phase:
            # Get target + all dependencies
            deps = get_all_dependencies(target_phase)
            deps.add(target_phase)
            phases = [p.name for p in PIPELINE_DAG if p.name in deps]
        else:
            phases = [p.name for p in PIPELINE_DAG]

        return phases

    def run(
        self,
        manager: ConnectionManager,
        source_id: str,
        table_ids: list[str] | None = None,
        target_phase: str | None = None,
        run_config: dict[str, Any] | None = None,
    ) -> dict[str, PhaseResult]:
        """Run the pipeline.

        Uses ThreadPoolExecutor for true parallel execution of CPU-bound phases.
        Each phase runs in its own thread with its own session.
        With free-threaded Python (python3.14t), this enables real parallelism.

        Args:
            manager: Connection manager for database access
            source_id: Source identifier
            table_ids: Optional list of table IDs to process
            target_phase: Optional target phase (runs phase + dependencies)
            run_config: Optional configuration overrides

        Returns:
            Dict mapping phase names to their results
        """
        # Reset state
        self._completed = set()
        self._running = set()
        self._failed = set()
        self._skipped = set()
        self._outputs = {}

        # Create pipeline run record (needs its own session)
        # Generate run_id explicitly since SQLAlchemy defaults only apply at INSERT time
        run_id = str(uuid4())
        with manager.session_scope() as session:
            run = PipelineRun(
                run_id=run_id,
                source_id=source_id,
                target_phase=target_phase,
                config=run_config or {},
            )
            session.add(run)

            # Load existing checkpoints if resuming
            if self.config.skip_completed:
                self._load_completed_checkpoints(session, source_id)

        # Get phases to run
        phases_to_run = self.get_phases_to_run(target_phase)
        results: dict[str, PhaseResult] = {}

        start_time = time.time()

        try:
            with ThreadPoolExecutor(max_workers=self.config.max_parallel) as pool:
                # Track all active futures
                active_futures: dict[Future[PhaseResult], str] = {}

                while not self._is_complete(phases_to_run):
                    # Fill available slots with ready phases
                    available_slots = self.config.max_parallel - len(active_futures)
                    if available_slots > 0:
                        ready = self._get_ready_phases(phases_to_run)
                        batch = list(ready)[:available_slots]

                        for name in batch:
                            self._running.add(name)
                            future = pool.submit(
                                self._run_phase,
                                name,
                                manager,
                                source_id,
                                table_ids or [],
                                run_id,
                                run_config or {},
                                self._outputs.copy(),
                            )
                            active_futures[future] = name
                            logger.info(f"Started phase: {name} (running: {len(active_futures)})")

                    if not active_futures:
                        # No phases running and none ready - deadlock or done
                        break

                    # Wait for at least one future to complete (with timeout for responsiveness)
                    done_futures: set[Future[PhaseResult]] = set()
                    try:
                        for future in as_completed(active_futures.keys(), timeout=0.5):
                            done_futures.add(future)
                            # Process one at a time so we can fill slots quickly
                            break
                    except TimeoutError:
                        # No futures completed yet, loop back to check for new ready phases
                        continue

                    # Process completed futures
                    for future in done_futures:
                        name = active_futures.pop(future)
                        self._running.discard(name)

                        try:
                            phase_result = future.result()
                        except Exception as e:
                            phase_result = PhaseResult.failed(str(e))

                        results[name] = phase_result

                        if phase_result.status == PhaseStatus.COMPLETED:
                            self._completed.add(name)
                            self._outputs[name] = phase_result.outputs
                            logger.info(
                                f"Phase {name} completed in {phase_result.duration_seconds:.1f}s"
                            )
                            # Log any warnings from completed phase
                            if phase_result.warnings:
                                for warning in phase_result.warnings:
                                    logger.warning(f"Phase {name}: {warning}")
                        elif phase_result.status == PhaseStatus.SKIPPED:
                            self._skipped.add(name)
                            logger.info(f"Phase {name} skipped: {phase_result.error}")
                        else:
                            self._failed.add(name)
                            logger.error(f"Phase {name} failed: {phase_result.error}")
                            # Log any warnings from failed phase
                            if phase_result.warnings:
                                for warning in phase_result.warnings:
                                    logger.warning(f"Phase {name}: {warning}")
                            if self.config.fail_fast:
                                # Cancel remaining futures
                                for f in active_futures:
                                    f.cancel()
                                active_futures.clear()
                                break

                    if self.config.fail_fast and self._failed:
                        break

            # Update run record with final status
            with manager.session_scope() as session:
                from sqlalchemy import update

                status = "completed" if not self._failed else "failed"
                phases_completed = sum(
                    1 for r in results.values() if r.status == PhaseStatus.COMPLETED
                )
                phases_failed = sum(1 for r in results.values() if r.status == PhaseStatus.FAILED)
                phases_skipped = sum(1 for r in results.values() if r.status == PhaseStatus.SKIPPED)
                error_msg = None
                if self._failed:
                    failed_name = next(iter(self._failed))
                    if failed_name in results:
                        error_msg = results[failed_name].error

                stmt = (
                    update(PipelineRun)
                    .where(PipelineRun.run_id == run_id)
                    .values(
                        status=status,
                        completed_at=datetime.now(UTC),
                        total_duration_seconds=time.time() - start_time,
                        phases_completed=phases_completed,
                        phases_failed=phases_failed,
                        phases_skipped=phases_skipped,
                        error=error_msg,
                    )
                )
                session.execute(stmt)

        except Exception as e:
            # Update run record with error
            with manager.session_scope() as session:
                from sqlalchemy import update

                stmt = (
                    update(PipelineRun)
                    .where(PipelineRun.run_id == run_id)
                    .values(
                        status="failed",
                        completed_at=datetime.now(UTC),
                        total_duration_seconds=time.time() - start_time,
                        error=str(e),
                    )
                )
                session.execute(stmt)
            raise

        return results

    def _run_phase(
        self,
        phase_name: str,
        manager: ConnectionManager,
        source_id: str,
        table_ids: list[str],
        run_id: str,
        run_config: dict[str, Any],
        previous_outputs: dict[str, dict[str, Any]],
    ) -> PhaseResult:
        """Run a single phase in its own thread with its own session.

        This method is called from ThreadPoolExecutor. Each phase runs in
        its own thread with its own SQLAlchemy session and DuckDB cursor.

        Args:
            phase_name: Name of the phase to run
            manager: Connection manager for database access
            source_id: Source identifier
            table_ids: List of table IDs to process
            run_id: Pipeline run ID
            run_config: Runtime configuration
            previous_outputs: Outputs from previous phases

        Returns:
            PhaseResult from the phase execution
        """
        start_time = time.time()
        started_at = datetime.now(UTC)

        # Get phase implementation
        phase = self.phases.get(phase_name)
        if not phase:
            return PhaseResult.failed(f"No implementation registered for phase: {phase_name}")

        try:
            # Each phase gets its own session and DuckDB cursor
            # Sessions are thread-safe when each thread has its own
            # DuckDB cursors are thread-safe for reads
            with manager.session_scope() as session:
                with manager.duckdb_cursor() as cursor:
                    # Build context with this phase's session and cursor
                    ctx = PhaseContext(
                        session=session,
                        duckdb_conn=cursor,
                        source_id=source_id,
                        table_ids=table_ids,
                        previous_outputs=previous_outputs,
                        config=run_config,
                        session_factory=manager.session_scope,
                    )

                    # Check if should skip
                    skip_reason = phase.should_skip(ctx)
                    if skip_reason:
                        result = PhaseResult.skipped(skip_reason)
                    else:
                        # Run the phase
                        result = phase.run(ctx)
                        result.duration_seconds = time.time() - start_time

                # Save checkpoint (outside cursor context, inside session)
                checkpoint = PhaseCheckpoint(
                    run_id=run_id,
                    source_id=source_id,
                    phase_name=phase_name,
                    status=result.status.value,
                    started_at=started_at,
                    completed_at=datetime.now(UTC),
                    duration_seconds=result.duration_seconds,
                    outputs=result.outputs,
                    records_processed=result.records_processed,
                    records_created=result.records_created,
                    error=result.error,
                    warnings=result.warnings,
                )
                session.add(checkpoint)
                # session.commit() happens automatically in session_scope()

        except Exception as e:
            result = PhaseResult.failed(str(e), duration=time.time() - start_time)
            # Save failed checkpoint
            try:
                with manager.session_scope() as session:
                    checkpoint = PhaseCheckpoint(
                        run_id=run_id,
                        source_id=source_id,
                        phase_name=phase_name,
                        status="failed",
                        started_at=started_at,
                        completed_at=datetime.now(UTC),
                        duration_seconds=result.duration_seconds,
                        error=str(e),
                    )
                    session.add(checkpoint)
            except Exception:
                pass  # Don't mask the original error

        return result

    def _load_completed_checkpoints(self, session: Session, source_id: str) -> None:
        """Load previously completed checkpoints."""
        stmt = select(PhaseCheckpoint).where(
            PhaseCheckpoint.source_id == source_id,
            PhaseCheckpoint.status == "completed",
        )
        result = session.execute(stmt)
        checkpoints = result.scalars().all()

        for cp in checkpoints:
            self._completed.add(cp.phase_name)
            self._outputs[cp.phase_name] = cp.outputs or {}

    def _is_complete(self, phases_to_run: list[str]) -> bool:
        """Check if all phases are done."""
        done = self._completed | self._failed | self._skipped
        return all(p in done for p in phases_to_run)

    def _get_ready_phases(self, phases_to_run: list[str]) -> set[str]:
        """Get phases whose dependencies are satisfied."""
        ready = set()
        done = self._completed | self._skipped

        for name in phases_to_run:
            if name in self._completed or name in self._running or name in self._failed:
                continue
            if name in self._skipped:
                continue

            phase_def = get_phase_definition(name)
            if not phase_def:
                continue

            # Check if phase has an implementation
            if name not in self.phases:
                logger.info(f"Phase {name} skipped: no implementation registered")
                self._skipped.add(name)
                continue

            # Check if LLM phase should be skipped
            if self.config.skip_llm_phases and phase_def.requires_llm:
                logger.info(f"Phase {name} skipped: LLM phase (skip_llm_phases=True)")
                self._skipped.add(name)
                continue

            # Check dependencies
            deps_met = all(d in done for d in phase_def.dependencies)

            # Check if any dependency failed (can't run this phase)
            failed_deps = [d for d in phase_def.dependencies if d in self._failed]
            if failed_deps:
                logger.warning(f"Phase {name} blocked: dependencies failed: {failed_deps}")
                self._skipped.add(name)
                continue

            if deps_met:
                ready.add(name)
            else:
                # Some dependencies still running - phase will wait
                pending_deps = [d for d in phase_def.dependencies if d not in done]
                logger.debug(f"Phase {name} waiting for dependencies: {pending_deps}")

        return ready


# Global pipeline instance
_pipeline: Pipeline | None = None


def get_pipeline() -> Pipeline:
    """Get the global pipeline instance."""
    global _pipeline
    if _pipeline is None:
        _pipeline = Pipeline()
        _register_builtin_phases(_pipeline)
    return _pipeline


def _register_builtin_phases(pipeline: Pipeline) -> None:
    """Register all built-in phase implementations."""
    from dataraum_context.pipeline.phases import (
        CorrelationsPhase,
        ImportPhase,
        RelationshipsPhase,
        StatisticalQualityPhase,
        StatisticsPhase,
        TemporalPhase,
        TypingPhase,
    )

    pipeline.register(ImportPhase())
    pipeline.register(TypingPhase())
    pipeline.register(StatisticsPhase())
    pipeline.register(StatisticalQualityPhase())
    pipeline.register(RelationshipsPhase())
    pipeline.register(CorrelationsPhase())
    pipeline.register(TemporalPhase())


def run_pipeline(
    manager: ConnectionManager,
    source_id: str,
    table_ids: list[str] | None = None,
    target_phase: str | None = None,
    config: PipelineConfig | None = None,
    run_config: dict[str, Any] | None = None,
) -> dict[str, PhaseResult]:
    """Run the pipeline.

    Convenience function that uses the global pipeline instance.

    Args:
        manager: Connection manager for database access
        source_id: Source identifier
        table_ids: Optional list of table IDs
        target_phase: Optional target phase (runs phase + dependencies)
        config: Pipeline configuration
        run_config: Runtime configuration overrides

    Returns:
        Dict mapping phase names to their results
    """
    pipeline = get_pipeline()
    if config:
        pipeline.config = config

    return pipeline.run(
        manager=manager,
        source_id=source_id,
        table_ids=table_ids,
        target_phase=target_phase,
        run_config=run_config,
    )
