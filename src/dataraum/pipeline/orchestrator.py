"""Pipeline orchestrator.

Runs phases in dependency order with parallel execution where possible.

Uses ThreadPoolExecutor for true parallel execution of CPU-bound phases.
Each phase runs in its own thread with its own SQLAlchemy session.
With free-threaded Python (python3.14t), this enables real parallelism.
"""

from __future__ import annotations

import math
import time
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError, as_completed
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from dataraum.core.connections import ConnectionManager
from dataraum.core.logging import (
    end_phase_metrics,
    end_pipeline_metrics,
    get_logger,
    start_phase_metrics,
    start_pipeline_metrics,
)
from dataraum.pipeline.base import (
    PIPELINE_DAG,
    Phase,
    PhaseContext,
    PhaseResult,
    PhaseStatus,
    get_all_dependencies,
    get_phase_definition,
)
from dataraum.pipeline.db_models import PhaseCheckpoint, PipelineRun

logger = get_logger(__name__)


def _sanitize_for_json(obj: Any) -> Any:
    """Recursively sanitize values for JSON serialization.

    Handles:
    - datetime/date -> ISO format strings
    - numpy NaN/Inf -> None (JSON doesn't support these)
    - numpy types -> Python native types
    - Nested dicts and lists
    """
    if obj is None:
        return None

    # Handle datetime and date
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()

    # Handle float special values (NaN, Inf)
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj

    # Handle numpy types (if numpy is available)
    type_name = type(obj).__name__
    module_name = type(obj).__module__

    # numpy scalar types
    if module_name == "numpy":
        # numpy.nan, numpy.inf
        if type_name in ("float64", "float32", "float16"):
            val = float(obj)
            if math.isnan(val) or math.isinf(val):
                return None
            return val
        # numpy integer types
        if "int" in type_name:
            return int(obj)
        # numpy bool
        if type_name == "bool_":
            return bool(obj)
        # numpy datetime64
        if type_name == "datetime64":
            return str(obj)

    # Handle dicts recursively
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}

    # Handle lists/tuples recursively
    if isinstance(obj, (list, tuple)):
        return [_sanitize_for_json(item) for item in obj]

    # Default: return as-is (str, int, bool, etc.)
    return obj


@dataclass
class PipelineConfig:
    """Configuration for pipeline execution."""

    max_parallel: int = 4
    fail_fast: bool = True
    skip_completed: bool = True


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
    _logged_waiting: set[str] = field(default_factory=set)  # Track phases we've logged waiting for

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
        run_id: str | None = None,
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
            run_id: Optional run ID (generated if not provided)

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
        # Generate run_id if not provided
        if run_id is None:
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

        # Start pipeline metrics collection (sets context var)
        start_pipeline_metrics(run_id=run_id, source_id=source_id)

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
                            # Clear logged_waiting so pending phases log their deps again
                            self._logged_waiting.clear()
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

            # End pipeline metrics collection
            final_metrics = end_pipeline_metrics()

            # Update run record with final status and aggregate metrics
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

                # Calculate aggregate metrics from collected phase metrics
                total_llm_calls = 0
                total_llm_input_tokens = 0
                total_llm_output_tokens = 0
                total_tables_processed = 0
                total_rows_processed = 0

                if final_metrics:
                    for pm in final_metrics.phases:
                        total_llm_calls += pm.llm_calls
                        total_llm_input_tokens += pm.llm_input_tokens
                        total_llm_output_tokens += pm.llm_output_tokens
                        total_tables_processed += pm.tables_processed
                        total_rows_processed += pm.rows_processed

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
                        total_llm_calls=total_llm_calls,
                        total_llm_input_tokens=total_llm_input_tokens,
                        total_llm_output_tokens=total_llm_output_tokens,
                        total_tables_processed=total_tables_processed,
                        total_rows_processed=total_rows_processed,
                        error=error_msg,
                    )
                )
                session.execute(stmt)

        except Exception as e:
            # End pipeline metrics even on failure
            end_pipeline_metrics()
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
        """Run a single phase with retry on transient SQLite errors.

        Wraps _execute_phase with retry logic. Each retry gets a completely
        fresh session and DuckDB cursor, which is the correct recovery for
        both SQLITE_BUSY (busy_timeout exceeded) and SQLITE_BUSY_SNAPSHOT.

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
        max_retries = 2
        for attempt in range(max_retries + 1):
            try:
                return self._execute_phase(
                    phase_name,
                    manager,
                    source_id,
                    table_ids,
                    run_id,
                    run_config,
                    previous_outputs,
                )
            except OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries:
                    wait = 2.0 * (2**attempt)  # 2s, 4s
                    logger.warning(
                        f"Phase {phase_name} SQLite contention "
                        f"(attempt {attempt + 1}/{max_retries + 1}), "
                        f"retrying in {wait}s"
                    )
                    time.sleep(wait)
                    continue
                raise
        # Unreachable, but satisfies type checker
        return PhaseResult.failed("Retry logic error")  # pragma: no cover

    def _execute_phase(
        self,
        phase_name: str,
        manager: ConnectionManager,
        source_id: str,
        table_ids: list[str],
        run_id: str,
        run_config: dict[str, Any],
        previous_outputs: dict[str, dict[str, Any]],
    ) -> PhaseResult:
        """Execute a single phase in its own thread with its own session.

        This method is called from _run_phase (possibly with retries).
        Each invocation gets its own SQLAlchemy session and DuckDB cursor.

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

        # Start collecting phase metrics (sets context var)
        start_phase_metrics(phase_name)

        # Get phase implementation
        phase = self.phases.get(phase_name)
        if not phase:
            end_phase_metrics()  # Clean up metrics context
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
                        manager=manager,
                    )

                    # Check if should skip
                    skip_reason = phase.should_skip(ctx)
                    if skip_reason:
                        result = PhaseResult.skipped(skip_reason)
                    else:
                        # Run the phase
                        result = phase.run(ctx)
                        result.duration_seconds = time.time() - start_time

                # End phase metrics collection and get the data
                collected_metrics = end_phase_metrics()

                # Save checkpoint with detailed metrics (outside cursor context, inside session)
                checkpoint = PhaseCheckpoint(
                    run_id=run_id,
                    source_id=source_id,
                    phase_name=phase_name,
                    status=result.status.value,
                    started_at=started_at,
                    completed_at=datetime.now(UTC),
                    duration_seconds=result.duration_seconds,
                    outputs=_sanitize_for_json(result.outputs),
                    records_processed=result.records_processed,
                    records_created=result.records_created,
                    # Detailed metrics from PhaseMetrics
                    tables_processed=collected_metrics.tables_processed if collected_metrics else 0,
                    columns_processed=collected_metrics.columns_processed
                    if collected_metrics
                    else 0,
                    rows_processed=collected_metrics.rows_processed if collected_metrics else 0,
                    llm_calls=collected_metrics.llm_calls if collected_metrics else 0,
                    llm_input_tokens=collected_metrics.llm_input_tokens if collected_metrics else 0,
                    llm_output_tokens=collected_metrics.llm_output_tokens
                    if collected_metrics
                    else 0,
                    db_queries=collected_metrics.db_queries if collected_metrics else 0,
                    db_writes=collected_metrics.db_writes if collected_metrics else 0,
                    timings=_sanitize_for_json(
                        collected_metrics.timings if collected_metrics else {}
                    ),
                    error=result.error,
                    warnings=result.warnings,
                )
                session.add(checkpoint)
                # session.commit() happens automatically in session_scope()

        except Exception as e:
            result = PhaseResult.failed(str(e), duration=time.time() - start_time)
            # End phase metrics even on failure
            collected_metrics = end_phase_metrics()
            # Save failed checkpoint with any collected metrics
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
                        # Include any metrics collected before failure
                        tables_processed=collected_metrics.tables_processed
                        if collected_metrics
                        else 0,
                        columns_processed=collected_metrics.columns_processed
                        if collected_metrics
                        else 0,
                        rows_processed=collected_metrics.rows_processed if collected_metrics else 0,
                        llm_calls=collected_metrics.llm_calls if collected_metrics else 0,
                        llm_input_tokens=collected_metrics.llm_input_tokens
                        if collected_metrics
                        else 0,
                        llm_output_tokens=collected_metrics.llm_output_tokens
                        if collected_metrics
                        else 0,
                        db_queries=collected_metrics.db_queries if collected_metrics else 0,
                        db_writes=collected_metrics.db_writes if collected_metrics else 0,
                        timings=_sanitize_for_json(
                            collected_metrics.timings if collected_metrics else {}
                        ),
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
                # Clear from logged set if it was waiting before
                self._logged_waiting.discard(name)
            else:
                # Only log once per phase when it starts waiting
                if name not in self._logged_waiting:
                    pending_deps = [d for d in phase_def.dependencies if d not in done]
                    logger.debug(f"Phase {name} waiting for dependencies: {pending_deps}")
                    self._logged_waiting.add(name)

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
    from dataraum.pipeline.phases import (
        BusinessCyclesPhase,
        ColumnEligibilityPhase,
        CorrelationsPhase,
        CrossTableQualityPhase,
        EntropyInterpretationPhase,
        EntropyPhase,
        GraphExecutionPhase,
        ImportPhase,
        QualitySummaryPhase,
        RelationshipsPhase,
        SemanticPhase,
        SliceAnalysisPhase,
        SlicingPhase,
        StatisticalQualityPhase,
        StatisticsPhase,
        TemporalPhase,
        TemporalSliceAnalysisPhase,
        TypingPhase,
        ValidationPhase,
    )

    pipeline.register(ImportPhase())
    pipeline.register(TypingPhase())
    pipeline.register(StatisticsPhase())
    pipeline.register(ColumnEligibilityPhase())  # After statistics, before correlations
    pipeline.register(StatisticalQualityPhase())
    pipeline.register(RelationshipsPhase())
    pipeline.register(CorrelationsPhase())
    pipeline.register(TemporalPhase())
    pipeline.register(SemanticPhase())
    pipeline.register(SlicingPhase())
    pipeline.register(SliceAnalysisPhase())
    pipeline.register(QualitySummaryPhase())
    pipeline.register(TemporalSliceAnalysisPhase())
    pipeline.register(BusinessCyclesPhase())
    pipeline.register(CrossTableQualityPhase())
    pipeline.register(EntropyPhase())
    pipeline.register(EntropyInterpretationPhase())
    pipeline.register(GraphExecutionPhase())
    pipeline.register(ValidationPhase())


def run_pipeline(
    manager: ConnectionManager,
    source_id: str,
    table_ids: list[str] | None = None,
    target_phase: str | None = None,
    config: PipelineConfig | None = None,
    run_config: dict[str, Any] | None = None,
    run_id: str | None = None,
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
        run_id: Optional run ID (generated if not provided)

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
        run_id=run_id,
    )
