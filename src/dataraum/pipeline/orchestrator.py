"""Pipeline orchestrator.

Runs phases in dependency order with parallel execution where possible.

Uses ThreadPoolExecutor for true parallel execution of CPU-bound phases.
Each phase runs in its own thread with its own SQLAlchemy session.
With free-threaded Python (python3.14t), this enables real parallelism.
"""

from __future__ import annotations

import math
import time
from collections import deque
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError, as_completed
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import select, update
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
    Phase,
    PhaseContext,
    PhaseResult,
    PhaseStatus,
)
from dataraum.pipeline.db_models import PhaseCheckpoint, PipelineRun
from dataraum.pipeline.entropy_state import PipelineEntropyState
from dataraum.pipeline.registry import get_all_dependencies, get_registry

logger = get_logger(__name__)

# Sync callback: (current_step, total_steps, message) -> None
ProgressCallback = Callable[[int, int, str], None]


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
    max_retries: int = 2
    backoff_base: float = 2.0
    gate_mode: str = "skip"  # "skip", "pause", "fail"


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
    _gate_blocked: set[str] = field(default_factory=set)
    _outputs: dict[str, dict[str, Any]] = field(default_factory=dict)
    _phase_priority: dict[str, int] = field(default_factory=dict)  # Transitive dependent count
    _entropy_state: PipelineEntropyState = field(default_factory=PipelineEntropyState)

    def register(self, phase: Phase) -> None:
        """Register a phase implementation."""
        self.phases[phase.name] = phase

    def _compute_phase_priority(self) -> None:
        """Compute priority for each phase based on transitive dependent count.

        Phases that unblock more downstream work get higher priority.
        This ensures critical-path phases get worker slots first when
        multiple phases are ready simultaneously.
        """
        # Build reverse dependency graph: phase -> set of phases that depend on it
        reverse_deps: dict[str, set[str]] = {name: set() for name in self.phases}
        for name, phase in self.phases.items():
            for dep in phase.dependencies:
                if dep in reverse_deps:
                    reverse_deps[dep].add(name)

        # Count transitive dependents for each phase via BFS
        for name in self.phases:
            visited: set[str] = set()
            queue = list(reverse_deps.get(name, set()))
            while queue:
                current = queue.pop()
                if current in visited:
                    continue
                visited.add(current)
                queue.extend(reverse_deps.get(current, set()) - visited)
            self._phase_priority[name] = len(visited)

    @staticmethod
    def _notify_progress(
        callback: ProgressCallback | None,
        current: int,
        total: int,
        message: str,
    ) -> None:
        """Send a progress notification, swallowing any errors."""
        if callback is None:
            return
        try:
            callback(current, total, message)
        except Exception:
            pass  # Never let progress reporting crash the pipeline

    def get_phases_to_run(self, target_phase: str | None = None) -> list[str]:
        """Get phases to run based on registered phases.

        Args:
            target_phase: If set, only run this phase and its dependencies.
                         If None, run all registered phases.

        Returns:
            List of phase names to execute.
        """
        if target_phase:
            # Get target + all transitive dependencies, filtered to registered
            deps = get_all_dependencies(target_phase)
            deps.add(target_phase)
            return [name for name in self.phases if name in deps]
        else:
            return list(self.phases.keys())

    def run(
        self,
        manager: ConnectionManager,
        source_id: str,
        table_ids: list[str] | None = None,
        target_phase: str | None = None,
        phase_configs: dict[str, dict[str, Any]] | None = None,
        runtime_config: dict[str, Any] | None = None,
        run_id: str | None = None,
        progress_callback: ProgressCallback | None = None,
        force_phase: bool = False,
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
            phase_configs: Per-phase config dicts keyed by phase name.
                Each phase gets its own scoped config from this mapping.
            runtime_config: Runtime overrides (source_path, source_name)
                merged into every phase's config.
            run_id: Optional run ID (generated if not provided)
            progress_callback: Optional callback for progress notifications.
                Called with (current_step, total_steps, message).
            force_phase: If True, force re-run of target_phase by cleaning
                up its previous output and bypassing skip logic.

        Returns:
            Dict mapping phase names to their results
        """
        # Store phase configs for use in _execute_phase
        self._phase_configs = phase_configs or {}
        self._runtime_config = runtime_config or {}
        self._force_target = target_phase if force_phase else None

        # Reset state
        self._completed = set()
        self._running = set()
        self._failed = set()
        self._skipped = set()
        self._gate_blocked = set()
        self._outputs = {}
        self._entropy_state = PipelineEntropyState()

        # Build a serializable record of the full config for the DB
        stored_config = {
            "phase_configs": self._phase_configs,
            "runtime_config": self._runtime_config,
        }

        # Create pipeline run record (needs its own session)
        # Generate run_id if not provided
        if run_id is None:
            run_id = str(uuid4())
        with manager.session_scope() as session:
            run = PipelineRun(
                run_id=run_id,
                source_id=source_id,
                target_phase=target_phase,
                config=stored_config,
            )
            session.add(run)

            # Load existing checkpoints if resuming
            if self.config.skip_completed:
                self._load_completed_checkpoints(session, source_id)

            # If forcing a phase, remove it from completed so it re-runs
            if self._force_target and self._force_target in self._completed:
                self._completed.discard(self._force_target)
                self._outputs.pop(self._force_target, None)
                logger.info(f"Force re-run: removed {self._force_target} from completed set")

        # Get phases to run
        phases_to_run = self.get_phases_to_run(target_phase)
        results: dict[str, PhaseResult] = {}
        total_phases = len(phases_to_run)
        completed_step = 0  # Counter for progress notifications

        # Compute priority so ready phases are scheduled in critical-path order
        self._compute_phase_priority()

        start_time = time.time()

        # Start pipeline metrics collection (sets context var)
        start_pipeline_metrics(run_id=run_id, source_id=source_id)

        try:
            with ThreadPoolExecutor(max_workers=self.config.max_parallel) as pool:
                active_futures: dict[Future[PhaseResult], str] = {}

                # Work queue: phases sorted by priority (highest first).
                # Pop from front, submit if ready, push to back if blocked.
                work_queue: deque[str] = deque(
                    sorted(
                        phases_to_run,
                        key=lambda n: self._phase_priority.get(n, 0),
                        reverse=True,
                    )
                )

                while work_queue or active_futures:
                    # Pop phases from front of queue, submit ready ones,
                    # collect blocked ones to re-queue at the back.
                    not_ready: list[str] = []
                    queue_len = len(work_queue)
                    scanned = 0

                    while work_queue and len(active_futures) < self.config.max_parallel:
                        name = work_queue.popleft()
                        scanned += 1

                        # Skip already-handled phases
                        if (
                            name in self._completed
                            or name in self._failed
                            or name in self._skipped
                            or name in self._gate_blocked
                        ):
                            continue
                        if name in self._running:
                            continue

                        phase = self.phases.get(name)
                        if not phase:
                            logger.info(f"Phase {name} skipped: no implementation registered")
                            self._skipped.add(name)
                            continue

                        deps = phase.dependencies
                        failed_deps = [d for d in deps if d in self._failed]
                        if failed_deps:
                            logger.warning(
                                f"Phase {name} blocked: dependencies failed: {failed_deps}"
                            )
                            self._skipped.add(name)
                            continue

                        done = self._completed | self._skipped
                        if all(d in done for d in deps):
                            # Check entropy gate preconditions
                            gate_passed, gate_reason = self._check_gate(name)
                            if not gate_passed:
                                if self.config.gate_mode in ("skip", "auto_fix"):
                                    logger.warning(f"Phase {name}: {gate_reason} (skipping gate)")
                                elif self.config.gate_mode == "fail":
                                    logger.error(f"Phase {name}: {gate_reason}")
                                    self._failed.add(name)
                                    results[name] = PhaseResult.failed(gate_reason)
                                    continue
                                else:  # pause
                                    logger.warning(f"Phase {name}: {gate_reason} (paused)")
                                    self._gate_blocked.add(name)
                                    results[name] = PhaseResult(
                                        status=PhaseStatus.GATE_BLOCKED,
                                        error=gate_reason,
                                    )
                                    continue

                            # Ready — submit to executor immediately
                            self._running.add(name)
                            self._notify_progress(
                                progress_callback,
                                completed_step,
                                total_phases,
                                f"Running {name}...",
                            )
                            future = pool.submit(
                                self._run_phase,
                                name,
                                manager,
                                source_id,
                                table_ids or [],
                                run_id,
                                self._outputs.copy(),
                            )
                            active_futures[future] = name
                            logger.info(f"Started phase: {name} (running: {len(active_futures)})")
                        else:
                            # Not ready — re-queue at back
                            not_ready.append(name)

                        # If we've scanned the entire original queue, stop
                        if scanned >= queue_len:
                            break

                    # Push blocked phases back to the end of the queue
                    work_queue.extend(not_ready)

                    if not active_futures:
                        # Nothing running, nothing could be submitted — done or deadlock
                        break

                    # Wait for at least one future to complete
                    done_futures: set[Future[PhaseResult]] = set()
                    try:
                        for future in as_completed(active_futures.keys(), timeout=0.5):
                            done_futures.add(future)
                            break
                    except TimeoutError:
                        continue

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
                            completed_step += 1

                            # Update entropy state from phase outputs
                            hard_scores = phase_result.outputs.get(
                                "entropy_hard_scores"
                            )
                            if hard_scores and isinstance(hard_scores, dict):
                                for dim, score in hard_scores.items():
                                    self._entropy_state.update_score(dim, score)
                            self._notify_progress(
                                progress_callback,
                                completed_step,
                                total_phases,
                                f"Completed {name}",
                            )
                            logger.info(
                                f"Phase {name} completed in {phase_result.duration_seconds:.1f}s"
                            )
                            if phase_result.warnings:
                                for warning in phase_result.warnings:
                                    logger.warning(f"Phase {name}: {warning}")
                        elif phase_result.status == PhaseStatus.SKIPPED:
                            self._skipped.add(name)
                            completed_step += 1
                            self._notify_progress(
                                progress_callback,
                                completed_step,
                                total_phases,
                                f"Skipped {name}",
                            )
                            logger.info(f"Phase {name} skipped: {phase_result.error}")
                        else:
                            self._failed.add(name)
                            completed_step += 1
                            self._notify_progress(
                                progress_callback,
                                completed_step,
                                total_phases,
                                f"Failed {name}: {phase_result.error}",
                            )
                            logger.error(f"Phase {name} failed: {phase_result.error}")
                            if phase_result.warnings:
                                for warning in phase_result.warnings:
                                    logger.warning(f"Phase {name}: {warning}")
                            if self.config.fail_fast:
                                for f in active_futures:
                                    f.cancel()
                                active_futures.clear()
                                work_queue.clear()
                                break

                    if self.config.fail_fast and self._failed:
                        break

            # Final progress notification
            self._notify_progress(
                progress_callback,
                completed_step,
                total_phases,
                "Pipeline complete",
            )

            # End pipeline metrics collection
            final_metrics = end_pipeline_metrics()

            # Update run record with final status and aggregate metrics
            with manager.session_scope() as session:
                if self._failed:
                    status = "failed"
                elif self._gate_blocked:
                    status = "gate_blocked"
                else:
                    status = "completed"
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
                total_tables_processed = 0
                total_rows_processed = 0

                if final_metrics:
                    for pm in final_metrics.phases:
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
                        total_tables_processed=total_tables_processed,
                        total_rows_processed=total_rows_processed,
                        error=error_msg,
                        final_entropy_state=self._entropy_state.to_dict() or None,
                    )
                )
                session.execute(stmt)

        except Exception as e:
            # End pipeline metrics even on failure
            end_pipeline_metrics()
            # Update run record with error
            with manager.session_scope() as session:
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
            previous_outputs: Outputs from previous phases

        Returns:
            PhaseResult from the phase execution
        """
        max_retries = self.config.max_retries
        backoff_base = self.config.backoff_base
        for attempt in range(max_retries + 1):
            try:
                return self._execute_phase(
                    phase_name,
                    manager,
                    source_id,
                    table_ids,
                    run_id,
                    previous_outputs,
                )
            except OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries:
                    wait = backoff_base * (2**attempt)
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
                    # Build scoped config: phase-specific + runtime overrides
                    phase_section = self._phase_configs.get(phase_name, {})
                    scoped_config = {**phase_section, **self._runtime_config}

                    # Build context with this phase's session and cursor
                    ctx = PhaseContext(
                        session=session,
                        duckdb_conn=cursor,
                        source_id=source_id,
                        table_ids=table_ids,
                        previous_outputs=previous_outputs,
                        config=scoped_config,
                        session_factory=manager.session_scope,
                        manager=manager,
                    )

                    # Force cleanup and bypass skip for forced phase
                    if phase_name == self._force_target:
                        from dataraum.pipeline.cleanup import cleanup_phase

                        deleted = cleanup_phase(phase_name, source_id, session, cursor)
                        logger.info(f"Force cleanup: deleted {deleted} records for {phase_name}")
                        skip_reason = None  # Force re-run
                    else:
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

    def _check_gate(self, phase_name: str) -> tuple[bool, str]:
        """Check if a phase's entropy preconditions are met.

        Args:
            phase_name: Name of the phase to check

        Returns:
            (passed, reason): passed=True if phase can run,
            reason describes violations if blocked.
        """
        phase = self.phases.get(phase_name)
        if not phase:
            return True, ""

        preconditions = phase.entropy_preconditions
        if not preconditions:
            return True, ""

        violations = self._entropy_state.check_preconditions(preconditions)
        if not violations:
            return True, ""

        # Format violation message
        parts = []
        for dim, (current, threshold) in violations.items():
            parts.append(f"{dim}: {current:.2f} > {threshold:.2f}")
        reason = f"Gate blocked: {', '.join(parts)}"
        return False, reason

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



# Global pipeline instance
_pipeline: Pipeline | None = None


def get_pipeline(active_phases: list[str] | None = None) -> Pipeline:
    """Get a pipeline instance with phases from the registry.

    Args:
        active_phases: List of phase names to activate. If None, uses
            all registered phases (from @analysis_phase decorators).

    Returns:
        Configured Pipeline instance.
    """
    global _pipeline
    if _pipeline is None:
        registry = get_registry()
        names = active_phases if active_phases is not None else list(registry.keys())

        _pipeline = Pipeline()
        for name in names:
            cls = registry.get(name)
            if cls:
                _pipeline.register(cls())
            else:
                logger.warning(f"Phase '{name}' listed in config but not found in registry")
    return _pipeline


def run_pipeline(
    manager: ConnectionManager,
    source_id: str,
    table_ids: list[str] | None = None,
    target_phase: str | None = None,
    config: PipelineConfig | None = None,
    phase_configs: dict[str, dict[str, Any]] | None = None,
    runtime_config: dict[str, Any] | None = None,
    run_id: str | None = None,
    progress_callback: ProgressCallback | None = None,
    force_phase: bool = False,
) -> dict[str, PhaseResult]:
    """Run the pipeline.

    Convenience function that uses the global pipeline instance.

    Args:
        manager: Connection manager for database access
        source_id: Source identifier
        table_ids: Optional list of table IDs
        target_phase: Optional target phase (runs phase + dependencies)
        config: Pipeline configuration
        phase_configs: Per-phase config dicts keyed by phase name
        runtime_config: Runtime overrides merged into every phase's config
        run_id: Optional run ID (generated if not provided)
        progress_callback: Optional callback for progress notifications.
        force_phase: If True, force re-run of target_phase.

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
        phase_configs=phase_configs,
        runtime_config=runtime_config,
        run_id=run_id,
        progress_callback=progress_callback,
        force_phase=force_phase,
    )
