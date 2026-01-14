"""Pipeline orchestrator.

Runs phases in dependency order with parallel execution where possible.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

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

if TYPE_CHECKING:
    import duckdb


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

    async def run(
        self,
        session: AsyncSession,
        duckdb_conn: duckdb.DuckDBPyConnection,
        source_id: str,
        table_ids: list[str] | None = None,
        target_phase: str | None = None,
        run_config: dict[str, Any] | None = None,
    ) -> dict[str, PhaseResult]:
        """Run the pipeline.

        Args:
            session: SQLAlchemy async session
            duckdb_conn: DuckDB connection
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

        # Create pipeline run record
        run = PipelineRun(
            source_id=source_id,
            target_phase=target_phase,
            config=run_config or {},
        )
        session.add(run)
        await session.flush()

        # Load existing checkpoints if resuming
        if self.config.skip_completed:
            await self._load_completed_checkpoints(session, source_id)

        # Get phases to run
        phases_to_run = self.get_phases_to_run(target_phase)
        results: dict[str, PhaseResult] = {}

        start_time = time.time()

        try:
            while not self._is_complete(phases_to_run):
                # Find phases ready to run
                ready = self._get_ready_phases(phases_to_run)

                if not ready:
                    if self._running:
                        # Wait for running phases
                        await asyncio.sleep(0.1)
                        continue
                    else:
                        # Deadlock or all remaining phases blocked by failures
                        break

                # Limit parallel execution
                batch = list(ready)[: self.config.max_parallel - len(self._running)]

                # Run batch in parallel
                tasks = [
                    self._run_phase(
                        phase_name=name,
                        session=session,
                        duckdb_conn=duckdb_conn,
                        source_id=source_id,
                        table_ids=table_ids or [],
                        run_id=run.run_id,
                        run_config=run_config or {},
                    )
                    for name in batch
                ]

                batch_results = await asyncio.gather(*tasks, return_exceptions=True)

                for name, batch_result in zip(batch, batch_results, strict=True):
                    self._running.discard(name)

                    if isinstance(batch_result, BaseException):
                        phase_result = PhaseResult.failed(str(batch_result))
                    else:
                        phase_result = batch_result

                    results[name] = phase_result

                    if phase_result.status == PhaseStatus.COMPLETED:
                        self._completed.add(name)
                        self._outputs[name] = phase_result.outputs
                        run.phases_completed += 1
                    elif phase_result.status == PhaseStatus.SKIPPED:
                        self._skipped.add(name)
                        run.phases_skipped += 1
                    else:
                        self._failed.add(name)
                        run.phases_failed += 1
                        if self.config.fail_fast:
                            run.error = phase_result.error
                            break

                if self.config.fail_fast and self._failed:
                    break

        except Exception as e:
            run.error = str(e)
            run.status = "failed"
        else:
            run.status = "completed" if not self._failed else "failed"

        # Finalize run
        run.completed_at = datetime.now(UTC)
        run.total_duration_seconds = time.time() - start_time
        await session.commit()

        return results

    async def _load_completed_checkpoints(self, session: AsyncSession, source_id: str) -> None:
        """Load previously completed checkpoints."""
        stmt = select(PhaseCheckpoint).where(
            PhaseCheckpoint.source_id == source_id,
            PhaseCheckpoint.status == "completed",
        )
        result = await session.execute(stmt)
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

            # Check if LLM phase should be skipped
            if self.config.skip_llm_phases and phase_def.requires_llm:
                self._skipped.add(name)
                continue

            # Check dependencies
            deps_met = all(d in done for d in phase_def.dependencies)

            # Check if any dependency failed (can't run this phase)
            deps_failed = any(d in self._failed for d in phase_def.dependencies)

            if deps_met and not deps_failed:
                ready.add(name)

        return ready

    async def _run_phase(
        self,
        phase_name: str,
        session: AsyncSession,
        duckdb_conn: duckdb.DuckDBPyConnection,
        source_id: str,
        table_ids: list[str],
        run_id: str,
        run_config: dict[str, Any],
    ) -> PhaseResult:
        """Run a single phase."""
        self._running.add(phase_name)
        start_time = time.time()
        started_at = datetime.now(UTC)

        # Get phase implementation
        phase = self.phases.get(phase_name)
        if not phase:
            return PhaseResult.failed(f"No implementation registered for phase: {phase_name}")

        # Build context
        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            table_ids=table_ids,
            previous_outputs=self._outputs.copy(),
            config=run_config,
        )

        try:
            # Check if should skip
            skip_reason = await phase.should_skip(ctx)
            if skip_reason:
                result = PhaseResult.skipped(skip_reason)
            else:
                # Run the phase
                result = await phase.run(ctx)
                result.duration_seconds = time.time() - start_time

        except Exception as e:
            result = PhaseResult.failed(str(e), duration=time.time() - start_time)

        # Save checkpoint
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
        await session.flush()

        return result


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
    from dataraum_context.pipeline.phases import ImportPhase, TypingPhase

    pipeline.register(ImportPhase())
    pipeline.register(TypingPhase())


async def run_pipeline(
    session: AsyncSession,
    duckdb_conn: duckdb.DuckDBPyConnection,
    source_id: str,
    table_ids: list[str] | None = None,
    target_phase: str | None = None,
    config: PipelineConfig | None = None,
    run_config: dict[str, Any] | None = None,
) -> dict[str, PhaseResult]:
    """Run the pipeline.

    Convenience function that uses the global pipeline instance.

    Args:
        session: SQLAlchemy async session
        duckdb_conn: DuckDB connection
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

    return await pipeline.run(
        session=session,
        duckdb_conn=duckdb_conn,
        source_id=source_id,
        table_ids=table_ids,
        target_phase=target_phase,
        run_config=run_config,
    )
