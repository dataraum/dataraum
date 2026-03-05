"""Pipeline scheduler — generator-based reactive execution loop.

Contract-driven approach that uses entropy thresholds and fix replay.
"""

from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.entropy.fix_executor import FixExecutor, FixRequest, FixResult
from dataraum.pipeline.base import Phase, PhaseContext, PhaseResult, PhaseStatus
from dataraum.pipeline.cleanup import cleanup_phase
from dataraum.pipeline.db_models import Fix, PhaseLog
from dataraum.pipeline.events import EventType, PipelineEvent

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


@dataclass
class ExitCheckIssue:
    """A contract violation detected during post-verification."""

    dimension_path: str  # e.g. "structural.types.type_fidelity"
    score: float
    threshold: float
    producing_phase: str  # phase whose post_verification measured this
    affected_targets: list[str] = field(default_factory=list)


class ResolutionAction(str, Enum):
    """How the caller wants to resolve an exit check."""

    FIX = "fix"
    DEFER = "defer"
    ABORT = "abort"


@dataclass
class Resolution:
    """Caller's response to an EXIT_CHECK event."""

    action: ResolutionAction
    fixes: list[FixRequest] = field(default_factory=list)


@dataclass
class PipelineResult:
    """Final result returned by the scheduler generator."""

    success: bool
    phases_completed: list[str]
    phases_failed: list[str]
    phases_skipped: list[str]
    phases_blocked: list[str]  # PENDING phases blocked by failed dependencies
    final_scores: dict[str, float]  # dimension_path -> score
    deferred_issues: list[ExitCheckIssue]
    error: str | None = None


class PipelineAborted(Exception):
    """Raised when the caller sends ABORT resolution."""


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------


class PipelineScheduler:
    """Generator-based reactive pipeline scheduler.

    Yields PipelineEvent objects and receives Resolution objects via
    generator.send() at EXIT_CHECK points.
    """

    def __init__(
        self,
        phases: dict[str, Phase],
        source_id: str,
        run_id: str,
        session: Session,
        duckdb_conn: Any,
        contract_thresholds: dict[str, float] | None = None,
        fix_executor: FixExecutor | None = None,
        phase_configs: dict[str, dict[str, Any]] | None = None,
        runtime_config: dict[str, Any] | None = None,
        session_factory: Callable[[], Any] | None = None,
        manager: Any | None = None,
    ) -> None:
        # Validate that all declared dependencies reference known phases
        for name, phase in phases.items():
            unknown = [d for d in phase.dependencies if d not in phases]
            if unknown:
                raise ValueError(
                    f"Phase {name!r} declares unknown dependencies: {unknown}"
                )

        self.phases = phases
        self.source_id = source_id
        self.run_id = run_id
        self.session = session
        self.duckdb_conn = duckdb_conn
        self.contract_thresholds = contract_thresholds or {}
        self.fix_executor = fix_executor
        self._phase_configs = phase_configs or {}
        self._runtime_config = runtime_config or {}
        self.session_factory = session_factory
        self.manager = manager

        # Internal state
        self._state: dict[str, PhaseStatus] = dict.fromkeys(
            phases, PhaseStatus.PENDING
        )
        self._scores: dict[str, float] = {}
        self._column_details: dict[str, dict[str, float]] = {}
        self._pending_issues: list[ExitCheckIssue] = []
        self._deferred_issues: list[ExitCheckIssue] = []
        self._step = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> Generator[PipelineEvent, Resolution | None, PipelineResult]:
        """Execute the pipeline as a generator.

        Phases in the same dependency wave run concurrently when a
        session_factory is available.  Post-processing (phase log writes,
        fix replay, post-verification) always runs on the main thread.

        Yields:
            PipelineEvent for each lifecycle event.

        Receives:
            Resolution via send() after EXIT_CHECK events.

        Returns:
            PipelineResult summarising the run.
        """
        total = len(self.phases)
        yield self._event(EventType.PIPELINE_STARTED, total=total)

        try:
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

                # Natural pause — all ready phases in this wave done
                if self._pending_issues:
                    violations = {
                        issue.dimension_path: (issue.score, issue.threshold)
                        for issue in self._pending_issues
                    }
                    resolution = yield self._event(
                        EventType.EXIT_CHECK,
                        total=total,
                        violations=violations,
                        column_details=dict(self._column_details),
                    )
                    if resolution is not None:
                        fix_events = self._apply_resolution(resolution)
                        yield from fix_events
                    self._pending_issues.clear()
                    self._column_details = {}

        except PipelineAborted as e:
            return PipelineResult(
                success=False,
                phases_completed=self._phases_with_status(PhaseStatus.COMPLETED),
                phases_failed=self._phases_with_status(PhaseStatus.FAILED),
                phases_skipped=self._phases_with_status(PhaseStatus.SKIPPED),
                phases_blocked=self._phases_with_status(PhaseStatus.PENDING),
                final_scores=dict(self._scores),
                deferred_issues=list(self._deferred_issues),
                error=str(e) or "Pipeline aborted by user",
            )

        yield self._event(EventType.PIPELINE_COMPLETED, total=total)

        return PipelineResult(
            success=not self._phases_with_status(PhaseStatus.FAILED),
            phases_completed=self._phases_with_status(PhaseStatus.COMPLETED),
            phases_failed=self._phases_with_status(PhaseStatus.FAILED),
            phases_skipped=self._phases_with_status(PhaseStatus.SKIPPED),
            phases_blocked=self._phases_with_status(PhaseStatus.PENDING),
            final_scores=dict(self._scores),
            deferred_issues=list(self._deferred_issues),
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

        if self.session_factory is not None:
            with self.session_factory() as phase_session:
                config: dict[str, Any] = {}
                config.update(self._phase_configs.get(phase_name, {}))
                config.update(self._runtime_config)
                ctx = PhaseContext(
                    session=phase_session,
                    duckdb_conn=self.duckdb_conn,
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

    def _post_process_phase(
        self, phase_name: str, result: PhaseResult, started_at: datetime, total: int
    ) -> Generator[PipelineEvent]:
        """Post-process a completed phase on the main thread.

        Writes phase log, yields events, replays fixes, runs post-verification.
        """
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

        # Merge entropy scores from phase outputs (e.g. entropy phase)
        if result.outputs and "entropy_scores" in result.outputs:
            self._scores.update(result.outputs["entropy_scores"])

        # Replay active fixes for this phase
        self._replay_fixes(phase_name)

        # Post-verify (run detectors)
        scores = self._post_verify(phase_name)
        if scores:
            yield self._event(
                EventType.POST_VERIFICATION,
                phase=phase_name,
                total=total,
                scores=scores,
            )

        # Assess contract impact on newly produced scores
        phase_scores = dict(scores)
        if result.outputs and "entropy_scores" in result.outputs:
            phase_scores.update(result.outputs["entropy_scores"])
        issues = self._assess_impact(phase_scores, phase_name)
        self._pending_issues.extend(issues)

    def _run_sequential(
        self, phase_names: list[str], total: int
    ) -> Generator[PipelineEvent]:
        """Run phases sequentially (single phase or no session_factory)."""
        for phase_name in phase_names:
            yield self._event(EventType.PHASE_STARTED, phase=phase_name, total=total)
            result, started_at = self._run_phase(phase_name)
            yield from self._post_process_phase(phase_name, result, started_at, total)

    def _run_parallel(
        self, phase_names: list[str], total: int
    ) -> Generator[PipelineEvent]:
        """Run phases concurrently via ThreadPoolExecutor.

        PHASE_STARTED events are yielded for all phases before execution.
        Results are post-processed on the main thread as they complete.
        """
        # Yield STARTED for all phases in this wave
        for phase_name in phase_names:
            yield self._event(EventType.PHASE_STARTED, phase=phase_name, total=total)

        max_workers = min(len(phase_names), 4)
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(self._run_phase, name): name
                for name in phase_names
            }
            for future in as_completed(futures):
                phase_name = futures[future]
                try:
                    result, started_at = future.result()
                except Exception as exc:
                    # Phase raised an unhandled exception
                    result = PhaseResult.failed(str(exc))
                    started_at = datetime.now(UTC)
                yield from self._post_process_phase(
                    phase_name, result, started_at, total
                )

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
        scores: dict[str, float] | None = None,
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
            entropy_scores=scores,
            outputs=outputs,
        )
        self.session.add(log)
        self.session.commit()

    def _replay_fixes(self, phase_name: str) -> list[FixResult]:
        """Replay active Fix records bound to this phase."""
        if self.fix_executor is None:
            return []

        fixes = (
            self.session.execute(
                select(Fix).where(
                    Fix.source_id == self.source_id,
                    Fix.after_phase == phase_name,
                    Fix.status == "active",
                )
            )
            .scalars()
            .all()
        )

        results: list[FixResult] = []
        now = datetime.now(UTC)
        for fix in fixes:
            request = FixRequest(
                action_type=fix.action_type,
                target=fix.target,
                parameters=fix.parameters,
                source_id=self.source_id,
                run_id=self.run_id,
            )
            result = self.fix_executor.execute(
                request, self.session, self.duckdb_conn
            )
            fix.last_applied_at = now
            fix.last_applied_run_id = self.run_id
            if result.success:
                fix.status = "applied"
            results.append(result)

        if fixes:
            self.session.commit()
        return results

    def _post_verify(self, phase_name: str) -> dict[str, float]:
        """Run detectors for dimensions listed in phase.post_verification.

        Dispatches column-scoped and table-scoped detectors separately:
        - Column dims: snapshot per column on each typed table
        - Table dims: snapshot per typed table (cross-column analysis)
        """
        phase = self.phases[phase_name]
        dims = phase.post_verification
        if not dims:
            return {}

        from dataraum.entropy.detectors.base import get_default_registry
        from dataraum.entropy.snapshot import take_snapshot
        from dataraum.storage.models import Column as ColumnModel
        from dataraum.storage.models import Table

        registry = get_default_registry()

        # Partition dims into column vs table scope
        table_sub_dims = {
            d.sub_dimension for d in registry.get_all_detectors() if d.scope == "table"
        }
        col_dims = [d for d in dims if d not in table_sub_dims]
        tbl_dims = [d for d in dims if d in table_sub_dims]

        # Get all typed tables for this source
        typed_tables = (
            self.session.execute(
                select(Table).where(
                    Table.source_id == self.source_id, Table.layer == "typed"
                )
            )
            .scalars()
            .all()
        )

        scores_by_dim: dict[str, list[float]] = defaultdict(list)
        column_scores: dict[str, dict[str, float]] = defaultdict(dict)

        # Column-scoped pass
        if col_dims:
            for table in typed_tables:
                columns = (
                    self.session.execute(
                        select(ColumnModel).where(ColumnModel.table_id == table.table_id)
                    )
                    .scalars()
                    .all()
                )
                for col in columns:
                    target = f"column:{table.table_name}.{col.column_name}"
                    snapshot = take_snapshot(
                        target=target,
                        session=self.session,
                        duckdb_conn=self.duckdb_conn,
                        dimensions=col_dims,
                    )
                    for sub_dim, score in snapshot.scores.items():
                        scores_by_dim[sub_dim].append(score)
                        column_scores[sub_dim][target] = score

        # Table-scoped pass
        if tbl_dims:
            for table in typed_tables:
                target = f"table:{table.table_name}"
                snapshot = take_snapshot(
                    target=target,
                    session=self.session,
                    duckdb_conn=self.duckdb_conn,
                    dimensions=tbl_dims,
                )
                for sub_dim, score in snapshot.scores.items():
                    scores_by_dim[sub_dim].append(score)

        # View-scoped pass
        view_sub_dims = {d.sub_dimension for d in registry.get_all_detectors() if d.scope == "view"}
        view_dims = [d for d in dims if d in view_sub_dims]

        if view_dims:
            from dataraum.analysis.views.db_models import EnrichedView

            enriched_views = (
                self.session.execute(
                    select(EnrichedView).where(
                        EnrichedView.fact_table_id.in_([t.table_id for t in typed_tables])
                    )
                )
                .scalars()
                .all()
            )

            for ev in enriched_views:
                target = f"view:{ev.view_name}"
                snapshot = take_snapshot(
                    target=target,
                    session=self.session,
                    duckdb_conn=self.duckdb_conn,
                    dimensions=view_dims,
                )
                for sub_dim, score in snapshot.scores.items():
                    scores_by_dim[sub_dim].append(score)

        # Build sub_dimension -> dimension_path mapping
        sub_dim_to_path = {
            d.sub_dimension: d.dimension_path
            for d in registry.get_all_detectors()
        }

        # Average per dimension
        result: dict[str, float] = {}
        for sub_dim, score_list in scores_by_dim.items():
            mean_score = sum(score_list) / len(score_list)
            path = sub_dim_to_path.get(sub_dim, sub_dim)
            result[path] = mean_score
            self._scores[path] = mean_score

        # Merge per-column details keyed by dimension_path (not replace,
        # so that multiple phases in the same wave accumulate correctly)
        for sd, targets in column_scores.items():
            path = sub_dim_to_path.get(sd, sd)
            self._column_details[path] = targets

        return result

    def _assess_impact(
        self, scores: dict[str, float], phase_name: str
    ) -> list[ExitCheckIssue]:
        """Check scores against contract thresholds."""
        if not self.contract_thresholds or not scores:
            return []

        issues: list[ExitCheckIssue] = []
        for dimension_path, score in scores.items():
            threshold = self._match_threshold(dimension_path)
            if threshold is not None and score > threshold:
                col_scores = self._column_details.get(dimension_path, {})
                affected = [t for t, s in col_scores.items() if s > threshold]
                issues.append(
                    ExitCheckIssue(
                        dimension_path=dimension_path,
                        score=score,
                        threshold=threshold,
                        producing_phase=phase_name,
                        affected_targets=affected,
                    )
                )
        return issues

    def _match_threshold(self, dimension_path: str) -> float | None:
        """Find contract threshold by prefix match.

        Given "structural.types.type_fidelity", checks for thresholds at:
        - "structural.types.type_fidelity" (exact)
        - "structural.types" (prefix)
        - "structural" (prefix)
        """
        parts = dimension_path.split(".")
        for i in range(len(parts), 0, -1):
            prefix = ".".join(parts[:i])
            if prefix in self.contract_thresholds:
                return self.contract_thresholds[prefix]
        return None

    def _apply_resolution(self, resolution: Resolution) -> list[PipelineEvent]:
        """Apply a caller-provided resolution to pending issues.

        Returns:
            List of FIX_APPLIED events for each fix attempt.
        """
        fix_events: list[PipelineEvent] = []

        if resolution.action == ResolutionAction.DEFER:
            self._deferred_issues.extend(self._pending_issues)
        elif resolution.action == ResolutionAction.ABORT:
            raise PipelineAborted("Pipeline aborted by user")
        elif resolution.action == ResolutionAction.FIX:
            if self.fix_executor is None:
                logger.warning("FIX resolution but no fix_executor configured")
                self._deferred_issues.extend(self._pending_issues)
                return fix_events
            any_succeeded = False
            for fix_request in resolution.fixes:
                result = self.fix_executor.execute(
                    fix_request, self.session, self.duckdb_conn
                )
                if result.success:
                    any_succeeded = True
                    # Create persistent Fix record
                    fix = Fix(
                        source_id=self.source_id,
                        action_type=fix_request.action_type,
                        target=fix_request.target,
                        parameters=fix_request.parameters,
                        after_phase=fix_request.blocked_phase or "",
                        status="active",
                        last_applied_at=datetime.now(UTC),
                        last_applied_run_id=self.run_id,
                    )
                    self.session.add(fix)
                    fix_events.append(
                        self._event(
                            EventType.FIX_APPLIED,
                            message=f"{fix_request.action_type} on {fix_request.target}",
                            scores=result.after_scores,
                            before_scores=result.before_scores,
                            after_scores=result.after_scores,
                        )
                    )
                else:
                    fix_events.append(
                        self._event(
                            EventType.FIX_APPLIED,
                            message=f"{fix_request.action_type} on {fix_request.target}",
                            error=result.error or "Fix failed",
                        )
                    )
            # Invalidate downstream once per unique producing phase
            if any_succeeded:
                phases_to_invalidate = {
                    issue.producing_phase for issue in self._pending_issues
                }
                for phase_name in phases_to_invalidate:
                    self._invalidate_downstream(phase_name)
            else:
                # All fixes failed — defer the issues so they aren't lost
                self._deferred_issues.extend(self._pending_issues)
            self.session.commit()

        return fix_events

    def _invalidate_downstream(self, phase_name: str) -> None:
        """Reset downstream phases to PENDING so they re-run.

        Resets COMPLETED, SKIPPED, and FAILED phases. COMPLETED phases
        need cleanup (delete output records). SKIPPED phases need to
        re-evaluate should_skip() since the upstream data changed.
        FAILED phases should retry with the corrected data.
        """
        dependents = self._transitive_dependents(phase_name)
        for dep_name in dependents:
            dep_status = self._state[dep_name]
            if dep_status == PhaseStatus.COMPLETED:
                cleanup_phase(
                    dep_name, self.source_id, self.session, self.duckdb_conn
                )
                self._state[dep_name] = PhaseStatus.PENDING
            elif dep_status in (PhaseStatus.SKIPPED, PhaseStatus.FAILED):
                self._state[dep_name] = PhaseStatus.PENDING

    def _transitive_dependents(self, phase_name: str) -> list[str]:
        """BFS over reverse dependency graph to find all downstream phases."""
        # Build reverse adjacency: phase -> phases that depend on it
        reverse: dict[str, list[str]] = defaultdict(list)
        for name, phase in self.phases.items():
            for dep in phase.dependencies:
                reverse[dep].append(name)

        visited: set[str] = set()
        queue = deque(reverse.get(phase_name, []))
        result: list[str] = []
        while queue:
            current = queue.popleft()
            if current in visited:
                continue
            visited.add(current)
            result.append(current)
            queue.extend(reverse.get(current, []))

        return result
