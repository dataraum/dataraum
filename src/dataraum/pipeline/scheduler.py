"""Pipeline scheduler — generator-based reactive execution loop.

Contract-driven approach with gate-based entropy measurement at quality checkpoints.
"""

from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.entropy.dimensions import AnalysisKey, _StrValueMixin
from dataraum.entropy.gate import (
    ExitCheckIssue,
    GateResult,
    assess_contracts,
    measure_at_gate,
    persist_gate_result,
)
from dataraum.pipeline.base import Phase, PhaseContext, PhaseResult, PhaseStatus
from dataraum.pipeline.cleanup import cleanup_phase
from dataraum.pipeline.db_models import PhaseLog
from dataraum.pipeline.events import EventType, PipelineEvent
from dataraum.pipeline.fixes import FixInput

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


class ResolutionAction(_StrValueMixin):
    """How the caller wants to resolve an exit check."""

    DEFER = "defer"
    ABORT = "abort"
    FIX = "fix"


@dataclass
class Resolution:
    """Caller's response to an EXIT_CHECK event."""

    action: ResolutionAction
    fix_inputs: list[FixInput] = field(default_factory=list)


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


def _parse_col_ref(ref: str) -> tuple[str, str | None]:
    """Parse a column reference like 'column:table.col' → (table, col).

    Handles formats: 'table.col', 'column:table.col', 'table', 'column:table'.
    """
    bare = ref.split(":", 1)[-1] if ":" in ref else ref
    parts = bare.split(".", 1)
    return parts[0], parts[1] if len(parts) > 1 else None


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

    def run(self) -> Generator[PipelineEvent, Resolution | None, PipelineResult]:
        """Execute the pipeline as a generator.

        Phases in the same dependency wave run concurrently when a
        session_factory is available.  Gate measurement and contract
        assessment run on the main thread after each wave.

        Yields:
            PipelineEvent for each lifecycle event.

        Receives:
            Resolution via send() after EXIT_CHECK events.

        Returns:
            PipelineResult summarising the run.
        """
        total = len(self.phases)
        yield self._event(EventType.PIPELINE_STARTED, total=total)

        all_scores: dict[str, float] = {}
        deferred_issues: list[ExitCheckIssue] = []

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
                    wave_results: list[tuple[str, PhaseResult]] = yield from self._run_parallel(
                        to_run, total
                    )
                else:
                    wave_results = yield from self._run_sequential(to_run, total)

                # 3. Merge entropy scores from phase outputs
                for _phase_name, result in wave_results:
                    if result.outputs and "entropy_scores" in result.outputs:
                        all_scores.update(result.outputs["entropy_scores"])

                # 4. Gate-based measurement for quality gate phases.
                # column_details is per-wave (reset each wave) while
                # all_scores accumulates across the entire run.
                pending_issues: list[ExitCheckIssue] = []
                column_details: dict[str, dict[str, float]] = {}
                table_details: dict[str, dict[str, float]] = {}
                view_details: dict[str, dict[str, float]] = {}
                column_evidence: dict[str, dict[str, dict[str, Any]]] = {}
                resolution_actions: dict[str, set[str]] = {}
                wave_skipped: list[dict[str, str]] = []
                last_gate_phase: str = ""

                for phase_name, _result in wave_results:
                    phase = self.phases[phase_name]
                    if self._state[phase_name] == PhaseStatus.COMPLETED and phase.is_quality_gate:
                        available = self._available_analyses()
                        gate_result = measure_at_gate(
                            self.session,
                            self.duckdb_conn,
                            self.source_id,
                            available,
                        )
                        all_scores.update(gate_result.scores)
                        column_details.update(gate_result.column_details)
                        table_details.update(gate_result.table_details)
                        view_details.update(gate_result.view_details)
                        column_evidence.update(gate_result.column_evidence)
                        for path, acts in gate_result.resolution_actions.items():
                            resolution_actions.setdefault(path, set()).update(acts)
                        # Collect skipped detectors (deduplicate by detector_id)
                        seen_ids = {s["detector_id"] for s in wave_skipped}
                        for sd in gate_result.skipped_detectors:
                            if sd.detector_id not in seen_ids:
                                wave_skipped.append(
                                    {"detector_id": sd.detector_id, "reason": sd.reason}
                                )
                                seen_ids.add(sd.detector_id)
                        last_gate_phase = phase_name

                # Persist gate scores to PhaseLog for the gate phase
                if last_gate_phase:
                    self._persist_gate_scores(last_gate_phase, gate_result)

                # Emit one POST_VERIFICATION per wave (after all gates measured)
                if last_gate_phase and all_scores:
                    yield self._event(
                        EventType.POST_VERIFICATION,
                        phase=last_gate_phase,
                        total=total,
                        scores=dict(all_scores),
                        skipped_detectors=wave_skipped,
                        column_details=dict(column_details),
                        table_details=dict(table_details),
                        view_details=dict(view_details),
                        column_evidence=dict(column_evidence),
                    )
                    issues = assess_contracts(
                        dict(all_scores),
                        self.contract_thresholds,
                        column_details,
                        last_gate_phase,
                        resolution_actions=resolution_actions,
                        column_evidence=column_evidence,
                    )
                    pending_issues.extend(issues)

                # 5. EXIT_CHECK — natural pause after wave
                if pending_issues:
                    violations = {
                        issue.dimension_path: (issue.score, issue.threshold)
                        for issue in pending_issues
                    }
                    fixes = self._gather_available_fixes(pending_issues)
                    resolution = yield self._event(
                        EventType.EXIT_CHECK,
                        total=total,
                        violations=violations,
                        scores=dict(all_scores),
                        column_details=dict(column_details),
                        table_details=dict(table_details),
                        view_details=dict(view_details),
                        column_evidence=dict(column_evidence),
                        available_fixes=fixes,
                    )
                    if resolution is not None:
                        if resolution.action == ResolutionAction.DEFER:
                            deferred_issues.extend(pending_issues)
                        elif resolution.action == ResolutionAction.ABORT:
                            raise PipelineAborted("Pipeline aborted by user")
                        elif resolution.action == ResolutionAction.FIX:
                            if not resolution.fix_inputs:
                                logger.warning("fix_resolution_empty")
                                deferred_issues.extend(pending_issues)
                            else:
                                self._apply_fixes(resolution.fix_inputs)
                                # Clear all scores (including any from phase
                                # outputs) so gates re-measure from scratch.
                                all_scores.clear()

        except PipelineAborted as e:
            return PipelineResult(
                success=False,
                phases_completed=self._phases_with_status(PhaseStatus.COMPLETED),
                phases_failed=self._phases_with_status(PhaseStatus.FAILED),
                phases_skipped=self._phases_with_status(PhaseStatus.SKIPPED),
                phases_blocked=self._phases_with_status(PhaseStatus.PENDING),
                final_scores=dict(all_scores),
                deferred_issues=deferred_issues,
                error=str(e) or "Pipeline aborted by user",
            )

        yield self._event(EventType.PIPELINE_COMPLETED, total=total)

        return PipelineResult(
            success=not self._phases_with_status(PhaseStatus.FAILED),
            phases_completed=self._phases_with_status(PhaseStatus.COMPLETED),
            phases_failed=self._phases_with_status(PhaseStatus.FAILED),
            phases_skipped=self._phases_with_status(PhaseStatus.SKIPPED),
            phases_blocked=self._phases_with_status(PhaseStatus.PENDING),
            final_scores=dict(all_scores),
            deferred_issues=deferred_issues,
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

    def _record_phase(
        self, phase_name: str, result: PhaseResult, started_at: datetime, total: int
    ) -> Generator[PipelineEvent]:
        """Record phase result: update state, write log, yield events."""
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

    def _run_sequential(
        self, phase_names: list[str], total: int
    ) -> Generator[PipelineEvent, None, list[tuple[str, PhaseResult]]]:
        """Run phases sequentially, yield events, return results."""
        results: list[tuple[str, PhaseResult]] = []
        for phase_name in phase_names:
            yield self._event(EventType.PHASE_STARTED, phase=phase_name, total=total)
            result, started_at = self._run_phase(phase_name)
            yield from self._record_phase(phase_name, result, started_at, total)
            results.append((phase_name, result))
        return results

    def _run_parallel(
        self, phase_names: list[str], total: int
    ) -> Generator[PipelineEvent, None, list[tuple[str, PhaseResult]]]:
        """Run phases concurrently via ThreadPoolExecutor.

        PHASE_STARTED events are yielded for all phases before execution.
        Results are recorded on the main thread as they complete.
        """
        results: list[tuple[str, PhaseResult]] = []

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
                results.append((phase_name, result))

        return results

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

    def _persist_gate_scores(
        self,
        gate_phase: str,
        gate_result: GateResult,
    ) -> None:
        """Update the PhaseLog for a gate phase with entropy scores.

        Delegates to the shared ``persist_gate_result`` utility.
        """
        persist_gate_result(
            self.session,
            self.source_id,
            gate_result,
            phase_name=gate_phase,
            run_id=self.run_id,
        )

    def _validate_analysis_coverage(self) -> None:
        """Warn if detectors require analyses that no phase produces."""
        from dataraum.entropy.detectors.base import get_default_registry

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

    def _available_analyses(self) -> set[AnalysisKey]:
        """Build available analyses set from COMPLETED and SKIPPED phases.

        SKIPPED phases have their output from a prior run, so their
        analyses are available for gate measurement.
        """
        available: set[AnalysisKey] = set()
        for name, status in self._state.items():
            if status in (PhaseStatus.COMPLETED, PhaseStatus.SKIPPED):
                available.update(self.phases[name].produces_analyses)
        return available

    def _apply_fixes(self, fix_inputs: list[FixInput]) -> None:
        """Apply fix inputs via bridge + interpreters, log to ledger, reset.

        Routing:
        - preprocess: cascade-clean + phase re-run (existing behaviour)
        - postprocess: apply fix document only, skip cascade-clean.
          The next gate measurement (with overrides applied) picks up the change.
        """
        from dataraum.core.config import _get_config_root
        from dataraum.documentation.ledger import log_fix
        from dataraum.entropy.detectors.base import get_default_registry
        from dataraum.pipeline.fixes.bridge import build_fix_documents
        from dataraum.pipeline.fixes.interpreters import apply_and_persist

        config_root = _get_config_root()
        detector_registry = get_default_registry()
        phases_to_rerun: set[str] = set()

        for fix_input in fix_inputs:
            schema = detector_registry.get_fix_schema(
                fix_input.action_name, fix_input.dimension or None
            )
            if schema is None:
                logger.warning(
                    "fix_schema_not_found",
                    action=fix_input.action_name,
                )
                continue

            # Parse all column refs once
            parsed = [
                _parse_col_ref(ref)
                for ref in (fix_input.affected_columns or [fix_input.action_name])
            ]
            table_name, column_name = parsed[0]
            dimension = schema.requires_rerun or schema.gate or ""

            documents = build_fix_documents(schema, fix_input, table_name, column_name, dimension)

            if documents:
                apply_and_persist(
                    self.source_id,
                    documents,
                    session=self.session,
                    config_root=config_root,
                    duckdb_conn=self.duckdb_conn,
                )

            # Only preprocess fixes trigger cascade-clean + phase re-run
            if schema.routing == "preprocess" and schema.requires_rerun:
                phases_to_rerun.add(schema.requires_rerun)

            # Log to fix ledger
            for t_name, c_name in parsed:
                log_fix(
                    session=self.session,
                    source_id=self.source_id,
                    action_name=fix_input.action_name,
                    table_name=t_name,
                    column_name=c_name,
                    user_input=fix_input.interpretation,
                    interpretation=f"{schema.action}: {', '.join(fix_input.affected_columns)}",
                )

            logger.info(
                "fix_applied",
                action=fix_input.action_name,
                documents=len(documents),
                routing=schema.routing,
                rerun=schema.requires_rerun,
            )

        # Reload configs from disk so re-runs pick up the patches
        from dataraum.core.config import load_phase_config
        from dataraum.entropy.config import clear_entropy_config_cache

        clear_entropy_config_cache()
        for phase_name in phases_to_rerun:
            self._phase_configs[phase_name] = load_phase_config(phase_name)

        # Cleanup and reset affected phases + all downstream, then commit
        # so per-phase sessions (via session_factory) see the cleared state.
        # Order matters: invalidate downstream FIRST to remove FK references,
        # then clean up the target phase itself.
        try:
            for phase_name in phases_to_rerun:
                if phase_name in self.phases:
                    self._invalidate_downstream(phase_name)
                    cleanup_phase(phase_name, self.source_id, self.session, self.duckdb_conn)
                    self._state[phase_name] = PhaseStatus.PENDING

            if phases_to_rerun:
                self.session.commit()
        except Exception:
            self.session.rollback()
            logger.error("fix_cleanup_failed", phases=list(phases_to_rerun))
            raise

    @staticmethod
    def _gather_available_fixes(
        issues: list[ExitCheckIssue],
    ) -> dict[str, list[dict[str, str]]]:
        """Gather available fixes for EXIT_CHECK event display.

        Consults the detector registry's fix_schemas, filtered to only
        actions that appear in the entropy objects' resolution options.

        Returns:
            dim_path -> [{"action_name": str, "phase_name": str, ...}]
        """
        from dataraum.entropy.detectors.base import get_default_registry

        detector_registry = get_default_registry()

        # Build dim_path -> detector lookup for matching issues
        from dataraum.entropy.fix_schemas import get_schemas_for_detector

        detector_by_path = {d.dimension_path: d for d in detector_registry.get_all_detectors()}

        result: dict[str, list[dict[str, str]]] = {}
        for issue in issues:
            detector = detector_by_path.get(issue.dimension_path)
            if detector:
                actions: list[dict[str, str]] = []
                for schema in get_schemas_for_detector(detector.detector_id):
                    # Only include schemas matching actual resolution options
                    if issue.available_actions and schema.action not in issue.available_actions:
                        continue
                    action_dict: dict[str, str] = {
                        "action_name": schema.action,
                        "phase_name": schema.requires_rerun or "",
                    }
                    if schema.guidance:
                        action_dict["guidance"] = schema.guidance
                    if schema.fields:
                        # Serialize field schema as structured text for LLM
                        field_lines: list[str] = []
                        for fname, fschema in schema.fields.items():
                            parts = [f"{fname} ({fschema.type}"]
                            if fschema.required:
                                parts[0] += ", required"
                            parts[0] += ")"
                            if fschema.description:
                                parts.append(f"  {fschema.description}")
                            if fschema.enum_values:
                                parts.append(f"  values: {fschema.enum_values}")
                            if fschema.examples:
                                parts.append(f"  examples: {fschema.examples}")
                            field_lines.append("\n".join(parts))
                        action_dict["fields"] = "\n".join(field_lines)
                    actions.append(action_dict)
                if actions:
                    result[issue.dimension_path] = actions

        return result

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
            if dep_status in (PhaseStatus.COMPLETED, PhaseStatus.SKIPPED):
                # Clean outputs so the phase can't skip via should_skip()
                # when upstream data changed.
                cleanup_phase(dep_name, self.source_id, self.session, self.duckdb_conn)
                self._state[dep_name] = PhaseStatus.PENDING
            elif dep_status == PhaseStatus.FAILED:
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
