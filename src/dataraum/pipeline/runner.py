#!/usr/bin/env python3
"""Simple pipeline runner.

Run the pipeline against CSV data from the command line.
This module can be used as a script or imported for programmatic use.

Usage:
    # Run against a directory of CSVs
    python -m dataraum.pipeline.runner /path/to/csv/directory

    # Run against a single CSV
    python -m dataraum.pipeline.runner /path/to/file.csv

    # Specify output directory
    python -m dataraum.pipeline.runner /path/to/data --output ./data_output

    # Run specific phase only
    python -m dataraum.pipeline.runner /path/to/data --phase import

    # Skip LLM phases
    python -m dataraum.pipeline.runner /path/to/data --skip-llm
"""

from __future__ import annotations

import hashlib
import json
import sys
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy import select

from dataraum.core.config import load_phase_config, load_pipeline_config
from dataraum.core.connections import ConnectionConfig, ConnectionManager
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.pipeline.base import Phase
from dataraum.pipeline.db_models import PhaseLog, PipelineRun
from dataraum.pipeline.events import EventCallback, EventType, PipelineEvent  # noqa: F401
from dataraum.pipeline.registry import get_all_dependencies, get_registry
from dataraum.pipeline.scheduler import (
    PipelineResult,
    PipelineScheduler,
    Resolution,
    ResolutionAction,
)
from dataraum.storage import Source

logger = get_logger(__name__)


class GateMode(str, Enum):
    """How the pipeline handles entropy gates."""

    SKIP = "skip"  # Log warning, continue (backward compatible)
    PAUSE = "pause"  # Return GateBlockedResult, pipeline state saved
    FAIL = "fail"  # Treat as pipeline failure
    AUTO_FIX = "auto_fix"  # Attempt automatic fix, fall back to skip


@dataclass
class RunConfig:
    """Configuration for a pipeline run.

    Either source_path or registered sources (resolved from the output DB)
    must be available. When source_path is None, the runner queries
    registered sources from the output database.
    """

    source_path: Path | None = None
    output_dir: Path = field(default_factory=lambda: Path("./pipeline_output"))
    source_name: str | None = None
    target_phase: str | None = None
    force_phase: bool = False
    event_callback: EventCallback | None = None

    # Gate configuration
    gate_mode: GateMode = GateMode.SKIP
    contract: str | None = None  # Target contract name


@dataclass
class PhaseRunResult:
    """Result of a single phase execution."""

    phase_name: str
    status: str  # completed, failed, skipped
    duration_seconds: float = 0.0
    error: str | None = None

    # Entropy scores from post-verification
    post_verification_scores: dict[str, float] = field(default_factory=dict)


@dataclass
class RunResult:
    """Result of a pipeline run.

    Contains all information needed for CLI display:
    - Overall success/failure
    - Per-phase detailed results
    - Timing and metrics
    - Output locations
    """

    success: bool
    source_id: str
    duration_seconds: float
    phases: list[PhaseRunResult] = field(default_factory=list)
    output_dir: Path | None = None
    error: str | None = None  # Overall error (exception during setup, etc.)

    # Entropy scores
    final_entropy_scores: dict[str, float] = field(default_factory=dict)

    @property
    def phases_completed(self) -> int:
        """Count of successfully completed phases."""
        return sum(1 for p in self.phases if p.status == "completed")

    @property
    def phases_failed(self) -> int:
        """Count of failed phases."""
        return sum(1 for p in self.phases if p.status == "failed")

    @property
    def phases_skipped(self) -> int:
        """Count of skipped phases."""
        return sum(1 for p in self.phases if p.status == "skipped")

    def get_failed_phases(self) -> list[PhaseRunResult]:
        """Get all failed phases with their errors."""
        return [p for p in self.phases if p.status == "failed"]

    def get_phase_summary(self) -> dict[str, str]:
        """Get phase name -> status mapping for simple display."""
        return {p.phase_name: p.status for p in self.phases}

    def get_slowest_phases(self, n: int = 5) -> list[tuple[str, float]]:
        """Get the N slowest phases by duration."""
        sorted_phases = sorted(self.phases, key=lambda p: p.duration_seconds, reverse=True)
        return [(p.phase_name, p.duration_seconds) for p in sorted_phases[:n]]



def _compute_source_set_fingerprint(sources: list[dict[str, Any]]) -> str:
    """Compute a SHA-256 fingerprint of the registered source set.

    Changes in source configuration (name, type, path, backend) will
    produce a different fingerprint, triggering a full pipeline re-run.
    """
    # Sort by name for deterministic ordering
    normalized = sorted(
        (s["name"], s["source_type"], json.dumps(s.get("connection_config", {}), sort_keys=True))
        for s in sources
    )
    return hashlib.sha256(json.dumps(normalized).encode()).hexdigest()[:16]


def _resolve_registered_sources(manager: ConnectionManager) -> list[dict[str, Any]] | None:
    """Query registered sources from the output database.

    Returns:
        List of source dicts suitable for the import phase, or None if no sources registered.
    """
    with manager.session_scope() as session:
        stmt = (
            select(Source)
            .where(
                Source.status.in_(["configured", "validated"]),
                Source.archived_at.is_(None),
            )
            .order_by(Source.name)
        )
        sources = session.execute(stmt).scalars().all()

        if not sources:
            return None

        result = []
        for s in sources:
            entry: dict[str, Any] = {
                "name": s.name,
                "source_type": s.source_type,
                "connection_config": s.connection_config or {},
            }
            # File sources have path in connection_config
            if s.connection_config and "path" in s.connection_config:
                entry["path"] = s.connection_config["path"]
            # Database sources have backend
            if s.backend:
                entry["backend"] = s.backend
            if s.credential_ref:
                entry["credential_ref"] = s.credential_ref
            # Include table filter if present
            if s.connection_config and "tables" in s.connection_config:
                entry["tables"] = s.connection_config["tables"]

            result.append(entry)

        return result


def _check_fingerprint_changed(
    manager: ConnectionManager,
    source_id: str,
    new_fingerprint: str,
) -> bool:
    """Check if the source set fingerprint changed.

    The scheduler's should_skip() handles resume via DB queries, so we
    don't need to invalidate anything here — just detect the change.

    Returns:
        True if fingerprint changed (full re-run needed), False otherwise.
    """
    with manager.session_scope() as session:
        stmt = (
            select(PipelineRun)
            .where(PipelineRun.source_id == source_id)
            .order_by(PipelineRun.started_at.desc())
            .limit(1)
        )
        last_run = session.execute(stmt).scalar_one_or_none()

        if last_run is None:
            return True  # No previous run, will do full run

        old_fingerprint = (last_run.config or {}).get("source_set_fingerprint")
        if old_fingerprint == new_fingerprint:
            return False

        logger.debug(
            "source_set_changed",
            source_id=source_id,
            old_fingerprint=old_fingerprint,
            new_fingerprint=new_fingerprint,
        )
        return True


def run(config: RunConfig) -> Result[RunResult]:
    """Run the pipeline with the given configuration.

    Args:
        config: Run configuration

    Returns:
        Result containing RunResult. The Result is always Ok unless there's
        an exception during setup. Check RunResult.success for pipeline outcome.
        Warnings contain any phase failure messages.
    """
    start_time = time.time()
    warnings: list[str] = []
    source_id = ""

    try:
        # Setup connection manager
        config.output_dir.mkdir(parents=True, exist_ok=True)
        conn_config = ConnectionConfig.for_directory(config.output_dir)
        manager = ConnectionManager(conn_config)
        manager.initialize()

        # Determine mode: single-path (legacy) or multi-source
        multi_source_mode = config.source_path is None
        registered_sources: list[dict[str, Any]] | None = None

        if multi_source_mode:
            registered_sources = _resolve_registered_sources(manager)
            if not registered_sources:
                return Result.ok(
                    RunResult(
                        success=False,
                        source_id="",
                        duration_seconds=time.time() - start_time,
                        error="No registered sources found. Use add_source first.",
                    ),
                    warnings=["No registered sources found"],
                )

        # Resolve source_id
        if multi_source_mode:
            source_id = str(
                uuid4()
                if not config.output_dir.name
                else hashlib.md5(str(config.output_dir.resolve()).encode()).hexdigest()[:32]
            )
            with manager.session_scope() as session:
                existing = session.execute(
                    select(Source).where(Source.name == "multi_source")
                ).scalar_one_or_none()
                if existing:
                    source_id = existing.source_id
        else:
            assert config.source_path is not None
            source_name = config.source_name or config.source_path.stem
            with manager.session_scope() as session:
                existing_source = session.execute(
                    select(Source).where(Source.name == source_name)
                ).scalar_one_or_none()

                if existing_source:
                    source_id = existing_source.source_id
                    logger.debug(
                        "using_existing_source",
                        source_name=source_name,
                        source_id=source_id,
                    )
                else:
                    source_id = str(uuid4())
                    logger.debug(
                        "creating_new_source",
                        source_name=source_name,
                        source_id=source_id,
                    )

        # Fingerprint check for multi-source mode
        if multi_source_mode and registered_sources:
            fingerprint = _compute_source_set_fingerprint(registered_sources)
            changed = _check_fingerprint_changed(manager, source_id, fingerprint)
            if changed:
                logger.debug("source_set_fingerprint_changed", fingerprint=fingerprint)
        else:
            fingerprint = None

        logger.debug(
            "pipeline_run_started",
            source_path=str(config.source_path) if config.source_path else "(registered sources)",
            output_dir=str(config.output_dir),
            source_id=source_id,
            target_phase=config.target_phase,
        )

        # Load pipeline configuration from YAML
        pipeline_yaml_config = load_pipeline_config()

        # Load per-phase configs
        active_phase_names = pipeline_yaml_config.get("phases", [])
        phase_configs = {name: load_phase_config(name) for name in active_phase_names}

        # Build runtime config passed to every phase
        if multi_source_mode and registered_sources:
            runtime_config: dict[str, Any] = {
                "source_name": "multi_source",
                "registered_sources": registered_sources,
                "source_set_fingerprint": fingerprint,
            }
        else:
            assert config.source_path is not None
            runtime_config = {
                "source_path": str(config.source_path),
                "source_name": config.source_name or config.source_path.stem,
            }

        # Build phase dict from registry
        registry = get_registry()
        phases: dict[str, Phase] = {name: cls() for name, cls in registry.items()}

        # Filter phases if target_phase is set
        if config.target_phase:
            deps = get_all_dependencies(config.target_phase)
            keep = deps | {config.target_phase}
            phases = {n: p for n, p in phases.items() if n in keep}

        # Load contract thresholds
        thresholds: dict[str, float] = {}
        if config.contract:
            from dataraum.entropy.contracts import get_contract

            contract_obj = get_contract(config.contract)
            if contract_obj:
                thresholds = contract_obj.dimension_thresholds

        # Create PipelineRun record
        session = manager.get_session()
        duckdb_conn = manager._duckdb_conn  # noqa: SLF001
        run_id = str(uuid4())
        run_record = PipelineRun(
            run_id=run_id,
            source_id=source_id,
            status="running",
            config={
                "target_phase": config.target_phase,
                "force_phase": config.force_phase,
                "source_set_fingerprint": fingerprint,
            },
        )
        session.add(run_record)
        session.flush()

        # Force-clean target phase before scheduling
        if config.force_phase and config.target_phase:
            from dataraum.pipeline.cleanup import cleanup_phase

            cleanup_phase(config.target_phase, source_id, session, duckdb_conn)
            session.flush()

        # Create fix executor
        from dataraum.entropy.fix_executor import FixExecutor, get_default_action_registry

        action_registry = get_default_action_registry()
        fix_executor = FixExecutor(action_registry)

        # Create scheduler
        scheduler = PipelineScheduler(
            phases=phases,
            source_id=source_id,
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            contract_thresholds=thresholds,
            fix_executor=fix_executor,
            phase_configs=phase_configs,
            runtime_config=runtime_config,
        )

        # Drive the generator — collect events, auto-defer gates
        gen = scheduler.run()
        collected_events: list[PipelineEvent] = []
        pipeline_result: PipelineResult | None = None

        try:
            event = next(gen)
            while True:
                collected_events.append(event)

                # Forward to event callback if provided
                if config.event_callback:
                    try:
                        config.event_callback(event)
                    except Exception:
                        pass  # Never let callback failures break the pipeline

                if event.event_type == EventType.EXIT_CHECK:
                    # Resolve based on gate_mode
                    resolution = _resolve_exit_check(config.gate_mode, event)
                    event = gen.send(resolution)
                else:
                    event = next(gen)
        except StopIteration as e:
            pipeline_result = e.value

        if pipeline_result is None:
            pipeline_result = PipelineResult(
                success=False,
                phases_completed=[],
                phases_failed=[],
                phases_skipped=[],
                final_scores={},
                deferred_issues=[],
                error="Generator ended without returning a result",
            )

        duration = time.time() - start_time

        # Read phase logs for detailed results
        logs_stmt = select(PhaseLog).where(PhaseLog.run_id == run_id)
        phase_logs = {
            log.phase_name: log for log in session.execute(logs_stmt).scalars().all()
        }

        # Build phase results from logs
        phase_results: list[PhaseRunResult] = []
        all_phase_names = (
            pipeline_result.phases_completed
            + pipeline_result.phases_failed
            + pipeline_result.phases_skipped
        )
        for phase_name in all_phase_names:
            log = phase_logs.get(phase_name)
            phase_results.append(
                PhaseRunResult(
                    phase_name=phase_name,
                    status=log.status if log else "unknown",
                    duration_seconds=log.duration_seconds if log else 0.0,
                    error=log.error if log else None,
                    post_verification_scores=log.entropy_scores or {} if log else {},
                )
            )
            if log and log.status == "failed" and log.error:
                warnings.append(f"{phase_name} failed: {log.error}")

        # Commit session and close connections
        session.commit()
        manager.close()

        logger.debug(
            "pipeline_run_completed",
            source_id=source_id,
            phases_completed=len(pipeline_result.phases_completed),
            phases_failed=len(pipeline_result.phases_failed),
            phases_skipped=len(pipeline_result.phases_skipped),
            duration_seconds=round(duration, 2),
            success=pipeline_result.success,
        )

        run_result = RunResult(
            success=pipeline_result.success,
            source_id=source_id,
            duration_seconds=duration,
            phases=phase_results,
            output_dir=config.output_dir,
            final_entropy_scores=pipeline_result.final_scores,
            error=pipeline_result.error,
        )

        return Result.ok(run_result, warnings=warnings if warnings else None)

    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            "pipeline_run_failed",
            source_id=source_id,
            error=str(e),
            duration_seconds=round(duration, 2),
        )

        run_result = RunResult(
            success=False,
            source_id=source_id,
            duration_seconds=duration,
            phases=[],
            output_dir=config.output_dir,
            error=str(e),
        )
        return Result.ok(run_result, warnings=[f"Pipeline error: {e}"])


def _resolve_exit_check(gate_mode: GateMode, event: PipelineEvent) -> Resolution:
    """Resolve an EXIT_CHECK event for programmatic callers.

    Args:
        gate_mode: How the caller wants gates handled.
        event: The EXIT_CHECK event with violations.

    Returns:
        Resolution for the scheduler.
    """
    if gate_mode == GateMode.FAIL:
        return Resolution(action=ResolutionAction.ABORT)
    # SKIP, PAUSE (non-interactive — can't prompt), AUTO_FIX (no targets available)
    # all fall back to DEFER in programmatic context.
    return Resolution(action=ResolutionAction.DEFER)


def _print_run_result(run_result: RunResult, config: RunConfig, warnings: list[str]) -> None:
    """Print run result to console.

    This is the CLI output layer - all user-facing output goes here.
    """
    print()
    print("Pipeline Run")
    print("=" * 60)
    print(f"Source: {config.source_path or '(registered sources)'}")
    print(f"Output: {config.output_dir}")
    print(f"Source ID: {run_result.source_id}")

    if config.target_phase:
        print(f"Target Phase: {config.target_phase}")

    # Show per-phase results
    if run_result.phases:
        print()
        print("Phase Results")
        print("-" * 60)
        for phase in run_result.phases:
            status_icon = {"completed": "✓", "failed": "✗", "skipped": "○"}.get(phase.status, "?")
            duration_str = f" ({phase.duration_seconds:.1f}s)" if phase.duration_seconds > 0 else ""
            print(f"  {status_icon} {phase.phase_name}: {phase.status}{duration_str}")
            if phase.error:
                print(f"      Error: {phase.error}")

    # Summary
    print()
    print("Summary")
    print("-" * 60)
    print(f"  Completed: {run_result.phases_completed}")
    print(f"  Failed: {run_result.phases_failed}")
    print(f"  Skipped: {run_result.phases_skipped}")
    print(f"  Duration: {run_result.duration_seconds:.2f}s")

    # Output files
    if run_result.output_dir:
        print()
        print("Output files:")
        print(f"  Metadata: {run_result.output_dir / 'metadata.db'}")
        print(f"  Data: {run_result.output_dir / 'data.duckdb'}")

    # Overall error (exception during setup)
    if run_result.error:
        print()
        print(f"Error: {run_result.error}")

    # Warnings from phase failures
    if warnings:
        print()
        print("Warnings:")
        for warning in warnings:
            print(f"  - {warning}")

    print()


def main() -> int:
    """Main entry point for command-line usage."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Run the dataraum-context pipeline on CSV data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s /path/to/csv/directory
  %(prog)s /path/to/file.csv --output ./my_output
  %(prog)s /path/to/data --phase import --skip-llm
        """,
    )

    parser.add_argument(
        "source",
        type=Path,
        nargs="?",
        default=None,
        help="Path to CSV file or directory containing CSV files. "
        "When omitted, uses registered sources from the output database.",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=Path("./pipeline_output"),
        help="Output directory for database files (default: ./pipeline_output)",
    )
    parser.add_argument(
        "--name",
        "-n",
        type=str,
        default=None,
        help="Name for the data source (default: derived from path)",
    )
    parser.add_argument(
        "--phase",
        "-p",
        type=str,
        default=None,
        help="Run only this phase and its dependencies",
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Suppress output",
    )

    args = parser.parse_args()

    # Validate source path if provided
    if args.source is not None and not args.source.exists():
        logger.error("source_path_not_found", path=str(args.source))
        print(f"Error: Source path does not exist: {args.source}")
        return 1

    config = RunConfig(
        source_path=args.source,
        output_dir=args.output,
        source_name=args.name,
        target_phase=args.phase,
    )

    # Run pipeline - always returns Result.ok with RunResult
    result = run(config)
    run_result = result.unwrap()

    # Print results (CLI output layer)
    if not args.quiet:
        _print_run_result(run_result, config, result.warnings)

    return 0 if run_result.success else 1


if __name__ == "__main__":
    sys.exit(main())
