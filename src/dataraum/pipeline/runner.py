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

import sys
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from sqlalchemy import select

from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.pipeline.db_models import PhaseLog
from dataraum.pipeline.events import EventCallback, EventType, PipelineEvent  # noqa: F401
from dataraum.pipeline.scheduler import (
    PipelineResult,
    Resolution,
    ResolutionAction,
)
from dataraum.pipeline.setup import setup_pipeline

logger = get_logger(__name__)


class GateMode(str, Enum):
    """How the pipeline handles entropy gates."""

    SKIP = "skip"  # Log warning, continue (backward compatible)
    FAIL = "fail"  # Treat as pipeline failure


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

    # Phase outputs (typed tables, relationship counts, etc.)
    outputs: dict[str, Any] = field(default_factory=dict)


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
        setup = setup_pipeline(
            source_path=config.source_path,
            output_dir=config.output_dir,
            source_name=config.source_name,
            target_phase=config.target_phase,
            force_phase=config.force_phase,
            contract=config.contract,
        )
        source_id = setup.source_id
        session = setup.session

        logger.debug(
            "pipeline_run_started",
            source_path=str(config.source_path) if config.source_path else "(registered sources)",
            output_dir=str(config.output_dir),
            source_id=source_id,
            target_phase=config.target_phase,
        )

        # Drive the generator — collect events, auto-defer gates
        gen = setup.scheduler.run()
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
                phases_blocked=[],
                final_scores={},
                deferred_issues=[],
                error="Generator ended without returning a result",
            )

        duration = time.time() - start_time

        # Read phase logs for detailed results
        logs_stmt = select(PhaseLog).where(PhaseLog.run_id == setup.run_id)
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
                    outputs=log.outputs or {} if log else {},
                )
            )
            if log and log.status == "failed" and log.error:
                warnings.append(f"{phase_name} failed: {log.error}")

        # Commit session and close connections
        session.commit()
        setup.manager.close()

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
    # SKIP always defers in programmatic context.
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
            status_icon = {"completed": "\u2713", "failed": "\u2717", "skipped": "\u25cb"}.get(phase.status, "?")
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
