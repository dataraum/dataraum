#!/usr/bin/env python3
"""Simple pipeline runner.

Run the pipeline against CSV data from the command line.
This module can be used as a script or imported for programmatic use.

Usage:
    # Run against a directory of CSVs
    python -m dataraum_context.pipeline.runner /path/to/csv/directory

    # Run against a single CSV
    python -m dataraum_context.pipeline.runner /path/to/file.csv

    # Specify output directory
    python -m dataraum_context.pipeline.runner /path/to/data --output ./data_output

    # Run specific phase only
    python -m dataraum_context.pipeline.runner /path/to/data --phase import

    # Skip LLM phases
    python -m dataraum_context.pipeline.runner /path/to/data --skip-llm
"""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from uuid import uuid4

from dataraum_context.core.connections import ConnectionConfig, ConnectionManager
from dataraum_context.core.logging import get_logger
from dataraum_context.core.models.base import Result
from dataraum_context.pipeline.base import PhaseStatus
from dataraum_context.pipeline.orchestrator import Pipeline, PipelineConfig
from dataraum_context.pipeline.phases import (
    BusinessCyclesPhase,
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

logger = get_logger(__name__)

# Default junk columns to remove from CSV imports
DEFAULT_JUNK_COLUMNS = [
    "Unnamed: 0",
    "Unnamed: 0.1",
    "Unnamed: 0.2",
    "column0",
    "column00",
]


@dataclass
class RunConfig:
    """Configuration for a pipeline run."""

    source_path: Path
    output_dir: Path = field(default_factory=lambda: Path("./pipeline_output"))
    source_name: str | None = None
    target_phase: str | None = None
    skip_llm: bool = False
    junk_columns: list[str] = field(default_factory=lambda: DEFAULT_JUNK_COLUMNS.copy())


@dataclass
class PhaseRunResult:
    """Result of a single phase execution."""

    phase_name: str
    status: str  # completed, failed, skipped
    duration_seconds: float = 0.0
    error: str | None = None
    records_processed: int = 0
    records_created: int = 0


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


def create_pipeline(config: RunConfig) -> Pipeline:
    """Create and configure the pipeline.

    Args:
        config: Run configuration

    Returns:
        Configured Pipeline instance
    """
    pipeline_config = PipelineConfig(
        skip_llm_phases=config.skip_llm,
        skip_completed=True,
        fail_fast=True,
        # Parallel execution using ThreadPoolExecutor
        # NullPool is used in ConnectionManager to allow AsyncEngine across event loops
        max_parallel=4,
    )

    pipeline = Pipeline(config=pipeline_config)

    # Register available phases in dependency order
    # Foundation phases
    pipeline.register(ImportPhase())
    pipeline.register(TypingPhase())
    pipeline.register(StatisticsPhase())

    # Analysis phases
    pipeline.register(StatisticalQualityPhase())
    pipeline.register(RelationshipsPhase())
    pipeline.register(CorrelationsPhase())
    pipeline.register(TemporalPhase())
    pipeline.register(SemanticPhase())
    pipeline.register(ValidationPhase())

    # Slicing phases
    pipeline.register(SlicingPhase())
    pipeline.register(SliceAnalysisPhase())
    pipeline.register(TemporalSliceAnalysisPhase())

    # Entropy and quality phases
    pipeline.register(EntropyPhase())
    pipeline.register(EntropyInterpretationPhase())
    pipeline.register(BusinessCyclesPhase())
    pipeline.register(CrossTableQualityPhase())
    pipeline.register(QualitySummaryPhase())

    # Metric calculation (also builds execution context)
    pipeline.register(GraphExecutionPhase())

    return pipeline


def get_latest_implemented_phase(pipeline: Pipeline) -> str:
    """Get the latest phase in the DAG that has an implementation.

    This allows running the pipeline without specifying a target phase,
    limiting execution to phases that actually have implementations.

    Args:
        pipeline: Pipeline with registered phases

    Returns:
        Name of the latest implemented phase
    """
    from dataraum_context.pipeline.base import PIPELINE_DAG

    latest = None
    for phase_def in PIPELINE_DAG:
        if phase_def.name in pipeline.phases:
            latest = phase_def.name
    return latest or "import"


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
    source_id = str(uuid4())
    warnings: list[str] = []

    logger.info(
        "pipeline_run_started",
        source_path=str(config.source_path),
        output_dir=str(config.output_dir),
        source_id=source_id,
        target_phase=config.target_phase,
        skip_llm=config.skip_llm,
    )

    try:
        # Setup connection manager
        config.output_dir.mkdir(parents=True, exist_ok=True)
        conn_config = ConnectionConfig.for_directory(config.output_dir)
        manager = ConnectionManager(conn_config)
        manager.initialize()

        # Create pipeline
        pipeline = create_pipeline(config)

        # Build run configuration
        run_config = {
            "source_path": str(config.source_path),
            "source_name": config.source_name or config.source_path.stem,
            "junk_columns": config.junk_columns,
        }

        # Execute pipeline
        results = pipeline.run(
            manager=manager,
            source_id=source_id,
            target_phase=config.target_phase,
            run_config=run_config,
        )

        # Close connections
        manager.close()

        duration = time.time() - start_time

        # Build detailed phase results
        phase_results = [
            PhaseRunResult(
                phase_name=phase_name,
                status=result.status.value,
                duration_seconds=result.duration_seconds,
                error=result.error,
                records_processed=result.records_processed,
                records_created=result.records_created,
            )
            for phase_name, result in results.items()
        ]

        # Count results
        completed = sum(1 for r in results.values() if r.status == PhaseStatus.COMPLETED)
        failed = sum(1 for r in results.values() if r.status == PhaseStatus.FAILED)

        # Collect warnings from ALL phases (not just failed)
        for phase_name, result in results.items():
            # Collect phase-level warnings (e.g., "3 tables failed out of 5")
            if result.warnings:
                for warning in result.warnings:
                    warnings.append(f"{phase_name}: {warning}")
            # Also include error message from failed phases
            if result.status == PhaseStatus.FAILED and result.error:
                warnings.append(f"{phase_name} failed: {result.error}")

        logger.info(
            "pipeline_run_completed",
            source_id=source_id,
            phases_completed=completed,
            phases_failed=failed,
            phases_skipped=len(results) - completed - failed,
            duration_seconds=round(duration, 2),
            success=failed == 0,
        )

        # Log individual phase results at debug level
        for phase_name, result in results.items():
            logger.debug(
                "phase_result",
                phase=phase_name,
                status=result.status.value,
                duration_seconds=round(result.duration_seconds, 2),
                error=result.error if result.status == PhaseStatus.FAILED else None,
            )

        run_result = RunResult(
            success=failed == 0,
            source_id=source_id,
            duration_seconds=duration,
            phases=phase_results,
            output_dir=config.output_dir,
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

        # Return a Result with an error RunResult so CLI can still show partial info
        run_result = RunResult(
            success=False,
            source_id=source_id,
            duration_seconds=duration,
            phases=[],
            output_dir=config.output_dir,
            error=str(e),
        )
        return Result.ok(run_result, warnings=[f"Pipeline error: {e}"])


def _print_run_result(run_result: RunResult, config: RunConfig, warnings: list[str]) -> None:
    """Print run result to console.

    This is the CLI output layer - all user-facing output goes here.
    """
    print()
    print("Pipeline Run")
    print("=" * 60)
    print(f"Source: {config.source_path}")
    print(f"Output: {config.output_dir}")
    print(f"Source ID: {run_result.source_id}")

    if config.target_phase:
        print(f"Target Phase: {config.target_phase}")
    if config.skip_llm:
        print("LLM Phases: Skipped")

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
        help="Path to CSV file or directory containing CSV files",
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
        "--skip-llm",
        action="store_true",
        help="Skip phases that require LLM",
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Suppress output",
    )

    args = parser.parse_args()

    # Validate source path
    if not args.source.exists():
        logger.error("source_path_not_found", path=str(args.source))
        print(f"Error: Source path does not exist: {args.source}")
        return 1

    config = RunConfig(
        source_path=args.source,
        output_dir=args.output,
        source_name=args.name,
        target_phase=args.phase,
        skip_llm=args.skip_llm,
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
