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
from typing import Any
from uuid import uuid4

from dataraum_context.core.connections import ConnectionConfig, ConnectionManager
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
    verbose: bool = True


@dataclass
class RunResult:
    """Result of a pipeline run."""

    success: bool
    source_id: str
    phases_completed: int
    phases_failed: int
    phases_skipped: int
    duration_seconds: float
    error: str | None = None
    phase_results: dict[str, Any] = field(default_factory=dict)


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


def run(config: RunConfig) -> RunResult:
    """Run the pipeline with the given configuration.

    Args:
        config: Run configuration

    Returns:
        RunResult with execution details
    """
    start_time = time.time()
    source_id = str(uuid4())

    if config.verbose:
        print("Pipeline Run")
        print("=" * 60)
        print(f"Source: {config.source_path}")
        print(f"Output: {config.output_dir}")
        print(f"Source ID: {source_id}")
        if config.target_phase:
            print(f"Target Phase: {config.target_phase}")
        if config.skip_llm:
            print("LLM Phases: Skipped")
        print()

    try:
        # Setup connection manager
        config.output_dir.mkdir(parents=True, exist_ok=True)
        conn_config = ConnectionConfig.for_directory(config.output_dir)
        manager = ConnectionManager(conn_config)
        manager.initialize()

        # Create pipeline
        pipeline = create_pipeline(config)

        # Use target phase if specified, otherwise run all implemented phases
        target_phase = config.target_phase

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
            target_phase=target_phase,
            run_config=run_config,
        )

        # Close connections
        manager.close()

        duration = time.time() - start_time

        # Count results
        completed = sum(1 for r in results.values() if r.status == PhaseStatus.COMPLETED)
        failed = sum(1 for r in results.values() if r.status == PhaseStatus.FAILED)
        skipped = sum(1 for r in results.values() if r.status == PhaseStatus.SKIPPED)

        if config.verbose:
            print("Results")
            print("-" * 60)
            for phase_name, result in results.items():
                status_icon = {
                    PhaseStatus.COMPLETED: "✓",
                    PhaseStatus.FAILED: "✗",
                    PhaseStatus.SKIPPED: "○",
                }.get(result.status, "?")
                print(f"  {status_icon} {phase_name}: {result.status.value}")
                if result.error and result.status == PhaseStatus.FAILED:
                    print(f"      Error: {result.error}")

            print()
            print("Summary")
            print("-" * 60)
            print(f"  Completed: {completed}")
            print(f"  Failed: {failed}")
            print(f"  Skipped: {skipped}")
            print(f"  Duration: {duration:.2f}s")
            print()
            print("Output files:")
            print(f"  Metadata: {config.output_dir / 'metadata.db'}")
            print(f"  Data: {config.output_dir / 'data.duckdb'}")

        return RunResult(
            success=failed == 0,
            source_id=source_id,
            phases_completed=completed,
            phases_failed=failed,
            phases_skipped=skipped,
            duration_seconds=duration,
            phase_results={k: v.status.value for k, v in results.items()},
        )

    except Exception as e:
        duration = time.time() - start_time
        if config.verbose:
            print(f"Error: {e}")

        return RunResult(
            success=False,
            source_id=source_id,
            phases_completed=0,
            phases_failed=1,
            phases_skipped=0,
            duration_seconds=duration,
            error=str(e),
        )


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

    if not args.source.exists():
        print(f"Error: Source path does not exist: {args.source}")
        return 1

    config = RunConfig(
        source_path=args.source,
        output_dir=args.output,
        source_name=args.name,
        target_phase=args.phase,
        skip_llm=args.skip_llm,
        verbose=not args.quiet,
    )

    result = run(config)
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
