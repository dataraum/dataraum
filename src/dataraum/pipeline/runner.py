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
from dataraum.pipeline.base import PhaseStatus
from dataraum.pipeline.db_models import PhaseCheckpoint, PipelineRun
from dataraum.pipeline.events import EventCallback, EventType  # noqa: F401
from dataraum.pipeline.orchestrator import Pipeline, PipelineConfig, ProgressCallback, get_pipeline
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
    progress_callback: ProgressCallback | None = None
    event_callback: EventCallback | None = None

    # Gate configuration
    gate_mode: GateMode = GateMode.SKIP
    contract: str | None = None  # Target contract name
    max_fix_attempts: int = 3
    gate_handler: Any | None = None  # GateHandler implementation


@dataclass
class PhaseRunResult:
    """Result of a single phase execution."""

    phase_name: str
    status: str  # completed, failed, skipped
    duration_seconds: float = 0.0
    error: str | None = None
    records_processed: int = 0
    records_created: int = 0

    # Detailed metrics
    tables_processed: int = 0
    columns_processed: int = 0
    rows_processed: int = 0
    db_queries: int = 0
    db_writes: int = 0
    timings: dict[str, float] = field(default_factory=dict)

    # Entropy / gate info
    post_verification_scores: dict[str, float] = field(default_factory=dict)
    gate_status: str = ""  # "passed" | "blocked" | "skipped" | ""


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

    # Entropy / gate summary
    final_entropy_scores: dict[str, float] = field(default_factory=dict)
    gate_events: list[dict[str, Any]] = field(default_factory=list)

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

    @property
    def total_tables_processed(self) -> int:
        """Total tables processed across all phases."""
        return max(p.tables_processed for p in self.phases) if self.phases else 0

    @property
    def total_rows_processed(self) -> int:
        """Total rows processed across all phases."""
        return max(p.rows_processed for p in self.phases) if self.phases else 0

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

    def get_bottleneck_operations(self, n: int = 5) -> list[tuple[str, str, float]]:
        """Get the N slowest operations across all phases."""
        all_ops: list[tuple[str, str, float]] = []
        for phase in self.phases:
            for op_name, duration in phase.timings.items():
                all_ops.append((phase.phase_name, op_name, duration))
        sorted_ops = sorted(all_ops, key=lambda x: x[2], reverse=True)
        return sorted_ops[:n]


def create_pipeline(config: RunConfig, pipeline_yaml: dict[str, Any] | None = None) -> Pipeline:
    """Create and configure the pipeline from YAML config + registry.

    Args:
        config: Run configuration
        pipeline_yaml: Pre-loaded YAML config (loaded if not provided)

    Returns:
        Configured Pipeline instance
    """
    if pipeline_yaml is None:
        pipeline_yaml = load_pipeline_config()

    pcfg = pipeline_yaml.get("pipeline", {})
    retry_cfg = pcfg.get("retry", {})
    pipeline_config = PipelineConfig(
        skip_completed=pcfg.get("skip_completed", True),
        fail_fast=pcfg.get("fail_fast", True),
        max_parallel=pcfg.get("max_parallel", 4),
        max_retries=retry_cfg.get("max_retries", 2),
        backoff_base=retry_cfg.get("backoff_base", 2.0),
        gate_mode=config.gate_mode.value,
        contract=config.contract,
        gate_handler=config.gate_handler,
        max_fix_attempts=config.max_fix_attempts,
    )

    # Active phases from YAML config (or all registered if not specified)
    active_phases = pipeline_yaml.get("phases", None)

    pipeline = get_pipeline(active_phases=active_phases)
    pipeline.config = pipeline_config
    return pipeline


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


def _check_fingerprint_and_invalidate(
    manager: ConnectionManager,
    source_id: str,
    new_fingerprint: str,
) -> bool:
    """Check if the source set fingerprint changed. If so, delete checkpoints.

    Returns:
        True if fingerprint changed (full re-run needed), False otherwise.
    """
    with manager.session_scope() as session:
        # Find the most recent pipeline run for this source_id
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
            return False  # Same sources, skip_completed can work

        # Fingerprint changed — delete all checkpoints for this source_id
        logger.debug(
            "source_set_changed",
            source_id=source_id,
            old_fingerprint=old_fingerprint,
            new_fingerprint=new_fingerprint,
        )
        checkpoints = session.execute(
            select(PhaseCheckpoint).where(PhaseCheckpoint.source_id == source_id)
        ).scalars().all()
        for cp in checkpoints:
            session.delete(cp)

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
            # Deterministic source_id from output directory name
            source_id = str(
                uuid4()
                if not config.output_dir.name
                else hashlib.md5(
                    str(config.output_dir.resolve()).encode()
                ).hexdigest()[:32]
            )
            # Check if we already have a source with this approach
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
            changed = _check_fingerprint_and_invalidate(manager, source_id, fingerprint)
            if changed:
                logger.debug("source_set_fingerprint_changed", fingerprint=fingerprint)
        else:
            fingerprint = None

        # Inject pipeline context into gate handler (for fix execution)
        if config.gate_handler and hasattr(config.gate_handler, "set_context"):
            config.gate_handler.set_context(manager, source_id)

        logger.info(
            "pipeline_run_started",
            source_path=str(config.source_path) if config.source_path else "(registered sources)",
            output_dir=str(config.output_dir),
            source_id=source_id,
            target_phase=config.target_phase,
        )

        # Load pipeline configuration from YAML
        pipeline_yaml_config = load_pipeline_config()

        # Create pipeline with YAML-loaded settings
        pipeline = create_pipeline(config, pipeline_yaml=pipeline_yaml_config)

        # Load per-phase configs by convention
        active_phases = pipeline_yaml_config.get("phases", [])
        phase_configs = {name: load_phase_config(name) for name in active_phases}

        # Runtime config passed to every phase
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

        # Execute pipeline
        results = pipeline.run(
            manager=manager,
            source_id=source_id,
            target_phase=config.target_phase,
            phase_configs=phase_configs,
            runtime_config=runtime_config,
            progress_callback=config.progress_callback,
            force_phase=config.force_phase,
            event_callback=config.event_callback,
        )

        duration = time.time() - start_time

        # Read detailed metrics from checkpoints
        # Checkpoints have the full metrics collected during execution
        with manager.session_scope() as session:
            stmt = select(PhaseCheckpoint).where(PhaseCheckpoint.source_id == source_id)
            checkpoint_result = session.execute(stmt)
            checkpoints = {cp.phase_name: cp for cp in checkpoint_result.scalars().all()}

        # Build detailed phase results with metrics from checkpoints
        phase_results = []
        for phase_name, result in results.items():
            checkpoint = checkpoints.get(phase_name)
            phase_results.append(
                PhaseRunResult(
                    phase_name=phase_name,
                    status=result.status.value,
                    duration_seconds=result.duration_seconds,
                    error=result.error,
                    records_processed=result.records_processed,
                    records_created=result.records_created,
                    # Detailed metrics from checkpoint
                    tables_processed=checkpoint.tables_processed if checkpoint else 0,
                    columns_processed=checkpoint.columns_processed if checkpoint else 0,
                    rows_processed=checkpoint.rows_processed if checkpoint else 0,
                    db_queries=checkpoint.db_queries if checkpoint else 0,
                    db_writes=checkpoint.db_writes if checkpoint else 0,
                    timings=checkpoint.timings if checkpoint else {},
                    # Entropy / gate info from checkpoint
                    post_verification_scores=(
                        checkpoint.entropy_hard_scores if checkpoint and checkpoint.entropy_hard_scores else {}
                    ),
                    gate_status=checkpoint.gate_status or "" if checkpoint else "",
                )
            )

        # Extract entropy and gate data from pipeline
        final_entropy_scores = pipeline._entropy_state.to_dict()
        gate_events_list: list[dict[str, Any]] = []
        for evt in pipeline._collected_events:
            if evt.event_type in (
                EventType.GATE_EVALUATED,
                EventType.GATE_BLOCKED,
                EventType.GATE_RESOLVED,
            ):
                gate_events_list.append({
                    "event_type": evt.event_type.value,
                    "phase": evt.phase,
                    "gate_status": evt.gate_status,
                    "violations": {
                        k: {"current": v[0], "threshold": v[1]}
                        for k, v in evt.violations.items()
                    },
                    "message": evt.message,
                })

        # Close connections
        manager.close()

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
            final_entropy_scores=final_entropy_scores,
            gate_events=gate_events_list,
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
