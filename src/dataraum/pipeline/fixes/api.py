"""Public API for applying fixes and re-running the pipeline.

Thin wrapper around existing components: ``apply_and_persist()`` creates
DataFix records, ``cleanup_phase_cascade()`` clears affected phases,
``pipeline_run()`` re-executes through quality_review, and
``persist_gate_result()`` writes scores to PhaseLog.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dataraum.core.logging import get_logger
from dataraum.pipeline.fixes.models import DataFix, FixDocument

logger = get_logger(__name__)

_ZONE1_ANALYSES = None  # lazy singleton
_ZONE2_ANALYSES = None  # lazy singleton


def _zone1_analyses() -> set[Any]:
    """Return the set of Zone 1 analysis keys."""
    global _ZONE1_ANALYSES  # noqa: PLW0603
    if _ZONE1_ANALYSES is None:
        from dataraum.entropy.dimensions import AnalysisKey

        _ZONE1_ANALYSES = {
            AnalysisKey.TYPING,
            AnalysisKey.STATISTICS,
            AnalysisKey.RELATIONSHIPS,
            AnalysisKey.SEMANTIC,
        }
    return _ZONE1_ANALYSES


def _zone2_analyses() -> set[Any]:
    """Return the set of Zone 1 + Zone 2 analysis keys."""
    global _ZONE2_ANALYSES  # noqa: PLW0603
    if _ZONE2_ANALYSES is None:
        from dataraum.entropy.dimensions import AnalysisKey

        _ZONE2_ANALYSES = _zone1_analyses() | {
            AnalysisKey.CORRELATION,
            AnalysisKey.DRIFT_SUMMARIES,
            AnalysisKey.SLICE_VARIANCE,
            AnalysisKey.COLUMN_QUALITY_REPORTS,
            AnalysisKey.ENRICHED_VIEW,
        }
    return _ZONE2_ANALYSES


def _analyses_for_gate(target_phase: str) -> set[Any]:
    """Return analysis keys available at the given gate."""
    if target_phase == "analysis_review":
        return _zone2_analyses()
    return _zone1_analyses()


@dataclass
class ApplyFixResult:
    """Result of applying fixes and re-running the pipeline."""

    applied_fixes: list[DataFix] = field(default_factory=list)
    phases_rerun: list[str] = field(default_factory=list)
    gate_before: dict[str, dict[str, float]] = field(default_factory=dict)
    gate_after: dict[str, dict[str, float]] = field(default_factory=dict)
    success: bool = True
    error: str | None = None


def _get_source(session: Any) -> Any:
    """Query the single Source from the database."""
    from sqlalchemy import select

    from dataraum.storage import Source

    source = session.execute(select(Source)).scalars().first()
    if not source:
        raise RuntimeError("No source found in output database")
    return source


def _determine_rerun_phases(fix_documents: list[FixDocument]) -> set[str]:
    """Determine which phases need re-running based on fix actions.

    Looks up each fix action's FixSchema from the detector registry
    and collects ``requires_rerun`` values. Gate-only phases (quality_review,
    semantic) are excluded — measure_at_gate handles them directly.
    """
    from dataraum.entropy.detectors.base import get_default_registry

    gate_only_phases = {"quality_review", "analysis_review", "semantic"}
    registry = get_default_registry()
    phases: set[str] = set()

    for doc in fix_documents:
        # Check all detectors' fix schemas for a matching action
        for detector in registry.get_all_detectors():
            for schema in detector.fix_schemas:
                if schema.action == doc.action and schema.requires_rerun:
                    if schema.requires_rerun not in gate_only_phases:
                        phases.add(schema.requires_rerun)
                    break

    return phases


def apply_fixes(
    output_dir: Path,
    fix_documents: list[FixDocument],
    *,
    source_path: Path | None = None,
    contract: str | None = "aggregation_safe",
    target_phase: str = "quality_review",
) -> ApplyFixResult:
    """Apply fixes to a pipeline output and re-run affected phases.

    This is the main entry point for programmatic fix application.
    Wraps existing components: apply_and_persist, cleanup_phase_cascade,
    pipeline_run, and persist_gate_result.

    Args:
        output_dir: Pipeline output directory to fix.
        fix_documents: Fix documents to apply.
        source_path: Original source data path (needed for pipeline re-runs).
        contract: Contract name for gate evaluation.
        target_phase: Gate phase to re-measure (quality_review or analysis_review).

    Returns:
        ApplyFixResult with before/after gate scores.
    """
    from dataraum.core.config import reset_config_root, set_config_root
    from dataraum.core.connections import ConnectionConfig, ConnectionManager
    from dataraum.entropy.config import clear_entropy_config_cache
    from dataraum.entropy.gate import measure_at_gate, persist_gate_result
    from dataraum.pipeline.cleanup import cleanup_phase_cascade
    from dataraum.pipeline.fixes.interpreters import apply_and_persist
    from dataraum.pipeline.runner import RunConfig
    from dataraum.pipeline.runner import run as pipeline_run

    config_root = output_dir / "config"
    set_config_root(config_root)
    clear_entropy_config_cache()

    manager = ConnectionManager(ConnectionConfig.for_directory(output_dir))
    manager.initialize()

    try:
        # 1. Snapshot gate BEFORE, apply fixes, cascade-clean
        with manager.session_scope() as session:
            source = _get_source(session)

            gate_before = measure_at_gate(
                session,
                manager._duckdb_conn,
                source.source_id,
                _analyses_for_gate(target_phase),
            )

            applied = apply_and_persist(
                source.source_id,
                fix_documents,
                session=session,
                config_root=config_root,
                duckdb_conn=manager._duckdb_conn,
            )

            rerun_phases = _determine_rerun_phases(fix_documents)
            for phase_name in sorted(rerun_phases):
                logger.info("fix_api_cascade_clean", phase=phase_name)
                assert manager._duckdb_conn is not None  # set by initialize()
                cleanup_phase_cascade(
                    phase_name,
                    source.source_id,
                    session,
                    manager._duckdb_conn,
                )
            session.commit()

        manager.close()

        # 2. Re-run pipeline — scheduler picks up PENDING phases,
        #    DataFixesPhase replays metadata fixes, gate re-measures
        reset_config_root()
        set_config_root(config_root)
        clear_entropy_config_cache()

        run_result = pipeline_run(
            RunConfig(
                source_path=source_path,
                output_dir=output_dir,
                target_phase="quality_review",
                contract=contract,
            )
        ).unwrap()

        logger.info(
            "fix_api_rerun_done",
            success=run_result.success,
            phases_completed=run_result.phases_completed,
        )

        # 3. Read gate AFTER
        reset_config_root()
        set_config_root(config_root)
        clear_entropy_config_cache()

        manager2 = ConnectionManager(ConnectionConfig.for_directory(output_dir))
        manager2.initialize()
        try:
            with manager2.session_scope() as session2:
                source2 = _get_source(session2)
                gate_after = measure_at_gate(
                    session2,
                    manager2._duckdb_conn,
                    source2.source_id,
                    _analyses_for_gate(target_phase),
                )
                persist_gate_result(session2, source2.source_id, gate_after)
        finally:
            manager2.close()

        return ApplyFixResult(
            applied_fixes=applied,
            phases_rerun=sorted(rerun_phases),
            gate_before=dict(gate_before.column_details),
            gate_after=dict(gate_after.column_details),
        )

    except Exception as e:
        logger.error("fix_api_failed", error=str(e))
        return ApplyFixResult(success=False, error=str(e))

    finally:
        reset_config_root()
        clear_entropy_config_cache()
