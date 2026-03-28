"""Public API for applying fixes and re-running the pipeline.

Routes fixes by schema routing field:
- preprocess: ``cleanup_phase_cascade()`` + ``pipeline_run()``
- postprocess: MetadataInterpreter patches DB directly, measured at next run
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dataraum.core.logging import get_logger
from dataraum.pipeline.fixes.models import DataFix, FixDocument

logger = get_logger(__name__)


def _detector_ids_for_gate(target_phase: str) -> list[str]:
    """Collect detector IDs from all phases preceding (and including) a gate.

    Loads YAML declarations and walks the dependency chain to find all
    phases that contribute detectors before the given gate phase.
    """
    from dataraum.pipeline.pipeline_config import (
        get_all_dependencies_from_declarations,
        load_phase_declarations,
    )

    declarations = load_phase_declarations()
    # Include all transitive dependencies + the gate phase itself
    deps = get_all_dependencies_from_declarations(target_phase, declarations)
    deps.add(target_phase)

    ids: list[str] = []
    seen: set[str] = set()
    for name in declarations:
        if name in deps:
            for d_id in declarations[name].detectors:
                if d_id not in seen:
                    ids.append(d_id)
                    seen.add(d_id)
    return ids


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

    Looks up each fix action's FixSchema from the YAML fix schema loader.
    Only preprocess schemas (those with ``requires_rerun``) contribute —
    postprocess schemas have ``requires_rerun=None`` and are skipped.
    """
    from dataraum.entropy.fix_schemas import get_fix_schema

    phases: set[str] = set()

    for doc in fix_documents:
        schema = get_fix_schema(doc.action, dimension_path=doc.dimension)
        if schema and schema.requires_rerun:
            phases.add(schema.requires_rerun)

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
    and pipeline_run.

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
    from dataraum.entropy.measurement import measure_entropy
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
        # Determine routing: are there preprocess fixes?
        rerun_phases = _determine_rerun_phases(fix_documents)
        has_preprocess = bool(rerun_phases)

        # 1. Snapshot measurement BEFORE, apply fixes
        with manager.session_scope() as session:
            source = _get_source(session)

            measurement_before = measure_entropy(
                session,
                source.source_id,
                _detector_ids_for_gate(target_phase),
            )

            applied = apply_and_persist(
                source.source_id,
                fix_documents,
                session=session,
                config_root=config_root,
            )

            if has_preprocess:
                # Cascade-clean for preprocess fixes
                for phase_name in sorted(rerun_phases):
                    logger.info("fix_api_cascade_clean", phase=phase_name)
                    assert manager._duckdb_conn is not None  # set by initialize()
                    cleanup_phase_cascade(
                        phase_name,
                        source.source_id,
                        session,
                        manager._duckdb_conn,
                    )
            # Postprocess-only: MetadataInterpreter already patched
            # DB rows directly — no config intermediary needed.

            session.commit()

        manager.close()

        if has_preprocess:
            # 2. Re-run pipeline — scheduler picks up PENDING phases,
            #    DataFixesPhase replays metadata fixes, gate re-measures
            reset_config_root()
            set_config_root(config_root)
            clear_entropy_config_cache()

            run_result = pipeline_run(
                RunConfig(
                    source_path=source_path,
                    output_dir=output_dir,
                    target_phase=target_phase,
                    contract=contract,
                )
            ).unwrap()

            logger.info(
                "fix_api_rerun_done",
                success=run_result.success,
                phases_completed=run_result.phases_completed,
            )

        # 3. Read measurement AFTER
        reset_config_root()
        set_config_root(config_root)
        clear_entropy_config_cache()

        manager2 = ConnectionManager(ConnectionConfig.for_directory(output_dir))
        manager2.initialize()
        try:
            with manager2.session_scope() as session2:
                source2 = _get_source(session2)
                measurement_after = measure_entropy(
                    session2,
                    source2.source_id,
                    _detector_ids_for_gate(target_phase),
                )
        finally:
            manager2.close()

        return ApplyFixResult(
            applied_fixes=applied,
            phases_rerun=sorted(rerun_phases),
            gate_before=dict(measurement_before.column_details),
            gate_after=dict(measurement_after.column_details),
        )

    except Exception as e:
        logger.error("fix_api_failed", error=str(e))
        return ApplyFixResult(success=False, error=str(e))

    finally:
        reset_config_root()
        clear_entropy_config_cache()
