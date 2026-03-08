"""Shared pipeline setup logic.

Single source of truth for pipeline initialization, used by both the
programmatic runner and the CLI command.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.core.config import (
    _get_config_root,
    load_phase_config,
    load_pipeline_config,
    set_config_root,
)
from dataraum.core.connections import ConnectionConfig, ConnectionManager
from dataraum.core.logging import get_logger
from dataraum.pipeline.base import Phase
from dataraum.pipeline.db_models import PipelineRun
from dataraum.pipeline.registry import get_all_dependencies, get_registry
from dataraum.pipeline.scheduler import PipelineScheduler
from dataraum.storage import Source

logger = get_logger(__name__)


@dataclass
class PipelineSetup:
    """Result of pipeline initialization."""

    scheduler: PipelineScheduler
    manager: ConnectionManager
    session: Session
    source_id: str
    run_id: str
    contract_name: str | None = None
    contract_thresholds: dict[str, float] = field(default_factory=dict)


def setup_pipeline(
    *,
    source_path: Path | None,
    output_dir: Path,
    source_name: str | None = None,
    target_phase: str | None = None,
    force_phase: bool = False,
    contract: str | None = None,
) -> PipelineSetup:
    """Initialize the pipeline: connections, source resolution, scheduler.

    Args:
        source_path: Path to CSV/Parquet file or directory. None for registered sources.
        output_dir: Directory for pipeline output databases.
        source_name: Override source name (default: derived from path).
        target_phase: Run only this phase and its dependencies.
        force_phase: Force re-run of target phase.
        contract: Target contract name for gate evaluation.

    Returns:
        PipelineSetup with scheduler ready to run.
    """
    # 1. Initialize storage
    output_dir.mkdir(parents=True, exist_ok=True)
    conn_config = ConnectionConfig.for_directory(output_dir)
    manager = ConnectionManager(conn_config)
    manager.initialize()

    # 2. Per-source config: copy global config to output_dir on first run
    _ensure_source_config(output_dir)

    # 3. Determine mode: single-path or multi-source
    multi_source_mode = source_path is None
    registered_sources: list[dict[str, Any]] | None = None

    if multi_source_mode:
        registered_sources = _resolve_registered_sources(manager)

    # 4. Resolve source_id
    source_id = _resolve_source_id(
        manager=manager,
        source_path=source_path,
        source_name=source_name,
        multi_source_mode=multi_source_mode,
        output_dir=output_dir,
    )

    # 5. Fingerprint check for multi-source mode
    fingerprint: str | None = None
    if multi_source_mode and registered_sources:
        fingerprint = _compute_source_set_fingerprint(registered_sources)
        changed = _check_fingerprint_changed(manager, source_id, fingerprint)
        if changed:
            logger.debug("source_set_fingerprint_changed", fingerprint=fingerprint)

    # 6. Load pipeline and phase configs (from source-specific copy)
    pipeline_yaml_config = load_pipeline_config()
    active_phase_names = pipeline_yaml_config.get("phases", [])
    phase_configs = {name: load_phase_config(name) for name in active_phase_names}

    # 7. Build runtime config
    runtime_config: dict[str, Any]
    if multi_source_mode and registered_sources:
        runtime_config = {
            "source_name": "multi_source",
            "registered_sources": registered_sources,
            "source_set_fingerprint": fingerprint,
        }
    elif multi_source_mode:
        runtime_config = {"source_name": "multi_source"}
    else:
        assert source_path is not None
        runtime_config = {
            "source_path": str(source_path),
            "source_name": source_name or source_path.stem,
        }

    # 8. Load phases from registry
    registry = get_registry()
    phases: dict[str, Phase] = {name: cls() for name, cls in registry.items()}

    # 9. Filter phases if --phase set
    if target_phase:
        deps = get_all_dependencies(target_phase)
        keep = deps | {target_phase}
        phases = {n: p for n, p in phases.items() if n in keep}

    # 10. Load contract thresholds
    thresholds: dict[str, float] = {}
    if contract:
        from dataraum.entropy.contracts import get_contract

        contract_obj = get_contract(contract)
        if contract_obj:
            thresholds = contract_obj.dimension_thresholds

    # 11. Create PipelineRun record
    session = manager.get_session()
    duckdb_conn = manager._duckdb_conn  # noqa: SLF001
    run_id = str(uuid4())
    run_record = PipelineRun(
        run_id=run_id,
        source_id=source_id,
        status="running",
        config={
            "target_phase": target_phase,
            "force_phase": force_phase,
            "source_set_fingerprint": fingerprint,
            "contract": contract,
        },
    )
    session.add(run_record)
    session.commit()  # Commit immediately to release SQLite write lock.
    # Phase sessions (via session_factory) need write access; holding an
    # uncommitted write transaction here would block them for busy_timeout.

    # 12. Force-clean target phase before scheduling
    if force_phase and target_phase:
        from dataraum.pipeline.cleanup import cleanup_phase

        assert duckdb_conn is not None
        cleanup_phase(target_phase, source_id, session, duckdb_conn)
        session.commit()

    # 13. Create scheduler
    scheduler = PipelineScheduler(
        phases=phases,
        source_id=source_id,
        run_id=run_id,
        session=session,
        duckdb_conn=duckdb_conn,
        contract_thresholds=thresholds,
        phase_configs=phase_configs,
        runtime_config=runtime_config,
        session_factory=manager.session_scope,
        manager=manager,
    )

    return PipelineSetup(
        scheduler=scheduler,
        manager=manager,
        session=session,
        source_id=source_id,
        run_id=run_id,
        contract_name=contract,
        contract_thresholds=thresholds,
    )


def _resolve_source_id(
    *,
    manager: ConnectionManager,
    source_path: Path | None,
    source_name: str | None,
    multi_source_mode: bool,
    output_dir: Path,
) -> str:
    """Resolve or create a source_id for the pipeline run."""
    if multi_source_mode:
        source_id = str(
            uuid4()
            if not output_dir.name
            else hashlib.md5(str(output_dir.resolve()).encode()).hexdigest()[:32]
        )
        with manager.session_scope() as session:
            existing = session.execute(
                select(Source).where(Source.name == "multi_source")
            ).scalar_one_or_none()
            if existing:
                source_id = existing.source_id
        return source_id

    assert source_path is not None
    resolved_name = source_name or source_path.stem
    with manager.session_scope() as session:
        existing = session.execute(
            select(Source).where(Source.name == resolved_name)
        ).scalar_one_or_none()
        if existing:
            logger.debug(
                "using_existing_source",
                source_name=resolved_name,
                source_id=existing.source_id,
            )
            return existing.source_id

    logger.debug("creating_new_source", source_name=resolved_name)
    return str(uuid4())


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
            if s.connection_config and "path" in s.connection_config:
                entry["path"] = s.connection_config["path"]
            if s.backend:
                entry["backend"] = s.backend
            if s.credential_ref:
                entry["credential_ref"] = s.credential_ref
            if s.connection_config and "tables" in s.connection_config:
                entry["tables"] = s.connection_config["tables"]

            result.append(entry)

        return result


def _compute_source_set_fingerprint(sources: list[dict[str, Any]]) -> str:
    """Compute a SHA-256 fingerprint of the registered source set."""
    normalized = sorted(
        (s["name"], s["source_type"], json.dumps(s.get("connection_config", {}), sort_keys=True))
        for s in sources
    )
    return hashlib.sha256(json.dumps(normalized).encode()).hexdigest()[:16]


def _check_fingerprint_changed(
    manager: ConnectionManager,
    source_id: str,
    new_fingerprint: str,
) -> bool:
    """Check if the source set fingerprint changed since the last run."""
    with manager.session_scope() as session:
        stmt = (
            select(PipelineRun)
            .where(PipelineRun.source_id == source_id)
            .order_by(PipelineRun.started_at.desc())
            .limit(1)
        )
        last_run = session.execute(stmt).scalar_one_or_none()

        if last_run is None:
            return True

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


def _ensure_source_config(output_dir: Path) -> None:
    """Copy global config to output_dir/config/ on first run, then activate it.

    On first run, copies the entire global config tree so that fix handlers
    can write per-source config modifications. Subsequent runs reuse the
    existing copy (preserving user's fixes).

    Args:
        output_dir: Pipeline output directory.
    """
    source_config = output_dir / "config"
    if not source_config.exists():
        global_config = _get_config_root()
        shutil.copytree(global_config, source_config)
        logger.debug(
            "source_config_copied",
            source=str(global_config),
            destination=str(source_config),
        )
    else:
        logger.debug("source_config_reused", path=str(source_config))

    # Switch config root so all load_phase_config() etc. read from source copy
    set_config_root(source_config)
