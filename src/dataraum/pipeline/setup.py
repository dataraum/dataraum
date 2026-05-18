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
from dataraum.pipeline.db_models import PipelineRun
from dataraum.pipeline.pipeline_config import (
    get_all_dependencies_from_declarations,
    get_downstream_phases_from_declarations,
    load_phase_declarations,
)
from dataraum.pipeline.registry import build_yaml_aware_phases
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
    vertical: str | None = None,
    session_id: str | None = None,
) -> PipelineSetup:
    """Initialize the pipeline for a single source.

    Two callers:

    - **CLI** (``dataraum run /path/to/data``) passes ``source_path``. setup
      derives the source name from the path stem and registers (or reuses)
      a Source row in the output DB.
    - **MCP** (``measure`` after ``begin_session(source=...)``) leaves
      ``source_path`` as None. setup reads the one Source row that
      ``begin_session`` already wrote into the session DB.

    Either way, the result is one Source. Multi-source semantics, the
    synthetic ``multi_source`` row, fingerprint-of-set, and the
    ``registered_sources`` plumbing are gone (DAT-290).

    Args:
        source_path: Path to a data file, directory, or recipe yaml.
            ``None`` means "read the registered source from the session DB".
        output_dir: Directory for pipeline output databases.
        source_name: Override source name when registering from a path
            (default: derived from path stem). Ignored when ``source_path``
            is None.
        target_phase: Run only this phase and its dependencies.
        force_phase: Force re-run of target phase.
        contract: Target contract name for gate evaluation.
        vertical: Domain vertical (e.g. 'finance'). None → '_adhoc'.

    Returns:
        PipelineSetup with scheduler ready to run.
    """
    # 1. Initialize storage
    output_dir.mkdir(parents=True, exist_ok=True)
    conn_config = ConnectionConfig.for_directory(output_dir)
    manager = ConnectionManager(conn_config, session_id=session_id)
    manager.initialize()

    if session_id is None:
        raise RuntimeError(
            "setup_pipeline requires session_id post-DAT-321; per-session writes "
            "(PipelineRun, PhaseLog, EntropyObjectRecord, ...) all carry an FK to "
            "investigation_sessions.session_id. CLI flows must pass an active "
            "InvestigationSession id (or the CLI itself is going away — see L6)."
        )

    # 2. Per-source config: copy global config to output_dir on first run
    _ensure_source_config(output_dir)

    # 2b. Ensure _adhoc vertical scaffold exists (write target for induction + teach)
    _ensure_adhoc_vertical(output_dir)

    # 3. Resolve the source spec — either from the registry (MCP) or by
    # registering the path on the fly (CLI). Both yield a `_SourceSpec`.
    source_spec = _resolve_source_spec(
        manager=manager,
        source_path=source_path,
        source_name=source_name,
    )

    # 4. Per-source fingerprint, captured in the PipelineRun for traceability.
    fingerprint = _compute_source_fingerprint(
        {
            "name": source_spec.name,
            "source_type": source_spec.source_type,
            "connection_config": source_spec.connection_config,
        }
    )

    # 5. Load pipeline and phase configs (from source-specific copy)
    pipeline_yaml_config = load_pipeline_config()
    declarations = load_phase_declarations(pipeline_yaml_config)
    active_phase_names = list(declarations)
    phase_configs = {name: load_phase_config(name) for name in active_phase_names}

    # 6. Build runtime config — one source, all its info in one place.
    effective_vertical = vertical or "_adhoc"
    runtime_config: dict[str, Any] = {
        "source_id": source_spec.source_id,
        "source_name": source_spec.name,
        "source_type": source_spec.source_type,
        "source_connection_config": source_spec.connection_config,
        "source_backend": source_spec.backend,
        "source_fingerprint": fingerprint,
        "vertical": effective_vertical,
    }
    if source_path is not None:
        # CLI mode also passes source_path so the import phase can stat the file
        # before reading from the (just-registered) Source row.
        runtime_config["source_path"] = str(source_path)

    source_id = source_spec.source_id

    # 7. Load phases from YAML declarations + registry
    phases = build_yaml_aware_phases(pipeline_yaml_config)

    # 8. Filter phases if --phase set (upstream deps + target + downstream cascade)
    if target_phase:
        deps = get_all_dependencies_from_declarations(target_phase, declarations)
        downstream = get_downstream_phases_from_declarations(target_phase, declarations)
        keep = deps | {target_phase} | downstream
        phases = {n: p for n, p in phases.items() if n in keep}

    # 9. Load contract thresholds
    thresholds: dict[str, float] = {}
    if contract:
        from dataraum.entropy.contracts import get_contract

        contract_obj = get_contract(contract)
        if contract_obj:
            thresholds = contract_obj.dimension_thresholds

    # 10. Create PipelineRun record
    session = manager.get_session()
    duckdb_conn = manager._duckdb_conn  # noqa: SLF001
    run_id = str(uuid4())
    run_record = PipelineRun(
        run_id=run_id,
        session_id=session_id,
        source_id=source_id,
        status="running",
        config={
            "target_phase": target_phase,
            "force_phase": force_phase,
            "source_fingerprint": fingerprint,
            "contract": contract,
        },
    )
    session.add(run_record)
    session.commit()  # Commit immediately to release SQLite write lock.
    # Phase sessions (via session_factory) need write access; holding an
    # uncommitted write transaction here would block them for busy_timeout.

    # 11. Force-clean target phase + all downstream before scheduling
    #     Upstream deps keep their data (should_skip handles them).
    #     Target + downstream are cleaned so they re-run with fresh data.
    if force_phase and target_phase:
        from dataraum.pipeline.cleanup import cleanup_phase_cascade

        assert duckdb_conn is not None
        cleanup_phase_cascade(target_phase, source_id, session, duckdb_conn)
        session.commit()

    # 12. Create scheduler
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
        session_id=session_id,
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


@dataclass
class _SourceSpec:
    """Resolved single source for a pipeline run."""

    source_id: str
    name: str
    source_type: str
    connection_config: dict[str, Any]
    backend: str | None


def _resolve_source_spec(
    *,
    manager: ConnectionManager,
    source_path: Path | None,
    source_name: str | None,
) -> _SourceSpec:
    """Resolve the single source for this pipeline run.

    - CLI mode (``source_path`` given): derive name from path stem, get-or-create
      a Source row, persist if newly created.
    - MCP mode (``source_path`` is None): read the (one) Source row that
      ``begin_session`` wrote into the session DB.

    Raises:
        RuntimeError: in MCP mode, if the session DB has zero or multiple
            non-archived Source rows. Both are configuration bugs upstream —
            ``begin_session`` is responsible for the one-source invariant.
    """
    import re

    if source_path is not None:
        # CLI mode — register on the fly.
        raw_name = source_name or source_path.stem.lower()
        clean_name = re.sub(r"[^a-z0-9_]", "_", raw_name).strip("_") or "source"
        with manager.session_scope() as session:
            existing = session.execute(
                select(Source).where(Source.name == clean_name)
            ).scalar_one_or_none()
            if existing is not None:
                logger.debug(
                    "using_existing_source",
                    source_name=clean_name,
                    source_id=existing.source_id,
                )
                return _SourceSpec(
                    source_id=existing.source_id,
                    name=existing.name,
                    source_type=existing.source_type,
                    connection_config=existing.connection_config or {},
                    backend=existing.backend,
                )
            logger.debug("creating_new_source", source_name=clean_name)
            new_source = Source(
                source_id=str(uuid4()),
                name=clean_name,
                source_type=_infer_source_type(source_path),
                connection_config={"path": str(source_path.resolve())},
                status="configured",
            )
            session.add(new_source)
            session.flush()
            return _SourceSpec(
                source_id=new_source.source_id,
                name=new_source.name,
                source_type=new_source.source_type,
                connection_config=new_source.connection_config or {},
                backend=new_source.backend,
            )

    # MCP mode — single Source row already populated by begin_session.
    with manager.session_scope() as session:
        active = list(session.execute(select(Source).where(Source.archived_at.is_(None))).scalars())
        if not active:
            raise RuntimeError(
                "No source available in the session DB. begin_session was expected "
                "to copy one Source row from the workspace registry."
            )
        if len(active) > 1:
            names = [s.name for s in active]
            raise RuntimeError(
                f"Session DB has multiple sources ({names}). DAT-290 guarantees one "
                "source per session — this state is a bug in begin_session."
            )
        s = active[0]
        return _SourceSpec(
            source_id=s.source_id,
            name=s.name,
            source_type=s.source_type,
            connection_config=s.connection_config or {},
            backend=s.backend,
        )


def _infer_source_type(path: Path) -> str:
    """Best-effort source_type from a CLI path (CSV-default for unknowns)."""
    if path.is_dir():
        return "csv"  # mixed-format directories supported by the file loader
    suffix = path.suffix.lower()
    if suffix in {".parquet", ".pq"}:
        return "parquet"
    if suffix in {".json", ".jsonl"}:
        return "json"
    if suffix in {".yaml", ".yml"}:
        return "db_recipe"
    return "csv"


def _compute_source_fingerprint(source: dict[str, Any]) -> str:
    """Compute a SHA-256 fingerprint of a single source's identity.

    Used by ``begin_session`` to key the per-source session directory and
    by ``setup_pipeline`` to stamp the PipelineRun. A source whose name,
    type, or connection_config changes produces a different fingerprint —
    forcing a fresh session directory.
    """
    normalized = (
        source["name"],
        source["source_type"],
        json.dumps(source.get("connection_config", {}) or {}, sort_keys=True),
    )
    return hashlib.sha256(json.dumps(normalized).encode()).hexdigest()[:16]


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


def _ensure_adhoc_vertical(output_dir: Path) -> None:
    """Create the _adhoc vertical scaffold for cold-start sessions.

    The _adhoc vertical is the write target for induction agents (which
    generate ontology, cycles, validations on cold start) and for teach
    (which refines them later). Created idempotently on every pipeline setup.

    Structure mirrors config/verticals/{name}/:
        ontology.yaml, cycles.yaml, validations/, metrics/

    Args:
        output_dir: Pipeline output directory.
    """
    import yaml

    adhoc_dir = output_dir / "config" / "verticals" / "_adhoc"
    if adhoc_dir.exists():
        return

    adhoc_dir.mkdir(parents=True)

    # Empty ontology — induction agent populates this
    ontology = {
        "name": "_adhoc",
        "version": "1.0.0",
        "description": "Auto-generated",
        "concepts": [],
    }
    with open(adhoc_dir / "ontology.yaml", "w") as f:
        yaml.dump(ontology, f, default_flow_style=False, sort_keys=False)

    # Empty cycles vocabulary
    with open(adhoc_dir / "cycles.yaml", "w") as f:
        yaml.dump({"cycle_types": {}}, f, default_flow_style=False, sort_keys=False)

    # Empty directories for validations, metrics
    (adhoc_dir / "validations").mkdir()
    (adhoc_dir / "metrics").mkdir()

    logger.debug("adhoc_vertical_created", path=str(adhoc_dir))
