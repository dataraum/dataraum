"""Phase-runner for the Temporal activity worker (DAT-344, per-table in DAT-370).

One place wires connections to a phase. :func:`run_phase` leases a *scoped*
SQLAlchemy session + DuckDB cursor from the worker's single
:class:`~dataraum.core.connections.ConnectionManager`, builds the
``PhaseContext`` (source identity from the ``Source`` row + the phase's static
config, scoped to ``table_ids``), and runs the sync phase — without a scheduler
or ``PipelineRun``/``PhaseLog`` monitoring rows (Temporal's event history is the
execution log).

Detectors are **not** run here per phase. Per DAT-394 they run once per workflow
in a single terminal step: :func:`run_detectors` runs every wired detector over
the whole source after the fan-out + reduce. The activity wrappers
(:mod:`dataraum.worker.activities`) translate these Temporal-agnostic helpers
into the per-boundary contracts.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, select

from dataraum.core.config import load_phase_config, load_pipeline_config
from dataraum.core.logging import get_logger
from dataraum.core.vertical import require_known_vertical
from dataraum.entropy.engine import run_detector_post_step
from dataraum.entropy.readiness import persist_readiness
from dataraum.investigation.db_models import InvestigationSession
from dataraum.investigation.queries import link_session_tables, tables_for_session
from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.pipeline_config import load_phase_declarations
from dataraum.pipeline.registry import get_phase_class
from dataraum.server.workspace import get_active_workspace_id
from dataraum.storage import Column, MetadataSnapshotHead, Source, Table, session_head_target
from dataraum.worker.contracts import OperatingModelScope, SessionIdentity, SourceIdentity

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from dataraum.core.connections import ConnectionManager

logger = get_logger(__name__)

__all__ = [
    "SESSION_DETECTOR_PHASES",
    "PhaseRun",
    "begin_session_select",
    "check_run_column_limit",
    "declared_detector_ids",
    "materialize_session_overlays",
    "promote_operating_model_run",
    "promote_run",
    "promote_session_run",
    "raw_table_ids",
    "resolve_operating_model_scope",
    "run_detectors",
    "run_phase",
    "run_session_phase",
    "typed_table_id_for_raw",
    "write_session_keepers",
]

# The executed-chain phases that declare entropy detectors (DAT-394). The single
# terminal ``detect`` activity runs the union of their detectors ONCE over the
# run-session's tables — after the per-table fan-out and the ``semantic_per_column``
# reduce. Detectors moved here from the old per-table ``detect_table`` + parent
# ``detect_source`` split: nothing reads entropy mid-run, there are no live
# detector->detector ordering edges, and the split bought ~no parallelism (it ran
# two cheap structural detectors), so one terminal pass is correct and simpler.
# Running once, sequentially, makes the delete-before-insert safe (no concurrency)
# and guarantees every detector's inputs are present: ``entropy_objects`` deletes by
# ``(source_id, detector_id)`` scoped to the table set, ``entropy_readiness`` deletes
# by ``table_id.in_()`` (the session's tables, DAT-410).
#
# Source of truth is the executed chain in ``workflows.py`` (``typing`` +
# ``_ANALYTICS_PHASES`` in the child, ``semantic_per_column`` in the parent).
# ``tests/unit/worker/test_phase_constants.py`` pins that no chain-declared
# detector is orphaned (the regression eval caught when DAT-370 first split them).
_DETECTOR_PHASES = (
    "typing",
    "statistics",
    "column_eligibility",
    "statistical_quality",
    "temporal",
    "semantic_per_column",
)

# The add_source stages whose per-(table, stage) snapshot the terminal
# ``promote_to_latest`` step flips the head pointer to (DAT-413). This is the
# producing-stage axis ``MetadataSnapshotHead.stage`` records — every add_source
# stage that writes run_id-stamped metadata, plus the terminal ``detect``. With
# exactly one run at a time (Phase 2) nothing reads the head yet, so promoting it
# is byte-identical to today's delete-then-insert; Phase 3 switches the readers.
_PROMOTE_STAGES = (
    "typing",
    "statistics",
    "column_eligibility",
    "statistical_quality",
    "temporal",
    "semantic_per_column",
    "detect",
)

# The begin_session chain phases whose declared detectors the terminal session
# ``detect`` runs: ``semantic_per_table`` declares the relationship detectors
# (join_path_determinism + relationship_entropy, DAT-408); ``enriched_views``
# declares ``dimension_coverage`` (table-grain fact-table enrichment coverage,
# DAT-415); the value layer (DAT-403) declares ``slice_variance`` (slice_analysis),
# ``temporal_drift`` + ``dimensional_entropy`` (temporal_slice_analysis), and
# ``derived_value`` (correlations) — column/table-grain value-readiness signals over
# the slices + enriched views the begin_session spine just built. Distinct from the
# source-scoped ``_DETECTOR_PHASES`` so add_source never runs these and begin_session
# never runs the column-profiling ones. A declared detector whose inputs are absent
# (no slice profiles / drift / derived columns) simply produces no objects — the
# value detectors no-op cleanly on a relationship-only run. Public (imported by
# ``activities.py`` + tests).
SESSION_DETECTOR_PHASES = (
    "relationships",
    "semantic_per_table",
    "enriched_views",
    "slice_analysis",
    "temporal_slice_analysis",
    "correlations",
)


@dataclass
class PhaseRun:
    """Temporal-agnostic outcome of one phase body (no detector side-effects)."""

    status: str
    summary: str = ""
    error: str | None = None


def run_phase(
    manager: ConnectionManager,
    phase_name: str,
    identity: SourceIdentity,
    table_ids: list[str],
) -> PhaseRun:
    """Run one pipeline phase scoped to ``table_ids``, leasing connections from ``manager``.

    Args:
        manager: the worker's single workspace-level ConnectionManager
            (Postgres + workspace DuckDB already open via ``open_lake``).
        phase_name: a key in pipeline.yaml / the phase registry (e.g. "import").
        identity: the source-identity header carried by the workflow.
        table_ids: the phase's table scope. ``[]`` = source-wide (import,
            semantic_per_column); a single typed id for the analytics phases.
    """
    # Anti-footgun for the deferred multi-workspace isolation (DAT-364): the
    # worker is bound to exactly one workspace, so a payload addressed to a
    # different one must never run — it would silently write into this worker's
    # lake + ws_<id> schema. Fail loud before touching any connection (FAILED →
    # non-retryable PhaseFailed in the activity wrapper). Today workspace_id is
    # decorative; this becomes the routing key when isolation lands.
    active_workspace_id = get_active_workspace_id()
    if identity.workspace_id != active_workspace_id:
        return PhaseRun(
            status=PhaseStatus.FAILED.value,
            error=(
                f"Workspace mismatch: payload targets '{identity.workspace_id}' "
                f"but this worker is bound to '{active_workspace_id}'. Refusing "
                "to run to avoid a cross-workspace miswrite (DAT-364)."
            ),
        )

    phase_cls = get_phase_class(phase_name)
    if phase_cls is None:
        return PhaseRun(
            status=PhaseStatus.FAILED.value,
            error=f"Unknown phase '{phase_name}' — not in the phase registry.",
        )
    phase = phase_cls()

    # Lease a scoped session + DuckDB cursor for the phase body. session_scope
    # commits on clean exit (so the phase's writes are visible to later
    # activities); duckdb_cursor closes the derived cursor on exit.
    with manager.session_scope() as session, manager.duckdb_cursor() as cursor:
        # Only ``import`` ingests from a ``Source`` (it reads connection_config +
        # backend to load files); past it the run is source-free (DAT-422), so a
        # ``None`` source_id means "no Source to resolve" — the config is built from
        # the phase's static config + the run's vertical. A source_id that IS set
        # but doesn't resolve is still a fail-loud caller bug (the import caller
        # must write the Source row before the phase runs).
        source = None
        if identity.source_id is not None:
            source = session.get(Source, identity.source_id)
            if source is None:
                return PhaseRun(
                    status=PhaseStatus.FAILED.value,
                    error=(
                        f"Source '{identity.source_id}' not found in workspace "
                        f"'{identity.workspace_id}'. The workflow caller must write the "
                        "Source row before the phase runs."
                    ),
                )
        config = _build_phase_config(source, phase_name, identity)
        ctx = PhaseContext(
            session=session,
            duckdb_conn=cursor,
            source_id=identity.source_id,
            table_ids=list(table_ids),
            config=config,
            session_factory=manager.session_scope,
            manager=manager,
            session_id=identity.session_id,
            run_id=identity.run_id,
        )

        skip_reason = phase.should_skip(ctx)
        if skip_reason:
            logger.info("activity.phase_skipped", phase=phase_name, reason=skip_reason)
            return PhaseRun(status=PhaseStatus.SKIPPED.value, summary=skip_reason)

        result = phase.run(ctx)

    logger.info(
        "activity.phase_done",
        phase=phase_name,
        status=result.status.value,
        duration=result.duration_seconds,
    )
    return PhaseRun(
        status=result.status.value,
        summary=result.summary,
        error=result.error,
    )


def raw_table_ids(manager: ConnectionManager, source_id: str) -> list[str]:
    """The source's raw table ids — the fan-out source the parent needs.

    Read after the ``import`` activity (run or skipped), so the parent always
    has the authoritative set regardless of whether import did fresh work.
    """
    with manager.session_scope() as session:
        rows = session.execute(
            select(Table.table_id).where(Table.source_id == source_id, Table.layer == "raw")
        )
        return [row[0] for row in rows]


def typed_table_id_for_raw(
    manager: ConnectionManager,
    raw_table_id: str,
) -> str | None:
    """Resolve the typed table id ``typing`` produced for one raw table.

    Typing creates the typed table under the same ``(source_id, table_name)`` as
    its raw input, so the mapping is by name within the raw table's OWN source —
    read off the raw row, not threaded in (DAT-422). That keeps the call site
    source-agnostic: a run spanning per-object sources resolves each raw's typed
    counterpart without ambiguity (a same-named table in another source can't
    shadow it). Returns the persisted id whether typing just minted it or it
    already existed (skip path) — ``None`` if neither the raw row nor its typed
    table is present.
    """
    with manager.session_scope() as session:
        raw = session.get(Table, raw_table_id)
        if raw is None:
            return None
        return session.execute(
            select(Table.table_id).where(
                Table.source_id == raw.source_id,
                Table.table_name == raw.table_name,
                Table.layer == "typed",
            )
        ).scalar_one_or_none()


def declared_detector_ids(phase_names: Iterable[str]) -> list[str]:
    """The de-duplicated detectors the given phases declare in pipeline.yaml."""
    declarations = load_phase_declarations()
    detector_ids: list[str] = []
    for phase_name in phase_names:
        decl = declarations.get(phase_name)
        if not decl:
            continue
        for detector_id in decl.detectors:
            if detector_id not in detector_ids:
                detector_ids.append(detector_id)
    return detector_ids


def check_run_column_limit(
    manager: ConnectionManager,
    identity: SourceIdentity,
    table_ids: list[str],
) -> PhaseRun:
    """Gate the RUN's total raw column count before the per-table fan-out (DAT-430).

    ``limits.max_columns`` bounds a run's pipeline/LLM cost, so it must judge the
    run's whole object set: a per-source check stopped meaning anything once a
    run became a SET of per-file content sources (DAT-422 — 100 sources at 499
    columns each would each pass a per-source cap). The parent workflow calls
    this once after the import loop with the union of the run's raw table ids,
    so the gate also fires when every import skipped (already-imported sources
    recomposed into a bigger run). A breach is a deterministic FAILED →
    non-retryable ``PhaseFailed`` in the activity wrapper.
    """
    active_workspace_id = get_active_workspace_id()
    if identity.workspace_id != active_workspace_id:
        return PhaseRun(
            status=PhaseStatus.FAILED.value,
            error=(
                f"Workspace mismatch: payload targets '{identity.workspace_id}' "
                f"but this worker is bound to '{active_workspace_id}'. Refusing "
                "to run to avoid a cross-workspace miswrite (DAT-364)."
            ),
        )

    max_columns = load_pipeline_config().get("limits", {}).get("max_columns", 500)
    with manager.session_scope() as session:
        count = session.execute(
            select(func.count(Column.column_id)).where(Column.table_id.in_(table_ids))
        ).scalar_one()

    if count > max_columns:
        return PhaseRun(
            status=PhaseStatus.FAILED.value,
            error=(
                f"Column limit exceeded for this run: {count} columns across "
                f"{len(table_ids)} raw table(s) > max_columns={max_columns}. "
                "Import fewer/narrower objects in one run, or raise "
                "limits.max_columns in pipeline.yaml."
            ),
        )
    return PhaseRun(
        status=PhaseStatus.COMPLETED.value,
        summary=(
            f"{count} columns across {len(table_ids)} raw table(s) within max_columns={max_columns}"
        ),
    )


def begin_session_select(
    manager: ConnectionManager,
    identity: SessionIdentity,
    table_ids: list[str],
) -> PhaseRun:
    """Pre-flight the selected tables + link them to the session (DAT-401).

    The first step of ``beginSessionWorkflow``: validate every id is a known
    *typed* table (reject unknown — a deterministic FAILED → non-retryable
    ``PhaseFailed`` in the wrapper) and write the ``session_tables`` links via
    the same idempotent merge ``typing`` uses for add_source. The session row
    itself is seeded by the caller (cockpit in 2.0c; the test driver now),
    mirroring add_source — so its absence is a fail-loud caller error, not a
    create-on-demand path.
    """
    active_workspace_id = get_active_workspace_id()
    if identity.workspace_id != active_workspace_id:
        return PhaseRun(
            status=PhaseStatus.FAILED.value,
            error=(
                f"Workspace mismatch: payload targets '{identity.workspace_id}' "
                f"but this worker is bound to '{active_workspace_id}'. Refusing "
                "to run to avoid a cross-workspace miswrite (DAT-364)."
            ),
        )
    if not table_ids:
        return PhaseRun(
            status=PhaseStatus.FAILED.value,
            error="begin_session requires at least one table id.",
        )

    with manager.session_scope() as session:
        if session.get(InvestigationSession, identity.session_id) is None:
            return PhaseRun(
                status=PhaseStatus.FAILED.value,
                error=(
                    f"InvestigationSession '{identity.session_id}' not found — the "
                    "begin_session caller must seed the session row before the "
                    "workflow runs (mirrors add_source's cockpit seed)."
                ),
            )
        found = set(
            session.execute(
                select(Table.table_id).where(Table.table_id.in_(table_ids), Table.layer == "typed")
            ).scalars()
        )
        unknown = [tid for tid in table_ids if tid not in found]
        if unknown:
            return PhaseRun(
                status=PhaseStatus.FAILED.value,
                error=f"Unknown or non-typed table ids in selection: {unknown}",
            )
        link_session_tables(session, identity.session_id, table_ids)

    logger.info(
        "activity.begin_session_select",
        session_id=identity.session_id,
        table_count=len(table_ids),
    )
    return PhaseRun(
        status=PhaseStatus.COMPLETED.value,
        summary=f"linked {len(table_ids)} table(s) to session {identity.session_id}",
    )


def run_session_phase(
    manager: ConnectionManager,
    phase_name: str,
    identity: SessionIdentity,
    table_ids: list[str],
    extra_config: dict[str, Any] | None = None,
) -> PhaseRun:
    """Run one begin_session phase over ``table_ids`` — source-free (DAT-401).

    The session-scoped sibling of :func:`run_phase`. Past the add_source
    boundary a source is meaningless (feedback-source-dies-at-addsource): the
    phase scopes purely by the typed ``table_ids`` (the session's selection,
    threaded from the workflow input) and reads the frame ``vertical`` off the
    ``InvestigationSession`` row — never a ``Source``. The ``PhaseContext`` is
    built with ``source_id=None`` so the phase body cannot silently fall back to
    source scoping.

    ``extra_config`` merges runtime keys over the static phase config — e.g.
    the operating_model activities thread the resolved ``base_runs`` pin
    (ADR-0008) into ``ctx.config`` through it.
    """
    active_workspace_id = get_active_workspace_id()
    if identity.workspace_id != active_workspace_id:
        return PhaseRun(
            status=PhaseStatus.FAILED.value,
            error=(
                f"Workspace mismatch: payload targets '{identity.workspace_id}' "
                f"but this worker is bound to '{active_workspace_id}'. Refusing "
                "to run to avoid a cross-workspace miswrite (DAT-364)."
            ),
        )

    phase_cls = get_phase_class(phase_name)
    if phase_cls is None:
        return PhaseRun(
            status=PhaseStatus.FAILED.value,
            error=f"Unknown phase '{phase_name}' — not in the phase registry.",
        )
    phase = phase_cls()

    with manager.session_scope() as session, manager.duckdb_cursor() as cursor:
        inv_session = session.get(InvestigationSession, identity.session_id)
        if inv_session is None:
            return PhaseRun(
                status=PhaseStatus.FAILED.value,
                error=(
                    f"InvestigationSession '{identity.session_id}' not found in "
                    f"workspace '{identity.workspace_id}'. The begin_session caller "
                    "must seed the session row before the workflow runs."
                ),
            )
        config = _build_session_phase_config(phase_name, inv_session.vertical)
        if extra_config:
            config.update(extra_config)
        ctx = PhaseContext(
            session=session,
            duckdb_conn=cursor,
            source_id=None,
            table_ids=list(table_ids),
            config=config,
            session_factory=manager.session_scope,
            manager=manager,
            session_id=identity.session_id,
            run_id=identity.run_id,
        )

        skip_reason = phase.should_skip(ctx)
        if skip_reason:
            logger.info("activity.session_phase_skipped", phase=phase_name, reason=skip_reason)
            return PhaseRun(status=PhaseStatus.SKIPPED.value, summary=skip_reason)

        result = phase.run(ctx)

    logger.info(
        "activity.session_phase_done",
        phase=phase_name,
        status=result.status.value,
        duration=result.duration_seconds,
    )
    return PhaseRun(
        status=result.status.value,
        summary=result.summary,
        error=result.error,
    )


def run_detectors(
    manager: ConnectionManager,
    *,
    session_id: str,
    run_id: str | None,
    detector_phases: tuple[str, ...] = _DETECTOR_PHASES,
) -> int:
    """Run every wired detector once over the run-session's tables — the terminal ``detect`` step.

    Source-free (DAT-408): the single stage-level detector pass (DAT-394) runs the
    union of the detectors ``detector_phases`` declare over the tables the run's
    session composes (``session_tables``, DAT-407/410) — for ``add_source`` that is
    exactly the source's freshly-typed tables, so the scope is identical to the prior
    source-wide pass; begin_session passes its own phase set + a multi-source table
    set. It runs once, sequentially, after the per-table fan-out and the reduce — so
    the delete-before-insert is safe (no concurrency) and every detector's inputs are
    present. ``run_id`` stamps the snapshot version axis (DAT-413).

    Base-run pinning (DAT-448): the promoted ``(table:{id}, stage)`` heads for the
    add_source stages session detects read (semantic_per_column, statistics,
    statistical_quality) are resolved ONCE here and threaded down — per-call head
    resolution in the loaders let a concurrent promote tear reads mid-run.
    """
    from dataraum.entropy.detectors.loaders import resolve_base_runs

    detector_ids = declared_detector_ids(detector_phases)
    if not detector_ids:
        return 0

    total = 0
    with manager.session_scope() as session, manager.duckdb_cursor() as cursor:
        table_ids = tables_for_session(session, session_id)
        if not table_ids:
            # add_source links its typed tables in ``typing`` (same transaction as
            # the Table row), so an empty set here means the session has no tables —
            # nothing to detect. Log it: a populated source with no links is a bug.
            logger.warning("detect_no_session_tables", session_id=session_id)
            return 0
        base_runs = resolve_base_runs(session, table_ids)
        for detector_id in detector_ids:
            # Scoped to the session's tables. The single terminal pass runs once,
            # sequentially after the fan-out — no concurrent writers to collide on
            # the per-(detector, table) delete-before-insert.
            total += run_detector_post_step(
                session,
                detector_id,
                cursor,
                session_id=session_id,
                table_ids=table_ids,
                run_id=run_id,
                base_runs=base_runs,
            )
        # Persist readiness from the freshly-written entropy objects, in the same
        # transaction (DAT-394). flush() makes the just-added rows visible to the
        # rollup's repository select before we read them back.
        session.flush()
        readiness_rows = persist_readiness(session, session_id, table_ids, run_id=run_id)
        logger.info(
            "terminal_detect_done",
            session_id=session_id,
            detector_records=total,
            readiness_rows=readiness_rows,
        )
    return total


def materialize_session_overlays(manager: ConnectionManager, identity: SessionIdentity) -> int:
    """Materialize the session's durable relationship overlays into this run (DAT-409).

    Runs between ``semantic_per_table`` and ``session_detect``: writes the user's
    ``add``/``keep`` relationship overlays as run-stamped ``manual``/``keeper``
    ``Relationship`` rows so they re-appear every run (skipping pairs the run already
    produced as ``llm``, and rejected pairs). ``session_detect`` then measures the
    whole defined catalog. Source-free; tables resolve from ``session_tables``.
    """
    from dataraum.analysis.relationships.materialize import materialize_relationship_overlays

    with manager.session_scope() as session:
        table_ids = tables_for_session(session, identity.session_id)
        if not table_ids:
            logger.warning("materialize_no_session_tables", session_id=identity.session_id)
            return 0
        count = materialize_relationship_overlays(
            session, identity.session_id, run_id=identity.run_id, table_ids=table_ids
        )
    logger.info("session_materialize_done", session_id=identity.session_id, count=count)
    return count


def write_session_keepers(manager: ConnectionManager, identity: SessionIdentity) -> int:
    """Lift silently-accepted relationships into keep overlays (DAT-409 C3).

    Pre-promote step: while the per-session head still points at the prior run,
    compare it to this run and write a ``keep`` overlay for each promoted ``llm``
    the current run didn't reproduce and the user didn't reject. They materialize as
    ``keeper`` from the next run onward.
    """
    from dataraum.analysis.relationships.materialize import write_relationship_keepers

    with manager.session_scope() as session:
        count = write_relationship_keepers(
            session, identity.session_id, current_run_id=identity.run_id
        )
    logger.info("session_keepers_done", session_id=identity.session_id, count=count)
    return count


def promote_run(manager: ConnectionManager, identity: SourceIdentity) -> int:
    """Flip the snapshot head to this run for every ``(table_id, stage)`` — terminal step (DAT-413).

    The single terminal ``promote_to_latest`` step: after ``detect``, record this
    run as the *current* (promoted) snapshot for each of the run's tables × each
    add_source stage (``_PROMOTE_STAGES``). It resolves the run's tables via
    ``session_tables`` (the exact set :func:`run_detectors` scopes to) and
    **upserts** :class:`MetadataSnapshotHead`: an existing ``(table_id, stage)``
    row has its ``run_id`` re-pointed + ``version`` bumped; a missing one is
    inserted at ``version=0``.

    Behavior-preserving in Phase 2: with exactly one run at a time nothing reads
    the head yet (every phase still does delete-then-insert), so writing it has no
    effect on downstream output — Phase 3 switches the readers to head-resolution.

    Workspace-mismatch guard mirrors the other run-side helpers (DAT-364).

    Returns:
        Number of head rows promoted (``len(tables) * len(_PROMOTE_STAGES)``).
    """
    active_workspace_id = get_active_workspace_id()
    if identity.workspace_id != active_workspace_id:
        raise RuntimeError(
            f"Workspace mismatch: promote payload targets "
            f"'{identity.workspace_id}' but this worker is bound to "
            f"'{active_workspace_id}'."
        )

    # The head names a run, so promote is meaningless without one. The
    # AddSourceWorkflow always mints + stamps ``run_id`` before any activity, so a
    # missing one here is a caller bug — fail loud rather than write a NULL head.
    run_id = identity.run_id
    if run_id is None:
        raise RuntimeError(
            "promote_run requires a stamped identity.run_id — the workflow mints "
            "it via workflow.uuid4() before the first activity (DAT-413)."
        )

    promoted = 0
    with manager.session_scope() as session:
        table_ids = tables_for_session(session, identity.session_id)
        if not table_ids:
            # Same empty-set signal as run_detectors: a populated source with no
            # session links is a bug; nothing to promote.
            logger.warning("promote_no_session_tables", session_id=identity.session_id)
            return 0
        now = datetime.now(UTC)
        for table_id in table_ids:
            # add_source's per-table stages key the head by the generic
            # ``table:{id}`` target (DAT-408); relationship targets are promoted by
            # begin_session's own promote.
            target = f"table:{table_id}"
            for stage in _PROMOTE_STAGES:
                _upsert_head(session, target, stage, run_id, now)
                promoted += 1

    logger.info(
        "promote_to_latest_done",
        session_id=identity.session_id,
        run_id=run_id,
        heads_promoted=promoted,
    )
    return promoted


def _upsert_head(session: Session, target: str, stage: str, run_id: str, now: datetime) -> None:
    """Point the ``(target, stage)`` snapshot head at ``run_id`` (insert or re-point)."""
    head = session.execute(
        select(MetadataSnapshotHead).where(
            MetadataSnapshotHead.target == target,
            MetadataSnapshotHead.stage == stage,
        )
    ).scalar_one_or_none()
    if head is None:
        session.add(
            MetadataSnapshotHead(
                target=target, stage=stage, run_id=run_id, promoted_at=now, version=0
            )
        )
    else:
        head.run_id = run_id
        head.promoted_at = now
        head.version = head.version + 1


def promote_session_run(manager: ConnectionManager, identity: SessionIdentity) -> int:
    """Seal this begin_session run as the session's current run (DAT-408).

    begin_session's terminal promote: after ``session_detect`` writes this run's
    relationship catalog + readiness (all stamped ``run_id``), point the single
    per-session head ``(session:{id}, "detect")`` at this run. Readers
    (``load_relationship_readiness``, the cockpit) resolve the session's current run
    through it. Per-session (not per-target) because the run is atomic — every run
    measures the whole catalog. Workspace guard mirrors the other session helpers.
    """
    active_workspace_id = get_active_workspace_id()
    if identity.workspace_id != active_workspace_id:
        raise RuntimeError(
            f"Workspace mismatch: session promote targets '{identity.workspace_id}' "
            f"but this worker is bound to '{active_workspace_id}'."
        )
    run_id = identity.run_id
    if run_id is None:
        raise RuntimeError(
            "promote_session_run requires a stamped identity.run_id — "
            "BeginSessionWorkflow mints it before the first activity (DAT-408)."
        )

    with manager.session_scope() as session:
        _upsert_head(
            session, session_head_target(identity.session_id), "detect", run_id, datetime.now(UTC)
        )

    logger.info("session_promote_done", session_id=identity.session_id, run_id=run_id)
    return 1


def resolve_operating_model_scope(
    manager: ConnectionManager, identity: SessionIdentity
) -> OperatingModelScope:
    """Pre-flight for ``operatingModelWorkflow`` — table set + pinned base runs (DAT-438).

    The session anchors its table set (``session_tables``, persisted by
    ``begin_session_select``); operating_model re-reads it instead of trusting
    a re-passed copy. The base-run map (ADR-0008 in-run mode) is resolved HERE,
    once per run, and travels with the workflow's contracts — no per-phase head
    resolution downstream.

    Raises:
        RuntimeError: workspace mismatch, unknown session, or a session with
            no linked tables (begin_session must have run) — fail loud, the
            workflow has nothing to operate on.
    """
    from dataraum.lifecycle import resolve_operating_model_base_runs

    active_workspace_id = get_active_workspace_id()
    if identity.workspace_id != active_workspace_id:
        raise RuntimeError(
            f"Workspace mismatch: operating_model resolve targets '{identity.workspace_id}' "
            f"but this worker is bound to '{active_workspace_id}'."
        )

    with manager.session_scope() as session:
        inv_session = session.get(InvestigationSession, identity.session_id)
        if inv_session is None:
            raise RuntimeError(
                f"InvestigationSession '{identity.session_id}' not found — "
                "operating_model runs over an existing journey session."
            )
        # Born-loud on a typo'd / never-framed vertical (DAT-480): an unknown
        # name would silently resolve to no declared validations/cycles/metrics
        # and every phase would emit a benign no_declared_*. A placeholder
        # (_adhoc), shipped, or framed name passes through unchanged.
        require_known_vertical(inv_session.vertical)
        table_ids = tables_for_session(session, identity.session_id)
        if not table_ids:
            raise RuntimeError(
                f"Session '{identity.session_id}' has no linked tables — "
                "begin_session must compose the workspace before operating_model runs."
            )
        base_runs = resolve_operating_model_base_runs(session, identity.session_id, table_ids)

    logger.info(
        "operating_model_scope_resolved",
        session_id=identity.session_id,
        tables=len(table_ids),
        relationship_run=base_runs.relationship_run_id,
        semantic_runs=len(base_runs.semantic_runs),
    )
    return OperatingModelScope(
        table_ids=table_ids,
        relationship_run_id=base_runs.relationship_run_id,
        semantic_runs=base_runs.semantic_runs,
    )


def promote_operating_model_run(manager: ConnectionManager, identity: SessionIdentity) -> int:
    """Seal this operating_model run as the session's current run (DAT-438).

    Terminal promote: point the per-session head ``(session:{id},
    "operating_model")`` at this run. Readers (the cockpit's validation
    surfaces, cross_table_consistency's query tier) resolve the current
    lifecycle artifacts + validation results through it. Distinct stage from
    begin_session's ``"detect"`` head — the two stages' runs coexist on the
    same session target.
    """
    active_workspace_id = get_active_workspace_id()
    if identity.workspace_id != active_workspace_id:
        raise RuntimeError(
            f"Workspace mismatch: operating_model promote targets '{identity.workspace_id}' "
            f"but this worker is bound to '{active_workspace_id}'."
        )
    run_id = identity.run_id
    if run_id is None:
        raise RuntimeError(
            "promote_operating_model_run requires a stamped identity.run_id — "
            "OperatingModelWorkflow mints it before the first activity (DAT-408)."
        )

    with manager.session_scope() as session:
        _upsert_head(
            session,
            session_head_target(identity.session_id),
            "operating_model",
            run_id,
            datetime.now(UTC),
        )

    logger.info("operating_model_promote_done", session_id=identity.session_id, run_id=run_id)
    return 1


def _build_phase_config(
    source: Source | None,
    phase_name: str,
    identity: SourceIdentity,
) -> dict[str, Any]:
    """Reconstruct ``ctx.config`` = phase static config + run-identity runtime config.

    Mirrors the ``phase_config | runtime_config`` merge ``setup_pipeline`` did,
    minus the PipelineRun-only fields (fingerprint, source_path) the worker path
    doesn't carry. ``source`` is set only for the per-source ``import`` (DAT-422):
    every downstream phase is source-free, so the source-identity fields are
    omitted and only the static config + the run's ``vertical`` remain — which is
    all a reduce like ``semantic_per_column`` reads off the config.
    """
    runtime_config: dict[str, Any] = {"vertical": identity.vertical or "_adhoc"}
    if source is not None:
        runtime_config.update(
            {
                "source_id": source.source_id,
                "source_name": source.name,
                "source_type": source.source_type,
                "source_connection_config": source.connection_config or {},
                "source_backend": source.backend,
            }
        )

    config: dict[str, Any] = {}
    config.update(load_phase_config(phase_name))
    config.update(runtime_config)
    return config


def _build_session_phase_config(phase_name: str, vertical: str | None) -> dict[str, Any]:
    """Phase static config + the session's frame ``vertical`` (DAT-401).

    Source-free analogue of :func:`_build_phase_config`: a begin_session phase
    needs its pipeline.yaml static config (e.g. relationships' ``min_confidence``
    / ``sample_percent``) plus the ``vertical`` the LLM table-synthesis reads —
    sourced from the session's frame, defaulting to ``"_adhoc"`` on a cold-start
    session (mirrors add_source's ``identity.vertical or "_adhoc"``).
    """
    config: dict[str, Any] = {}
    config.update(load_phase_config(phase_name))
    config["vertical"] = vertical or "_adhoc"
    return config
