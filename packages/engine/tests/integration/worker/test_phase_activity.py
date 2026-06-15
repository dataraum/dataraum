"""Integration test for the Temporal activity-worker substrate (DAT-344; DAT-370).

The de-risk slice's core assertion: a single workspace-level ``ConnectionManager``
(the one a worker holds for its whole life) runs the real phases as separate
``run_phase`` calls, with the DuckLake ``:memory:`` anchor + the manager's DuckDB
connection surviving across them and rows landing in the lake. It runs against
the testcontainer Postgres + a real DuckLake (``lake_anchor``), not SQLite or
mocks — the substrate reconstitution is exactly what's under test.

DAT-370 makes the table the unit of work: ``import`` enumerates raw tables,
``typing`` mints one typed id per raw table, and the analytics phases run scoped
to a single typed table. Detectors run once, source-wide, in the terminal
``detect`` step after the fan-out (DAT-394 — ``run_detectors``). These tests
drive that path directly (the production workflows fan the chains out across
child workflows, then run one terminal detect in the parent).
"""

from __future__ import annotations

import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import select, text

from dataraum.core.connections import ConnectionConfig, ConnectionManager
from dataraum.entropy.db_models import EntropyObjectRecord, EntropyReadinessRecord
from dataraum.storage import Source, Table
from dataraum.worker import (
    SourceIdentity,
    promote_run,
    raw_table_ids,
    run_detectors,
    run_phase,
    typed_table_id_for_raw,
)

# The table-local analytics phases wrapped as activities, in dependency order.
# ``typing`` precedes them (it mints the typed id). Detectors no longer run per
# table — the single terminal ``detect`` step (``run_detectors``) runs them once,
# source-wide, after the fan-out + reduce (DAT-394). ``semantic_per_column`` (the
# source-level LLM reduce) is exercised separately, gated behind a real key.
_ANALYTICS_PHASES = (
    "statistics",
    "column_eligibility",
    "statistical_quality",
    "temporal",
)

# The table-local detectors (pipeline.yaml: type_fidelity from typing, null_ratio
# from statistics) — they end up attached per typed table after the terminal
# source-wide detect step.
_TABLE_LOCAL_DETECTORS = {"type_fidelity", "null_ratio"}

_LIVE_LLM_ENV = "DATARAUM_LIVE_LLM_TEST"


@pytest.fixture(autouse=True)
def _allow_local_fixture_uris(monkeypatch: pytest.MonkeyPatch) -> None:
    """Let the import ingress accept the local fixture paths these tests use.

    The subject here is the worker substrate (one ConnectionManager + the
    DuckLake anchor surviving across phases, per-table concurrency), not the
    DAT-389 source-URI gate — and there is no object store in the test process,
    so the fixtures are local CSVs DuckDB reads directly. The gate's correctness
    (rejecting non-``s3://<lake-bucket>`` URIs) is proven by the dedicated unit
    tests; here we pass it through so the real ``run_phase`` worker path can load
    the local fixtures.
    """
    monkeypatch.setattr(
        "dataraum.pipeline.phases.import_phase.validate_source_uri",
        lambda uri: uri,
    )


@pytest.fixture
def worker_manager(pg_url_clean: str, lake_anchor, lake_clean):  # noqa: ANN001
    """A workspace-level ConnectionManager built exactly as the worker bootstraps it.

    Mirrors ``bootstrap_worker_substrate`` (workspace config + ``open_lake``)
    minus the global ``bootstrap_lake``/``bootstrap_workspace`` calls, which the
    ``lake_anchor`` fixture + the conftest workspace pointer already provide.
    """
    manager = ConnectionManager(ConnectionConfig(database_url=pg_url_clean))
    manager.initialize()
    manager.open_lake()
    yield manager
    manager.close()


def _identity(
    session_id: str, *, source_id: str | None = None, run_id: str | None = None
) -> SourceIdentity:
    """Source-FREE by default — the production shape past ``import`` (DAT-422/426).

    ``AddSourceWorkflow`` drops ``source_id`` after the import loop, so every
    post-import phase runs source-free. Pass ``source_id`` ONLY for the
    per-source ``import`` activity; feeding it anywhere else would mask a
    regression that reintroduces a ``ctx.source_id`` read.
    """
    return SourceIdentity(
        workspace_id="test", source_id=source_id, session_id=session_id, run_id=run_id
    )


def _seed_source(
    manager: ConnectionManager,
    source_id: str,
    name: str,
    paths: Path | list[Path],
) -> None:
    """Seed the Source row the workflow would create before ``import`` runs.

    ``addSourceWorkflow`` writes this in production; the activity reads the Source
    for its config. Sessions live in cockpit_db now (DAT-506) — the engine no
    longer seeds an InvestigationSession; a run links its tables via
    ``run_tables`` (written inside the typing phase) instead.

    Post-DAT-378 a file source carries its objects as an explicit ``file_uris``
    list under ``connection_config`` (the cockpit ``select`` stage enumerated the
    prefix into it). ``paths`` accepts a single file or a list; both seed the
    same ``file_uris`` shape, so the multi-file tests seed >1 URI and the
    single-file tests seed exactly one. The autouse ``_allow_local_fixture_uris``
    fixture lets these local paths through the production ``validate_source_uri``
    gate so ``ImportPhase._run`` loads them directly.
    """
    file_uris = [str(p) for p in (paths if isinstance(paths, list) else [paths])]
    with manager.session_scope() as session:
        session.add(
            Source(
                source_id=source_id,
                name=name,
                source_type="csv",
                connection_config={"file_uris": file_uris},
                status="configured",
            )
        )


# Data-file extensions the multi-file seed enumerates out of the small_finance
# directory fixture — mirrors the cockpit ``select`` enumeration that lists an
# s3:// prefix into an explicit URI list (DAT-378). The directory of distinct
# CSVs stands in for that enumerated list in the test process (no object store).
_FIXTURE_DATA_SUFFIXES = (".csv", ".tsv", ".parquet", ".pq", ".json", ".jsonl")


def _enumerate_fixture_files(directory: Path) -> list[Path]:
    """Sorted list of loadable data files in a directory fixture (>1 → multi-URI)."""
    files = sorted(
        p for p in directory.iterdir() if p.is_file() and p.suffix.lower() in _FIXTURE_DATA_SUFFIXES
    )
    assert len(files) > 1, f"expected a multi-file fixture in {directory}"
    return files


def _lake_tables(manager: ConnectionManager, schema: str) -> list[str]:
    with manager.duckdb_cursor() as cursor:
        rows = cursor.execute(
            "SELECT table_name FROM duckdb_tables() "
            f"WHERE database_name = 'lake' AND schema_name = '{schema}'"
        ).fetchall()
    return [r[0] for r in rows]


def _process_one_table(
    manager: ConnectionManager,
    identity: SourceIdentity,
    raw_table_id: str,
) -> str:
    """Run the table-local chain for one raw table — the ProcessTableWorkflow body.

    typing(raw) -> typed_id -> analytics phases (typed_id). Detectors do NOT run
    here (DAT-394) — they run once, source-wide, in the terminal ``detect`` step
    after the fan-out. Returns the typed table id. Asserts each step completed.
    """
    typing = run_phase(manager, "typing", identity, [raw_table_id], "finance")
    assert typing.status == "completed", f"typing failed: {typing.error}"

    typed_id = typed_table_id_for_raw(manager, raw_table_id)
    assert typed_id is not None, f"no typed table for raw {raw_table_id}"
    assert typed_id != raw_table_id, "typed id must differ from the raw id"

    for phase in _ANALYTICS_PHASES:
        result = run_phase(manager, phase, identity, [typed_id], "finance")
        # SKIPPED is a valid per-table outcome (e.g. temporal on a table with no
        # date columns); only FAILED is an error. The activity wrapper likewise
        # only raises on FAILED.
        assert result.status in ("completed", "skipped"), f"{phase} failed: {result.error}"

    return typed_id


def test_import_then_typing_share_one_manager(
    worker_manager: ConnectionManager, tmp_path: Path
) -> None:
    """import + typing run as two phases on one manager; the anchor survives both."""
    csv = tmp_path / "orders.csv"
    csv.write_text(
        "id,amount,booked_on\n1,10.50,2024-01-01\n2,20.00,2024-01-02\n3,30.25,2024-01-03\n"
    )
    source_id = str(uuid4())
    session_id = str(uuid4())
    run_id = str(uuid4())
    _seed_source(worker_manager, source_id, "orders", csv)

    # import (no LLM, no prereq) — the one source-bearing activity; raw tables
    # land and the source's raw ids are the fan-out source the parent reads.
    import_result = run_phase(
        worker_manager,
        "import",
        _identity(session_id, source_id=source_id, run_id=run_id),
        [],
        "finance",
    )
    assert import_result.status == "completed", import_result.error
    raw_ids = raw_table_ids(worker_manager, source_id)
    assert raw_ids, "import produced no raw tables"
    assert _lake_tables(worker_manager, "raw"), "no tables in lake.raw after import"

    # The DuckLake connection the worker holds is reused — not reopened — for the
    # next phase. Capture identity to prove the anchor survived the boundary.
    duckdb_conn_after_import = worker_manager._duckdb_conn  # noqa: SLF001

    # typing scoped to the single raw table (DuckDB-heavy; runs on the SAME
    # long-lived manager), source-free as the workflow threads it (DAT-422). typing
    # links the typed table to the run via ``run_tables`` (DAT-506), so it needs a
    # stamped run_id — minted up front and threaded in.
    typing_result = run_phase(
        worker_manager, "typing", _identity(session_id, run_id=run_id), [raw_ids[0]], "finance"
    )
    assert typing_result.status == "completed", typing_result.error
    assert _lake_tables(worker_manager, "typed"), "no tables in lake.typed after typing"
    assert typed_table_id_for_raw(worker_manager, raw_ids[0]) is not None

    assert worker_manager._duckdb_conn is duckdb_conn_after_import, (  # noqa: SLF001
        "worker DuckDB connection was reopened between phases — the anchor "
        "lifecycle did not survive the phase boundary"
    )


def test_unknown_phase_returns_failed(worker_manager: ConnectionManager) -> None:
    """A phase name not in the registry fails cleanly rather than raising."""
    result = run_phase(worker_manager, "does_not_exist", _identity(str(uuid4())), [], "finance")
    assert result.status == "failed"
    assert "does_not_exist" in (result.error or "")


# DAT-505: the per-activity workspace-mismatch guard was removed. Workspace
# isolation is now the per-workspace task queue (engine-<workspace_id>) plus the
# single boot-time assertion in bootstrap_workspace — a payload for another
# workspace never reaches this worker's queue, so there is no activity-level
# guard to exercise here. (test_addsource_runs_under_nondefault_workspace below
# still proves the data-side path is workspace-id-clean.)


def test_failed_phase_rolls_back_partial_writes(
    worker_manager: ConnectionManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A FAILED phase commits nothing — its partial writes are rolled back.

    This is what makes a transient-failure activity retry safe: the retry re-runs
    under the SAME run_id, so attempt 1 must not leave committed rows for it to
    clash with (e.g. a within-run UNIQUE on a non-idempotent writer). A stub phase
    writes a Source then returns FAILED; the row must not survive.
    """
    import dataraum.worker.activity as activity_mod
    from dataraum.pipeline.base import PhaseResult

    ghost_id = str(uuid4())

    class WriteThenFailPhase:
        def should_skip(self, ctx: object) -> None:
            return None

        def run(self, ctx: object) -> PhaseResult:
            ctx.session.add(Source(source_id=ghost_id, name="ghost", source_type="csv"))  # type: ignore[attr-defined]
            return PhaseResult.failed("simulated transient failure")

    monkeypatch.setattr(activity_mod, "get_phase_class", lambda _name: WriteThenFailPhase)

    run = run_phase(worker_manager, "typing", _identity(str(uuid4())), [], "finance")
    assert run.status == "failed"

    with worker_manager.session_scope() as session:
        assert session.get(Source, ghost_id) is None


def test_addsource_runs_under_nondefault_workspace(
    pg_url_clean: str,
    lake_anchor,  # noqa: ANN001
    lake_clean,  # noqa: ANN001
    small_finance_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The full per-table chain runs green under a non-default workspace_id.

    The DAT-364 anti-footgun on the *data* side (the workflow-ID side is covered
    by ``tests/unit/worker/test_workflow_ids.py``): nothing may hardcode the
    default workspace pointer or a default UUID, and ``schema_name_for`` must
    produce a valid ``ws_<id>`` schema for a real UUID — not just the ``"test"``
    sentinel the rest of the suite runs under. We repoint the worker's active
    workspace to a non-default UUID, bootstrap a manager exactly as the worker
    would (which creates ``ws_<uuid>`` + its tables), and run import → the
    per-table chain over the same multi-file fixture the default-workspace chain
    test uses. A stray ``"test"`` left in the path (a hardcoded schema/pointer)
    would write into the wrong ws_<id> schema and this chain would fail loudly.
    """
    import importlib

    nondefault_workspace = "abcdef12-3456-7890-abcd-ef1234567890"
    ws_mod = importlib.import_module("dataraum.server.workspace")
    # Repoint BEFORE initialize(): the manager's search_path listener + CREATE
    # SCHEMA both read the pointer at initialize() time. monkeypatch restores it
    # so the suite's other tests keep running under "test".
    monkeypatch.setattr(ws_mod, "_active_workspace_id", nondefault_workspace)

    manager = ConnectionManager(ConnectionConfig(database_url=pg_url_clean))
    manager.initialize()  # creates ws_abcdef12_... + Base tables under it
    manager.open_lake()
    try:
        source_id = str(uuid4())
        session_id = str(uuid4())
        run_id = str(uuid4())
        files = _enumerate_fixture_files(small_finance_path)
        _seed_source(manager, source_id, "small_finance", files)
        import_identity = SourceIdentity(
            workspace_id=nondefault_workspace,
            source_id=source_id,
            session_id=session_id,
            run_id=run_id,
        )

        assert run_phase(manager, "import", import_identity, [], "finance").status == "completed"
        raw_ids = raw_table_ids(manager, source_id)
        assert raw_ids, "import produced no raw tables under the non-default workspace"

        # Past import the chain runs source-free (DAT-422), as the workflow threads it.
        # typing links the typed tables to the run (DAT-506), so the child identity
        # carries the same run_id minted up front.
        child_identity = SourceIdentity(
            workspace_id=nondefault_workspace, session_id=session_id, run_id=run_id
        )
        typed_ids = [_process_one_table(manager, child_identity, raw_id) for raw_id in raw_ids]
        assert len(typed_ids) == len(raw_ids)
        assert _lake_tables(manager, "typed"), "no typed tables after the chain"
    finally:
        # pg_url_clean only truncates the ``ws_test`` schema, so drop this test's
        # one-off ``ws_<uuid>`` schema explicitly — otherwise it lingers in the
        # session-scoped testcontainer as residue.
        try:
            from dataraum.server.workspace import schema_name_for

            schema = schema_name_for(nondefault_workspace)
            with manager.session_scope() as session:
                session.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        finally:
            manager.close()


def test_per_table_chain_runs(worker_manager: ConnectionManager, small_finance_path: Path) -> None:
    """The full table-local chain runs green per table on a multi-FILE source.

    Drives the production worker path (``run_phase`` + the terminal
    ``run_detectors``, not the retired ``PipelineTestHarness``) on the one
    long-lived manager. The source is seeded with a multi-URI
    ``connection_config['file_uris']`` list (the DAT-378 shape the cockpit
    ``select`` stage writes): ONE ``import`` activity loops the per-URI loader and
    yields >1 raw tables. Then the typing→analytics chain runs scoped per raw
    table — the sequential shape of the ProcessTableWorkflow fan-out — and a
    single terminal ``detect`` runs the detectors once, source-wide (DAT-394).
    """
    source_id = str(uuid4())
    session_id = str(uuid4())
    run_id = str(uuid4())
    files = _enumerate_fixture_files(small_finance_path)
    _seed_source(worker_manager, source_id, "small_finance", files)

    # ONE import activity (source-bearing) over the multi-URI list yields N raw tables.
    import_identity = _identity(session_id, source_id=source_id, run_id=run_id)
    assert run_phase(worker_manager, "import", import_identity, [], "finance").status == "completed"
    raw_ids = raw_table_ids(worker_manager, source_id)
    assert len(raw_ids) > 1, "one import over a multi-URI source must yield >1 raw tables"
    assert len(raw_ids) == len(files), "each enumerated URI maps to one raw table"

    # CALIBRATION GUARD (DAT-378): the per-URI loop must reproduce the EXACT
    # ``small_finance__<file_stem>`` raw-table set the pre-DAT-389 directory
    # branch produced, so dataraum-eval recall baselines do not move. Assert the
    # raw-table NAME set is byte-identical to ``small_finance__<stem>`` for every
    # enumerated fixture file.
    with worker_manager.session_scope() as session:
        raw_names = {
            row[0]
            for row in session.execute(
                select(Table.table_name).where(Table.source_id == source_id, Table.layer == "raw")
            )
        }
    expected_names = {f"small_finance__{p.stem}" for p in files}
    assert raw_names == expected_names, (
        "per-URI loop changed the raw-table set — dataraum-eval baselines would move"
    )

    # Past import the chain + terminal detect run source-free (DAT-422), under the
    # same run_id the typing phase links the typed tables to (DAT-506).
    identity = _identity(session_id, run_id=run_id)
    typed_ids = [_process_one_table(worker_manager, identity, raw_id) for raw_id in raw_ids]
    assert len(typed_ids) == len(raw_ids)
    assert _lake_tables(worker_manager, "typed"), "no typed tables after the chain"

    # Single terminal detect step (DAT-394): one source-wide pass after the
    # fan-out. Offline (no LLM), so only the structural detectors persist rows;
    # that is enough to prove the terminal step runs and writes per-table records.
    assert run_detectors(worker_manager, run_id=run_id) > 0, "terminal detect produced no records"


def test_terminal_detect_persists_per_column_readiness_and_replay_overwrites(
    worker_manager: ConnectionManager, small_finance_path: Path
) -> None:
    """The terminal detect step writes entropy_readiness per column; re-run overwrites (DAT-394).

    Drives the production path (per-table chains → terminal ``run_detectors``) on a
    multi-file source, then asserts the readiness snapshot: one row per analyzed
    column, FK-resolved, valid band, per-intent JSONB payload shape. Re-running the
    terminal step (what every replay does) must delete-before-insert — overwrite,
    not accumulate stale duplicates.
    """
    source_id = str(uuid4())
    session_id = str(uuid4())
    run_id = str(uuid4())
    files = _enumerate_fixture_files(small_finance_path)
    _seed_source(worker_manager, source_id, "small_finance", files)

    import_identity = _identity(session_id, source_id=source_id, run_id=run_id)
    assert run_phase(worker_manager, "import", import_identity, [], "finance").status == "completed"
    raw_ids = raw_table_ids(worker_manager, source_id)
    # Past import: source-free, as the workflow threads it (DAT-422); the run_id
    # threads through typing (which links run_tables) + detect (DAT-506).
    identity = _identity(session_id, run_id=run_id)
    typed_ids = {_process_one_table(worker_manager, identity, raw_id) for raw_id in raw_ids}

    # Terminal detect: writes entropy_objects AND persists readiness in one pass.
    assert run_detectors(worker_manager, run_id=run_id) > 0

    with worker_manager.session_scope() as session:
        rows = list(
            session.execute(
                select(EntropyReadinessRecord).where(EntropyReadinessRecord.run_id == run_id)
            ).scalars()
        )

    assert rows, "terminal detect persisted no entropy_readiness rows"
    column_ids = [r.column_id for r in rows]
    assert all(cid is not None for cid in column_ids), "readiness row missing column_id FK"
    assert len(column_ids) == len(set(column_ids)), "duplicate readiness rows for a column"
    for r in rows:
        assert r.table_id in typed_ids, "readiness row points to a non-source table"
        assert r.band in ("ready", "investigate", "blocked")
        # JSONB payloads — intents may be empty on clean data (all signals below
        # the detection floor), but the shape must hold when present.
        assert isinstance(r.intents, list)
        for intent in r.intents:
            assert {"intent", "band", "risk", "drivers"} <= intent.keys()
            assert isinstance(intent["drivers"], list)
    first_count = len(rows)

    # Re-run the terminal detect (the replay path always re-runs it): the
    # delete-before-insert scoped to the session's tables (DAT-410) must
    # overwrite, not duplicate. (Single-source run: session tables = source tables.)
    assert run_detectors(worker_manager, run_id=run_id) > 0
    with worker_manager.session_scope() as session:
        second_count = len(
            list(
                session.execute(
                    select(EntropyReadinessRecord).where(EntropyReadinessRecord.run_id == run_id)
                ).scalars()
            )
        )
    assert second_count == first_count, (
        "readiness rows not overwritten on re-run (stale duplicates)"
    )


def test_persisted_readiness_is_single_source_of_truth(
    worker_manager: ConnectionManager, small_finance_path: Path
) -> None:
    """Query-time reads of the persisted band match the live rollup (DAT-399 slice D).

    After terminal detect persists readiness, the query-time consumers no longer
    recompute the loss rollup: they read ``load_persisted_readiness`` for the band and
    ``build_column_evidence`` (rollup-free) for the contract gate. Both must equal
    what the full ``build_for_readiness`` rollup produced — proving the persisted
    snapshot IS the source of truth and the contract dimension_scores are unchanged
    (calibration-preserving).
    """
    from dataraum.entropy.views.query_context import network_to_column_summaries
    from dataraum.entropy.views.readiness_context import (
        build_column_evidence,
        build_for_readiness,
        load_persisted_readiness,
    )

    source_id = str(uuid4())
    session_id = str(uuid4())
    # Stamp a run_id + promote at the end, exactly as AddSourceWorkflow does: the
    # workflow mints run_id via workflow.uuid4() before the first activity and runs
    # the terminal promote_to_latest after detect. load_persisted_readiness is
    # head-resolved (DAT-413) — it reads the run the snapshot head names — so the
    # query-time read only resolves once this run is promoted.
    run_id = str(uuid4())
    files = _enumerate_fixture_files(small_finance_path)
    _seed_source(worker_manager, source_id, "small_finance", files)

    import_identity = _identity(session_id, source_id=source_id, run_id=run_id)
    assert run_phase(worker_manager, "import", import_identity, [], "finance").status == "completed"
    raw_ids = raw_table_ids(worker_manager, source_id)
    # Past import: source-free (DAT-422) — the chain, detect AND promote all run
    # under the source-free identity, exactly as AddSourceWorkflow threads it.
    identity = _identity(session_id, run_id=run_id)
    typed_ids = sorted({_process_one_table(worker_manager, identity, raw_id) for raw_id in raw_ids})
    assert run_detectors(worker_manager, run_id=run_id) > 0
    # Promote this run so the head names it — the query-time read is head-resolved.
    assert promote_run(worker_manager, identity) > 0

    with worker_manager.session_scope() as session:
        live = build_for_readiness(session, typed_ids)
        persisted = load_persisted_readiness(session, typed_ids)

        # Band parity: same columns, same band + worst_intent_risk per column.
        assert persisted.columns.keys() == live.columns.keys()
        assert persisted.columns, "expected at least one column with readiness"
        for target, live_col in live.columns.items():
            pcol = persisted.columns[target]
            assert pcol.readiness == live_col.readiness, target
            assert pcol.worst_intent_risk == round(live_col.worst_intent_risk, 4), target
            assert {i.intent_name for i in pcol.intents} == {
                i.intent_name for i in live_col.intents
            }, target
        assert persisted.overall_readiness == live.overall_readiness
        assert persisted.columns_blocked == live.columns_blocked
        assert persisted.columns_investigate == live.columns_investigate

        # Contract parity: rollup-free evidence + persisted band yields identical
        # dimension_scores AND readiness (the calibration-preserving invariant).
        live_summaries = network_to_column_summaries(live)
        band_by_target = {t: c.readiness for t, c in persisted.columns.items()}
        evidence_summaries = network_to_column_summaries(
            build_column_evidence(session, typed_ids), band_by_target=band_by_target
        )
        assert evidence_summaries.keys() == live_summaries.keys()
        for key, live_summary in live_summaries.items():
            assert evidence_summaries[key].dimension_scores == live_summary.dimension_scores, key
            assert evidence_summaries[key].readiness == live_summary.readiness, key


def test_source_free_children_run_the_full_per_table_chain(
    worker_manager: ConnectionManager, small_finance_path: Path
) -> None:
    """The fan-out children run SOURCE-FREE (DAT-422) — typing + every analytics
    phase resolve their table by ``table_ids`` alone, never ``source_id``.

    AddSourceWorkflow scopes ``import`` to each source but threads a source-free
    identity (``source_id=None``) into the per-table children + the reduce. This
    drives that exact shape: ``import`` with the source-bearing identity, then the
    whole ProcessTableWorkflow body (typing → statistics → … → temporal) under a
    source-free identity. It guards the regression where the table-local phases
    scoped via ``Table.source_id == require_source_id()`` and raised on the
    source-free child — a break the source-bearing test identities masked. Since
    DAT-426 the whole suite defaults to the source-free shape; this test stays as
    the explicit, named statement of the invariant.
    """
    source_id = str(uuid4())
    session_id = str(uuid4())
    run_id = str(uuid4())
    files = _enumerate_fixture_files(small_finance_path)
    _seed_source(worker_manager, source_id, "small_finance", files)

    # import is the one per-source activity — it needs the source.
    import_identity = _identity(session_id, source_id=source_id, run_id=run_id)
    assert run_phase(worker_manager, "import", import_identity, [], "finance").status == "completed"
    raw_ids = raw_table_ids(worker_manager, source_id)
    assert raw_ids, "import produced no raw tables"

    # Past import the run is source-free: the children carry source_id=None. Running
    # the full per-table chain under it proves typing + every analytics phase resolve
    # their table without a source (the blocker DAT-422 introduced + fixed). The
    # children still carry the run_id so typing can link run_tables (DAT-506).
    child_identity = SourceIdentity(
        workspace_id="test", source_id=None, session_id=session_id, run_id=run_id
    )
    for raw_id in raw_ids:
        assert _process_one_table(worker_manager, child_identity, raw_id)


def test_parallel_tables_do_not_conflict_and_terminal_detect_covers_all(
    worker_manager: ConnectionManager, small_finance_path: Path
) -> None:
    """Per-table analytics chains run concurrently; one terminal detect covers all.

    The DAT-370 concurrency assertion (analytics fan-out) + the DAT-394 terminal
    detect, over the DAT-378 multi-file source. The source is seeded with a
    multi-URI ``file_uris`` list, so ONE ``import`` activity yields >1 raw tables;
    then each raw table's typing→analytics chain runs on its own thread — the
    fan-out the parent does with child workflows. Each ``run_phase`` call leases
    its own DuckDB cursor (an independent channel to the shared lake), so DuckLake
    reconciles the writers via MVCC.

    Detectors no longer run per table (DAT-394): a single terminal
    ``run_detectors`` pass runs them once, source-wide, after the fan-out. We
    assert it lands each typed table's full table-local detector set — proving the
    one source-wide pass covers every table that the concurrent chains produced.
    """
    source_id = str(uuid4())
    session_id = str(uuid4())
    run_id = str(uuid4())
    files = _enumerate_fixture_files(small_finance_path)
    _seed_source(worker_manager, source_id, "small_finance", files)

    # ONE import activity (source-bearing) over the multi-URI list yields N raw tables.
    import_identity = _identity(session_id, source_id=source_id, run_id=run_id)
    assert run_phase(worker_manager, "import", import_identity, [], "finance").status == "completed"
    raw_ids = raw_table_ids(worker_manager, source_id)
    assert len(raw_ids) > 1, "one import over a multi-URI source must yield >1 raw tables"
    assert len(raw_ids) == len(files), "each enumerated URI maps to one raw table"

    # Past import: source-free, as the workflow threads it into the children (DAT-422).
    # The run_id threads through typing (which links run_tables) + detect (DAT-506).
    identity = _identity(session_id, run_id=run_id)

    # Fan the per-table analytics chains out across threads — concurrent typing +
    # analytics against the shared lake from independent cursors.
    with ThreadPoolExecutor(max_workers=len(raw_ids)) as executor:
        futures = [
            executor.submit(_process_one_table, worker_manager, identity, raw_id)
            for raw_id in raw_ids
        ]
        typed_ids = {future.result() for future in futures}

    assert len(typed_ids) == len(raw_ids), "each raw table maps to a distinct typed table"

    # One terminal source-wide detect pass after the fan-out (DAT-394).
    assert run_detectors(worker_manager, run_id=run_id) > 0, "terminal detect produced no records"

    # Every detector record belongs to one of this source's typed tables (no
    # orphan), and each typed table carries the full table-local detector set —
    # proof the single source-wide pass reached every table the fan-out produced.
    with worker_manager.session_scope() as session:
        rows = session.execute(
            select(EntropyObjectRecord.table_id, EntropyObjectRecord.detector_id).where(
                EntropyObjectRecord.run_id == run_id,
                EntropyObjectRecord.detector_id.in_(_TABLE_LOCAL_DETECTORS),
            )
        ).all()

    detectors_by_table: dict[str, set[str]] = defaultdict(set)
    for table_id, detector_id in rows:
        assert table_id in typed_ids, f"detector record for unknown table {table_id}"
        detectors_by_table[table_id].add(detector_id)

    assert set(detectors_by_table) == typed_ids, (
        "some typed table is missing all its detector records — the terminal "
        "source-wide detect did not reach every table"
    )
    for table_id, detectors in detectors_by_table.items():
        assert detectors == _TABLE_LOCAL_DETECTORS, (
            f"table {table_id} missing detectors {_TABLE_LOCAL_DETECTORS - detectors}"
        )


@pytest.mark.skipif(
    not os.environ.get(_LIVE_LLM_ENV),
    reason=(
        f"Set {_LIVE_LLM_ENV}=1 (with a real ANTHROPIC_API_KEY) to run the live "
        "semantic_per_column reduce — it makes real Anthropic calls."
    ),
)
def test_semantic_per_column_reduce_runs_live(
    worker_manager: ConnectionManager, small_finance_path: Path
) -> None:
    """semantic_per_column runs as the source-level reduce end-to-end against a real LLM.

    Opt-in only (real Anthropic calls). Validates provider/prompt-config
    resolution + the API key in the worker substrate — the one slice-1 activity
    E4a could not de-risk offline. It depends on statistics, so the per-table
    chain runs first, then the source-level reduce.
    """
    source_id = str(uuid4())
    session_id = str(uuid4())
    run_id = str(uuid4())
    files = _enumerate_fixture_files(small_finance_path)
    _seed_source(worker_manager, source_id, "small_finance", files)

    import_identity = _identity(session_id, source_id=source_id, run_id=run_id)
    assert run_phase(worker_manager, "import", import_identity, [], "finance").status == "completed"
    # Past import: source-free (DAT-422) — typing, statistics AND the reduce. The
    # run_id threads through typing (which links run_tables) + detect (DAT-506).
    identity = _identity(session_id, run_id=run_id)
    for raw_id in raw_table_ids(worker_manager, source_id):
        assert (
            run_phase(worker_manager, "typing", identity, [raw_id], "finance").status == "completed"
        )
        typed_id = typed_table_id_for_raw(worker_manager, raw_id)
        assert (
            run_phase(worker_manager, "statistics", identity, [typed_id], "finance").status
            == "completed"
        )

    result = run_phase(worker_manager, "semantic_per_column", identity, [], "finance")
    assert result.status == "completed", f"semantic_per_column failed: {result.error}"

    # Terminal detect step (DAT-394) runs all wired detectors source-wide after the
    # reduce — including semantic_per_column's (business_meaning, unit_entropy, …).
    # With real annotations present it should persist records.
    records = run_detectors(worker_manager, run_id=run_id)
    assert records > 0, "terminal detect produced no detector records"
