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
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import select, text

from dataraum.core.connections import ConnectionConfig, ConnectionManager
from dataraum.entropy.db_models import EntropyObjectRecord, EntropyReadinessRecord
from dataraum.investigation.db_models import InvestigationSession
from dataraum.storage import Source, Table
from dataraum.worker import (
    SourceIdentity,
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


def _identity(source_id: str, session_id: str) -> SourceIdentity:
    return SourceIdentity(workspace_id="test", source_id=source_id, session_id=session_id)


def _seed_source_and_session(
    manager: ConnectionManager,
    source_id: str,
    session_id: str,
    name: str,
    paths: Path | list[Path],
) -> None:
    """Seed the Source + InvestigationSession rows the workflow would create.

    ``begin_session``/``addSourceWorkflow`` writes these in production; the
    activity reads the Source for its config and FK-references the session.

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
        session.flush()
        session.add(
            InvestigationSession(
                session_id=session_id,
                source_id=source_id,
                intent="e4a de-risk",
                status="active",
                started_at=datetime.now(UTC),
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
    typing = run_phase(manager, "typing", identity, [raw_table_id])
    assert typing.status == "completed", f"typing failed: {typing.error}"

    typed_id = typed_table_id_for_raw(manager, identity.source_id, raw_table_id)
    assert typed_id is not None, f"no typed table for raw {raw_table_id}"
    assert typed_id != raw_table_id, "typed id must differ from the raw id"

    for phase in _ANALYTICS_PHASES:
        result = run_phase(manager, phase, identity, [typed_id])
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
    _seed_source_and_session(worker_manager, source_id, session_id, "orders", csv)
    identity = _identity(source_id, session_id)

    # import (no LLM, no prereq) — raw tables land; the source's raw ids are the
    # fan-out source the parent reads.
    import_result = run_phase(worker_manager, "import", identity, [])
    assert import_result.status == "completed", import_result.error
    raw_ids = raw_table_ids(worker_manager, source_id)
    assert raw_ids, "import produced no raw tables"
    assert _lake_tables(worker_manager, "raw"), "no tables in lake.raw after import"

    # The DuckLake connection the worker holds is reused — not reopened — for the
    # next phase. Capture identity to prove the anchor survived the boundary.
    duckdb_conn_after_import = worker_manager._duckdb_conn  # noqa: SLF001

    # typing scoped to the single raw table (DuckDB-heavy; runs on the SAME
    # long-lived manager).
    typing_result = run_phase(worker_manager, "typing", identity, [raw_ids[0]])
    assert typing_result.status == "completed", typing_result.error
    assert _lake_tables(worker_manager, "typed"), "no tables in lake.typed after typing"
    assert typed_table_id_for_raw(worker_manager, source_id, raw_ids[0]) is not None

    assert worker_manager._duckdb_conn is duckdb_conn_after_import, (  # noqa: SLF001
        "worker DuckDB connection was reopened between phases — the anchor "
        "lifecycle did not survive the phase boundary"
    )


def test_unknown_phase_returns_failed(worker_manager: ConnectionManager) -> None:
    """A phase name not in the registry fails cleanly rather than raising."""
    identity = _identity(str(uuid4()), str(uuid4()))
    result = run_phase(worker_manager, "does_not_exist", identity, [])
    assert result.status == "failed"
    assert "does_not_exist" in (result.error or "")


def test_workspace_mismatch_fails_loud(worker_manager: ConnectionManager) -> None:
    """A payload addressed to another workspace is refused before any work.

    Anti-footgun for the deferred multi-workspace isolation (DAT-364): the worker
    is bound to one workspace (``"test"`` under the conftest pointer), so a
    mismatched ``workspace_id`` must fail rather than silently write into this
    worker's lake/schema. FAILED here becomes a non-retryable PhaseFailed in the
    activity wrapper.
    """
    identity = SourceIdentity(
        workspace_id="some-other-workspace",
        source_id=str(uuid4()),
        session_id=str(uuid4()),
    )
    result = run_phase(worker_manager, "import", identity, [])
    assert result.status == "failed"
    assert "Workspace mismatch" in (result.error or "")
    assert "some-other-workspace" in (result.error or "")


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
    test uses. The mismatch guard means a stray ``"test"`` left anywhere in the
    path would fail this loudly.
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
        files = _enumerate_fixture_files(small_finance_path)
        _seed_source_and_session(manager, source_id, session_id, "small_finance", files)
        identity = SourceIdentity(
            workspace_id=nondefault_workspace,
            source_id=source_id,
            session_id=session_id,
        )

        assert run_phase(manager, "import", identity, []).status == "completed"
        raw_ids = raw_table_ids(manager, source_id)
        assert raw_ids, "import produced no raw tables under the non-default workspace"

        typed_ids = [_process_one_table(manager, identity, raw_id) for raw_id in raw_ids]
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
    files = _enumerate_fixture_files(small_finance_path)
    _seed_source_and_session(worker_manager, source_id, session_id, "small_finance", files)
    identity = _identity(source_id, session_id)

    # ONE import activity over the multi-URI list yields N raw tables.
    assert run_phase(worker_manager, "import", identity, []).status == "completed"
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

    typed_ids = [_process_one_table(worker_manager, identity, raw_id) for raw_id in raw_ids]
    assert len(typed_ids) == len(raw_ids)
    assert _lake_tables(worker_manager, "typed"), "no typed tables after the chain"

    # Single terminal detect step (DAT-394): one source-wide pass after the
    # fan-out. Offline (no LLM), so only the structural detectors persist rows;
    # that is enough to prove the terminal step runs and writes per-table records.
    assert run_detectors(worker_manager, identity) > 0, "terminal detect produced no records"


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
    files = _enumerate_fixture_files(small_finance_path)
    _seed_source_and_session(worker_manager, source_id, session_id, "small_finance", files)
    identity = _identity(source_id, session_id)

    assert run_phase(worker_manager, "import", identity, []).status == "completed"
    raw_ids = raw_table_ids(worker_manager, source_id)
    typed_ids = {_process_one_table(worker_manager, identity, raw_id) for raw_id in raw_ids}

    # Terminal detect: writes entropy_objects AND persists readiness in one pass.
    assert run_detectors(worker_manager, identity) > 0

    with worker_manager.session_scope() as session:
        rows = list(
            session.execute(
                select(EntropyReadinessRecord).where(
                    EntropyReadinessRecord.source_id == source_id
                )
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
    # delete-before-insert scoped to source_id must overwrite, not duplicate.
    assert run_detectors(worker_manager, identity) > 0
    with worker_manager.session_scope() as session:
        second_count = len(
            list(
                session.execute(
                    select(EntropyReadinessRecord).where(
                        EntropyReadinessRecord.source_id == source_id
                    )
                ).scalars()
            )
        )
    assert second_count == first_count, "readiness rows not overwritten on re-run (stale duplicates)"


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
    files = _enumerate_fixture_files(small_finance_path)
    _seed_source_and_session(worker_manager, source_id, session_id, "small_finance", files)
    identity = _identity(source_id, session_id)

    # ONE import activity over the multi-URI list yields N raw tables.
    assert run_phase(worker_manager, "import", identity, []).status == "completed"
    raw_ids = raw_table_ids(worker_manager, source_id)
    assert len(raw_ids) > 1, "one import over a multi-URI source must yield >1 raw tables"
    assert len(raw_ids) == len(files), "each enumerated URI maps to one raw table"

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
    assert run_detectors(worker_manager, identity) > 0, "terminal detect produced no records"

    # Every detector record belongs to one of this source's typed tables (no
    # orphan), and each typed table carries the full table-local detector set —
    # proof the single source-wide pass reached every table the fan-out produced.
    with worker_manager.session_scope() as session:
        rows = session.execute(
            select(EntropyObjectRecord.table_id, EntropyObjectRecord.detector_id).where(
                EntropyObjectRecord.source_id == source_id,
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
    files = _enumerate_fixture_files(small_finance_path)
    _seed_source_and_session(worker_manager, source_id, session_id, "small_finance", files)
    identity = _identity(source_id, session_id)

    assert run_phase(worker_manager, "import", identity, []).status == "completed"
    for raw_id in raw_table_ids(worker_manager, source_id):
        assert run_phase(worker_manager, "typing", identity, [raw_id]).status == "completed"
        typed_id = typed_table_id_for_raw(worker_manager, source_id, raw_id)
        assert run_phase(worker_manager, "statistics", identity, [typed_id]).status == "completed"

    result = run_phase(worker_manager, "semantic_per_column", identity, [])
    assert result.status == "completed", f"semantic_per_column failed: {result.error}"

    # Terminal detect step (DAT-394) runs all wired detectors source-wide after the
    # reduce — including semantic_per_column's (business_meaning, unit_entropy, …).
    # With real annotations present it should persist records.
    records = run_detectors(worker_manager, identity)
    assert records > 0, "terminal detect produced no detector records"
