"""Integration test for the Temporal activity-worker substrate (DAT-344; DAT-370).

The de-risk slice's core assertion: a single workspace-level ``ConnectionManager``
(the one a worker holds for its whole life) runs the real phases as separate
``run_phase`` calls, with the DuckLake ``:memory:`` anchor + the manager's DuckDB
connection surviving across them and rows landing in the lake. It runs against
the testcontainer Postgres + a real DuckLake (``lake_anchor``), not SQLite or
mocks — the substrate reconstitution is exactly what's under test.

DAT-370 makes the table the unit of work: ``import`` enumerates raw tables,
``typing`` mints one typed id per raw table, the analytics phases run scoped to a
single typed table, and ``detect_table`` runs the table-local detectors scoped to
that table. These tests drive that per-table path directly (the production
workflows fan it out across child workflows).
"""

from __future__ import annotations

import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import select

from dataraum.core.connections import ConnectionConfig, ConnectionManager
from dataraum.entropy.db_models import EntropyObjectRecord
from dataraum.investigation.db_models import InvestigationSession
from dataraum.storage import Source
from dataraum.worker import (
    SourceIdentity,
    raw_table_ids,
    run_phase,
    run_source_detectors,
    run_table_detectors,
    typed_table_id_for_raw,
)

# The table-local analytics phases wrapped as activities, in dependency order.
# ``typing`` precedes them (it mints the typed id); ``detect_table`` follows.
# ``semantic_per_column`` (the source-level LLM reduce) is exercised separately,
# gated behind a real key.
_ANALYTICS_PHASES = (
    "statistics",
    "column_eligibility",
    "statistical_quality",
    "temporal",
)

# The table-local detectors the stage-level detect step runs (pipeline.yaml:
# type_fidelity from typing, null_ratio from statistics).
_TABLE_LOCAL_DETECTORS = {"type_fidelity", "null_ratio"}

_LIVE_LLM_ENV = "DATARAUM_LIVE_LLM_TEST"


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
    path: Path,
) -> None:
    """Seed the Source + InvestigationSession rows the workflow would create.

    ``begin_session``/``addSourceWorkflow`` writes these in production; the
    activity reads the Source for its config and FK-references the session.
    """
    with manager.session_scope() as session:
        session.add(
            Source(
                source_id=source_id,
                name=name,
                source_type="csv",
                connection_config={"path": str(path)},
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

    typing(raw) -> typed_id -> analytics phases (typed_id) -> detect_table(typed_id).
    Returns the typed table id. Asserts each step completed.
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

    run_table_detectors(manager, identity, typed_id)
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


def test_per_table_chain_runs(worker_manager: ConnectionManager, small_finance_path: Path) -> None:
    """The full table-local chain runs green per table on a multi-table source.

    Drives the production worker path (``run_phase`` + ``run_table_detectors``,
    not the retired ``PipelineTestHarness``) on the one long-lived manager: import
    once, then the typing→analytics→detect chain scoped per raw table — the
    sequential shape of the ProcessTableWorkflow fan-out.
    """
    source_id = str(uuid4())
    session_id = str(uuid4())
    _seed_source_and_session(
        worker_manager, source_id, session_id, "small_finance", small_finance_path
    )
    identity = _identity(source_id, session_id)

    assert run_phase(worker_manager, "import", identity, []).status == "completed"
    raw_ids = raw_table_ids(worker_manager, source_id)
    assert len(raw_ids) > 1, "expected a multi-table source"

    typed_ids = [_process_one_table(worker_manager, identity, raw_id) for raw_id in raw_ids]
    assert len(typed_ids) == len(raw_ids)
    assert _lake_tables(worker_manager, "typed"), "no typed tables after the chain"


def test_parallel_tables_do_not_conflict_and_detectors_stay_table_scoped(
    worker_manager: ConnectionManager, small_finance_path: Path
) -> None:
    """Per-table chains run concurrently; the stage detect step stays table-scoped.

    The DAT-370 concurrency + correctness assertion. After ``import``, each raw
    table's full chain (typing→analytics→detect) runs on its own thread — the
    fan-out the parent does with child workflows. Each ``run_phase`` /
    ``run_table_detectors`` call leases its own DuckDB cursor (an independent
    channel to the shared lake), so DuckLake reconciles the writers via MVCC.

    The detector step is the part the per-phase post-step couldn't do safely:
    ``run_detector_post_step`` deletes-before-inserts on ``(source_id,
    detector_id)``. Scoped to one table, parallel children touch only their own
    rows — so every typed table ends up with its table-local detector records and
    none clobbers a sibling. We assert exactly that partition.
    """
    source_id = str(uuid4())
    session_id = str(uuid4())
    _seed_source_and_session(
        worker_manager, source_id, session_id, "small_finance", small_finance_path
    )
    identity = _identity(source_id, session_id)

    assert run_phase(worker_manager, "import", identity, []).status == "completed"
    raw_ids = raw_table_ids(worker_manager, source_id)
    assert len(raw_ids) > 1, "expected a multi-table source"

    # Fan the per-table chains out across threads — concurrent typing + analytics
    # + detect against the shared lake from independent cursors.
    with ThreadPoolExecutor(max_workers=len(raw_ids)) as executor:
        futures = [
            executor.submit(_process_one_table, worker_manager, identity, raw_id)
            for raw_id in raw_ids
        ]
        typed_ids = {future.result() for future in futures}

    assert len(typed_ids) == len(raw_ids), "each raw table maps to a distinct typed table"

    # Every detector record belongs to one of this source's typed tables (no
    # orphan / cross-table clobber), and each typed table carries the full set of
    # table-local detectors — proof the scoped delete-before-insert didn't wipe a
    # sibling's rows under concurrency.
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
        "some typed table is missing all its detector records — a concurrent "
        "scoped delete clobbered a sibling"
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
    _seed_source_and_session(
        worker_manager, source_id, session_id, "small_finance", small_finance_path
    )
    identity = _identity(source_id, session_id)

    assert run_phase(worker_manager, "import", identity, []).status == "completed"
    for raw_id in raw_table_ids(worker_manager, source_id):
        assert run_phase(worker_manager, "typing", identity, [raw_id]).status == "completed"
        typed_id = typed_table_id_for_raw(worker_manager, source_id, raw_id)
        assert run_phase(worker_manager, "statistics", identity, [typed_id]).status == "completed"

    result = run_phase(worker_manager, "semantic_per_column", identity, [])
    assert result.status == "completed", f"semantic_per_column failed: {result.error}"

    # Source-level detect step (detect_source) runs semantic_per_column's declared
    # detectors after the reduce — the path DAT-370 originally orphaned. With real
    # annotations present it should persist records.
    records = run_source_detectors(worker_manager, identity)
    assert records > 0, "detect_source produced no source-level detector records"
