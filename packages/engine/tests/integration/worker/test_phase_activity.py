"""Integration test for the Temporal activity-worker substrate (DAT-344, P1).

This is the de-risk slice's core assertion: a single workspace-level
``ConnectionManager`` (the one a worker holds for its whole life) can run two
real phases — ``import`` then ``typing`` — as *separate* ``run_phase_activity``
calls, with the DuckLake ``:memory:`` anchor + the manager's DuckDB connection
surviving across both, and raw + typed tables landing in the lake. It runs
against the testcontainer Postgres + a real DuckLake (``lake_anchor``), not
SQLite or mocks — the substrate reconstitution is exactly what's under test.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from dataraum.core.connections import ConnectionConfig, ConnectionManager
from dataraum.investigation.db_models import InvestigationSession
from dataraum.storage import Source
from dataraum.worker import PhaseActivityInput, run_phase_activity

# The table-local analytics chain wrapped as activities in E4b (DAT-368), in
# dependency order. ``semantic_per_column`` (the first LLM phase) is exercised
# separately, gated behind a real key.
_SLICE1_ANALYTICS_CHAIN = (
    "import",
    "typing",
    "statistics",
    "column_eligibility",
    "statistical_quality",
    "temporal",
)

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


def test_import_then_typing_share_one_manager(
    worker_manager: ConnectionManager, tmp_path: Path
) -> None:
    """import + typing run as two activities on one manager; anchor survives both."""
    csv = tmp_path / "orders.csv"
    csv.write_text(
        "id,amount,booked_on\n1,10.50,2024-01-01\n2,20.00,2024-01-02\n3,30.25,2024-01-03\n"
    )
    source_id = str(uuid4())
    session_id = str(uuid4())
    _seed_source_and_session(worker_manager, source_id, session_id, "orders", csv)

    payload = PhaseActivityInput(workspace_id="test", source_id=source_id, session_id=session_id)

    # Activity 1: import (no LLM, no prereq) — raw tables land.
    import_result = run_phase_activity(worker_manager, "import", payload)
    assert import_result.status == "completed", import_result.error
    assert import_result.outputs.get("raw_tables"), "import produced no raw tables"
    assert _lake_tables(worker_manager, "raw"), "no tables in lake.raw after import"

    # The DuckLake connection the worker holds is reused — not reopened — for
    # the next activity. Capture identity to prove the anchor/connection
    # survived across the activity boundary.
    duckdb_conn_after_import = worker_manager._duckdb_conn  # noqa: SLF001

    # Activity 2: typing (DuckDB-heavy; runs the type_fidelity detector, which
    # writes session-scoped EntropyObjectRecord rows — exercises the session_id
    # FK + the cursor lifecycle on the SAME long-lived manager).
    typing_result = run_phase_activity(worker_manager, "typing", payload)
    assert typing_result.status == "completed", typing_result.error
    assert _lake_tables(worker_manager, "typed"), "no tables in lake.typed after typing"

    assert worker_manager._duckdb_conn is duckdb_conn_after_import, (  # noqa: SLF001
        "worker DuckDB connection was reopened between activities — the anchor "
        "lifecycle did not survive the activity boundary"
    )


def test_unknown_phase_returns_failed(worker_manager: ConnectionManager, tmp_path: Path) -> None:
    """A phase name not in the registry fails cleanly rather than raising."""
    payload = PhaseActivityInput(
        workspace_id="test", source_id=str(uuid4()), session_id=str(uuid4())
    )
    result = run_phase_activity(worker_manager, "does_not_exist", payload)
    assert result.status == "failed"
    assert "does_not_exist" in (result.error or "")


def test_slice1_analytics_chain_runs(
    worker_manager: ConnectionManager, small_finance_path: Path
) -> None:
    """The six table-local analytics activities run green on a multi-table source.

    Drives each phase through ``run_phase_activity`` — the production worker path,
    not the retired ``PipelineTestHarness`` — on the one long-lived worker
    manager. This is the E4b assertion that the slice-1 chain (minus the LLM
    ``semantic_per_column`` phase, exercised separately) runs end-to-end as
    activities against a real multi-table DuckLake source.
    """
    source_id = str(uuid4())
    session_id = str(uuid4())
    _seed_source_and_session(
        worker_manager, source_id, session_id, "small_finance", small_finance_path
    )
    payload = PhaseActivityInput(
        workspace_id="test", source_id=source_id, session_id=session_id
    )

    for phase in _SLICE1_ANALYTICS_CHAIN:
        result = run_phase_activity(worker_manager, phase, payload)
        assert result.status == "completed", f"{phase} failed: {result.error}"

    assert _lake_tables(worker_manager, "typed"), "no typed tables after the analytics chain"


@pytest.mark.skipif(
    not os.environ.get(_LIVE_LLM_ENV),
    reason=(
        f"Set {_LIVE_LLM_ENV}=1 (with a real ANTHROPIC_API_KEY) to run the live "
        "semantic_per_column activity — it makes real Anthropic calls."
    ),
)
def test_semantic_per_column_activity_runs_live(
    worker_manager: ConnectionManager, small_finance_path: Path
) -> None:
    """semantic_per_column runs as an activity end-to-end against a real LLM.

    Opt-in only (real Anthropic calls). Validates provider/prompt-config
    resolution + the API key in the worker substrate — the one slice-1 activity
    E4a could not de-risk offline.
    """
    source_id = str(uuid4())
    session_id = str(uuid4())
    _seed_source_and_session(
        worker_manager, source_id, session_id, "small_finance", small_finance_path
    )
    payload = PhaseActivityInput(
        workspace_id="test", source_id=source_id, session_id=session_id
    )

    # semantic_per_column depends on statistics (per pipeline.yaml); run its
    # prerequisites, then the LLM phase itself.
    for phase in ("import", "typing", "statistics", "semantic_per_column"):
        result = run_phase_activity(worker_manager, phase, payload)
        assert result.status == "completed", f"{phase} failed: {result.error}"
