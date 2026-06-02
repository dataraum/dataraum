"""Integration tests for the begin_session spine activities (DAT-401).

The session-scoped, source-free counterpart to ``test_phase_activity``. Drives
the real activity helpers against the testcontainer Postgres + a real DuckLake:

* ``begin_session_select`` pre-flights a selection (rejecting unknown ids) and
  links it to the session via ``session_tables`` — proven over a selection that
  **spans two sources**, the begin_session requirement.
* ``run_session_phase`` runs a revived cross-table phase (``relationships``)
  source-free: scoped purely to the session's selected typed tables, never a
  source. Typed tables are built by the real add_source chain so DuckDB has data
  to scan; begin_session then composes a subset of them.

The full Temporal workflow execution + offline Replayer determinism are covered
by compose-smoke (the project's Temporal-test convention), the same place the
add_source spine is exercised live; the live LLM ``semantic_per_table`` recall is
the dataraum-eval gate.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import select

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.core.connections import ConnectionConfig, ConnectionManager
from dataraum.investigation.db_models import InvestigationSession
from dataraum.investigation.queries import sources_for_session, tables_for_session
from dataraum.pipeline.base import PhaseStatus
from dataraum.storage import Source, Table
from dataraum.worker import (
    SessionIdentity,
    SourceIdentity,
    begin_session_select,
    raw_table_ids,
    run_phase,
    run_session_phase,
    typed_table_id_for_raw,
)


@pytest.fixture(autouse=True)
def _allow_local_fixture_uris(monkeypatch: pytest.MonkeyPatch) -> None:
    """Let import accept the local fixture paths (no object store in-process)."""
    monkeypatch.setattr(
        "dataraum.pipeline.phases.import_phase.validate_source_uri",
        lambda uri: uri,
    )


@pytest.fixture
def worker_manager(pg_url_clean: str, lake_anchor, lake_clean):  # noqa: ANN001
    """A workspace-level ConnectionManager built as the worker bootstraps it."""
    manager = ConnectionManager(ConnectionConfig(database_url=pg_url_clean))
    manager.initialize()
    manager.open_lake()
    yield manager
    manager.close()


_FIXTURE_DATA_SUFFIXES = (".csv", ".tsv", ".parquet", ".pq", ".json", ".jsonl")


def _enumerate_fixture_files(directory: Path) -> list[Path]:
    files = sorted(
        p for p in directory.iterdir() if p.is_file() and p.suffix.lower() in _FIXTURE_DATA_SUFFIXES
    )
    assert len(files) > 1, f"expected a multi-file fixture in {directory}"
    return files


def _seed_session(manager: ConnectionManager, session_id: str, vertical: str | None = None) -> None:
    """Seed the InvestigationSession the begin_session driver writes (cockpit in 2.0c)."""
    with manager.session_scope() as session:
        session.add(
            InvestigationSession(
                session_id=session_id,
                intent="begin_session test",
                status="active",
                started_at=datetime.now(UTC),
                vertical=vertical,
            )
        )


def _seed_typed_table(
    manager: ConnectionManager, source_id: str, source_name: str, name: str
) -> str:
    """Insert a Source (if new) + one typed Table row directly (no DuckDB data)."""
    table_id = str(uuid4())
    with manager.session_scope() as session:
        if session.get(Source, source_id) is None:
            # Source.name is UNIQUE — keep it unique per source within a run.
            session.add(
                Source(source_id=source_id, name=f"{source_name}_{source_id[:8]}", source_type="csv")
            )
            session.flush()
        session.add(
            Table(
                table_id=table_id,
                source_id=source_id,
                table_name=name,
                layer="typed",
                duckdb_path=f"typed_{name}",
                row_count=10,
            )
        )
    return table_id


def _session_identity(session_id: str) -> SessionIdentity:
    return SessionIdentity(workspace_id="test", session_id=session_id)


# ---------------------------------------------------------------------------
# begin_session_select — preflight + session_tables link (cross-source).
# ---------------------------------------------------------------------------


def test_select_links_a_cross_source_selection(worker_manager: ConnectionManager) -> None:
    """A selection spanning two sources is linked to the session; source is derived."""
    session_id = str(uuid4())
    _seed_session(worker_manager, session_id)
    src_a, src_b = str(uuid4()), str(uuid4())
    a1 = _seed_typed_table(worker_manager, src_a, "src_a", "a_orders")
    a2 = _seed_typed_table(worker_manager, src_a, "src_a", "a_lines")
    b1 = _seed_typed_table(worker_manager, src_b, "src_b", "b_customers")

    run = begin_session_select(worker_manager, _session_identity(session_id), [a1, a2, b1])
    assert run.status == PhaseStatus.COMPLETED.value, run.error

    with worker_manager.session_scope() as session:
        assert set(tables_for_session(session, session_id)) == {a1, a2, b1}
        # The session's source is DERIVED from its tables — and spans both.
        assert sources_for_session(session, session_id) == {src_a, src_b}


def test_select_rejects_unknown_table_ids(worker_manager: ConnectionManager) -> None:
    """An id that is not a known typed table fails loud — nothing is linked."""
    session_id = str(uuid4())
    _seed_session(worker_manager, session_id)
    known = _seed_typed_table(worker_manager, str(uuid4()), "src", "t1")
    ghost = str(uuid4())

    run = begin_session_select(worker_manager, _session_identity(session_id), [known, ghost])
    assert run.status == PhaseStatus.FAILED.value
    assert ghost in (run.error or "")

    with worker_manager.session_scope() as session:
        assert tables_for_session(session, session_id) == []


def test_select_rejects_raw_table_ids(worker_manager: ConnectionManager) -> None:
    """A non-typed (raw) table is not a valid begin_session selection member."""
    session_id = str(uuid4())
    _seed_session(worker_manager, session_id)
    source_id = str(uuid4())
    raw_id = str(uuid4())
    with worker_manager.session_scope() as session:
        session.add(Source(source_id=source_id, name="src", source_type="csv"))
        session.flush()
        session.add(
            Table(
                table_id=raw_id,
                source_id=source_id,
                table_name="raw_orders",
                layer="raw",
                duckdb_path="raw_orders",
            )
        )

    run = begin_session_select(worker_manager, _session_identity(session_id), [raw_id])
    assert run.status == PhaseStatus.FAILED.value


def test_select_fails_when_session_not_seeded(worker_manager: ConnectionManager) -> None:
    """The driver must seed the session row first (mirrors add_source's cockpit seed)."""
    session_id = str(uuid4())  # never seeded
    t1 = _seed_typed_table(worker_manager, str(uuid4()), "src", "t1")

    run = begin_session_select(worker_manager, _session_identity(session_id), [t1])
    assert run.status == PhaseStatus.FAILED.value
    assert session_id in (run.error or "")


def test_select_requires_at_least_one_table(worker_manager: ConnectionManager) -> None:
    session_id = str(uuid4())
    _seed_session(worker_manager, session_id)
    run = begin_session_select(worker_manager, _session_identity(session_id), [])
    assert run.status == PhaseStatus.FAILED.value


# ---------------------------------------------------------------------------
# run_session_phase — source-free runner over real typed tables (relationships).
# ---------------------------------------------------------------------------


def _build_typed_tables(manager: ConnectionManager, small_finance_path: Path) -> list[str]:
    """Run the add_source import→typing chain to get real DuckDB-backed typed tables."""
    source_id = str(uuid4())
    add_session_id = str(uuid4())
    files = _enumerate_fixture_files(small_finance_path)
    with manager.session_scope() as session:
        session.add(
            Source(
                source_id=source_id,
                name=f"sf_{source_id[:8]}",
                source_type="csv",
                connection_config={"file_uris": [str(p) for p in files]},
                status="configured",
            )
        )
        session.flush()
        session.add(
            InvestigationSession(
                session_id=add_session_id,
                intent="add",
                status="active",
                started_at=datetime.now(UTC),
            )
        )
    identity = SourceIdentity(workspace_id="test", source_id=source_id, session_id=add_session_id)
    assert run_phase(manager, "import", identity, []).status == "completed"
    typed: list[str] = []
    for raw_id in raw_table_ids(manager, source_id):
        assert run_phase(manager, "typing", identity, [raw_id]).status == "completed"
        typed_id = typed_table_id_for_raw(manager, source_id, raw_id)
        assert typed_id is not None
        typed.append(typed_id)
    assert len(typed) > 1, "need >1 typed table to detect relationships"
    return typed


def test_run_session_phase_relationships_scopes_to_selection(
    worker_manager: ConnectionManager, small_finance_path: Path
) -> None:
    """``relationships`` via the source-free runner scopes to the session's selection.

    Real typed tables (built by the add_source chain) give DuckDB data to scan.
    begin_session composes a SUBSET; any candidate relationships the phase
    persists must reference only the selected tables — never an excluded one,
    proving the phase scopes by ``ctx.table_ids`` and ignores ``source_id``.
    """
    typed = _build_typed_tables(worker_manager, small_finance_path)
    selection = typed[:2]
    excluded = set(typed) - set(selection)

    session_id = str(uuid4())
    _seed_session(worker_manager, session_id, vertical="_adhoc")
    sel_run = begin_session_select(worker_manager, _session_identity(session_id), selection)
    assert sel_run.status == PhaseStatus.COMPLETED.value, sel_run.error

    run = run_session_phase(
        worker_manager, "relationships", _session_identity(session_id), selection
    )
    # Completed (candidates found) or skipped (<2 overlap) — never FAILED.
    assert run.status in (PhaseStatus.COMPLETED.value, PhaseStatus.SKIPPED.value), run.error

    with worker_manager.session_scope() as session:
        rels = list(
            session.execute(
                select(Relationship).where(Relationship.session_id == session_id)
            ).scalars()
        )
    scoped = set(selection)
    for rel in rels:
        assert rel.from_table_id in scoped, "candidate references a table outside the selection"
        assert rel.to_table_id in scoped, "candidate references a table outside the selection"
        assert rel.from_table_id not in excluded and rel.to_table_id not in excluded


def test_run_session_phase_fails_when_session_not_seeded(
    worker_manager: ConnectionManager,
) -> None:
    """The runner fails loud if the InvestigationSession row is missing."""
    t1 = _seed_typed_table(worker_manager, str(uuid4()), "src", "t1")
    t2 = _seed_typed_table(worker_manager, str(uuid4()), "src", "t2")
    run = run_session_phase(
        worker_manager, "relationships", _session_identity(str(uuid4())), [t1, t2]
    )
    assert run.status == PhaseStatus.FAILED.value
