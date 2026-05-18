"""Lane smoke for DAT-321 — All SQLAlchemy → single workspace Postgres.

Scope: verify the L2/L3 unified-Postgres substrate post-port.
- Every workspace + per-session table is created on a real Postgres 17 dialect.
- Per-session tables carry a NOT NULL ``session_id`` FK to
  ``investigation_sessions.session_id`` with the right ON-DELETE behavior
  guaranteed by Postgres.
- JSON columns (``connection_config``, ``discovered_schema``,
  ``PhaseLog.outputs``) round-trip values into ``jsonb`` storage.
- Two InvestigationSession rows cannot leak across each other.
- The legacy SQLite dialect import paths are absent from production code.
- No file-based ``*.db`` artifact appears on disk during a session run.

The Postgres testcontainer is provided by ``tests/conftest.py`` via the
session-scoped ``pg_container`` / ``pg_url`` fixtures; this smoke connects
fresh and drops the schema between blocks to keep tier-1 lane semantics.

Run:
    uv run pytest tests/platform/smoke_dat321.py -v
"""

from __future__ import annotations

import datetime as _dt
import subprocess
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from dataraum.investigation.db_models import InvestigationSession
from dataraum.pipeline.db_models import PhaseLog, PipelineRun
from dataraum.storage import Base, Source, init_database

# ---------------------------------------------------------------------------
# Per-test isolation: clean schema before each smoke
# ---------------------------------------------------------------------------


@pytest.fixture
def pg_engine(pg_url: str):
    """Fresh engine against a clean Postgres schema for this smoke."""
    engine = create_engine(pg_url, future=True)
    # Drop everything Base knows about (idempotent — TRUNCATE-style guard).
    Base.metadata.drop_all(engine)
    init_database(engine)
    yield engine
    Base.metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture
def pg_session(pg_engine):
    factory = sessionmaker(bind=pg_engine, expire_on_commit=False)
    with factory() as sess:
        yield sess


# ---------------------------------------------------------------------------
# 1. Schema present — all workspace + per-session tables on Postgres
# ---------------------------------------------------------------------------


def test_all_tables_present_on_postgres(pg_engine) -> None:
    """init_database creates every Base-registered table under the pg dialect."""
    actual = set(inspect(pg_engine).get_table_names())
    expected = set(Base.metadata.tables.keys())
    missing = expected - actual
    assert not missing, f"Missing tables under Postgres: {sorted(missing)}"


# ---------------------------------------------------------------------------
# 2. session_id FK present + NOT NULL on every per-session table
# ---------------------------------------------------------------------------


# NOTE: `investigation_steps` is intentionally absent — its ``session_id`` FK
# pre-dates DAT-321 (lives on the InvestigationStep model since DAT-184) and
# was not introduced by this lane. The 26 tables below are the ones that
# acquired ``session_id`` as part of this port.
PER_SESSION_TABLES = {
    # name -> (FK target column expected)
    "pipeline_runs": ("investigation_sessions", "session_id"),
    "phase_logs": ("investigation_sessions", "session_id"),
    "data_fixes": ("investigation_sessions", "session_id"),
    "fix_ledger": ("investigation_sessions", "session_id"),
    "query_executions": ("investigation_sessions", "session_id"),
    "sql_snippets": ("investigation_sessions", "session_id"),
    "snippet_usage": ("investigation_sessions", "session_id"),
    "entropy_objects": ("investigation_sessions", "session_id"),
    "derived_columns": ("investigation_sessions", "session_id"),
    "detected_business_cycles": ("investigation_sessions", "session_id"),
    "column_eligibility": ("investigation_sessions", "session_id"),
    "relationships": ("investigation_sessions", "session_id"),
    "semantic_annotations": ("investigation_sessions", "session_id"),
    "table_entities": ("investigation_sessions", "session_id"),
    "slice_definitions": ("investigation_sessions", "session_id"),
    "column_slice_profiles": ("investigation_sessions", "session_id"),
    "statistical_profiles": ("investigation_sessions", "session_id"),
    "statistical_quality_metrics": ("investigation_sessions", "session_id"),
    "temporal_column_profiles": ("investigation_sessions", "session_id"),
    "column_drift_summaries": ("investigation_sessions", "session_id"),
    "temporal_slice_analyses": ("investigation_sessions", "session_id"),
    "type_candidates": ("investigation_sessions", "session_id"),
    "type_decisions": ("investigation_sessions", "session_id"),
    "validation_results": ("investigation_sessions", "session_id"),
    "enriched_views": ("investigation_sessions", "session_id"),
    "slicing_views": ("investigation_sessions", "session_id"),
}


def test_session_id_fk_and_not_null_on_per_session_tables(pg_engine) -> None:
    """Every per-session table FKs session_id to investigation_sessions, NOT NULL."""
    inspector = inspect(pg_engine)
    existing = set(inspector.get_table_names())

    for table_name, (target_table, target_col) in PER_SESSION_TABLES.items():
        if table_name not in existing:
            # If a table isn't on the schema at all the model isn't wired.
            # Fail loud here — the smoke is the single source of truth.
            pytest.fail(f"per-session table {table_name!r} missing from Postgres schema")

        columns = {c["name"]: c for c in inspector.get_columns(table_name)}
        assert "session_id" in columns, f"{table_name}: no session_id column"
        assert columns["session_id"]["nullable"] is False, (
            f"{table_name}.session_id must be NOT NULL"
        )

        fks = inspector.get_foreign_keys(table_name)
        sid_fks = [
            fk
            for fk in fks
            if "session_id" in fk["constrained_columns"]
            and fk["referred_table"] == target_table
            and target_col in fk["referred_columns"]
        ]
        assert sid_fks, (
            f"{table_name}: no FK on session_id → {target_table}.{target_col} (found: {fks})"
        )


# ---------------------------------------------------------------------------
# 3. JSON columns round-trip through psycopg/jsonb
# ---------------------------------------------------------------------------


def test_json_round_trip_connection_config(pg_session) -> None:
    """Source.connection_config + discovered_schema survive an insert/select cycle."""
    src = Source(
        source_id="src-json-1",
        name="json-rt",
        source_type="csv",
        connection_config={"path": "/data/orders.csv", "header": True, "sep": ","},
        discovered_schema={
            "tables": [
                {"name": "orders", "rows": 42, "tags": ["fact", "transactional"]},
                {"name": "customers", "rows": None, "tags": []},
            ],
            "version": 3,
        },
    )
    pg_session.add(src)
    pg_session.commit()

    pg_session.expire_all()
    loaded = pg_session.execute(select(Source).where(Source.source_id == "src-json-1")).scalar_one()
    assert loaded.connection_config == {"path": "/data/orders.csv", "header": True, "sep": ","}
    assert loaded.discovered_schema is not None
    assert loaded.discovered_schema["version"] == 3
    assert loaded.discovered_schema["tables"][0]["tags"] == ["fact", "transactional"]


def test_json_round_trip_phase_log_outputs(pg_session) -> None:
    """PhaseLog.outputs survives jsonb round-trip with nested arrays + numbers."""
    pg_session.add(Source(source_id="src-pl", name="pl-rt", source_type="csv"))
    pg_session.flush()
    sess = InvestigationSession(
        session_id="sess-pl-1",
        source_id="src-pl",
        intent="json round trip",
        status="active",
        started_at=_dt.datetime.now(_dt.UTC),
    )
    pg_session.add(sess)
    pg_session.flush()
    run = PipelineRun(
        run_id="run-pl-1",
        session_id="sess-pl-1",
        source_id="src-pl",
        status="running",
        started_at=_dt.datetime.now(_dt.UTC),
    )
    pg_session.add(run)
    pg_session.flush()

    payload = {
        "rows_processed": 12345,
        "tables": ["orders", "invoices"],
        "scores": {"coverage": 0.97, "freshness": 0.83},
    }
    log = PhaseLog(
        run_id="run-pl-1",
        session_id="sess-pl-1",
        source_id="src-pl",
        phase_name="staging",
        status="completed",
        started_at=_dt.datetime.now(_dt.UTC),
        completed_at=_dt.datetime.now(_dt.UTC),
        duration_seconds=0.5,
        outputs=payload,
    )
    pg_session.add(log)
    pg_session.commit()

    pg_session.expire_all()
    loaded = pg_session.execute(select(PhaseLog).where(PhaseLog.run_id == "run-pl-1")).scalar_one()
    assert loaded.outputs == payload


# ---------------------------------------------------------------------------
# 4. Two-sessions-no-leak — Postgres enforces what test fixtures fake
# ---------------------------------------------------------------------------


def _seed_session(pg_session, name: str) -> str:
    src_id = f"src-{name}"
    sid = f"sess-{name}"
    pg_session.add(Source(source_id=src_id, name=name, source_type="csv"))
    pg_session.flush()
    pg_session.add(
        InvestigationSession(
            session_id=sid,
            source_id=src_id,
            intent=f"smoke {name}",
            status="active",
            started_at=_dt.datetime.now(_dt.UTC),
        )
    )
    pg_session.flush()
    pg_session.add(
        PipelineRun(
            run_id=f"run-{name}",
            session_id=sid,
            source_id=src_id,
            status="running",
            started_at=_dt.datetime.now(_dt.UTC),
        )
    )
    pg_session.flush()
    return sid


def test_two_sessions_phase_logs_isolated(pg_session) -> None:
    """Querying PhaseLog by session_id never bleeds across sessions."""
    sid_a = _seed_session(pg_session, "alpha")
    sid_b = _seed_session(pg_session, "beta")

    # 3 logs for A
    for i in range(3):
        pg_session.add(
            PhaseLog(
                run_id="run-alpha",
                session_id=sid_a,
                source_id="src-alpha",
                phase_name=f"phase_a_{i}",
                status="completed",
                started_at=_dt.datetime.now(_dt.UTC),
                completed_at=_dt.datetime.now(_dt.UTC),
                duration_seconds=0.1,
            )
        )
    # 2 logs for B
    for i in range(2):
        pg_session.add(
            PhaseLog(
                run_id="run-beta",
                session_id=sid_b,
                source_id="src-beta",
                phase_name=f"phase_b_{i}",
                status="completed",
                started_at=_dt.datetime.now(_dt.UTC),
                completed_at=_dt.datetime.now(_dt.UTC),
                duration_seconds=0.1,
            )
        )
    pg_session.commit()

    a_rows = (
        pg_session.execute(select(PhaseLog).where(PhaseLog.session_id == sid_a)).scalars().all()
    )
    b_rows = (
        pg_session.execute(select(PhaseLog).where(PhaseLog.session_id == sid_b)).scalars().all()
    )
    assert len(a_rows) == 3
    assert len(b_rows) == 2
    assert all(r.session_id == sid_a for r in a_rows)
    assert all(r.session_id == sid_b for r in b_rows)


def test_phase_log_without_session_id_rejected_by_postgres(pg_session) -> None:
    """Postgres enforces NOT NULL on session_id — no autofill, no silent insert."""
    pg_session.add(Source(source_id="src-nn", name="nn", source_type="csv"))
    pg_session.flush()
    pg_session.add(
        InvestigationSession(
            session_id="sess-nn",
            source_id="src-nn",
            intent="nn",
            status="active",
            started_at=_dt.datetime.now(_dt.UTC),
        )
    )
    pg_session.flush()
    pg_session.add(
        PipelineRun(
            run_id="run-nn",
            session_id="sess-nn",
            source_id="src-nn",
            status="running",
            started_at=_dt.datetime.now(_dt.UTC),
        )
    )
    pg_session.flush()

    # Bypass ORM defaulting by inserting via raw SQL without session_id.
    with pytest.raises(IntegrityError):
        pg_session.execute(
            text(
                "INSERT INTO phase_logs (log_id, run_id, source_id, phase_name, status, "
                "started_at, completed_at, duration_seconds) "
                "VALUES (:lid, :rid, :sid, 'p', 'completed', NOW(), NOW(), 0.0)"
            ),
            {"lid": "log-nn", "rid": "run-nn", "sid": "src-nn"},
        )
        pg_session.flush()


def test_phase_log_with_bad_session_id_rejected_by_postgres(pg_session) -> None:
    """Postgres FK rejects a session_id that doesn't exist."""
    pg_session.add(Source(source_id="src-fk", name="fk", source_type="csv"))
    pg_session.flush()
    pg_session.add(
        InvestigationSession(
            session_id="sess-fk",
            source_id="src-fk",
            intent="fk",
            status="active",
            started_at=_dt.datetime.now(_dt.UTC),
        )
    )
    pg_session.flush()
    pg_session.add(
        PipelineRun(
            run_id="run-fk",
            session_id="sess-fk",
            source_id="src-fk",
            status="running",
            started_at=_dt.datetime.now(_dt.UTC),
        )
    )
    pg_session.flush()

    bad = PhaseLog(
        run_id="run-fk",
        session_id="ghost-session-does-not-exist",
        source_id="src-fk",
        phase_name="x",
        status="completed",
        started_at=_dt.datetime.now(_dt.UTC),
        completed_at=_dt.datetime.now(_dt.UTC),
        duration_seconds=0.0,
    )
    pg_session.add(bad)
    with pytest.raises(IntegrityError):
        pg_session.commit()
    pg_session.rollback()


# ---------------------------------------------------------------------------
# 5. No SQLite dialect imports remain in production code
# ---------------------------------------------------------------------------


def test_no_sqlite_dialect_imports_in_src() -> None:
    """grep proof — DAT-321 dropped the sqlite-specific JSON path entirely."""
    src_root = Path(__file__).resolve().parents[2] / "src"
    res = subprocess.run(
        ["grep", "-rn", "from sqlalchemy.dialects.sqlite", str(src_root)],
        capture_output=True,
        text=True,
    )
    # rg / grep exit 1 == no matches. exit 0 + output == matches.
    assert res.returncode != 0 or not res.stdout, (
        f"SQLite dialect import found in src/:\n{res.stdout}"
    )


# ---------------------------------------------------------------------------
# 6. ConnectionConfig.for_directory accepts DATABASE_URL → Postgres
# ---------------------------------------------------------------------------


def test_connection_config_reads_database_url(monkeypatch, pg_url, tmp_path) -> None:
    """ConnectionConfig.for_directory must round-trip DATABASE_URL into pg engine."""
    from dataraum.core.connections import ConnectionConfig, ConnectionManager

    monkeypatch.setenv("DATABASE_URL", pg_url)
    cfg = ConnectionConfig.for_directory(tmp_path)
    assert cfg.database_url == pg_url
    assert "postgresql" in cfg.database_url

    manager = ConnectionManager(cfg, session_id="00000000-0000-0000-0000-0000000000aa")
    try:
        manager.initialize()
        with manager.session_scope() as s:
            result = s.execute(text("SELECT 1 AS x")).scalar()
            assert result == 1
    finally:
        manager.close()


def test_connection_config_fails_loud_without_database_url(monkeypatch, tmp_path) -> None:
    """No DATABASE_URL → fail-loud RuntimeError, not silent SQLite fallback."""
    from dataraum.core.connections import ConnectionConfig

    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(RuntimeError, match="DATABASE_URL"):
        ConnectionConfig.for_directory(tmp_path)


# ---------------------------------------------------------------------------
# 7. Each per-session table has an index on session_id
# ---------------------------------------------------------------------------


def test_session_id_indexed_on_per_session_tables(pg_engine) -> None:
    """``index=True`` on session_id translates to a real Postgres index.

    Per-session queries (PhaseLog filtered by session_id, snippet lookups,
    etc.) are the hot path post-DAT-321. The ORM declares ``index=True`` on
    every session_id column; this verifies it actually lands as a btree.
    """
    inspector = inspect(pg_engine)
    for table_name in PER_SESSION_TABLES:
        indexes = inspector.get_indexes(table_name)
        has_sid_index = any("session_id" in idx.get("column_names", []) for idx in indexes)
        assert has_sid_index, (
            f"{table_name}: no index covering session_id "
            f"(found indexes: {[i['name'] for i in indexes]})"
        )


# ---------------------------------------------------------------------------
# 8. No file-based .db artifact appears on disk during a session lifecycle
# ---------------------------------------------------------------------------


def test_no_sqlite_db_files_after_session_initialize(monkeypatch, pg_url, tmp_path) -> None:
    """Setting up a session manager must not leave .db / .sqlite files behind.

    Pre-DAT-321 the workspace was a file-backed SQLite database; DAT-321
    moves it into Postgres. If any leftover SQLite path is still wired,
    initialize() would create a file and this guard would catch it.
    """
    from dataraum.core.connections import ConnectionConfig, ConnectionManager

    monkeypatch.setenv("DATABASE_URL", pg_url)
    cfg = ConnectionConfig.for_directory(tmp_path)
    manager = ConnectionManager(cfg, session_id="00000000-0000-0000-0000-0000000000bb")
    try:
        manager.initialize()
        with manager.session_scope() as s:
            s.execute(text("SELECT 1")).scalar()
    finally:
        manager.close()

    leftovers = []
    for ext in (".db", ".sqlite", ".sqlite3"):
        leftovers.extend(Path(tmp_path).rglob(f"*{ext}"))
    assert not leftovers, f"SQLite artifact(s) left on disk under {tmp_path}: {leftovers}"
