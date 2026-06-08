"""Dimensional entropy detector — undocumented cross-column dependency via NMI (DAT-442/472).

The measurement is ``stats.nmi`` over column pairs read from the typed table: the
double-entry mutex (``debit ≠ 0`` ⇔ ``credit = 0``) scores ≈ 1.0, independent columns
≈ 0.0. It measures INTRINSIC structure (the mutex is real in clean data too), so the bar
is teach-closure — a ``document_business_rule`` teach (``ConfigOverlay`` of type
``expected_dependency``) excludes the pair and the score drops. Identifiers and derived
columns are never candidates (their dependency is an id, or the formula ``derived_value``
already owns).

Real session rows for the column metadata + a real in-memory DuckDB for the values; no
mocks — the same two input paths the engine uses at detect time.
"""

from __future__ import annotations

import duckdb
from sqlalchemy.orm import Session

from dataraum.analysis.correlation.db_models import DerivedColumn
from dataraum.entropy.detectors.base import DetectorContext
from dataraum.entropy.detectors.semantic.dimensional_entropy import DimensionalEntropyDetector
from dataraum.storage import Column, ConfigOverlay, Source, Table

_SOURCE_ID = "dim_src"
_TABLE_ID = "dim_tbl"
_TABLE_NAME = "journal_lines"


def _make_table(session: Session) -> None:
    session.add(Source(source_id=_SOURCE_ID, name=_SOURCE_ID, source_type="csv"))
    session.flush()
    session.add(
        Table(
            table_id=_TABLE_ID,
            source_id=_SOURCE_ID,
            table_name=_TABLE_NAME,
            layer="typed",
            duckdb_path=_TABLE_NAME,
            row_count=60,
        )
    )
    session.flush()


def _add_column(
    session: Session, column_id: str, name: str, resolved_type: str, position: int
) -> None:
    session.add(
        Column(
            column_id=column_id,
            table_id=_TABLE_ID,
            column_name=name,
            column_position=position,
            raw_type="VARCHAR",
            resolved_type=resolved_type,
        )
    )
    session.flush()


def _typed_conn(columns: list[tuple[str, str]], rows: list[tuple]) -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB exposing ``lake.typed."journal_lines"`` with the planted rows."""
    conn = duckdb.connect()
    conn.execute("ATTACH ':memory:' AS lake")
    conn.execute("CREATE SCHEMA lake.typed")
    ddl = ", ".join(f'"{name}" {dtype}' for name, dtype in columns)
    conn.execute(f'CREATE TABLE lake.typed."{_TABLE_NAME}" ({ddl})')
    placeholders = ", ".join("?" for _ in columns)
    conn.executemany(f'INSERT INTO lake.typed."{_TABLE_NAME}" VALUES ({placeholders})', rows)
    return conn


def _context(session: Session, conn: duckdb.DuckDBPyConnection | None) -> DetectorContext:
    return DetectorContext(
        session=session,
        source_id=_SOURCE_ID,
        table_id=_TABLE_ID,
        table_name=_TABLE_NAME,
        duckdb_conn=conn,
        run_id=None,
        base_runs={},
    )


def _mutex_rows() -> list[tuple]:
    """debit/credit double-entry mutex; currency (period 3) + cost_center (period 5) independent."""
    currencies = ["USD", "EUR", "GBP"]
    centers = ["A", "B", "C", "D", "E"]
    rows: list[tuple] = []
    for i in range(60):
        debit, credit = (100.0, 0.0) if i % 2 == 0 else (0.0, 75.0)
        rows.append((debit, credit, currencies[i % 3], centers[i % 5]))
    return rows


_MUTEX_COLS = [
    ("debit", "DOUBLE"),
    ("credit", "DOUBLE"),
    ("currency", "VARCHAR"),
    ("cost_center", "VARCHAR"),
]


def _seed_mutex_columns(session: Session) -> None:
    _make_table(session)
    _add_column(session, "c_debit", "debit", "DOUBLE", 0)
    _add_column(session, "c_credit", "credit", "DOUBLE", 1)
    _add_column(session, "c_currency", "currency", "VARCHAR", 2)
    _add_column(session, "c_center", "cost_center", "VARCHAR", 3)


def test_scores_the_debit_credit_mutex(session: Session) -> None:
    _seed_mutex_columns(session)
    conn = _typed_conn(_MUTEX_COLS, _mutex_rows())

    objects = DimensionalEntropyDetector().detect(_context(session, conn))

    assert len(objects) == 1
    obj = objects[0]
    assert obj.detector_id == "dimensional_entropy"
    assert obj.dimension_path == "semantic.dimensional.cross_column_patterns"
    assert obj.target == "table:journal_lines"
    assert obj.score > 0.9  # perfect anti-correlation → NMI ≈ 1.0
    top = obj.evidence[0]
    assert set(top["columns"]) == {"debit", "credit"}
    assert set(top["column_ids"]) == {"c_debit", "c_credit"}


def test_independent_columns_score_low(session: Session) -> None:
    _make_table(session)
    _add_column(session, "c_currency", "currency", "VARCHAR", 0)
    _add_column(session, "c_center", "cost_center", "VARCHAR", 1)
    _add_column(session, "c_region", "region", "VARCHAR", 2)
    currencies = ["USD", "EUR", "GBP"]
    centers = ["A", "B", "C", "D", "E"]
    regions = ["N", "S", "E", "W", "C", "X", "Y"]
    # coprime periods 3/5/7 over a full LCM (105) ⇒ every pair is exactly independent.
    rows = [(currencies[i % 3], centers[i % 5], regions[i % 7]) for i in range(105)]
    conn = _typed_conn(
        [("currency", "VARCHAR"), ("cost_center", "VARCHAR"), ("region", "VARCHAR")], rows
    )

    objects = DimensionalEntropyDetector().detect(_context(session, conn))

    assert len(objects) == 1
    assert objects[0].score < 0.3


def test_document_business_rule_teach_closes_the_score(session: Session) -> None:
    _seed_mutex_columns(session)
    conn = _typed_conn(_MUTEX_COLS, _mutex_rows())
    detector = DimensionalEntropyDetector()

    before = detector.detect(_context(session, conn))[0]
    assert before.score > 0.9

    # A document_business_rule teach marks the pair EXPECTED structure.
    session.add(
        ConfigOverlay(
            type="expected_dependency",
            payload={"column_ids": ["c_debit", "c_credit"], "rule": "double-entry mutex"},
        )
    )
    session.flush()

    after = detector.detect(_context(session, conn))[0]
    assert after.score < 0.3  # the only strong pair was documented → drops to independence
    assert all(set(ev["column_ids"]) != {"c_debit", "c_credit"} for ev in after.evidence)


def test_identifier_columns_are_not_candidates(session: Session) -> None:
    _make_table(session)
    _add_column(session, "c_debit", "debit", "DOUBLE", 0)
    _add_column(session, "c_credit", "credit", "DOUBLE", 1)
    _add_column(session, "c_lineid", "line_id", "VARCHAR", 2)  # excluded by *_id name
    # line_id is perfectly determined by the debit indicator → NMI 1.0 if it were a candidate.
    rows: list[tuple] = []
    for i in range(60):
        if i % 2 == 0:
            rows.append((100.0, 0.0, "POS"))
        else:
            rows.append((0.0, 75.0, "NEG"))
    conn = _typed_conn([("debit", "DOUBLE"), ("credit", "DOUBLE"), ("line_id", "VARCHAR")], rows)

    obj = DimensionalEntropyDetector().detect(_context(session, conn))[0]

    scored_columns = {name for ev in obj.evidence for name in ev["columns"]}
    assert "line_id" not in scored_columns  # the would-be 1.0 pair was excluded
    assert obj.score > 0.9  # debit/credit still detected


def test_derived_columns_are_not_candidates(session: Session) -> None:
    _make_table(session)
    _add_column(session, "c_debit", "debit", "DOUBLE", 0)
    _add_column(session, "c_credit", "credit", "DOUBLE", 1)
    _add_column(session, "c_net", "net_amount", "DOUBLE", 2)  # derived from debit/credit
    session.add(
        DerivedColumn(
            run_id=None,
            table_id=_TABLE_ID,
            derived_column_id="c_net",
            source_column_ids=["c_debit", "c_credit"],
            derivation_type="difference",
            formula="net_amount = debit - credit",
            match_rate=1.0,
            total_rows=60,
            matching_rows=60,
        )
    )
    session.flush()
    # net_amount's zero pattern mirrors credit → NMI 1.0 with credit if it were a candidate.
    rows: list[tuple] = []
    for i in range(60):
        if i % 2 == 0:
            rows.append((100.0, 0.0, 0.0))
        else:
            rows.append((0.0, 75.0, 75.0))
    conn = _typed_conn([("debit", "DOUBLE"), ("credit", "DOUBLE"), ("net_amount", "DOUBLE")], rows)

    obj = DimensionalEntropyDetector().detect(_context(session, conn))[0]

    scored_columns = {name for ev in obj.evidence for name in ev["columns"]}
    assert "net_amount" not in scored_columns  # derived ⇒ its dependency is the formula
    assert obj.score > 0.9  # debit/credit still detected


def test_no_candidate_pairs_returns_nothing(session: Session) -> None:
    """Fewer than two eligible columns ⇒ no measurement (not a fabricated 0)."""
    _make_table(session)
    _add_column(session, "c_debit", "debit", "DOUBLE", 0)
    _add_column(session, "c_entry", "entry_id", "VARCHAR", 1)  # excluded by name
    rows = [(100.0 if i % 2 else 0.0, str(i)) for i in range(20)]
    conn = _typed_conn([("debit", "DOUBLE"), ("entry_id", "VARCHAR")], rows)

    assert DimensionalEntropyDetector().detect(_context(session, conn)) == []


def test_missing_duckdb_connection_skips(session: Session) -> None:
    _seed_mutex_columns(session)
    context = _context(session, conn=None)

    assert DimensionalEntropyDetector().detect(context) == []
