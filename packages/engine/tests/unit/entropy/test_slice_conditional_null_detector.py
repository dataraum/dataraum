"""Slice-conditional null detector — nulls concentrated in a slice (DAT-473).

The statistic is bias-corrected Cramér's V of ``(amount IS NULL) × cost_center``: a column
5% null overall but 50% null in ONE cost center scores high; a flat (MCAR) null rate scores
≈ 0. Teach-closeable — a ``document_business_rule`` teach (``ConfigOverlay`` of type
``expected_dependency`` over the column + its slice) marks the conditional missingness
expected and the score drops. Per-column VALUE/NULLS signal, so it rolls into the column's
band beside ``null_ratio``.

Real session rows for the column metadata + a real in-memory DuckDB for the values; no
mocks — the same two input paths the engine uses at detect time (cf. test_dimensional_entropy).
"""

from __future__ import annotations

import duckdb
from sqlalchemy.orm import Session

from dataraum.entropy.detectors.base import DetectorContext
from dataraum.entropy.detectors.value.slice_conditional_null import SliceConditionalNullDetector
from dataraum.storage import Column, ConfigOverlay, Source, Table

_SOURCE_ID = "scn_src"
_TABLE_ID = "scn_tbl"
_TABLE_NAME = "journal_lines"
_CENTERS = ["A", "B", "C", "D", "E"]


def _seed_columns(session: Session) -> None:
    session.add(Source(source_id=_SOURCE_ID, name=_SOURCE_ID, source_type="csv"))
    session.flush()
    session.add(
        Table(
            table_id=_TABLE_ID,
            source_id=_SOURCE_ID,
            table_name=_TABLE_NAME,
            layer="typed",
            duckdb_path=_TABLE_NAME,
            row_count=250,
        )
    )
    session.flush()
    for col_id, name, dtype, pos in (
        ("c_amount", "amount", "DOUBLE", 0),
        ("c_center", "cost_center", "VARCHAR", 1),
        ("c_lineid", "line_id", "VARCHAR", 2),
    ):
        session.add(
            Column(
                column_id=col_id,
                table_id=_TABLE_ID,
                column_name=name,
                column_position=pos,
                raw_type="VARCHAR",
                resolved_type=dtype,
            )
        )
    session.flush()


def _typed_conn(rows: list[tuple]) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute("ATTACH ':memory:' AS lake")
    conn.execute("CREATE SCHEMA lake.typed")
    conn.execute(
        f'CREATE TABLE lake.typed."{_TABLE_NAME}" '
        '("amount" DOUBLE, "cost_center" VARCHAR, "line_id" VARCHAR)'
    )
    conn.executemany(f'INSERT INTO lake.typed."{_TABLE_NAME}" VALUES (?, ?, ?)', rows)
    return conn


def _context(session: Session, conn: duckdb.DuckDBPyConnection, null_count: int) -> DetectorContext:
    ctx = DetectorContext(
        session=session,
        source_id=_SOURCE_ID,
        table_id=_TABLE_ID,
        table_name=_TABLE_NAME,
        column_id="c_amount",
        column_name="amount",
        duckdb_conn=conn,
        run_id=None,
        base_runs={},
    )
    # statistics profile (null_count) is what load_data would populate at detect time.
    ctx.analysis_results["statistics"] = {"null_count": null_count}
    return ctx


def _rows(null_in: dict[str, float]) -> tuple[list[tuple], int]:
    """250 rows over 5 cost centers; ``null_in`` maps a center → its null fraction.

    Returns the rows and the total null count. line_id is unique (an identifier the
    detector must never treat as a slice dimension).
    """
    rows: list[tuple] = []
    nulls = 0
    for i in range(250):
        center = _CENTERS[i % 5]
        is_null = (i // 5) / 50.0 < null_in.get(center, 0.0)  # deterministic per-center fraction
        amount = None if is_null else float(100 + i)
        if is_null:
            nulls += 1
        rows.append((amount, center, f"L{i:04d}"))
    return rows, nulls


def test_concentrated_nulls_in_one_slice_score_high(session: Session) -> None:
    _seed_columns(session)
    rows, nulls = _rows({"E": 0.6})  # 60% null in center E, clean elsewhere
    conn = _typed_conn(rows)

    objects = SliceConditionalNullDetector().detect(_context(session, conn, nulls))

    assert len(objects) == 1
    obj = objects[0]
    assert obj.detector_id == "slice_conditional_null"
    assert obj.dimension_path == "value.nulls.slice_conditional_null"
    assert obj.target == "column:journal_lines.amount"
    assert obj.score > 0.3  # concentration is read, well above the MCAR floor
    top = obj.evidence[0]
    assert top["slice_column"] == "cost_center"
    assert top["slice_column_id"] == "c_center"


def test_flat_null_rate_scores_low(session: Session) -> None:
    _seed_columns(session)
    rows, nulls = _rows(dict.fromkeys(_CENTERS, 0.2))  # MCAR: 20% null in every center
    conn = _typed_conn(rows)

    obj = SliceConditionalNullDetector().detect(_context(session, conn, nulls))[0]
    assert obj.score < 0.15


def test_concentrated_orders_above_flat(session: Session) -> None:
    _seed_columns(session)
    flat_rows, flat_n = _rows(dict.fromkeys(_CENTERS, 0.2))
    conc_rows, conc_n = _rows({"E": 0.6})
    flat = SliceConditionalNullDetector().detect(_context(session, _typed_conn(flat_rows), flat_n))[
        0
    ]
    conc = SliceConditionalNullDetector().detect(_context(session, _typed_conn(conc_rows), conc_n))[
        0
    ]
    assert conc.score > flat.score


def test_no_nulls_scores_zero(session: Session) -> None:
    _seed_columns(session)
    rows, nulls = _rows({})  # no center has nulls
    assert nulls == 0
    obj = SliceConditionalNullDetector().detect(_context(session, _typed_conn(rows), nulls))[0]
    assert obj.score == 0.0
    assert obj.evidence == []


def test_document_business_rule_teach_closes_the_score(session: Session) -> None:
    _seed_columns(session)
    rows, nulls = _rows({"E": 0.6})
    conn = _typed_conn(rows)
    detector = SliceConditionalNullDetector()

    before = detector.detect(_context(session, conn, nulls))[0]
    assert before.score > 0.3

    # A teach documents that amount's missingness is expected given cost_center.
    session.add(
        ConfigOverlay(
            type="expected_dependency",
            payload={
                "column_ids": ["c_amount", "c_center"],
                "rule": "amount is intentionally blank for center E (accrual-only)",
            },
        )
    )
    session.flush()

    after = detector.detect(_context(session, conn, nulls))[0]
    assert after.score == 0.0  # the only slice dimension was documented → no entropy left
    assert after.evidence == []


def test_identifier_is_not_a_slice_dimension(session: Session) -> None:
    """line_id is unique — were it scanned as a slice every row is its own group and the
    statistic would be meaningless; the name filter must keep it out (only cost_center scores)."""
    _seed_columns(session)
    rows, nulls = _rows({"E": 0.6})
    obj = SliceConditionalNullDetector().detect(_context(session, _typed_conn(rows), nulls))[0]
    slice_cols = {ev["slice_column"] for ev in obj.evidence}
    assert slice_cols == {"cost_center"}
