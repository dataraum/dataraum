"""Tests for type-inference cast testing against real DuckDB.

Pins the TRY_-normalization behavior end-to-end: one malformed value among
clean DD.MM.YYYY dates must reduce the success rate and surface a failed
example — NOT score the pattern 0.0 via a thrown STRPTIME (the bug that
froze such columns to VARCHAR with zero diagnostics).
"""

from __future__ import annotations

import duckdb
import pytest

from dataraum.analysis.typing.inference import _test_type_cast
from dataraum.analysis.typing.patterns import Pattern
from dataraum.core.models.base import DataType


@pytest.fixture
def conn():
    c = duckdb.connect(":memory:")
    yield c
    c.close()


def _eu_date_pattern() -> Pattern:
    """The builtin eu_date pattern as loaded from YAML (pre-normalization form)."""
    return Pattern(
        name="eu_date",
        pattern=r"^\d{1,2}\.\d{1,2}\.\d{2,4}$",
        inferred_type=DataType.DATE,
        standardization_expr="STRPTIME(\"{col}\", '%d.%m.%Y')",
    )


def test_one_malformed_value_does_not_zero_the_pattern(conn: duckdb.DuckDBPyConnection) -> None:
    """29.02.2023 (no leap day) matches the regex but fails the parse.

    Old behavior: STRPTIME threw on it, the whole COUNT query errored, and the
    pattern scored success_rate=0.0 with no failed examples — VARCHAR fallback
    for a 75%-clean date column. New behavior: the bad value returns NULL,
    success_rate reflects reality, and the failed example is captured.
    """
    conn.execute(
        "CREATE TABLE t AS SELECT * FROM (VALUES "
        "('15.01.2024'), ('31.12.2023'), ('29.02.2023'), ('01.06.2026')) v(tag_datum)"
    )
    result = _test_type_cast(
        table_name="t",
        col_name="tag_datum",
        target_type=DataType.DATE,
        duckdb_conn=conn,
        standardization_expr=_eu_date_pattern().standardization_expr,
    )
    assert result.success_rate == pytest.approx(0.75)
    assert result.failed_examples == ["29.02.2023"]


def test_clean_column_parses_fully(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        "CREATE TABLE t AS SELECT * FROM (VALUES ('15.01.2024'), ('01.06.2026')) v(tag_datum)"
    )
    result = _test_type_cast(
        table_name="t",
        col_name="tag_datum",
        target_type=DataType.DATE,
        duckdb_conn=conn,
        standardization_expr=_eu_date_pattern().standardization_expr,
    )
    assert result.success_rate == 1.0
    assert result.failed_examples == []
