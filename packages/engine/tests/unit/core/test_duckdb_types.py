"""The shared type-family predicates (DAT-835).

The cases that matter are the ones the old hardcoded lists got wrong, so they are
asserted explicitly rather than as a blanket round-trip: a regression here is a
column silently vanishing from statistics or temporal profiling, with no error.
"""

from __future__ import annotations

import duckdb
import pytest

from dataraum.core.duckdb_types import (
    DATETIME_LIKE_TYPES,
    NUMERIC_TYPES,
    TIME_POINT_TYPES,
    family,
    is_datetime_like,
    is_numeric,
    is_time_point,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("DECIMAL(18,2)", "DECIMAL"),
        ("decimal(18,2)", "DECIMAL"),
        ("  BIGINT  ", "BIGINT"),
        ("TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH TIME ZONE"),
        ("STRUCT(a INTEGER)", "STRUCT"),  # a struct is not its interior
        (None, ""),
        ("", ""),
    ],
)
def test_family_strips_parameters_and_folds_case(raw: str | None, expected: str) -> None:
    assert family(raw) == expected


@pytest.mark.parametrize(
    "resolved_type",
    [
        # The four the old lists knew about.
        "INTEGER",
        "BIGINT",
        "DOUBLE",
        "DECIMAL",
        # The ones they silently dropped — each is a real parquet column type.
        "FLOAT",
        "DECIMAL(18,2)",
        "DECIMAL(38,10)",
        "TINYINT",
        "SMALLINT",
        "HUGEINT",
        "UBIGINT",
        "UINTEGER",
        "REAL",
    ],
)
def test_is_numeric_accepts_every_summable_type(resolved_type: str) -> None:
    assert is_numeric(resolved_type)


@pytest.mark.parametrize(
    "resolved_type",
    ["VARCHAR", "BOOLEAN", "DATE", "TIMESTAMP", "BLOB", "JSON", "STRUCT(a INTEGER)", None, ""],
)
def test_is_numeric_rejects_non_numerics(resolved_type: str | None) -> None:
    assert not is_numeric(resolved_type)


@pytest.mark.parametrize(
    "resolved_type",
    [
        "DATE",
        "TIMESTAMP",
        # pandas writes nanosecond timestamps by default, so this is the common
        # case for any parquet the engine did not produce itself.
        "TIMESTAMP_NS",
        "TIMESTAMP_MS",
        "TIMESTAMP_S",
        "TIMESTAMPTZ",
        # What DuckDB's DESCRIBE actually returns for a tz-aware column — the
        # short spelling alone never matched one.
        "TIMESTAMP WITH TIME ZONE",
    ],
)
def test_is_time_point_accepts_every_timestamp_spelling(resolved_type: str) -> None:
    assert is_time_point(resolved_type)


@pytest.mark.parametrize("resolved_type", ["TIME", "INTERVAL", "TIME WITH TIME ZONE"])
def test_durations_are_datetime_like_but_not_time_points(resolved_type: str) -> None:
    """A duration has no position in time, so it bounds no window."""
    assert is_datetime_like(resolved_type)
    assert not is_time_point(resolved_type)


@pytest.mark.parametrize("resolved_type", ["VARCHAR", "BIGINT", "DECIMAL(18,2)", None, ""])
def test_is_time_point_rejects_non_temporal(resolved_type: str | None) -> None:
    assert not is_time_point(resolved_type)
    assert not is_datetime_like(resolved_type)


def test_time_points_are_a_subset_of_datetime_like() -> None:
    assert TIME_POINT_TYPES < DATETIME_LIKE_TYPES


def test_numeric_and_temporal_families_are_disjoint() -> None:
    assert not NUMERIC_TYPES & DATETIME_LIKE_TYPES


# --- The predicates vs DuckDB itself ------------------------------------------
#
# The sets above are only correct if they match the names DuckDB's DESCRIBE
# actually emits — which is where the original bug lived (the code guessed
# "TIMESTAMPTZ"; DuckDB says "TIMESTAMP WITH TIME ZONE"). So ask DuckDB.

_DUCKDB_NUMERIC = [
    "42::TINYINT",
    "42::SMALLINT",
    "42::INTEGER",
    "42::BIGINT",
    "42::UBIGINT",
    "42.5::FLOAT",
    "42.5::DOUBLE",
    "42.50::DECIMAL(18,2)",
]
_DUCKDB_TIME_POINT = [
    "DATE '2026-01-01'",
    "TIMESTAMP '2026-01-01'",
    "TIMESTAMPTZ '2026-01-01'",
    "TIMESTAMP_NS '2026-01-01'",
    "TIMESTAMP_MS '2026-01-01'",
    "TIMESTAMP_S '2026-01-01'",
]


def _describe(expr: str) -> str:
    conn = duckdb.connect()
    try:
        rows = conn.execute(f"DESCRIBE SELECT {expr} AS c").fetchall()
        return str(rows[0][1])
    finally:
        conn.close()


@pytest.mark.parametrize("expr", _DUCKDB_NUMERIC)
def test_duckdbs_own_numeric_names_are_recognized(expr: str) -> None:
    described = _describe(expr)
    assert is_numeric(described), f"{expr} → {described!r} not recognized as numeric"


@pytest.mark.parametrize("expr", _DUCKDB_TIME_POINT)
def test_duckdbs_own_timestamp_names_are_recognized(expr: str) -> None:
    described = _describe(expr)
    assert is_time_point(described), f"{expr} → {described!r} not recognized as a time point"
