"""DAT-536 Phase 1 — equivalence gate for the witness re-point.

The ``structural_reconciliation`` substrate — per ``(dim value, period)`` row
count + numeric-column sums — is claimed PATH-INDEPENDENT: the current
per-value slice-table path (``compute_period_sums`` over a ``… WHERE dim =
value`` table) and a single ``GROUP BY dim, period`` over the same source must
produce byte-identical sums.

This proves the claim on the CURRENT code — ``compute_period_sums`` is the
oracle — BEFORE any inline producer exists. Green ⇒ the re-point (replacing the
slice→TemporalSliceAnalysis substrate with inline aggregation, DAT-536) is
mechanical; red ⇒ the substrate is not path-independent and the re-point must
stop. The ``_inline_group_by`` SQL here is the reference the Phase-2 production
producer lifts into ``analysis/lineage``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dataraum.analysis.temporal_slicing.analyzer import compute_period_sums
from dataraum.analysis.temporal_slicing.models import TimeGrain

if TYPE_CHECKING:
    import duckdb

_VALUES = ("assets", "liabilities")
_UNDECLARED = "equity"  # present in the data, NOT a declared slice value
_MONTHS = list(range(1, 13))
# Fact-own numerics the slice path sums; ``dim_attr_num`` stands in for a joined
# dimension-attribute numeric the slicing-view projection would also carry — the
# inline path must sum exactly the same set.
_NUMERIC_COLS = ["balance", "net_change", "dim_attr_num"]

_Cells = dict[tuple[str, str], tuple[int, dict[str, float]]]


def _net(k: int, i: int) -> float:
    """Per-period movement for entity k in period i (the DAT-459 shape)."""
    return 40.0 + (i + 1) * (1 + k)


def _seed_enriched_view(conn: duckdb.DuckDBPyConnection) -> None:
    """An enriched-view-shaped table: dim + time + fact numerics.

    Two declared values (a cumulative ``balance`` stock + a per-period
    ``net_change`` flow), plus an undeclared value and a NULL-period row that
    both paths must drop.
    """
    conn.execute(
        "CREATE OR REPLACE TABLE enriched_tb ("
        " acct VARCHAR, period_date DATE,"
        " balance DOUBLE, net_change DOUBLE, dim_attr_num DOUBLE)"
    )
    rows: list[str] = []
    for k, value in enumerate(_VALUES, start=1):
        running = 0.0
        for i, month in enumerate(_MONTHS):
            net = _net(k, i)
            running += net
            rows.append(f"('{value}', DATE '2025-{month:02d}-15', {running}, {net}, {k * 100.0})")
    rows.append(f"('{_UNDECLARED}', DATE '2025-01-15', 1.0, 1.0, 9.0)")
    rows.append(f"('{_VALUES[0]}', NULL, 999.0, 999.0, 999.0)")
    conn.execute(f"INSERT INTO enriched_tb VALUES {', '.join(rows)}")


def _golden_via_slice_path(conn: duckdb.DuckDBPyConnection) -> _Cells:
    """Oracle: the CURRENT path — one slice table per declared value, summed by
    ``compute_period_sums`` (GROUP BY period over the value-filtered table)."""
    out: _Cells = {}
    for value in _VALUES:
        name = f"slice_enriched_tb_acct_{value}"
        conn.execute(
            f"CREATE OR REPLACE VIEW \"{name}\" AS SELECT * FROM enriched_tb WHERE acct = '{value}'"
        )
        result = compute_period_sums(name, "period_date", TimeGrain.MONTHLY, conn)
        assert result.success, result.error
        for p in result.value or []:
            out[(value, p.period_label)] = (p.row_count, p.column_sums)
    return out


def _inline_group_by(conn: duckdb.DuckDBPyConnection) -> _Cells:
    """The re-point: ONE ``GROUP BY dim, period`` over the enriched view, keyed
    to the declared value set, summing the same numeric columns."""
    sum_parts = "".join(f', SUM("{c}") AS s{i}' for i, c in enumerate(_NUMERIC_COLS))
    values_sql = ", ".join(f"'{v}'" for v in _VALUES)
    sql = f"""
        SELECT "acct" AS dim_value,
            CAST(date_trunc('month', CAST("period_date" AS DATE)) AS DATE) AS period_start,
            COUNT(*) AS row_count
            {sum_parts}
        FROM enriched_tb
        WHERE "period_date" IS NOT NULL AND "acct" IN ({values_sql})
        GROUP BY 1, 2
        ORDER BY 1, 2
    """
    out: _Cells = {}
    for row in conn.execute(sql).fetchall():
        label = row[1].strftime("%Y-%m")
        sums = {
            col: float(row[3 + i]) for i, col in enumerate(_NUMERIC_COLS) if row[3 + i] is not None
        }
        out[(row[0], label)] = (int(row[2]), sums)
    return out


class TestInlineAggregationEquivalence:
    def test_inline_group_by_matches_slice_path(
        self, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_enriched_view(duckdb_conn)
        golden = _golden_via_slice_path(duckdb_conn)
        inline = _inline_group_by(duckdb_conn)

        assert golden, "oracle produced no cells — fixture is wrong, not a pass"
        assert set(inline) == set(golden), (
            "cell set differs (declared-value keying or period bucketing drift)"
        )
        for key, (g_rows, g_sums) in golden.items():
            i_rows, i_sums = inline[key]
            assert i_rows == g_rows, f"row_count differs at {key}: {i_rows} != {g_rows}"
            assert i_sums.keys() == g_sums.keys(), f"column set differs at {key}"
            for col, g_val in g_sums.items():
                assert i_sums[col] == g_val, f"sum differs at {key}.{col}: {i_sums[col]} != {g_val}"

    def test_undeclared_value_and_null_period_excluded(
        self, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_enriched_view(duckdb_conn)
        inline = _inline_group_by(duckdb_conn)
        assert not any(v == _UNDECLARED for (v, _) in inline), "undeclared value leaked"
        # 2 declared values × 12 months; the NULL-period row is dropped.
        assert len(inline) == 2 * len(_MONTHS)
