"""Unit-grain composition and its additivity gate (DAT-671 B1).

Two things are proven here, both against in-memory DuckDB rather than by string
comparison: that the composed unit-grain SQL computes the right per-entity numbers,
and that the parts sum back to the workspace scalar exactly when the served verdict
says they should. The gate itself is enumerated over every verdict state, because a
state that falls through a gate becomes a silent offer.
"""

from __future__ import annotations

import duckdb
import pytest

from dataraum.graphs.additivity import (
    AbstainReason,
    AdditivityStatus,
    AxisAdditivity,
    AxisVerdict,
)
from dataraum.graphs.formula_composer import compose_extract_sql, compose_formula_sql
from dataraum.graphs.unit_grain import UnitGrainDecision, gate_unit_grain

# --- the AP fixture: a periodic-snapshot relation with an entity that drops out ---
# acct_c stops reporting in June, so it has no row at the bound fiscal instant.
_AP_ROWS = """
    ('acct_a', DATE '2024-06-01', 100.0), ('acct_a', DATE '2024-12-01', 150.0),
    ('acct_a', DATE '2025-02-01', 999.0),
    ('acct_b', DATE '2024-06-01',  40.0), ('acct_b', DATE '2024-12-01',  60.0),
    ('acct_b', DATE '2025-02-01', 888.0),
    ('acct_c', DATE '2024-06-01',  25.0)
"""
_BOUND = ["\"period\" = TIMESTAMP '2024-12-01 00:00:00'"]


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect()
    c.execute(
        f"CREATE TABLE ap AS SELECT * FROM (VALUES {_AP_ROWS}) t(account_id, period, balance)"
    )
    c.execute(
        "CREATE TABLE purchases AS SELECT * FROM (VALUES"
        " ('acct_a', 1200.0), ('acct_b', 400.0), ('acct_d', 700.0)) t(account_id, cogs)"
    )
    return c


class TestGroupedExtract:
    def test_renders_key_projection_and_group_by(self) -> None:
        sql = compose_extract_sql("SUM(balance)", "ap", [], ["account_id"])
        assert sql == (
            'SELECT "account_id" AS "account_id", SUM(balance) AS value\n'
            "FROM ap\n"
            'GROUP BY "account_id"'
        )

    def test_scalar_render_is_unchanged_when_no_grain(self) -> None:
        assert compose_extract_sql("SUM(x)", "t", ["a = 1"]) == (
            "SELECT SUM(x) AS value\nFROM t\nWHERE a = 1"
        )

    def test_quotes_a_key_containing_a_quote(self) -> None:
        assert '"we""ird"' in compose_extract_sql("SUM(x)", "t", [], ['we"ird'])

    def test_parts_reconcile_to_the_scalar_under_a_bound_instant(
        self, con: duckdb.DuckDBPyConnection
    ) -> None:
        """The crossing case: per-entity grain x a bound reporting instant.

        The binding is resolved ONCE for the relation, so the grouped rows are a
        partition of the very rows the scalar aggregates — which is what makes the
        served `additive` categorical verdict for a summed stock true.
        """
        scalar = con.execute(compose_extract_sql("SUM(balance)", "ap", _BOUND)).fetchone()
        grouped = compose_extract_sql("SUM(balance)", "ap", _BOUND, ["account_id"])
        parts = con.execute(f"SELECT SUM(value) FROM ({grouped})").fetchone()
        assert scalar is not None and parts is not None
        assert parts[0] == scalar[0] == 210.0

    def test_an_entity_absent_at_the_bound_instant_is_absent_not_zero(
        self, con: duckdb.DuckDBPyConnection
    ) -> None:
        """acct_c has no row at the close; the data records no level for it.

        A zero here would assert a measurement nobody made — and would still sum to
        the right total, so nothing downstream could catch it.
        """
        grouped = compose_extract_sql("SUM(balance)", "ap", _BOUND, ["account_id"])
        rows = con.execute(f"{grouped} ORDER BY 1").fetchall()
        assert [r[0] for r in rows] == ["acct_a", "acct_b"]

    def test_a_per_entity_binding_would_not_reconcile(self, con: duckdb.DuckDBPyConnection) -> None:
        """The rejected alternative, pinned so nobody re-introduces it.

        Binding each entity to its own latest period picks up acct_c's JUNE level
        beside the others' December ones: the column mixes instants and no longer
        sums to the total, which is exactly what the `stock` time verdict forbids.
        """
        per_entity = con.execute(
            "SELECT SUM(value) FROM (SELECT balance AS value, ROW_NUMBER() OVER "
            "(PARTITION BY account_id ORDER BY period DESC) rn FROM ap "
            "WHERE period < TIMESTAMP '2025-01-01') WHERE rn = 1"
        ).fetchone()
        assert per_entity is not None
        assert per_entity[0] == 235.0  # != the 210.0 scalar


class TestGroupedFormula:
    def _dpo(self) -> str:
        return compose_formula_sql(
            "accounts_payable / cost_of_goods_sold * days_in_period",
            {"accounts_payable", "cost_of_goods_sold", "days_in_period"},
            group_by=["account_id"],
            grouped_steps=frozenset({"accounts_payable", "cost_of_goods_sold"}),
        )

    def _run(self, con: duckdb.DuckDBPyConnection) -> list[tuple[str, float | None]]:
        ap = compose_extract_sql("SUM(balance)", "ap", _BOUND, ["account_id"])
        cogs = compose_extract_sql("SUM(cogs)", "purchases", [], ["account_id"])
        return con.execute(
            f"WITH accounts_payable AS ({ap}), cost_of_goods_sold AS ({cogs}),"
            f" days_in_period AS (SELECT 365 AS value)"
            f" SELECT * FROM ({self._dpo()}) ORDER BY 1"
        ).fetchall()

    def test_computes_the_ratio_per_entity(self, con: duckdb.DuckDBPyConnection) -> None:
        rows = dict(self._run(con))
        assert rows["acct_a"] == pytest.approx(150.0 / 1200.0 * 365)
        assert rows["acct_b"] == pytest.approx(60.0 / 400.0 * 365)

    def test_a_constant_stays_a_scalar_subquery(self) -> None:
        """An entity-independent operand has no key to join on."""
        assert "(SELECT value FROM days_in_period)" in self._dpo()
        assert "days_in_period.value" not in self._dpo()

    def test_an_entity_missing_from_one_carrier_is_kept_with_a_null_value(
        self, con: duckdb.DuckDBPyConnection
    ) -> None:
        """Full outer, so neither side silently drops an entity.

        acct_c has a payable but no purchases; acct_d the reverse. Both belong in
        the breakdown, and both are NULL — "not computable for this entity" — never
        a fabricated number. (acct_c is absent for the separate reason that it has
        no row at the bound instant.)
        """
        rows = dict(self._run(con))
        assert rows["acct_d"] is None
        assert set(rows) == {"acct_a", "acct_b", "acct_d"}

    def test_refuses_a_grain_no_dependency_carries(self) -> None:
        with pytest.raises(ValueError, match="none of its dependencies"):
            compose_formula_sql(
                "a * b", {"a", "b"}, group_by=["account_id"], grouped_steps=frozenset()
            )

    def test_scalar_formula_render_is_unchanged(self) -> None:
        assert compose_formula_sql("revenue - cogs", {"revenue", "cogs"}) == (
            "SELECT ((SELECT value FROM revenue) - (SELECT value FROM cogs)) AS value"
        )


def _verdict(
    *,
    status: AdditivityStatus = AdditivityStatus.CLASSIFIED,
    verdict: AxisVerdict | None = AxisVerdict.ADDITIVE,
    reason: str | None = None,
    abstain: AbstainReason | None = None,
) -> AxisAdditivity:
    """A served verdict as `read_categorical_verdict` reconstructs it from its row."""
    return AxisAdditivity(status=status, verdict=verdict, reason=reason, abstain_reason=abstain)


class TestGate:
    """Every served state answered explicitly — a fall-through would be a silent offer."""

    def test_additive_offers_and_reconciles(self) -> None:
        d = gate_unit_grain("account_id", _verdict())
        assert (d.offered, d.reconciles, d.recompute) == (True, True, False)

    def test_no_verdict_row_is_not_permission(self) -> None:
        d = gate_unit_grain("account_id", None)
        assert not d.offered
        assert "unknown" in (d.reason or "")

    def test_abstention_withholds_naming_its_reason(self) -> None:
        d = gate_unit_grain(
            "account_id",
            _verdict(
                status=AdditivityStatus.ABSTAINED,
                verdict=None,
                abstain=AbstainReason.MISSING_EXTRACT,
            ),
        )
        assert not d.offered
        assert "missing_extract" in (d.reason or "")

    def test_recompute_offers_when_every_carrier_is_additive(self) -> None:
        d = gate_unit_grain(
            "account_id",
            _verdict(verdict=AxisVerdict.NON_ADDITIVE_RECOMPUTE, reason="ratio"),
            {"accounts_payable": _verdict(), "cost_of_goods_sold": _verdict()},
        )
        assert (d.offered, d.reconciles, d.recompute) == (True, False, True)

    def test_recompute_withholds_when_a_carrier_does_not_partition(self) -> None:
        d = gate_unit_grain(
            "account_id",
            _verdict(verdict=AxisVerdict.NON_ADDITIVE_RECOMPUTE, reason="ratio"),
            {
                "accounts_payable": _verdict(),
                "headcount": _verdict(
                    verdict=AxisVerdict.NON_ADDITIVE_RECOMPUTE, reason="distinct_count"
                ),
            },
        )
        assert not d.offered
        assert "headcount" in (d.reason or "")

    def test_recompute_withholds_when_a_carrier_is_unjudged(self) -> None:
        d = gate_unit_grain(
            "account_id",
            _verdict(verdict=AxisVerdict.NON_ADDITIVE_RECOMPUTE, reason="ratio"),
            {"accounts_payable": _verdict(), "mystery": None},
        )
        assert not d.offered
        assert "mystery" in (d.reason or "")

    def test_semi_additive_offers_without_reconciling(self) -> None:
        d = gate_unit_grain(
            "account_id", _verdict(verdict=AxisVerdict.SEMI_ADDITIVE, reason="stock")
        )
        assert (d.offered, d.reconciles) == (True, False)

    def test_a_withheld_decision_must_name_why(self) -> None:
        with pytest.raises(ValueError, match="must name why"):
            UnitGrainDecision(axis="account_id", offered=False)
