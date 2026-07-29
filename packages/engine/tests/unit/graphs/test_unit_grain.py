"""Unit-grain composition and its additivity gate (DAT-671 B1).

Two things are proven here, both against in-memory DuckDB rather than by string
comparison: that the composed unit-grain SQL computes the right per-entity numbers,
and that the parts sum back to the workspace scalar exactly when the served verdict
says they should. The gate itself is enumerated over every verdict state, because a
state that falls through a gate becomes a silent offer.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import duckdb
import pytest

from dataraum.graphs.additivity import (
    AbstainReason,
    AdditivityStatus,
    AxisAdditivity,
    AxisVerdict,
)
from dataraum.graphs.agent import ExecutionContext, GraphAgent
from dataraum.graphs.formula_composer import (
    compose_extract_sql,
    compose_formula_sql,
    same_name_keys,
)
from dataraum.graphs.models import (
    GraphMetadata,
    GraphSource,
    GraphStep,
    OutputDef,
    OutputType,
    StepSource,
    StepType,
    TransformationGraph,
)
from dataraum.graphs.unit_grain import UnitGrainDecision, gate_unit_grain
from dataraum.query.snippet_models import SQLSnippetRecord

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

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
        sql = compose_extract_sql("SUM(balance)", "ap", [], same_name_keys("account_id"))
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
        assert '"we""ird"' in compose_extract_sql("SUM(x)", "t", [], same_name_keys('we"ird'))

    def test_parts_reconcile_to_the_scalar_under_a_bound_instant(
        self, con: duckdb.DuckDBPyConnection
    ) -> None:
        """The crossing case: per-entity grain x a bound reporting instant.

        The binding is resolved ONCE for the relation, so the grouped rows are a
        partition of the very rows the scalar aggregates — which is what makes the
        served `additive` categorical verdict for a summed stock true.
        """
        scalar = con.execute(compose_extract_sql("SUM(balance)", "ap", _BOUND)).fetchone()
        grouped = compose_extract_sql("SUM(balance)", "ap", _BOUND, same_name_keys("account_id"))
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
        grouped = compose_extract_sql("SUM(balance)", "ap", _BOUND, same_name_keys("account_id"))
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
        ap = compose_extract_sql("SUM(balance)", "ap", _BOUND, same_name_keys("account_id"))
        cogs = compose_extract_sql("SUM(cogs)", "purchases", [], same_name_keys("account_id"))
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


_WORKSPACE = "ws-unit-grain"


def _ap_graph() -> TransformationGraph:
    """A one-extract metric — the AP balance, its extract also the output step."""
    return TransformationGraph(
        graph_id="ap_total",
        version="1.0",
        metadata=GraphMetadata(
            name="ap_total", description="", category="liquidity", source=GraphSource.SYSTEM
        ),
        output=OutputDef(output_type=OutputType.SCALAR),
        steps={
            "accounts_payable": GraphStep(
                step_id="accounts_payable",
                step_type=StepType.EXTRACT,
                source=StepSource(
                    standard_field="accounts_payable", statement="balance_sheet", predicate=""
                ),
                aggregation="sum",
                output_step=True,
            )
        },
    )


def _warm_snippet(session: Session) -> SQLSnippetRecord:
    """The extract as the warm pass leaves it: parts, plus their SCALAR render.

    ``sql`` is deliberately the one-row render. A unit-grain composition that
    reached for it instead of re-rendering the parts would produce a single
    column named ``value`` and no ``account_id`` at all — which is exactly what
    the assertions below would catch.
    """
    row = SQLSnippetRecord(
        workspace_id=_WORKSPACE,
        snippet_type="extract",
        standard_field="accounts_payable",
        statement="balance_sheet",
        aggregation="sum",
        predicate="",
        schema_mapping_id=_WORKSPACE,
        sql=compose_extract_sql("SUM(balance)", "ap", _BOUND),
        description="accounts payable at the fiscal close",
        source="graph:ap_total",
        parts={
            "select": [{"expr": "SUM(balance)", "alias": "value"}],
            "from": ["ap"],
            "where": _BOUND,
        },
    )
    session.add(row)
    session.flush()
    return row


class TestComposeUnitGrain:
    """The phase-facing seam: compose one metric at grain, no LLM, no state threading."""

    def _agent(self) -> tuple[GraphAgent, list[Any]]:
        agent = GraphAgent(config=MagicMock(), provider=MagicMock(), prompt_renderer=MagicMock())
        authored: list[Any] = []

        def _never(*args: Any, **kwargs: Any) -> Any:
            authored.append(args)
            raise AssertionError("unit-grain composition must never author anything")

        agent._generate_sql = _never  # type: ignore[method-assign]
        return agent, authored

    def test_compose_unit_grain_reuses_the_binding_map(
        self, session: Session, con: duckdb.DuckDBPyConnection
    ) -> None:
        """It re-renders what the authoring pass already decided — nothing new.

        The binding map's outcome IS the warm snippet: its parts, its relation, its
        predicates. Composing at grain re-renders those, so the per-entity numbers
        are a partition of the very scalar the metric executed, and no concept can
        ground differently just because someone asked for a breakdown. The LLM
        boundary is wired to explode: reaching it at all is the failure.
        """
        _warm_snippet(session)
        agent, authored = self._agent()

        composed = agent.compose_unit_grain(
            session,
            _ap_graph(),
            ExecutionContext(duckdb_conn=con, schema_mapping_id=_WORKSPACE),
            axis="account_id",
            workspace_id=_WORKSPACE,
        )

        assert composed.success, composed.error
        assert authored == []
        assert composed.value is not None
        assert composed.value.truncated_at is None
        rows = {r.entity_value: r.value for r in composed.value.rows}
        # The same 210.0 the scalar reports, partitioned — and acct_c, which has no
        # row at the bound instant, is absent rather than zero.
        assert rows == {"acct_a": Decimal("150.0"), "acct_b": Decimal("60.0")}

    def test_an_ungrounded_metric_refuses_rather_than_falling_back_to_the_scalar(
        self, session: Session, con: duckdb.DuckDBPyConnection
    ) -> None:
        """No snippet, no breakdown — never a quietly-degraded workspace scalar."""
        agent, _ = self._agent()
        composed = agent.compose_unit_grain(
            session,
            _ap_graph(),
            ExecutionContext(duckdb_conn=con, schema_mapping_id=_WORKSPACE),
            axis="account_id",
            workspace_id=_WORKSPACE,
        )
        assert not composed.success
        assert "cannot be composed per 'account_id'" in (composed.error or "")

    def test_a_snippet_without_parts_cannot_be_regrouped(
        self, session: Session, con: duckdb.DuckDBPyConnection
    ) -> None:
        """Parts are the artifact; the stored string is never edited into a grain."""
        snippet = _warm_snippet(session)
        snippet.parts = None
        session.flush()

        agent, _ = self._agent()
        composed = agent.compose_unit_grain(
            session,
            _ap_graph(),
            ExecutionContext(duckdb_conn=con, schema_mapping_id=_WORKSPACE),
            axis="account_id",
            workspace_id=_WORKSPACE,
        )
        assert not composed.success

    def test_a_null_entity_refuses_the_whole_breakdown(
        self, session: Session, con: duckdb.DuckDBPyConnection
    ) -> None:
        """Rows the axis does not name make the WHOLE breakdown unpublishable.

        Two dishonest options and one honest one. Labelling the NULL group invents
        an entity — ``str(None)`` writes the literal string "None" as a business
        key. Dropping it silently withholds part of a total the served verdict
        says RECONCILES, so the parts no longer sum and nothing says why. The
        breakdown is refused whole instead.
        """
        con.execute("INSERT INTO ap VALUES (NULL, DATE '2024-12-01', 77.0)")
        _warm_snippet(session)
        agent, _ = self._agent()

        composed = agent.compose_unit_grain(
            session,
            _ap_graph(),
            ExecutionContext(duckdb_conn=con, schema_mapping_id=_WORKSPACE),
            axis="account_id",
            workspace_id=_WORKSPACE,
        )

        assert not composed.success
        assert "no 'account_id' value" in (composed.error or "")

    def test_a_breakdown_past_the_bound_is_truncated_and_says_so(
        self, session: Session, con: duckdb.DuckDBPyConnection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The cost bound cuts, in entity order, and never silently.

        A categorical axis can carry hundreds of thousands of distinct entities on
        a real fact; composed unbounded, one metric would load them all into
        Python and write them all. The cut is ORDERED so the prefix is
        reproducible run-to-run, and the count of what was left out rides back for
        the caller to disclose.
        """
        monkeypatch.setattr("dataraum.graphs.unit_grain.UNIT_GRAIN_MAX_ENTITIES", 2)
        con.execute(
            "INSERT INTO ap VALUES ('acct_d', DATE '2024-12-01', 5.0),"
            " ('acct_e', DATE '2024-12-01', 6.0)"
        )
        _warm_snippet(session)
        agent, _ = self._agent()

        composed = agent.compose_unit_grain(
            session,
            _ap_graph(),
            ExecutionContext(duckdb_conn=con, schema_mapping_id=_WORKSPACE),
            axis="account_id",
            workspace_id=_WORKSPACE,
        )

        assert composed.success, composed.error
        assert composed.value is not None
        assert composed.value.truncated_at == 2
        assert composed.value.total_entities == 4
        # ORDERED, so the same two entities every run — not whatever the scan emits.
        assert [r.entity_value for r in composed.value.rows] == ["acct_a", "acct_b"]


class TestEntityAxisOrdering:
    """Which axis a breakdown lands on must not depend on row order (DAT-671 B1).

    The axis is persisted per target, so an ordering that flips between runs on
    identical data silently re-keys the breakdown — the codebase's known
    ``.limit(1)``-without-ORDER-BY class, one layer up. The ranking is the
    catalog's own (``curated_slices``), not a private copy: judged tier, then
    measured relevance, then NAME as the tiebreak that makes it total.
    """

    _TABLE = "tbl-axes"

    def _seed(self, session: Session, rows: list[tuple[str, str | None, float | None]]) -> None:
        from dataraum.analysis.slicing.db_models import SliceDefinition
        from dataraum.storage import Column, Source, Table

        source = Source(name="axes_src", source_type="csv")
        session.add(source)
        session.flush()
        table = Table(
            table_id=self._TABLE,
            source_id=source.source_id,
            table_name="facts",
            layer="typed",
            duckdb_path="facts",
            row_count=1,
        )
        session.add(table)
        session.flush()
        for position, (name, interest, relevance) in enumerate(rows):
            column = Column(
                table_id=self._TABLE,
                column_name=name,
                column_position=position,
                raw_type="VARCHAR",
                resolved_type="VARCHAR",
            )
            session.add(column)
            session.flush()
            session.add(
                SliceDefinition(
                    run_id="run-axes",
                    table_id=self._TABLE,
                    column_id=column.column_id,
                    column_name=name,
                    slice_type="categorical",
                    slice_interest=interest,
                    slice_relevance=relevance,
                    detection_source="llm",
                )
            )
        session.flush()

    def test_judged_tier_then_relevance_then_name(self, session: Session) -> None:
        """Pins the contract; note honestly what this harness canNOT prove.

        Reverting to the pre-fix ranking (the hand-rolled 2-tuple with no name
        tiebreak) leaves this test GREEN — measured, not assumed. SQLite answers
        the unfiltered read from the ``(table_id, column_name, run_id)`` unique
        index, so the scan already arrives in name order and a stable sort over it
        reproduces the tiebreak by accident. Postgres offers no such guarantee,
        which is why the flip was invisible until someone read the code.

        What makes the order hold on BOTH backends is that
        ``uq_slice_def_table_column_run`` makes ``column_name`` unique within a
        ``(table_id, run_id)`` read, so ``curated_slices``' three-element sort key
        is a TOTAL order and input order cannot affect the output. The ORDER BY in
        the read is belt-and-braces, not the mechanism — do not drop the
        ``curated_slices`` reuse believing the ORDER BY carries this.
        """
        from dataraum.graphs.unit_grain import resolve_entity_axes

        # Seeded in an order that contradicts every ranking signal, so a resolver
        # that leaned on insertion order would be caught.
        self._seed(
            session,
            [
                ("z_supporting", "supporting", 0.99),
                ("b_tied", "primary", 0.5),
                ("m_best", "primary", 0.9),
                ("a_tied", "primary", 0.5),
            ],
        )
        served = resolve_entity_axes(session, table_id=self._TABLE, run_id="run-axes")

        assert served.axes == ("m_best", "a_tied", "b_tied", "z_supporting")
        assert served.note == ""

    def test_an_unjudged_axis_is_not_served_as_curated(self, session: Session) -> None:
        """Never assessed is not the same as assessed and ranked last."""
        from dataraum.graphs.unit_grain import resolve_entity_axes

        self._seed(session, [("judged", "primary", 0.1), ("never_looked_at", None, 0.99)])
        served = resolve_entity_axes(session, table_id=self._TABLE, run_id="run-axes")

        assert served.axes == ("judged",), "the unjudged axis does not out-rank the judged one"

    def test_nothing_judged_withholds_the_axes_and_says_why(self, session: Session) -> None:
        """The ranker did not run: serving its structural order as curated is the lie.

        Withholding with the catalog's own note keeps "never assessed" distinct
        from "assessed and rejected" — only one of those is a reason to go and
        look at the ranker.
        """
        from dataraum.graphs.unit_grain import resolve_entity_axes

        self._seed(session, [("a", None, 0.9), ("b", None, 0.1)])
        served = resolve_entity_axes(session, table_id=self._TABLE, run_id="run-axes")

        assert served.axes == ()
        assert "business-relevance judgment" in served.note
