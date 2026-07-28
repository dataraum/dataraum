"""The unit-grain wire-in inside the metrics phase (DAT-671 B1).

The composers and the gate are proven at unit level (``tests/unit/graphs/
test_unit_grain.py``); what is proven HERE is that the phase actually reaches
them — that a declared metric whose served verdict permits a breakdown ends the
run with per-entity rows in ``metric_unit_grain``, that a target the verdict
withholds ends it with none AND a disclosed reason, and that neither outcome can
disturb the metric bookkeeping the phase already recorded.

The substrate is seeded exactly as the pipeline leaves it — a fact table, its
enriched view, the curated categorical slice that NAMES the entity axis, and the
warm extract snippet carrying its clause parts — because every one of those is a
served fact the wire-in reads rather than infers. The LLM is stubbed at the two
boundaries the phase calls it through, as in ``test_metrics_phase.py``.
``_persist_additivity_verdicts`` is replaced by a stub that writes the verdict
under test: the classifier is W3-a's tested surface, and standing in for it is
also what pins the ORDER — the stub writes, and the unit-grain step reads what it
wrote, in the same run.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.core.models.base import Result
from dataraum.graphs.additivity import AdditivityStatus, AxisKind, AxisVerdict
from dataraum.graphs.additivity_db_models import AXIS_KEY_ALL, MetricAxisAdditivity
from dataraum.graphs.unit_grain_db_models import MetricUnitGrain
from dataraum.lifecycle import ArtifactState, LifecycleArtifact
from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases import metrics_phase as gep
from dataraum.pipeline.phases.metrics_phase import MetricsPhase
from dataraum.query.snippet_models import SQLSnippetRecord
from dataraum.storage import Column, Source, Table

if TYPE_CHECKING:
    import duckdb

_WORKSPACE_ID = "ws-unit-grain"
_CATALOGUE_RUN = "run-catalogue"
_OM_RUN = "run-om-grain"
_AXIS = "account_id"

# One fiscal instant, bound once for the relation (DAT-887) — acct_c reported in
# June and stopped, so it carries no level at the close and is ABSENT from the
# breakdown rather than zero.
_BOUND = ["\"period\" = TIMESTAMP '2024-12-01 00:00:00'"]
_AP_ROWS = (
    "('acct_a', DATE '2024-06-01', 100.0), ('acct_a', DATE '2024-12-01', 150.0),"
    " ('acct_b', DATE '2024-06-01', 40.0), ('acct_b', DATE '2024-12-01', 60.0),"
    " ('acct_c', DATE '2024-06-01', 25.0)"
)
_COGS_ROWS = "('acct_a', 1200.0), ('acct_b', 400.0), ('acct_d', 700.0)"


# ---------------------------------------------------------------------------
# Substrate: a fact, its enriched view, its curated axis, its warm snippet
# ---------------------------------------------------------------------------


def _seed_relation(
    session: Session,
    conn: duckdb.DuckDBPyConnection,
    *,
    view_name: str,
    values: str,
    columns: str,
) -> str:
    """A fact + enriched view registered as the pipeline registers them.

    Returns the FACT table id — the id ``slice_definitions`` are keyed on (the
    slicing phase reads the view's columns but writes them against the fact),
    while the grounded relation is the VIEW.
    """
    source = Source(name=f"src_{view_name}", source_type="csv")
    session.add(source)
    session.flush()

    fact = Table(
        table_id=str(uuid4()),
        source_id=source.source_id,
        table_name=f"typed_{view_name}",
        layer="typed",
        duckdb_path=f"typed_{view_name}",
        row_count=10,
    )
    view = Table(
        table_id=str(uuid4()),
        source_id=source.source_id,
        table_name=view_name,
        layer="enriched",
        duckdb_path=view_name,
        row_count=10,
    )
    session.add_all([fact, view])
    session.flush()

    axis_column = Column(
        table_id=fact.table_id,
        column_name=_AXIS,
        column_position=0,
        raw_type="VARCHAR",
        resolved_type="VARCHAR",
    )
    session.add(axis_column)
    session.flush()

    session.add(
        EnrichedView(
            fact_table_id=fact.table_id,
            view_table_id=view.table_id,
            view_name=view_name,
            run_id=_CATALOGUE_RUN,
        )
    )
    # The workspace's own statement that this column is a breakdown axis — the
    # entity axis is SERVED, never a column picked because its name looks like an id.
    session.add(
        SliceDefinition(
            run_id=_CATALOGUE_RUN,
            table_id=fact.table_id,
            column_id=axis_column.column_id,
            column_name=_AXIS,
            slice_type="categorical",
            slice_interest="primary",
            slice_relevance=0.9,
            detection_source="llm",
        )
    )
    session.flush()

    conn.execute(f"CREATE TABLE {view_name} AS SELECT * FROM (VALUES {values}) t({columns})")
    return str(fact.table_id)


def _seed_snippet(
    session: Session, *, field: str, relation: str, expr: str, where: list[str]
) -> None:
    """The extract as the warm pass leaves it: clause parts + their scalar render."""
    from dataraum.graphs.formula_composer import compose_extract_sql

    session.add(
        SQLSnippetRecord(
            workspace_id=_WORKSPACE_ID,
            snippet_type="extract",
            standard_field=field,
            statement="balance_sheet",
            aggregation="sum",
            predicate="",
            schema_mapping_id=_WORKSPACE_ID,
            sql=compose_extract_sql(expr, relation, where),
            description=field,
            source="graph:test",
            parts={
                "select": [{"expr": expr, "alias": "value"}],
                "from": [relation],
                "where": where,
            },
        )
    )
    session.flush()


def _extract_def(field: str, *, output: bool = False) -> dict[str, Any]:
    step: dict[str, Any] = {
        "type": "extract",
        "source": {"standard_field": field, "statement": "balance_sheet"},
        "aggregation": "sum",
    }
    if output:
        step["output_step"] = True
    return step


def _metric_def(graph_id: str, dependencies: dict[str, Any]) -> dict[str, Any]:
    return {
        "graph_id": graph_id,
        "metadata": {"name": graph_id.upper(), "category": "liquidity"},
        "output": {"type": "scalar"},
        "dependencies": dependencies,
    }


def _verdict_stub(rows: list[tuple[str, str, AxisVerdict | None, str | None]]):
    """Stand in for ``_persist_additivity_verdicts``, writing the verdicts under test.

    Each row is ``(target_kind, target_key, verdict, doctrine_reason)``; a
    ``None`` verdict writes the typed ABSTENTION instead. Written on the phase
    session under the phase's own run_id, so the unit-grain step that reads them
    next is reading THIS run's verdicts.
    """

    def _persist(session: Session, _conn: Any, *, run_id: str, **_kw: Any) -> None:
        for target_kind, target_key, verdict, reason in rows:
            session.add(
                MetricAxisAdditivity(
                    run_id=run_id,
                    target_kind=target_kind,
                    target_key=target_key,
                    axis_kind=AxisKind.CATEGORICAL.value,
                    axis_key=AXIS_KEY_ALL,
                    status=(
                        AdditivityStatus.CLASSIFIED.value
                        if verdict is not None
                        else AdditivityStatus.ABSTAINED.value
                    ),
                    verdict=verdict.value if verdict is not None else None,
                    reason=reason if verdict is not None else None,
                    abstain_reason=None if verdict is not None else "missing_extract",
                )
            )
        session.flush()

    return _persist


def _ctx(session: Session, conn: duckdb.DuckDBPyConnection, table_ids: list[str]) -> PhaseContext:
    return PhaseContext(
        session=session,
        duckdb_conn=conn,
        table_ids=table_ids,
        run_id=_OM_RUN,
        config={
            "vertical": "finance",
            "base_runs": {"relationship_run_id": _CATALOGUE_RUN},
            "workspace_id": _WORKSPACE_ID,
        },
    )


def _unit_grain_rows(session: Session) -> list[MetricUnitGrain]:
    return list(
        session.execute(
            select(MetricUnitGrain).order_by(MetricUnitGrain.target_key, MetricUnitGrain.axis)
        )
        .scalars()
        .all()
    )


@pytest.fixture
def session(engine):  # noqa: ANN001, ANN201 - mirrors the root fixture's shape
    """The phase session with PRODUCTION semantics — ``autoflush=False``.

    ``core.connections``' sessionmaker sets ``autoflush=False``, and
    ``test_metrics_phase.py``'s ``_RealSessionManager`` mirrors that on purpose
    ("the two properties the guard's cross-session correctness rests on"). The
    shared root fixture autoflushes, which is a shape the phase never runs under
    — and the savepoint-isolation claim below is specifically about what happens
    on the real one. Asserting it under different session semantics would prove
    something about the harness, not about the phase.
    """
    from sqlalchemy.orm import sessionmaker

    from dataraum.storage import Source

    factory = sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)
    with factory() as sess:
        sess.add(
            Source(
                source_id="00000000-0000-0000-0000-000000000002",
                name="test_baseline",
                source_type="csv",
            )
        )
        sess.flush()
        yield sess


@pytest.fixture
def grain_duckdb():
    """A bare in-memory DuckDB — the composed statements read seeded VALUES tables."""
    import duckdb as _duckdb

    conn = _duckdb.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture()
def _mock_llm():
    """Patch LLM infrastructure so the phase can initialize without config."""
    mock_config = MagicMock()
    mock_config.active_provider = "anthropic"
    mock_config.providers = {"anthropic": MagicMock()}
    with (
        patch("dataraum.pipeline.phases.metrics_phase.load_llm_config", return_value=mock_config),
        patch("dataraum.pipeline.phases.metrics_phase.create_provider", return_value=MagicMock()),
        patch("dataraum.pipeline.phases.metrics_phase.PromptRenderer", return_value=MagicMock()),
    ):
        yield


def _fake_warm():
    def _execute(session, graph, context, *args, **kw):  # noqa: ANN001, ANN202
        execution = MagicMock()
        execution.assumptions = []
        return Result.ok(execution)

    return _execute


def _fake_assemble():
    def _assemble(session, graph, context, bindings, parameters=None, *, workspace_id=""):  # noqa: ANN001, ANN202
        execution = MagicMock()
        execution.assumptions = []
        return Result.ok(execution)

    return _assemble


class _UnitGrainCase:
    """Shared wiring: the LLM boundaries stubbed, the phase run for real."""

    @staticmethod
    def run(session: Session, conn: duckdb.DuckDBPyConnection, defs, verdicts, table_ids):  # noqa: ANN001, ANN205
        with (
            patch("dataraum.graphs.config.get_metric_definitions", return_value=defs),
            patch("dataraum.graphs.agent.GraphAgent.execute", side_effect=_fake_warm()),
            patch("dataraum.graphs.agent.GraphAgent.assemble", side_effect=_fake_assemble()),
            patch("dataraum.graphs.agent.ExecutionContext.with_rich_context", MagicMock()),
            patch.object(gep, "_persist_additivity_verdicts", _verdict_stub(verdicts)),
        ):
            return MetricsPhase()._run(_ctx(session, conn, table_ids))


@pytest.mark.usefixtures("_mock_llm")
class TestUnitGrainWireIn:
    def test_eligible_metric_persists_unit_grain_rows(
        self, session: Session, grain_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """The wire-in, end to end: a permitted breakdown reaches the database.

        Also the MUTATION SENTINEL for the wire-in itself — delete the
        ``_persist_unit_grain`` call from the phase and this is the test that
        fails, because nothing else in the suite asserts the rows exist.
        """
        fact_id = _seed_relation(
            session,
            grain_duckdb,
            view_name="ap_enriched",
            values=_AP_ROWS,
            columns=f"{_AXIS}, period, balance",
        )
        _seed_snippet(
            session,
            field="accounts_payable",
            relation="ap_enriched",
            expr="SUM(balance)",
            where=_BOUND,
        )

        result = _UnitGrainCase.run(
            session,
            grain_duckdb,
            {
                "ap_total": _metric_def(
                    "ap_total", {"ap": _extract_def("accounts_payable", output=True)}
                )
            },
            [("metric", "ap_total", AxisVerdict.ADDITIVE, None)],
            [fact_id],
        )
        session.flush()

        assert result.status == PhaseStatus.COMPLETED
        rows = _unit_grain_rows(session)
        assert {r.entity_value: r.value for r in rows} == {
            "acct_a": Decimal("150.0"),
            "acct_b": Decimal("60.0"),
        }
        assert {(r.target_kind, r.target_key, r.axis, r.run_id) for r in rows} == {
            ("metric", "ap_total", _AXIS, _OM_RUN)
        }
        # The served verdict rides every row: these parts DO sum to the total, and
        # they were summed rather than recomputed.
        assert all(r.reconciles and not r.recompute for r in rows)
        assert result.outputs["unit_grain_offered"] == 1
        assert result.outputs["unit_grain_rows"] == 2
        assert result.outputs["unit_grain_withheld"] == {}

    def test_gated_target_persists_no_rows_and_discloses_the_skip(
        self, session: Session, grain_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """An unjudged target is withheld OUT LOUD — never a silent absence.

        A typed abstention is not permission, and "no rows" on its own is
        indistinguishable from "we never tried". The reason is a normal phase
        OUTPUT rather than a warning: a number nobody could judge additive should
        have no breakdown, which is the system working.
        """
        fact_id = _seed_relation(
            session,
            grain_duckdb,
            view_name="ap_enriched",
            values=_AP_ROWS,
            columns=f"{_AXIS}, period, balance",
        )
        _seed_snippet(
            session,
            field="accounts_payable",
            relation="ap_enriched",
            expr="SUM(balance)",
            where=_BOUND,
        )

        result = _UnitGrainCase.run(
            session,
            grain_duckdb,
            {
                "ap_total": _metric_def(
                    "ap_total", {"ap": _extract_def("accounts_payable", output=True)}
                )
            },
            [("metric", "ap_total", None, None)],  # abstained
            [fact_id],
        )
        session.flush()

        assert result.status == PhaseStatus.COMPLETED
        assert _unit_grain_rows(session) == []
        assert result.outputs["unit_grain_offered"] == 0
        withheld = result.outputs["unit_grain_withheld"]
        assert "missing_extract" in withheld["ap_total"]
        # Withholding is not a failure: it does not ride the warning channel.
        assert not any("unit grain failed" in w for w in result.warnings)

    def test_unit_grain_failure_leaves_the_scalar_intact(
        self, session: Session, grain_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """A breakdown that blows up costs the breakdown — never the metric.

        The step runs on the shared phase session AFTER every metric's execute
        bookkeeping is recorded there. Without the SAVEPOINT an unhandled failure
        would surface as a phase failure and roll that session back, discarding
        executed lifecycle state (and forcing a full Temporal retry of work that
        succeeded) over an annotation.
        """
        fact_id = _seed_relation(
            session,
            grain_duckdb,
            view_name="ap_enriched",
            values=_AP_ROWS,
            columns=f"{_AXIS}, period, balance",
        )
        _seed_snippet(
            session,
            field="accounts_payable",
            relation="ap_enriched",
            expr="SUM(balance)",
            where=_BOUND,
        )

        with patch(
            "dataraum.graphs.agent.GraphAgent.compose_unit_grain",
            side_effect=RuntimeError("composition exploded"),
        ):
            result = _UnitGrainCase.run(
                session,
                grain_duckdb,
                {
                    "ap_total": _metric_def(
                        "ap_total", {"ap": _extract_def("accounts_payable", output=True)}
                    )
                },
                [("metric", "ap_total", AxisVerdict.ADDITIVE, None)],
                [fact_id],
            )
        session.flush()

        assert result.status == PhaseStatus.COMPLETED
        assert _unit_grain_rows(session) == []
        artifact = (
            session.execute(
                select(LifecycleArtifact).where(LifecycleArtifact.artifact_key == "ap_total")
            )
            .scalars()
            .one()
        )
        assert artifact.state == ArtifactState.EXECUTED.value
        # A failure IS loud — it rides the per-metric warning channel.
        assert any("unit grain failed" in w and "exploded" in w for w in result.warnings)

    def test_absent_entity_is_absent_not_zero(
        self, session: Session, grain_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """Two ways of not having a number, and neither of them is zero.

        ``acct_c`` has no row at the bound instant — the data records no level for
        it, so it is ABSENT from the breakdown. ``acct_d`` has purchases but no
        payable, so the FULL OUTER keeps it with a NULL value: "not computable for
        this entity". A zero for either would assert a measurement nobody made,
        and would still sum to the right total, so nothing downstream could catch
        it.
        """
        ap_fact = _seed_relation(
            session,
            grain_duckdb,
            view_name="ap_enriched",
            values=_AP_ROWS,
            columns=f"{_AXIS}, period, balance",
        )
        cogs_fact = _seed_relation(
            session,
            grain_duckdb,
            view_name="purchases_enriched",
            values=_COGS_ROWS,
            columns=f"{_AXIS}, cogs",
        )
        _seed_snippet(
            session,
            field="accounts_payable",
            relation="ap_enriched",
            expr="SUM(balance)",
            where=_BOUND,
        )
        _seed_snippet(
            session,
            field="cost_of_goods_sold",
            relation="purchases_enriched",
            expr="SUM(cogs)",
            where=[],
        )

        dpo = _metric_def(
            "dpo",
            {
                "accounts_payable": _extract_def("accounts_payable"),
                "cost_of_goods_sold": _extract_def("cost_of_goods_sold"),
                "ratio": {
                    "type": "formula",
                    "expression": "accounts_payable / cost_of_goods_sold",
                    "depends_on": ["accounts_payable", "cost_of_goods_sold"],
                    "output_step": True,
                },
            },
        )
        result = _UnitGrainCase.run(
            session,
            grain_duckdb,
            {"dpo": dpo},
            [
                # A ratio: meaningful per entity, RECOMPUTED from its carriers, and
                # the parts do not sum to the total. Offered only because both
                # carriers themselves partition cleanly.
                ("metric", "dpo", AxisVerdict.NON_ADDITIVE_RECOMPUTE, "ratio"),
                ("measure", "accounts_payable", AxisVerdict.ADDITIVE, None),
                ("measure", "cost_of_goods_sold", AxisVerdict.ADDITIVE, None),
            ],
            [ap_fact, cogs_fact],
        )
        session.flush()

        assert result.status == PhaseStatus.COMPLETED
        rows = {r.entity_value: r.value for r in _unit_grain_rows(session)}
        assert set(rows) == {"acct_a", "acct_b", "acct_d"}, "acct_c is absent, not zero"
        assert rows["acct_d"] is None, "not computable here — never 0"
        assert rows["acct_a"] == pytest.approx(Decimal(150) / Decimal(1200))
        # The recompute family: per-entity meaningful, total not reconciling.
        persisted = _unit_grain_rows(session)
        assert all(r.recompute and not r.reconciles for r in persisted)

    def test_a_failed_write_is_loud_and_claims_nothing(
        self, session: Session, grain_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """Composed-but-not-persisted must not read as a delivered breakdown.

        The offers were real and the rows are not. Left alone, the summary
        rendered "1 broken down per entity" over an EMPTY table with nothing on
        the warning channel — the exact shape of a silent data loss, and the one
        outcome a best-effort step must never produce.
        """
        fact_id = _seed_relation(
            session,
            grain_duckdb,
            view_name="ap_enriched",
            values=_AP_ROWS,
            columns=f"{_AXIS}, period, balance",
        )
        _seed_snippet(
            session,
            field="accounts_payable",
            relation="ap_enriched",
            expr="SUM(balance)",
            where=_BOUND,
        )

        with patch("dataraum.storage.upsert.upsert", side_effect=RuntimeError("write refused")):
            result = _UnitGrainCase.run(
                session,
                grain_duckdb,
                {
                    "ap_total": _metric_def(
                        "ap_total", {"ap": _extract_def("accounts_payable", output=True)}
                    )
                },
                [("metric", "ap_total", AxisVerdict.ADDITIVE, None)],
                [fact_id],
            )
        session.flush()

        assert result.status == PhaseStatus.COMPLETED
        assert _unit_grain_rows(session) == []
        assert result.outputs["unit_grain_offered"] == 0, "nothing landed, so nothing is claimed"
        assert result.outputs["unit_grain_rows"] == 0
        assert any("unit grain failed" in w and "not persisted" in w for w in result.warnings)

    def test_a_truncated_breakdown_is_offered_not_withheld(
        self,
        session: Session,
        grain_duckdb: duckdb.DuckDBPyConnection,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Bounded rows still EXIST — a different fact from having none.

        Truncation and withholding disclose opposite things: "render these, there
        are more" versus "there is nothing to render". Sharing one channel made a
        truncated metric count against the summary's withheld total, and would let
        a consumer reading `withheld` as "no breakdown here" skip rows that are
        sitting in the table.
        """
        monkeypatch.setattr("dataraum.graphs.unit_grain.UNIT_GRAIN_MAX_ENTITIES", 1)
        fact_id = _seed_relation(
            session,
            grain_duckdb,
            view_name="ap_enriched",
            values=_AP_ROWS,
            columns=f"{_AXIS}, period, balance",
        )
        _seed_snippet(
            session,
            field="accounts_payable",
            relation="ap_enriched",
            expr="SUM(balance)",
            where=_BOUND,
        )

        result = _UnitGrainCase.run(
            session,
            grain_duckdb,
            {
                "ap_total": _metric_def(
                    "ap_total", {"ap": _extract_def("accounts_payable", output=True)}
                )
            },
            [("metric", "ap_total", AxisVerdict.ADDITIVE, None)],
            [fact_id],
        )
        session.flush()

        assert result.status == PhaseStatus.COMPLETED
        # The rows are real and the metric counts as offered...
        assert [r.entity_value for r in _unit_grain_rows(session)] == ["acct_a"]
        assert result.outputs["unit_grain_offered"] == 1
        assert result.outputs["unit_grain_rows"] == 1
        # ...and the cut is disclosed on its OWN channel, never as a withholding.
        assert "ap_total" not in result.outputs["unit_grain_withheld"]
        truncated = result.outputs["unit_grain_truncated"]["ap_total"]
        assert "truncated at 1 of 2 entities" in truncated
        assert not any("unit grain failed" in w for w in result.warnings)
