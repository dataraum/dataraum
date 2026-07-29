"""Cross-fact drill-across through the metrics phase (DAT-809 / DAT-740).

What is proven here is the NUMBERS: a ratio whose two carriers ground on DIFFERENT
facts composes one subquery per grounding, merges them on the shared conformed
dimension, and lands per-entity rows that equal the arithmetic done by hand.

FIXTURE PROVENANCE — read this before changing the seed. The finance corpus is
FK-normalized and ships NO dim tables, so the conformed-dimension shape cannot come
from it; this fixture IS the evidence base for the cross-fact path. Its shape is
derived from the ONE production writer,
``analysis/hierarchies/bus_matrix.py::derive_bus_matrix``, not from an idealized
picture of what a bus matrix "should" look like:

- a REFERENCED cell always carries a ``conformed_group`` — ``_ref_group_signature``
  renders ``ref:{dim_table_id}:{sorted roles}`` unconditionally — and a non-null
  ``dimension_table_id``;
- ``confirmation_source`` is the WEAKEST-LINK floor over the FK relationships the
  role reaches, from the closed vocabulary ``unconfirmed|judge|keeper|user``;
- ``roles`` holds FK role names and ``attributes`` bare attribute names;
- ``signature`` is ``bus:referenced:{fact}:{dim}:{roles}`` and is half of the
  ``(signature, run_id)`` upsert key.

The two facts deliberately spell the conformed dimension DIFFERENTLY
(``account_id`` vs ``acct``). That is the case a column-name intersection cannot
see, and it is why the merge keys on the served ``conformed_group`` instead.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy import delete, select

from dataraum.analysis.hierarchies.db_models import BusMatrixEntry
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.core.models.base import Result
from dataraum.graphs.additivity import AdditivityStatus, AxisKind, AxisVerdict
from dataraum.graphs.additivity_db_models import AXIS_KEY_ALL, MetricAxisAdditivity
from dataraum.graphs.unit_grain_db_models import MetricUnitGrain
from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases import metrics_phase as gep
from dataraum.pipeline.phases.metrics_phase import MetricsPhase
from dataraum.query.snippet_models import SQLSnippetRecord
from dataraum.storage import Column, Source, Table

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

_WORKSPACE_ID = "ws-cross-fact"
_CATALOGUE_RUN = "run-catalogue-xf"
_OM_RUN = "run-om-xf"

# The stock carrier is pinned to ONE reporting instant (W3's period binding rides in
# the persisted `where` parts). It is NOT re-aggregated to LAST: pinning is what makes
# a per-account balance well-defined, and re-writing the grounding's aggregate would
# mean parsing generated SQL as typed data.
_BOUND = ["\"period\" = TIMESTAMP '2024-12-01 00:00:00'"]

# acct_a / acct_b are on both facts. acct_c is AP-only, acct_d is GL-only — each must
# survive the FULL OUTER merge with a NULL value, never a fabricated zero.
_AP_ROWS = (
    "('acct_a', DATE '2024-06-01', 100.0), ('acct_a', DATE '2024-12-01', 150.0),"
    " ('acct_b', DATE '2024-06-01', 40.0), ('acct_b', DATE '2024-12-01', 60.0),"
    " ('acct_c', DATE '2024-12-01', 25.0)"
)
_GL_ROWS = "('acct_a', 1200.0), ('acct_a', 300.0), ('acct_b', 400.0), ('acct_d', 700.0)"


def _seed_dim(session: Session) -> str:
    source = Source(name="src_accounts", source_type="csv")
    session.add(source)
    session.flush()
    dim = Table(
        table_id=str(uuid4()),
        source_id=source.source_id,
        table_name="chart_of_accounts",
        layer="typed",
        duckdb_path="chart_of_accounts",
        row_count=4,
    )
    session.add(dim)
    session.flush()
    return str(dim.table_id)


def _seed_fact(
    session: Session,
    conn: duckdb.DuckDBPyConnection,
    *,
    view_name: str,
    values: str,
    columns: str,
    axis_column: str,
    dim_id: str,
) -> str:
    """A fact + its enriched view + the curated FK-KEY slice naming the axis.

    ``dimension_attribute`` is NULL on that slice because it is the KEY itself — a
    breakdown groups by what the FACT carries (an id), not by a dim-side attribute.
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

    column = Column(
        table_id=fact.table_id,
        column_name=axis_column,
        column_position=0,
        raw_type="VARCHAR",
        resolved_type="VARCHAR",
    )
    session.add(column)
    session.flush()

    session.add(
        EnrichedView(
            fact_table_id=fact.table_id,
            view_table_id=view.table_id,
            view_name=view_name,
            run_id=_CATALOGUE_RUN,
        )
    )
    session.add(
        SliceDefinition(
            run_id=_CATALOGUE_RUN,
            table_id=fact.table_id,
            column_id=column.column_id,
            column_name=axis_column,
            dimension_table_id=dim_id,
            dimension_attribute=None,
            fk_role=axis_column,
            slice_type="categorical",
            slice_interest="primary",
            slice_relevance=0.9,
            detection_source="llm",
        )
    )
    session.flush()

    conn.execute(f"CREATE TABLE {view_name} AS SELECT * FROM (VALUES {values}) t({columns})")
    return str(fact.table_id)


def _seed_cell(
    session: Session,
    *,
    fact_id: str,
    dim_id: str,
    role: str,
    group: str,
    source: str = "judge",
    needs_confirmation: bool = False,
) -> None:
    """A referenced bus-matrix cell in the shape `derive_bus_matrix` writes."""
    session.add(
        BusMatrixEntry(
            run_id=_CATALOGUE_RUN,
            fact_table_id=fact_id,
            attachment="referenced",
            concept_label="chart_of_accounts",
            dimension_table_id=dim_id,
            roles=[role],
            attributes=[],
            confirmation_source=source,
            conformed_group=group,
            needs_confirmation=needs_confirmation,
            signature=f"bus:referenced:{fact_id}:{dim_id}:{role}",
        )
    )
    session.flush()


def _seed_snippet(
    session: Session, *, field: str, relation: str, expr: str, where: list[str]
) -> None:
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


def _ratio_metric() -> dict[str, Any]:
    """A ratio whose operands ground on two different facts."""
    return {
        "graph_id": "ap_over_spend",
        "metadata": {"name": "AP over spend", "category": "liquidity"},
        "output": {"type": "scalar"},
        "dependencies": {
            "ap": {
                "type": "extract",
                "source": {"standard_field": "accounts_payable", "statement": "balance_sheet"},
                "aggregation": "sum",
            },
            "spend": {
                "type": "extract",
                "source": {"standard_field": "spend", "statement": "balance_sheet"},
                "aggregation": "sum",
            },
            "ratio": {
                "type": "formula",
                "expression": "ap / spend",
                "depends_on": ["ap", "spend"],
                "output_step": True,
            },
        },
    }


def _verdict_stub(rows: list[tuple[str, str, AxisVerdict | None, str | None]]):  # noqa: ANN202
    def _persist(session: Session, _conn: Any, *, run_id: str, **_kw: Any) -> None:
        for target_kind, target_key, verdict, reason in rows:
            session.add(
                MetricAxisAdditivity(
                    run_id=run_id,
                    vertical="financial_reporting",
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


@pytest.fixture
def session(engine):  # noqa: ANN001, ANN201
    from sqlalchemy.orm import sessionmaker

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
def xf_duckdb():  # noqa: ANN201
    import duckdb as _duckdb

    conn = _duckdb.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture()
def _mock_llm():  # noqa: ANN202
    mock_config = MagicMock()
    mock_config.active_provider = "anthropic"
    mock_config.providers = {"anthropic": MagicMock()}
    with (
        patch("dataraum.pipeline.phases.metrics_phase.load_llm_config", return_value=mock_config),
        patch("dataraum.pipeline.phases.metrics_phase.create_provider", return_value=MagicMock()),
        patch("dataraum.pipeline.phases.metrics_phase.PromptRenderer", return_value=MagicMock()),
    ):
        yield


def _fake_result(*_a: Any, **_kw: Any) -> Result:
    execution = MagicMock()
    execution.assumptions = []
    return Result.ok(execution)


def _run(session: Session, conn: duckdb.DuckDBPyConnection, defs, verdicts, table_ids):  # noqa: ANN001, ANN202
    with (
        patch("dataraum.graphs.config.get_metric_definitions", return_value=defs),
        patch("dataraum.graphs.agent.GraphAgent.execute", side_effect=_fake_result),
        patch("dataraum.graphs.agent.GraphAgent.assemble", side_effect=_fake_result),
        patch("dataraum.graphs.agent.ExecutionContext.with_rich_context", MagicMock()),
        patch.object(gep, "_persist_additivity_verdicts", _verdict_stub(verdicts)),
    ):
        return MetricsPhase()._run(_ctx(session, conn, table_ids))


def _rows(session: Session) -> list[MetricUnitGrain]:
    return list(
        session.execute(select(MetricUnitGrain).order_by(MetricUnitGrain.entity_value))
        .scalars()
        .all()
    )


def _seed_conformed_pair(
    session: Session,
    conn: duckdb.DuckDBPyConnection,
    *,
    source: str = "judge",
    needs_confirmation: bool = False,
) -> tuple[str, str, str]:
    """Two facts conformed on one dimension they spell differently."""
    dim_id = _seed_dim(session)
    ap_id = _seed_fact(
        session,
        conn,
        view_name="ap_enriched",
        values=_AP_ROWS,
        columns="account_id, period, balance",
        axis_column="account_id",
        dim_id=dim_id,
    )
    gl_id = _seed_fact(
        session,
        conn,
        view_name="gl_enriched",
        values=_GL_ROWS,
        columns="acct, amount",
        axis_column="acct",
        dim_id=dim_id,
    )
    # `_ref_group_signature` sorts and joins the role names, so a judge CONFORM over
    # differently-named roles yields the UNION — not either role alone.
    group = f"ref:{dim_id}:acct|account_id"
    _seed_cell(
        session,
        fact_id=ap_id,
        dim_id=dim_id,
        role="account_id",
        group=group,
        source=source,
        needs_confirmation=needs_confirmation,
    )
    _seed_cell(
        session,
        fact_id=gl_id,
        dim_id=dim_id,
        role="acct",
        group=group,
        source=source,
        needs_confirmation=needs_confirmation,
    )
    _seed_snippet(
        session,
        field="accounts_payable",
        relation="ap_enriched",
        expr="SUM(balance)",
        where=_BOUND,
    )
    _seed_snippet(session, field="spend", relation="gl_enriched", expr="SUM(amount)", where=[])
    return ap_id, gl_id, group


@pytest.mark.usefixtures("_mock_llm")
class TestCrossFactDrillAcross:
    def test_the_numbers_are_correct_on_the_conformed_fixture(
        self, session: Session, xf_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """The scorecard band, computed by hand.

        AP is pinned to the 2024-12-01 close; GL sums the whole relation:

          acct_a: 150 / (1200 + 300) = 0.1
          acct_b:  60 /  400         = 0.15
          acct_c:  25 /  NULL        = NULL   (AP-only)
          acct_d: NULL /  700        = NULL   (GL-only)

        acct_c and acct_d are the FULL OUTER's whole point — an inner join would
        drop them, and a zero would assert a measurement nobody made.
        """
        ap_id, gl_id, group = _seed_conformed_pair(session, xf_duckdb)

        result = _run(
            session,
            xf_duckdb,
            {"ap_over_spend": _ratio_metric()},
            [
                ("metric", "ap_over_spend", AxisVerdict.ADDITIVE, None),
                ("measure", "accounts_payable", AxisVerdict.ADDITIVE, None),
                ("measure", "spend", AxisVerdict.ADDITIVE, None),
            ],
            [ap_id, gl_id],
        )
        session.flush()

        assert result.status == PhaseStatus.COMPLETED
        rows = _rows(session)
        assert {r.entity_value: r.value for r in rows} == {
            "acct_a": Decimal("0.1"),
            "acct_b": Decimal("0.15"),
            "acct_c": None,
            "acct_d": None,
        }

    def test_the_axis_key_is_the_conformed_identity_not_a_column_name(
        self, session: Session, xf_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """Neither fact's column name could name this axis — they disagree.

        The persisted axis is the group signature, which is also part of the
        ADR-0010 upsert key and therefore must not be a drifting label (DAT-800).
        """
        ap_id, gl_id, group = _seed_conformed_pair(session, xf_duckdb)
        _run(
            session,
            xf_duckdb,
            {"ap_over_spend": _ratio_metric()},
            [
                ("metric", "ap_over_spend", AxisVerdict.ADDITIVE, None),
                ("measure", "accounts_payable", AxisVerdict.ADDITIVE, None),
                ("measure", "spend", AxisVerdict.ADDITIVE, None),
            ],
            [ap_id, gl_id],
        )
        session.flush()

        axes = {r.axis for r in _rows(session)}
        assert axes == {group}
        assert "account_id" not in axes and "acct" not in axes

    def test_unconfirmed_conformance_yields_no_rows_and_discloses_why(
        self, session: Session, xf_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """The silent-join guard, end to end.

        The group key is present and identical on both facts — only the
        confirmation is missing. A `conformed_group`-only filter would merge here.
        """
        ap_id, gl_id, _ = _seed_conformed_pair(session, xf_duckdb, source="unconfirmed")

        result = _run(
            session,
            xf_duckdb,
            {"ap_over_spend": _ratio_metric()},
            [
                ("metric", "ap_over_spend", AxisVerdict.ADDITIVE, None),
                ("measure", "accounts_payable", AxisVerdict.ADDITIVE, None),
                ("measure", "spend", AxisVerdict.ADDITIVE, None),
            ],
            [ap_id, gl_id],
        )
        session.flush()

        assert result.status == PhaseStatus.COMPLETED
        assert _rows(session) == []
        # Absence must fall LOUD: the phase names the refusal on its withheld
        # channel, so "no breakdown" is never indistinguishable from "not attempted".
        withheld = result.outputs["unit_grain_withheld"]["ap_over_spend"]
        assert "unconfirmed or awaiting review" in withheld
        assert "would assert an identity nobody did" in withheld

    def test_a_cell_awaiting_review_yields_no_rows_and_discloses_why(
        self, session: Session, xf_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        ap_id, gl_id, _ = _seed_conformed_pair(session, xf_duckdb, needs_confirmation=True)
        result = _run(
            session,
            xf_duckdb,
            {"ap_over_spend": _ratio_metric()},
            [
                ("metric", "ap_over_spend", AxisVerdict.ADDITIVE, None),
                ("measure", "accounts_payable", AxisVerdict.ADDITIVE, None),
                ("measure", "spend", AxisVerdict.ADDITIVE, None),
            ],
            [ap_id, gl_id],
        )
        session.flush()
        assert _rows(session) == []
        # Two-sided: an empty table alone also passes on a crash or an unrelated
        # withhold, so the REASON is what pins this to the review gate.
        withheld = result.outputs["unit_grain_withheld"]["ap_over_spend"]
        assert "unconfirmed or awaiting review" in withheld

    def test_a_confirmed_but_uncurated_axis_names_the_slice_not_the_pairing(
        self, session: Session, xf_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """The confirmed-but-unsliced case reports its OWN reason.

        Falling through to "share no confirmed conformed dimension" would be
        factually wrong — the pairing IS confirmed — and would send someone to
        confirm it again instead of curating the column.
        """
        ap_id, gl_id, _ = _seed_conformed_pair(session, xf_duckdb)
        # Retract GL's curated key slice, leaving the conformance untouched.
        session.execute(
            delete(SliceDefinition).where(
                SliceDefinition.table_id == gl_id, SliceDefinition.column_name == "acct"
            )
        )
        session.flush()

        result = _run(
            session,
            xf_duckdb,
            {"ap_over_spend": _ratio_metric()},
            [
                ("metric", "ap_over_spend", AxisVerdict.ADDITIVE, None),
                ("measure", "accounts_payable", AxisVerdict.ADDITIVE, None),
                ("measure", "spend", AxisVerdict.ADDITIVE, None),
            ],
            [ap_id, gl_id],
        )
        session.flush()

        assert _rows(session) == []
        withheld = result.outputs["unit_grain_withheld"]["ap_over_spend"]
        assert "curated slice inventory" in withheld
        assert "'acct'" in withheld
        # The wrongly-pinned-reason class: it must NOT claim the pairing is missing
        # or unconfirmed, because both are false here.
        assert "share no confirmed conformed dimension" not in withheld
        assert "unconfirmed or awaiting review" not in withheld

    def test_display_strings_use_the_label_and_never_the_uuid_identity(
        self, session: Session, xf_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """The persisted axis is the identity; everything a human reads is the label.

        `metric_unit_grain.axis` is part of the upsert key so it stays the stable
        group signature — but that signature embeds a table uuid, so a disclosure
        rendering it would put `ref:8f0a…:acct|account_id` in front of a reader.
        """
        ap_id, gl_id, group = _seed_conformed_pair(session, xf_duckdb)
        result = _run(
            session,
            xf_duckdb,
            {"ap_over_spend": _ratio_metric()},
            [
                ("metric", "ap_over_spend", AxisVerdict.ADDITIVE, None),
                ("measure", "accounts_payable", AxisVerdict.ADDITIVE, None),
                ("measure", "spend", AxisVerdict.ADDITIVE, None),
            ],
            [ap_id, gl_id],
        )
        session.flush()

        # Persisted: the identity (stable, unique, part of the upsert key).
        assert {r.axis for r in _rows(session)} == {group}
        # Disclosed: never the identity. No human-facing channel may carry it.
        disclosed = " ".join(
            str(v) for k, v in result.outputs.items() if k.startswith("unit_grain")
        )
        assert group not in disclosed
        assert ap_id not in disclosed and gl_id not in disclosed
