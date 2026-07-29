"""The reconciles_with evaluation inside the metrics phase (DAT-739).

The executor's rules are proven at unit level (``tests/unit/analysis/semantic/
test_reconciliation.py``); what is proven HERE is that the phase reaches them,
and that it does so at the one point in the run where the answer can be right:
AFTER the derivation has settled which assertions exist. The derivation is NOT
stubbed — a concept with two healthy groundings ends the run with a derived
self-loop edge AND the evaluated tie-out for it, which is the whole chain the
ticket asks for.

The substrate is seeded as the pipeline leaves it: two enriched views over real
DuckDB tables, and the warm extract snippets carrying their clause parts. The
LLM is stubbed at the two boundaries the phase calls it through, as in the
sibling wire-in tests.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.db_models import Concept, ConceptEdge, ConceptEdgePredicate
from dataraum.analysis.semantic.reconciliation_db_models import (
    ConceptReconciliation,
    ReconciliationStatus,
    ReconciliationVerdict,
)
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.core.models.base import Result
from dataraum.graphs.formula_composer import compose_extract_sql
from dataraum.lifecycle import ArtifactState, LifecycleArtifact
from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases import metrics_phase as gep
from dataraum.pipeline.phases.metrics_phase import MetricsPhase
from dataraum.query.snippet_models import SQLSnippetRecord
from dataraum.storage import Column, Source, Table

if TYPE_CHECKING:
    import duckdb

_WORKSPACE_ID = "ws-reconcile"
_CATALOGUE_RUN = "run-catalogue"
_OM_RUN = "run-om-reconcile"
_CONCEPT = "accounts_payable"

# The two angles, disagreeing: the general ledger says 2.21M, the subledger
# 3.13M. Same quantity, two routes, one of them wrong — and until this phase
# evaluated the assertion, nothing in the system compared them.
_GL_TOTAL = 2_210_000.0
_SUBLEDGER_TOTAL = 3_130_000.0


def _seed_relation(
    session: Session,
    conn: duckdb.DuckDBPyConnection,
    *,
    view_name: str,
    total: float,
) -> str:
    """A fact + enriched view registered as the pipeline registers them."""
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
    session.add(
        Column(
            table_id=fact.table_id,
            column_name="amount",
            column_position=0,
            raw_type="DOUBLE",
            resolved_type="DOUBLE",
        )
    )
    session.add(
        EnrichedView(
            fact_table_id=fact.table_id,
            view_table_id=view.table_id,
            view_name=view_name,
            run_id=_CATALOGUE_RUN,
        )
    )
    session.flush()

    conn.execute(f"CREATE TABLE {view_name} AS SELECT * FROM (VALUES ({total})) t(amount)")
    return str(fact.table_id)


def _seed_grounding(
    session: Session, *, statement: str, relation: str, as_of: str | None = None
) -> None:
    """The extract as the warm pass leaves it: clause parts + their scalar render."""
    parts: dict[str, Any] = {
        "select": [{"expr": "SUM(amount)", "alias": "value"}],
        "from": [relation],
        "where": [],
    }
    if as_of is not None:
        parts["period_binding"] = {"as_of": as_of, "window_close": as_of}
    session.add(
        SQLSnippetRecord(
            workspace_id=_WORKSPACE_ID,
            snippet_type="extract",
            standard_field=_CONCEPT,
            statement=statement,
            aggregation="sum",
            predicate="",
            schema_mapping_id=_WORKSPACE_ID,
            sql=compose_extract_sql("SUM(amount)", relation, []),
            description=_CONCEPT,
            source="graph:test",
            parts=parts,
        )
    )
    session.flush()


def _metric_defs() -> dict[str, Any]:
    return {
        "ap_total": {
            "graph_id": "ap_total",
            "metadata": {"name": "AP TOTAL", "category": "liquidity"},
            "output": {"type": "scalar"},
            "dependencies": {
                "ap": {
                    "type": "extract",
                    "source": {"standard_field": _CONCEPT, "statement": "general_ledger"},
                    "aggregation": "sum",
                    "output_step": True,
                }
            },
        }
    }


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


def _reconciliations(session: Session) -> list[ConceptReconciliation]:
    session.flush()
    return list(
        session.execute(
            select(ConceptReconciliation).order_by(ConceptReconciliation.pair_key)
        ).scalars()
    )


@pytest.fixture
def session(engine):  # noqa: ANN001, ANN201 - mirrors the sibling wire-in fixture
    """The phase session with PRODUCTION semantics — ``autoflush=False``.

    The savepoint-isolation claim below is specifically about the real session
    shape; asserting it under the autoflushing root fixture would prove
    something about the harness instead.
    """
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
def recon_duckdb():  # noqa: ANN201
    """A bare in-memory DuckDB — the stored groundings re-execute against it."""
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


def _fake_warm():  # noqa: ANN202
    def _execute(session, graph, context, *args, **kw):  # noqa: ANN001, ANN202
        execution = MagicMock()
        execution.assumptions = []
        return Result.ok(execution)

    return _execute


def _fake_assemble():  # noqa: ANN202
    def _assemble(session, graph, context, bindings, parameters=None, *, workspace_id=""):  # noqa: ANN001, ANN202
        execution = MagicMock()
        execution.assumptions = []
        return Result.ok(execution)

    return _assemble


def _run_phase(session: Session, conn: duckdb.DuckDBPyConnection, table_ids: list[str]):  # noqa: ANN202
    """The phase for real, with only the LLM boundaries and the verdicts stubbed."""
    with (
        patch("dataraum.graphs.config.get_metric_definitions", return_value=_metric_defs()),
        patch("dataraum.graphs.agent.GraphAgent.execute", side_effect=_fake_warm()),
        patch("dataraum.graphs.agent.GraphAgent.assemble", side_effect=_fake_assemble()),
        patch("dataraum.graphs.agent.ExecutionContext.with_rich_context", MagicMock()),
        patch.object(gep, "_persist_additivity_verdicts", lambda *a, **kw: None),
    ):
        return MetricsPhase()._run(_ctx(session, conn, table_ids))


def _seed_two_angles(
    session: Session, conn: duckdb.DuckDBPyConnection, *, as_of: tuple[str | None, str | None]
) -> list[str]:
    session.add(Concept(vertical="finance", name=_CONCEPT, kind="measure"))
    gl = _seed_relation(session, conn, view_name="general_ledger", total=_GL_TOTAL)
    sub = _seed_relation(session, conn, view_name="ap_subledger", total=_SUBLEDGER_TOTAL)
    _seed_grounding(session, statement="general_ledger", relation="general_ledger", as_of=as_of[0])
    _seed_grounding(session, statement="ap_subledger", relation="ap_subledger", as_of=as_of[1])
    return [gl, sub]


@pytest.mark.usefixtures("_mock_llm")
class TestReconciliationWireIn:
    def test_multi_grounding_concept_ends_the_run_with_an_evaluated_tie_out(
        self, session: Session, recon_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """The wire-in, end to end — derivation to evaluated row.

        Also the MUTATION SENTINEL for the wire-in itself: delete the
        ``evaluate_reconciliations`` call from the phase and this is the test
        that fails, because nothing else asserts the rows exist.
        """
        table_ids = _seed_two_angles(session, recon_duckdb, as_of=(None, None))

        result = _run_phase(session, recon_duckdb, table_ids)

        assert result.status == PhaseStatus.COMPLETED
        # The derivation ran first and asserted the self-loop …
        edge = session.execute(
            select(ConceptEdge).where(
                ConceptEdge.predicate == ConceptEdgePredicate.RECONCILES_WITH.value
            )
        ).scalar_one()
        assert edge.from_concept == edge.to_concept == _CONCEPT
        assert edge.tolerance is None
        # … and the evaluation measured it.
        (row,) = _reconciliations(session)
        assert row.run_id == _OM_RUN
        assert row.status == ReconciliationStatus.EVALUATED.value
        assert row.verdict == ReconciliationVerdict.NO_TOLERANCE_DECLARED.value
        assert abs(row.delta or Decimal(0)) == Decimal(920_000)
        assert {row.left_relation, row.right_relation} == {"general_ledger", "ap_subledger"}

    def test_the_observed_delta_is_disclosed_as_output_not_shouted(
        self, session: Session, recon_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """No band was declared, so a measured break is a fact, not an alarm."""
        table_ids = _seed_two_angles(session, recon_duckdb, as_of=(None, None))

        result = _run_phase(session, recon_duckdb, table_ids)

        assert result.outputs["reconciliation_evaluated"] == 1
        assert _CONCEPT in result.outputs["reconciliation_observed"]
        assert not any("reconciliation breached" in w for w in result.warnings)
        assert not any("reconciliation grounding failed" in w for w in result.warnings)

    def test_different_instants_are_withheld_with_the_reason(
        self, session: Session, recon_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """Two angles at two fiscal instants disclose non-comparability."""
        table_ids = _seed_two_angles(session, recon_duckdb, as_of=("2024-03-31", "2024-06-30"))

        result = _run_phase(session, recon_duckdb, table_ids)

        (row,) = _reconciliations(session)
        assert row.status == ReconciliationStatus.ABSTAINED.value
        assert row.delta is None
        assert _CONCEPT in result.outputs["reconciliation_withheld"]

    def test_a_reconciliation_failure_leaves_the_metric_state_intact(
        self, session: Session, recon_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """SAVEPOINT isolation: tie-out evidence is never worth a metric's state."""
        table_ids = _seed_two_angles(session, recon_duckdb, as_of=(None, None))

        def _boom(*_a: Any, **_kw: Any) -> None:
            raise RuntimeError("reconciliation exploded")

        with patch(
            "dataraum.analysis.semantic.reconciliation.evaluate_reconciliations",
            side_effect=_boom,
        ):
            result = _run_phase(session, recon_duckdb, table_ids)

        assert result.status == PhaseStatus.COMPLETED
        assert _reconciliations(session) == []
        artifact = session.execute(
            select(LifecycleArtifact).where(LifecycleArtifact.artifact_key == "ap_total")
        ).scalar_one()
        assert artifact.state == ArtifactState.EXECUTED.value

    def test_a_single_grounding_concept_is_untouched(
        self, session: Session, recon_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """No second angle, no assertion, no row — and no gate on the metric."""
        session.add(Concept(vertical="finance", name=_CONCEPT, kind="measure"))
        gl = _seed_relation(session, recon_duckdb, view_name="general_ledger", total=_GL_TOTAL)
        _seed_grounding(session, statement="general_ledger", relation="general_ledger")

        result = _run_phase(session, recon_duckdb, [gl])

        assert result.status == PhaseStatus.COMPLETED
        assert _reconciliations(session) == []
        assert result.outputs["reconciliation_rows"] == 0
