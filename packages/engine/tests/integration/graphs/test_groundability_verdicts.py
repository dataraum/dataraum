"""The ungroundable dimension, end to end (DAT-620).

A coded discriminator with no resolving reference table must fail HONESTLY —
never a confident mislabel. This drives the whole story through the REAL agent
against a real DuckDB relation and a real Postgres read surface (the DAT-658
drift-test shape — no pipeline, no LLM beyond one mocked authoring):

1. the workspace serves a fact whose ``entity_code`` values are opaque codes:
   role ``identifier`` at the generation head, no ``ColumnConcept`` meaning at
   the catalogue run, no reference edge — the three-leg trigger holds;
2. the (mocked) agent grounds a served code and the aggregate NULLs → the
   classification seam evaluates the verdict live and the failure carries
   ``ungroundable_dimension`` with the generic link-a-reference message — NOT
   ``operand_all_null``'s misdirection;
3. the metrics-phase persist writes the verdict row, readable on
   ``current_dimension_groundability`` under the promoted operating_model head;
4. LINKING the resolving reference table clears the verdict on the next run's
   rows (the ticket's no-false-abstention criterion, at the persisted surface).

Plus the persistence discipline the additivity twin pins: idempotent
``(column_id, run_id)`` upsert, and fault isolation (a bug costs the
annotation, never the phase session).
"""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch
from uuid import uuid4

import duckdb
import pytest
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session, sessionmaker

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.core.models.base import Result
from dataraum.graphs.agent import ExecutionContext, GeneratedCode, GraphAgent
from dataraum.graphs.context_models import (
    ColumnContext,
    GraphExecutionContext,
    TableContext,
)
from dataraum.graphs.field_mapping import ColumnMeaning
from dataraum.graphs.groundability_db_models import DimensionGroundability
from dataraum.graphs.models import (
    GraphMetadata,
    GraphSource,
    GraphStep,
    OutputDef,
    OutputType,
    SnippetFailureMode,
    StepSource,
    StepType,
    TransformationGraph,
)
from dataraum.pipeline.phases.metrics_phase import _persist_groundability_verdicts
from dataraum.server.workspace import schema_name_for
from dataraum.storage import Column, Source, Table
from dataraum.storage.property_graph import drop_property_graph, materialize_property_graph
from dataraum.storage.read_views import materialize_read_schema, read_schema_name_for
from dataraum.storage.snapshot_head import GENERATION_STAGE, MetadataSnapshotHead

WS_ID = os.environ["DATARAUM_WORKSPACE_ID"]
CAT_RUN = "cat-run-620"
GEN_RUN = "gen-run-620"
OM_RUN_1 = "om-run-620-1"
OM_RUN_2 = "om-run-620-2"
OM_RUN_3 = "om-run-620-3"
VIEW = "enriched_coded_ledger"


@pytest.fixture
def pg_session(integration_engine: Engine) -> Session:
    factory = sessionmaker(bind=integration_engine, expire_on_commit=False)
    with factory() as sess:
        yield sess


def _seed_catalog(session: Session) -> dict[str, str]:
    """The coded-discriminator workspace: fact + enriched view + identifier role.

    All three trigger legs hold after this seed — no ColumnConcept meaning, role
    ``identifier`` at the promoted generation head, no reference edge.
    """
    source = Source(name=f"src_{uuid4().hex[:8]}", source_type="csv")
    session.add(source)
    session.flush()
    fact = Table(
        table_id=str(uuid4()),
        source_id=source.source_id,
        table_name="coded_ledger",
        layer="typed",
        duckdb_path="coded_ledger",
    )
    view = Table(
        table_id=str(uuid4()),
        source_id=source.source_id,
        table_name=VIEW,
        layer="enriched",
        duckdb_path=VIEW,
    )
    session.add_all([fact, view])
    session.flush()
    code = Column(
        table_id=fact.table_id,
        column_name="entity_code",
        column_position=0,
        raw_type="VARCHAR",
        resolved_type="VARCHAR",
    )
    amount = Column(
        table_id=fact.table_id,
        column_name="amount",
        column_position=1,
        raw_type="VARCHAR",
        resolved_type="DOUBLE",
    )
    session.add_all([code, amount])
    session.flush()
    session.add_all(
        [
            Column(
                table_id=view.table_id,
                column_name="entity_code",
                column_position=0,
                origin="fact",
                source_column_id=code.column_id,
            ),
            Column(
                table_id=view.table_id,
                column_name="amount",
                column_position=1,
                origin="fact",
                source_column_id=amount.column_id,
            ),
            EnrichedView(
                fact_table_id=fact.table_id,
                view_table_id=view.table_id,
                view_name=VIEW,
                run_id=CAT_RUN,
            ),
            SemanticAnnotation(
                column_id=code.column_id, run_id=GEN_RUN, semantic_role="identifier"
            ),
            # The promoted generation head the classification seam self-resolves
            # the role through (the same head BaseRunMap pins).
            MetadataSnapshotHead(
                target=f"table:{fact.table_id}", stage=GENERATION_STAGE, run_id=GEN_RUN
            ),
        ]
    )
    session.commit()
    return {"fact_id": fact.table_id, "code_id": code.column_id}


def _graph() -> TransformationGraph:
    return TransformationGraph(
        graph_id="segment_cost",
        version="1.0",
        metadata=GraphMetadata(
            name="Segment cost", description="", category="cost", source=GraphSource.SYSTEM
        ),
        output=OutputDef(output_type=OutputType.SCALAR),
        steps={
            "value": GraphStep(
                step_id="value",
                step_type=StepType.EXTRACT,
                source=StepSource(standard_field="segment_cost", statement="income_statement"),
                aggregation="sum",
                output_step=True,
            )
        },
    )


def _context(duckdb_conn: duckdb.DuckDBPyConnection) -> ExecutionContext:
    """The served context: the codes ARE the complete enumeration (contract-v2
    validation then admits a member on them — a served code, honestly selected,
    that still resolves to nothing)."""
    rich = GraphExecutionContext(
        tables=[
            TableContext(
                table_id="t_coded",
                table_name=VIEW,
                duckdb_name=VIEW,
                columns=[
                    ColumnContext(
                        column_id="c_code",
                        column_name="entity_code",
                        table_name=VIEW,
                        distinct_count=2,
                        top_values=[
                            {"value": "4000", "count": 1},
                            {"value": "5100", "count": 1},
                        ],
                    ),
                    ColumnContext(column_id="c_amt", column_name="amount", table_name=VIEW),
                ],
            )
        ],
        # The MEASURE has an authored meaning; the coded discriminator does NOT —
        # exactly the DAT-620 shape (and the feed the agent refuses to author
        # without).
        field_mappings=[
            ColumnMeaning(
                column_id="c_amt",
                column_name="amount",
                table_name=VIEW,
                meaning="Ledger amount",
            )
        ],
    )
    return ExecutionContext(
        duckdb_conn=duckdb_conn,
        schema_mapping_id=WS_ID,
        rich_context=rich,
        catalogue_run_id=CAT_RUN,
    )


def _authoring_agent() -> GraphAgent:
    """A GraphAgent whose mocked LLM grounds the concept via ``entity_code = '4000'``
    — a SERVED code, declared as a member (the DAT-787-honest shape). The guess is
    the value→concept mapping, which nothing in the workspace can resolve."""
    config = MagicMock()
    config.limits.max_output_tokens_per_request = 4000
    config.features.graph_sql_generation = None
    renderer = MagicMock()
    renderer.render_split.return_value = ("system", "user")
    agent = GraphAgent(config=config, provider=MagicMock(), prompt_renderer=renderer)
    agent.provider.get_model_for_tier.return_value = "test-model"
    response = MagicMock()
    response.tool_calls = []
    response.content = json.dumps(
        {
            "grounding": "segment_cost via entity_code = '4000' (a served code)",
            "relation": VIEW,
            "where": ["entity_code = '4000'"],
            "select_expr": "SUM(amount)",
            "description": "segment cost: SUM(amount) where entity_code = '4000'",
            "assumptions": [],
            "provenance": {
                "column_mappings_basis": [
                    {
                        "concept": "segment_cost",
                        "basis": {
                            "measure_columns": ["amount"],
                            "filter_columns": ["entity_code"],
                            "filter": "entity_code = '4000'",
                            "filter_members": [{"column": "entity_code", "value": "4000"}],
                        },
                    }
                ],
            },
        }
    )
    agent.provider.converse = MagicMock(return_value=Result.ok(response))
    return agent


def _fall_loud_agent() -> GraphAgent:
    """A GraphAgent whose mocked LLM COMPLIES with the do-not-guess guidance:
    the fall-loud shape (empty relation, select_expr NULL, empty basis)."""
    config = MagicMock()
    config.limits.max_output_tokens_per_request = 4000
    config.features.graph_sql_generation = None
    renderer = MagicMock()
    renderer.render_split.return_value = ("system", "user")
    agent = GraphAgent(config=config, provider=MagicMock(), prompt_renderer=renderer)
    agent.provider.get_model_for_tier.return_value = "test-model"
    response = MagicMock()
    response.tool_calls = []
    response.content = json.dumps(
        {
            "grounding": "cannot ground: the filter column's coded values are unresolvable",
            "relation": "",
            "where": [],
            "select_expr": "NULL",
            "description": "fall-loud: unresolvable coded discriminator",
            "assumptions": [],
            "provenance": {"column_mappings_basis": []},
        }
    )
    agent.provider.converse = MagicMock(return_value=Result.ok(response))
    return agent


def _link_reference(session: Session, ids: dict[str, str]) -> None:
    """The cure: a reference table resolving the codes, joined by a defined FK."""
    source = Source(name=f"src_ref_{uuid4().hex[:8]}", source_type="csv")
    session.add(source)
    session.flush()
    ref = Table(
        table_id=str(uuid4()),
        source_id=source.source_id,
        table_name="entity_reference",
        layer="typed",
        duckdb_path="entity_reference",
    )
    session.add(ref)
    session.flush()
    key = Column(
        table_id=ref.table_id,
        column_name="entity_code",
        column_position=0,
        resolved_type="VARCHAR",
    )
    name = Column(
        table_id=ref.table_id,
        column_name="entity_name",
        column_position=1,
        resolved_type="VARCHAR",
    )
    session.add_all([key, name])
    session.flush()
    session.add(
        Relationship(
            run_id=CAT_RUN,
            from_table_id=ids["fact_id"],
            from_column_id=ids["code_id"],
            to_table_id=ref.table_id,
            to_column_id=key.column_id,
            relationship_type="foreign_key",
            cardinality="many-to-one",
            confidence=0.95,
            detection_method="llm",
            confirmation_source="judge",
        )
    )
    session.commit()


def _seed_healthy_snippet(session: Session) -> None:
    """A HEALTHY grounding row for ``_graph()``'s extract — typed filter_members
    on the provenance, parts with the coded filter."""
    from dataraum.query.snippet_models import SQLSnippetRecord

    session.add(
        SQLSnippetRecord(
            workspace_id=WS_ID,
            schema_mapping_id=WS_ID,
            snippet_type="extract",
            standard_field="segment_cost",
            statement="income_statement",
            aggregation="sum",
            sql=f"SELECT SUM(amount) AS value FROM {VIEW} WHERE entity_code = '4000'",
            source="graph:segment_cost",
            parts={
                "select": [{"expr": "SUM(amount)", "alias": "value"}],
                "from": [VIEW],
                "where": ["entity_code = '4000'"],
            },
            provenance={
                "column_mappings_basis": {
                    "segment_cost": {
                        "measure_columns": ["amount"],
                        "filter_columns": ["entity_code"],
                        "filter": "entity_code = '4000'",
                        "filter_members": [{"column": "entity_code", "value": "4000"}],
                    }
                },
                "assumptions": [],
            },
        )
    )
    session.commit()


def _promote_om(session: Session, run_id: str) -> None:
    session.execute(
        text(
            "INSERT INTO metadata_snapshot_head (head_id, target, stage, run_id, promoted_at) "
            "VALUES (:head_id, 'catalog', 'operating_model', :run, now()) "
            "ON CONFLICT (target, stage) DO UPDATE SET run_id = :run, promoted_at = now()"
        ),
        {"head_id": str(uuid4()), "run": run_id},
    )
    session.commit()


def _current_rows(session: Session) -> list[tuple[str, str | None, str | None]]:
    view = f'"{read_schema_name_for(schema_name_for(WS_ID))}".current_dimension_groundability'
    return [
        (r[0], r[1], r[2])
        for r in session.execute(
            text(f"SELECT column_name, verdict, reason FROM {view} ORDER BY column_name")  # noqa: S608
        ).all()
    ]


def test_coded_discriminator_fails_honest_and_clears_when_reference_links(
    integration_engine: Engine,
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    # The served relation: the guessed code SELECTS rows, but the operand is NULL
    # over them — without the verdict this would classify operand_all_null and
    # misdirect ("fix NULL-safety") when the truth is the code resolves to nothing.
    duckdb_conn.execute(f"CREATE TABLE {VIEW} (entity_code VARCHAR, amount DOUBLE)")
    duckdb_conn.execute(f"INSERT INTO {VIEW} VALUES ('4000', NULL), ('5100', 10.0)")
    ids = _seed_catalog(pg_session)
    schema = schema_name_for(WS_ID)
    with integration_engine.begin() as conn:
        # The full boot shape (the DAT-658 test's `_boot`): a property graph from
        # another test survives the TRUNCATE-based clean and would block the read
        # views' DROP-then-CREATE, and the og_* element views the agent's period
        # binding reads come from the property-graph materialization.
        drop_property_graph(conn, schema)
        materialize_read_schema(conn, schema)
        materialize_property_graph(conn, schema)

    # 1+2 — the authored grounding NULLs; the failure carries the fifth class and
    # the generic cure, evaluated live at the classification seam.
    agent = _authoring_agent()
    result = agent.execute(pg_session, _graph(), _context(duckdb_conn), workspace_id=WS_ID)
    assert not result.success
    assert "ungroundable_dimension" in result.error
    assert "entity_code" in result.error
    assert "link a reference/lookup table" in result.error
    assert "operand" not in result.error, "the probe's misdirection must not surface"
    pg_session.flush()

    # The retained row carries the typed pairing, readable off current_groundings.
    groundings = f'"{read_schema_name_for(schema)}".current_groundings'
    prov = pg_session.execute(
        text(f"SELECT provenance FROM {groundings} WHERE concept = 'segment_cost'")  # noqa: S608
    ).scalar_one()
    assert prov["no_support_class"] == "ungroundable_dimension"
    assert "entity_code" in prov["no_support_evidence"]

    # 3 — the metrics-phase persist: the dependency read resolves the RETAINED
    # FAILURE row's filter column (from its parts.where — a failed row carries no
    # basis) and writes the ungroundable verdict, served on the read view once
    # the operating_model head promotes this run.
    _persist_groundability_verdicts(
        pg_session,
        duckdb_conn,
        graphs={"segment_cost": _graph()},
        workspace_id=WS_ID,
        vertical="financial_reporting",
        run_id=OM_RUN_1,
        catalogue_run_id=CAT_RUN,
        semantic_runs={ids["fact_id"]: GEN_RUN},
    )
    pg_session.commit()
    _promote_om(pg_session, OM_RUN_1)
    assert _current_rows(pg_session) == [("entity_code", "ungroundable", "no_resolving_reference")]

    # 4 — the COMPLIANT FALL-LOUD refresh (senior critical regression): the model
    # obeys the do-not-guess guidance; the sticky carry must keep the class AND
    # the prior row's parts through save_snippet's refresh — without the parts,
    # the dependency read would drop the column and the verdict row (and the
    # relation on current_groundings) would silently vanish from this run on.
    fall_loud = _fall_loud_agent()
    result = fall_loud.execute(pg_session, _graph(), _context(duckdb_conn), workspace_id=WS_ID)
    assert not result.success
    pg_session.flush()
    row = pg_session.execute(
        text(  # noqa: S608
            f"SELECT provenance, relation, parts FROM {groundings} WHERE concept = 'segment_cost'"
        )
    ).one()
    assert row[0]["no_support_class"] == "ungroundable_dimension", "compliance must not strip"
    assert row[1] == VIEW, "the refresh must not wipe the row's relation off the read surface"
    assert row[2]["where"] == ["entity_code = '4000'"], "the prior parts must survive"

    _persist_groundability_verdicts(
        pg_session,
        duckdb_conn,
        graphs={"segment_cost": _graph()},
        workspace_id=WS_ID,
        vertical="financial_reporting",
        run_id=OM_RUN_2,
        catalogue_run_id=CAT_RUN,
        semantic_runs={ids["fact_id"]: GEN_RUN},
    )
    pg_session.commit()
    _promote_om(pg_session, OM_RUN_2)
    assert _current_rows(pg_session) == [
        ("entity_code", "ungroundable", "no_resolving_reference")
    ], "the verdict row must persist across a compliant fall-loud run — absence would lie"

    # 5 — LINK the resolving reference table: the next run's rows clear the
    # verdict (no false abstention), and the promoted head serves them.
    _link_reference(pg_session, ids)
    _persist_groundability_verdicts(
        pg_session,
        duckdb_conn,
        graphs={"segment_cost": _graph()},
        workspace_id=WS_ID,
        vertical="financial_reporting",
        run_id=OM_RUN_3,
        catalogue_run_id=CAT_RUN,
        semantic_runs={ids["fact_id"]: GEN_RUN},
    )
    pg_session.commit()
    _promote_om(pg_session, OM_RUN_3)
    assert _current_rows(pg_session) == [
        ("entity_code", "groundable", "resolving_reference_linked")
    ]


def test_persist_upserts_idempotently_and_isolates_failure(
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """The additivity persistence discipline on the twin table."""
    duckdb_conn.execute(f"CREATE TABLE IF NOT EXISTS {VIEW} (entity_code VARCHAR, amount DOUBLE)")
    ids = _seed_catalog(pg_session)
    # A healthy grounding row whose typed filter_members carry the column (the
    # committed-grounding arm of the dependency read).
    _seed_healthy_snippet(pg_session)

    kwargs: dict[str, object] = {
        "graphs": {"segment_cost": _graph()},
        "workspace_id": WS_ID,
        "vertical": "financial_reporting",
        "run_id": "om-idem",
        "catalogue_run_id": CAT_RUN,
        "semantic_runs": {ids["fact_id"]: GEN_RUN},
    }
    _persist_groundability_verdicts(pg_session, duckdb_conn, **kwargs)  # type: ignore[arg-type]
    _persist_groundability_verdicts(pg_session, duckdb_conn, **kwargs)  # type: ignore[arg-type]
    pg_session.commit()
    rows = (
        pg_session.query(DimensionGroundability)
        .filter(DimensionGroundability.run_id == "om-idem")
        .all()
    )
    assert len(rows) == 1, "re-delivery must upsert the (column_id, run_id) row, not duplicate"
    assert rows[0].column_id == ids["code_id"]
    assert rows[0].verdict == "ungroundable"

    # Fault isolation: a bug in the evaluator costs this run's annotation only —
    # unrelated pending work on the phase session survives.
    marker = SemanticAnnotation(
        column_id=ids["code_id"], run_id="marker-run", semantic_role="identifier"
    )
    pg_session.add(marker)
    with patch(
        "dataraum.graphs.groundability.evaluate_groundability",
        side_effect=RuntimeError("boom"),
    ):
        _persist_groundability_verdicts(
            pg_session,
            duckdb_conn,
            graphs={"segment_cost": _graph()},
            workspace_id=WS_ID,
            vertical="financial_reporting",
            run_id="om-faulty",
            catalogue_run_id=CAT_RUN,
            semantic_runs={ids["fact_id"]: GEN_RUN},
        )
    pg_session.commit()
    assert (
        pg_session.query(DimensionGroundability)
        .filter(DimensionGroundability.run_id == "om-faulty")
        .count()
        == 0
    )
    assert (
        pg_session.query(SemanticAnnotation)
        .filter(SemanticAnnotation.run_id == "marker-run")
        .count()
        == 1
    ), "the failed annotation must never roll back unrelated phase-session work"


def _mock_agent() -> GraphAgent:
    """A GraphAgent for LLM-free paths (cache assembly, direct method calls)."""
    return GraphAgent(config=MagicMock(), provider=MagicMock(), prompt_renderer=MagicMock())


def _fall_loud_generated_code() -> GeneratedCode:
    """The compliant refresh's code object for ``_graph()``'s single extract."""
    from datetime import UTC, datetime

    return GeneratedCode(
        code_id="c-fall-loud",
        graph_id="segment_cost",
        summary="fall-loud",
        steps=[
            {
                "step_id": "value",
                "sql": "SELECT NULL AS value",
                "description": "fall-loud",
                "parts": {
                    "select": [{"expr": "NULL", "alias": "value"}],
                    "from": [],
                    "where": [],
                },
            }
        ],
        final_sql="SELECT * FROM value",
        llm_model="m",
        prompt_hash="h",
        generated_at=datetime.now(UTC),
    )


def test_cached_healthy_row_with_firing_verdict_demotes(
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """Drift demotion through the REAL cache-assembly path (chosen over a
    seam-mocked unit test — the mocked variant would prove only the mock): a
    HEALTHY cached extract whose filter column carries a firing verdict NULLs,
    classifies UNGROUNDABLE_DIMENSION at the live seam, and is demoted with the
    typed provenance while keeping its parts (the skip_steps protection)."""
    duckdb_conn.execute(f"CREATE TABLE IF NOT EXISTS {VIEW} (entity_code VARCHAR, amount DOUBLE)")
    duckdb_conn.execute(f"INSERT INTO {VIEW} VALUES ('4000', NULL), ('5100', 10.0)")
    _seed_catalog(pg_session)
    _seed_healthy_snippet(pg_session)

    result = _mock_agent().execute(pg_session, _graph(), _context(duckdb_conn), workspace_id=WS_ID)
    assert not result.success
    assert "ungroundable_dimension" in result.error
    pg_session.flush()

    from dataraum.query.snippet_library import SnippetLibrary

    rec = SnippetLibrary(pg_session).retained_failure(
        snippet_type="extract",
        schema_mapping_id=WS_ID,
        standard_field="segment_cost",
        statement="income_statement",
        aggregation="sum",
        predicate="",
    )
    assert rec is not None, "the stale healthy row must be demoted, not served forever"
    prov = rec.provenance or {}
    assert prov["failure_mode"] == "verifier_rejected"
    assert prov["no_support_class"] == "ungroundable_dimension"
    assert "entity_code" in prov["no_support_evidence"]
    # Demotion RETAINS: parts survive the follow-up retained-failure write.
    assert rec.parts is not None
    assert rec.parts["where"] == ["entity_code = '4000'"]


def test_stale_carry_drops_when_the_verdict_clears(
    pg_session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """The DAT-620 re-evaluation gate: a compliant fall-loud refresh over an
    ungroundable row RE-CHECKS the verdict where the pinned reads are available
    — a since-linked reference drops the stale carry (the row becomes an
    unclassified failure, so the next authoring gets the generic steer and
    re-attempts) while the prior parts still survive the refresh."""
    ids = _seed_catalog(pg_session)
    _link_reference(pg_session, ids)  # the verdict no longer fires
    from dataraum.query.snippet_library import SnippetLibrary

    prior_parts = {
        "select": [{"expr": "SUM(amount)", "alias": "value"}],
        "from": [VIEW],
        "where": ["entity_code = '4000'"],
    }
    SnippetLibrary(pg_session, workspace_id=WS_ID).save_snippet(
        snippet_type="extract",
        sql=f"SELECT SUM(amount) AS value FROM {VIEW} WHERE entity_code = '4000'",
        description="prior ungroundable attempt",
        schema_mapping_id=WS_ID,
        source="graph:segment_cost",
        standard_field="segment_cost",
        statement="income_statement",
        aggregation="sum",
        provenance={
            "failure_mode": "verifier_rejected",
            "failure_reason": "no support (ungroundable_dimension)",
            "no_support_class": "ungroundable_dimension",
            "no_support_evidence": "SENTINEL_STALE_EVIDENCE",
        },
        parts=prior_parts,
        failed=True,
    )
    pg_session.flush()

    _mock_agent()._save_failed_snippet(
        pg_session,
        _graph(),
        _fall_loud_generated_code(),
        WS_ID,
        workspace_id=WS_ID,
        mode=SnippetFailureMode.VERIFIER_REJECTED,
        reason="no support: aggregated to NULL",
        no_support={},  # the fall-loud shape classified as nothing
        context=_context(duckdb_conn),
    )
    pg_session.flush()

    rec = SnippetLibrary(pg_session).retained_failure(
        snippet_type="extract",
        schema_mapping_id=WS_ID,
        standard_field="segment_cost",
        statement="income_statement",
        aggregation="sum",
        predicate="",
    )
    assert rec is not None
    prov = rec.provenance or {}
    assert prov.get("no_support_class") is None, "a cleared verdict must not be carried stale"
    assert "SENTINEL_STALE_EVIDENCE" not in (prov.get("no_support_evidence") or "")
    # The dependency identity still survives the refresh regardless of the
    # carry's fate — the parts preservation covers the whole sticky branch.
    assert rec.parts == prior_parts
