"""The surrogate_mint phase (DAT-277) — mint, reconcile, and abstain semantics.

Seeds the tenant-scoped fan-out shape (fact ``txn`` whose ``account`` recurs across
``business_id`` tenants, dim ``coa`` keyed on the composite) with real raw +
typed DuckDB tables, generation-head recipes, and a confirmed intent — then
drives the phase and asserts on the physical lake, the Column/profile rows,
and the persisted surrogate relationship. The worst-case contract is pinned
throughout: every abstain path leaves the working single-column world
untouched.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import duckdb
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.relationships.db_models import Relationship, SurrogateKeyIntent
from dataraum.analysis.statistics.db_models import StatisticalProfile
from dataraum.analysis.typing.db_models import MaterializationRecipe
from dataraum.core.connections import _LakeScopedConnection
from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases.surrogate_mint_phase import SurrogateMintPhase
from dataraum.storage import Column, Source, Table
from dataraum.storage.snapshot_head import GENERATION_STAGE, MetadataSnapshotHead

_GEN_RUN = "gen-run-1"
_RUN_1 = "session-run-1"
_RUN_2 = "session-run-2"

_TXN_DDL = (
    'CREATE OR REPLACE TABLE lake.typed."txn" AS '
    'SELECT TRY_CAST("account" AS VARCHAR) AS "account", '
    'TRY_CAST("business_id" AS VARCHAR) AS "business_id", '
    'TRY_CAST("amount" AS INTEGER) AS "amount" FROM lake.raw."txn"'
)
_COA_DDL = (
    'CREATE OR REPLACE TABLE lake.typed."coa" AS '
    'SELECT TRY_CAST("account_name" AS VARCHAR) AS "account_name", '
    'TRY_CAST("business_id" AS VARCHAR) AS "business_id", '
    'TRY_CAST("account_type" AS VARCHAR) AS "account_type" FROM lake.raw."coa"'
)


@pytest.fixture
def lake() -> Iterator[_LakeScopedConnection]:
    """The worker-connection shape: lake catalog + the cursor-USE reissue wrapper.

    The profiler opens derived cursors, and DuckDB cursors do NOT inherit
    ``USE`` — production scopes every cursor via ``_LakeScopedConnection``
    (core/connections.py), so the fixture must too or bare typed-table names
    resolve against the wrong catalog.
    """
    c = duckdb.connect()
    try:
        c.execute("ATTACH ':memory:' AS lake")
        c.execute("CREATE SCHEMA lake.raw")
        c.execute("CREATE SCHEMA lake.typed")
        c.execute("USE lake.typed")
        c.execute(
            'CREATE TABLE lake.raw."txn" (account VARCHAR, business_id VARCHAR, amount VARCHAR)'
        )
        c.execute(
            'INSERT INTO lake.raw."txn" VALUES '
            "('Sales','B1','10'),('Sales','B1','20'),('COGS','B1','5'),"
            "('Sales','B2','30'),('COGS','B2','7')"
        )
        c.execute(
            'CREATE TABLE lake.raw."coa" '
            "(account_name VARCHAR, business_id VARCHAR, account_type VARCHAR)"
        )
        c.execute(
            'INSERT INTO lake.raw."coa" VALUES '
            "('Sales','B1','Income'),('COGS','B1','Expense'),"
            "('Sales','B2','Income'),('COGS','B2','Expense')"
        )
        c.execute(_TXN_DDL)
        c.execute(_COA_DDL)
        yield _LakeScopedConnection(c, "lake.typed")
    finally:
        c.close()


def _seed(session: Session) -> dict[str, Any]:
    """Typed tables + columns + generation-head recipes for the multi-tenant bookkeeping shape."""
    src = Source(name="s", source_type="csv")
    session.add(src)
    session.flush()

    tables: dict[str, Table] = {}
    cols: dict[tuple[str, str], Column] = {}
    for name, ddl, col_names in (
        ("txn", _TXN_DDL, ["account", "business_id", "amount"]),
        ("coa", _COA_DDL, ["account_name", "business_id", "account_type"]),
    ):
        t = Table(
            source_id=src.source_id, table_name=name, layer="typed", duckdb_path=name, row_count=5
        )
        session.add(t)
        session.flush()
        tables[name] = t
        for pos, cn in enumerate(col_names):
            c = Column(
                table_id=t.table_id, column_name=cn, column_position=pos, resolved_type="VARCHAR"
            )
            session.add(c)
            cols[(name, cn)] = c
        session.flush()
        session.add(
            MaterializationRecipe(
                table_id=t.table_id,
                layer="typed",
                run_id=_GEN_RUN,
                target_fqn=f'lake.typed."{name}"',
                ddl=ddl,
                depends_on=[f'lake.raw."{name}"'],
            )
        )
        session.add(
            MetadataSnapshotHead(
                target=f"table:{t.table_id}",
                stage=GENERATION_STAGE,
                run_id=_GEN_RUN,
                promoted_at=datetime.now(UTC),
            )
        )
    session.flush()
    return {"tables": tables, "cols": cols}


def _intent(session: Session, seed: dict[str, Any], run_id: str) -> SurrogateKeyIntent:
    cols = seed["cols"]
    intent = SurrogateKeyIntent(
        run_id=run_id,
        intent_digest="digest-1",
        from_table_id=seed["tables"]["txn"].table_id,
        to_table_id=seed["tables"]["coa"].table_id,
        column_pairs=[
            [cols[("txn", "account")].column_id, cols[("coa", "account_name")].column_id],
            [cols[("txn", "business_id")].column_id, cols[("coa", "business_id")].column_id],
        ],
        cardinality="many-to-one",
        confidence=0.9,
        reasoning="account recurs per tenant",
    )
    session.add(intent)
    session.flush()
    return intent


def _ctx(session: Session, lake: duckdb.DuckDBPyConnection, seed: dict[str, Any], run_id: str):
    return PhaseContext(
        session=session,
        duckdb_conn=lake,
        table_ids=[t.table_id for t in seed["tables"].values()],
        run_id=run_id,
    )


def _sk_columns(lake: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    rows = lake.execute(f'DESCRIBE lake.typed."{table}"').fetchall()
    return [r[0] for r in rows if r[0].startswith("_sk__")]


def test_mint_end_to_end(session, lake) -> None:
    seed = _seed(session)
    _intent(session, seed, _RUN_1)

    result = SurrogateMintPhase().run(_ctx(session, lake, seed, _RUN_1))
    session.flush()

    assert result.status == PhaseStatus.COMPLETED
    assert result.warnings == []
    # Physical: both sides carry the deterministic surrogate.
    assert _sk_columns(lake, "txn") == ["_sk__account__business_id"]
    assert _sk_columns(lake, "coa") == ["_sk__account_name__business_id"]
    # The surrogate join holds grain: LEFT JOIN keeps exactly the fact rows.
    row = lake.execute(
        'SELECT COUNT(*) FROM lake.typed."txn" f '
        'LEFT JOIN lake.typed."coa" d ON f."_sk__account__business_id" = '
        'd."_sk__account_name__business_id"'
    ).fetchone()
    assert row is not None and row[0] == 5  # no fan-out (account alone would give 10)

    # Metadata: Column rows registered + profiled on the typed layer.
    sk_cols = (
        session.execute(select(Column).where(Column.column_name.like("_sk__%"))).scalars().all()
    )
    assert {c.column_name for c in sk_cols} == {
        "_sk__account__business_id",
        "_sk__account_name__business_id",
    }
    profiles = session.execute(select(StatisticalProfile)).scalars().all()
    assert {p.column_id for p in profiles} == {c.column_id for c in sk_cols}

    # Catalog: ONE ordinary single-column llm relationship on the surrogate pair.
    rels = (
        session.execute(select(Relationship).where(Relationship.detection_method == "llm"))
        .scalars()
        .all()
    )
    assert len(rels) == 1
    rel = rels[0]
    by_id = {c.column_id: c.column_name for c in sk_cols}
    assert by_id[rel.from_column_id] == "_sk__account__business_id"
    assert by_id[rel.to_column_id] == "_sk__account_name__business_id"
    assert rel.cardinality == "many-to-one"
    assert rel.evidence["introduces_duplicates"] is False
    assert rel.evidence["coverage"] == 1.0  # every fact row joins in this fixture (DAT-695)
    assert rel.evidence["surrogate"]["natural_pairs"] == [
        ["account", "account_name"],
        ["business_id", "business_id"],
    ]
    # The amended DDL is stored on the recipe substrate under this run.
    recipes = (
        session.execute(select(MaterializationRecipe).where(MaterializationRecipe.run_id == _RUN_1))
        .scalars()
        .all()
    )
    assert len(recipes) == 2


def test_dim_to_fact_intent_persists_fk_side_first(session, lake) -> None:
    """The LLM may confirm the composite dim→fact (one-to-many) — seen live on
    the bookkeeping smoke corpus, where all four clean composites arrived in that orientation and
    the enrichment grain-safe marker (which reads the STORED direction) then
    refused every join. The mint must orient by measured cardinality: persist
    fact→dim many-to-one with flipped provenance.
    """
    seed = _seed(session)
    cols = seed["cols"]
    session.add(
        SurrogateKeyIntent(
            run_id=_RUN_1,
            intent_digest="digest-rev",
            from_table_id=seed["tables"]["coa"].table_id,  # dim side as FROM
            to_table_id=seed["tables"]["txn"].table_id,
            column_pairs=[
                [cols[("coa", "account_name")].column_id, cols[("txn", "account")].column_id],
                [cols[("coa", "business_id")].column_id, cols[("txn", "business_id")].column_id],
            ],
            cardinality="one-to-many",
            confidence=0.85,
            reasoning="confirmed dim-first",
        )
    )
    session.flush()

    result = SurrogateMintPhase().run(_ctx(session, lake, seed, _RUN_1))
    session.flush()

    assert result.status == PhaseStatus.COMPLETED
    rel = session.execute(
        select(Relationship).where(Relationship.detection_method == "llm")
    ).scalar_one()
    names = {
        c.column_id: (c.table_id, c.column_name)
        for c in session.execute(select(Column).where(Column.column_name.like("_sk__%"))).scalars()
    }
    # FK side first: fact table's surrogate is FROM, cardinality many-to-one.
    assert names[rel.from_column_id] == (
        seed["tables"]["txn"].table_id,
        "_sk__account__business_id",
    )
    assert names[rel.to_column_id] == (
        seed["tables"]["coa"].table_id,
        "_sk__account_name__business_id",
    )
    assert rel.cardinality == "many-to-one"
    assert rel.confirmation_source == "judge"  # a minted composite is judge-confirmed (DAT-776)
    assert rel.evidence["introduces_duplicates"] is False  # measured in the stored direction
    assert rel.evidence["surrogate"]["natural_pairs"] == [
        ["account", "account_name"],
        ["business_id", "business_id"],
    ]  # provenance flipped WITH the orientation


def test_noop_without_intents(session, lake) -> None:
    seed = _seed(session)

    result = SurrogateMintPhase().run(_ctx(session, lake, seed, _RUN_1))
    session.flush()

    assert result.status == PhaseStatus.COMPLETED
    assert _sk_columns(lake, "txn") == []
    assert session.execute(select(Relationship)).scalars().all() == []


def test_retry_is_idempotent(session, lake) -> None:
    """A Temporal at-least-once retry (same run) re-derives the identical state."""
    seed = _seed(session)
    _intent(session, seed, _RUN_1)

    phase = SurrogateMintPhase()
    assert phase.run(_ctx(session, lake, seed, _RUN_1)).status == PhaseStatus.COMPLETED
    session.flush()
    first_ids = {
        c.column_name: c.column_id
        for c in session.execute(select(Column).where(Column.column_name.like("_sk__%"))).scalars()
    }
    assert phase.run(_ctx(session, lake, seed, _RUN_1)).status == PhaseStatus.COMPLETED
    session.flush()

    second_ids = {
        c.column_name: c.column_id
        for c in session.execute(select(Column).where(Column.column_name.like("_sk__%"))).scalars()
    }
    assert second_ids == first_ids  # stable column_id — overlays keyed on it survive
    rels = session.execute(select(Relationship)).scalars().all()
    assert len(rels) == 1


def test_unconfirmed_unkept_surrogate_is_dropped(session, lake) -> None:
    """No fresh intent, nothing promoted/kept → reconcile removes the surrogate."""
    seed = _seed(session)
    _intent(session, seed, _RUN_1)
    phase = SurrogateMintPhase()
    assert phase.run(_ctx(session, lake, seed, _RUN_1)).status == PhaseStatus.COMPLETED
    session.flush()
    assert _sk_columns(lake, "txn") == ["_sk__account__business_id"]

    # Next session run: the LLM did not re-confirm (no intent) and the catalog
    # head was never promoted to run 1 — the grace window is empty.
    assert phase.run(_ctx(session, lake, seed, _RUN_2)).status == PhaseStatus.COMPLETED
    session.flush()

    assert _sk_columns(lake, "txn") == []
    assert _sk_columns(lake, "coa") == []
    assert (
        session.execute(select(Column).where(Column.column_name.like("_sk__%"))).scalars().all()
        == []
    )


def test_promoted_surrogate_survives_the_grace_window(session, lake) -> None:
    """A promoted-run surrogate is kept for the keeper lift-up (DAT-409)."""
    seed = _seed(session)
    _intent(session, seed, _RUN_1)
    phase = SurrogateMintPhase()
    assert phase.run(_ctx(session, lake, seed, _RUN_1)).status == PhaseStatus.COMPLETED
    session.flush()
    session.add(
        MetadataSnapshotHead(
            target="catalog", stage="catalog", run_id=_RUN_1, promoted_at=datetime.now(UTC)
        )
    )
    session.flush()

    assert phase.run(_ctx(session, lake, seed, _RUN_2)).status == PhaseStatus.COMPLETED
    session.flush()

    # Physical + metadata survive; session_write_keepers can lift the promoted
    # relationship into a keep overlay with its columns intact.
    assert _sk_columns(lake, "txn") == ["_sk__account__business_id"]
    assert _sk_columns(lake, "coa") == ["_sk__account_name__business_id"]
    assert (
        len(
            session.execute(select(Column).where(Column.column_name.like("_sk__%"))).scalars().all()
        )
        == 2
    )


def test_divergent_component_types_abstain(session, lake) -> None:
    """The rescue was measured with NATIVE comparison; the hash compares VARCHAR
    renderings. Divergent resolved types ('007' vs 7) would make the minted join
    weaker than its proof — abstain, never ship a silently-orphaning join.
    """
    seed = _seed(session)
    _intent(session, seed, _RUN_1)
    seed["cols"][("coa", "business_id")].resolved_type = "BIGINT"
    session.flush()

    result = SurrogateMintPhase().run(_ctx(session, lake, seed, _RUN_1))
    session.flush()

    assert result.status == PhaseStatus.COMPLETED
    assert any("type mismatch" in w for w in result.warnings)
    assert _sk_columns(lake, "txn") == []
    assert _sk_columns(lake, "coa") == []
    assert session.execute(select(Relationship)).scalars().all() == []


def test_float_typed_component_is_refused_even_when_types_match(session, lake) -> None:
    """Same-type is not sufficient for floats: -0.0 = 0.0 natively but renders
    '-0.0' vs '0.0' — a both-sides-DOUBLE component must abstain."""
    seed = _seed(session)
    _intent(session, seed, _RUN_1)
    seed["cols"][("txn", "business_id")].resolved_type = "DOUBLE"
    seed["cols"][("coa", "business_id")].resolved_type = "DOUBLE"
    session.flush()

    result = SurrogateMintPhase().run(_ctx(session, lake, seed, _RUN_1))
    session.flush()

    assert result.status == PhaseStatus.COMPLETED
    assert any("float-typed" in w for w in result.warnings)
    assert _sk_columns(lake, "txn") == []
    assert session.execute(select(Relationship)).scalars().all() == []


def test_fresh_intent_on_a_frozen_table_defers_with_named_cause(session, lake) -> None:
    """A mint job whose endpoint is frozen must defer (named warning), never
    half-run against a table reconcile skipped."""
    seed = _seed(session)
    _intent(session, seed, _RUN_1)
    phase = SurrogateMintPhase()
    assert phase.run(_ctx(session, lake, seed, _RUN_1)).status == PhaseStatus.COMPLETED
    session.flush()
    minted = session.execute(
        select(Relationship).where(Relationship.detection_method == "llm")
    ).scalar_one()
    # Run 2 carries BOTH a keeper with unrecoverable provenance (→ freeze) and a
    # fresh intent on the same tables.
    session.add(
        Relationship(
            run_id=_RUN_2,
            from_table_id=minted.from_table_id,
            from_column_id=minted.from_column_id,
            to_table_id=minted.to_table_id,
            to_column_id=minted.to_column_id,
            relationship_type="foreign_key",
            cardinality=minted.cardinality,
            confidence=1.0,
            detection_method="keeper",
            evidence={"source": "config_overlay", "action": "keep"},
        )
    )
    session.delete(minted)
    session.flush()
    _intent(session, seed, _RUN_2)

    result = phase.run(_ctx(session, lake, seed, _RUN_2))
    session.flush()

    assert result.status == PhaseStatus.COMPLETED
    assert any("frozen this run" in w and "mint deferred" in w for w in result.warnings)
    # No run-2 llm row was persisted; the frozen state survived untouched.
    assert (
        session.execute(
            select(Relationship).where(
                Relationship.run_id == _RUN_2, Relationship.detection_method == "llm"
            )
        )
        .scalars()
        .all()
        == []
    )
    assert _sk_columns(lake, "txn") == ["_sk__account__business_id"]


def test_keeper_row_recovers_provenance_from_the_prior_mint(session, lake) -> None:
    """The steady-state silent-accept path: an overlay-materialized keeper row
    carries NO surrogate evidence ({'source': 'config_overlay'}) — the mint must
    recover the natural pairs from the ORIGINAL mint's llm row and keep the
    columns intact.
    """
    seed = _seed(session)
    _intent(session, seed, _RUN_1)
    phase = SurrogateMintPhase()
    assert phase.run(_ctx(session, lake, seed, _RUN_1)).status == PhaseStatus.COMPLETED
    session.flush()
    minted = session.execute(
        select(Relationship).where(Relationship.detection_method == "llm")
    ).scalar_one()

    # Run 3 shape (DAT-409): session_materialize_overlays already wrote the
    # keeper for THIS run, evidence is the overlay stamp only. No intent, no
    # promoted head — the keeper row is the only thing keeping the surrogate.
    session.add(
        Relationship(
            run_id=_RUN_2,
            from_table_id=minted.from_table_id,
            from_column_id=minted.from_column_id,
            to_table_id=minted.to_table_id,
            to_column_id=minted.to_column_id,
            relationship_type="foreign_key",
            cardinality=minted.cardinality,
            confidence=1.0,
            detection_method="keeper",
            evidence={"source": "config_overlay", "action": "keep"},
        )
    )
    session.flush()

    result = phase.run(_ctx(session, lake, seed, _RUN_2))
    session.flush()

    assert result.status == PhaseStatus.COMPLETED
    assert result.warnings == []  # provenance recovered — no freeze, no drop
    assert _sk_columns(lake, "txn") == ["_sk__account__business_id"]
    assert _sk_columns(lake, "coa") == ["_sk__account_name__business_id"]
    kept_cols = (
        session.execute(select(Column).where(Column.column_name.like("_sk__%"))).scalars().all()
    )
    assert {c.column_id for c in kept_cols} == {minted.from_column_id, minted.to_column_id}


def test_unrecoverable_keeper_provenance_freezes_the_tables(session, lake) -> None:
    """The defensive tail: a kept surrogate whose provenance is gone must freeze
    its tables — reconcile would otherwise delete the still-referenced column
    AND (via the dependents cascade) the keeper row itself.
    """
    seed = _seed(session)
    _intent(session, seed, _RUN_1)
    phase = SurrogateMintPhase()
    assert phase.run(_ctx(session, lake, seed, _RUN_1)).status == PhaseStatus.COMPLETED
    session.flush()
    minted = session.execute(
        select(Relationship).where(Relationship.detection_method == "llm")
    ).scalar_one()
    keeper = Relationship(
        run_id=_RUN_2,
        from_table_id=minted.from_table_id,
        from_column_id=minted.from_column_id,
        to_table_id=minted.to_table_id,
        to_column_id=minted.to_column_id,
        relationship_type="foreign_key",
        cardinality=minted.cardinality,
        confidence=1.0,
        detection_method="keeper",
        evidence={"source": "config_overlay", "action": "keep"},
    )
    session.add(keeper)
    # Erase the original mint's provenance (should never happen — pinned here so
    # the do-no-harm branch is proven, not assumed).
    session.delete(minted)
    session.flush()

    result = phase.run(_ctx(session, lake, seed, _RUN_2))
    session.flush()

    assert result.status == PhaseStatus.COMPLETED
    assert any("freezing" in w for w in result.warnings)
    # Physical, metadata, and the keeper row all survive untouched.
    assert _sk_columns(lake, "txn") == ["_sk__account__business_id"]
    assert _sk_columns(lake, "coa") == ["_sk__account_name__business_id"]
    assert (
        len(
            session.execute(select(Column).where(Column.column_name.like("_sk__%"))).scalars().all()
        )
        == 2
    )
    assert (
        session.execute(
            select(Relationship).where(Relationship.detection_method == "keeper")
        ).scalar_one()
        is not None
    )


def test_transient_commit_conflict_fails_retryable_not_abstain(session, lake) -> None:
    """A DuckLake commit race must surface as a FAILED phase carrying the
    'Transaction conflict' text (the DAT-641 retry classifier's signal), never
    be swallowed as an abstain — the run would silently lose its rescue.
    """
    seed = _seed(session)
    _intent(session, seed, _RUN_1)

    class _RacingConn:
        """Delegates everything; the amended CREATE loses the commit race."""

        def __init__(self, inner: object) -> None:
            self._inner = inner

        def execute(self, sql: str, *args: object) -> object:
            if sql.startswith("CREATE OR REPLACE TABLE"):
                raise duckdb.TransactionException(
                    "Failed to commit DuckLake transaction: Transaction conflict!"
                )
            return self._inner.execute(sql, *args)  # type: ignore[attr-defined]

        def __getattr__(self, name: str) -> object:
            return getattr(self._inner, name)

    ctx = _ctx(session, lake, seed, _RUN_1)
    ctx.duckdb_conn = _RacingConn(lake)  # type: ignore[assignment]
    result = SurrogateMintPhase().run(ctx)

    assert result.status == PhaseStatus.FAILED
    assert result.error is not None and "Transaction conflict" in result.error


def test_missing_typing_recipe_abstains(session, lake) -> None:
    seed = _seed(session)
    _intent(session, seed, _RUN_1)
    session.execute(
        MetadataSnapshotHead.__table__.delete().where(
            MetadataSnapshotHead.stage == GENERATION_STAGE
        )
    )
    session.flush()

    result = SurrogateMintPhase().run(_ctx(session, lake, seed, _RUN_1))
    session.flush()

    assert result.status == PhaseStatus.COMPLETED  # abstain, never fail the spine
    assert any("no typing recipe" in w for w in result.warnings)
    assert _sk_columns(lake, "txn") == []
    assert session.execute(select(Relationship)).scalars().all() == []


def test_semantic_to_mint_to_enriched_join_holds_grain(session, lake) -> None:
    """The full handoff: LLM confirms → intent → mint → the UNTOUCHED single-column
    builder joins on the surrogate and the view holds exact fact grain.

    This is the DAT-277 payoff pinned end-to-end: ``account`` alone fans the
    5-row fact out to 9; the surrogate join returns exactly 5.
    """
    from unittest.mock import MagicMock

    from dataraum.analysis.semantic.models import (
        Relationship as SemanticRelationship,
    )
    from dataraum.analysis.semantic.models import (
        SemanticEnrichmentResult,
    )
    from dataraum.analysis.semantic.processor import synthesize_and_store_tables
    from dataraum.analysis.views.builder import DimensionJoin, build_enriched_view_sql
    from dataraum.core.models.base import RelationshipType, Result

    seed = _seed(session)
    agent = MagicMock()
    agent.provider.get_model_for_tier = MagicMock(return_value="test-model")
    agent.synthesize_tables = MagicMock(
        return_value=Result.ok(
            SemanticEnrichmentResult(
                annotations=[],
                entity_detections=[],
                relationships=[
                    SemanticRelationship(
                        relationship_id="rel-1",
                        from_table="txn",
                        from_column="account",
                        to_table="coa",
                        to_column="account_name",
                        key_columns=[("business_id", "business_id")],
                        relationship_type=RelationshipType.FOREIGN_KEY,
                        confidence=0.9,
                        detection_method="llm_tool",
                        evidence={"source": "table_synthesis", "reasoning": "composite"},
                    )
                ],
            )
        )
    )
    table_ids = [t.table_id for t in seed["tables"].values()]
    assert synthesize_and_store_tables(
        session, agent, table_ids, duckdb_conn=lake, run_id=_RUN_1
    ).success
    session.flush()

    assert SurrogateMintPhase().run(_ctx(session, lake, seed, _RUN_1)).status == (
        PhaseStatus.COMPLETED
    )
    session.flush()

    rel = session.execute(
        select(Relationship).where(Relationship.detection_method == "llm")
    ).scalar_one()
    sql, dim_cols = build_enriched_view_sql(
        'lake.typed."enriched_txn"',
        'lake.typed."txn"',
        [
            DimensionJoin(
                dim_table_name="coa",
                dim_duckdb_path='lake.typed."coa"',
                fact_fk_column="_sk__account__business_id",
                dim_pk_column="_sk__account_name__business_id",
                include_columns=["account_type"],
                relationship_id=rel.relationship_id,
            )
        ],
    )
    lake.execute(sql)
    grain = lake.execute('SELECT COUNT(*) FROM lake.typed."enriched_txn"').fetchone()
    assert grain is not None and grain[0] == 5  # exact fact grain — the rescue
    fanout = lake.execute(
        'SELECT COUNT(*) FROM lake.typed."txn" f '
        'LEFT JOIN lake.typed."coa" d ON f."account" = d."account_name"'
    ).fetchone()
    assert fanout is not None and fanout[0] == 10  # what the naive join would do
    assert [c.name for c in dim_cols] == ["_sk__account__business_id__account_type"]
    # The enriched view serves the correct discriminator, grain-safe.
    typed = lake.execute(
        'SELECT DISTINCT "_sk__account__business_id__account_type" '
        'FROM lake.typed."enriched_txn" WHERE "account" = \'COGS\''
    ).fetchall()
    assert typed == [("Expense",)]


def test_vanished_component_abstains(session, lake) -> None:
    """A component column missing from the physical table abstains per-intent."""
    seed = _seed(session)
    _intent(session, seed, _RUN_1)
    lake.execute('ALTER TABLE lake.typed."txn" DROP COLUMN "business_id"')

    result = SurrogateMintPhase().run(_ctx(session, lake, seed, _RUN_1))
    session.flush()

    assert result.status == PhaseStatus.COMPLETED
    assert any("missing" in w for w in result.warnings)
    assert _sk_columns(lake, "coa") == []  # the pair aborts as a unit: no half-mint
    assert session.execute(select(Relationship)).scalars().all() == []
