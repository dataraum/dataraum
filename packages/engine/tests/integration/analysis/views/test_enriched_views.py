"""Integration tests for enriched views phase."""

from __future__ import annotations

import duckdb
import pytest

from dataraum.analysis.views.builder import DimensionJoin, build_enriched_view_sql
from dataraum.pipeline.phases.enriched_views_phase import EnrichedViewsPhase


class TestEnrichedViewsIntegration:
    """Integration tests for enriched views with DuckDB."""

    @pytest.fixture
    def duckdb_conn(self):
        conn = duckdb.connect(":memory:")
        yield conn
        conn.close()

    def test_view_creation_preserves_grain(self, duckdb_conn):
        """Test that enriched view preserves fact table row count."""
        # Create fact table
        duckdb_conn.execute("""
            CREATE TABLE typed_orders (
                order_id INTEGER,
                customer_id INTEGER,
                amount DOUBLE,
                order_date DATE
            )
        """)
        duckdb_conn.execute("""
            INSERT INTO typed_orders VALUES
                (1, 10, 100.0, '2024-01-01'),
                (2, 20, 200.0, '2024-01-02'),
                (3, 10, 150.0, '2024-01-03')
        """)

        # Create dimension table
        duckdb_conn.execute("""
            CREATE TABLE typed_customers (
                id INTEGER,
                name VARCHAR,
                country VARCHAR
            )
        """)
        duckdb_conn.execute("""
            INSERT INTO typed_customers VALUES
                (10, 'Alice', 'US'),
                (20, 'Bob', 'UK')
        """)

        # Build and execute view
        joins = [
            DimensionJoin(
                dim_table_name="customers",
                dim_duckdb_path="typed_customers",
                fact_fk_column="customer_id",
                dim_pk_column="id",
                include_columns=["name", "country"],
                relationship_id="rel-1",
            )
        ]

        view = '"enriched_orders"'
        sql, dim_cols = build_enriched_view_sql(view, "typed_orders", joins)

        duckdb_conn.execute(sql)

        # Verify grain preserved (3 fact rows)
        result = duckdb_conn.execute(f"SELECT COUNT(*) FROM {view}").fetchone()
        assert result[0] == 3

        # Verify dimension columns present
        result = duckdb_conn.execute(f"SELECT * FROM {view} ORDER BY order_id").fetchall()
        assert result[0][4] == "Alice"  # customers__name
        assert result[0][5] == "US"  # customers__country
        assert result[1][4] == "Bob"  # customers__name

    def test_view_with_no_match_uses_null(self, duckdb_conn):
        """Test that LEFT JOIN produces NULLs for unmatched rows."""
        duckdb_conn.execute("""
            CREATE TABLE typed_orders (
                order_id INTEGER,
                customer_id INTEGER,
                amount DOUBLE
            )
        """)
        duckdb_conn.execute("""
            INSERT INTO typed_orders VALUES
                (1, 10, 100.0),
                (2, 99, 200.0)
        """)

        duckdb_conn.execute("""
            CREATE TABLE typed_customers (
                id INTEGER,
                name VARCHAR
            )
        """)
        duckdb_conn.execute("INSERT INTO typed_customers VALUES (10, 'Alice')")

        joins = [
            DimensionJoin(
                dim_table_name="customers",
                dim_duckdb_path="typed_customers",
                fact_fk_column="customer_id",
                dim_pk_column="id",
                include_columns=["name"],
            )
        ]

        sql, _ = build_enriched_view_sql('"enriched_orders"', "typed_orders", joins)

        duckdb_conn.execute(sql)

        # Grain preserved (2 rows)
        assert duckdb_conn.execute('SELECT COUNT(*) FROM "enriched_orders"').fetchone()[0] == 2

        # Unmatched customer_id=99 gets NULL
        # Column is named {fact_fk_column}__{dim_col} = customer_id__name
        result = duckdb_conn.execute(
            'SELECT "customer_id__name" FROM "enriched_orders" WHERE customer_id = 99'
        ).fetchone()
        assert result[0] is None


class TestEnrichedViewsPhaseProperties:
    """Tests for EnrichedViewsPhase static properties.

    The phase is source-free (DAT-415): ``should_skip`` scopes by the session's
    ``ctx.table_ids``, never ``source_id``.
    """

    def test_skip_when_selection_empty(self, session):
        """Skip when the session selection carries no tables."""
        from dataraum.pipeline.base import PhaseContext

        ctx = PhaseContext(session=session, duckdb_conn=None, table_ids=[])

        reason = EnrichedViewsPhase().should_skip(ctx)
        assert reason == "No tables in session selection"

    def test_skip_when_no_fact_tables(self, session):
        """Skip when the selection has typed tables but none classified as a fact."""
        from uuid import uuid4

        from dataraum.pipeline.base import PhaseContext
        from dataraum.storage import Source, Table

        source = Source(name="test", source_type="csv")
        session.add(source)
        session.flush()
        table = Table(
            table_id=str(uuid4()),
            source_id=source.source_id,
            table_name="orders",
            layer="typed",
            duckdb_path="test__orders",
        )
        session.add(table)
        session.flush()

        ctx = PhaseContext(session=session, duckdb_conn=None, table_ids=[table.table_id])

        # No TableEntity marked is_fact_table → nothing to enrich.
        reason = EnrichedViewsPhase().should_skip(ctx)
        assert reason == "No fact tables identified"


class TestEnrichedViewsPhaseDuckLake:
    """Drive the phase end-to-end against the real DuckLake (DAT-415).

    Proves the source-free phase composes the right FQNs, materializes a real
    ``lake.typed`` view, versions its ``CREATE VIEW`` DDL on the recipe
    substrate, and writes a latest-only ``EnrichedView`` (one row per fact,
    DB-enforced) driven by THIS run's run-scoped ``TableEntity`` — idempotent on
    a same-run retry. The enrichment LLM is stubbed (a real call would be e2e);
    everything below the recommendation is exercised.
    """

    @staticmethod
    def _seed(session, duckdb_conn):
        """Materialize a typed fact + dimension in lake.typed + their metadata.

        Does NOT write the ``TableEntity`` — that is run-versioned, so each run
        seeds its own via ``_seed_fact_entity`` (mirroring how ``semantic_per_table``
        re-detects entities per run). Returns
        ``(fact_table_id, dim_table_id, canned_recommendations)``.
        """
        from uuid import uuid4

        from dataraum.analysis.views.builder import DimensionJoin
        from dataraum.analysis.views.enrichment_models import (
            EnrichmentAnalysisResult,
            EnrichmentRecommendation,
        )
        from dataraum.storage import Column, Source, Table

        # Physical typed tables in the lake (the phase joins these into a view).
        duckdb_conn.execute(
            'CREATE OR REPLACE TABLE lake.typed."csv__orders" AS '
            "SELECT * FROM (VALUES (1, 10, 100.0), (2, 20, 200.0), (3, 10, 150.0)) "
            "AS t(order_id, customer_id, amount)"
        )
        duckdb_conn.execute(
            'CREATE OR REPLACE TABLE lake.typed."csv__customers" AS '
            "SELECT * FROM (VALUES (10, 'Alice', 'US'), (20, 'Bob', 'UK')) "
            "AS t(id, name, country)"
        )

        source = Source(source_id=str(uuid4()), name="csv", source_type="csv")
        session.add(source)
        session.flush()

        fact = Table(
            table_id=str(uuid4()),
            source_id=source.source_id,
            table_name="orders",
            layer="typed",
            duckdb_path="csv__orders",
            row_count=3,
        )
        dim = Table(
            table_id=str(uuid4()),
            source_id=source.source_id,
            table_name="customers",
            layer="typed",
            duckdb_path="csv__customers",
            row_count=2,
        )
        session.add_all([fact, dim])
        session.flush()

        for tbl, names in (
            (fact, ("order_id", "customer_id", "amount")),
            (dim, ("id", "name", "country")),
        ):
            for pos, name in enumerate(names):
                session.add(
                    Column(
                        column_id=str(uuid4()),
                        table_id=tbl.table_id,
                        column_name=name,
                        column_position=pos,
                        raw_type="VARCHAR",
                        resolved_type="VARCHAR",
                    )
                )

        session.flush()

        canned = EnrichmentAnalysisResult(
            recommendations=[
                EnrichmentRecommendation(
                    fact_table_id=fact.table_id,
                    fact_table_name="orders",
                    dimension_joins=[
                        DimensionJoin(
                            dim_table_name="customers",
                            dim_duckdb_path="(rewritten by the phase to the dim FQN)",
                            fact_fk_column="customer_id",
                            dim_pk_column="id",
                            include_columns=["name", "country"],
                            relationship_id="rel-1",
                        )
                    ],
                    dimension_type="reference",
                    confidence=0.9,
                    reasoning="customers names/regions enrich orders",
                    enrichment_columns=["name", "country"],
                )
            ],
            summary="stub",
            model_name="stub-model",
        )
        return fact.table_id, dim.table_id, canned

    @staticmethod
    def _seed_fact_entity(session, *, table_id: str, run_id: str) -> None:
        """Write a run-scoped fact ``TableEntity`` for ``run_id``.

        Mirrors ``semantic_per_table``'s run-scoped delete-then-insert
        (``processor.synthesize_and_store_tables``) so a same-run retry is
        idempotent and prior runs' entities coexist — the conditions under which
        the unscoped fact query used to over-iterate.
        """
        from uuid import uuid4

        from sqlalchemy import delete

        from dataraum.analysis.semantic.db_models import TableEntity
        from tests.conftest import baseline_session_id

        session.execute(
            delete(TableEntity).where(
                TableEntity.table_id == table_id, TableEntity.run_id == run_id
            )
        )
        session.add(
            TableEntity(
                entity_id=str(uuid4()),
                session_id=baseline_session_id(),
                table_id=table_id,
                run_id=run_id,
                detected_entity_type="fact",
                is_fact_table=True,
            )
        )
        session.flush()

    def test_phase_materializes_versioned_view(self, session, duckdb_conn, monkeypatch):
        from sqlalchemy import select

        from dataraum.analysis.typing.db_models import MaterializationRecipe
        from dataraum.analysis.views.db_models import EnrichedView
        from dataraum.pipeline.base import PhaseContext, PhaseStatus
        from dataraum.storage import Table
        from tests.conftest import baseline_session_id

        fact_id, dim_id, canned = self._seed(session, duckdb_conn)
        rec = {"current": canned}
        monkeypatch.setattr(
            EnrichedViewsPhase, "_get_llm_recommendations", lambda self, **kw: rec["current"]
        )

        def run(run_id: str) -> None:
            # Each run re-detects its entities (run-scoped), as semantic_per_table
            # does in begin_session — prior runs' rows coexist.
            self._seed_fact_entity(session, table_id=fact_id, run_id=run_id)
            ctx = PhaseContext(
                session=session,
                duckdb_conn=duckdb_conn,
                table_ids=[fact_id, dim_id],
                session_id=baseline_session_id(),
                run_id=run_id,
            )
            result = EnrichedViewsPhase().run(ctx)
            assert result.status == PhaseStatus.COMPLETED, result.error
            session.flush()

        def enriched_views():
            return (
                session.execute(select(EnrichedView).where(EnrichedView.fact_table_id == fact_id))
                .scalars()
                .all()
            )

        def enriched_recipes():
            return (
                session.execute(
                    select(MaterializationRecipe).where(
                        MaterializationRecipe.table_id == fact_id,
                        MaterializationRecipe.layer == "enriched",
                    )
                )
                .scalars()
                .all()
            )

        # --- Run 1: the view materializes + is versioned -------------------
        run("run-1")

        # Physical view exists in lake.typed, grain-preserved, dim columns present.
        rows = duckdb_conn.execute(
            'SELECT order_id, "customer_id__name", "customer_id__country" '
            'FROM lake.typed."enriched_csv__orders" ORDER BY order_id'
        ).fetchall()
        assert rows == [(1, "Alice", "US"), (2, "Bob", "UK"), (3, "Alice", "US")]

        views = enriched_views()
        assert len(views) == 1
        assert views[0].run_id == "run-1"
        assert views[0].is_grain_verified is True
        assert views[0].view_table_id is not None
        assert "view_sql" not in EnrichedView.__table__.columns

        recipe = session.execute(
            select(MaterializationRecipe).where(
                MaterializationRecipe.table_id == fact_id,
                MaterializationRecipe.layer == "enriched",
                MaterializationRecipe.run_id == "run-1",
            )
        ).scalar_one()
        assert recipe.target_fqn == 'lake.typed."enriched_csv__orders"'
        assert "CREATE OR REPLACE VIEW" in recipe.ddl
        assert 'lake.typed."csv__orders"' in (recipe.depends_on or [])
        assert 'lake.typed."csv__customers"' in (recipe.depends_on or [])

        enriched_tables = (
            session.execute(select(Table).where(Table.layer == "enriched")).scalars().all()
        )
        assert len(enriched_tables) == 1

        # --- Run 1 retry: same run_id is idempotent (no duplicate rows) ----
        run("run-1")
        assert len(enriched_views()) == 1, "a same-run retry must not duplicate the view definition"
        assert {r.run_id for r in enriched_recipes()} == {"run-1"}

        # --- Run 2 (identical SQL): definition is latest-only, recipe gated -
        run("run-2")
        views = enriched_views()
        assert len(views) == 1, "EnrichedView is latest-only — one row per fact"
        assert views[0].run_id == "run-2", "the latest run stamps the (reconciled) definition"
        assert {r.run_id for r in enriched_recipes()} == {"run-1"}, (
            "unchanged canonical SQL adds no new recipe version (sqlglot-gated)"
        )

        # --- Run 3 (changed joins): a new recipe version is stamped --------
        from dataraum.analysis.views.builder import DimensionJoin
        from dataraum.analysis.views.enrichment_models import (
            EnrichmentAnalysisResult,
            EnrichmentRecommendation,
        )

        rec["current"] = EnrichmentAnalysisResult(
            recommendations=[
                EnrichmentRecommendation(
                    fact_table_id=fact_id,
                    fact_table_name="orders",
                    dimension_joins=[
                        DimensionJoin(
                            dim_table_name="customers",
                            dim_duckdb_path="(rewritten)",
                            fact_fk_column="customer_id",
                            dim_pk_column="id",
                            include_columns=["name"],  # dropped "country" → DDL changes
                            relationship_id="rel-1",
                        )
                    ],
                    dimension_type="reference",
                    confidence=0.9,
                    reasoning="narrower enrichment",
                    enrichment_columns=["name"],
                )
            ],
            summary="stub",
            model_name="stub-model",
        )
        run("run-3")
        views = enriched_views()
        assert len(views) == 1 and views[0].run_id == "run-3"
        assert {r.run_id for r in enriched_recipes()} == {"run-1", "run-3"}, (
            "a changed view definition stamps a new recipe version"
        )
        # Substrate stays latest-only across every run.
        assert (
            len(session.execute(select(Table).where(Table.layer == "enriched")).scalars().all())
            == 1
        )

    def test_run_scoped_fact_query_ignores_prior_runs(self, session, duckdb_conn, monkeypatch):
        """Coexisting prior-run fact entities must not multiply EnrichedViews.

        Regression for the ``dimension_coverage`` ``MultipleResultsFound`` bug:
        after N begin_session runs there are N coexisting ``TableEntity`` rows for
        the same fact (DAT-408/413, run-versioned). The phase must enrich only
        THIS run's entity — one fact processed, one ``EnrichedView`` — so the
        ``scalar_one_or_none`` reader in ``dimension_coverage`` resolves cleanly.

        The ``records_processed`` assertion is what fails without the run-scoping
        fix even under the test session's ``autoflush=True`` (which would otherwise
        mask the duplicate-row symptom that bites production's ``autoflush=False``).
        """
        from sqlalchemy import select

        from dataraum.analysis.views.db_models import EnrichedView
        from dataraum.pipeline.base import PhaseContext, PhaseStatus
        from tests.conftest import baseline_session_id

        fact_id, dim_id, canned = self._seed(session, duckdb_conn)
        monkeypatch.setattr(
            EnrichedViewsPhase, "_get_llm_recommendations", lambda self, **kw: canned
        )

        # Two coexisting run-scoped fact entities — as if begin_session ran twice.
        self._seed_fact_entity(session, table_id=fact_id, run_id="run-1")
        self._seed_fact_entity(session, table_id=fact_id, run_id="run-2")

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            table_ids=[fact_id, dim_id],
            session_id=baseline_session_id(),
            run_id="run-2",
        )
        result = EnrichedViewsPhase().run(ctx)
        assert result.status == PhaseStatus.COMPLETED, result.error

        # Only THIS run's fact was processed (an unscoped read would see 2).
        assert result.records_processed == 1
        assert result.outputs["fact_tables"] == 1

        # The dimension_coverage reader contract resolves to exactly one row.
        view = session.execute(
            select(EnrichedView).where(EnrichedView.fact_table_id == fact_id)
        ).scalar_one_or_none()
        assert view is not None
        assert view.run_id == "run-2"

    def test_rebuild_rematerializes_current_and_drops_strays(self, session, duckdb_conn):
        """rebuild_enriched_views re-execs the latest recipe per fact + drops strays.

        Two recipes for one fact (an older ``enriched_legacy``, a newer
        ``enriched_csv__orders``) model a view whose name changed between runs.
        A reset rebuilds the current target and drops the session's stale view —
        proving the transactional, dependency-ordered re-materialization + the
        session-scoped drop-views-not-in-run.
        """
        from datetime import UTC, datetime, timedelta
        from uuid import uuid4

        from dataraum.analysis.typing.db_models import MaterializationRecipe
        from dataraum.analysis.views.recipe import rebuild_enriched_views
        from dataraum.storage import Source, Table
        from tests.conftest import baseline_session_id

        duckdb_conn.execute(
            'CREATE OR REPLACE TABLE lake.typed."csv__orders" AS '
            "SELECT * FROM (VALUES (1, 100.0), (2, 200.0), (3, 150.0)) AS t(order_id, amount)"
        )
        source = Source(source_id=str(uuid4()), name="csv", source_type="csv")
        session.add(source)
        session.flush()
        fact = Table(
            table_id=str(uuid4()),
            source_id=source.source_id,
            table_name="orders",
            layer="typed",
            duckdb_path="csv__orders",
            row_count=3,
        )
        session.add(fact)
        session.flush()

        fact_fqn = 'lake.typed."csv__orders"'
        legacy_fqn = 'lake.typed."enriched_legacy"'
        current_fqn = 'lake.typed."enriched_csv__orders"'
        base = datetime(2026, 6, 4, tzinfo=UTC)
        session.add_all(
            [
                MaterializationRecipe(
                    session_id=baseline_session_id(),
                    table_id=fact.table_id,
                    layer="enriched",
                    run_id="r0",
                    target_fqn=legacy_fqn,
                    ddl=f"CREATE OR REPLACE VIEW {legacy_fqn} AS SELECT * FROM {fact_fqn}",
                    depends_on=[fact_fqn],
                    created_at=base,
                ),
                MaterializationRecipe(
                    session_id=baseline_session_id(),
                    table_id=fact.table_id,
                    layer="enriched",
                    run_id="r1",
                    target_fqn=current_fqn,
                    ddl=f"CREATE OR REPLACE VIEW {current_fqn} AS SELECT * FROM {fact_fqn}",
                    depends_on=[fact_fqn],
                    created_at=base + timedelta(hours=1),
                ),
            ]
        )
        session.flush()

        # The stale view physically exists; the current one does not (a lost rebuild).
        duckdb_conn.execute(f"CREATE OR REPLACE VIEW {legacy_fqn} AS SELECT * FROM {fact_fqn}")

        rebuilt = rebuild_enriched_views(session, duckdb_conn, session_id=baseline_session_id())

        assert rebuilt == [current_fqn], "only the latest recipe per fact is re-materialized"
        assert duckdb_conn.execute(f"SELECT COUNT(*) FROM {current_fqn}").fetchone()[0] == 3
        remaining = {row[0] for row in duckdb_conn.execute("SHOW TABLES").fetchall()}
        assert "enriched_csv__orders" in remaining
        assert "enriched_legacy" not in remaining, "the session's stray view is dropped"


class TestVersionedGrainConstraints:
    """The latest-only / run-grain invariants are DB-enforced, not app-level only.

    These pin the structural backstops added alongside the run-scoped reads: a
    duplicate fails loudly at insert instead of silently surfacing later as a
    ``MultipleResultsFound`` crash in a reader.
    """

    @staticmethod
    def _fact(session):
        from uuid import uuid4

        from dataraum.storage import Source, Table

        source = Source(source_id=str(uuid4()), name="csv", source_type="csv")
        session.add(source)
        session.flush()
        fact = Table(
            table_id=str(uuid4()),
            source_id=source.source_id,
            table_name="orders",
            layer="typed",
            duckdb_path="csv__orders",
        )
        session.add(fact)
        session.flush()
        return fact.table_id

    def test_enriched_view_unique_per_fact(self, session):
        """A second EnrichedView for the same fact_table_id is rejected."""
        from uuid import uuid4

        from sqlalchemy.exc import IntegrityError

        from dataraum.analysis.views.db_models import EnrichedView
        from tests.conftest import baseline_session_id

        fact_id = self._fact(session)
        session.add(
            EnrichedView(
                view_id=str(uuid4()),
                session_id=baseline_session_id(),
                fact_table_id=fact_id,
                view_name="enriched_csv__orders",
            )
        )
        session.flush()
        session.add(
            EnrichedView(
                view_id=str(uuid4()),
                session_id=baseline_session_id(),
                fact_table_id=fact_id,
                view_name="enriched_csv__orders_dup",
            )
        )
        with pytest.raises(IntegrityError):
            session.flush()
        session.rollback()

    def test_table_entity_unique_per_table_run(self, session):
        """A second TableEntity for the same (table_id, run_id) is rejected."""
        from uuid import uuid4

        from sqlalchemy.exc import IntegrityError

        from dataraum.analysis.semantic.db_models import TableEntity
        from tests.conftest import baseline_session_id

        fact_id = self._fact(session)
        for _ in range(2):
            session.add(
                TableEntity(
                    entity_id=str(uuid4()),
                    session_id=baseline_session_id(),
                    table_id=fact_id,
                    run_id="run-1",
                    detected_entity_type="fact",
                    is_fact_table=True,
                )
            )
        with pytest.raises(IntegrityError):
            session.flush()
        session.rollback()

    def test_table_entity_null_run_id_coexists(self, session):
        """Run-versioned coexistence is intact: different run_ids (incl. NULL) are allowed."""
        from uuid import uuid4

        from sqlalchemy import select

        from dataraum.analysis.semantic.db_models import TableEntity
        from tests.conftest import baseline_session_id

        fact_id = self._fact(session)
        for run_id in ("run-1", "run-2", None, None):
            session.add(
                TableEntity(
                    entity_id=str(uuid4()),
                    session_id=baseline_session_id(),
                    table_id=fact_id,
                    run_id=run_id,
                    detected_entity_type="fact",
                    is_fact_table=True,
                )
            )
        session.flush()
        rows = (
            session.execute(select(TableEntity).where(TableEntity.table_id == fact_id))
            .scalars()
            .all()
        )
        assert len(rows) == 4, "distinct run_ids and NULLs coexist (NULLs are distinct)"
