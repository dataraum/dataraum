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

        # No fact-role TableEntity → nothing to enrich.
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
                    relationship_role="reference/lookup",
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

        session.execute(
            delete(TableEntity).where(
                TableEntity.table_id == table_id, TableEntity.run_id == run_id
            )
        )
        session.add(
            TableEntity(
                entity_id=str(uuid4()),
                table_id=table_id,
                run_id=run_id,
                detected_entity_type="fact",
                table_role="fact",
            )
        )
        session.flush()

    @staticmethod
    def _seed_relationship(
        session, *, from_table_id: str, from_col: str, to_table_id: str, to_col: str, run_id: str
    ) -> None:
        """Run-stamped confirmed relationship, as relationship_discovery mints each run.

        Each begin_session run re-creates relationships with a fresh ``relationship_id``
        (per-run uuid4) but the SAME column pair — so seeding per run exercises DAT-516's
        cross-run-stable ``(from_column_id, to_column_id)`` key. Idempotent per run_id.
        """
        from uuid import uuid4

        from sqlalchemy import delete, select

        from dataraum.analysis.relationships.db_models import Relationship
        from dataraum.storage import Column

        fk_id = session.execute(
            select(Column.column_id).where(
                Column.table_id == from_table_id, Column.column_name == from_col
            )
        ).scalar_one()
        pk_id = session.execute(
            select(Column.column_id).where(
                Column.table_id == to_table_id, Column.column_name == to_col
            )
        ).scalar_one()
        session.execute(
            delete(Relationship).where(
                Relationship.run_id == run_id,
                Relationship.from_column_id == fk_id,
                Relationship.to_column_id == pk_id,
            )
        )
        session.add(
            Relationship(
                relationship_id=str(uuid4()),
                run_id=run_id,
                from_table_id=from_table_id,
                to_table_id=to_table_id,
                from_column_id=fk_id,
                to_column_id=pk_id,
                relationship_type="foreign_key",
                cardinality="many-to-one",
                confidence=0.9,
                detection_method="llm",
                confirmation_source="judge",
            )
        )
        session.flush()

    def test_phase_materializes_versioned_view(self, session, duckdb_conn, monkeypatch):
        from sqlalchemy import select

        from dataraum.analysis.typing.db_models import MaterializationRecipe
        from dataraum.analysis.views.db_models import EnrichedView
        from dataraum.pipeline.base import PhaseContext, PhaseStatus
        from dataraum.storage import Table

        fact_id, dim_id, canned = self._seed(session, duckdb_conn)
        rec = {"current": canned}
        calls = {"n": 0}

        def _stub(self, **kw):
            calls["n"] += 1
            return rec["current"]

        monkeypatch.setattr(EnrichedViewsPhase, "_get_llm_recommendations", _stub)

        def run(run_id: str) -> None:
            # Each run re-detects its entities + re-mints its relationships (run-scoped),
            # as semantic_per_table + relationship_discovery do in begin_session — prior
            # runs' rows coexist; the relationship_id changes but the column pair doesn't.
            self._seed_fact_entity(session, table_id=fact_id, run_id=run_id)
            self._seed_relationship(
                session,
                from_table_id=fact_id,
                from_col="customer_id",
                to_table_id=dim_id,
                to_col="id",
                run_id=run_id,
            )
            ctx = PhaseContext(
                session=session,
                duckdb_conn=duckdb_conn,
                table_ids=[fact_id, dim_id],
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

        assert calls["n"] == 1, "run 1 judged the one undecided relationship"

        # --- Run 1 retry: same run_id is idempotent (no duplicate rows) ----
        run("run-1")
        assert len(enriched_views()) == 1, "a same-run retry must not duplicate the view definition"
        assert {r.run_id for r in enriched_recipes()} == {"run-1"}
        assert calls["n"] == 1, "the retry inherits the shape — no second LLM call"

        # --- Run 2 (relationship already considered): LLM SKIPPED, shape inherited ---
        run("run-2")
        views = enriched_views()
        assert len(views) == 1, "EnrichedView is latest-only — one row per fact"
        assert views[0].run_id == "run-2", "the latest run stamps the (reconciled) definition"
        assert {r.run_id for r in enriched_recipes()} == {"run-1"}, (
            "unchanged canonical SQL adds no new recipe version (canonical-SQL-gated)"
        )
        assert calls["n"] == 1, "run 2 inherits the shape — the LLM is not consulted again"
        # The inherited shape still exposes BOTH columns (name + country).
        assert sorted(views[0].dimension_columns or []) == [
            "customer_id__country",
            "customer_id__name",
        ]

        # --- Run 3 (DAT-516 determinism): a CONTRADICTORY re-judgment is ignored ----
        # The relationship is already in considered, so the enrichment LLM is never asked
        # again — even though this stub drops "country", the sticky shape is unchanged and
        # no new recipe version is stamped. (Under the old re-judge-every-run model this
        # would have shrunk the view + stamped a new recipe.)
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
                            include_columns=["name"],  # would drop "country" — but is ignored
                            relationship_id="rel-1",
                        )
                    ],
                    relationship_role="reference/lookup",
                    confidence=0.9,
                    reasoning="contradictory re-judgment",
                    enrichment_columns=["name"],
                )
            ],
            summary="stub",
            model_name="stub-model",
        )
        run("run-3")
        views = enriched_views()
        assert len(views) == 1 and views[0].run_id == "run-3"
        assert sorted(views[0].dimension_columns or []) == [
            "customer_id__country",
            "customer_id__name",
        ], "the contradictory re-judgment was ignored — shape is sticky"
        assert {r.run_id for r in enriched_recipes()} == {"run-1"}, (
            "no shape change → no new recipe version (the re-judgment was never applied)"
        )
        assert calls["n"] == 1, "run 3's contradictory stub was never consulted (sticky shape)"
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

        fact_id, dim_id, canned = self._seed(session, duckdb_conn)
        monkeypatch.setattr(
            EnrichedViewsPhase, "_get_llm_recommendations", lambda self, **kw: canned
        )

        # Two coexisting run-scoped fact entities — as if begin_session ran twice.
        self._seed_fact_entity(session, table_id=fact_id, run_id="run-1")
        self._seed_fact_entity(session, table_id=fact_id, run_id="run-2")
        # This run's confirmed relationship (so the enrichment LLM has a candidate to judge).
        self._seed_relationship(
            session,
            from_table_id=fact_id,
            from_col="customer_id",
            to_table_id=dim_id,
            to_col="id",
            run_id="run-2",
        )

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            table_ids=[fact_id, dim_id],
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

    def test_rerun_preserves_dim_column_ids_and_profiles(self, session, duckdb_conn, monkeypatch):
        """A re-run with an UNCHANGED shape preserves dim ``column_id``s + profiles (DAT-516).

        Reconcile-don't-replace: the enriched ``Table`` is reused, and columns whose name
        survives keep their ``column_id`` AND their ``StatisticalProfile`` (same join → same
        data). A consumer holding a ``column_id`` — or the profiles those columns carry — is
        not silently invalidated by an unchanged re-run. (The old code delete+reinserted
        every run, minting fresh ``column_id``s and re-profiling under the new run.)
        """
        from sqlalchemy import select

        from dataraum.analysis.statistics.db_models import StatisticalProfile
        from dataraum.pipeline.base import PhaseContext, PhaseStatus
        from dataraum.storage import Column, Table

        fact_id, dim_id, canned = self._seed(session, duckdb_conn)
        monkeypatch.setattr(
            EnrichedViewsPhase, "_get_llm_recommendations", lambda self, **kw: canned
        )

        def run(run_id: str) -> PhaseStatus:
            self._seed_fact_entity(session, table_id=fact_id, run_id=run_id)
            self._seed_relationship(
                session,
                from_table_id=fact_id,
                from_col="customer_id",
                to_table_id=dim_id,
                to_col="id",
                run_id=run_id,
            )
            ctx = PhaseContext(
                session=session, duckdb_conn=duckdb_conn, table_ids=[fact_id, dim_id], run_id=run_id
            )
            result = EnrichedViewsPhase().run(ctx)
            session.flush()
            return result.status

        def view_table() -> Table:
            return session.execute(select(Table).where(Table.layer == "enriched")).scalar_one()

        def cols_by_name(view_table_id: str) -> dict[str, str]:
            return {
                name: cid
                for (cid, name) in session.execute(
                    select(Column.column_id, Column.column_name).where(
                        Column.table_id == view_table_id
                    )
                ).all()
            }

        def profile_run_ids(view_table_id: str) -> set[str]:
            return set(
                session.execute(
                    select(StatisticalProfile.run_id)
                    .join(Column, StatisticalProfile.column_id == Column.column_id)
                    .where(Column.table_id == view_table_id)
                ).scalars()
            )

        # Run 1 seeds the enriched Table + dim columns + their profiles.
        assert run("run-1") == PhaseStatus.COMPLETED
        vt = view_table()
        run1_cols = cols_by_name(vt.table_id)
        assert run1_cols, "run-1 must register dimension columns"
        assert profile_run_ids(vt.table_id) == {"run-1"}

        # Run 2 over the SAME unchanged shape: the inherited columns keep their column_id
        # AND their run-1 profiles — reconciled in place, not delete+reinserted.
        assert run("run-2") == PhaseStatus.COMPLETED
        vt2 = view_table()
        assert vt2.table_id == vt.table_id, "the enriched Table row is reused across runs"
        assert cols_by_name(vt2.table_id) == run1_cols, (
            "an unchanged shape preserves every column_id (no churn)"
        )
        assert profile_run_ids(vt2.table_id) == {"run-1"}, (
            "kept columns keep their original profile — not re-minted under run-2"
        )

    def test_shape_grows_on_new_relationship_and_shrinks_on_reject(
        self, session, duckdb_conn, monkeypatch
    ):
        """Monotonic shape: a newly-confirmed relationship ADDS its dimension columns; a
        user reject REMOVES them — the only two signals that move a sticky shape (DAT-516)."""
        from uuid import uuid4

        from sqlalchemy import select

        from dataraum.analysis.views.db_models import EnrichedView
        from dataraum.pipeline.base import PhaseContext, PhaseStatus
        from dataraum.storage import Column
        from dataraum.storage.overlay_models import ConfigOverlay

        fact_id, dim_id, canned = self._seed(session, duckdb_conn)
        monkeypatch.setattr(
            EnrichedViewsPhase, "_get_llm_recommendations", lambda self, **kw: canned
        )

        def run(run_id: str, *, with_rel: bool) -> None:
            self._seed_fact_entity(session, table_id=fact_id, run_id=run_id)
            if with_rel:
                self._seed_relationship(
                    session,
                    from_table_id=fact_id,
                    from_col="customer_id",
                    to_table_id=dim_id,
                    to_col="id",
                    run_id=run_id,
                )
            ctx = PhaseContext(
                session=session, duckdb_conn=duckdb_conn, table_ids=[fact_id, dim_id], run_id=run_id
            )
            assert EnrichedViewsPhase().run(ctx).status == PhaseStatus.COMPLETED
            session.flush()

        def dim_cols() -> list[str]:
            v = session.execute(
                select(EnrichedView).where(EnrichedView.fact_table_id == fact_id)
            ).scalar_one()
            return sorted(v.dimension_columns or [])

        # Run 1: no relationship yet → passthrough view, zero dimension columns.
        run("run-1", with_rel=False)
        assert dim_cols() == []

        # Run 2: a newly-confirmed relationship is judged in → its columns are ADDED.
        run("run-2", with_rel=True)
        assert dim_cols() == ["customer_id__country", "customer_id__name"], "grow on new confirm"

        # Run 3: the user rejects the relationship → its columns DROP (shrink), even though
        # the relationship is still structurally confirmed and already in `considered`.
        fk_id = session.execute(
            select(Column.column_id).where(
                Column.table_id == fact_id, Column.column_name == "customer_id"
            )
        ).scalar_one()
        pk_id = session.execute(
            select(Column.column_id).where(Column.table_id == dim_id, Column.column_name == "id")
        ).scalar_one()
        session.add(
            ConfigOverlay(
                overlay_id=str(uuid4()),
                type="relationship",
                payload={"action": "reject", "from_column_id": fk_id, "to_column_id": pk_id},
            )
        )
        session.flush()
        run("run-3", with_rel=True)
        assert dim_cols() == [], "shrink on explicit reject"

    def test_dropped_then_reconfirmed_relationship_is_rejudged(
        self, session, duckdb_conn, monkeypatch
    ):
        """A relationship that leaves Layer A and returns is RE-judged, not stuck invisible.

        Stickiness must not outlive the relationship: ``considered`` is pruned to what Layer A
        currently confirms, so a genuine drop + re-confirm re-asks the LLM (vs the determinism
        case, where Layer A keeps the set stable and the LLM is never re-asked). Distinguished
        by the LLM call count.
        """
        from sqlalchemy import select

        from dataraum.analysis.views.db_models import EnrichedView
        from dataraum.pipeline.base import PhaseContext, PhaseStatus

        fact_id, dim_id, canned = self._seed(session, duckdb_conn)
        calls = {"n": 0}

        def _stub(self, **kw):
            calls["n"] += 1
            return canned

        monkeypatch.setattr(EnrichedViewsPhase, "_get_llm_recommendations", _stub)

        def run(run_id: str, *, with_rel: bool) -> None:
            self._seed_fact_entity(session, table_id=fact_id, run_id=run_id)
            if with_rel:
                self._seed_relationship(
                    session,
                    from_table_id=fact_id,
                    from_col="customer_id",
                    to_table_id=dim_id,
                    to_col="id",
                    run_id=run_id,
                )
            ctx = PhaseContext(
                session=session, duckdb_conn=duckdb_conn, table_ids=[fact_id, dim_id], run_id=run_id
            )
            assert EnrichedViewsPhase().run(ctx).status == PhaseStatus.COMPLETED
            session.flush()

        def dim_cols() -> list[str]:
            v = session.execute(
                select(EnrichedView).where(EnrichedView.fact_table_id == fact_id)
            ).scalar_one()
            return sorted(v.dimension_columns or [])

        run("run-1", with_rel=True)  # judged → exposed
        assert dim_cols() and calls["n"] == 1
        run("run-2", with_rel=False)  # Layer A no longer confirms it → dropped, considered pruned
        assert dim_cols() == [] and calls["n"] == 1  # no candidate → LLM not called
        run("run-3", with_rel=True)  # re-confirmed → RE-judged (not stuck), exposed again
        assert dim_cols() == ["customer_id__country", "customer_id__name"]
        assert calls["n"] == 2, "a genuinely re-confirmed relationship is re-judged, not inherited"

    @staticmethod
    def _seed_fanout(session, duckdb_conn):
        """Seed a fact with TWO proposed joins: one grain-preserving, one fan-out.

        The good dim (``customers``) has a UNIQUE key → its LEFT JOIN keeps the
        3-row grain. The fan-out dim (``customer_tags``) has DUPLICATE keys on the
        join column → its LEFT JOIN inflates the fact (DAT-801). Both joins ride the
        same fact FK (``customer_id``), so the drop is driven by the MEASURED
        fan-out, not the cardinality label. Returns
        ``(fact_id, good_dim_id, fanout_dim_id, canned)``.
        """
        from uuid import uuid4

        from dataraum.analysis.views.builder import DimensionJoin
        from dataraum.analysis.views.enrichment_models import (
            EnrichmentAnalysisResult,
            EnrichmentRecommendation,
        )
        from dataraum.storage import Column, Source, Table

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
        # Duplicate keys on cust_key → a one-to-many LEFT JOIN that fans the fact out.
        duckdb_conn.execute(
            'CREATE OR REPLACE TABLE lake.typed."csv__customer_tags" AS '
            "SELECT * FROM (VALUES (10, 'vip'), (10, 'gold'), (20, 'new')) "
            "AS t(cust_key, tag)"
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
        good = Table(
            table_id=str(uuid4()),
            source_id=source.source_id,
            table_name="customers",
            layer="typed",
            duckdb_path="csv__customers",
            row_count=2,
        )
        fanout = Table(
            table_id=str(uuid4()),
            source_id=source.source_id,
            table_name="customer_tags",
            layer="typed",
            duckdb_path="csv__customer_tags",
            row_count=3,
        )
        session.add_all([fact, good, fanout])
        session.flush()

        for tbl, names in (
            (fact, ("order_id", "customer_id", "amount")),
            (good, ("id", "name", "country")),
            (fanout, ("cust_key", "tag")),
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
                            dim_duckdb_path="(rewritten by the phase)",
                            fact_fk_column="customer_id",
                            dim_pk_column="id",
                            include_columns=["name", "country"],
                            relationship_id="rel-good",
                        ),
                        DimensionJoin(
                            dim_table_name="customer_tags",
                            dim_duckdb_path="(rewritten by the phase)",
                            fact_fk_column="customer_id",
                            dim_pk_column="cust_key",
                            include_columns=["tag"],
                            relationship_id="rel-fanout",
                        ),
                    ],
                    relationship_role="reference/lookup",
                    confidence=0.9,
                    reasoning="customers + tags both proposed",
                    enrichment_columns=["name", "country", "tag"],
                )
            ],
            summary="stub",
            model_name="stub-model",
        )
        return fact.table_id, good.table_id, fanout.table_id, canned

    def test_fanout_join_dropped_view_ships_with_survivors(
        self, session, duckdb_conn, monkeypatch, capsys
    ):
        """A fan-out join drops the OFFENDING join and rebuilds; the view still ships (DAT-801).

        Two joins are proposed for the fact: a grain-preserving one (``customers``,
        unique key) and a fan-out one (``customer_tags``, duplicate keys). The view
        must SHIP with the good join's columns, EXCLUDE the fan-out join's columns,
        preserve the fact grain, and the drop must be logged born-loud. Under the
        old all-or-nothing behavior the whole view was dropped instead.
        """
        from sqlalchemy import select

        from dataraum.analysis.views.db_models import EnrichedView
        from dataraum.pipeline.base import PhaseContext, PhaseStatus

        fact_id, good_id, fanout_id, canned = self._seed_fanout(session, duckdb_conn)
        monkeypatch.setattr(
            EnrichedViewsPhase, "_get_llm_recommendations", lambda self, **kw: canned
        )

        self._seed_fact_entity(session, table_id=fact_id, run_id="run-1")
        self._seed_relationship(
            session,
            from_table_id=fact_id,
            from_col="customer_id",
            to_table_id=good_id,
            to_col="id",
            run_id="run-1",
        )
        self._seed_relationship(
            session,
            from_table_id=fact_id,
            from_col="customer_id",
            to_table_id=fanout_id,
            to_col="cust_key",
            run_id="run-1",
        )

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            table_ids=[fact_id, good_id, fanout_id],
            run_id="run-1",
        )
        result = EnrichedViewsPhase().run(ctx)
        assert result.status == PhaseStatus.COMPLETED, result.error

        # The view SHIPS (not dropped) with exactly the good join's columns dropped
        # to the survivors, and one join was dropped for fanning out.
        assert result.outputs["enriched_views"] == 1
        assert result.outputs["views_dropped"] == 0
        assert result.outputs["joins_dropped"] == 1

        view = session.execute(
            select(EnrichedView).where(EnrichedView.fact_table_id == fact_id)
        ).scalar_one()
        assert view.is_grain_verified is True
        assert sorted(view.dimension_columns or []) == [
            "customer_id__country",
            "customer_id__name",
        ], "the good join's columns ship; the fan-out join's column is excluded"

        # Physical view: grain preserved (3 fact rows), the good columns resolve,
        # and the fan-out column is absent from the schema.
        rows = duckdb_conn.execute(
            'SELECT order_id, "customer_id__name", "customer_id__country" '
            'FROM lake.typed."enriched_csv__orders" ORDER BY order_id'
        ).fetchall()
        assert rows == [(1, "Alice", "US"), (2, "Bob", "UK"), (3, "Alice", "US")]
        view_columns = {
            r[0]
            for r in duckdb_conn.execute('DESCRIBE lake.typed."enriched_csv__orders"').fetchall()
        }
        assert "customer_id__tag" not in view_columns, "the fan-out column never ships"

        # Born-loud: the drop names the fact, the neighbour, and expected-vs-actual.
        err = capsys.readouterr().err
        assert "enrichment_join_fans_out" in err
        assert "customer_tags" in err

    def test_all_good_joins_still_ship(self, session, duckdb_conn, monkeypatch):
        """The all-grain-preserving case is unchanged: every proposed join ships.

        Guards against the per-join filter over-dropping — with no fan-out, nothing
        is dropped and the view carries both dimension columns.
        """
        from sqlalchemy import select

        from dataraum.analysis.views.db_models import EnrichedView
        from dataraum.pipeline.base import PhaseContext, PhaseStatus

        fact_id, dim_id, canned = self._seed(session, duckdb_conn)
        monkeypatch.setattr(
            EnrichedViewsPhase, "_get_llm_recommendations", lambda self, **kw: canned
        )
        self._seed_fact_entity(session, table_id=fact_id, run_id="run-1")
        self._seed_relationship(
            session,
            from_table_id=fact_id,
            from_col="customer_id",
            to_table_id=dim_id,
            to_col="id",
            run_id="run-1",
        )

        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            table_ids=[fact_id, dim_id],
            run_id="run-1",
        )
        result = EnrichedViewsPhase().run(ctx)
        assert result.status == PhaseStatus.COMPLETED, result.error
        assert result.outputs["enriched_views"] == 1
        assert result.outputs["joins_dropped"] == 0

        view = session.execute(
            select(EnrichedView).where(EnrichedView.fact_table_id == fact_id)
        ).scalar_one()
        assert view.is_grain_verified is True
        assert sorted(view.dimension_columns or []) == [
            "customer_id__country",
            "customer_id__name",
        ]


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

        fact_id = self._fact(session)
        session.add(
            EnrichedView(
                view_id=str(uuid4()),
                fact_table_id=fact_id,
                view_name="enriched_csv__orders",
            )
        )
        session.flush()
        session.add(
            EnrichedView(
                view_id=str(uuid4()),
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

        fact_id = self._fact(session)
        for _ in range(2):
            session.add(
                TableEntity(
                    entity_id=str(uuid4()),
                    table_id=fact_id,
                    run_id="run-1",
                    detected_entity_type="fact",
                    table_role="fact",
                )
            )
        with pytest.raises(IntegrityError):
            session.flush()
        session.rollback()

    def test_table_entity_distinct_runs_coexist(self, session):
        """Run-versioned coexistence is intact: distinct run_ids are allowed."""
        from uuid import uuid4

        from sqlalchemy import select

        from dataraum.analysis.semantic.db_models import TableEntity

        fact_id = self._fact(session)
        for run_id in ("run-1", "run-2", "run-3"):
            session.add(
                TableEntity(
                    entity_id=str(uuid4()),
                    table_id=fact_id,
                    run_id=run_id,
                    detected_entity_type="fact",
                    table_role="fact",
                )
            )
        session.flush()
        rows = (
            session.execute(select(TableEntity).where(TableEntity.table_id == fact_id))
            .scalars()
            .all()
        )
        assert len(rows) == 3, "distinct run_ids coexist"
