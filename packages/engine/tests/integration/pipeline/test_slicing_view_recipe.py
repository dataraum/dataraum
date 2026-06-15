"""slicing_view DDL versioning on the recipe substrate (DAT-415, Phase B).

Drives ``SlicingViewPhase`` end-to-end against the real DuckLake: it must
materialize a ``lake.typed`` slicing view, version its ``CREATE VIEW`` DDL as a
``MaterializationRecipe`` (``layer="slicing"``, depends_on the enriched view it
projects from), and write a latest-only ``SlicingView`` (one row per fact,
DB-enforced) — sqlglot-gated so an unchanged re-run reconciles in place without
stamping a redundant recipe version.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.db_models import TableEntity
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.typing.db_models import MaterializationRecipe
from dataraum.analysis.views.db_models import EnrichedView, SlicingView
from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases.slicing_view_phase import SlicingViewPhase
from dataraum.storage import Column, Source, Table

if TYPE_CHECKING:
    import duckdb


def _seed_fact_with_enriched_view(session: Session, duckdb_conn: duckdb.DuckDBPyConnection) -> str:
    """Materialize a typed fact + its enriched view in lake.typed + metadata.

    Returns the fact table id. The enriched view is a passthrough (no dim join)
    so the slicing view projects the fact columns straight through — enough to
    exercise the DDL-versioning path without an LLM enrichment.

    Does NOT seed the ``TableEntity`` — it is run-versioned, so each run seeds its
    own via :func:`_seed_fact_entity` (mirroring how ``semantic_per_table``
    re-detects the entity per run).
    """
    duckdb_conn.execute(
        'CREATE OR REPLACE TABLE lake.typed."csv__orders" AS '
        "SELECT * FROM (VALUES (1, 'us', 100.0), (2, 'eu', 200.0), (3, 'us', 150.0)) "
        "AS t(order_id, region, amount)"
    )
    duckdb_conn.execute(
        'CREATE OR REPLACE VIEW lake.typed."enriched_csv__orders" AS '
        'SELECT * FROM lake.typed."csv__orders"'
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

    region_col = str(uuid4())
    for pos, (cid, name) in enumerate(
        ((str(uuid4()), "order_id"), (region_col, "region"), (str(uuid4()), "amount"))
    ):
        session.add(
            Column(
                column_id=cid,
                table_id=fact.table_id,
                column_name=name,
                column_position=pos,
                raw_type="VARCHAR",
                resolved_type="VARCHAR",
            )
        )

    session.add(
        EnrichedView(
            view_id=str(uuid4()),
            fact_table_id=fact.table_id,
            view_name="enriched_csv__orders",
            is_grain_verified=True,
            dimension_columns=[],
            dimension_table_ids=[],
        )
    )
    _seed_slice_definition(session, fact.table_id, "run-1")
    session.flush()
    return fact.table_id


def _seed_slice_definition(session: Session, fact_id: str, run_id: str) -> None:
    """Seed this run's slice definition — run-versioned (DAT-448), one per run.

    Mirrors what the run-scoped ``slicing`` phase now produces: a fresh run
    re-derives its own definition (here byte-identical), it never reuses a
    prior run's row.
    """
    region_col = session.execute(
        select(Column.column_id).where(Column.table_id == fact_id, Column.column_name == "region")
    ).scalar_one()
    session.add(
        SliceDefinition(
            table_id=fact_id,
            column_id=region_col,
            column_name="region",
            run_id=run_id,
            slice_priority=1,
            slice_type="categorical",
            distinct_values=["us", "eu"],
            value_count=2,
            detection_source="llm",
            sql_template=(
                "CREATE OR REPLACE VIEW slice_orders_region_us AS "
                "SELECT * FROM enriched_csv__orders WHERE region = 'us'"
            ),
        )
    )
    session.flush()


def _seed_fact_entity(session: Session, fact_id: str, run_id: str) -> None:
    """Seed this run's fact classification — run-versioned, one per run."""
    session.add(
        TableEntity(
            entity_id=str(uuid4()),
            table_id=fact_id,
            run_id=run_id,
            detected_entity_type="fact",
            is_fact_table=True,
        )
    )
    session.flush()


def _slicing_recipes(session: Session, fact_id: str) -> list[MaterializationRecipe]:
    return list(
        session.execute(
            select(MaterializationRecipe).where(
                MaterializationRecipe.table_id == fact_id,
                MaterializationRecipe.layer == "slicing",
            )
        ).scalars()
    )


def _slicing_views(session: Session, fact_id: str) -> list[SlicingView]:
    return list(
        session.execute(select(SlicingView).where(SlicingView.fact_table_id == fact_id)).scalars()
    )


class TestSlicingViewRecipeVersioning:
    def test_versions_ddl_and_reconciles_latest_only(self, session, duckdb_conn) -> None:
        """One run stamps a slicing recipe + a latest-only SlicingView; a re-run reconciles.

        Run A materializes the view, stamps a ``layer="slicing"`` recipe (depends_on
        the enriched view) and one ``SlicingView``. Run B (fresh run_id, identical
        DDL) must reconcile the SAME ``SlicingView`` in place — never a duplicate the
        unique constraint would reject — and the sqlglot gate must add NO second
        recipe version.
        """
        fact_id = _seed_fact_with_enriched_view(session, duckdb_conn)
        _seed_fact_entity(session, fact_id, "run-1")

        ctx_a = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            table_ids=[fact_id],
            run_id="run-1",
        )
        result = SlicingViewPhase().run(ctx_a)
        assert result.status == PhaseStatus.COMPLETED, result.error

        # The physical slicing view exists in lake.typed and preserves grain. Its
        # name is source-qualified off the fact's duckdb_path (csv__orders, DAT-356).
        assert (
            duckdb_conn.execute('SELECT COUNT(*) FROM lake.typed."slicing_csv_orders"').fetchone()[
                0
            ]
            == 3
        )

        # One slicing recipe, run-stamped, depending on the enriched view it projects from.
        recipes = _slicing_recipes(session, fact_id)
        assert len(recipes) == 1
        assert recipes[0].run_id == "run-1"
        assert recipes[0].target_fqn == 'lake.typed."slicing_csv_orders"'
        assert recipes[0].depends_on == ['lake.typed."enriched_csv__orders"']

        # Exactly one SlicingView, stamped with this run.
        views = _slicing_views(session, fact_id)
        assert len(views) == 1
        assert views[0].run_id == "run-1"

        # Run B: fresh run_id, identical DDL — reconcile in place, no new recipe version.
        # The entity re-detects per run (run-versioned), as semantic_per_table would;
        # the slice definition re-derives per run (DAT-448), as slicing would.
        _seed_fact_entity(session, fact_id, "run-2")
        _seed_slice_definition(session, fact_id, "run-2")
        ctx_b = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            table_ids=[fact_id],
            run_id="run-2",
        )
        result_b = SlicingViewPhase().run(ctx_b)
        assert result_b.status == PhaseStatus.COMPLETED, result_b.error

        views = _slicing_views(session, fact_id)
        assert len(views) == 1, "latest-only: reconciled in place, not duplicated"
        assert views[0].run_id == "run-2"

        recipes = _slicing_recipes(session, fact_id)
        assert {r.run_id for r in recipes} == {"run-1"}, (
            "sqlglot gate: an unchanged re-run stamps no redundant recipe version"
        )
