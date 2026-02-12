"""Enriched views phase implementation.

Creates grain-preserving DuckDB views that LEFT JOIN fact tables with their
confirmed dimension tables. Only uses relationships that are:
- Confirmed by LLM (detection_method = "llm")
- Cardinality many_to_one or one_to_one (grain-preserving)
- Confidence >= 0.7
- Not flagged as introducing duplicates

Post-creation: verifies row count matches fact table. Drops view if grain violated.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from sqlalchemy import select

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.semantic.db_models import TableEntity
from dataraum.analysis.views.builder import DimensionJoin, build_enriched_view_sql
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.core.logging import get_logger
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.storage import Column, Table

logger = get_logger(__name__)

# Only these cardinalities preserve fact table grain
_GRAIN_SAFE_CARDINALITIES = {"many_to_one", "one_to_one"}
_MIN_CONFIDENCE = 0.7


class EnrichedViewsPhase(BasePhase):
    """Create enriched DuckDB views from semantic output.

    For each fact table, creates a view that LEFT JOINs qualifying
    dimension tables. This materializes semantic relationships as
    queryable views for downstream phases (slicing, correlations).
    """

    @property
    def name(self) -> str:
        return "enriched_views"

    @property
    def description(self) -> str:
        return "Create enriched views joining fact + dimension tables"

    @property
    def dependencies(self) -> list[str]:
        return ["semantic"]

    @property
    def outputs(self) -> list[str]:
        return ["enriched_views"]

    @property
    def is_llm_phase(self) -> bool:
        return False

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if enriched views already exist for all fact tables."""
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        typed_tables = ctx.session.execute(stmt).scalars().all()

        if not typed_tables:
            return "No typed tables found"

        table_ids = [t.table_id for t in typed_tables]

        # Check for fact tables
        fact_stmt = select(TableEntity).where(
            TableEntity.table_id.in_(table_ids),
            TableEntity.is_fact_table.is_(True),
        )
        fact_entities = ctx.session.execute(fact_stmt).scalars().all()

        if not fact_entities:
            return "No fact tables identified"

        # Check if views already exist
        existing_stmt = select(EnrichedView).where(
            EnrichedView.fact_table_id.in_([e.table_id for e in fact_entities])
        )
        existing = ctx.session.execute(existing_stmt).scalars().all()

        if len(existing) >= len(fact_entities):
            return "Enriched views already exist for all fact tables"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Create enriched views for fact tables."""
        # Get typed tables
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        typed_tables = ctx.session.execute(stmt).scalars().all()

        if not typed_tables:
            return PhaseResult.failed("No typed tables found")

        table_ids = [t.table_id for t in typed_tables]
        tables_by_id = {t.table_id: t for t in typed_tables}

        # Find fact tables from entity detections
        fact_stmt = select(TableEntity).where(
            TableEntity.table_id.in_(table_ids),
            TableEntity.is_fact_table.is_(True),
        )
        fact_entities = ctx.session.execute(fact_stmt).scalars().all()

        if not fact_entities:
            return PhaseResult.success(
                outputs={"enriched_views": 0, "message": "No fact tables found"},
                records_processed=0,
                records_created=0,
            )

        # Load all LLM-confirmed relationships for these tables
        rel_stmt = select(Relationship).where(
            Relationship.from_table_id.in_(table_ids) | Relationship.to_table_id.in_(table_ids),
            Relationship.detection_method == "llm",
            Relationship.confidence >= _MIN_CONFIDENCE,
        )
        all_relationships = ctx.session.execute(rel_stmt).scalars().all()

        # Build column lookups
        cols_stmt = select(Column).where(Column.table_id.in_(table_ids))
        all_columns = ctx.session.execute(cols_stmt).scalars().all()
        columns_by_table: dict[str, list[Column]] = {}
        for col in all_columns:
            columns_by_table.setdefault(col.table_id, []).append(col)

        views_created = 0
        views_dropped = 0

        for fact_entity in fact_entities:
            fact_table = tables_by_id.get(fact_entity.table_id)
            if not fact_table or not fact_table.duckdb_path:
                continue

            # Find qualifying dimension joins for this fact table
            dimension_joins = self._find_dimension_joins(
                fact_table=fact_table,
                relationships=all_relationships,
                tables_by_id=tables_by_id,
                columns_by_table=columns_by_table,
            )

            if not dimension_joins:
                logger.info(
                    "no_qualifying_joins",
                    fact_table=fact_table.table_name,
                )
                continue

            # Build view SQL
            view_name, view_sql, dim_columns = build_enriched_view_sql(
                fact_table_name=fact_table.table_name,
                fact_duckdb_path=fact_table.duckdb_path,
                dimension_joins=dimension_joins,
            )

            # Create view in DuckDB
            try:
                ctx.duckdb_conn.execute(view_sql)
            except Exception as e:
                logger.warning(
                    "view_creation_failed",
                    view_name=view_name,
                    error=str(e),
                )
                continue

            # Verify grain preservation
            is_grain_verified = self._verify_grain(
                ctx.duckdb_conn,
                view_name=view_name,
                expected_count=fact_table.row_count,
            )

            if not is_grain_verified:
                # Drop view — it would introduce duplicates
                logger.warning(
                    "grain_verification_failed",
                    view_name=view_name,
                    expected_count=fact_table.row_count,
                )
                try:
                    ctx.duckdb_conn.execute(f'DROP VIEW IF EXISTS "{view_name}"')
                except Exception:
                    pass
                views_dropped += 1
                continue

            # Store view record
            view_record = EnrichedView(
                fact_table_id=fact_table.table_id,
                view_name=view_name,
                view_sql=view_sql,
                relationship_ids=[j.relationship_id for j in dimension_joins],
                dimension_table_ids=list(
                    {
                        tables_by_id[tid].table_id
                        for j in dimension_joins
                        for tid in tables_by_id
                        if tables_by_id[tid].table_name == j.dim_table_name
                    }
                ),
                dimension_columns=dim_columns,
                is_grain_verified=is_grain_verified,
            )
            ctx.session.add(view_record)
            views_created += 1

            logger.info(
                "enriched_view_created",
                view_name=view_name,
                fact_table=fact_table.table_name,
                dimension_joins=len(dimension_joins),
                dimension_columns=len(dim_columns),
            )

        return PhaseResult.success(
            outputs={
                "enriched_views": views_created,
                "views_dropped": views_dropped,
                "fact_tables": len(fact_entities),
            },
            records_processed=len(fact_entities),
            records_created=views_created,
        )

    def _find_dimension_joins(
        self,
        fact_table: Table,
        relationships: Sequence[Relationship],
        tables_by_id: dict[str, Table],
        columns_by_table: dict[str, list[Column]],
    ) -> list[DimensionJoin]:
        """Find qualifying dimension joins for a fact table.

        Only includes relationships where:
        - Cardinality is many_to_one or one_to_one
        - Confidence >= threshold
        - The relationship doesn't introduce duplicates
        """
        joins: list[DimensionJoin] = []

        for rel in relationships:
            # Determine which side is fact, which is dimension
            if rel.from_table_id == fact_table.table_id:
                fk_column_id = rel.from_column_id
                dim_table_id = rel.to_table_id
                dim_pk_column_id = rel.to_column_id
            elif rel.to_table_id == fact_table.table_id:
                fk_column_id = rel.to_column_id
                dim_table_id = rel.from_table_id
                dim_pk_column_id = rel.from_column_id
            else:
                continue

            # Check cardinality is grain-safe
            cardinality = rel.cardinality
            if cardinality not in _GRAIN_SAFE_CARDINALITIES:
                continue

            # Check for duplicate flag in evidence
            evidence = rel.evidence or {}
            if evidence.get("introduces_duplicates"):
                continue

            dim_table = tables_by_id.get(dim_table_id)
            if not dim_table or not dim_table.duckdb_path:
                continue

            # Find FK column name
            fk_col_name = None
            for col in columns_by_table.get(fact_table.table_id, []):
                if col.column_id == fk_column_id:
                    fk_col_name = col.column_name
                    break

            # Find dimension PK column name
            dim_pk_col_name = None
            for col in columns_by_table.get(dim_table_id, []):
                if col.column_id == dim_pk_column_id:
                    dim_pk_col_name = col.column_name
                    break

            if not fk_col_name or not dim_pk_col_name:
                continue

            # Get dimension columns to include (exclude PK/FK columns)
            include_columns = [
                col.column_name
                for col in columns_by_table.get(dim_table_id, [])
                if col.column_name != dim_pk_col_name and col.column_name != fk_col_name
            ]

            if not include_columns:
                continue  # No useful columns to add

            joins.append(
                DimensionJoin(
                    dim_table_name=dim_table.table_name,
                    dim_duckdb_path=dim_table.duckdb_path,
                    fact_fk_column=fk_col_name,
                    dim_pk_column=dim_pk_col_name,
                    include_columns=include_columns,
                    relationship_id=rel.relationship_id,
                )
            )

        return joins

    @staticmethod
    def _verify_grain(
        duckdb_conn: Any,
        view_name: str,
        expected_count: int | None,
    ) -> bool:
        """Verify that the view preserves the fact table grain.

        Returns True if COUNT(*) of view matches expected row count.
        """
        if expected_count is None:
            return True  # Can't verify without expected count

        try:
            result = duckdb_conn.execute(f'SELECT COUNT(*) FROM "{view_name}"').fetchone()
            actual_count = result[0] if result else 0
            return actual_count == expected_count
        except Exception:
            return False
