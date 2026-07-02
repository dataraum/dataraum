"""In-place teardown of a source's materialized tables + metadata (DAT-596).

Re-importing a ``db_recipe`` source under the SAME user-chosen name with a
CHANGED recipe (re-pointed SQL) is a replace, not an error: the old raw/typed/
quarantine tables and every metadata row hanging off them must go before the new
recipe rematerializes. Files never reach here — they are content-keyed
(``src_<digest>``), so changed bytes mint a new source — this is the db_recipe
path only.

Why a NAME-keyed drop is correct and unambiguous: table names are UNIQUE per
workspace across raw/typed/quarantine/enriched (one ``ws_<id>`` schema per
workspace), so enumerating a source's tables by ``source_id`` and dropping each
``(layer, duckdb_path)`` cannot touch another source's data. No run-versioned
raw identity is needed (deferred future work).

The cascade surface this clears (verified against ``schema.sql``): only
``columns.table_id`` FK-cascades on a ``tables`` delete — every other child that
references ``tables`` (or ``columns``) has NO ``ON DELETE CASCADE`` (the
torn-window cut removed them, see ``_column_cleanup``). So this helper deletes,
in FK-safe order:

1. column-keyed children via :func:`delete_column_dependents` (the 14 children
   of ``columns``),
2. the table-keyed children that reference ``tables`` directly,
3. the source-keyed children (``column_eligibility``),
4. the per-table ``metadata_snapshot_head`` rows (``table:{id}`` /
   ``GENERATION_STAGE``) so no head dangles at a deleted target,
5. the ``tables`` rows (DB-cascades their ``columns``),
6. the underlying DuckDB objects (``DROP TABLE`` for raw/typed/quarantine,
   ``DROP VIEW`` for the ``enriched`` layer — it's a view, not a table).

It runs inside the import activity's session (the phase runner rolls back on a
FAILED result, DAT-502), so a mid-teardown failure leaves the prior state intact.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import delete, or_, select

from dataraum.core.duckdb_naming import schema_for_layer
from dataraum.pipeline.phases._column_cleanup import delete_column_dependents
from dataraum.server.storage import LAKE_CATALOG_ALIAS
from dataraum.storage import Column, Table
from dataraum.storage.snapshot_head import GENERATION_STAGE, MetadataSnapshotHead

if TYPE_CHECKING:
    from dataraum.pipeline.base import PhaseContext


def teardown_source_tables(ctx: PhaseContext, source_id: str) -> int:
    """Drop every table a source materialized + all its metadata children (DAT-596).

    Enumerates the source's ``Table`` rows across ALL layers (raw/typed/
    quarantine/enriched) by ``source_id``, then removes — in FK-safe order — the
    column-keyed and table-keyed metadata, the per-table snapshot heads, the
    ``Table``/``Column`` rows, and the underlying DuckDB tables. Idempotent and
    name-unambiguous: table names are unique per workspace, so a ``source_id``
    enumeration never reaches another source's data.

    Args:
        ctx: The import phase context (its ``session`` + ``duckdb_conn`` carry the
            teardown; the phase runner's rollback-on-FAILED keeps it atomic).
        source_id: The source whose materialized tables are being replaced.

    Returns:
        The number of ``Table`` rows torn down.
    """
    tables = ctx.session.execute(select(Table).where(Table.source_id == source_id)).scalars().all()
    if not tables:
        return 0

    table_ids = [t.table_id for t in tables]
    column_ids = list(
        ctx.session.execute(select(Column.column_id).where(Column.table_id.in_(table_ids)))
        .scalars()
        .all()
    )

    # 1. Column-keyed children (the 14 FK children of ``columns``).
    delete_column_dependents(ctx, column_ids)

    # 2. Table-keyed children that reference ``tables`` directly (none cascade).
    _delete_table_dependents(ctx, table_ids, source_id)

    # 3. Per-table snapshot heads — add_source promotes one ``(table:{id},
    #    GENERATION_STAGE)`` head per table; remove them so none dangles at a
    #    deleted target. (Catalog / operating_model heads are workspace-grain,
    #    not per-table, so they are untouched.)
    head_targets = [f"table:{tid}" for tid in table_ids]
    ctx.session.execute(
        delete(MetadataSnapshotHead).where(
            MetadataSnapshotHead.target.in_(head_targets),
            MetadataSnapshotHead.stage == GENERATION_STAGE,
        )
    )

    # 4. The ``tables`` rows. ``columns.table_id`` ON DELETE CASCADE drops the
    #    Column rows with them; passive_deletes lets the DB do it.
    ctx.session.execute(delete(Table).where(Table.table_id.in_(table_ids)))
    ctx.session.flush()

    # 5. The underlying DuckDB objects, one per (layer, bare-name). Raw/typed/
    #    quarantine share the bare ``duckdb_path``; the schema discriminates. The
    #    ``enriched`` layer is a DuckDB VIEW (enriched_views_phase builds it with
    #    CREATE OR REPLACE VIEW), so DROP TABLE would silently no-op and leave the
    #    view dangling over the deleted typed tables — drop it as a VIEW.
    for table in tables:
        bare = table.duckdb_path
        if not bare:
            continue
        schema = schema_for_layer(table.layer)
        fqn = f'{LAKE_CATALOG_ALIAS}.{schema}."{bare}"'
        kind = "VIEW" if table.layer == "enriched" else "TABLE"
        ctx.duckdb_conn.execute(f"DROP {kind} IF EXISTS {fqn}")

    return len(tables)


def _delete_table_dependents(ctx: PhaseContext, table_ids: list[str], source_id: str) -> None:
    """Delete every run-stamped row that FK-references the given tables (DAT-596).

    Covers each child of ``tables`` that does NOT ``ON DELETE CASCADE`` (only
    ``columns`` does). Reached by whichever FK column points at ``tables`` —
    single ``table_id``, the fact/view pair, the measure/event pair, or the
    relationship from/to endpoints. ``column_eligibility`` is removed by
    ``source_id`` (it carries it directly).

    Run BEFORE deleting the ``tables`` rows so the FK constraints hold.
    """
    from dataraum.analysis.correlation.db_models import DerivedColumn
    from dataraum.analysis.drivers.db_models import DriverRankingArtifact
    from dataraum.analysis.eligibility.db_models import ColumnEligibilityRecord
    from dataraum.analysis.hierarchies.db_models import DimensionHierarchy
    from dataraum.analysis.lineage.db_models import MeasureAggregationLineage
    from dataraum.analysis.relationships.db_models import Relationship
    from dataraum.analysis.semantic.db_models import TableEntity
    from dataraum.analysis.slicing.db_models import SliceDefinition
    from dataraum.analysis.typing.db_models import MaterializationRecipe
    from dataraum.analysis.views.db_models import EnrichedView
    from dataraum.entropy.db_models import (
        ClaimWitnessRecord,
        EntropyObjectRecord,
        EntropyReadinessRecord,
    )
    from dataraum.investigation.db_models import RunTable

    # Single ``table_id`` FK.
    single_table_keyed = (
        DerivedColumn,
        DimensionHierarchy,
        SliceDefinition,
        MaterializationRecipe,
        TableEntity,
        RunTable,
        ClaimWitnessRecord,
        EntropyObjectRecord,
        EntropyReadinessRecord,
    )
    for model in single_table_keyed:
        ctx.session.execute(delete(model).where(model.table_id.in_(table_ids)))

    # Fact / view pair.
    ctx.session.execute(
        delete(EnrichedView).where(
            or_(
                EnrichedView.fact_table_id.in_(table_ids),
                EnrichedView.view_table_id.in_(table_ids),
            )
        )
    )
    # Measure / event pair.
    ctx.session.execute(
        delete(MeasureAggregationLineage).where(
            or_(
                MeasureAggregationLineage.measure_table_id.in_(table_ids),
                MeasureAggregationLineage.event_table_id.in_(table_ids),
            )
        )
    )
    # Driver rankings are keyed on the measure table.
    ctx.session.execute(
        delete(DriverRankingArtifact).where(DriverRankingArtifact.measure_table_id.in_(table_ids))
    )
    # Relationships reach a table through either endpoint.
    ctx.session.execute(
        delete(Relationship).where(
            or_(
                Relationship.from_table_id.in_(table_ids),
                Relationship.to_table_id.in_(table_ids),
            )
        )
    )

    # Source-keyed children.
    ctx.session.execute(
        delete(ColumnEligibilityRecord).where(ColumnEligibilityRecord.source_id == source_id)
    )
    ctx.session.flush()
