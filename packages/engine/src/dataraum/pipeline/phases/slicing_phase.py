"""Slicing phase implementation.

LLM-powered analysis to identify optimal data slicing dimensions:
- Identifies categorical columns suitable for creating data subsets
- Generates SQL for creating slice tables
- Considers semantic meaning and statistical properties
"""

from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from dataraum.analysis.slicing.agent import SlicingAgent
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.slicing.models import (
    SliceRecommendation,
    SlicingAnalysisResult,
)
from dataraum.core.logging import get_logger
from dataraum.llm import PromptRenderer, create_provider, load_llm_config
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Column, Table
from dataraum.storage.upsert import upsert

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)


@analysis_phase
class SlicingPhase(BasePhase):
    """LLM-powered slicing analysis phase.

    Analyzes tables to identify the best categorical dimensions for
    creating data subsets (slices). Uses statistical profiles,
    semantic annotations, and correlation data as context.

    Requires: statistics, semantic phases.
    """

    @property
    def name(self) -> str:
        return "slicing"

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.slicing import db_models

        return [db_models]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip only when THIS run already produced slice definitions (DAT-448).

        Catalog definitions are run-versioned: a fresh run always re-derives
        against its own enriched views — silent cross-run reuse of stale
        definitions was the DAT-405 bug class. The run-scoped check keeps the
        activity idempotent under Temporal retry.
        """
        fact_tables = self._get_fact_tables(ctx)

        if not fact_tables:
            return "No fact tables with enriched views found"

        table_ids = [t.table_id for t in fact_tables]

        # Check which tables this run already sliced
        sliced_stmt = select(SliceDefinition.table_id.distinct()).where(
            SliceDefinition.table_id.in_(table_ids),
            SliceDefinition.run_id == ctx.run_id,
        )
        sliced_ids = set((ctx.session.execute(sliced_stmt)).scalars().all())

        if len(sliced_ids) >= len(table_ids):
            return "All fact tables already have slice definitions in this run"

        return None

    def _get_fact_tables(self, ctx: PhaseContext) -> list[Table]:
        """Return only the session's typed tables that have an enriched view (fact tables).

        Source-free (feedback-source-dies-at-addsource): scopes by the session's
        selected typed tables (``ctx.table_ids`` via ``BasePhase._typed_tables``),
        which may span sources — never ``source_id`` (None past add_source).
        """
        from dataraum.analysis.views.db_models import EnrichedView

        typed_tables = self._typed_tables(ctx)

        if not typed_tables:
            return []

        # Keep only tables that are fact tables in at least one verified enriched view
        fact_table_ids = set(
            ctx.session.execute(
                select(EnrichedView.fact_table_id.distinct()).where(
                    EnrichedView.fact_table_id.in_([t.table_id for t in typed_tables]),
                    EnrichedView.is_grain_verified.is_(True),
                )
            )
            .scalars()
            .all()
        )

        return [t for t in typed_tables if t.table_id in fact_table_ids]

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run slicing analysis using LLM."""
        # Get only fact tables (those with enriched views) for this source
        fact_tables = self._get_fact_tables(ctx)

        if not fact_tables:
            return PhaseResult.failed(
                "No fact tables with enriched views found. Run enriched_views phase first."
            )

        table_ids = [t.table_id for t in fact_tables]

        # Check which tables THIS run already sliced (run-versioned, DAT-448)
        sliced_stmt = select(SliceDefinition.table_id.distinct()).where(
            SliceDefinition.table_id.in_(table_ids),
            SliceDefinition.run_id == ctx.run_id,
        )
        sliced_ids = set((ctx.session.execute(sliced_stmt)).scalars().all())
        unsliced_tables = [t for t in fact_tables if t.table_id not in sliced_ids]

        if not unsliced_tables:
            return PhaseResult.success(
                outputs={
                    "slice_definitions": 0,
                    "slice_queries": 0,
                    "message": "All tables already have slice definitions in this run",
                },
                records_processed=0,
                records_created=0,
            )

        # Initialize LLM infrastructure. LLM intentionally unavailable (no config,
        # feature disabled) is a documented operating mode — SKIP gracefully so a
        # wired begin_session run proceeds, exactly like ``enriched_views``. A
        # misconfiguration WITH the feature enabled (below) stays a loud failure.
        try:
            config = load_llm_config()
        except FileNotFoundError:
            return PhaseResult.success(
                outputs={"slice_definitions": 0, "message": "LLM config not found, skipping"},
                records_processed=0,
                records_created=0,
                summary="skipped (LLM config not found)",
            )

        # Check if slicing analysis is enabled
        if not config.features.slicing_analysis or not config.features.slicing_analysis.enabled:
            return PhaseResult.success(
                outputs={"slice_definitions": 0, "message": "slicing analysis disabled"},
                records_processed=0,
                records_created=0,
                summary="skipped (slicing analysis disabled)",
            )

        # Create provider. Missing provider config / creation failures ARE
        # misconfigurations now that the feature is enabled — fail loudly.
        provider_config = config.providers.get(config.active_provider)
        if not provider_config:
            return PhaseResult.failed(f"Provider '{config.active_provider}' not configured")

        try:
            provider = create_provider(config.active_provider, provider_config.model_dump())
        except Exception as e:
            return PhaseResult.failed(f"Failed to create LLM provider: {e}")

        # Create other components
        renderer = PromptRenderer()

        # Create slicing agent
        agent = SlicingAgent(
            config=config,
            provider=provider,
            prompt_renderer=renderer,
        )

        # Build context data for the agent
        context_data = self._build_context_data(ctx, unsliced_tables)

        # Pre-filter columns: remove objectively bad slice candidates
        # before sending to LLM (saves tokens, prevents bad recommendations)
        self._pre_filter_columns(context_data)

        # Pass config constraints so the prompt can reference them
        context_data["constraints"] = {
            "max_recommendations": ctx.config.get("max_recommendations", 6),
        }

        # Run slicing analysis
        analysis_result = agent.analyze(
            session=ctx.session,
            table_ids=[t.table_id for t in unsliced_tables],
            context_data=context_data,
        )

        if not analysis_result.success:
            return PhaseResult.failed(analysis_result.error or "Slicing analysis failed")

        slicing = analysis_result.unwrap()

        # Propagate enriched FK dimension recommendations to other tables
        # that share the same dimension column
        slicing = self._propagate_enriched_dimensions(slicing, context_data)

        # Land the agent's time-axis judgments (DAT-491/565): seed
        # ``TableEntity.time_columns`` where semantic_per_table left it empty —
        # gap-closing only, never overriding the earlier judgment. Run-scoped:
        # this run's entity row, same version axis as the rest of the spine.
        if slicing.time_columns:
            from dataraum.analysis.semantic.db_models import TableEntity

            id_by_name = {t.table_name: t.table_id for t in unsliced_tables}
            judged_ids = [id_by_name[name] for name in slicing.time_columns if name in id_by_name]
            if judged_ids:
                entities = ctx.session.execute(
                    select(TableEntity).where(
                        TableEntity.table_id.in_(judged_ids),
                        TableEntity.run_id == ctx.run_id,
                    )
                ).scalars()
                name_by_id = {tid: name for name, tid in id_by_name.items()}
                # Referential integrity on the agent's choice: the named column
                # must exist on the table (own or enriched) — a hallucinated name
                # must not land in the canonical field the query agent and
                # resolver consume. Validate against the UNFILTERED universe
                # (``col_id_by_name``, snapshotted before ``_pre_filter_columns``):
                # the prompt filter drops high-cardinality columns as slice-
                # DIMENSION candidates, and a time axis is exactly a high-
                # cardinality column — checking the filtered list deterministically
                # rejects every real enriched date axis (live false-reject:
                # ``journal_lines`` ← ``entry_id__date``, DAT-491).
                known_cols_by_table: dict[str, set[str]] = {
                    t["table_name"]: set(t.get("col_id_by_name", {}))
                    for t in context_data.get("tables", [])
                }
                for entity in entities:
                    if entity.time_columns:
                        continue  # the table already has axes — inherit, never override
                    table_name = name_by_id.get(entity.table_id, "")
                    chosen = slicing.time_columns.get(table_name)
                    if not chosen:
                        continue
                    if chosen not in known_cols_by_table.get(table_name, set()):
                        logger.warning("time_axis_unknown_column", table=table_name, column=chosen)
                        continue
                    # Fallback fires only when semantic found NO axis, so a fresh
                    # single-element list is correct; reassign (not append) so the
                    # JSON column is marked dirty for the flush. Typed per DAT-780:
                    # the one synthesized axis is a genuine event axis and, being the
                    # only one, the table's anchor.
                    entity.time_columns = [
                        {
                            "column": chosen,
                            "aspect": "event",
                            "role": "event",
                            "is_anchor": True,
                            "note": "Event-time axis identified by the slice-agent fallback (semantic phase found none).",
                        }
                    ]
                    logger.info("time_axis_filled", table=table_name, column=chosen)

        # Deterministic backstop (DAT-720): naming the enriched time axis above is
        # an LLM step, and at effort:low Sonnet 5 silently returned it empty —
        # disabling the structural stock/flow witness for header-dated facts
        # (journal_lines ← entry_id__date). But is_dimension_time_column is computed
        # deterministically (the joined header's own event date), so fill
        # TableEntity.time_columns straight from the flag for any analyzed table
        # the agent AND semantic left empty. Never overrides an existing axis;
        # fixes every consumer at the source — lineage, drivers, and the drill.
        from dataraum.analysis.semantic.db_models import TableEntity

        # Read the flag from the pre-filter snapshot, NOT context_data["tables"]:
        # _pre_filter_columns already dropped the high-cardinality date columns, so
        # the flag is only preserved in this captured map (DAT-720).
        flagged_by_table = {
            name: cols
            for name, cols in (context_data.get("dimension_time_axes") or {}).items()
            if cols
        }
        if flagged_by_table:
            id_by_name = {t.table_name: t.table_id for t in unsliced_tables}
            target_ids = [id_by_name[n] for n in flagged_by_table if n in id_by_name]
            if target_ids:
                name_by_id = {tid: n for n, tid in id_by_name.items()}
                for entity in ctx.session.execute(
                    select(TableEntity).where(
                        TableEntity.table_id.in_(target_ids),
                        TableEntity.run_id == ctx.run_id,
                    )
                ).scalars():
                    if entity.time_columns:
                        continue  # semantic or the agent already set it — never override
                    name = name_by_id.get(entity.table_id, "")
                    cols = flagged_by_table.get(name, [])
                    # Typed per DAT-780: each flagged column is a genuine event axis;
                    # ``cols`` is deterministically sorted (see ``dimension_time_axes``),
                    # so anchoring the first is a stable, non-positional-accident choice
                    # for a backstop that has no ranking signal — exactly one anchor.
                    entity.time_columns = [
                        {
                            "column": col,
                            "aspect": "event",
                            "role": "event",
                            "is_anchor": i == 0,
                            "note": (
                                "Event-time axis from the deterministic "
                                "is_dimension_time_column flag (DAT-720 backstop)."
                            ),
                        }
                        for i, col in enumerate(cols)
                    ]
                    logger.info("time_axis_filled_deterministic", table=name, columns=cols)

        # Store slice definitions — form-(a) idempotent writer (DAT-502):
        # in-batch dedup on ``uq_slice_def_table_column_run`` (the agent can
        # emit a dimension twice; propagation adds more), then UPSERT so a
        # Temporal success-redelivery (same run_id) converges. PK omitted so
        # the model's Python-side default applies.
        run_id = ctx.require_run_id()
        # Referenced-dimension identity (DAT-756): the slice's ``column_id`` is the
        # fact's FK column; resolve its FK-target dim table from the enriched view's
        # relationship provenance. An enriched slice name is ``fk__attr`` — the prefix
        # is the FK column (``fk_role``), the suffix is the dim attribute/level. A
        # slice with no grain-safe FK resolves a null identity (folded — DAT-757).
        dim_table_by_fk_col: dict[str, str] = context_data.get("dim_table_by_fk_col", {})
        rows: dict[tuple[str, str | None, str], dict[str, Any]] = {}
        for rec in slicing.recommendations:
            dimension_table_id = dim_table_by_fk_col.get(rec.column_id)
            dimension_attribute: str | None = None
            fk_role: str | None = None
            if dimension_table_id:
                name = rec.column_name or ""
                if "__" in name:
                    # The enriched dim column is ``{fk_column}__{attr}`` (builder.py):
                    # the FK column is the segment before the FIRST ``__``, matching
                    # the codebase convention (``_propagate_enriched_dimensions``,
                    # ``_build_context_data``). Assumes the FK column name itself has
                    # no ``__`` — the same assumption every other split site makes.
                    fk_role, dimension_attribute = name.split("__", 1)
                else:
                    # Slicing directly by the FK key itself — no enriched attribute.
                    fk_role = name or None
            rows[(rec.table_id, rec.column_name, run_id)] = {
                "run_id": run_id,
                "table_id": rec.table_id,
                "column_id": rec.column_id,
                "column_name": rec.column_name,
                "dimension_table_id": dimension_table_id,
                "dimension_attribute": dimension_attribute,
                "fk_role": fk_role,
                "slice_priority": rec.slice_priority,
                "slice_type": "categorical",
                "distinct_values": rec.distinct_values,
                "value_count": rec.value_count,
                "reasoning": rec.reasoning,
                "business_context": rec.business_context,
                "confidence": rec.confidence,
                "detection_source": "llm",
            }
        upsert(
            ctx.session,
            SliceDefinition,
            list(rows.values()),
            index_elements=["table_id", "column_name", "run_id"],
        )

        return PhaseResult.success(
            outputs={
                "slice_definitions": len(slicing.recommendations),
                "tables_analyzed": [t.table_name for t in unsliced_tables],
            },
            records_processed=len(unsliced_tables),
            records_created=len(slicing.recommendations),
            summary=f"{len(slicing.recommendations)} slice definitions",
        )

    def _pre_filter_columns(self, context_data: dict[str, Any]) -> None:
        """Remove columns that are objectively bad slice candidates.

        Mutates context_data in place, removing columns with:
        - distinct_count > 200 (too high cardinality for slicing)
        - null_ratio > 0.5 (majority NULL)
        - cardinality_ratio > 0.5 (approaching identifier territory)

        Enriched dimension columns are exempt from the cardinality_ratio
        check since they are specifically designed for analytical grouping.

        Preserves a ``col_id_by_name`` lookup per table so that
        ``_propagate_enriched_dimensions`` can resolve FK column_ids
        even after the FK column itself was filtered out.
        """
        for table_data in context_data.get("tables", []):
            original = table_data.get("columns", [])

            # Snapshot column_id by name before filtering — propagation needs
            # FK column_ids that the filter removes (high cardinality).
            table_data["col_id_by_name"] = {
                col["column_name"]: col.get("column_id", "")
                for col in original
                if col.get("column_id")
            }

            filtered = []
            for col in original:
                distinct = col.get("distinct_count")
                null_ratio = col.get("null_ratio")
                card_ratio = col.get("cardinality_ratio")
                is_enriched = col.get("is_enriched_dimension", False)

                if distinct is not None and distinct > 200:
                    continue
                if null_ratio is not None and null_ratio > 0.5:
                    continue
                if not is_enriched and card_ratio is not None and card_ratio > 0.5:
                    continue

                filtered.append(col)

            if len(filtered) < len(original):
                logger.debug(
                    "pre_filtered_columns",
                    table=table_data.get("table_name"),
                    removed=len(original) - len(filtered),
                    kept=len(filtered),
                )
            table_data["columns"] = filtered

    def _propagate_enriched_dimensions(
        self,
        result: SlicingAnalysisResult,
        context_data: dict[str, Any],
    ) -> SlicingAnalysisResult:
        """Copy enriched FK dim recommendations to all tables sharing the same dimension column.

        When the LLM recommends an enriched dimension (e.g. ``account_id__account_type``)
        for one fact table, this method finds other fact tables that also have that
        enriched column and creates matching catalog recommendations for them.

        Args:
            result: LLM slicing analysis result.
            context_data: Context data with table/column metadata.

        Returns:
            Updated result with propagated recommendations.
        """
        tables_data = context_data.get("tables", [])
        if len(tables_data) < 2:
            return result

        # Build lookup: dim_column_name → list of table dicts that have it
        dim_col_to_tables: dict[str, list[dict[str, Any]]] = {}
        for tdata in tables_data:
            for col in tdata.get("columns", []):
                if col.get("is_enriched_dimension") and "__" in col.get("column_name", ""):
                    dim_col_to_tables.setdefault(col["column_name"], []).append(tdata)

        # Track which (table_name, column_name) combos already have recommendations
        existing_recs: set[tuple[str, str]] = set()
        for rec in result.recommendations:
            existing_recs.add((rec.table_name, rec.column_name))

        new_recs: list[SliceRecommendation] = []

        for rec in result.recommendations:
            col_name = rec.column_name
            if "__" not in col_name:
                continue

            candidate_tables = dim_col_to_tables.get(col_name, [])
            for tdata in candidate_tables:
                target_table_name = tdata["table_name"]
                if (target_table_name, col_name) in existing_recs:
                    continue

                # Resolve FK column_id from the pre-filter snapshot — the FK
                # column itself is typically filtered out (high cardinality).
                fk_prefix = col_name.split("__")[0]
                target_col_id = tdata.get("col_id_by_name", {}).get(fk_prefix, "")

                if not target_col_id:
                    continue

                new_rec = SliceRecommendation(
                    table_id=tdata.get("table_id", ""),
                    table_name=target_table_name,
                    column_id=target_col_id,
                    column_name=col_name,
                    slice_priority=rec.slice_priority,
                    distinct_values=rec.distinct_values,
                    value_count=rec.value_count,
                    reasoning=f"Propagated from {rec.table_name}: {rec.reasoning}",
                    business_context=rec.business_context,
                    confidence=rec.confidence,
                )
                new_recs.append(new_rec)
                existing_recs.add((target_table_name, col_name))

                logger.info(
                    "propagated_enriched_dimension",
                    column=col_name,
                    from_table=rec.table_name,
                    to_table=target_table_name,
                )

        if new_recs:
            result.recommendations.extend(new_recs)

        return result

    def _build_context_data(self, ctx: PhaseContext, tables: list[Table]) -> dict[str, Any]:
        """Build context data for the slicing agent.

        Statistics and semantic annotations are merged directly into each column dict
        to eliminate cross-referencing in the prompt and reduce token usage.

        Enriched FK-prefixed dimension columns (e.g. ``fk_col__dim_col``) are appended
        to the fact table's column list so the LLM can recommend them as slice candidates.
        Their ``column_id`` is set to the FK column's column_id (the prefix part) because
        enriched dim columns are not yet individually registered as Column records.
        """
        from dataraum.analysis.relationships.db_models import Relationship
        from dataraum.analysis.semantic.db_models import SemanticAnnotation, TableEntity
        from dataraum.analysis.statistics.db_models import StatisticalProfile
        from dataraum.analysis.views.db_models import EnrichedView

        table_ids = [t.table_id for t in tables]
        tables_data = []
        column_count = 0

        # Pre-load enriched views for all tables so we can merge dim cols per-table
        ev_by_fact: dict[str, EnrichedView | None] = {}
        try:
            ev_stmt = select(EnrichedView).where(
                EnrichedView.fact_table_id.in_(table_ids),
                EnrichedView.is_grain_verified.is_(True),
            )
            for ev in ctx.session.execute(ev_stmt).scalars().all():
                ev_by_fact[ev.fact_table_id] = ev
        except Exception:
            pass  # Enriched views not available, proceed without

        # Time-axis context (DAT-491/565): each table's already-identified
        # time_columns (this run's TableEntity — the semantic_per_table judgment
        # the agent INHERITS), plus the joined dimension tables' time columns so an
        # enriched "fk__col" entry can be flagged as the header's event date.
        dim_table_ids = {
            tid for ev in ev_by_fact.values() if ev for tid in (ev.dimension_table_ids or [])
        }
        entity_stmt = select(TableEntity).where(
            TableEntity.table_id.in_(list(set(table_ids) | dim_table_ids))
        )
        if ctx.run_id is not None:
            entity_stmt = entity_stmt.where(TableEntity.run_id == ctx.run_id)
        time_col_by_table: dict[str, list[dict[str, Any]]] = {
            e.table_id: (e.time_columns or []) for e in ctx.session.execute(entity_stmt).scalars()
        }
        # FK column -> joined dimension table, from the enriched views' own
        # relationship provenance (never name-inferred). This is also the referenced-
        # dimension identity source (DAT-756): the slice's ``column_id`` is the fact's
        # FK column, so this map resolves ``FK column -> dim table``. The relationships
        # an enriched view references are grain-safe BY CONSTRUCTION — a view is only
        # built from grain-verified many-to-one joins — so a fan-out join can never
        # mint a dimension identity here; no extra cardinality filter is needed.
        rel_ids = [rid for ev in ev_by_fact.values() if ev for rid in (ev.relationship_ids or [])]
        dim_table_by_fk_col: dict[str, str] = {}
        if rel_ids:
            for rel in ctx.session.execute(
                select(Relationship).where(Relationship.relationship_id.in_(rel_ids))
            ).scalars():
                dim_table_by_fk_col[rel.from_column_id] = rel.to_table_id

        for table in tables:
            # Get columns for this table
            col_stmt = select(Column).where(Column.table_id == table.table_id)
            columns = list((ctx.session.execute(col_stmt)).scalars().all())
            column_count += len(columns)

            col_ids = [c.column_id for c in columns]

            columns_list: list[dict[str, Any]] = [
                {
                    "column_id": col.column_id,
                    "column_name": col.column_name,
                    "raw_type": col.raw_type,
                    "resolved_type": col.resolved_type,
                }
                for col in columns
            ]

            # Build lookup from column_id -> column dict
            col_dict_by_id = {
                col.column_id: col_dict for col, col_dict in zip(columns, columns_list, strict=True)
            }
            # Also build lookup from column_name -> column_id (for FK prefix resolution)
            col_id_by_name = {col.column_name: col.column_id for col in columns}

            # Merge statistical profiles into columns
            stats_stmt = select(StatisticalProfile).where(StatisticalProfile.column_id.in_(col_ids))
            for profile in (ctx.session.execute(stats_stmt)).scalars().all():
                col_dict = col_dict_by_id.get(profile.column_id)
                if col_dict:
                    profile_data = profile.profile_data or {}
                    col_dict["total_count"] = profile.total_count
                    col_dict["null_count"] = profile.null_count
                    col_dict["null_ratio"] = profile.null_ratio
                    col_dict["distinct_count"] = profile.distinct_count
                    col_dict["cardinality_ratio"] = profile.cardinality_ratio
                    col_dict["top_values"] = profile_data.get("top_values", [])

            # Merge semantic annotations into columns
            sem_stmt = select(SemanticAnnotation).where(SemanticAnnotation.column_id.in_(col_ids))
            for ann in (ctx.session.execute(sem_stmt)).scalars().all():
                col_dict = col_dict_by_id.get(ann.column_id)
                if col_dict:
                    col_dict["semantic_role"] = ann.semantic_role
                    col_dict["entity_type"] = ann.entity_type
                    col_dict["business_name"] = ann.business_name
                    col_dict["business_description"] = ann.business_description

            # Append enriched dimension columns from the enriched view's
            # registered Table + Column records (persisted during enriched_views phase).
            # Stats come from StatisticalProfile — no ad-hoc DuckDB queries needed.
            table_ev = ev_by_fact.get(table.table_id)
            if table_ev and table_ev.view_table_id and table_ev.dimension_columns:
                # Load Column records for dimension columns
                dim_col_stmt = select(Column).where(Column.table_id == table_ev.view_table_id)
                dim_cols = list(ctx.session.execute(dim_col_stmt).scalars().all())
                dim_col_ids = [c.column_id for c in dim_cols]

                # Load their StatisticalProfiles
                dim_profiles: dict[str, StatisticalProfile] = {}
                if dim_col_ids:
                    prof_stmt = select(StatisticalProfile).where(
                        StatisticalProfile.column_id.in_(dim_col_ids)
                    )
                    for prof in ctx.session.execute(prof_stmt).scalars().all():
                        dim_profiles[prof.column_id] = prof

                for dim_col in dim_cols:
                    fk_prefix = (
                        dim_col.column_name.split("__")[0] if "__" in dim_col.column_name else None
                    )
                    fk_col_id = col_id_by_name.get(fk_prefix) if fk_prefix else None
                    # Flag the joined dimension's event date (DAT-491): this
                    # enriched column IS the header table's identified time
                    # column, resolved through the view's relationship — the
                    # agent names it as the fact's time axis when the fact has
                    # no own time_column.
                    dim_table_id = dim_table_by_fk_col.get(fk_col_id or "")
                    dim_suffix = dim_col.column_name.split("__", 1)[1] if fk_prefix else None
                    # Plural (DAT-565): the enriched suffix is a time axis if it
                    # matches ANY of the dim table's event-time columns. (`x in
                    # set` already short-circuits on a falsy ``dim_suffix``.)
                    is_dim_time = bool(
                        dim_table_id
                        and dim_suffix
                        in {tc.get("column") for tc in time_col_by_table.get(dim_table_id, [])}
                    )
                    dim_entry: dict[str, Any] = {
                        "column_id": fk_col_id or dim_col.column_id,
                        "column_name": dim_col.column_name,
                        "is_enriched_dimension": True,
                        "fk_column_name": fk_prefix,
                        "is_dimension_time_column": is_dim_time,
                    }
                    dim_prof = dim_profiles.get(dim_col.column_id)
                    if dim_prof:
                        profile_data = dim_prof.profile_data or {}
                        dim_entry["total_count"] = dim_prof.total_count
                        dim_entry["null_count"] = dim_prof.null_count
                        dim_entry["null_ratio"] = dim_prof.null_ratio
                        dim_entry["distinct_count"] = dim_prof.distinct_count
                        dim_entry["cardinality_ratio"] = dim_prof.cardinality_ratio
                        dim_entry["top_values"] = profile_data.get("top_values", [])
                    columns_list.append(dim_entry)
                    column_count += 1

            enriched_view_name = table_ev.view_name if table_ev else None

            tables_data.append(
                {
                    "table_id": table.table_id,
                    "table_name": table.table_name,
                    "duckdb_path": table.duckdb_path,
                    "row_count": table.row_count,
                    # The already-identified time axes the agent INHERITS
                    # (DAT-491/565); empty = the agent judges, from the flagged
                    # enriched columns.
                    "time_columns": time_col_by_table.get(table.table_id, []),
                    "columns": columns_list,
                    # Use enriched view if available, otherwise use typed table
                    "enriched_view_name": enriched_view_name,
                    "enriched_duckdb_path": enriched_view_name if table_ev else None,
                }
            )

        # Flagged enriched time axes per table, captured BEFORE _pre_filter_columns
        # drops high-cardinality columns (a date is exactly that) — the deterministic
        # backstop in _run reads this so the is_dimension_time_column flag survives
        # the filter (DAT-720; the filter is why the agent's write-back also has to
        # validate against the unfiltered col_id_by_name).
        dimension_time_axes: dict[str, list[str]] = {}
        for t in tables_data:
            cols = t["columns"]
            name = t["table_name"]
            if not isinstance(cols, list) or not isinstance(name, str):
                continue
            axes = sorted(
                {
                    c["column_name"]
                    for c in cols
                    if isinstance(c, dict)
                    and c.get("is_dimension_time_column")
                    and c.get("column_name")
                }
            )
            if axes:
                dimension_time_axes[name] = axes

        return {
            "tables": tables_data,
            "column_count": column_count,
            "dimension_time_axes": dimension_time_axes,
            # FK column_id -> dim table_id (DAT-756): the referenced-dimension
            # identity source, consumed at slice-write time in ``_run``.
            "dim_table_by_fk_col": dim_table_by_fk_col,
        }
