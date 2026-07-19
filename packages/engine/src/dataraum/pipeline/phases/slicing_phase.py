"""Slicing phase implementation.

Persists the dimension inventory deterministically and ranks it with an LLM
(DAT-725 rescope — existence vs. enrichment):

- **Existence is deterministic**: every grain-safe pre-filter survivor
  (DAT-805 gates) whose ``semantic_role`` is not measure/timestamp becomes a
  ``SliceDefinition``. Same data + same code ⇒ every COMPLETED run persists
  the same set (a ranker runtime failure fails the whole activity loudly;
  Temporal retries it — never a partially-ranked or elected subset).
- **The agent is a ranker, not an elector**: its priority/context/reasoning/
  confidence merge onto rows that exist regardless; un-ranked rows carry the
  ``UNRANKED_SLICE_PRIORITY`` floor. LLM-unavailable modes (no config, feature
  disabled) skip only the ranking — the inventory and the deterministic
  time-axis backstop (DAT-720) still land.
"""

from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from dataraum.analysis.slicing.agent import SlicingAgent
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.slicing.models import (
    UNRANKED_SLICE_PRIORITY,
    SliceRecommendation,
    SlicingAnalysisResult,
)
from dataraum.analysis.views.served_columns import enriched_dimension_columns
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

# Slice-candidate eligibility (DAT-805 — mirrors the hierarchies near-key gate).
# The pre-filter's contract: every column reaching the LLM (hence every cataloged
# SliceDefinition) is grain-safe to aggregate — no fan-out, not degenerate. It
# excludes ONLY the DEFINITIVE extremes: a constant (nothing to partition) or a
# near-UNIQUE key (~one row per group). The ceiling is a FRACTION of NON-NULL rows
# (cardinality_ratio = distinct / non-null count — the _MAX_NULL_RATIO gate handles
# nullness separately, so dividing by total rows would double-count it), never an
# absolute count: an absolute cap drops a 400-value discriminator in a 10M-row
# table while passing a 150-value near-key in 160 rows (the old `distinct > 200`
# bug). Mid-cardinality columns are KEPT; thin per-group support is folded by the
# driver-tree's min_support (the answer agent + metrics surface them as-is —
# grain-safe, just fine-grained).
_MIN_DISTINCT_DIMENSION = 2  # a single value (+ NULL bucket) is not a slice axis
_NEAR_KEY_FRAC = 0.9  # distinct >= 0.9 * non-null rows => near-unique key (spurious)
_MAX_NULL_RATIO = 0.5  # majority-NULL => most rows fall in the NULL bucket

# Semantic-role existence gate (DAT-725): eligibility = grain-safe pre-filter
# survivors ∩ semantic_role ∉ this set. A closed vocabulary (semantic/models.py
# ``Literal["key", "measure", "dimension", "timestamp", "attribute"]``), read
# from the persisted annotations — existing deterministic metadata, no LLM call.
# The measure/timestamp cut is the semantic filter the LLM election used to
# provide. The set is EXACTLY {measure, timestamp} — pre-registered in the
# DAT-725 design; the two deliberate keeps:
# - ``key``: a FOLDED dimension key (account_id inlined on a fact grain) is
#   precisely a key with no resolved FK — gating on "key without FK" would
#   re-open the nondeterministic-existence hole this rescope closes.
#   Degenerate PKs already die as near-keys in the pre-filter.
# - ``attribute``: the label a FOLDED descriptive dimension member draws
#   (account_name inlined next to its key) — the role is judged per-table by
#   an LLM, so "not used for grouping" is a soft read, not a structural fact;
#   excluding it would re-open the same hole in its descriptive form. A kept
#   attribute is grain-safe by the pre-filter; the priority floor + the
#   curation budget keep it out of LLM context, and the driver-tree's
#   min_support folds it if its support is thin.
_EXCLUDED_SLICE_ROLES = frozenset({"measure", "timestamp"})


def _has_event_axis(time_columns: list[dict[str, Any]] | None) -> bool:
    """True when the entity already carries a genuine EVENT time axis (DAT-780).

    The backstops fill an event axis only when semantic found none. Under the
    event/attribute contract an attribute-only ``time_columns`` list is NOT an
    event axis, so the guard must test role='event' membership — a bare
    truthiness check would let an attribute date (due_date) suppress a real
    event-axis backstop.
    """
    return any(tc.get("role") == "event" for tc in (time_columns or []))


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

        # The deterministic inventory comes first (DAT-725): context + grain-safe
        # pre-filter (DAT-805) + semantic-role gate. What survives IS the eligible
        # set — persisted below whether or not a ranker runs, and exactly what the
        # ranker sees ("rank the most interesting of these").
        context_data = self._build_context_data(ctx, unsliced_tables)
        self._pre_filter_columns(context_data)
        self._exclude_non_dimension_roles(context_data)

        # The agent is a RANKER (enrichment), not an elector. LLM intentionally
        # unavailable (no config, feature disabled) is a documented operating
        # mode — skip ONLY the ranking; the inventory and the deterministic
        # time-axis backstop still land. A misconfiguration WITH the feature
        # enabled (below) stays a loud failure.
        slicing: SlicingAnalysisResult | None = None
        ranking_skipped: str | None = None
        try:
            config = load_llm_config()
        except FileNotFoundError:
            config = None
            ranking_skipped = "LLM config not found"
        if config is not None and (
            not config.features.slicing_analysis or not config.features.slicing_analysis.enabled
        ):
            config = None
            ranking_skipped = "slicing analysis disabled"

        if config is not None:
            # Create provider. Missing provider config / creation failures ARE
            # misconfigurations now that the feature is enabled — fail loudly.
            provider_config = config.providers.get(config.active_provider)
            if not provider_config:
                return PhaseResult.failed(f"Provider '{config.active_provider}' not configured")

            try:
                provider = create_provider(config.active_provider, provider_config.model_dump())
            except Exception as e:
                return PhaseResult.failed(f"Failed to create LLM provider: {e}")

            agent = SlicingAgent(
                config=config,
                provider=provider,
                prompt_renderer=PromptRenderer(),
            )

            # Pass config constraints so the prompt can reference them
            context_data["constraints"] = {
                "max_recommendations": ctx.config.get("max_recommendations", 6),
            }

            # Run the ranking
            analysis_result = agent.analyze(
                session=ctx.session,
                table_ids=[t.table_id for t in unsliced_tables],
                context_data=context_data,
            )

            # A ranker RUNTIME failure (render error, tool misuse, validation)
            # stays a loud, whole-activity failure — deliberately NOT a
            # "ranking skipped" fallback: Temporal retries the activity and the
            # retry converges to inventory + enrichment atomically. Persisting
            # the inventory before the ranking instead would strand the run
            # permanently unranked — the run-scoped guard above would skip the
            # retry as "already sliced". The determinism claim is about
            # COMPLETED runs: a failed run persists nothing and says so.
            if not analysis_result.success:
                return PhaseResult.failed(analysis_result.error or "Slicing analysis failed")

            slicing = analysis_result.unwrap()

            # Propagate enriched FK dimension rankings to other tables that share
            # the same dimension column (curation alignment — existence no longer
            # depends on it: every fact's own eligible set is persisted anyway).
            slicing = self._propagate_enriched_dimensions(slicing, context_data)
        else:
            logger.info("slice_ranking_skipped", reason=ranking_skipped)

        # Land the agent's time-axis judgments (DAT-491/565): seed
        # ``TableEntity.time_columns`` where semantic_per_table left it empty —
        # gap-closing only, never overriding the earlier judgment. Run-scoped:
        # this run's entity row, same version axis as the rest of the spine.
        # (Ranker-skipped runs land no agent judgments; the deterministic
        # DAT-720 backstop below still fires.)
        if slicing is not None and slicing.time_columns:
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
                    if _has_event_axis(entity.time_columns):
                        continue  # already has an EVENT axis — inherit, never override
                    table_name = name_by_id.get(entity.table_id, "")
                    chosen = slicing.time_columns.get(table_name)
                    if not chosen:
                        continue
                    if chosen not in known_cols_by_table.get(table_name, set()):
                        logger.warning("time_axis_unknown_column", table=table_name, column=chosen)
                        continue
                    # Fires only when semantic found no EVENT axis (DAT-780): the one
                    # synthesized axis is a genuine event axis and, being the only
                    # event axis, the table's anchor. PRESERVE any attribute-role
                    # dates semantic did emit (they get coverage; they are not event
                    # axes); reassign a new list (not in-place append) so the JSON
                    # column is marked dirty for the flush.
                    entity.time_columns = [
                        *(entity.time_columns or []),
                        {
                            "column": chosen,
                            "aspect": "event",
                            "role": "event",
                            "is_anchor": True,
                            "note": "Event-time axis identified by the slice-agent fallback (semantic phase found none).",
                        },
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
                    if _has_event_axis(entity.time_columns):
                        continue  # already has an EVENT axis — never override (DAT-780)
                    name = name_by_id.get(entity.table_id, "")
                    cols = flagged_by_table.get(name, [])
                    # Typed per DAT-780: each flagged column is a genuine event axis;
                    # ``cols`` is deterministically sorted (see ``dimension_time_axes``),
                    # so anchoring the first is a stable, non-positional-accident choice
                    # for a backstop that has no ranking signal — exactly one anchor.
                    # PRESERVE any attribute-role dates semantic emitted (coverage-only,
                    # never event axes).
                    entity.time_columns = [
                        *(entity.time_columns or []),
                        *(
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
                        ),
                    ]
                    logger.info("time_axis_filled_deterministic", table=name, columns=cols)

        # Store slice definitions — the ELIGIBLE SET is the inventory (DAT-725):
        # one row per grain-safe non-measure/non-timestamp column, with the
        # ranker's output merged on as enrichment. Form-(a) idempotent writer
        # (DAT-502): in-batch dedup on ``uq_slice_def_table_column_run``, then
        # UPSERT so a Temporal success-redelivery (same run_id) converges. PK
        # omitted so the model's Python-side default applies.
        run_id = ctx.require_run_id()

        # The ranker's enrichment by (table_id, column_name). Grounding in the
        # agent + propagation both operate on the same filtered context, so every
        # rec targets an eligible row; ties keep the later rec (the pre-rescope
        # writer's last-wins), a better (lower) rank always wins.
        ranked: dict[tuple[str, str], SliceRecommendation] = {}
        if slicing is not None:
            for rec in slicing.recommendations:
                if not rec.column_name:
                    continue
                prev = ranked.get((rec.table_id, rec.column_name))
                if prev is None or rec.slice_priority <= prev.slice_priority:
                    ranked[(rec.table_id, rec.column_name)] = rec

        # Referenced-dimension identity (DAT-756): the slice's ``column_id`` is the
        # fact's FK column; resolve its FK-target dim table from the enriched view's
        # relationship provenance. An enriched slice name is ``fk__attr`` — the prefix
        # is the FK column (``fk_role``), the suffix is the dim attribute/level. A
        # slice with no grain-safe FK resolves a null identity (folded — DAT-757).
        # Same block as pre-rescope; it now iterates the eligible set (superset
        # input) instead of the elected recommendations.
        dim_table_by_fk_col: dict[str, str] = context_data.get("dim_table_by_fk_col", {})
        rows: dict[tuple[str, str, str], dict[str, Any]] = {}
        for table_data in context_data.get("tables", []):
            table_id = table_data.get("table_id", "")
            for col in table_data.get("columns", []):
                column_id = col.get("column_id") or ""
                column_name = col.get("column_name") or ""
                if not table_id or not column_id or not column_name:
                    continue
                dimension_table_id = dim_table_by_fk_col.get(column_id)
                dimension_attribute: str | None = None
                fk_role: str | None = None
                if dimension_table_id:
                    if "__" in column_name:
                        # The enriched dim column is ``{fk_column}__{attr}`` (builder.py):
                        # the FK column is the segment before the FIRST ``__``, matching
                        # the codebase convention (``_propagate_enriched_dimensions``,
                        # ``_build_context_data``). Assumes the FK column name itself has
                        # no ``__`` — the same assumption every other split site makes.
                        fk_role, dimension_attribute = column_name.split("__", 1)
                    else:
                        # Slicing directly by the FK key itself — no enriched attribute.
                        fk_role = column_name or None
                rank = ranked.get((table_id, column_name))
                if rank is not None and rank.distinct_values:
                    distinct_values = rank.distinct_values
                else:
                    # Structural rows carry the profile's top values as the value
                    # evidence (the same fallback the agent applies to a ranked row
                    # without values); value_count stays the honest full distinct
                    # count, which can exceed the bounded list.
                    distinct_values = [
                        str(v.get("value", "")) for v in (col.get("top_values") or [])
                    ]
                rows[(table_id, column_name, run_id)] = {
                    "run_id": run_id,
                    "table_id": table_id,
                    "column_id": column_id,
                    "column_name": column_name,
                    "dimension_table_id": dimension_table_id,
                    "dimension_attribute": dimension_attribute,
                    "fk_role": fk_role,
                    "slice_priority": rank.slice_priority if rank else UNRANKED_SLICE_PRIORITY,
                    "slice_type": "categorical",
                    "distinct_values": distinct_values,
                    "value_count": rank.value_count if rank else col.get("distinct_count"),
                    "reasoning": rank.reasoning if rank else None,
                    "business_context": rank.business_context if rank else None,
                    "confidence": rank.confidence if rank else None,
                    "detection_source": "llm" if rank else "structural",
                }
        upsert(
            ctx.session,
            SliceDefinition,
            list(rows.values()),
            index_elements=["table_id", "column_name", "run_id"],
        )

        n_ranked = sum(1 for row in rows.values() if row["detection_source"] == "llm")
        summary = f"{len(rows)} slice definitions ({n_ranked} ranked)"
        if ranking_skipped:
            summary += f" — ranking skipped: {ranking_skipped}"
        return PhaseResult.success(
            outputs={
                "slice_definitions": len(rows),
                "ranked": n_ranked,
                "tables_analyzed": [t.table_name for t in unsliced_tables],
                **({"message": f"ranking skipped: {ranking_skipped}"} if ranking_skipped else {}),
            },
            records_processed=len(unsliced_tables),
            records_created=len(rows),
            summary=summary,
        )

    def _pre_filter_columns(self, context_data: dict[str, Any]) -> None:
        """Exclude columns that are DEFINITIVELY unusable as slice dimensions.

        Mutates ``context_data`` in place. The pre-filter's contract is that
        every column reaching the LLM — hence every cataloged ``SliceDefinition``
        — is grain-safe AND useful to aggregate, so no downstream consumer
        re-checks. It therefore drops ONLY the definitive extremes, born-loud:

        - a **constant** (``< _MIN_DISTINCT_DIMENSION`` distinct — nothing to
          partition);
        - a **majority-NULL** column (``null_ratio > _MAX_NULL_RATIO`` — most
          rows fall in the NULL bucket);
        - a **near-unique key** (``cardinality_ratio >= _NEAR_KEY_FRAC`` — ~one
          row per group, a degenerate slice). The ceiling is a FRACTION of rows,
          never an absolute count (DAT-805: the old ``distinct > 200`` dropped a
          400-value discriminator in a 10M-row table while passing a 150-value
          near-key in 160 rows; the old ``cardinality_ratio > 0.5`` silently
          killed legitimate mid-cardinality dimensions well below near-unique).

        The near-key check is uniform — an enriched dimension is grain-safe by its
        join, but a near-unique enriched column (a raw date axis, a per-row name)
        is just as useless a slice as an own near-key. Mid-cardinality columns are
        KEPT; thin per-group support is folded downstream by the driver-tree, not
        silently pre-dropped here.

        Preserves a ``col_id_by_name`` lookup per table so
        ``_propagate_enriched_dimensions`` and the DAT-491 time-axis validation
        resolve column_ids even for a column the filter removed (a high-cardinality
        date axis is exactly such a column).
        """
        for table_data in context_data.get("tables", []):
            original = table_data.get("columns", [])

            # Snapshot column_id by name before filtering — propagation + the
            # DAT-491 time-axis check need FK/axis column_ids the filter removes.
            table_data["col_id_by_name"] = {
                col["column_name"]: col.get("column_id", "")
                for col in original
                if col.get("column_id")
            }

            filtered = []
            for col in original:
                distinct = col.get("distinct_count")
                null_ratio = col.get("null_ratio")
                null_count = col.get("null_count") or 0
                card_ratio = col.get("cardinality_ratio")
                name = col.get("column_name")

                # Floor: a constant has nothing to partition — but NULL is its own
                # slice bucket, so a {value, NULL} column is a valid 2-way split, not
                # a constant. distinct_count is null-blind (COUNT(DISTINCT col),
                # profiler.py), so add the NULL category back before the floor.
                if (
                    distinct is not None
                    and distinct + (1 if null_count else 0) < _MIN_DISTINCT_DIMENSION
                ):
                    logger.info(
                        "slice_column_excluded", column=name, reason="constant", distinct=distinct
                    )
                    continue
                # Coverage: a majority-NULL column slices most rows into NULL.
                if null_ratio is not None and null_ratio > _MAX_NULL_RATIO:
                    logger.info(
                        "slice_column_excluded",
                        column=name,
                        reason="mostly_null",
                        null_ratio=null_ratio,
                    )
                    continue
                # Ceiling: a near-UNIQUE column (distinct ~ rows) yields ~one row
                # per group — a degenerate slice. Scale-invariant fraction, NOT an
                # absolute count. Applied uniformly: an enriched dim is grain-safe
                # by its join, but a near-unique enriched column (a raw date axis, a
                # per-row name) is just as useless a slice as an own near-key.
                if card_ratio is not None and card_ratio >= _NEAR_KEY_FRAC:
                    logger.info(
                        "slice_column_excluded",
                        column=name,
                        reason="near_key",
                        cardinality_ratio=card_ratio,
                    )
                    continue

                filtered.append(col)

            table_data["columns"] = filtered

    def _exclude_non_dimension_roles(self, context_data: dict[str, Any]) -> None:
        """Drop measures and timestamps from the slice-candidate set (DAT-725).

        Mutates ``context_data`` in place, AFTER ``_pre_filter_columns``:
        eligibility = grain-safe survivors ∩ ``semantic_role ∉ _EXCLUDED_SLICE_
        ROLES`` — the semantic filter the LLM election used to provide, now
        served deterministically from the persisted annotations. What survives
        is BOTH the persisted inventory and the ranker's candidate list, so the
        prompt's "rank the most interesting of these" is literally true.

        Columns with no annotation stay eligible (fail-open — no exclusion
        evidence; annotations are object-grain metadata, not a gate). Keys stay:
        a folded dimension key is a key with no FK (see ``_EXCLUDED_SLICE_ROLES``).
        Exclusions are born loud, same as the pre-filter's.
        """
        for table_data in context_data.get("tables", []):
            kept = []
            for col in table_data.get("columns", []):
                role = col.get("semantic_role")
                if role in _EXCLUDED_SLICE_ROLES:
                    logger.info(
                        "slice_column_excluded",
                        column=col.get("column_name"),
                        reason="semantic_role",
                        semantic_role=role,
                    )
                    continue
                kept.append(col)
            table_data["columns"] = kept

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

        # Run-staleness scoping for the role gate (DAT-725 × DAT-413):
        # ``SemanticAnnotation`` is run-versioned — a replay/teach leaves >1 row
        # per column across runs — and ``semantic_role`` now gates slice
        # EXISTENCE, so an arbitrary-run row pick would make existence flap on
        # exactly the axis this rescope pins down. Filter every annotation
        # through its table's promoted add_source generation head (the
        # ``graphs/context.py`` ``_is_current`` pattern): a table with no
        # promoted head keeps what's there — no "current" to scope to.
        from dataraum.storage.snapshot_head import GENERATION_STAGE, head_run_id

        gen_head_by_table: dict[str, str | None] = {
            tid: head_run_id(ctx.session, f"table:{tid}", GENERATION_STAGE)
            for tid in set(table_ids) | dim_table_ids
        }

        def _is_current(ann_table_id: str, ann_run_id: str) -> bool:
            want = gen_head_by_table.get(ann_table_id)
            return want is None or ann_run_id == want

        # Dim source-column semantic roles, keyed (dim_table_id, column_name) —
        # the DAT-725 role gate needs a role for enriched ``fk__attr`` entries,
        # and enriched-view Column records carry no annotations of their own
        # (they are registered after the semantic phase). Resolve through the
        # view's relationship provenance to the DIM table's ``attr`` column,
        # head-scoped as above.
        dim_role_by_attr: dict[tuple[str, str], str] = {}
        if dim_table_ids:
            dim_col_ident = {
                c.column_id: (c.table_id, c.column_name)
                for c in ctx.session.execute(
                    select(Column).where(Column.table_id.in_(list(dim_table_ids)))
                ).scalars()
            }
            if dim_col_ident:
                for ann in ctx.session.execute(
                    select(SemanticAnnotation).where(
                        SemanticAnnotation.column_id.in_(list(dim_col_ident))
                    )
                ).scalars():
                    ann_table_id, ann_col_name = dim_col_ident[ann.column_id]
                    if ann.semantic_role is not None and _is_current(ann_table_id, ann.run_id):
                        dim_role_by_attr[(ann_table_id, ann_col_name)] = ann.semantic_role

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

            # Merge semantic annotations into columns — head-scoped via
            # ``_is_current`` (see above): ``semantic_role`` gates existence now,
            # so a stale run's coexisting row must not win an arbitrary scan.
            sem_stmt = select(SemanticAnnotation).where(SemanticAnnotation.column_id.in_(col_ids))
            for ann in (ctx.session.execute(sem_stmt)).scalars().all():
                if not _is_current(table.table_id, ann.run_id):
                    continue
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
                # Only the JOINED dimension columns — the enriched view also registers the
                # fact's own f.* passthrough columns (DAT-811), already on this fact.
                dim_cols = enriched_dimension_columns(ctx.session, table_ev.view_table_id)
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
                    # matches ANY of the dim table's EVENT-time columns. EVENT-role
                    # only (DAT-780) — an attribute date on the dim (valid_until) must
                    # never be promoted to a fact's event axis by the backstop below.
                    # (`x in set` already short-circuits on a falsy ``dim_suffix``.)
                    is_dim_time = bool(
                        dim_table_id
                        and dim_suffix
                        in {
                            tc.get("column")
                            for tc in time_col_by_table.get(dim_table_id, [])
                            if tc.get("role") == "event"
                        }
                    )
                    dim_entry: dict[str, Any] = {
                        "column_id": fk_col_id or dim_col.column_id,
                        "column_name": dim_col.column_name,
                        "is_enriched_dimension": True,
                        "fk_column_name": fk_prefix,
                        "is_dimension_time_column": is_dim_time,
                    }
                    # The dim SOURCE column's semantic role (DAT-725 gate input);
                    # unresolvable provenance leaves it unset — fail-open eligible.
                    dim_role = dim_role_by_attr.get((dim_table_id or "", dim_suffix or ""))
                    if dim_role is not None:
                        dim_entry["semantic_role"] = dim_role
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
