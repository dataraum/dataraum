"""Enriched views phase implementation.

Creates grain-preserving DuckDB views that LEFT JOIN fact tables with their
confirmed dimension tables. Uses LLM to identify which relationships add
valuable analytical dimensions (geographic, category, reference data).

The enrichment judge sees EVERY defined relationship (llm / manual / keeper —
raw candidates never reach this phase; they die at the semantic judge). The
old ``confidence >= 0.7`` floor was an uncalibrated numeric gate deciding on
the judge's behalf which already-verified relationships it was allowed to see
— and keeper rows carried a fabricated ``confidence=1.0`` that sailed through
it anyway (DAT-699). Confidence and cardinality are served as evidence; the
judge decides.

Post-creation: grain is the view's CONTRACT (a fan-out view silently corrupts
every downstream number). Because the view is a one-hop STAR (every dimension
LEFT JOINs the fact, never another dimension), each join's fan-out is
independent — so a grain violation drops the OFFENDING join and rebuilds from
the survivors (DAT-801), never the fact's whole view. A belt-and-braces
whole-view COUNT then asserts the composed grain; if it ever fails, the
star-independence assumption broke and the view is dropped.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime
from types import ModuleType
from typing import Any
from uuid import uuid4

from sqlalchemy import delete, select

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.relationships.utils import (
    load_defined_relationships,
    load_suppressed_relationship_pairs,
)
from dataraum.analysis.semantic.db_models import SemanticAnnotation, TableEntity, TableRole
from dataraum.analysis.statistics.db_models import StatisticalProfile
from dataraum.analysis.statistics.profiler import _profile_column_stats_parallel
from dataraum.analysis.typing.db_models import MaterializationRecipe
from dataraum.analysis.typing.recipe import store_recipe
from dataraum.analysis.views.builder import DimensionJoin, build_enriched_view_sql
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.analysis.views.enrichment_agent import EnrichmentAgent
from dataraum.analysis.views.enrichment_models import EnrichmentAnalysisResult
from dataraum.core.duckdb_naming import schema_for_layer
from dataraum.core.logging import get_logger
from dataraum.core.sql_normalize import sql_equivalent
from dataraum.llm import PromptRenderer, create_provider, load_llm_config
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases._column_cleanup import delete_column_dependents
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.server.storage import LAKE_CATALOG_ALIAS
from dataraum.storage import Column, Table

logger = get_logger(__name__)


def _dossier(rel: Relationship) -> str:
    """The enrichment judge's dossier fingerprint for one relationship row."""
    from dataraum.analysis.views.enrichment_agent import dossier_fingerprint

    return dossier_fingerprint(
        rel.cardinality, rel.confidence, (rel.evidence or {}).get("coverage")
    )


def _unchanged_considered_pairs(
    entries: list[Any] | None,
    current: dict[tuple[str, str], Relationship],
) -> set[tuple[str, str]]:
    """The prior considered pairs whose dossier is UNCHANGED (DAT-699).

    Entries are ``[from_col, to_col, dossier_fingerprint]``. The DAT-516 sticky
    shape froze the VERDICT but the dossier isn't frozen — pairs judged before
    DAT-695 existed never saw the coverage note, and inheritance meant they
    never would. A pair re-opens (counts as undecided, re-offered to the
    judge) when its current dossier no longer matches the one its verdict was
    made on, and when the stored entry carries no fingerprint at all (the
    dossier the judge saw is unknown — conservatively re-ask). A pair absent
    from the current catalog stays considered: there is nothing to re-judge,
    and the existing Layer-A prune handles real drop+re-adds.
    """
    out: set[tuple[str, str]] = set()
    for entry in entries or []:
        pair = (entry[0], entry[1])
        fingerprint = entry[2] if len(entry) > 2 else None
        rel = current.get(pair)
        if rel is None or (fingerprint is not None and fingerprint == _dossier(rel)):
            out.add(pair)
    return out


def _lake_fqn(layer: str, bare: str) -> str:
    """Fully-qualified DuckDB name ``catalog.schema."bare"`` for a lake-layer artifact.

    Enriched views resolve to the ``typed`` schema (``schema_for_layer`` default),
    so the view, its fact, and its dimensions all qualify through one helper.
    """
    return f'{LAKE_CATALOG_ALIAS}.{schema_for_layer(layer)}."{bare}"'


@analysis_phase
class EnrichedViewsPhase(BasePhase):
    """Create enriched DuckDB views from semantic output.

    For each fact table, creates a view that LEFT JOINs qualifying
    dimension tables. Uses LLM to identify which confirmed relationships
    add valuable analytical dimensions (geographic, category, reference).

    This materializes semantic relationships as queryable views for
    downstream phases (slicing, correlations).
    """

    @property
    def name(self) -> str:
        return "enriched_views"

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.analysis.views import db_models

        return [db_models]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip only when the session's selection has no fact table to enrich.

        Source-free (feedback-source-dies-at-addsource): begin_session scopes by
        ``ctx.table_ids`` — the session's selected typed tables, which may span
        sources — never ``source_id``. Structural early-out only (mirrors typing's
        DAT-413 re-seam): there is NO "views already exist → skip" bail — a re-run
        mints a fresh ``run_id`` and re-derives the view definitions under it, and
        ``_run`` is idempotent on the ``run_id`` grain.

        Run-scoped: ``TableEntity`` is run-versioned and coexists across runs
        (DAT-408/413), so the fact lookup filters to ``ctx.run_id`` — same as
        ``_run`` and ``load_defined_relationships`` below. An unscoped read would
        see every prior run's fact row for the same table.
        """
        if not ctx.table_ids:
            return "No tables in session selection"

        fact_stmt = select(TableEntity.table_id).where(
            TableEntity.table_id.in_(ctx.table_ids),
            TableEntity.table_role.in_([TableRole.FACT, TableRole.PERIODIC_SNAPSHOT]),
        )
        if ctx.run_id is not None:
            fact_stmt = fact_stmt.where(TableEntity.run_id == ctx.run_id)
        if ctx.session.execute(fact_stmt).first() is None:
            return "No fact tables identified"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Create enriched views for the session's fact tables using LLM recommendations."""
        # Source-free: scope to the session's selected typed tables (DAT-415).
        if not ctx.table_ids:
            return PhaseResult.failed("No tables in session selection")
        stmt = select(Table).where(Table.table_id.in_(ctx.table_ids), Table.layer == "typed")
        typed_tables = ctx.session.execute(stmt).scalars().all()

        if not typed_tables:
            return PhaseResult.failed("No typed tables found")

        table_ids = [t.table_id for t in typed_tables]
        tables_by_id = {t.table_id: t for t in typed_tables}
        tables_by_name = {t.table_name: t for t in typed_tables}

        # Find this run's fact tables from entity detections. Run-scoped to
        # ``ctx.run_id`` — exactly like the relationship query below and
        # ``load_defined_relationships``. ``TableEntity`` is run-versioned and
        # coexists across runs (DAT-408/413: delete-by-run_id then insert), so an
        # unscoped read iterates every prior run's fact row for the same table and
        # drives a redundant loop iteration per stale row — which, with the
        # production session's ``autoflush=False``, inserts a duplicate
        # ``EnrichedView`` per stale row and breaks the latest-only,
        # one-row-per-fact contract that ``dimension_coverage`` reads via
        # ``scalar_one_or_none``.
        fact_stmt = select(TableEntity).where(
            TableEntity.table_id.in_(table_ids),
            TableEntity.table_role.in_([TableRole.FACT, TableRole.PERIODIC_SNAPSHOT]),
        )
        if ctx.run_id is not None:
            fact_stmt = fact_stmt.where(TableEntity.run_id == ctx.run_id)
        fact_entities = ctx.session.execute(fact_stmt).scalars().all()

        if not fact_entities:
            return PhaseResult.success(
                outputs={"enriched_views": 0, "message": "No fact tables found"},
                records_processed=0,
                records_created=0,
            )

        # The session's defined relationships (not candidate) touching these tables.
        all_relationships = load_defined_relationships(
            ctx.session,
            table_ids,
            run_id=ctx.run_id,
            both_tables=False,
        )

        # Build column lookups
        cols_stmt = select(Column).where(Column.table_id.in_(table_ids))
        all_columns = ctx.session.execute(cols_stmt).scalars().all()
        columns_by_table: dict[str, list[Column]] = {}
        for col in all_columns:
            columns_by_table.setdefault(col.table_id, []).append(col)

        # DAT-516 — sticky enriched-view shape. The shape (which fk__attr columns a fact
        # exposes) is decided once and inherited, not re-judged every run. Load each fact's
        # prior decided shape (EnrichedView is latest-only — one row per fact), the reject
        # overlay, and a (table_id, column_name) → column_id map to resolve the enrichment
        # LLM's name-based joins back to the stable column-pair key.
        prior_views = {
            v.fact_table_id: v
            for v in ctx.session.execute(
                select(EnrichedView).where(EnrichedView.fact_table_id.in_(table_ids))
            ).scalars()
        }
        suppressed = load_suppressed_relationship_pairs(ctx.session)
        col_id_by_name: dict[tuple[str, str], str] = {
            (c.table_id, c.column_name): c.column_id for c in all_columns
        }
        # Per fact: the relationships touching it, keyed by directional column pair (the
        # cross-run-stable identity; relationship_id is a per-run uuid4).
        rels_touching: dict[str, dict[tuple[str, str], Relationship]] = {}
        for r in all_relationships:
            pair = (r.from_column_id, r.to_column_id)
            for tid in (r.from_table_id, r.to_table_id):
                if tid in tables_by_id:
                    rels_touching.setdefault(tid, {})[pair] = r

        def considered_pairs(fact_id: str) -> set[tuple[str, str]]:
            pv = prior_views.get(fact_id)
            if pv is None:
                return set()
            return _unchanged_considered_pairs(
                pv.considered_relationship_pairs, rels_touching.get(fact_id, {})
            )

        # Feed the enrichment LLM ONLY the undecided relationships (candidates not yet
        # judged for their fact), and skip the call entirely when none are undecided — the
        # shape is then fully inherited (the DAT-516 stickiness + a re-run latency win).
        undecided_rels: list[Relationship] = []
        seen: set[str] = set()
        for fact_entity in fact_entities:
            done = considered_pairs(fact_entity.table_id)
            for pair, r in rels_touching.get(fact_entity.table_id, {}).items():
                if pair not in done and r.relationship_id not in seen:
                    undecided_rels.append(r)
                    seen.add(r.relationship_id)

        has_inherited = any(pv.exposed_dimension_joins for pv in prior_views.values())
        llm_recommendations: EnrichmentAnalysisResult | None = None
        if undecided_rels:
            # RuntimeError = an attempted LLM call that failed; None = LLM config-disabled.
            try:
                llm_recommendations = self._get_llm_recommendations(
                    ctx=ctx,
                    typed_tables=typed_tables,
                    fact_entities=fact_entities,
                    all_relationships=undecided_rels,
                    columns_by_table=columns_by_table,
                    tables_by_id=tables_by_id,
                )
            except RuntimeError as exc:
                return PhaseResult.failed(str(exc))
            if llm_recommendations is None and not has_inherited:
                # Nothing decided yet and the LLM can't decide it → skip (as before).
                return PhaseResult.success(
                    outputs={
                        "enriched_views": 0,
                        "message": "LLM unavailable, skipping enrichment",
                    },
                    records_processed=0,
                    records_created=0,
                    summary="skipped (LLM unavailable)",
                )
            if llm_recommendations is None:
                # Inherit the prior shape; leave the undecided pairs unjudged (retried when
                # the LLM returns) rather than losing the already-decided views.
                logger.info(
                    "enrichment_llm_unavailable_inherit_only", undecided=len(undecided_rels)
                )
        judged = llm_recommendations is not None

        views_created = 0
        views_dropped = 0
        joins_dropped = 0

        for fact_entity in fact_entities:
            fact_table = tables_by_id.get(fact_entity.table_id)
            if not fact_table or not fact_table.duckdb_path:
                continue

            # Assemble the dimension joins: inherit the prior exposed shape + add only the
            # newly-judged joins (DAT-516). Each join is paired with its stable
            # (from_column_id, to_column_id) for persistence + the candidacy check.
            fact_id = fact_table.table_id
            cand_by_pair = rels_touching.get(fact_id, {})
            joins_with_ids: list[tuple[DimensionJoin, tuple[str, str]]] = []

            # Inherit: a prior exposed join survives iff Layer A still confirms its
            # relationship (pair ∈ candidates) and the user hasn't rejected it. Monotonic —
            # the shape shrinks only on those explicit signals, never on a fresh re-judgment.
            prior_view = prior_views.get(fact_id)
            for spec in (prior_view.exposed_dimension_joins if prior_view else None) or []:
                pair = (spec["from_column_id"], spec["to_column_id"])
                # Suppression is undirected (DAT-777) — a reject holds either way.
                if pair not in cand_by_pair or frozenset(pair) in suppressed:
                    continue
                joins_with_ids.append(
                    (
                        DimensionJoin(
                            dim_table_name=spec["dim_table_name"],
                            dim_duckdb_path="",  # resolved to a FQN below
                            fact_fk_column=spec["fact_fk_column"],
                            dim_pk_column=spec["dim_pk_column"],
                            include_columns=list(spec["include_columns"]),
                            relationship_id=cand_by_pair[pair].relationship_id,
                        ),
                        pair,
                    )
                )

            # New: joins the enrichment LLM judged this run (it only saw undecided pairs).
            inherited_pairs = {pair for _, pair in joins_with_ids}
            if llm_recommendations:
                for rec in llm_recommendations.recommendations:
                    if rec.fact_table_id != fact_id:
                        continue
                    for join in rec.dimension_joins:
                        new_pair = self._join_pair(
                            join, fact_id, tables_by_name, col_id_by_name, cand_by_pair
                        )
                        if new_pair is None or frozenset(new_pair) in suppressed:
                            continue
                        if new_pair in inherited_pairs:
                            continue  # already inherited — never double-add a pair
                        inherited_pairs.add(new_pair)
                        joins_with_ids.append(
                            (
                                replace(
                                    join, relationship_id=cand_by_pair[new_pair].relationship_id
                                ),
                                new_pair,
                            )
                        )

            # Name the view off the fact's narrow, workspace-unique duckdb_path
            # (``enriched_{table}`` — DAT-639). Table names are workspace-unique
            # (``uq_table_name_layer``), so two sources can't each own an
            # ``orders`` fact in the first place. CREATE OR
            # REPLACE only ever replaces THIS view on a re-run, never a sibling.
            view_name = f"enriched_{fact_table.duckdb_path}"
            view_fqn = _lake_fqn("enriched", view_name)
            fact_fqn = _lake_fqn("typed", fact_table.duckdb_path)

            # Resolve each candidate join to its dimension FQN, carrying the stable
            # column-pair so persistence tracks exactly what ships. A join whose dim
            # table isn't in this session can't be resolved → dropped here (as before).
            fqn_joins_with_ids = [
                (replace(join, dim_duckdb_path=_lake_fqn("typed", dim.duckdb_path)), pair)
                for join, pair in joins_with_ids
                if (dim := tables_by_name.get(join.dim_table_name)) is not None and dim.duckdb_path
            ]

            # DAT-801: a grain violation drops the OFFENDING join and rebuilds from
            # the survivors — never the fact's whole view. The enriched view is a
            # one-hop STAR (every dimension LEFT JOINs the fact, never another
            # dimension — see ``build_enriched_view_sql``), so each join's fan-out is
            # INDEPENDENT: a join whose isolated COUNT keeps the fact row count keeps
            # it in the composed view, and composing grain-preservers preserves grain.
            # Verifying each candidate ALONE is therefore sufficient for the whole
            # view — no bisect. (DAT-801 reframed the prompt to join more neighbours,
            # so a single fan-out pick used to cost a central fact its entire view.)
            surviving = [
                (join, pair)
                for join, pair in fqn_joins_with_ids
                if self._join_preserves_grain(ctx.duckdb_conn, fact_table, fact_fqn, join)
            ]
            joins_dropped += len(fqn_joins_with_ids) - len(surviving)

            # Persistence + the view SQL now reflect the survivors only.
            joins_with_ids = surviving
            dimension_joins: list[DimensionJoin] = [join for join, _ in surviving]
            fqn_joins = dimension_joins  # already FQN-resolved above

            if not dimension_joins:
                logger.info(
                    "passthrough_enriched_view",
                    fact_table=fact_table.table_name,
                    reason="no qualifying dimension joins",
                )

            # Build view SQL from the grain-preserving survivors.
            view_sql, dim_columns = build_enriched_view_sql(view_fqn, fact_fqn, fqn_joins)

            # Create view in DuckDB
            try:
                ctx.duckdb_conn.execute(view_sql)
            except Exception as e:
                logger.warning("view_creation_failed", view_name=view_name, error=str(e))
                continue

            # Belt-and-braces (DAT-801): per-join filtering above already guarantees
            # the grain, so this whole-view COUNT should never fail. If it does, the
            # star-independence assumption broke — do NOT silently ship a fan-out view
            # (grain is the view's CONTRACT). Log LOUD and drop the view.
            is_grain_verified = self._verify_grain(
                ctx.duckdb_conn,
                view_target=view_fqn,
                expected_count=fact_table.row_count,
            )

            if not is_grain_verified:
                logger.error(
                    "grain_verification_failed_after_per_join_filter",
                    view_name=view_name,
                    fact_table=fact_table.table_name,
                    expected_count=fact_table.row_count,
                    surviving_joins=len(fqn_joins),
                )
                try:
                    ctx.duckdb_conn.execute(f"DROP VIEW IF EXISTS {view_fqn}")
                except Exception:
                    pass
                views_dropped += 1
                continue

            # Build evidence with LLM reasoning if available
            evidence: dict[str, Any] = {}
            if llm_recommendations:
                for rec in llm_recommendations.recommendations:
                    if rec.fact_table_id == fact_table.table_id:
                        evidence = {
                            "llm_reasoning": rec.reasoning,
                            "relationship_role": rec.relationship_role,
                            "enrichment_columns": rec.enrichment_columns,
                            "model_name": llm_recommendations.model_name,
                        }
                        break

            # Register and profile dimension columns (latest-only substrate:
            # reconciled by view_name, prior columns/profiles replaced).
            view_table = self._register_and_profile_dim_columns(
                ctx,
                fact_table,
                view_name,
                view_fqn,
                dim_columns,
            )

            # Version the view DDL on the DAT-414 recipe substrate (emit → store →
            # execute; the view was executed above), canonical-SQL-gated (DAT-415):
            # only stamp a NEW run-versioned recipe when the canonical SQL differs
            # from the fact's latest stored enriched recipe — an unchanged re-run
            # (temp-0 LLM → same joins) adds no redundant version, and a
            # same-run retry sees its own just-stored recipe as equivalent and
            # no-ops. Keyed on the stable fact id + ``layer="enriched"``
            # (collision-free vs typing's typed/quarantine recipes); ``depends_on``
            # is the typed fact/dim FQNs the recipe topo-sort rebuilds after.
            dim_fqns = [j.dim_duckdb_path for j in fqn_joins]
            latest_recipe = ctx.session.execute(
                select(MaterializationRecipe)
                .where(
                    MaterializationRecipe.table_id == fact_table.table_id,
                    MaterializationRecipe.layer == "enriched",
                )
                .order_by(MaterializationRecipe.created_at.desc())
                .limit(1)
            ).scalar_one_or_none()
            if latest_recipe is None or not sql_equivalent(latest_recipe.ddl, view_sql):
                store_recipe(
                    ctx.session,
                    table_id=fact_table.table_id,
                    layer="enriched",
                    run_id=ctx.require_run_id(),
                    target_fqn=view_fqn,
                    ddl=view_sql,
                    depends_on=[fact_fqn, *dim_fqns],
                )

            # The view definition is latest-only substrate (one row per fact,
            # DAT-415): reconcile-in-place, stamping the run that materialized it.
            # The recipe (above) carries the version history; readers resolve the
            # current view here without run-scoping. Keyed on fact_table_id alone
            # (no session filter) and now DB-enforced by ``uq_enriched_view_fact_table``
            # — the physical view is shared in lake.typed, so the metadata is
            # last-writer-wins per fact. The run-scoped fact query above means one
            # iteration per fact; the unique constraint is the structural backstop
            # so any errant second insert fails loudly here instead of silently
            # surfacing as ``MultipleResultsFound`` in a downstream reader.
            view_record = ctx.session.execute(
                select(EnrichedView).where(EnrichedView.fact_table_id == fact_table.table_id)
            ).scalar_one_or_none()
            if view_record is None:
                view_record = EnrichedView(
                    fact_table_id=fact_table.table_id,
                )
                ctx.session.add(view_record)
            view_record.run_id = ctx.require_run_id()
            view_record.view_name = view_name
            view_record.relationship_ids = [j.relationship_id for j in dimension_joins]
            view_record.dimension_table_ids = list(
                {
                    tables_by_name[j.dim_table_name].table_id
                    for j in dimension_joins
                    if j.dim_table_name in tables_by_name
                }
            )
            view_record.dimension_columns = dim_columns
            # DAT-516 sticky shape: every candidate fed to the LLM this run is now decided
            # (judged → considered); the exposed joins are persisted in full so a future
            # re-run inherits the shape without re-judging. When the LLM was unavailable we
            # didn't judge the undecided pairs, so they stay out of ``considered`` (retried).
            # Keep prior verdicts only for pairs Layer A STILL confirms (prune stale), plus
            # every candidate judged this run. A relationship that genuinely left Layer A and
            # later returns is thus re-judged, not stuck invisible — and since Layer A's
            # silent-accept keeps the confirmed SET stable across runs, this prune only fires
            # on a real drop+re-add, never on LLM re-judgment (the determinism the ticket wants).
            # Each entry carries the DOSSIER FINGERPRINT its verdict was made on
            # (DAT-699): the verdict sticks only while the measured evidence the
            # judge saw is unchanged — a changed dossier re-opens the pair.
            considered_now = considered_pairs(fact_id) & set(cand_by_pair)
            if judged:
                considered_now |= set(cand_by_pair)
            view_record.considered_relationship_pairs = [
                [a, b, _dossier(cand_by_pair[(a, b)])] for (a, b) in sorted(considered_now)
            ]
            view_record.exposed_dimension_joins = [
                {
                    "from_column_id": pair[0],
                    "to_column_id": pair[1],
                    "fact_fk_column": j.fact_fk_column,
                    "dim_pk_column": j.dim_pk_column,
                    "dim_table_name": j.dim_table_name,
                    "include_columns": list(j.include_columns),
                }
                for j, pair in joins_with_ids
            ]
            view_record.is_grain_verified = is_grain_verified
            view_record.evidence = evidence if evidence else None
            view_record.view_table_id = view_table.table_id if view_table else None

            views_created += 1

            logger.info(
                "enriched_view_created",
                view_name=view_name,
                fact_table=fact_table.table_name,
                dimension_joins=len(dimension_joins),
                dimension_columns=len(dim_columns),
            )

        # Atomic-publish visibility (DAT-506): DuckLake buffers writes in memory
        # until a CHECKPOINT, and the cockpit's READ_ONLY ATTACH sees only the last
        # CHECKPOINTed snapshot. After this run's COMPLETE set of enriched-view DDL
        # is materialized, force the snapshot so the readers see the whole batch at
        # once (not a torn mid-materialize view set).
        ctx.duckdb_conn.execute("CHECKPOINT")

        return PhaseResult.success(
            outputs={
                "enriched_views": views_created,
                "views_dropped": views_dropped,
                "joins_dropped": joins_dropped,
                "fact_tables": len(fact_entities),
            },
            records_processed=len(fact_entities),
            records_created=views_created,
            summary=f"{views_created} enriched views created ({len(fact_entities)} fact tables)",
        )

    @staticmethod
    def _join_pair(
        join: DimensionJoin,
        fact_id: str,
        tables_by_name: dict[str, Table],
        col_id_by_name: dict[tuple[str, str], str],
        cand_by_pair: dict[tuple[str, str], Relationship],
    ) -> tuple[str, str] | None:
        """Resolve a name-based enrichment join to its relationship column-pair (DAT-516).

        Returns the relationship's directional ``(from_column_id, to_column_id)``, or
        ``None`` if the join doesn't map to a confirmed relationship. The agent emits join
        column NAMES; map them to ``column_id``s and match either orientation against the
        fact's candidate relationships (the stored direction wins).
        """
        dim = tables_by_name.get(join.dim_table_name)
        if dim is None:
            return None
        fk_id = col_id_by_name.get((fact_id, join.fact_fk_column))
        pk_id = col_id_by_name.get((dim.table_id, join.dim_pk_column))
        if fk_id is None or pk_id is None:
            return None
        for cand in ((fk_id, pk_id), (pk_id, fk_id)):
            if cand in cand_by_pair:
                return cand
        return None

    def _register_and_profile_dim_columns(
        self,
        ctx: PhaseContext,
        fact_table: Table,
        view_name: str,
        view_fqn: str,
        dim_columns: list[str],
    ) -> Table | None:
        """Register enriched-layer Table + Column records for dimension columns and profile them.

        Latest-only substrate (DAT-415): the enriched ``Table`` is reconciled on
        its ``(source, view_name, "enriched")`` unique key — reused across runs,
        with its prior ``Column``s replaced. Since the FK children of ``columns``
        no longer ``ON DELETE CASCADE`` (DAT-506), the prior run's
        ``StatisticalProfile``s (and every other child) are deleted explicitly via
        ``delete_column_dependents`` before the columns go. The view *definition*
        is what is run-versioned (``EnrichedView`` + recipe DDL); the lake
        substrate stays latest-only and is re-materialized from the versioned
        recipe on a reset.

        Args:
            ctx: Phase context.
            fact_table: The fact table this view is based on.
            view_name: Bare name of the enriched DuckDB view (stored as duckdb_path).
            view_fqn: Fully-qualified view name, used to query the view (DESCRIBE / profile).
            dim_columns: List of dimension column names in the view.

        Returns:
            The enriched-layer Table record, or None if no dimension columns.
        """
        if not dim_columns:
            return None

        view_table = ctx.session.execute(
            select(Table).where(
                Table.source_id == fact_table.source_id,
                Table.table_name == view_name,
                Table.layer == "enriched",
            )
        ).scalar_one_or_none()
        existing: dict[str, Column] = {}
        if view_table is None:
            view_table = Table(
                table_id=str(uuid4()),
                source_id=fact_table.source_id,
                table_name=view_name,
                layer="enriched",
                duckdb_path=view_name,
                row_count=fact_table.row_count,
            )
            ctx.session.add(view_table)
            ctx.session.flush()
        else:
            # Reconcile, don't replace (DAT-516): keep each dimension column (with its
            # ``column_id`` AND its ``StatisticalProfile``) whose name survives in the new
            # set, drop only columns whose join left the shape, add only genuinely-new ones.
            # This preserves ``column_id`` across re-runs, so a consumer holding one (and the
            # profiles those columns carry) is not silently invalidated by an unchanged shape.
            # Dropped columns' FK children are cleared first (``columns`` no longer
            # ``ON DELETE CASCADE`` — DAT-506) so the ``delete(Column)`` can't FK-violate.
            view_table.duckdb_path = view_name
            view_table.row_count = fact_table.row_count
            existing = {
                c.column_name: c
                for c in ctx.session.execute(
                    select(Column).where(Column.table_id == view_table.table_id)
                ).scalars()
            }
            wanted = set(dim_columns)
            removed = [c for name, c in existing.items() if name not in wanted]
            if removed:
                delete_column_dependents(ctx, [c.column_id for c in removed])
                ctx.session.execute(
                    delete(Column).where(Column.column_id.in_([c.column_id for c in removed]))
                )
                ctx.session.flush()
                for c in removed:
                    existing.pop(c.column_name)

        # Get DuckDB types for dimension columns
        duckdb_cols = ctx.duckdb_conn.execute(f"DESCRIBE {view_fqn}").fetchall()
        type_by_name = {row[0]: row[1] for row in duckdb_cols}

        # Keep + reposition existing columns; mint only genuinely-new ones (DAT-516).
        new_columns: list[Column] = []
        for pos, col_name in enumerate(dim_columns):
            kept = existing.get(col_name)
            if kept is not None:
                kept.column_position = pos  # keep column_id + its profile; refresh order only
                continue
            col_type = type_by_name.get(col_name, "VARCHAR")
            col = Column(
                column_id=str(uuid4()),
                table_id=view_table.table_id,
                column_name=col_name,
                column_position=pos,
                raw_type=col_type,
                resolved_type=col_type,
            )
            ctx.session.add(col)
            new_columns.append(col)
        ctx.session.flush()

        # Profile ONLY the new columns. A kept column's join is unchanged, so its data —
        # and thus its existing profile — is stable; re-profiling would just churn it.
        # Per-column profiling failures are absorbed inside ``_profile_column_stats_parallel``
        # (returns None → skipped); a genuine DB / IntegrityError on the column-set
        # reconcile above is NOT swallowed — it propagates and fails the activity loud.
        profiled_at = datetime.now(UTC)
        profiled_count = 0
        for col in new_columns:
            # Bare name, NOT view_fqn: the profiler interpolates the path as
            # ONE quoted identifier, so a pre-quoted catalog.schema."name"
            # FQN parses as a zero-length identifier and every dim-column
            # profile fails (eval DAT-405 finding). Bare lake-view names
            # resolve on ctx.duckdb_conn (same as DESCRIBE in slicing_view),
            # and match the Table.duckdb_path the row persists.
            profile = _profile_column_stats_parallel(
                duckdb_conn=ctx.duckdb_conn,
                table_name=view_name,
                table_duckdb_path=view_name,
                column_id=col.column_id,
                column_name=col.column_name,
                resolved_type=col.resolved_type or "VARCHAR",
                profiled_at=profiled_at,
                top_k=10,
            )
            if profile:
                non_null = profile.total_count - profile.null_count
                is_unique = profile.distinct_count == non_null if non_null > 0 else False
                db_profile = StatisticalProfile(
                    profile_id=str(uuid4()),
                    column_id=col.column_id,
                    run_id=ctx.require_run_id(),
                    profiled_at=profiled_at,
                    layer="enriched",
                    total_count=profile.total_count,
                    null_count=profile.null_count,
                    distinct_count=profile.distinct_count,
                    null_ratio=profile.null_ratio,
                    cardinality_ratio=profile.cardinality_ratio,
                    is_unique=is_unique,
                    is_numeric=profile.numeric_stats is not None,
                    profile_data=profile.model_dump(mode="json"),
                )
                ctx.session.add(db_profile)
                profiled_count += 1

        logger.info(
            "dim_columns_profiled",
            view_name=view_name,
            columns=len(dim_columns),
            new_columns=len(new_columns),
            profiles=profiled_count,
        )
        return view_table

    def _get_llm_recommendations(
        self,
        ctx: PhaseContext,
        typed_tables: Sequence[Table],
        fact_entities: Sequence[TableEntity],
        all_relationships: Sequence[Relationship],
        columns_by_table: dict[str, list[Column]],
        tables_by_id: dict[str, Table],
    ) -> EnrichmentAnalysisResult | None:
        """Get LLM recommendations for valuable dimension joins.

        Returns None when LLM is intentionally unavailable (no config,
        feature disabled, no provider configured) — pipeline proceeds
        without enriched views, which is a documented operating mode.

        Raises ``RuntimeError`` when an LLM call was attempted and failed
        (transient or permanent). The phase translates this into
        ``PhaseResult.failed`` so the pipeline halts loudly instead of
        silently producing degraded output.
        """
        # Try to load LLM config
        try:
            config = load_llm_config()
        except FileNotFoundError:
            logger.info("llm_config_not_found", result="skipped")
            return None

        # Check if enrichment analysis is enabled
        if (
            not config.features.enrichment_analysis
            or not config.features.enrichment_analysis.enabled
        ):
            logger.info("enrichment_analysis_disabled", result="skipped")
            return None

        # Create provider. Missing provider config and provider-creation
        # failures are configuration errors, not intentional skips —
        # surface them as runtime failures so the pipeline halts loudly.
        provider_config = config.providers.get(config.active_provider)
        if not provider_config:
            raise RuntimeError(
                f"Provider '{config.active_provider}' not configured — "
                "enrichment analysis is enabled but cannot run."
            )

        try:
            provider = create_provider(config.active_provider, provider_config.model_dump())
        except Exception as e:
            raise RuntimeError(f"Failed to create LLM provider for enrichment: {e}") from e

        # Build context data for the agent
        context_data = self._build_context_data(
            ctx=ctx,
            typed_tables=typed_tables,
            fact_entities=fact_entities,
            all_relationships=all_relationships,
            columns_by_table=columns_by_table,
            tables_by_id=tables_by_id,
        )

        # Create and call the enrichment agent
        renderer = PromptRenderer()
        agent = EnrichmentAgent(
            config=config,
            provider=provider,
            prompt_renderer=renderer,
        )

        result = agent.analyze(
            session=ctx.session,
            context_data=context_data,
        )

        if not result.success:
            logger.error(
                "enrichment_analysis_failed",
                error=result.error,
            )
            raise RuntimeError(f"Enrichment analysis failed: {result.error}")

        return result.value

    def _build_context_data(
        self,
        ctx: PhaseContext,
        typed_tables: Sequence[Table],
        fact_entities: Sequence[TableEntity],
        all_relationships: Sequence[Relationship],
        columns_by_table: dict[str, list[Column]],
        tables_by_id: dict[str, Table],
    ) -> dict[str, Any]:
        """Build context data for the enrichment agent."""
        table_ids = [t.table_id for t in typed_tables]

        # Build tables with entity info
        fact_table_ids = {e.table_id for e in fact_entities}
        tables_data = []
        for table in typed_tables:
            columns_list = [
                {
                    "column_id": col.column_id,
                    "column_name": col.column_name,
                    "resolved_type": col.resolved_type,
                }
                for col in columns_by_table.get(table.table_id, [])
            ]
            tables_data.append(
                {
                    "table_id": table.table_id,
                    "table_name": table.table_name,
                    "duckdb_path": table.duckdb_path,
                    "row_count": table.row_count,
                    "is_fact_table": table.table_id in fact_table_ids,
                    "columns": columns_list,
                }
            )

        # Build semantic annotations
        annotations_data = []
        ann_stmt = select(SemanticAnnotation).where(
            SemanticAnnotation.column_id.in_(
                [col.column_id for cols in columns_by_table.values() for col in cols]
            )
        )
        annotations = ctx.session.execute(ann_stmt).scalars().all()

        # Map column_id to column info for lookup
        column_id_to_info: dict[str, dict[str, str]] = {}
        for table in typed_tables:
            for col in columns_by_table.get(table.table_id, []):
                column_id_to_info[col.column_id] = {
                    "table_name": table.table_name,
                    "column_name": col.column_name,
                }

        for ann in annotations:
            col_info = column_id_to_info.get(ann.column_id, {})
            annotations_data.append(
                {
                    "table_name": col_info.get("table_name", ""),
                    "column_name": col_info.get("column_name", ""),
                    "semantic_role": ann.semantic_role,
                    "entity_type": ann.entity_type,
                    "business_name": ann.business_name,
                }
            )

        # Build confirmed relationships
        relationships_data = []
        for rel in all_relationships:
            from_table = tables_by_id.get(rel.from_table_id)
            to_table = tables_by_id.get(rel.to_table_id)

            # Get column names
            from_col_name = ""
            for col in columns_by_table.get(rel.from_table_id, []):
                if col.column_id == rel.from_column_id:
                    from_col_name = col.column_name
                    break

            to_col_name = ""
            for col in columns_by_table.get(rel.to_table_id, []):
                if col.column_id == rel.to_column_id:
                    to_col_name = col.column_name
                    break

            if from_table and to_table:
                relationships_data.append(
                    {
                        "from_table": from_table.table_name,
                        "from_column": from_col_name,
                        "to_table": to_table.table_name,
                        "to_column": to_col_name,
                        "cardinality": rel.cardinality,
                        "confidence": rel.confidence,
                        # Measured join coverage from the mint's evidence
                        # (DAT-695) — how much of the fact the join enriches.
                        "coverage": (rel.evidence or {}).get("coverage"),
                    }
                )

        # Get existing enriched views
        existing_views_data = []
        existing_stmt = select(EnrichedView).where(EnrichedView.fact_table_id.in_(table_ids))
        existing_views = ctx.session.execute(existing_stmt).scalars().all()
        for ev in existing_views:
            fact_table = tables_by_id.get(ev.fact_table_id)
            existing_views_data.append(
                {
                    "view_name": ev.view_name,
                    "fact_table": fact_table.table_name if fact_table else "",
                    "dimension_columns": ev.dimension_columns or [],
                }
            )

        return {
            "tables": tables_data,
            "annotations": annotations_data,
            "confirmed_relationships": relationships_data,
            "existing_views": existing_views_data,
        }

    @staticmethod
    def _join_preserves_grain(
        duckdb_conn: Any,
        fact_table: Table,
        fact_fqn: str,
        join: DimensionJoin,
    ) -> bool:
        """Whether ONE dimension join, applied alone, preserves the fact grain (DAT-801).

        The enriched view is a one-hop star (every dimension LEFT JOINs the fact,
        never another dimension — ``build_enriched_view_sql``), so each join's
        fan-out is independent: a join whose isolated ``COUNT(*)`` equals the fact
        row count keeps the grain in the composed view too. This lets a grain
        violation drop just the offending join and rebuild from the survivors,
        instead of costing the fact its whole enriched view.

        ``row_count is None`` → unmeasurable, keep the join (mirrors
        ``_verify_grain``: can't verify → don't drop). A LEFT JOIN can only preserve
        or inflate the fact's rows, so any COUNT above the expected count is a
        fan-out (``one_to_many``). Both a fan-out and a probe that can't even execute
        drop the join with a born-loud log naming the fact, the neighbour, and the
        counts — a fan-out is a real topology fact, not debug noise.
        """
        expected = fact_table.row_count
        if expected is None:
            return True  # can't measure → don't drop (mirrors _verify_grain)

        count_sql = (
            f"SELECT COUNT(*) FROM {fact_fqn} AS f "
            f"LEFT JOIN {join.dim_duckdb_path} AS d "
            f'ON f."{join.fact_fk_column}" = d."{join.dim_pk_column}"'
        )
        try:
            row = duckdb_conn.execute(count_sql).fetchone()
        except Exception as e:
            logger.warning(
                "enrichment_join_probe_failed",
                fact_table=fact_table.table_name,
                dim_table=join.dim_table_name,
                fact_fk_column=join.fact_fk_column,
                dim_pk_column=join.dim_pk_column,
                error=str(e),
            )
            return False

        actual = row[0] if row else 0
        if actual == expected:
            return True

        logger.warning(
            "enrichment_join_fans_out",
            fact_table=fact_table.table_name,
            dim_table=join.dim_table_name,
            fact_fk_column=join.fact_fk_column,
            dim_pk_column=join.dim_pk_column,
            expected_count=expected,
            actual_count=actual,
        )
        return False

    @staticmethod
    def _verify_grain(
        duckdb_conn: Any,
        view_target: str,
        expected_count: int | None,
    ) -> bool:
        """Verify that the view preserves the fact table grain.

        ``view_target`` is the fully-qualified view name. Returns True if
        COUNT(*) of the view matches the expected fact row count.
        """
        if expected_count is None:
            return True  # Can't verify without expected count

        try:
            result = duckdb_conn.execute(f"SELECT COUNT(*) FROM {view_target}").fetchone()
            actual_count = result[0] if result else 0
            return actual_count == expected_count
        except Exception:
            return False
