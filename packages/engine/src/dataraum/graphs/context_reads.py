"""Served-context assembly (DAT-869 split of ``graphs/context.py``).

``build_execution_context`` assembles the GraphAgent's served context by GRAPH
TRAVERSAL over the operating-model property graph (ADR-0021): concept →
part_of subconcepts → groundings (grounded_by) → columns (uses), with
disjoint_with / reconciles_with / conformed-dimension / references /
materializes_as served AS STRUCTURE. The knowledge sections with no graph
element yet — value sets, drivers, validation results, business cycles — are
assembled from their typed rows alongside the traversal core (conventions ride
their own prompt slot). ``context_format.format_served_context`` renders the
result for the grounding prompt.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.graphs.context_models import (
    _NON_CATEGORICAL_ROLES,
    _VALUE_SET_COMPLETE_MAX,
    BusinessCycleContext,
    ColumnContext,
    ConceptContext,
    ConceptReconciliation,
    ConformedDimensionContext,
    CycleStageContext,
    DriverContext,
    EnrichedViewContext,
    EntityFlowContext,
    GraphExecutionContext,
    GroundingContext,
    GroundingUseContext,
    RelationshipContext,
    ReportingCalendarContext,
    SliceContext,
    TableContext,
    ValidationContext,
)

if TYPE_CHECKING:
    import duckdb

    from dataraum.analysis.cycles.health import HealthReport

logger = get_logger(__name__)

# =============================================================================
# Context Builder
# =============================================================================


def build_execution_context(
    session: Session,
    table_ids: list[str],
    duckdb_conn: duckdb.DuckDBPyConnection | None = None,
    *,
    vertical: str | None = None,
    om_run_id: str | None = None,
    catalogue_run_id: str | None = None,
    workspace_id: str | None = None,
) -> GraphExecutionContext:
    """Build execution context: the graph traversal core + the typed sections.

    Aggregates the operating-model property graph (concepts, groundings,
    references, conformed axes, materialization) with the typed knowledge rows
    (statistical profiles / value sets, semantic roles, temporal profiles,
    slices, drivers, business cycles, validation results, readiness flags).

    Args:
        session: SQLAlchemy session
        table_ids: Tables to include in context
        duckdb_conn: Optional DuckDB connection for row counts
        vertical: Runtime vertical for the cycle-health computation (passed by the
            caller — the InvestigationSession lookup is gone, DAT-506).
        om_run_id: Explicit operating_model run for the cycle/validation/health
            reads (the in-run metrics phase passes its current run). Omitted ⇒ the
            promoted operating_model catalog head.
        catalogue_run_id: The begin_session catalogue head run (DAT-637) — scopes
            the catalogue-grain ``ColumnConcept`` reads (meaning,
            temporal_behavior, unit source). The metrics phase passes
            ``base_runs.relationship_run_id``. None ⇒ no concepts (object-grain
            column metadata still loads).
        workspace_id: The workspace whose operating-model property graph the
            traversal core reads (DAT-734) — resolves the read schema. In the
            engine paths this is ``schema_mapping_id`` (DAT-506). None ⇒ the
            graph-served sections stay empty (logged loud).

    Returns:
        GraphExecutionContext with all relevant metadata
    """
    # Lazy imports to avoid circular dependencies
    from dataraum.analysis.correlation.db_models import DerivedColumn
    from dataraum.analysis.cycles.db_models import DetectedBusinessCycle
    from dataraum.analysis.relationships.surrogate import is_surrogate_column
    from dataraum.analysis.semantic.db_models import SemanticAnnotation, TableEntity
    from dataraum.analysis.slicing.curation import curated_slices
    from dataraum.analysis.slicing.db_models import SliceDefinition
    from dataraum.analysis.statistics.db_models import (
        StatisticalProfile,
    )
    from dataraum.analysis.statistics.quality_db_models import (
        StatisticalQualityMetrics,
    )
    from dataraum.analysis.temporal import TemporalColumnProfile
    from dataraum.analysis.typing.db_models import TypeDecision
    from dataraum.graphs.field_mapping import load_column_meanings
    from dataraum.storage import Column, Table

    if not table_ids:
        return GraphExecutionContext()

    # 0. One traversal pass over the operating-model property graph (DAT-734):
    # the concept neighbourhoods + groundings, references, conformed axes,
    # column materialization/anchor, and each enriched view's dimension bases.
    # None ⇒ unreachable graph (non-PG dialect / no workspace) — sections stay
    # empty, logged loud inside the loader.
    graph_reads = _load_graph_reads(session, workspace_id, vertical, table_ids)

    # 1. Load tables
    tables_stmt = select(Table).where(Table.table_id.in_(table_ids))
    tables = session.execute(tables_stmt).scalars().all()
    table_map = {t.table_id: t for t in tables}

    # 2. Load all columns for these tables. Mint-owned surrogate join keys
    # (``_sk__*``, DAT-277) are excluded — they are not business columns and
    # must not reach the grounding prompt's column list/value-set rendering
    # (DAT-878). Relationship rendering is unaffected: ``_read_references``
    # resolves endpoint names through its OWN vertex map (``_load_graph_reads``
    # / ``og_columns``), a separate read from this one — a surrogate pair still
    # surfaces there as the join evidence it legitimately is.
    columns_stmt = select(Column).where(Column.table_id.in_(table_ids))
    columns = [
        c
        for c in session.execute(columns_stmt).scalars().all()
        if not is_surrogate_column(c.column_name)
    ]
    columns_by_table: dict[str, list[Column]] = {}
    for col in columns:
        if col.table_id not in columns_by_table:
            columns_by_table[col.table_id] = []
        columns_by_table[col.table_id].append(col)

    # Each table's current (promoted) add_source run names the run that wrote its
    # column metadata — ``promote_run`` flips the single ``(table:{id},
    # GENERATION_STAGE)`` generation head to that one run (DAT-506). The
    # column-metadata reads below drop rows from STALE earlier runs (a replay/teach
    # leaves >1 run per column, DAT-413); a table with no promoted run keeps what's
    # there — there is no "current" to scope to. This is run-STALENESS scoping:
    # column metadata is add_source-derived and shared across runs, so it is NOT the
    # cross-run isolation concern the entities/relationships read (below) fails
    # closed on.
    from dataraum.storage.snapshot_head import GENERATION_STAGE, head_run_id

    addsource_run_by_table = {
        tid: head_run_id(session, f"table:{tid}", GENERATION_STAGE) for tid in table_ids
    }
    run_by_column = {col.column_id: addsource_run_by_table.get(col.table_id) for col in columns}

    def _is_current(row: Any) -> bool:
        want = run_by_column.get(row.column_id)
        return want is None or row.run_id == want

    # 3. Load statistical profiles (current add_source run only)
    column_ids = [col.column_id for col in columns]
    stat_profiles: dict[str, StatisticalProfile] = {}
    if column_ids:
        stat_stmt = select(StatisticalProfile).where(StatisticalProfile.column_id.in_(column_ids))
        for profile in session.execute(stat_stmt).scalars():
            if _is_current(profile):
                stat_profiles[profile.column_id] = profile

    # 4. Load statistical quality metrics
    stat_quality: dict[str, StatisticalQualityMetrics] = {}
    if column_ids:
        qual_stmt = select(StatisticalQualityMetrics).where(
            StatisticalQualityMetrics.column_id.in_(column_ids)
        )
        for metrics in session.execute(qual_stmt).scalars():
            if _is_current(metrics):
                stat_quality[metrics.column_id] = metrics

    # 5. Load semantic annotations — OBJECT-grain (role, entity label, term),
    # scoped to each table's add_source generation head.
    semantic: dict[str, SemanticAnnotation] = {}
    if column_ids:
        sem_stmt = select(SemanticAnnotation).where(SemanticAnnotation.column_id.in_(column_ids))
        for ann in session.execute(sem_stmt).scalars():
            if _is_current(ann):
                semantic[ann.column_id] = ann

    # 6. Load temporal profiles
    temporal: dict[str, TemporalColumnProfile] = {}
    if column_ids:
        temp_stmt = select(TemporalColumnProfile).where(
            TemporalColumnProfile.column_id.in_(column_ids)
        )
        for temp_prof in session.execute(temp_stmt).scalars():
            if _is_current(temp_prof):
                temporal[temp_prof.column_id] = temp_prof

    # 7. Load type decisions
    type_decisions: dict[str, TypeDecision] = {}
    if column_ids:
        type_stmt = select(TypeDecision).where(TypeDecision.column_id.in_(column_ids))
        for decision in session.execute(type_stmt).scalars():
            if _is_current(decision):
                type_decisions[decision.column_id] = decision

    # Resolve the workspace's current (promoted) catalog run ONCE via the catalog
    # head (DAT-506 — sessions moved to cockpit_db; the version axis is the ONE
    # workspace catalog head, no session gate). TableEntity AND the relationship
    # catalog are both run-versioned and coexist across runs (DAT-408/413), so both
    # reads below must scope to the SAME run — else the assembled context silently
    # mixes runs. With no promoted catalog run the head is unresolved and the
    # run-versioned reads fail closed (empty) rather than fall back cross-run.
    from dataraum.storage.snapshot_head import catalog_head_target

    run_id = head_run_id(session, catalog_head_target(), "catalog")

    # Observability (DAT-429): the catalog head doesn't resolve — the context comes
    # back run-versioned-empty by design (fail-closed below), so surface WHY rather
    # than leave a silent hollow context to debug.
    if run_id is None:
        logger.warning(
            "catalog_run_unresolved",
            detail="no promoted catalog run; entity/relationship context is empty",
        )

    # 8. The run-versioned table entities (fact/dimension) are read ONLY when the
    # promoted catalog run resolves. **Fail-closed (DAT-429):** with no resolved
    # catalog run we MUST NOT fall back to a cross-run read — that would surface
    # superseded entities into this context. Leave empty instead. (The
    # non-run-versioned field metadata above is keyed by the passed table/column
    # ids and is unaffected.)
    table_entities: dict[str, TableEntity] = {}
    if run_id is not None:
        for entity in session.execute(
            select(TableEntity).where(
                TableEntity.table_id.in_(table_ids), TableEntity.run_id == run_id
            )
        ).scalars():
            table_entities[entity.table_id] = entity

    # 9. Relationships come from the GRAPH's refs relation (og_references over
    # current_relationships, DAT-734): head-resolved like the old ORM read (same
    # fail-closed behaviour — no promoted head ⇒ empty current_* views), with
    # the conformed-dimension fact↔fact pairs excluded by the element view's own
    # typing (they surface as conformed_dimensions instead, DAT-756).
    relationships: list[RelationshipContext] = graph_reads.references if graph_reads else []

    # 10. Load slice definitions — run-versioned since DAT-448: scope to the
    # promoted catalog run (the begin_session run that derived them). With no
    # resolved catalog run this fails CLOSED (a cross-run read would mix in
    # superseded definitions — the DAT-429 isolation discipline). CURATED read
    # (DAT-879): fetch the whole run-scoped inventory and let ``curated_slices``
    # decide — judged rows, ordered by measured relevance, with the count of
    # what was left out carried alongside so the formatter can SAY it. The old
    # ``LIMIT CURATED_SLICE_BUDGET`` cut here silently, and past the judged rows
    # it cut alphabetically.
    slice_contexts: list[SliceContext] = []
    slice_stmt = select(SliceDefinition).where(SliceDefinition.table_id.in_(table_ids))
    if run_id is not None:
        slice_stmt = slice_stmt.where(SliceDefinition.run_id == run_id)
    slice_defs = [] if run_id is None else list(session.execute(slice_stmt).scalars().all())
    curated = curated_slices(slice_defs)
    for slice_def in curated.served:
        slice_col = next((c for c in columns if c.column_id == slice_def.column_id), None)
        slice_tbl = table_map.get(slice_def.table_id)
        if slice_col and slice_tbl:
            slice_contexts.append(
                SliceContext(
                    column_name=slice_def.column_name or slice_col.column_name,
                    table_name=slice_tbl.table_name,
                    interest=slice_def.slice_interest,
                    relevance=slice_def.slice_relevance,
                    value_count=slice_def.value_count or 0,
                    business_context=slice_def.business_context,
                    distinct_values=slice_def.distinct_values or [],
                )
            )
    slice_catalog_note = curated.note

    # 10c. Load driver rankings (DAT-616) — begin_session value-layer artifact
    # (DAT-546), run-versioned; same fail-closed catalog-run scoping as the slices.
    # The engine GraphAgent served NONE before this (the cockpit/engine asymmetry).
    driver_contexts: list[DriverContext] = []
    if run_id is not None:
        from dataraum.analysis.drivers.db_models import DriverRankingArtifact

        driver_stmt = select(DriverRankingArtifact).where(
            DriverRankingArtifact.measure_table_id.in_(table_ids),
            DriverRankingArtifact.run_id == run_id,
        )
        for art in session.execute(driver_stmt).scalars().all():
            driver_contexts.append(
                DriverContext(
                    measure_label=art.measure_label,
                    # NULL only on an abstained ranking with no resolved type
                    # (DAT-859); "" is the honest placeholder, never rendered
                    # (_append_drivers skips non-"measured" rows outright).
                    target_type=art.target_type or "",
                    grain=art.grain,
                    entity=art.entity,
                    status=art.status,
                    abstain_reason=art.abstain_reason,
                    ranked_dimensions=art.ranked_dimensions or [],
                    interesting_slices=art.interesting_slices or [],
                    secondary_dimensions=art.secondary_dimensions or [],
                )
            )

    # 11. Load derived columns from correlation analysis — run-versioned since
    # DAT-448, same fail-closed discipline as the slices above (scoped to the
    # resolved catalog run; empty when none resolves).
    derived_columns: dict[str, str] = {}  # column_id -> formula
    if column_ids and run_id is not None:
        derived_stmt = select(DerivedColumn).where(DerivedColumn.derived_column_id.in_(column_ids))
        derived_stmt = derived_stmt.where(DerivedColumn.run_id == run_id)
        for derived in session.execute(derived_stmt).scalars().all():
            derived_columns[derived.derived_column_id] = derived.formula

    # The operating_model run — the version axis BOTH cycles (13), validation
    # results (13b), and cycle health (13d) scope to (they must all describe ONE
    # run). An explicit ``om_run_id`` wins: the in-run metrics phase passes its
    # CURRENT run so the graph context sees this run's evidence (the validation +
    # business_cycles activities ran earlier in the same run and committed). With
    # no override, fall back to the PROMOTED operating_model catalog head — the
    # post-promote current-state read (the query agent's path). **Fail-closed
    # (DAT-429):** no promoted run ⇒ no current operating_model state, never a
    # cross-run read that would mix superseded runs into this context.
    if om_run_id is None:
        om_run_id = head_run_id(session, catalog_head_target(), "operating_model")

    # 13. Load business cycles — run-versioned (DAT-455/DAT-506), scoped to the
    # promoted operating_model run (fail-closed above).
    business_cycle_contexts: list[BusinessCycleContext] = []
    cycles_iter = (
        session.execute(
            select(DetectedBusinessCycle)
            .where(DetectedBusinessCycle.run_id == om_run_id)
            .order_by(DetectedBusinessCycle.detected_at.desc())
        )
        .scalars()
        .all()
        if om_run_id is not None
        else []
    )
    for cycle in cycles_iter:
        stages = [
            CycleStageContext(
                stage_name=s.get("stage_name", ""),
                stage_order=s.get("stage_order", 0),
                indicator_column=s.get("indicator_column"),
                indicator_values=s.get("indicator_values", []),
                completion_rate=s.get("completion_rate"),
            )
            for s in (cycle.stages or [])
        ]
        entity_flows = [
            EntityFlowContext(
                entity_type=ef.get("entity_type", ""),
                entity_column=ef.get("entity_column", ""),
                entity_table=ef.get("entity_table", ""),
                fact_table=ef.get("fact_table"),
                relationship_type=ef.get("relationship_type"),
            )
            for ef in (cycle.entity_flows or [])
        ]
        business_cycle_contexts.append(
            BusinessCycleContext(
                cycle_name=cycle.cycle_name,
                cycle_type=cycle.canonical_type or cycle.cycle_type,
                family=cycle.family,
                direction=cycle.direction,
                tables_involved=cycle.tables_involved,
                completion_rate=cycle.completion_rate,
                description=cycle.description,
                business_value=cycle.business_value,
                confidence=cycle.confidence,
                stages=stages,
                entity_flows=entity_flows,
                # Bare parts (DAT-733) — the resolver + narrative re-combine as needed.
                status_table=cycle.status_table,
                status_column=cycle.status_column,
                completion_value=cycle.completion_value,
                total_records=cycle.total_records,
                completed_cycles=cycle.completed_cycles,
                evidence=cycle.evidence or [],
            )
        )

    # 13b. Load validation results — run-versioned since DAT-438: scope to the
    # SAME promoted operating_model head as the cycles above (resolved once at
    # 13). Fail-closed (DAT-429): no run ⇒ no current validation results.
    #
    # The pass/fail VERDICT is recomputed ON DEMAND (DAT-617): re-run each
    # check's run-versioned ``sql_used`` against current data rather than read a
    # stored verdict that goes stale on re-import. A bind failure (no
    # ``sql_used``) has no data verdict to recompute and no grounded SQL to feed
    # the metric agent — its grounding outcome lives in ``lifecycle_artifacts``,
    # surfaced by the cockpit, not in this data-quality context. So skip the
    # unbound rows here (and the unit/no-connection path, which can't re-run).
    from dataraum.analysis.validation.config import load_all_validation_specs
    from dataraum.analysis.validation.db_models import ValidationResultRecord
    from dataraum.analysis.validation.evaluate import evaluate_validation

    # Specs are read at the SAME run as the results below (DAT-877): the results are
    # scoped to ``om_run_id``, so reading the vocabulary head-free would join this
    # run's results against the PREVIOUS generation's specs — a newly-induced
    # validation_id silently drops at the spec lookup, and an id present in both
    # generations with a drifted tolerance/severity gets evaluated against the stale
    # one. Unconditional is safe post-promote: the promoted run's staged set IS the
    # materialized vocabulary.
    val_specs = load_all_validation_specs(vertical, session, run_id=om_run_id) if vertical else {}
    validation_contexts: list[ValidationContext] = []
    # No specs (no vertical) ⇒ every row would be skipped at the spec lookup, so
    # skip the read entirely rather than scan validation_results for nothing.
    if om_run_id is not None and duckdb_conn is not None and val_specs:
        # One row per validation_id per run (uq_validation_result_run) — no
        # latest-wins dedup needed. ValidationResultRecord has no source_id;
        # filter post-hoc by table_id overlap (table_ids is a JSON array).
        val_stmt = select(ValidationResultRecord).where(ValidationResultRecord.run_id == om_run_id)
        table_id_set = set(table_ids)
        for val_rec in session.execute(val_stmt).scalars().all():
            if not (table_id_set & set(val_rec.table_ids)):
                continue
            spec = val_specs.get(val_rec.validation_id)
            if not (val_rec.sql_used and spec is not None):
                continue
            verdict = evaluate_validation(duckdb_conn, val_rec.sql_used, spec)
            validation_contexts.append(
                ValidationContext(
                    validation_id=val_rec.validation_id,
                    status=verdict.status.value,
                    severity=spec.severity.value,
                    passed=verdict.passed,
                    message=verdict.message,
                    details=verdict.details,
                )
            )

    # 13c. Load enriched views
    from dataraum.analysis.views.db_models import EnrichedView

    enriched_view_contexts: list[EnrichedViewContext] = []
    ev_stmt = select(EnrichedView).where(EnrichedView.fact_table_id.in_(table_ids))
    for ev in session.execute(ev_stmt).scalars().all():
        fact_table = table_map.get(ev.fact_table_id)
        if fact_table:
            enriched_view_contexts.append(
                EnrichedViewContext(
                    view_name=ev.view_name,
                    fact_table=fact_table.table_name,
                    dimension_columns=ev.dimension_columns or [],
                    is_grain_verified=ev.is_grain_verified,
                    # derived_from bases (og_derived_from, DAT-734) — the graph's
                    # view → dimension-table edges served as structure.
                    dimension_tables=(
                        graph_reads.dimension_tables_by_view.get(ev.view_name, [])
                        if graph_reads
                        else []
                    ),
                )
            )

    # 13d. Compute cycle health. The runtime vertical is passed by the caller
    # (DAT-506 — the InvestigationSession row is gone; sessions live in cockpit_db).
    from dataraum.analysis.cycles.health import compute_cycle_health

    cycle_health_report: HealthReport | None = None
    if vertical and om_run_id is not None:
        try:
            # Same promoted operating_model run as 13/13b — cycles, their
            # validation evidence, and health all describe ONE run. duckdb_conn
            # lets the pass rate re-run each check's sql_used on demand (DAT-617).
            cycle_health_report = compute_cycle_health(
                session, duckdb_conn=duckdb_conn, vertical=vertical, run_id=om_run_id
            )
        except Exception as e:
            logger.warning("cycle_health_failed", error=str(e))

    # 14. Load the column meaning feed (catalogue-grain, DAT-637/769)
    field_mappings = load_column_meanings(session, table_ids, catalogue_run_id=catalogue_run_id)

    # 14b. Load the vertical's ontology — the conventions slot (DAT-645, piped
    # verbatim) plus the definition garnish for the graph-served concepts below.
    # On long-format data the discriminating measure (`amount`) may carry only a
    # generic meaning, so the concept definitions (indicators/excludes) are what
    # let the agent ground which discriminator VALUES mean each concept.
    conventions: str = ""
    ontology_obj: Any = None
    if vertical:
        try:
            from dataraum.analysis.semantic.concept_store import load_workspace_concepts
            from dataraum.analysis.semantic.ontology import OntologyLoader

            # Concepts AND conventions from the typed vocabulary tables (DAT-728 /
            # DAT-789, config→DB): load_workspace_concepts now carries the DB
            # conventions on its OntologyDefinition, so the extraction render reads the
            # typed `conventions` home, not the YAML.
            ontology_obj = load_workspace_concepts(session, vertical)
            conventions = OntologyLoader().format_conventions_for_prompt(ontology_obj, "extraction")
        except Exception as e:
            logger.warning("concept_vocabulary_load_failed", vertical=vertical, error=str(e))
            # A load FAILURE must never render as "the vertical declares none" —
            # the prompt template asserts exactly that for an empty conventions
            # slot, which would be affirmatively false here. Serve a labeled
            # failure the agent can see instead of a silent empty.
            conventions = (
                f"(convention lookup FAILED for vertical '{vertical}' — conventions "
                "may exist but could not be served; do not assume none apply)"
            )

    # 14c. Garnish the graph-served concepts (DAT-734) with the ontology's
    # description/indicators/exclude_patterns — the definition surface the agent
    # maps discriminator VALUES with. The graph rows stay authoritative for
    # membership (typed table, DAT-728); the ontology only decorates by name.
    concept_contexts: list[ConceptContext] = graph_reads.concepts if graph_reads else []
    if concept_contexts and ontology_obj is not None:
        by_name = {c.name: c for c in getattr(ontology_obj, "concepts", None) or []}
        for cc in concept_contexts:
            onto = by_name.get(cc.name)
            if onto is None:
                continue
            cc.description = onto.description or cc.description
            cc.indicators = list(onto.indicators or [])
            cc.exclude_patterns = list(onto.exclude_patterns or [])

    # 16. Build the per-column readiness lookup. The band is the single source of
    # truth the terminal detect step persisted (DAT-399 slice D) — read it, don't
    # recompute the noisy-OR. Serves the ⛔ blocked / ⚠ investigate markers the
    # grounding prompt's column-reliability contract reads.
    from dataraum.entropy.views.readiness_context import load_persisted_readiness

    persisted = load_persisted_readiness(session, table_ids)
    column_entropy_lookup: dict[str, dict[str, Any]] = {}
    for target, col_result in persisted.columns.items():
        # target is "column:table.col", extract "table.col"
        col_key = target.removeprefix("column:")
        column_entropy_lookup[col_key] = _column_readiness_to_dict(col_result)

    # 17. Build table contexts
    table_contexts: list[TableContext] = []

    for table_id in table_ids:
        table = table_map.get(table_id)
        if not table:
            continue

        # Get row count from DuckDB if available
        row_count = None
        if duckdb_conn and table.duckdb_path:
            try:
                query = f'SELECT COUNT(*) FROM "{table.duckdb_path}"'
                result = duckdb_conn.execute(query).fetchone()
                if result:
                    row_count = result[0]
            except Exception as e:
                logger.warning("row_count_query_failed", table=table.duckdb_path, error=str(e))

        # Build column contexts
        table_columns = columns_by_table.get(table_id, [])
        column_contexts: list[ColumnContext] = []

        for col in table_columns:
            stat_prof = stat_profiles.get(col.column_id)
            quality = stat_quality.get(col.column_id)
            sem_ann = semantic.get(col.column_id)
            temp_profile = temporal.get(col.column_id)
            type_dec = type_decisions.get(col.column_id)

            # Extract metrics
            null_ratio = stat_prof.null_ratio if stat_prof else None
            cardinality_ratio = stat_prof.cardinality_ratio if stat_prof else None
            # DAT-616: the value-set the agent needs to ground predicates lives in
            # the profile (distinct_count column + top_values in profile_data); the
            # assembler used to drop it. Lift it so format_served_context can serve
            # the complete enumeration for low-cardinality categoricals.
            distinct_count = stat_prof.distinct_count if stat_prof else None
            profile_data = (stat_prof.profile_data or {}) if stat_prof else {}
            top_values = profile_data.get("top_values", [])
            # DAT-621: the profiler stores only top-K (=20), incomplete for the median
            # dimension. For a categorical whose distinct_count is within the reasonable-top
            # but exceeds the stored set, fetch the COMPLETE value-set live so the agent
            # grounds on the full IN-list. Role+cardinality-gated; high-card columns keep the
            # stored top-K (rendered size+sample, never enumerated).
            _role = (sem_ann.semantic_role if sem_ann else None) or ""
            if (
                duckdb_conn is not None
                and table.duckdb_path
                and _role.lower() not in _NON_CATEGORICAL_ROLES
                and distinct_count is not None
                and len(top_values) < distinct_count <= _VALUE_SET_COMPLETE_MAX
            ):
                complete = _fetch_complete_value_set(
                    duckdb_conn, table.duckdb_path, col.column_name, _VALUE_SET_COMPLETE_MAX
                )
                if complete:
                    top_values = complete
            numeric_stats = profile_data.get("numeric_stats") or {}
            numeric_min = numeric_stats.get("min_value")
            numeric_max = numeric_stats.get("max_value")

            # Generate column flags (no outlier flag — DAT-543: outliers are not a
            # defect signal; heavy-tailed money columns naturally carry a high ratio).
            flags = _generate_column_flags(
                null_ratio=null_ratio,
                benford_status=quality.benford_status if quality else None,
                is_stale=temp_profile.is_stale if temp_profile else None,
                cardinality_ratio=cardinality_ratio,
            )

            # Add derived column flag
            is_derived = col.column_id in derived_columns
            if is_derived:
                flags.append("derived_column")

            # Get entropy data for this column
            entropy_key = f"{table.table_name}.{col.column_name}"
            col_entropy = column_entropy_lookup.get(entropy_key)

            column_contexts.append(
                ColumnContext(
                    column_id=col.column_id,
                    column_name=col.column_name,
                    table_name=table.table_name,
                    data_type=type_dec.decided_type if type_dec else None,
                    semantic_role=sem_ann.semantic_role if sem_ann else None,
                    # The graph-resolved materializes_as verdict + anchor axis
                    # (og_columns — witness posterior over concept prior).
                    materialization=(
                        graph_reads.materialization_by_column.get(col.column_id)
                        if graph_reads
                        else None
                    ),
                    anchor_time_axis=(
                        graph_reads.anchor_by_column.get(col.column_id) if graph_reads else None
                    ),
                    stored_sign=(
                        graph_reads.stored_sign_by_column.get(col.column_id)
                        if graph_reads
                        else None
                    ),
                    null_ratio=null_ratio,
                    cardinality_ratio=cardinality_ratio,
                    distinct_count=distinct_count,
                    top_values=top_values,
                    numeric_min=numeric_min,
                    numeric_max=numeric_max,
                    is_stale=temp_profile.is_stale if temp_profile else None,
                    detected_granularity=temp_profile.detected_granularity
                    if temp_profile
                    else None,
                    min_timestamp=str(temp_profile.min_timestamp)
                    if temp_profile and temp_profile.min_timestamp
                    else None,
                    max_timestamp=str(temp_profile.max_timestamp)
                    if temp_profile and temp_profile.max_timestamp
                    else None,
                    span_days=temp_profile.span_days if temp_profile else None,
                    largest_gap_days=temp_profile.largest_gap_days if temp_profile else None,
                    is_derived=is_derived,
                    derived_formula=derived_columns.get(col.column_id),
                    flags=flags,
                    entropy_scores=col_entropy,
                )
            )

        # Get table entity info
        table_entity = table_entities.get(table_id)

        # grain_columns is persisted as a bare JSON list of column names
        # (analysis/semantic/db_models.py TableEntity.grain_columns; DAT-775 —
        # a prior ``{"columns": [...]}`` wrapper was an unenforced convention
        # that corrupted a different reader's prompt).
        grain_cols: list[str] = (
            list(table_entity.grain_columns) if table_entity and table_entity.grain_columns else []
        )

        table_contexts.append(
            TableContext(
                table_id=table_id,
                table_name=table.table_name,
                duckdb_name=table.duckdb_path,
                row_count=row_count,
                column_count=len(column_contexts),
                table_role=table_entity.table_role if table_entity else None,
                entity_type=table_entity.detected_entity_type if table_entity else None,
                table_description=table_entity.description if table_entity else None,
                grain_columns=grain_cols,
                time_columns=(table_entity.time_columns or []) if table_entity else [],
                identity_columns=((table_entity.identity_columns or []) if table_entity else []),
                columns=column_contexts,
            )
        )

    return GraphExecutionContext(
        tables=table_contexts,
        reporting_calendar=_reporting_calendar(session, workspace_id),
        relationships=relationships,
        available_slices=slice_contexts,
        slice_catalog_note=slice_catalog_note,
        drivers=driver_contexts,
        business_cycles=business_cycle_contexts,
        cycle_health=cycle_health_report,
        validations=validation_contexts,
        enriched_views=enriched_view_contexts,
        concepts=concept_contexts,
        conformed_dimensions=(graph_reads.conformed_dimensions if graph_reads else []),
        field_mappings=field_mappings,
        conventions=conventions,
        operating_model_analyzed=om_run_id is not None,
        graph_readable=graph_reads is not None,
    )


# =============================================================================
# Property-graph reads (DAT-734) — the operating-model graph as context source
# =============================================================================
#
# The traversal core reads the LANDED property graph (ADR-0021), never the
# base tables: PGQ ``MATCH`` for the fixed-hop edge reads (grounded_by, uses,
# concept_edge), the bounded recursive CTE ONLY for the part_of transitive
# closure, and plain reads of the ``og_*`` element views for vertex maps and
# edges needing a non-graph property. Postgres-only (SQL/PGQ); on another
# dialect or a failed read the graph sections come back EMPTY with a loud log —
# assembly never crashes, and the eval's MATCH-returns-rows oracles catch a
# structurally dead graph.

# Depth cap for the part_of closure — the ADR-0021 bounded-CTE mechanism (≈4
# hops with a cycle guard; PGQ MATCH is fixed-depth and cannot walk it).
_PART_OF_MAX_DEPTH = 4


@dataclass
class _GraphReads:
    """Internal carrier for one traversal pass over the operating-model graph."""

    concepts: list[ConceptContext] = field(default_factory=list)
    references: list[RelationshipContext] = field(default_factory=list)
    conformed_dimensions: list[ConformedDimensionContext] = field(default_factory=list)
    materialization_by_column: dict[str, str] = field(default_factory=dict)
    anchor_by_column: dict[str, str] = field(default_factory=dict)
    stored_sign_by_column: dict[str, str] = field(default_factory=dict)
    dimension_tables_by_view: dict[str, list[str]] = field(default_factory=dict)


def _reporting_calendar(
    session: Session, workspace_id: str | None
) -> ReportingCalendarContext | None:
    """The workspace's reporting calendar for the served context (DAT-887).

    ONE home for the calendar read: ``boundary_resolver`` owns it (it is the module
    that binds against it), and this serves the same values to the author so the
    prompt's instruction to leave the period axis alone names a window the author
    can actually see. ``None`` — served as absence, never as a fabricated calendar
    year — when there is no read surface or no calendar on it.
    """
    read_schema = _graph_read_schema(session, workspace_id)
    if read_schema is None:
        return None
    from dataraum.graphs.boundary_resolver import read_reporting_calendar

    # Same degrade-to-None guard the graph reads carry: a read schema materialized
    # before DAT-730 has no og_period_grain, and an UndefinedTable raised here would
    # abort the transaction and kill the ENTIRE context build for every table — not
    # just this one section. Serving no calendar is the correct degraded state; the
    # binding then abstains loudly rather than assuming a calendar year.
    try:
        calendar = read_reporting_calendar(session, read_schema)
    except Exception as exc:  # noqa: BLE001 - degrade-to-absent IS the contract
        logger.warning("reporting_calendar_unreadable", error=str(exc))
        session.rollback()
        return None
    if calendar is None:
        return None
    return ReportingCalendarContext(
        fiscal_year_start_month=calendar.fiscal_year_start_month,
        source=calendar.source,
    )


def _graph_read_schema(session: Session, workspace_id: str | None) -> str | None:
    """The workspace's read schema, or ``None`` when the graph is unreachable.

    The operating-model property graph is Postgres-only (SQL/PGQ, ADR-0021) and
    lives in the read schema. On another dialect (unit-test SQLite) or without a
    workspace identity there is nothing to traverse — log and serve empty graph
    sections, never crash context assembly.
    """
    bind = session.get_bind()
    if bind is None or bind.dialect.name != "postgresql":
        logger.debug("graph_context_skipped", reason="non-postgres dialect")
        return None
    if not workspace_id:
        logger.warning("graph_context_skipped", reason="no workspace_id")
        return None
    from dataraum.server.workspace import schema_name_for
    from dataraum.storage.read_views import read_schema_name_for

    return read_schema_name_for(schema_name_for(workspace_id))


def _read_og_tables(session: Session, read_schema: str) -> dict[str, tuple[str, str | None]]:
    """Vertex map ``table_id → (table_name, layer)`` from ``og_tables``."""
    rows = session.execute(
        text(f'SELECT table_id, table_name, layer FROM "{read_schema}".og_tables')  # noqa: S608
    ).all()
    return {str(r.table_id): (str(r.table_name), r.layer) for r in rows}


def _read_og_columns(session: Session, read_schema: str) -> dict[str, Any]:
    """Vertex map ``column_id → row`` from ``og_columns`` (name, table, semantics)."""
    rows = session.execute(
        text(
            f"SELECT column_id, table_id, column_name, materialization, anchor_time_axis,"  # noqa: S608
            f" stored_sign\n"
            f'FROM "{read_schema}".og_columns'
        )
    ).all()
    return {str(r.column_id): r for r in rows}


def _read_concept_rows(
    session: Session, read_schema: str, vertical: str | None
) -> list[tuple[str, str | None]]:
    """Active vocabulary ``(name, kind)`` rows from ``og_concepts``, name-ordered."""
    sql = f'SELECT name, kind FROM "{read_schema}".og_concepts'  # noqa: S608
    params: dict[str, Any] = {}
    if vertical:
        sql += " WHERE vertical = :vertical"
        params["vertical"] = vertical
    sql += " ORDER BY name"
    return [(str(r.name), r.kind) for r in session.execute(text(sql), params).all()]


def _read_concept_edges(session: Session, read_schema: str) -> list[Any]:
    """All concept_edge rows (part_of / disjoint_with / reconciles_with) via PGQ MATCH."""
    return list(
        session.execute(
            text(
                f'SELECT * FROM GRAPH_TABLE ("{read_schema}".operating_model\n'
                "  MATCH (a IS concept_node)-[e IS concept_edge]->(b IS concept_node)\n"
                "  COLUMNS (a.name AS from_name, e.predicate AS predicate,\n"
                "           e.tolerance AS tolerance, b.name AS to_name))"
            )
        ).all()
    )


def _read_reconciliation_rows(session: Session, read_schema: str) -> dict[tuple[str, str], Any]:
    """The last promoted run's evaluated tie-out, folded per ``(concept, partner)``.

    Read from ``current_concept_reconciliation``, so this is what the last
    PROMOTED operating_model run observed — the in-run metrics phase assembles
    its context before it writes this run's own rows, and a run in flight is
    never readable as the current head anyway (ADR-0008/0010).

    Folded here rather than in the assembly because the fold is a property of
    the rows: an assertion covering several grounding pairs reports how many
    were comparable and the WIDEST divergence among them, which is the pair that
    puts the tie-out most in question. When nothing was comparable, the shared
    abstention reason is carried only if every pair agreed on it — the per-pair
    detail stays in the table rather than being guessed at here.
    """
    rows = session.execute(
        text(  # noqa: S608 - read_schema is an internal identifier, not user input
            "SELECT from_concept, to_concept, status, verdict, abstain_reason,"
            " delta, relative_delta\n"
            f'FROM "{read_schema}".current_concept_reconciliation'
        )
    ).all()

    folded: dict[tuple[str, str], dict[str, Any]] = {}
    for r in rows:
        key = (str(r.from_concept), str(r.to_concept))
        acc = folded.setdefault(
            key,
            {
                "pairs": 0,
                "evaluated_pairs": 0,
                "status": None,
                "verdict": None,
                "abstain_reason": None,
                "delta": None,
                "relative_delta": None,
                "reasons": set(),
            },
        )
        acc["pairs"] += 1
        if r.status != "evaluated":
            acc["reasons"].add(r.abstain_reason)
            continue
        acc["evaluated_pairs"] += 1
        acc["status"] = "evaluated"
        widest = acc["relative_delta"]
        current = float(r.relative_delta) if r.relative_delta is not None else 0.0
        if widest is None or current > widest:
            acc["relative_delta"] = current
            acc["delta"] = float(r.delta) if r.delta is not None else None
            acc["verdict"] = r.verdict

    for acc in folded.values():
        reasons = acc.pop("reasons")
        if acc["status"] is None:
            acc["status"] = "abstained"
            acc["abstain_reason"] = next(iter(reasons)) if len(reasons) == 1 else None
    return folded


def _read_part_of_ancestry(session: Session, read_schema: str) -> dict[str, list[str]]:
    """Transitive ``part_of`` ancestors per concept, depth 2..cap, nearest first.

    The bounded recursive CTE over ``og_concept_edges`` — ADR-0021's closure
    mechanism (PGQ MATCH is fixed-depth). Cycle guard: the walk never re-enters
    a concept already on its path; depth capped at ``_PART_OF_MAX_DEPTH``.
    1-hop parents are served separately (``part_of_parents``); this returns only
    the strictly-transitive tail (depth ≥ 2).
    """
    rows = session.execute(
        text(
            f"""
            WITH RECURSIVE part_of_walk AS (
                SELECT e.from_concept_id AS descendant_id,
                       e.to_concept_id AS ancestor_id,
                       1 AS depth,
                       ARRAY[e.from_concept_id, e.to_concept_id] AS path
                FROM "{read_schema}".og_concept_edges e
                WHERE e.predicate = 'part_of'
                UNION ALL
                SELECT w.descendant_id, e.to_concept_id, w.depth + 1,
                       w.path || e.to_concept_id
                FROM part_of_walk w
                JOIN "{read_schema}".og_concept_edges e
                  ON e.from_concept_id = w.ancestor_id
                WHERE e.predicate = 'part_of'
                  AND w.depth < {_PART_OF_MAX_DEPTH}
                  AND e.to_concept_id <> ALL(w.path)
            )
            SELECT cd.name AS descendant, ca.name AS ancestor, MIN(w.depth) AS depth
            FROM part_of_walk w
            JOIN "{read_schema}".og_concepts cd ON cd.concept_id = w.descendant_id
            JOIN "{read_schema}".og_concepts ca ON ca.concept_id = w.ancestor_id
            GROUP BY cd.name, ca.name
            HAVING MIN(w.depth) >= 2
            ORDER BY cd.name, MIN(w.depth), ca.name
            """  # noqa: S608 — read_schema is engine-derived, never user input
        )
    ).all()
    out: dict[str, list[str]] = {}
    for r in rows:
        out.setdefault(str(r.descendant), []).append(str(r.ancestor))
    return out


def _read_grounding_rows(session: Session, read_schema: str) -> list[Any]:
    """Concept → grounding rows via PGQ MATCH over ``grounded_by``."""
    return list(
        session.execute(
            text(
                f'SELECT * FROM GRAPH_TABLE ("{read_schema}".operating_model\n'
                "  MATCH (c IS concept_node)-[e IS grounded_by]->(g IS grounding_node)\n"
                "  COLUMNS (c.name AS concept_name, g.snippet_id AS snippet_id,\n"
                "           g.relation AS relation, g.select_expr AS select_expr,\n"
                "           g.where_predicates AS where_predicates,\n"
                "           g.statement AS statement, g.aggregation AS aggregation,\n"
                "           g.description AS description, g.failed AS failed))"
            )
        ).all()
    )


def _read_use_rows(session: Session, read_schema: str) -> list[Any]:
    """Grounding → column rows via PGQ MATCH over ``uses``."""
    return list(
        session.execute(
            text(
                f'SELECT * FROM GRAPH_TABLE ("{read_schema}".operating_model\n'
                "  MATCH (g IS grounding_node)-[u IS uses]->(col IS column_node)\n"
                "  COLUMNS (g.snippet_id AS snippet_id, u.role AS role,\n"
                "           col.column_name AS column_name, col.table_id AS table_id))"
            )
        ).all()
    )


def _read_grounding_provenance(session: Session, read_schema: str) -> dict[str, Any]:
    """``snippet_id → (concept, failed, failure_mode, failure_reason)`` from the source view.

    ``current_groundings`` is the graph's membership authority; the failure keys
    live in the provenance JSON (not vertex properties). Also the basis for the
    dropped-edge check: a snippet here but absent from the ``grounded_by`` MATCH
    names no ACTIVE concept — that edge dropped, and absence falls loud.
    """
    rows = session.execute(
        text(
            f"SELECT snippet_id, concept, failed,\n"  # noqa: S608
            f"       provenance->>'failure_mode' AS failure_mode,\n"
            f"       provenance->>'failure_reason' AS failure_reason\n"
            f'FROM "{read_schema}".current_groundings'
        )
    ).all()
    return {str(r.snippet_id): r for r in rows}


def _read_references(
    session: Session,
    read_schema: str,
    tables: dict[str, tuple[str, str | None]],
    columns: dict[str, Any],
    table_ids: list[str],
) -> list[RelationshipContext]:
    """FK edges from ``og_references``, enriched with ``introduces_duplicates``.

    The fan-out flag lives in ``current_relationships.evidence`` (measured by
    the writers, DAT-790) — not a graph property — so this reads the element
    view joined back to its source view by the shared local key. Endpoint names
    resolve through the vertex maps; a miss drops the edge VISIBLY. Scoped to
    edges between the context's tables (parity with the old per-context read).
    """
    rows = session.execute(
        text(
            f"SELECT r.relationship_id, r.from_table_id, r.to_table_id,\n"  # noqa: S608
            f"       r.from_column_id, r.to_column_id, r.cardinality,\n"
            f"       r.relationship_type, r.confidence, r.confirmation_source,\n"
            f"       (cr.evidence ->> 'introduces_duplicates')::boolean AS introduces_duplicates\n"
            f'FROM "{read_schema}".og_references r\n'
            f'JOIN "{read_schema}".current_relationships cr\n'
            f"  ON cr.relationship_id = r.relationship_id"
        )
    ).all()
    wanted = set(table_ids)
    out: list[RelationshipContext] = []
    for r in rows:
        if not (str(r.from_table_id) in wanted and str(r.to_table_id) in wanted):
            continue
        ft, tt = tables.get(str(r.from_table_id)), tables.get(str(r.to_table_id))
        fc, tc = columns.get(str(r.from_column_id)), columns.get(str(r.to_column_id))
        if ft is None or tt is None or fc is None or tc is None:
            logger.warning("reference_endpoint_unresolved", relationship_id=str(r.relationship_id))
            continue
        out.append(
            RelationshipContext(
                from_table=ft[0],
                from_column=str(fc.column_name),
                to_table=tt[0],
                to_column=str(tc.column_name),
                relationship_type=str(r.relationship_type or "unknown"),
                cardinality=r.cardinality,
                confidence=float(r.confidence or 0.0),
                confirmation_source=r.confirmation_source,
                introduces_duplicates=r.introduces_duplicates,
            )
        )
    out.sort(key=lambda x: (x.from_table, x.from_column, x.to_table, x.to_column))
    return out


def _read_conformed(
    session: Session, read_schema: str, tables: dict[str, tuple[str, str | None]]
) -> list[ConformedDimensionContext]:
    """Unordered conformed-dimension axes from ``og_conformed_dimension``.

    The view emits both directions and one row per slice-row pair; this dedupes
    to one row per (fact pair, dim table, attribute). A vertex the tables map
    cannot resolve drops the edge VISIBLY (warning), never silently.
    """
    rows = session.execute(
        text(
            f"SELECT DISTINCT from_table_id, to_table_id, dimension_table_id,\n"  # noqa: S608
            f"       dimension_attribute, conformed_group, confirmation_source,\n"
            f"       from_role, to_role\n"
            f'FROM "{read_schema}".og_conformed_dimension'
        )
    ).all()
    seen: set[tuple[str, str, str, str | None, str | None]] = set()
    out: list[ConformedDimensionContext] = []
    for r in rows:
        a_id, b_id = sorted((str(r.from_table_id), str(r.to_table_id)))
        # The pair is normalized by sorting the ids, so the roles must follow that
        # swap — otherwise each fact would be shown joining on the OTHER's column.
        role_a, role_b = (
            (r.from_role, r.to_role) if a_id == str(r.from_table_id) else (r.to_role, r.from_role)
        )
        key = (a_id, b_id, str(r.dimension_table_id), r.dimension_attribute, r.conformed_group)
        if key in seen:
            continue
        seen.add(key)
        a, b, dim = tables.get(a_id), tables.get(b_id), tables.get(str(r.dimension_table_id))
        if a is None or b is None or dim is None:
            logger.warning(
                "conformed_dimension_endpoint_unresolved",
                from_table_id=a_id,
                to_table_id=b_id,
                dimension_table_id=str(r.dimension_table_id),
            )
            continue
        out.append(
            ConformedDimensionContext(
                table_a=a[0],
                table_b=b[0],
                dimension_table=dim[0],
                attribute=r.dimension_attribute,
                conformed_group=r.conformed_group,
                confirmation_source=r.confirmation_source,
                role_a=role_a,
                role_b=role_b,
            )
        )
    out.sort(
        key=lambda c: (
            c.table_a,
            c.table_b,
            c.dimension_table,
            c.attribute or "",
            c.conformed_group or "",
        )
    )
    return out


def _read_derived_from(
    session: Session, read_schema: str, tables: dict[str, tuple[str, str | None]]
) -> dict[str, list[str]]:
    """``view_name → [dimension base tables]`` from ``og_derived_from``."""
    rows = session.execute(
        text(
            f"SELECT view_table_id, base_table_id, base_role\n"  # noqa: S608
            f"FROM \"{read_schema}\".og_derived_from WHERE base_role = 'dimension'"
        )
    ).all()
    out: dict[str, list[str]] = {}
    for r in rows:
        view, base = tables.get(str(r.view_table_id)), tables.get(str(r.base_table_id))
        if view is None or base is None:
            logger.warning(
                "derived_from_endpoint_unresolved",
                view_table_id=str(r.view_table_id),
                base_table_id=str(r.base_table_id),
            )
            continue
        out.setdefault(view[0], []).append(base[0])
    for bases in out.values():
        bases.sort()
    return out


def _assemble_concept_contexts(
    concept_rows: list[tuple[str, str | None]],
    edge_rows: list[Any],
    ancestry: dict[str, list[str]],
    grounding_rows: list[Any],
    use_rows: list[Any],
    provenance: dict[str, Any],
    tables: dict[str, tuple[str, str | None]],
    reconciliations: dict[tuple[str, str], Any],
) -> list[ConceptContext]:
    """Fold the traversal reads into per-concept contexts (deterministic order).

    Loud-absence rules applied here:
    - a ``current_groundings`` row absent from the ``grounded_by`` MATCH names no
      active concept — the dropped edge is WARNED, never silently invisible;
    - a healthy grounding with no ``uses`` rows (pre-v2 provenance / rename) is
      served but WARNED — the graph could not enumerate its columns;
    - a healthy grounding with no relation (pre-parts row) is skipped + WARNED.
    """
    # uses per snippet, resolved to (column, table, role) — endpoint misses are loud.
    uses_by_snippet: dict[str, list[GroundingUseContext]] = {}
    for r in use_rows:
        table = tables.get(str(r.table_id))
        if table is None:
            logger.warning("uses_endpoint_unresolved", table_id=str(r.table_id))
            continue
        uses_by_snippet.setdefault(str(r.snippet_id), []).append(
            GroundingUseContext(
                column_name=str(r.column_name), table_name=table[0], role=str(r.role)
            )
        )

    groundings_by_concept: dict[str, list[GroundingContext]] = {}
    matched_snippets: set[str] = set()
    for r in grounding_rows:
        snippet_id = str(r.snippet_id)
        matched_snippets.add(snippet_id)
        failed = bool(r.failed)
        if not failed and r.relation is None:
            logger.warning(
                "grounding_relation_missing", snippet_id=snippet_id, concept=str(r.concept_name)
            )
            continue
        prov = provenance.get(snippet_id)
        uses = sorted(
            uses_by_snippet.get(snippet_id, []),
            key=lambda u: (u.role, u.table_name, u.column_name),
        )
        if not failed and not uses:
            logger.warning(
                "grounding_uses_empty", snippet_id=snippet_id, concept=str(r.concept_name)
            )
        try:
            where = json.loads(r.where_predicates) if r.where_predicates else []
        except TypeError, ValueError:
            where = None
        # json.loads can SUCCEED on non-list JSON (the literal ``null``, a bare
        # string/number) — parse-time exceptions alone don't cover that, and
        # iterating None crashed the whole context build (reviewer critical).
        if not isinstance(where, list):
            logger.warning("grounding_where_unparsable", snippet_id=snippet_id)
            where = []
        groundings_by_concept.setdefault(str(r.concept_name), []).append(
            GroundingContext(
                snippet_id=snippet_id,
                concept=str(r.concept_name),
                relation=r.relation,
                select_expr=r.select_expr,
                where=[str(w) for w in where],
                statement=r.statement,
                aggregation=r.aggregation,
                description=r.description,
                failed=failed,
                failure_mode=(prov.failure_mode if prov is not None else None),
                failure_reason=(prov.failure_reason if prov is not None else None),
                uses=uses,
            )
        )

    # Absence falls loud: a grounding the graph could not attach to any active
    # concept (name resolves no og_concepts row) — the edge dropped visibly.
    for snippet_id, prov in provenance.items():
        if snippet_id not in matched_snippets:
            logger.warning(
                "grounding_concept_unresolved", snippet_id=snippet_id, concept=str(prov.concept)
            )

    children: dict[str, list[str]] = {}
    parents: dict[str, list[str]] = {}
    disjoint: dict[str, set[str]] = {}
    reconciles: dict[str, list[ConceptReconciliation]] = {}
    for e in edge_rows:
        frm, to = str(e.from_name), str(e.to_name)
        if e.predicate == "part_of":
            children.setdefault(to, []).append(frm)
            parents.setdefault(frm, []).append(to)
        elif e.predicate == "disjoint_with":
            # Symmetric predicates are stored in BOTH directions (concept_edges
            # contract), so accumulating each row under its from-side populates
            # both endpoints — no reverse insertion needed here.
            disjoint.setdefault(frm, set()).add(to)
        elif e.predicate == "reconciles_with":
            # Self-loop (from == to) = the derived multi-grounding tie-out; a
            # cross pair (seed/declared, both directions stored) reads from the
            # from-side. Either way one row per (concept, partner).
            # The evaluated state rides the assertion it belongs to. Absent =
            # never evaluated, which the renderer must not report as agreement.
            observed = reconciliations.get((frm, to), {})
            reconciles.setdefault(frm, []).append(
                ConceptReconciliation(
                    partner=to,
                    tolerance=float(e.tolerance) if e.tolerance is not None else None,
                    status=observed.get("status"),
                    verdict=observed.get("verdict"),
                    abstain_reason=observed.get("abstain_reason"),
                    observed_delta=observed.get("delta"),
                    relative_delta=observed.get("relative_delta"),
                    pairs=observed.get("pairs", 0),
                    evaluated_pairs=observed.get("evaluated_pairs", 0),
                )
            )

    out: list[ConceptContext] = []
    for name, kind in concept_rows:
        out.append(
            ConceptContext(
                name=name,
                kind=kind,
                part_of_children=sorted(children.get(name, [])),
                part_of_parents=sorted(parents.get(name, [])),
                part_of_ancestry=ancestry.get(name, []),
                disjoint_with=sorted(disjoint.get(name, set())),
                reconciles_with=sorted(reconciles.get(name, []), key=lambda x: x.partner),
                groundings=sorted(
                    groundings_by_concept.get(name, []),
                    key=lambda g: (g.failed, g.relation or "", g.snippet_id),
                ),
            )
        )
    return out


def _load_graph_reads(
    session: Session,
    workspace_id: str | None,
    vertical: str | None,
    table_ids: list[str],
) -> _GraphReads | None:
    """One traversal pass over the operating-model property graph (DAT-734).

    Returns ``None`` (empty graph sections) when the graph is unreachable —
    non-Postgres dialect, no workspace identity, or a failed read. A failure is
    WARNED, never raised: context assembly must not die on the graph, and the
    eval's MATCH-returns-rows oracles catch a structurally dead graph.
    """
    read_schema = _graph_read_schema(session, workspace_id)
    if read_schema is None:
        return None
    # The WHOLE pass — reads AND the assembly fold — degrades to None on any
    # failure. The fold must not sit outside this guard: an edge-shaped row it
    # chokes on would otherwise kill the entire context build for every table,
    # not just the graph sections (reviewer critical).
    try:
        tables = _read_og_tables(session, read_schema)
        columns = _read_og_columns(session, read_schema)
        concept_rows = _read_concept_rows(session, read_schema, vertical)
        edge_rows = _read_concept_edges(session, read_schema)
        ancestry = _read_part_of_ancestry(session, read_schema)
        grounding_rows = _read_grounding_rows(session, read_schema)
        use_rows = _read_use_rows(session, read_schema)
        provenance = _read_grounding_provenance(session, read_schema)
        reconciliations = _read_reconciliation_rows(session, read_schema)
        references = _read_references(session, read_schema, tables, columns, table_ids)
        conformed = _read_conformed(session, read_schema, tables)
        derived = _read_derived_from(session, read_schema, tables)
        concepts = _assemble_concept_contexts(
            concept_rows,
            edge_rows,
            ancestry,
            grounding_rows,
            use_rows,
            provenance,
            tables,
            reconciliations,
        )
        return _GraphReads(
            concepts=concepts,
            references=references,
            conformed_dimensions=conformed,
            materialization_by_column={
                cid: str(r.materialization) for cid, r in columns.items() if r.materialization
            },
            anchor_by_column={
                cid: str(r.anchor_time_axis) for cid, r in columns.items() if r.anchor_time_axis
            },
            stored_sign_by_column={
                cid: str(r.stored_sign) for cid, r in columns.items() if r.stored_sign
            },
            dimension_tables_by_view=derived,
        )
    except Exception as e:
        logger.warning("graph_context_read_failed", error=str(e))
        return None


# =============================================================================
# Flag Generation (inlined from quality/context.py)
# =============================================================================


def _generate_column_flags(
    null_ratio: float | None,
    benford_status: str | None,
    is_stale: bool | None,
    cardinality_ratio: float | None,
) -> list[str]:
    """Generate actionable flags from column metrics.

    NOTE (DAT-543): there is deliberately NO ``high_outliers`` flag. A raw IQR/
    z-score outlier RATIO assumes an ~normal distribution; monetary and other
    heavy-tailed columns (log-normal-ish — a few large invoices/journal lines)
    naturally carry a high outlier ratio, so the old ``ratio > 0.1`` rule flagged
    every money column as unreliable and fed the grounding agent a spurious
    "blocked" caveat. Outliers are legitimate data, not a defect — never gate on
    them here. (The entropy detectors still MEASURE outliers for their own
    calibrated signals; this is only the agent-facing flag.)
    """
    flags = []

    if null_ratio is not None and null_ratio > 0.5:
        flags.append("high_nulls")
    elif null_ratio is not None and null_ratio > 0.1:
        flags.append("moderate_nulls")

    # Only a MEASURED violation flags (DAT-843): 'not_applicable' (values under
    # one order of magnitude — Benford undefined) and NULL (not computed) must
    # never read as a violation to the agent.
    if benford_status == "violating":
        flags.append("benford_violation")

    if is_stale is True:
        flags.append("stale_data")

    if cardinality_ratio is not None:
        if cardinality_ratio > 0.99:
            flags.append("near_unique")
        elif cardinality_ratio < 0.01:
            flags.append("low_cardinality")

    return flags


# =============================================================================
# Readiness-to-dict converters
# =============================================================================


def _column_readiness_to_dict(result: Any) -> dict[str, Any]:
    """Convert ColumnReadinessResult to dict for ColumnContext.entropy_scores.

    Args:
        result: ColumnReadinessResult from the readiness rollup

    Returns:
        Dict compatible with existing entropy_scores consumers
    """
    high_dims = [ne.dimension_path for ne in result.node_evidence if ne.state != "low"]
    return {
        "worst_intent_risk": result.worst_intent_risk,
        "readiness": result.readiness,
        # DAT-853: 'unmeasured'/'partial' tell the agent a 'ready' band rests on
        # missing measurements, not on evidence of cleanliness.
        "coverage": result.coverage,
        "top_priority_node": result.top_priority_node,
        "top_priority_impact": result.top_priority_impact,
        "high_entropy_dimensions": high_dims,
        "intents": [
            {"name": i.intent_name, "risk": i.risk, "readiness": i.readiness}
            for i in result.intents
        ],
    }


def _fetch_complete_value_set(
    duckdb_conn: duckdb.DuckDBPyConnection,
    duckdb_path: str,
    column_name: str,
    limit: int,
) -> list[dict[str, Any]] | None:
    """Live freq-ordered value-set for a low-card categorical (DAT-621).

    The profiler stores only the top-K (=20), which is incomplete for the median
    dimension. When a categorical's `distinct_count` is within the reasonable-top, fetch
    its COMPLETE `{value, count}` set here so the agent grounds on the full IN-list. Bounded
    by `limit`; the assembled context is cacheable so the cost amortizes. Returns None on
    any failure (caller keeps the stored top-K).
    """
    try:
        rows = duckdb_conn.execute(
            f'SELECT "{column_name}" AS value, COUNT(*) AS count '
            f'FROM "{duckdb_path}" WHERE "{column_name}" IS NOT NULL '
            f"GROUP BY 1 ORDER BY count DESC, value LIMIT {limit}"
        ).fetchall()
        return [{"value": v, "count": int(c)} for v, c in rows]
    except Exception as e:  # pragma: no cover - best-effort; falls back to stored top-K
        logger.debug("complete_value_set_failed", column=column_name, error=str(e))
        return None


__all__ = [
    "build_execution_context",
]
