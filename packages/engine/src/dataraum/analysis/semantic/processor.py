"""Semantic enrichment processor.

Orchestrates semantic analysis using the SemanticAgent and stores results.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any

import duckdb
from sqlalchemy import delete
from sqlalchemy.orm import Session

if TYPE_CHECKING:
    from dataraum.llm.config import LLMConfig
    from dataraum.llm.prompts import PromptRenderer
    from dataraum.llm.providers.base import LLMProvider

from dataraum.analysis.relationships.composite import rescue_fanout_to_composite
from dataraum.analysis.relationships.db_models import Relationship as RelationshipModel
from dataraum.analysis.relationships.evaluator import (
    compute_actual_cardinality,
    compute_composite_cardinality,
    compute_introduces_duplicates,
    compute_ri_metrics,
)
from dataraum.analysis.relationships.models import JoinCandidate, RelationshipCandidate
from dataraum.analysis.semantic.agent import SemanticAgent
from dataraum.analysis.semantic.db_models import (
    ColumnConcept as ConceptModel,
)
from dataraum.analysis.semantic.db_models import (
    SemanticAnnotation as AnnotationModel,
)
from dataraum.analysis.semantic.db_models import (
    TableEntity as EntityModel,
)
from dataraum.analysis.semantic.models import (
    ColumnAnnotationOutput,
    ColumnConceptOutput,
    SemanticEnrichmentResult,
)
from dataraum.analysis.semantic.models import (
    Relationship as SemanticRelationship,
)
from dataraum.analysis.semantic.utils import load_column_mappings, load_table_mappings
from dataraum.core.logging import get_logger
from dataraum.core.models.base import DecisionSource, Result
from dataraum.storage.upsert import upsert

logger = get_logger(__name__)


def _resolve_cardinality(
    rel: SemanticRelationship,
    evidence: dict[str, Any],
    duckdb_conn: duckdb.DuckDBPyConnection | None,
) -> str | None:
    """Determine actual cardinality from data, not from LLM guesses.

    Priority:
    1. Use pre-computed cardinality from relationship candidates (already verified)
    2. Compute from actual data if DuckDB available
    3. Fall back to None (unknown)
    """
    # 1. Use candidate's verified cardinality if available
    candidate_cardinality = evidence.get("cardinality")
    if candidate_cardinality:
        return str(candidate_cardinality)

    # 2. Compute from actual data
    if duckdb_conn is not None:
        from_table_path = f'lake.typed."{rel.from_table}"'
        to_table_path = f'lake.typed."{rel.to_table}"'
        actual = compute_actual_cardinality(
            from_table_path,
            to_table_path,
            rel.from_column,
            rel.to_column,
            duckdb_conn,
        )
        if actual:
            evidence["cardinality_verified"] = True
            return actual

    return None


def _build_candidate_metrics_lookup(
    relationship_candidates: list[dict[str, Any]] | None,
) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    """Build lookup of evaluation metrics from relationship candidates.

    Returns a dict keyed by (from_table, from_column, to_table, to_column)
    containing the RI metrics for each candidate join.
    """
    if not relationship_candidates:
        return {}

    lookup: dict[tuple[str, str, str, str], dict[str, Any]] = {}

    for candidate in relationship_candidates:
        table1 = candidate.get("table1", "")
        table2 = candidate.get("table2", "")

        for jc in candidate.get("join_columns", []):
            col1 = jc.get("column1", "")
            col2 = jc.get("column2", "")

            # Extract evaluation metrics
            metrics: dict[str, Any] = {}
            if "left_referential_integrity" in jc:
                metrics["left_referential_integrity"] = jc["left_referential_integrity"]
            if "right_referential_integrity" in jc:
                metrics["right_referential_integrity"] = jc["right_referential_integrity"]
            if "orphan_count" in jc:
                metrics["orphan_count"] = jc["orphan_count"]
            if "cardinality_verified" in jc:
                metrics["cardinality_verified"] = jc["cardinality_verified"]
            if "cardinality" in jc:
                metrics["cardinality"] = jc["cardinality"]

            # Add relationship-level metrics
            if "join_success_rate" in candidate:
                metrics["join_success_rate"] = candidate["join_success_rate"]
            if "introduces_duplicates" in candidate:
                metrics["introduces_duplicates"] = candidate["introduces_duplicates"]

            if metrics:
                lookup[(table1, col1, table2, col2)] = metrics

                # Build reverse entry with flipped direction-dependent fields
                reverse = dict(metrics)
                card = reverse.get("cardinality")
                if card == "one-to-many":
                    reverse["cardinality"] = "many-to-one"
                elif card == "many-to-one":
                    reverse["cardinality"] = "one-to-many"
                # Swap left/right RI
                left_ri = reverse.pop("left_referential_integrity", None)
                right_ri = reverse.pop("right_referential_integrity", None)
                if left_ri is not None:
                    reverse["right_referential_integrity"] = left_ri
                if right_ri is not None:
                    reverse["left_referential_integrity"] = right_ri
                # introduces_duplicates is directional — drop from reverse
                reverse.pop("introduces_duplicates", None)
                lookup[(table2, col2, table1, col1)] = reverse

    return lookup


def persist_column_annotations(
    session: Session,
    column_output: ColumnAnnotationOutput,
    table_ids: list[str],
    *,
    annotated_by: str,
    run_id: str | None = None,
) -> int:
    """Persist the OBJECT-grain per-column annotations as ``SemanticAnnotation`` rows.

    The per-column phase's authoritative output — single-table-knowable fields
    only (role, entity label, term, the stock/flow claim). Catalogue-grain
    semantics (business_concept, ontology temporal_behavior, unit source, derived
    formula) are NOT written here: the table agent authors them onto
    ``ColumnConcept`` under the catalogue head (DAT-637).

    Args:
        session: Database session.
        column_output: Per-column tool output (tables -> columns).
        table_ids: Tables the annotations belong to (for column-id resolution).
        annotated_by: Model identifier that produced the annotations.

    Returns:
        Number of annotation rows persisted.
    """
    column_map = load_column_mappings(session, table_ids)

    rows: list[dict[str, Any]] = []
    for table in column_output.tables:
        for col in table.columns:
            column_id = column_map.get((table.table_name, col.column_name))
            if not column_id:
                continue
            # PK omitted so the model's Python-side default applies. OBJECT-grain
            # only (DAT-637): business_concept, ontology temporal_behavior,
            # unit_source_column, and the derived_formula hypothesis are
            # catalogue-grain — authored by the table agent onto ``ColumnConcept``,
            # never here. The stock/flow CLAIM stays (an independent single-column
            # read).
            rows.append(
                {
                    "column_id": column_id,
                    "run_id": run_id,
                    "semantic_role": col.semantic_role,
                    "entity_type": col.entity_type,
                    "business_name": col.business_term,
                    "business_description": col.description,
                    "temporal_behavior_claim": col.temporal_behavior_claim,
                    "temporal_behavior_claim_confidence": col.temporal_behavior_claim_confidence,
                    "annotation_source": DecisionSource.LLM.value,
                    "annotated_by": annotated_by,
                    "confidence": col.confidence,
                }
            )

    # Upsert on ``(column_id, run_id)`` so a Temporal at-least-once retry
    # (same run_id) updates the annotation in place instead of duplicating it —
    # which would make the head-resolved loaders' scalar_one_or_none() raise.
    upsert(session, AnnotationModel, rows, index_elements=["column_id", "run_id"])
    return len(rows)


def persist_column_concepts(
    session: Session,
    column_concepts: list[ColumnConceptOutput],
    table_ids: list[str],
    *,
    annotated_by: str,
    ontology_def: Any = None,
    run_id: str,
) -> int:
    """Persist the table agent's catalogue-grain per-column semantics (DAT-637).

    Writes ``ColumnConcept`` rows under the begin_session (catalogue head) run.
    ``temporal_behavior`` is the ontology concept's stock/flow, derived from the
    authored ``business_concept`` exactly as the legacy per-column path did — but
    now at catalogue grain, where the concept is authoritative. Run-scoped upsert
    on ``(column_id, run_id)``; a column the table agent did not bind this run has
    no row (absent = no concept), and run-scoped reads never see a prior run's.

    Returns:
        Number of concept rows persisted.
    """
    column_map = load_column_mappings(session, table_ids)
    concept_temporal = (
        {c.name: c.temporal_behavior for c in ontology_def.concepts} if ontology_def else {}
    )

    rows: list[dict[str, Any]] = []
    for cc in column_concepts:
        column_id = column_map.get((cc.table_name, cc.column_name))
        if not column_id:
            continue
        rows.append(
            {
                "column_id": column_id,
                "run_id": run_id,
                "business_concept": cc.business_concept,
                "temporal_behavior": concept_temporal.get(cc.business_concept)
                if cc.business_concept
                else None,
                "unit_source_column": cc.unit_source_column,
                "derived_formula_hypothesis": (cc.derived_formula_hypothesis or "").strip() or None,
                "derived_formula_confidence": cc.derived_formula_confidence,
                "annotation_source": DecisionSource.LLM.value,
                "annotated_by": annotated_by,
            }
        )

    # Dedup on the upsert key (column_id, run_id): the table agent can emit the same
    # column twice in column_concepts, and ON CONFLICT cannot touch a row twice in
    # one batch (CardinalityViolation). Last mention wins.
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        deduped[(row["column_id"], row["run_id"])] = row
    rows = list(deduped.values())

    upsert(session, ConceptModel, rows, index_elements=["column_id", "run_id"])
    return len(rows)


def ground_columns(
    *,
    session: Session,
    config: LLMConfig,
    provider: LLMProvider,
    renderer: PromptRenderer,
    table_ids: list[str],
    ontology: str,
    run_id: str | None = None,
) -> Result[int]:
    """Annotate columns against ``ontology`` and persist ``SemanticAnnotation`` rows.

    DAT-376: extracted verbatim from the grounding tail of
    ``SemanticPerColumnPhase._run``. Grounding maps each column to its semantic
    role, entity type, business term, and ontology concept — it assumes the
    ontology already exists. For a cold-start ``_adhoc`` workspace the concepts
    are declared upstream by the cockpit ``frame`` stage (DAT-382), which writes
    them as ``concept`` overlay rows before ``add_source`` runs.

    Args:
        session: Database session.
        config: Loaded LLM config (gates on ``features.column_annotation``).
        provider: LLM provider (resolves the annotation model tier).
        renderer: Prompt renderer for the annotation agent.
        table_ids: Typed tables to annotate.
        ontology: Vertical name the columns map their concepts into.

    Returns:
        ``Result.ok(count)`` with the number of annotation rows persisted, or
        ``Result.fail`` with the same messages the phase surfaced before.
    """
    from dataraum.analysis.semantic.column_agent import ColumnAnnotationAgent
    from dataraum.graphs.config import get_metric_definitions
    from dataraum.graphs.loader import GraphLoader, GraphLoadError

    col_config = config.features.column_annotation
    if not col_config or not col_config.enabled:
        return Result.fail("Column annotation is disabled in config.")

    # Standard-field concepts required by active metric graphs, so the model
    # prioritizes mapping those concepts to actual columns. OVERLAY-AWARE: the
    # declared set is the vertical's shipped graphs ⊕ `metric` overlay teach rows
    # (get_metric_definitions), so a FRAMED vertical's metrics — declared at frame
    # time, no on-disk directory — steer grounding too (a file-only read would
    # return nothing for a framed/_adhoc vertical — DAT-471 AC3). The worker
    # bootstrap installs the overlay resolver process-wide, so it resolves here in
    # the add_source semantic phase exactly as it does in the operating_model
    # metrics phase.
    metric_loader = GraphLoader()
    for graph_id, defn in get_metric_definitions(ontology).items():
        # A declared metric that won't parse is skipped for this grounding HINT —
        # its born-loud handling (declared-with-reason) is the metrics phase's job
        # at operating_model; here a malformed graph must not sink column grounding.
        try:
            metric_loader.graphs.update(metric_loader.graphs_from_definitions({graph_id: defn}))
        except GraphLoadError as exc:
            logger.warning("metric_grounding_hint_skip", graph_id=graph_id, error=str(exc))
    required_standard_fields = sorted(metric_loader.get_all_abstract_fields())

    agent = ColumnAnnotationAgent(config=config, provider=provider, prompt_renderer=renderer)
    annotation_result = agent.annotate(
        session=session,
        table_ids=table_ids,
        ontology=ontology,
        required_standard_fields=required_standard_fields,
    )
    if not annotation_result.success or not annotation_result.value:
        return Result.fail(f"Column annotation failed: {annotation_result.error}")

    model_name = provider.get_model_for_tier(col_config.model_tier)

    count = persist_column_annotations(
        session,
        annotation_result.value,
        table_ids,
        annotated_by=model_name,
        run_id=run_id,
    )
    return Result.ok(count)


def _lake_path(table_name: str) -> str:
    """Collision-safe DuckLake FQN for a typed table (matches the RI/cardinality paths)."""
    return f'lake.typed."{table_name}"'


def _augment_candidates_with_composite_rescue(
    relationship_candidates: list[dict[str, Any]],
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> None:
    """Surface composite-key rescues to the LLM judge (DAT-277), in place.

    The structural detector emits each value-overlapping column pair separately;
    when the best pair joins many-to-many it silently over-counts. For each such
    candidate this runs the greedy rescue — does fusing co-present columns collapse
    the fan-out? — and, when it does, attaches a ``composite_key`` hint to the
    candidate dict. The LLM (the only judge, never bypassed) sees the hint and may
    confirm it via ``RelationshipOutput.key_columns``. A miss attaches nothing.
    """
    for cand in relationship_candidates:
        table1 = cand.get("table1")
        table2 = cand.get("table2")
        join_cols = cand.get("join_columns") or []
        if not table1 or not table2 or len(join_cols) < 2:
            continue

        candidate = RelationshipCandidate(
            table1=table1,
            table2=table2,
            join_candidates=[
                JoinCandidate(
                    column1=jc.get("column1", ""),
                    column2=jc.get("column2", ""),
                    # The DB candidate-dict wire format keys it ``confidence``
                    # (load_relationship_candidates_for_semantic); tolerate both so
                    # the anchor (highest-confidence pair) is picked correctly.
                    join_confidence=jc.get("join_confidence", jc.get("confidence", 0.0)),
                    cardinality=jc.get("cardinality", "unknown"),
                )
                for jc in join_cols
                if jc.get("column1") and jc.get("column2")
            ],
        )
        try:
            key = rescue_fanout_to_composite(
                candidate, _lake_path(table1), _lake_path(table2), duckdb_conn
            )
        except Exception as e:  # never let a rescue probe break synthesis
            logger.warning("composite_rescue_failed", table1=table1, table2=table2, error=str(e))
            continue

        if key is not None:
            cand["composite_key"] = {
                "column_pairs": [list(pair) for pair in key.column_pairs],
                "cardinality": key.cardinality,
            }


def _build_composite_group_rows(
    *,
    rel: SemanticRelationship,
    from_table_id: str | None,
    from_col_id: str | None,
    to_table_id: str | None,
    to_col_id: str | None,
    column_map: dict[tuple[str, str], str],
    evidence: dict[str, Any],
    run_id: str | None,
    duckdb_conn: duckdb.DuckDBPyConnection | None,
) -> list[dict[str, Any]] | None:
    """The N component rows of an LLM-confirmed composite key, or None (DAT-277).

    Returns ``None`` when ``rel`` is single-column (no ``key_columns``) or any
    component column is unresolvable — the caller then persists the single-column
    anchor as usual. Otherwise returns the anchor (position 0) plus one row per
    additional pair, all sharing a ``relationship_group_id`` and the COMPOSITE
    cardinality (computed from data over the full key; the rescue already proved it
    collapses the fan-out). The group id is DETERMINISTIC in
    ``(run_id, component column ids)`` so a Temporal at-least-once retry re-derives
    the same id and the upsert is idempotent — it never orphans a prior attempt's
    component rows under a stale uuid.
    """
    if not rel.key_columns or not from_col_id or not to_col_id:
        return None

    components: list[tuple[str, str]] = [(from_col_id, to_col_id)]
    pairs: list[tuple[str, str]] = [(rel.from_column, rel.to_column)]
    # Component column-id pairs already in the key — the upsert key is
    # (run_id, from_column_id, to_column_id, detection_method), so a duplicate
    # component (e.g. the LLM echoes the anchor pair in key_columns) would collide
    # within this one INSERT batch. Skip dups defensively.
    seen_component_ids: set[tuple[str, str]] = {components[0]}
    for from_name, to_name in rel.key_columns:
        comp_from = column_map.get((rel.from_table, from_name))
        comp_to = column_map.get((rel.to_table, to_name))
        if not comp_from or not comp_to:
            logger.warning(
                "composite_key_column_unresolved",
                from_table=rel.from_table,
                to_table=rel.to_table,
                from_column=from_name,
                to_column=to_name,
            )
            return None
        if (comp_from, comp_to) in seen_component_ids:
            continue  # LLM repeated the anchor or a prior component — already in the key
        seen_component_ids.add((comp_from, comp_to))
        components.append((comp_from, comp_to))
        pairs.append((from_name, to_name))

    if len(components) < 2:
        return None  # only the anchor survived dedup — not actually composite

    composite_cardinality = rel.cardinality
    if duckdb_conn is not None:
        try:
            composite_cardinality = compute_composite_cardinality(
                _lake_path(rel.from_table), _lake_path(rel.to_table), pairs, duckdb_conn
            )
        except Exception as e:
            logger.warning(
                "composite_cardinality_failed",
                from_table=rel.from_table,
                to_table=rel.to_table,
                error=str(e),
            )

    # Deterministic group id (DAT-277): stable for this run + this exact set of
    # component column ids, so a retry re-derives it and the upsert refreshes the
    # group in place instead of minting a new id and orphaning the prior rows.
    group_key = "|".join([str(run_id), *sorted(f"{a}>{b}" for a, b in components)])
    group_id = hashlib.sha1(group_key.encode()).hexdigest()  # noqa: S324 (id, not security)
    group_evidence = {**evidence, "composite_key_columns": len(components)}
    return [
        {
            "run_id": run_id,
            "from_table_id": from_table_id,
            "from_column_id": comp_from,
            "to_table_id": to_table_id,
            "to_column_id": comp_to,
            "relationship_type": rel.relationship_type.value,
            "cardinality": composite_cardinality,
            "confidence": rel.confidence,
            "detection_method": "llm",
            "evidence": group_evidence,
            "relationship_group_id": group_id,
            "key_position": position,
        }
        for position, (comp_from, comp_to) in enumerate(components)
    ]


def _dedup_relationship_rows(rel_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse rows colliding on the upsert conflict key (DAT-277).

    The conflict key is ``(run_id, from_column_id, to_column_id, detection_method)``.
    The LLM can emit one column pair BOTH as a standalone relationship AND as a
    composite component (a shared scope column), and Postgres'
    ON CONFLICT DO UPDATE cannot touch the same row twice in one statement
    (``CardinalityViolation``). Prefer the composite-component row — it carries the
    group + multi-column key, the richer truth — over a plain duplicate; otherwise
    first-wins (also folds an LLM that lists the same plain relationship twice).

    When the SAME directed pair is a component of two DIFFERENT composite groups, the
    relationship grain (one row per pair per method) cannot represent both — keeping
    either would silently corrupt the other into a partial group. Abstain: drop BOTH
    groups whole rather than persist a half key (worst case is a missed composite,
    never a malformed one).
    """

    def conflict_key(row: dict[str, Any]) -> tuple[Any, Any, Any, Any]:
        return (
            row["run_id"],
            row["from_column_id"],
            row["to_column_id"],
            row["detection_method"],
        )

    # Pass 1: a pair claimed by >1 distinct group poisons every group involved.
    by_key: dict[tuple[Any, Any, Any, Any], list[dict[str, Any]]] = {}
    for row in rel_rows:
        by_key.setdefault(conflict_key(row), []).append(row)
    poisoned: set[str] = set()
    for rows in by_key.values():
        gids = {r["relationship_group_id"] for r in rows if r.get("relationship_group_id")}
        if len(gids) > 1:
            poisoned |= gids
    if poisoned:
        logger.warning("composite_group_pair_collision_abstain", groups=sorted(poisoned))
        rel_rows = [r for r in rel_rows if r.get("relationship_group_id") not in poisoned]

    # Pass 2: collapse the remaining conflict-key collisions (plain vs group / plain
    # vs plain), preferring the composite-component row.
    deduped: dict[tuple[Any, Any, Any, Any], dict[str, Any]] = {}
    for row in rel_rows:
        key = conflict_key(row)
        existing = deduped.get(key)
        if existing is None or (
            existing.get("relationship_group_id") is None
            and row.get("relationship_group_id") is not None
        ):
            deduped[key] = row
    return list(deduped.values())


def synthesize_and_store_tables(
    session: Session,
    agent: SemanticAgent,
    table_ids: list[str],
    ontology: str = "general",
    relationship_candidates: list[dict[str, Any]] | None = None,
    duckdb_conn: duckdb.DuckDBPyConnection | None = None,
    *,
    run_id: str | None = None,
) -> Result[SemanticEnrichmentResult]:
    """Run per-table synthesis and store entities + relationships (DAT-362).

    Calls :meth:`SemanticAgent.synthesize_tables` (which reads the persisted
    per-column annotations as context), then stores the resulting table entities
    and LLM-confirmed relationships. Cardinality + RI metrics are computed from
    actual data, not LLM guesses. Does NOT touch ``SemanticAnnotation`` rows —
    those are owned by the per-column phase.

    Returns:
        Result with the ``SemanticEnrichmentResult`` (entities + relationships).
    """
    # Composite-key rescue (DAT-277): probe each fan-out candidate for a composite
    # that collapses it and surface the hint to the LLM judge before it decides.
    if relationship_candidates and duckdb_conn is not None:
        _augment_candidates_with_composite_rescue(relationship_candidates, duckdb_conn)

    llm_result = agent.synthesize_tables(
        session=session,
        table_ids=table_ids,
        ontology=ontology,
        relationship_candidates=relationship_candidates,
    )
    if not llm_result.success:
        return Result.fail(llm_result.error or "Table synthesis failed")
    enrichment = llm_result.unwrap()

    table_map = load_table_mappings(session, table_ids)
    column_map = load_column_mappings(session, table_ids)

    # Idempotent + non-destructive (DAT-408): TableEntity is versioned by ``run_id``.
    # Clear only THIS run's prior entities before re-inserting — a Temporal
    # at-least-once retry (same run_id) is idempotent, and EARLIER runs survive.
    # Delete-before-insert (not upsert) because it must also drop tables no longer
    # classified as entities this run; the ``uq_table_entity_table_run`` constraint
    # then guarantees at most one row per ``(table_id, run_id)`` so run-scoped
    # readers can trust the grain.
    session.execute(delete(EntityModel).where(EntityModel.run_id == run_id))

    for entity in enrichment.entity_detections:
        table_id = table_map.get(entity.table_name)
        if not table_id:
            continue
        session.add(
            EntityModel(
                run_id=run_id,
                table_id=table_id,
                detected_entity_type=entity.entity_type,
                description=entity.description,
                confidence=entity.confidence,
                grain_columns={"columns": entity.grain_columns},
                is_fact_table=entity.is_fact_table,
                is_dimension_table=entity.is_dimension_table,
                time_columns=[tc.model_dump() for tc in entity.time_columns],
                identity_columns=[ic.model_dump() for ic in entity.identity_columns],
                detection_source="llm",
            )
        )

    candidate_metrics = _build_candidate_metrics_lookup(relationship_candidates)

    rel_rows: list[dict[str, Any]] = []
    for rel in enrichment.relationships:
        from_col_id = column_map.get((rel.from_table, rel.from_column))
        to_col_id = column_map.get((rel.to_table, rel.to_column))
        from_table_id = table_map.get(rel.from_table)
        to_table_id = table_map.get(rel.to_table)
        if not all([from_col_id, to_col_id, from_table_id, to_table_id]):
            continue

        evidence = dict(rel.evidence) if rel.evidence else {}
        candidate_key = (rel.from_table, rel.from_column, rel.to_table, rel.to_column)
        if candidate_key in candidate_metrics:
            evidence.update(candidate_metrics[candidate_key])
        elif duckdb_conn is not None:
            from_table_path = f'lake.typed."{rel.from_table}"'
            to_table_path = f'lake.typed."{rel.to_table}"'
            try:
                ri_metrics = compute_ri_metrics(
                    from_table=from_table_path,
                    from_column=rel.from_column,
                    to_table=to_table_path,
                    to_column=rel.to_column,
                    duckdb_conn=duckdb_conn,
                )
                for key, value in ri_metrics.items():
                    if value is not None:
                        evidence[key] = value
            except Exception as e:
                logger.warning(
                    "ri_metrics_computation_failed",
                    from_table=rel.from_table,
                    from_column=rel.from_column,
                    to_table=rel.to_table,
                    to_column=rel.to_column,
                    error=str(e),
                )

        cardinality = _resolve_cardinality(rel=rel, evidence=evidence, duckdb_conn=duckdb_conn)

        # Fan-trap signal. The structural evaluator computes this (evaluate_relationship_
        # candidate -> compute_introduces_duplicates), but the LLM-synthesis path lost it
        # in the DAT-362 split: this branch recomputes cardinality + RI from data yet
        # dropped the duplicate-introduction check, so synthesized relationships carried a
        # NULL introduces_duplicates and BOTH SQL agents' fan-out cautions read a dead flag
        # (a many-to-many join silently double-counts). Restore it — empirically, the same
        # LEFT-JOIN row-count check the structural path uses — whenever a candidate didn't
        # already supply it and the lake is reachable.
        if "introduces_duplicates" not in evidence and duckdb_conn is not None:
            try:
                evidence["introduces_duplicates"] = compute_introduces_duplicates(
                    f'lake.typed."{rel.from_table}"',
                    f'lake.typed."{rel.to_table}"',
                    rel.from_column,
                    rel.to_column,
                    duckdb_conn,
                )
            except Exception as e:
                logger.warning(
                    "introduces_duplicates_computation_failed",
                    from_table=rel.from_table,
                    to_table=rel.to_table,
                    error=str(e),
                )

        # Composite key (DAT-277): the LLM confirmed ADDITIONAL key columns that
        # collapse the single-column fan-out. Persist the whole key as ONE group —
        # N component rows sharing a ``relationship_group_id`` and the COMPOSITE
        # cardinality, the anchor at position 0. Resolve every component's column
        # ids; if any is unresolvable we cannot honour the composite, so fall back
        # to the single-column anchor (m2m, marked avoid) rather than persist a
        # half key.
        composite_rows = _build_composite_group_rows(
            rel=rel,
            from_table_id=from_table_id,
            from_col_id=from_col_id,
            to_table_id=to_table_id,
            to_col_id=to_col_id,
            column_map=column_map,
            evidence=evidence,
            run_id=run_id,
            duckdb_conn=duckdb_conn,
        )
        if composite_rows is not None:
            rel_rows.extend(composite_rows)
            continue

        rel_rows.append(
            {
                "run_id": run_id,
                "from_table_id": from_table_id,
                "from_column_id": from_col_id,
                "to_table_id": to_table_id,
                "to_column_id": to_col_id,
                "relationship_type": rel.relationship_type.value,
                "cardinality": cardinality,
                "confidence": rel.confidence,
                "detection_method": "llm",
                "evidence": evidence,
                "relationship_group_id": None,
                "key_position": None,
            }
        )

    rel_rows = _dedup_relationship_rows(rel_rows)

    # Run-versioned + idempotent (DAT-408): this run's llm relationships are stamped
    # with ``run_id`` and coexist with prior runs; the upsert keys on the run-grain
    # unique constraint so a Temporal at-least-once retry (same run_id) refreshes
    # rather than duplicates. Silent acceptance — keeping an llm a later run didn't
    # re-find — is handled by materializing a ``keeper`` from a teach overlay
    # (DAT-409), not by mutating across runs here.
    upsert(
        session,
        RelationshipModel,
        rel_rows,
        index_elements=[
            "run_id",
            "from_column_id",
            "to_column_id",
            "detection_method",
        ],
    )

    # Catalogue-grain per-column semantics (DAT-637): the table agent is the sole
    # author. Sealed under THIS (begin_session catalogue head) run. ``run_id`` is
    # always stamped by the workflow before the phase; guard only for the
    # type-checker / direct test callers.
    if run_id is not None:
        annotated_by = agent.provider.get_model_for_tier(
            agent.config.features.semantic_analysis.model_tier
        )
        persist_column_concepts(
            session,
            enrichment.column_concepts,
            table_ids,
            annotated_by=annotated_by,
            ontology_def=agent._ontology_loader.load(ontology),
            run_id=run_id,
        )

    return Result.ok(enrichment)
