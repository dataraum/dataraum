"""Semantic enrichment processor.

Orchestrates semantic analysis using the SemanticAgent and stores results.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import duckdb
from sqlalchemy import delete
from sqlalchemy.orm import Session

if TYPE_CHECKING:
    from dataraum.llm.config import LLMConfig
    from dataraum.llm.prompts import PromptRenderer
    from dataraum.llm.providers.base import LLMProvider

from dataraum.analysis.relationships.db_models import Relationship as RelationshipModel
from dataraum.analysis.relationships.evaluator import (
    compute_actual_cardinality,
    compute_ri_metrics,
)
from dataraum.analysis.semantic.agent import SemanticAgent
from dataraum.analysis.semantic.db_models import (
    SemanticAnnotation as AnnotationModel,
)
from dataraum.analysis.semantic.db_models import (
    TableEntity as EntityModel,
)
from dataraum.analysis.semantic.models import (
    ColumnAnnotationOutput,
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
    session_id: str,
    ontology_def: Any = None,
    run_id: str | None = None,
) -> int:
    """Persist per-column annotations as ``SemanticAnnotation`` rows (DAT-362).

    The per-column phase's authoritative output. Maps each annotated column to a
    DB row, backfilling ``temporal_behavior`` from the ontology concept (same as
    the legacy monolithic path did).

    Args:
        session: Database session.
        column_output: Per-column tool output (tables -> columns).
        table_ids: Tables the annotations belong to (for column-id resolution).
        annotated_by: Model identifier that produced the annotations.
        session_id: Per-session FK.
        ontology_def: Loaded ontology, for ``temporal_behavior`` backfill.

    Returns:
        Number of annotation rows persisted.
    """
    column_map = load_column_mappings(session, table_ids)
    concept_temporal = (
        {c.name: c.temporal_behavior for c in ontology_def.concepts} if ontology_def else {}
    )

    rows: list[dict[str, Any]] = []
    for table in column_output.tables:
        for col in table.columns:
            column_id = column_map.get((table.table_name, col.column_name))
            if not column_id:
                continue
            # PK omitted so the model's Python-side default applies.
            rows.append(
                {
                    "session_id": session_id,
                    "column_id": column_id,
                    "run_id": run_id,
                    "semantic_role": col.semantic_role,
                    "entity_type": col.entity_type,
                    "business_name": col.business_term,
                    "business_description": col.description,
                    "business_concept": col.business_concept,
                    "temporal_behavior": concept_temporal.get(col.business_concept)
                    if col.business_concept
                    else None,
                    "unit_source_column": col.unit_source_column,
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


def ground_columns(
    *,
    session: Session,
    config: LLMConfig,
    provider: LLMProvider,
    renderer: PromptRenderer,
    table_ids: list[str],
    ontology: str,
    session_id: str,
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
        session_id: Per-session FK for the persisted annotation rows.

    Returns:
        ``Result.ok(count)`` with the number of annotation rows persisted, or
        ``Result.fail`` with the same messages the phase surfaced before.
    """
    from dataraum.analysis.semantic.column_agent import ColumnAnnotationAgent
    from dataraum.analysis.semantic.ontology import OntologyLoader
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

    ontology_def = OntologyLoader().load(ontology)
    model_name = provider.get_model_for_tier(col_config.model_tier)

    count = persist_column_annotations(
        session,
        annotation_result.value,
        table_ids,
        annotated_by=model_name,
        session_id=session_id,
        ontology_def=ontology_def,
        run_id=run_id,
    )
    return Result.ok(count)


def synthesize_and_store_tables(
    session: Session,
    agent: SemanticAgent,
    table_ids: list[str],
    ontology: str = "general",
    relationship_candidates: list[dict[str, Any]] | None = None,
    duckdb_conn: duckdb.DuckDBPyConnection | None = None,
    *,
    session_id: str,
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

    # Idempotent + non-destructive (DAT-408): TableEntity is versioned by ``run_id``
    # (a session has MANY runs). Clear only THIS run's prior entities before
    # re-inserting — a Temporal at-least-once retry (same run_id) is idempotent,
    # and EARLIER runs in the session survive. Delete-before-insert (not upsert)
    # because it must also drop tables no longer classified as entities this run;
    # the ``uq_table_entity_table_run`` constraint then guarantees at most one row
    # per ``(table_id, run_id)`` so run-scoped readers can trust the grain.
    session.execute(
        delete(EntityModel).where(
            EntityModel.session_id == session_id, EntityModel.run_id == run_id
        )
    )

    for entity in enrichment.entity_detections:
        table_id = table_map.get(entity.table_name)
        if not table_id:
            continue
        session.add(
            EntityModel(
                session_id=session_id,
                run_id=run_id,
                table_id=table_id,
                detected_entity_type=entity.entity_type,
                description=entity.description,
                confidence=entity.confidence,
                grain_columns={"columns": entity.grain_columns},
                is_fact_table=entity.is_fact_table,
                is_dimension_table=entity.is_dimension_table,
                time_column=entity.time_column,
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

        rel_rows.append(
            {
                "session_id": session_id,
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
            }
        )

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
            "session_id",
            "run_id",
            "from_column_id",
            "to_column_id",
            "detection_method",
        ],
    )

    return Result.ok(enrichment)
