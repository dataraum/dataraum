"""Semantic enrichment processor.

Orchestrates semantic analysis using the SemanticAgent and stores results.
"""

from __future__ import annotations

from dataclasses import dataclass
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
from dataraum.analysis.relationships.db_models import SurrogateKeyIntent
from dataraum.analysis.relationships.evaluator import (
    compute_actual_cardinality,
    compute_composite_cardinality,
    compute_introduces_duplicates,
    compute_ri_metrics,
)
from dataraum.analysis.relationships.models import JoinCandidate, RelationshipCandidate
from dataraum.analysis.relationships.surrogate import composite_intent_digest
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
from dataraum.analysis.semantic.utils import (
    annotations_have_measure,
    load_column_mappings,
    load_persisted_annotations,
    load_table_mappings,
)
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


def _orient_fk_direction(
    from_table_id: str,
    from_col_id: str,
    to_table_id: str,
    to_col_id: str,
    cardinality: str | None,
    evidence: dict[str, Any],
) -> tuple[str, str, str, str, str | None]:
    """Orient a confirmed FK to the many→one convention (DAT-758).

    The LLM judge emits the ``from``/``to`` direction and intermittently reverses
    it (the header/line pair — ``journal_lines`` vs ``journal_entries`` — was seen
    stored parent→child on the eval oracle). The persisted direction is NOT
    cosmetic: ``og_references`` binds it verbatim as the graph edge, the cockpit
    ``<relationships>`` block reads the directional fan-out caution off it, and the
    referenced-dimension identity resolves ``from_column → to_table`` assuming
    ``from`` is the fact FK (DAT-756). So orient it deterministically here.

    The MEASURED cardinality (in the judge's ``from→to`` order — the candidate
    lookup flips it per direction, and the fallback measures it that way) is the
    reliable signal: when it reads ``one-to-many``, ``from`` is the ONE (parent/dim)
    side, so swap the endpoints — and the directional evidence — to store
    ``many-to-one`` child→parent. ``many-to-one`` is already correct; ``one-to-one``
    is orientation-agnostic; ``many-to-many``/``None`` cannot be oriented. Mutates
    ``evidence`` in place for the directional fields.
    """
    if cardinality != "one-to-many":
        return from_table_id, from_col_id, to_table_id, to_col_id, cardinality
    # RI is directional: left = fraction of FROM's values found in TO. After the
    # swap the old TO becomes FROM, so left/right exchange.
    left_ri = evidence.pop("left_referential_integrity", None)
    right_ri = evidence.pop("right_referential_integrity", None)
    if right_ri is not None:
        evidence["left_referential_integrity"] = right_ri
    if left_ri is not None:
        evidence["right_referential_integrity"] = left_ri
    # A many-to-one child→parent join matches each child row to exactly one parent
    # — it never fans out (the one-to-many parent→child join did).
    evidence["introduces_duplicates"] = False
    return to_table_id, to_col_id, from_table_id, from_col_id, "many-to-one"


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


@dataclass(frozen=True)
class ConceptPersistCounts:
    """Emitted vs resolved vs dropped for a ``column_concepts`` persist (DAT-768).

    ``emitted`` = concepts the table agent produced; ``resolved`` = rows actually
    written (name matched a column, after the same-column dedup); ``dropped_unresolved``
    = concepts whose ``(table, column)`` name matched no column in the batch. A
    silently-empty load-bearing surface is now a visible count, not indistinguishable
    from "no concepts to bind".
    """

    emitted: int
    resolved: int
    dropped_unresolved: int


def persist_column_concepts(
    session: Session,
    column_concepts: list[ColumnConceptOutput],
    table_ids: list[str],
    *,
    annotated_by: str,
    run_id: str,
) -> ConceptPersistCounts:
    """Persist the table agent's catalogue-grain per-column semantics (DAT-637).

    Writes ``ColumnConcept`` rows under the begin_session (catalogue head) run.
    ``temporal_behavior`` is NOT seeded here (DAT-657): stock/flow is a data-format
    property the ontology cannot declare, so it is left NULL at authoring and
    written only by the data-grounded resolve pass (``entropy.resolve``). Run-scoped
    upsert on ``(column_id, run_id)``; a column the table agent did not bind this
    run has no row (absent = no concept), and run-scoped reads never see a prior
    run's.

    Returns:
        A :class:`ConceptPersistCounts` breakdown. The counts are logged so a
        name-resolution wipeout (every emitted concept dropped as unresolved,
        DAT-768 path #2) is diagnosable rather than indistinguishable from an
        empty emission; the caller gates begin_session on ``resolved``.
    """
    column_map = load_column_mappings(session, table_ids)

    rows: list[dict[str, Any]] = []
    dropped: list[tuple[str, str]] = []
    for cc in column_concepts:
        column_id = column_map.get((cc.table_name, cc.column_name))
        if not column_id:
            dropped.append((cc.table_name, cc.column_name))
            continue
        rows.append(
            {
                "column_id": column_id,
                "run_id": run_id,
                "business_concept": cc.business_concept,
                "unit_source_column": cc.unit_source_column,
                "derived_formula_hypothesis": (cc.derived_formula_hypothesis or "").strip() or None,
                "derived_formula_confidence": cc.derived_formula_confidence,
                "annotation_source": DecisionSource.LLM.value,
                "annotated_by": annotated_by,
            }
        )

    # Dedup on the upsert key (column_id, run_id): the table agent can emit the same
    # column twice in column_concepts, and Postgres ON CONFLICT cannot touch a row
    # twice in one batch (CardinalityViolation). Last mention wins.
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        deduped[(row["column_id"], row["run_id"])] = row
    rows = list(deduped.values())

    upsert(session, ConceptModel, rows, index_elements=["column_id", "run_id"])

    counts = ConceptPersistCounts(
        emitted=len(column_concepts),
        resolved=len(rows),
        dropped_unresolved=len(dropped),
    )
    logger.info(
        "column_concepts_persisted",
        emitted=counts.emitted,
        resolved=counts.resolved,
        dropped_unresolved=counts.dropped_unresolved,
    )
    if dropped:
        # The exact names the agent echoed that resolved to no column — the signal
        # that distinguishes a naming drift (case, enriched prefix, display name)
        # from a genuinely empty emission.
        logger.debug("column_concepts_dropped_unresolved", dropped=dropped)
    return counts


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
                "coverage": key.coverage,
                "coverage_table": key.coverage_table,
            }


def _first_wins(rows: list[dict[str, Any]], key_fields: tuple[str, ...]) -> list[dict[str, Any]]:
    """Fold same-batch duplicate rows on the upsert key, keeping the first."""
    folded: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        folded.setdefault(tuple(row[f] for f in key_fields), row)
    return list(folded.values())


def _declined_intent_rows(
    candidates: list[dict[str, Any]],
    *,
    confirmed_digests: set[str],
    table_map: dict[str, str],
    column_map: dict[tuple[str, str], str],
    run_id: str,
) -> list[dict[str, Any]]:
    """Verdict rows for OFFERED composites the judge did not confirm (DAT-697).

    Every COMPOSITE-KEY RESCUE hint is adjudicated: the judge either confirms
    it (a ``status='confirmed'`` intent exists) or it was declined. A
    confirmation the system could not build into an intent (unresolvable
    component, non-collapsing measurement) also lands here: either way this
    run produced no usable composite, the hint re-offers deterministically
    next run, and the declined record is what stops the keeper machinery from
    silently resurrecting the pair against the judge's evidence-based verdict
    (``materialize.py``). An unresolvable HINT writes no record at all —
    without an identity there is no verdict, and keeper protection stands.
    """
    rows: list[dict[str, Any]] = []
    for cand in candidates:
        hint = cand.get("composite_key")
        table1, table2 = cand.get("table1"), cand.get("table2")
        if not hint or not table1 or not table2:
            continue
        from_table_id, to_table_id = table_map.get(table1), table_map.get(table2)
        id_pairs: list[tuple[str, str]] = []
        name_pairs: list[tuple[str, str]] = []
        for name1, name2 in hint.get("column_pairs", []):
            id1, id2 = column_map.get((table1, name1)), column_map.get((table2, name2))
            if not id1 or not id2:
                id_pairs = []
                break
            id_pairs.append((id1, id2))
            name_pairs.append((name1, name2))
        if len(id_pairs) < 2 or not from_table_id or not to_table_id:
            logger.warning(
                "composite_decline_unrecordable",
                table1=table1,
                table2=table2,
                pairs=hint.get("column_pairs"),
            )
            continue
        digest = composite_intent_digest(id_pairs)
        if digest in confirmed_digests:
            continue
        # Same canonical (direction-neutral) pair order as the confirmed path.
        ordered = sorted(zip(id_pairs, name_pairs, strict=True), key=lambda t: tuple(sorted(t[1])))
        coverage = hint.get("coverage")
        usage = f" (measured usage {coverage:.1%})" if coverage is not None else ""
        rows.append(
            {
                "run_id": run_id,
                "intent_digest": digest,
                "status": "declined",
                "from_table_id": from_table_id,
                "to_table_id": to_table_id,
                "column_pairs": [list(pair) for pair, _n in ordered],
                "cardinality": hint.get("cardinality"),
                "confidence": 0.0,
                # Neutral wording: this path also catches a confirmation the
                # system could not build into an intent (component/measurement
                # rejection), not only a judge omission.
                "reasoning": f"offered rescue hint produced no confirmed composite{usage}",
            }
        )
    return rows


def _build_surrogate_intent(
    *,
    rel: SemanticRelationship,
    from_table_id: str,
    from_col_id: str,
    to_table_id: str,
    to_col_id: str,
    column_map: dict[tuple[str, str], str],
    run_id: str | None,
    duckdb_conn: duckdb.DuckDBPyConnection | None,
) -> dict[str, Any] | None:
    """One ``surrogate_key_intents`` row for an LLM-confirmed composite, or None.

    ``None`` sends the caller down the ordinary single-column persist: when a
    component column is unresolvable (LLM named a column that doesn't exist),
    when de-duplication leaves only the anchor (the LLM echoed the anchor pair
    in ``key_columns``), or when there is no ``run_id`` to version the intent
    under. The anchor is still a real confirmed relationship in every fallback —
    its empirical cardinality and fan-trap flag then say what joining it alone
    does. Worst case is a missed mint, never a broken catalog.
    """
    if not rel.key_columns or run_id is None:
        return None

    # Ordered component id pairs, anchor first; skip an echoed anchor / dup pairs.
    components: list[tuple[str, str]] = [(from_col_id, to_col_id)]
    name_pairs: list[tuple[str, str]] = [(rel.from_column, rel.to_column)]
    seen: set[tuple[str, str]] = {components[0]}
    for from_name, to_name in rel.key_columns:
        comp_from = column_map.get((rel.from_table, from_name))
        comp_to = column_map.get((rel.to_table, to_name))
        if not comp_from or not comp_to:
            logger.warning(
                "surrogate_intent_component_unresolved",
                from_table=rel.from_table,
                to_table=rel.to_table,
                component=(from_name, to_name),
            )
            return None
        if (comp_from, comp_to) in seen:
            continue
        seen.add((comp_from, comp_to))
        components.append((comp_from, comp_to))
        name_pairs.append((from_name, to_name))

    if len(components) < 2:
        return None  # anchor-only after dedup — effectively single-column

    # Canonical component order: ALL pairs sorted by a DIRECTION-NEUTRAL name
    # key — including the anchor. Neither the LLM's key_columns ordering, NOR
    # its anchor choice (seen live 2026-07-06: the same composite arrived
    # anchored on payment_method one run and business_id the next), NOR its
    # from/to emission direction is stable across runs, and the surrogate
    # column NAME and the hash-input order both derive from this list. A
    # from-side-only sort key would reorder under a direction flip whenever
    # the two sides' names sort differently (account vs account_name). The
    # anchor's semantics live in the relationship DIRECTION, never in the
    # column identity.
    ordered = sorted(zip(components, name_pairs, strict=True), key=lambda t: tuple(sorted(t[1])))
    components = [c for c, _n in ordered]
    name_pairs = [n for _c, n in ordered]

    # The composite's measured cardinality (the collapse proof). Best-effort:
    # the mint recomputes on the minted surrogate column anyway.
    cardinality: str | None = None
    if duckdb_conn is not None:
        cardinality = compute_composite_cardinality(
            _lake_path(rel.from_table), _lake_path(rel.to_table), name_pairs, duckdb_conn
        )
        if cardinality == "many-to-many":
            # The LLM confirmed a composite the data measurably REJECTS (the
            # prompt's contract: only confirm when it resolves the fan-out).
            # Seen live on a multi-tenant bookkeeping smoke: the chart of accounts carries duplicate
            # (account, business) rows, so no name-based composite collapses.
            # Fall back to the plain single-column anchor — it persists with
            # its honest cardinality + fan-trap flag, exactly the pre-mint
            # behavior. Never mint a surrogate that is not a proven key.
            logger.warning(
                "surrogate_intent_not_collapsing",
                from_table=rel.from_table,
                to_table=rel.to_table,
                name_pairs=name_pairs,
            )
            return None

    return {
        "run_id": run_id,
        "intent_digest": composite_intent_digest(components),
        "status": "confirmed",
        "from_table_id": from_table_id,
        "to_table_id": to_table_id,
        "column_pairs": [list(pair) for pair in components],
        "cardinality": cardinality,
        "confidence": rel.confidence,
        "reasoning": (rel.evidence or {}).get("reasoning"),
    }


# Confirm/decline threshold for the semantic judge's relationship verdict. The
# judge encodes its verdict in ``confidence`` (there is no explicit accept/decline
# field): on the finance corpus it lands bimodally — declines ≤ 0.40 ("coincidental
# overlap; not a real FK") and accepts ≥ 0.85, with a wide empty dead zone between.
# 0.7 sits squarely in that dead zone, so it is the judge's own decision boundary,
# not an imposed floor. A relationship the judge did NOT confirm is persisted as a
# ``candidate`` (its evidence/reasoning kept), NOT as ``llm`` — so it never enters
# the "defined" catalog (``detection_method != 'candidate'``) that every downstream
# consumer reads. This cuts judge-declined relationships at the source instead of
# making each consumer re-weigh confidence (DAT-699 dropped the read-path gate;
# "defined" must mean judge-confirmed again). Mirrors the relationships phase's
# high-confidence band (``>= 0.7``).
REL_CONFIRM_MIN = 0.7


def _batch_has_measures(session: Session, table_ids: list[str]) -> bool:
    """Whether any column in the batch carries the ``measure`` semantic role.

    The upstream per-column phase has already annotated roles. A batch that holds
    a measure is one that MUST bind at least one ontology concept, so zero resolved
    ``column_concepts`` for it is an emptied surface, not a plausible judgment
    (DAT-768). Reads the same persisted annotations the table agent saw.
    """
    return annotations_have_measure(load_persisted_annotations(session, table_ids))


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
                table_role=entity.table_role,
                time_columns=[tc.model_dump() for tc in entity.time_columns],
                identity_columns=[ic.model_dump() for ic in entity.identity_columns],
                detection_source="llm",
            )
        )

    candidate_metrics = _build_candidate_metrics_lookup(relationship_candidates)

    rel_rows: list[dict[str, Any]] = []
    intent_rows: list[dict[str, Any]] = []
    for rel in enrichment.relationships:
        from_col_id = column_map.get((rel.from_table, rel.from_column))
        to_col_id = column_map.get((rel.to_table, rel.to_column))
        from_table_id = table_map.get(rel.from_table)
        to_table_id = table_map.get(rel.to_table)
        if not all([from_col_id, to_col_id, from_table_id, to_table_id]):
            continue
        assert from_col_id and to_col_id and from_table_id and to_table_id  # narrow for mypy

        # LLM-confirmed composite (DAT-277): persist as a surrogate-key INTENT for
        # the mint phase, never as a plain llm row — the single-column anchor is a
        # half-key and would fan out at every consumer. A composite the judge did
        # NOT confirm (confidence below REL_CONFIRM_MIN) is a decline like any
        # other — it falls through to the gated single-column persist (→
        # ``candidate``), so no write path routes a declined verdict into the
        # "defined" catalog (DAT-722). An unbuildable/non-collapsing intent also
        # falls through.
        if rel.key_columns and rel.confidence >= REL_CONFIRM_MIN:
            intent = _build_surrogate_intent(
                rel=rel,
                from_table_id=from_table_id,
                from_col_id=from_col_id,
                to_table_id=to_table_id,
                to_col_id=to_col_id,
                column_map=column_map,
                run_id=run_id,
                duckdb_conn=duckdb_conn,
            )
            if intent is not None:
                intent_rows.append(intent)
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

        # DAT-758: orient to the FK convention (many→one, child→parent) from the
        # measured cardinality before persisting. The judge intermittently reverses
        # the direction, and every consumer that reads it assumes from = the many/
        # fact side: og_references binds it verbatim, the conformed-dim slice identity
        # resolves from_column → to_table, and the enrichment prompt's grain-safe
        # marker (many-to-one/one-to-one) decides whether the join is offered at all —
        # a reversed one-to-many FK is shown as NOT grain-safe and the dim join is lost.
        from_table_id, from_col_id, to_table_id, to_col_id, cardinality = _orient_fk_direction(
            from_table_id, from_col_id, to_table_id, to_col_id, cardinality, evidence
        )

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
                # Confirmed only at or above the judge's own decision boundary; a
                # declined verdict stays a ``candidate`` (evidence kept) so it never
                # reaches the "defined" catalog. See REL_CONFIRM_MIN (DAT-722).
                "detection_method": "llm" if rel.confidence >= REL_CONFIRM_MIN else "candidate",
                "evidence": evidence,
            }
        )

    # Run-versioned + idempotent (DAT-408): this run's llm relationships are stamped
    # with ``run_id`` and coexist with prior runs; the upsert keys on the run-grain
    # unique constraint so a Temporal at-least-once retry (same run_id) refreshes
    # rather than duplicates. Silent acceptance — keeping an llm a later run didn't
    # re-find — is handled by materializing a ``keeper`` from a teach overlay
    # (DAT-409), not by mutating across runs here.
    # Fold same-batch duplicates first (keep the first): the LLM occasionally emits
    # one pair twice, and Postgres rejects an INSERT..ON CONFLICT batch that
    # affects the same row twice ("cannot affect row a second time").
    rel_rows = _first_wins(
        rel_rows, ("run_id", "from_column_id", "to_column_id", "detection_method")
    )
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

    # Surrogate-key intents (DAT-277 / DAT-697): the run's composite VERDICTS —
    # confirmed composites for the mint phase, plus declined records for every
    # offered-but-unconfirmed rescue hint (the keeper machinery must never
    # silently resurrect an adjudicated pair). Confirmed rows come first so
    # they win the fold when the same digest appears in both sets.
    if run_id is not None:
        intent_rows.extend(
            _declined_intent_rows(
                relationship_candidates or [],
                confirmed_digests={r["intent_digest"] for r in intent_rows},
                table_map=table_map,
                column_map=column_map,
                run_id=run_id,
            )
        )
    intent_rows = _first_wins(intent_rows, ("run_id", "intent_digest"))
    upsert(
        session,
        SurrogateKeyIntent,
        intent_rows,
        index_elements=["run_id", "intent_digest"],
    )

    # Catalogue-grain per-column semantics (DAT-637): the table agent is the sole
    # author. Sealed under THIS (begin_session catalogue head) run. ``run_id`` is
    # always stamped by the workflow before the phase; guard only for the
    # type-checker / direct test callers.
    if run_id is not None:
        annotated_by = agent.provider.get_model_for_tier(
            agent.config.features.semantic_analysis.model_tier
        )
        counts = persist_column_concepts(
            session,
            enrichment.column_concepts,
            table_ids,
            annotated_by=annotated_by,
            run_id=run_id,
        )
        # DAT-768: the column_concepts surface is load-bearing — every metric's
        # measure→concept binding grounds on it. With measure-role columns in the
        # batch, ZERO resolved concepts is not a judgment; it is an emptied surface
        # (the agent under-produced the whole field, or every name it echoed failed
        # to resolve) that would silently collapse all downstream grounding. Fail
        # begin_session loud rather than ship it green — the agent already
        # re-prompted once on an empty emission (see synthesize_tables), so a
        # still-empty surface here is a real defect, not a flake. This gates on
        # emptiness of the surface, never on any specific concept judgment.
        if counts.resolved == 0 and _batch_has_measures(session, table_ids):
            return Result.fail(
                "column_concepts empty despite measure-role columns in the batch "
                f"(emitted={counts.emitted}, resolved=0, "
                f"dropped_unresolved={counts.dropped_unresolved}) — metric grounding "
                "would collapse (DAT-768)."
            )

    return Result.ok(enrichment)
