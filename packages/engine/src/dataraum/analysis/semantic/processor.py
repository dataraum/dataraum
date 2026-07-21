"""Semantic enrichment processor.

Orchestrates semantic analysis using the SemanticAgent and stores results.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import duckdb
from sqlalchemy import and_, delete, or_, select
from sqlalchemy.orm import Session

if TYPE_CHECKING:
    from dataraum.llm.config import LLMConfig
    from dataraum.llm.prompts import PromptRenderer
    from dataraum.llm.providers.base import LLMProvider

from dataraum.analysis.relationships.composite import rescue_fanout_to_composite
from dataraum.analysis.relationships.db_models import Relationship as RelationshipModel
from dataraum.analysis.relationships.db_models import (
    SurrogateKeyIntent,
    swap_directional_evidence,
)
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
from dataraum.analysis.semantic.utils import (
    load_column_mappings,
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

            # Every per-side measurement the served candidate carries, both
            # sides of each pair. Carried for EVIDENCE COMPLETENESS, not for a
            # decision: no consumer orients on any of it — the judge is told the
            # rule in ``semantic_per_table`` and decides. Dropping them meant a
            # stored relationship did not carry the numbers its own direction
            # was argued from, which is why diagnosing a wrong direction took a
            # forensic dig through candidate rows (DAT-725 runs #2/#5).
            metrics: dict[str, Any] = {
                key: jc[key]
                for key in (
                    "left_referential_integrity",
                    "right_referential_integrity",
                    "left_key_coverage",
                    "right_key_coverage",
                    "left_orphan_count",
                    "right_orphan_count",
                    "left_uniqueness",
                    "right_uniqueness",
                    "cardinality_verified",
                    "cardinality",
                )
                if key in jc
            }

            # Add relationship-level metrics
            if "introduces_duplicates" in candidate:
                metrics["introduces_duplicates"] = candidate["introduces_duplicates"]

            if metrics:
                lookup[(table1, col1, table2, col2)] = metrics
                # The judge may emit the pair either way round, so the reverse
                # key is served too — re-expressed for that direction through
                # the ONE flip helper the persist path uses (DAT-725). This site
                # used to hand-roll the swap over a per-metric list, which meant
                # every metric not on the list kept describing the side it was
                # no longer on: ``orphan_count`` was measured on the from side
                # and carried through verbatim, so a judge that emitted the
                # opposite direction stored ``L=100% RI`` beside ``orphans=2``
                # — a reading that cannot be true, shipped to the orphan-rate
                # detector as fact. Two flip implementations is one too many:
                # prefix a directional metric and both sites get it right.
                lookup[(table2, col2, table1, col1)] = swap_directional_evidence(metrics)

    return lookup


def _candidate_rows_for_pair(
    session: Session, from_col_id: str, to_col_id: str, run_id: str | None
) -> list[RelationshipModel]:
    """This run's structural ``candidate`` rows for a pair, matched UNDIRECTED.

    The judge may name the pair either way while the stored candidate is oriented
    many→one (DAT-777), so both orientations are matched. One row per undirected
    pair per run in practice (single candidate writer, oriented dedup); returns a
    list so the caller reconciles every match deterministically.
    """
    stmt = select(RelationshipModel).where(
        RelationshipModel.detection_method == "candidate",
        or_(
            and_(
                RelationshipModel.from_column_id == from_col_id,
                RelationshipModel.to_column_id == to_col_id,
            ),
            and_(
                RelationshipModel.from_column_id == to_col_id,
                RelationshipModel.to_column_id == from_col_id,
            ),
        ),
    )
    if run_id is not None:
        stmt = stmt.where(RelationshipModel.run_id == run_id)
    return list(session.execute(stmt).scalars())


def _apply_judge_verdicts(
    session: Session,
    *,
    declined: list[tuple[str, str, str | None]],
    confirmed: list[tuple[str, str]],
    run_id: str | None,
) -> None:
    """Reconcile each adjudicated pair's ``candidate`` row with the judge's verdict.

    A judge verdict is recorded WITHOUT clobbering the measured value-overlap
    evidence (DAT-824): the pair's existing ``candidate`` row (written by the
    structural detector in the relationships phase) keeps its
    ``confidence``/``evidence``.

    - A DECLINE sets ``judge_verdict='declined'`` and merges the judge's reasoning
      into ``evidence['reasoning']`` (measured keys untouched).
    - A CONFIRM CLEARS any prior ``judge_verdict`` — the confirmation lives in the
      sibling ``llm`` row, so a candidate carrying a stale ``'declined'`` would
      contradict the model. Applied AFTER the declines so a pair the LLM emitted
      both ways lands NULL (its ``llm`` row is the truth).

    Clearing on confirm makes a Temporal at-least-once retry that FLIPS the verdict
    across attempts (decline on attempt 1, confirm on attempt 2 — the LLM is
    re-called, not cached) converge to the last attempt rather than stranding a
    contradictory ``'declined'`` beside the new ``llm`` row.

    A pair with no candidate row (never structurally proposed — a hallucinated
    pair or a column-map miss) has no measurement to reconcile: declines are
    dropped+logged, never fabricated into a confidence-less row.
    """
    for from_col_id, to_col_id, reasoning in declined:
        rows = _candidate_rows_for_pair(session, from_col_id, to_col_id, run_id)
        if not rows:
            logger.debug(
                "declined_pair_no_candidate_row",
                from_column_id=from_col_id,
                to_column_id=to_col_id,
            )
            continue
        for row in rows:
            row.judge_verdict = "declined"
            if reasoning:
                # Reassign so SQLAlchemy tracks the JSON mutation; PRESERVE the
                # measured keys, only ADD the judge's reasoning (never a clobber).
                row.evidence = {**(row.evidence or {}), "reasoning": reasoning}

    for from_col_id, to_col_id in confirmed:
        for row in _candidate_rows_for_pair(session, from_col_id, to_col_id, run_id):
            row.judge_verdict = None


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
    semantics (meaning, ontology temporal_behavior, unit source, derived
    formula) are NOT written here: the table agent authors them onto
    ``ColumnConcept`` under the catalogue head (DAT-637).

    Args:
        session: Database session.
        column_output: Per-column structured output (tables -> columns).
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
            # only (DAT-637): meaning, ontology temporal_behavior,
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
    """Fold same-batch duplicate rows on the upsert key, keeping the first.

    Logs what it dropped. The judge is the sole authority on a 1:1's direction
    (DAT-725), so if it emits the same pair twice with different verdicts, that
    disagreement is the only signal that its orientation reasoning is unstable —
    and this fold used to swallow it silently. "The judge was consistent" and
    "the collision resolver hid a disagreement" looked identical from the
    outside.
    """
    folded: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        key = tuple(row[f] for f in key_fields)
        kept = folded.setdefault(key, row)
        if kept is not row:
            logger.warning(
                "duplicate_row_dropped_in_batch",
                key=key,
                kept_confidence=kept.get("confidence"),
                dropped_confidence=row.get("confidence"),
                kept_cardinality=kept.get("cardinality"),
                dropped_cardinality=row.get("cardinality"),
            )
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
# field): the design intent is bimodal — declines ≤ 0.40 ("coincidental overlap;
# not a real FK") and accepts ≥ 0.85, with an empty dead zone between, making 0.7
# the judge's own decision boundary rather than an imposed floor. Observed drift
# (DAT-725 runs #1/#2): sparse/dirty FKs landed IN the dead zone — 0.55 while the
# same verdict's reasoning affirmed "genuine sparse FK", 0.85→0.6 under an
# RI-corruption injection — i.e. the judge dampened the number for data QUALITY,
# not existence. The synthesis prompt (semantic_per_table v2.1.0) answers this by
# defining confidence as existence-only and instructing decisive scoring; the
# threshold itself stands, and there is deliberately NO deterministic override of
# the judge's verdict (agentic-not-deterministic). A relationship the judge did
# NOT confirm is persisted as a ``candidate`` (its evidence/reasoning kept), NOT
# as ``llm`` — so it never enters the "defined" catalog
# (``detection_method != 'candidate'``) that every downstream consumer reads.
# This cuts judge-declined relationships at the source instead of making each
# consumer re-weigh confidence (DAT-699 dropped the read-path gate; "defined"
# must mean judge-confirmed again). Mirrors the relationships phase's
# high-confidence band (``>= 0.7``).
REL_CONFIRM_MIN = 0.7


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
                # Unclassified-stub pattern (DAT-823): the business reading —
                # detected_entity_type + description — is authored by the
                # catalogue_semantics phase, which UPDATEs this same
                # (table_id, run_id) row later in the run. The NULL window is
                # within-run only; nothing between the two phases reads either
                # column (verified at the rebalance).
                detected_entity_type=None,
                description=None,
                grain_columns=entity.grain_columns,
                table_role=entity.table_role,
                time_columns=[tc.model_dump() for tc in entity.time_columns],
                identity_columns=[ic.model_dump() for ic in entity.identity_columns],
                detection_source="llm",
            )
        )

    candidate_metrics = _build_candidate_metrics_lookup(relationship_candidates)

    rel_rows: list[dict[str, Any]] = []
    intent_rows: list[dict[str, Any]] = []
    declined_pairs: list[tuple[str, str, str | None]] = []
    confirmed_pairs: list[tuple[str, str]] = []
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
        # other — it falls through to the single-column DECLINE path below, so no
        # write path routes a declined verdict into the "defined" catalog (DAT-722).
        # An unbuildable/non-collapsing intent also falls through.
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

        # A judge DECLINE is a SEPARATE FACT from the structural measurement
        # (DAT-824). The old path rebuilt a ``candidate`` row here and upserted it
        # on the SAME key the structural detector used, so the LLM's low confidence
        # and its sparse ``{source, reasoning}`` evidence CLOBBERED the detector's
        # measured join_confidence/algorithm/statistical_confidence — the run's
        # overlap evidence destroyed. Instead, record the verdict by ANNOTATING the
        # pair's existing candidate row (``_record_declined_verdicts``): its measured
        # evidence survives intact, the decline is typed+queryable via
        # ``judge_verdict='declined'``, and it stays ``detection_method='candidate'``
        # so it is excluded from every reference-serving consumer for free.
        if rel.confidence < REL_CONFIRM_MIN:
            declined_pairs.append((from_col_id, to_col_id, (rel.evidence or {}).get("reasoning")))
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

        # Reached only for a CONFIRMED verdict (declines exited above) — persist as
        # a vouched-for ``llm`` row. See REL_CONFIRM_MIN (DAT-722).
        # DAT-777: build through the model's single orientation chokepoint. It
        # orients to the FK convention (many→one, child→parent) from the measured
        # cardinality — every consumer assumes from = the many/fact side
        # (og_references binds it verbatim, the conformed-dim slice identity resolves
        # from_column → to_table, and the enrichment prompt's grain-safe marker
        # decides whether the join is offered at all; a reversed one-to-many FK is
        # shown NOT grain-safe and the dim join is lost). The same chokepoint
        # resolves the edge KIND (DAT-850): the LLM's foreign_key/hierarchy claim
        # refuted by a measured many-to-many persists as 'conformed_dimension' —
        # two facts meeting at a shared axis, never served downstream as a
        # genuine reference.
        rel_rows.append(
            RelationshipModel.oriented_row(
                run_id=run_id,
                from_table_id=from_table_id,
                from_column_id=from_col_id,
                to_table_id=to_table_id,
                to_column_id=to_col_id,
                relationship_type=rel.relationship_type.value,
                cardinality=cardinality,
                confidence=rel.confidence,
                detection_method="llm",
                confirmation_source="judge",
                evidence=evidence,
            )
        )
        # Clear any stale decline verdict on the pair's candidate row (DAT-824):
        # this confirmation's ``llm`` row is the truth (undirected match below).
        confirmed_pairs.append((from_col_id, to_col_id))

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

    # Reconcile the run's judge verdicts onto the pairs' structural candidate rows,
    # preserving their measured evidence (DAT-824): a decline annotates
    # ``judge_verdict='declined'`` + reasoning, a confirm clears any prior verdict
    # (its ``llm`` row is the truth). Done AFTER the llm upsert so the llm rows the
    # confirm-clear reconciles against exist.
    _apply_judge_verdicts(
        session, declined=declined_pairs, confirmed=confirmed_pairs, run_id=run_id
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

    # Column concepts are NOT persisted here (DAT-823): the catalogue_semantics
    # phase — later in the same begin_session run, after enriched_views and
    # slicing — is the sole ColumnConcept INSERT writer, authoring meanings over
    # the composed catalogue this phase's verdicts feed.
    return Result.ok(enrichment)
