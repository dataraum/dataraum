"""Loader helpers for detector data loading.

Each helper extracts data from the DB for a specific analysis domain,
returning dict structures matching what detectors expect in
context.analysis_results. Extracted 1:1 from snapshot.load_column_analysis()
and snapshot.load_table_analysis().
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sqlalchemy.orm import Session

# The add_source stages whose promoted heads a detect run pins as its base
# snapshot (DAT-448): session detects read per-column rows these stages wrote.
_BASE_RUN_STAGES = ("semantic_per_column", "statistics", "statistical_quality")


def resolve_base_runs(session: Session, table_ids: Sequence[str]) -> dict[tuple[str, str], str]:
    """Pin the promoted base-run map for a detect run (DAT-448).

    Resolves the ``(table:{id}, stage)`` ``MetadataSnapshotHead`` ONCE per
    table × base stage at detect start. The detect orchestrator threads the
    map onto every ``DetectorContext``, so all loader reads in the run resolve
    to the same base runs regardless of concurrent promotes — per-call head
    resolution allowed a mid-run promote to tear reads (column 1 from run A,
    column 47 from run B). Unpromoted ``(table, stage)`` pairs are simply
    absent: readers fail closed, never guess a run.
    """
    from dataraum.storage.snapshot_head import head_run_id

    pinned: dict[tuple[str, str], str] = {}
    for table_id in table_ids:
        for stage in _BASE_RUN_STAGES:
            rid = head_run_id(session, f"table:{table_id}", stage)
            if rid is not None:
                pinned[(table_id, stage)] = rid
    return pinned


def _pinned_base_run(
    session: Session,
    column_id: str,
    stage: str,
    base_runs: Mapping[tuple[str, str], str] | None,
) -> str | None:
    """The pinned base ``run_id`` for the column's table at ``stage`` (DAT-448).

    Session-detect reads carry the begin_session run's ``run_id``, but the
    per-column analysis rows (semantic, statistics, …) were written — and
    promoted — by the add_source run that produced the typed table (DAT-405).
    The fallback consults the run-start pin instead of re-resolving the moving
    head per call. No pin → ``None`` — the caller keeps its no-data behaviour
    (fail closed, never guess).
    """
    if not base_runs:
        return None
    from dataraum.storage import Column

    col = session.get(Column, column_id)
    if col is None:
        return None
    return base_runs.get((col.table_id, stage))


def load_typing(
    session: Session, column_id: str, run_id: str | None = None
) -> dict[str, Any] | None:
    """Load type decision and candidate info for a column.

    Returns dict with resolved_type, confidence, parse_success_rate, etc.
    or None if no typing data exists.

    ``run_id`` (DAT-413): when set, restrict to THIS run's typing output — the
    detect path always passes the run's run_id so a detector reads its own run's
    upstream metadata. ``None`` (non-detect / test callers) reads the MOST RECENT
    typing for the column — which is the promoted re-run after a teach cycle — so a
    read across coexisting runs returns the current one rather than raising
    ``MultipleResultsFound`` (DAT-447).
    """
    from dataraum.analysis.typing.db_models import TypeCandidate, TypeDecision

    td_stmt = select(TypeDecision).where(TypeDecision.column_id == column_id)
    tc_stmt = select(TypeCandidate).where(TypeCandidate.column_id == column_id)
    if run_id is not None:
        td_stmt = td_stmt.where(TypeDecision.run_id == run_id)
        tc_stmt = tc_stmt.where(TypeCandidate.run_id == run_id)
    # Most-recent decision, highest-confidence candidate — ``.first()`` never raises
    # on coexisting runs (a teach re-run leaves the prior run's typing rows in place).
    td = session.execute(td_stmt.order_by(TypeDecision.decided_at.desc())).scalars().first()
    tc = session.execute(tc_stmt.order_by(TypeCandidate.confidence.desc())).scalars().first()

    if td:
        typing_dict: dict[str, Any] = {
            "resolved_type": td.decided_type,
            "data_type": td.decided_type,
            "detected_type": td.decided_type,
            "decision_source": td.decision_source,
            "decision_reason": td.decision_reason,
        }
        if tc:
            typing_dict["confidence"] = tc.confidence
            typing_dict["parse_success_rate"] = tc.parse_success_rate or 1.0
            typing_dict["failed_examples"] = tc.failed_examples or []
            typing_dict["detected_pattern"] = tc.detected_pattern
            typing_dict["pattern_match_rate"] = tc.pattern_match_rate
            typing_dict["detected_unit"] = tc.detected_unit
            typing_dict["unit_confidence"] = tc.unit_confidence
            typing_dict["quarantine_rate"] = tc.quarantine_rate
        return typing_dict
    elif tc:
        return {
            "data_type": tc.data_type,
            "detected_type": tc.data_type,
            "confidence": tc.confidence,
            "parse_success_rate": tc.parse_success_rate or 1.0,
            "failed_examples": tc.failed_examples or [],
            "detected_pattern": tc.detected_pattern,
            "pattern_match_rate": tc.pattern_match_rate,
            "detected_unit": tc.detected_unit,
            "unit_confidence": tc.unit_confidence,
            "quarantine_rate": tc.quarantine_rate,
        }
    return None


_TEXT_RESOLVED_TYPES = frozenset({"VARCHAR", "TEXT", "STRING", "CHAR"})


def rejected_token_counts(
    duckdb_conn: Any, raw_table_fqn: str, column_name: str, resolved_type: str
) -> list[tuple[str, int]]:
    """Distinct values of a column that fail ``TRY_CAST`` to its resolved type.

    Queries the RAW (VARCHAR) table, not the row-level quarantine table: a row is
    quarantined if ANY column failed, so a per-column read must apply the
    column's OWN cast. Returns ``[(token, count), …]`` (descending) for non-null
    values that do not parse — empty for a VARCHAR column (every cast succeeds).

    ``resolved_type`` is a DuckDB type keyword from the type decision (e.g.
    ``DECIMAL(18,2)``), not user input.
    """
    col = column_name.replace('"', '""')
    rows = duckdb_conn.execute(
        f'SELECT "{col}" AS token, COUNT(*) AS n FROM {raw_table_fqn} '
        f'WHERE "{col}" IS NOT NULL AND TRY_CAST("{col}" AS {resolved_type}) IS NULL '
        f'GROUP BY "{col}" ORDER BY n DESC'
    ).fetchall()
    return [(str(token), int(n)) for token, n in rows]


def load_quarantine_tokens(
    session: Session,
    column_id: str,
    duckdb_conn: Any,
    run_id: str | None = None,
) -> dict[str, Any] | None:
    """Per-token cast-failure counts for a column — the quarantine witness input.

    Resolves the column's raw table + resolved type, then counts the distinct
    tokens that fail the cast. ``None`` if the column/table is unresolved;
    ``{"rejected_tokens": [], "total_rejected": 0}`` for a VARCHAR column (no
    inferred type → no rejects → no null-token signal).
    """
    from dataraum.core.duckdb_naming import schema_for_layer
    from dataraum.server.storage import LAKE_CATALOG_ALIAS
    from dataraum.storage import Column, Table

    if duckdb_conn is None:
        return None
    col = session.get(Column, column_id)
    if col is None:
        return None
    table = session.get(Table, col.table_id)
    if table is None or not table.duckdb_path:
        return None

    typing = load_typing(session, column_id, run_id)
    resolved_type = (typing or {}).get("resolved_type")
    if not resolved_type or str(resolved_type).upper() in _TEXT_RESOLVED_TYPES:
        return {"rejected_tokens": [], "total_rejected": 0}
    # Typing is the authority on rejection. When it quarantined NOTHING for this
    # column, a plain TRY_CAST below would still over-reject formats typing's
    # pattern parser accepts — a DATE column of "2025-02" parses for typing but
    # not a bare CAST — minting phantom null tokens on a clean column. No typing
    # rejects → no candidates. (Live-run finding, DAT-457: trial_balance.period
    # typed cleanly as DATE, quarantine_rate 0, yet the bare cast "found" rejects.)
    if not (typing or {}).get("quarantine_rate"):
        return {"rejected_tokens": [], "total_rejected": 0}

    raw_fqn = f'{LAKE_CATALOG_ALIAS}.{schema_for_layer("raw")}."{table.duckdb_path}"'
    counts = rejected_token_counts(duckdb_conn, raw_fqn, col.column_name, str(resolved_type))
    return {
        "rejected_tokens": [{"token": token, "count": n} for token, n in counts],
        "total_rejected": sum(n for _, n in counts),
    }


def load_statistics(
    session: Session,
    column_id: str,
    run_id: str | None = None,
    base_runs: Mapping[tuple[str, str], str] | None = None,
) -> dict[str, Any] | None:
    """Load statistical profile and quality metrics for a column.

    Returns dict with null_count, null_ratio, distinct_count, quality, etc.
    or None if no statistics exist.

    ``run_id`` (DAT-413): when set, restrict to THIS run's profile + quality
    metrics. ``None`` (non-detect / test callers) adds no filter.

    Pinned fallback (DAT-405/448): like :func:`load_semantic` — session detects
    read statistics the add_source run wrote, so a this-run miss falls back to
    the run-start pin (``statistics`` for the profile, ``statistical_quality``
    for the quality metrics — distinct stages, distinct pins).
    """
    from dataraum.analysis.statistics.db_models import StatisticalProfile
    from dataraum.analysis.statistics.quality_db_models import StatisticalQualityMetrics

    sp_stmt = select(StatisticalProfile).where(StatisticalProfile.column_id == column_id)
    if run_id is not None:
        sp = session.execute(
            sp_stmt.where(StatisticalProfile.run_id == run_id)
        ).scalar_one_or_none()
        if sp is None:
            pinned = _pinned_base_run(session, column_id, "statistics", base_runs)
            if pinned is not None and pinned != run_id:
                sp = session.execute(
                    sp_stmt.where(StatisticalProfile.run_id == pinned)
                ).scalar_one_or_none()
    else:
        # No run pinned: most recent (= the promoted re-run), never scalar_one_or_none
        # — profiles coexist across re-runs (upsert is per (column_id, run_id)) (DAT-447).
        sp = (
            session.execute(sp_stmt.order_by(StatisticalProfile.profiled_at.desc()))
            .scalars()
            .first()
        )

    if not sp:
        return None

    stats_dict: dict[str, Any] = {
        "null_count": sp.null_count,
        "null_ratio": sp.null_count / sp.total_count if sp.total_count else 0,
        "distinct_count": sp.distinct_count,
        "cardinality_ratio": sp.cardinality_ratio,
        "total_count": sp.total_count,
        "profile_data": sp.profile_data,
    }
    qm_stmt = select(StatisticalQualityMetrics).where(
        StatisticalQualityMetrics.column_id == column_id
    )
    if run_id is not None:
        qm = session.execute(
            qm_stmt.where(StatisticalQualityMetrics.run_id == run_id)
        ).scalar_one_or_none()
        if qm is None:
            pinned = _pinned_base_run(session, column_id, "statistical_quality", base_runs)
            if pinned is not None and pinned != run_id:
                qm = session.execute(
                    qm_stmt.where(StatisticalQualityMetrics.run_id == pinned)
                ).scalar_one_or_none()
    else:
        qm = (
            session.execute(qm_stmt.order_by(StatisticalQualityMetrics.computed_at.desc()))
            .scalars()
            .first()
        )
    if qm:
        qd = qm.quality_data or {}
        quality_dict: dict[str, Any] = {
            "benford_compliant": bool(qm.benford_compliant)
            if qm.benford_compliant is not None
            else None,
            "benford_analysis": qd.get("benford_analysis"),
            "quality_data": qm.quality_data,
        }
        # Only include outlier_detection when outlier analysis was actually
        # performed.  Excluded columns (skip_outliers=True) store NULL for
        # iqr_outlier_ratio — omitting the key lets the detector return []
        # ("not assessed") instead of a false 0-score ("zero outliers").
        if qm.iqr_outlier_ratio is not None:
            outlier_data = qd.get("outlier_detection") or {}
            quality_dict["outlier_detection"] = {
                "iqr_outlier_ratio": qm.iqr_outlier_ratio,
                "iqr_outlier_count": outlier_data.get("iqr_outlier_count", 0),
                "iqr_lower_fence": outlier_data.get("iqr_lower_fence"),
                "iqr_upper_fence": outlier_data.get("iqr_upper_fence"),
                "zscore_outlier_ratio": qm.zscore_outlier_ratio or 0.0,
                "has_outliers": bool(qm.has_outliers) if qm.has_outliers is not None else False,
            }
        stats_dict["quality"] = quality_dict
    return stats_dict


def load_semantic(
    session: Session,
    column_id: str,
    run_id: str | None = None,
    base_runs: Mapping[tuple[str, str], str] | None = None,
) -> dict[str, Any] | None:
    """Load semantic annotation for a column.

    Returns dict with semantic_role, entity_type, business_name, etc.
    or None if no annotation exists.

    ``run_id`` (DAT-413): when set, restrict to THIS run's annotation. ``None``
    (non-detect / test callers) adds no filter.

    Pinned fallback (DAT-405/448): a begin_session detect carries the SESSION
    run's ``run_id``, but annotations are written by the add_source run — a
    strict this-run read finds nothing and silently disabled every
    semantic-gated session detector (dimensional_entropy and derived_value lose
    the role/semantic context their detect step needs). When this run has no
    annotation, read the run the orchestrator pinned for the column's table at
    ``semantic_per_column``; no pin → ``None`` as before.
    """
    from dataraum.analysis.semantic.db_models import SemanticAnnotation

    sa_stmt = select(SemanticAnnotation).where(SemanticAnnotation.column_id == column_id)
    if run_id is not None:
        sa = session.execute(
            sa_stmt.where(SemanticAnnotation.run_id == run_id)
        ).scalar_one_or_none()
        if sa is None:
            pinned = _pinned_base_run(session, column_id, "semantic_per_column", base_runs)
            if pinned is not None and pinned != run_id:
                sa = session.execute(
                    sa_stmt.where(SemanticAnnotation.run_id == pinned)
                ).scalar_one_or_none()
    else:
        sa = (
            session.execute(sa_stmt.order_by(SemanticAnnotation.annotated_at.desc()))
            .scalars()
            .first()
        )
    if not sa:
        return None

    semantic_dict: dict[str, Any] = {
        "semantic_role": sa.semantic_role,
        "entity_type": sa.entity_type,
        "business_name": sa.business_name,
        "business_description": sa.business_description,
        "confidence": sa.confidence,
        "business_concept": sa.business_concept,
    }
    if sa.unit_source_column:
        semantic_dict["unit_source_column"] = sa.unit_source_column
    if sa.temporal_behavior:
        semantic_dict["temporal_behavior"] = sa.temporal_behavior
    # The LLM stock/flow witness (DAT-445), read alongside the ontology prior.
    if sa.temporal_behavior_claim:
        semantic_dict["temporal_behavior_claim"] = sa.temporal_behavior_claim
    if sa.temporal_behavior_claim_confidence is not None:
        semantic_dict["temporal_behavior_claim_confidence"] = sa.temporal_behavior_claim_confidence
    return semantic_dict


def _relationship_to_dict(rel: Any, table_names: dict[str, str]) -> dict[str, Any]:
    """Shape a ``Relationship`` row into the dict the relationship detectors read."""
    return {
        "relationship_type": rel.relationship_type,
        "confidence": rel.confidence,
        "detection_method": rel.detection_method,
        "from_table": table_names.get(rel.from_table_id, "unknown"),
        "to_table": table_names.get(rel.to_table_id, "unknown"),
        "from_column_id": rel.from_column_id,
        "to_column_id": rel.to_column_id,
        "cardinality": rel.cardinality,
        "evidence": rel.evidence,
    }


# The measured referential-integrity evidence keys the relationships analyzer
# writes (``detector._store_candidates`` / ``evaluator.compute_ri_metrics``) and
# ``relationship_entropy`` scores. Teach-materialized rows (manual/keeper,
# ``materialize_relationship_overlays``) carry overlay provenance only — never
# these — so the representative backfills them from the measured rows of the
# same pair. Without the backfill, teaching a relationship silently deleted its
# orphan-rate measurement (recall = 0 on every taught pair, no error anywhere).
_RI_EVIDENCE_KEYS = (
    "left_referential_integrity",
    "right_referential_integrity",
    "orphan_count",
    "total_count",
    "left_total_count",
    "cardinality_verified",
)


def load_relationship_for_pair(
    session: Session,
    from_column_id: str,
    to_column_id: str,
    session_id: str | None = None,
    run_id: str | None = None,
) -> dict[str, Any] | None:
    """The representative relationship for one directional column pair (DAT-408).

    Several method-rows (candidate / llm / manual / keeper) may share a pair; the
    representative is the highest-precedence one (manual > keeper > llm > candidate)
    — that is the relationship the readiness measures. ``session_id`` + ``run_id``
    scope to this run's catalog (rows coexist across runs/sessions).

    The representative keeps its IDENTITY (method, confidence, confirmation), but
    measured RI evidence (:data:`_RI_EVIDENCE_KEYS`) is backfilled from the
    highest-precedence row of the pair that carries it: the data witness lives on
    the candidate/llm rows, and an overlay-materialized representative must not
    shadow it into a silent no-measure. ``ri_evidence_source`` records the donor
    method when a backfill happened.
    """
    from dataraum.analysis.relationships.db_models import Relationship
    from dataraum.storage import Table

    stmt = select(Relationship).where(
        Relationship.from_column_id == from_column_id,
        Relationship.to_column_id == to_column_id,
    )
    if session_id is not None:
        stmt = stmt.where(Relationship.session_id == session_id)
    if run_id is not None:
        stmt = stmt.where(Relationship.run_id == run_id)
    rels = list(session.execute(stmt).scalars())
    if not rels:
        return None
    precedence = {"manual": 4, "keeper": 3, "llm": 2, "candidate": 1}
    ranked = sorted(rels, key=lambda r: precedence.get(r.detection_method or "", 0), reverse=True)
    rel = ranked[0]
    table_names = dict(
        session.execute(
            select(Table.table_id, Table.table_name).where(
                Table.table_id.in_([rel.from_table_id, rel.to_table_id])
            )
        )
        .tuples()
        .all()
    )
    result = _relationship_to_dict(rel, table_names)
    evidence: dict[str, Any] = dict(result.get("evidence") or {})
    for donor in ranked[1:]:
        donor_evidence = donor.evidence or {}
        backfilled = [
            key for key in _RI_EVIDENCE_KEYS if key not in evidence and key in donor_evidence
        ]
        for key in backfilled:
            evidence[key] = donor_evidence[key]
        if backfilled:
            evidence.setdefault("ri_evidence_source", donor.detection_method)
    result["evidence"] = evidence
    return result


def load_session_relationships(
    session: Session, session_id: str, run_id: str | None = None
) -> list[dict[str, Any]]:
    """This run's **defined** relationships for a session (DAT-408) — the join-path set.

    "Defined" = ``detection_method != 'candidate'`` (the catalog contract in
    db_models.py / relationships.utils): join-path ambiguity is measured among the
    LLM-selected relationships, not the ephemeral structural candidates — two bare
    candidates between the same two tables are not a real ambiguous join. Scoped to
    ``run_id`` (the current run's catalog) so coexisting prior-run rows don't
    double-count; ``None`` (tests) reads the session unscoped.
    """
    from dataraum.analysis.relationships.db_models import Relationship
    from dataraum.storage import Table

    stmt = select(Relationship).where(
        Relationship.session_id == session_id,
        Relationship.detection_method != "candidate",
    )
    if run_id is not None:
        stmt = stmt.where(Relationship.run_id == run_id)
    rels = list(session.execute(stmt).scalars())
    if not rels:
        return []
    table_ids = {r.from_table_id for r in rels} | {r.to_table_id for r in rels}
    table_names = dict(
        session.execute(
            select(Table.table_id, Table.table_name).where(Table.table_id.in_(table_ids))
        )
        .tuples()
        .all()
    )
    return [_relationship_to_dict(rel, table_names) for rel in rels]


def load_correlation(
    session: Session, column_id: str, column_name: str, run_id: str | None = None
) -> dict[str, Any] | None:
    """Load derived column info for a column.

    Returns dict with derived_columns list or None if no derivations found.

    ``run_id`` (DAT-448): ``DerivedColumn`` rows are run-versioned and written
    by the SAME begin_session run that detects against them, so the read is a
    plain this-run filter — coexisting prior-run rows must not double-count.
    ``None`` (test callers) matches unstamped rows.
    """
    from dataraum.analysis.correlation.db_models import DerivedColumn

    dcs = (
        session.execute(
            select(DerivedColumn).where(
                DerivedColumn.derived_column_id == column_id,
                DerivedColumn.run_id == run_id,
            )
        )
        .scalars()
        .all()
    )
    if not dcs:
        return None

    return {
        "derived_columns": [
            {
                "derived_column_name": column_name,
                "formula": dc.formula,
                "match_rate": dc.match_rate,
                "derivation_type": dc.derivation_type,
                "source_column_ids": dc.source_column_ids or [],
            }
            for dc in dcs
        ]
    }


# load_drift_summaries removed with the temporal_drift detector (DAT-442 reset).
# dimensional_entropy reads ColumnDriftSummary directly; real drift moves to DAT-445.


def load_documented_dependencies(session: Session) -> set[frozenset[str]]:
    """Undirected column pairs a ``document_business_rule`` teach marked EXPECTED.

    A documented cross-column dependency (e.g. the debit/credit double-entry mutex) is
    expected structure, NOT undocumented entropy — ``dimensional_entropy`` excludes these
    pairs from its NMI score, so a teach closes the measurement. Mirrors
    ``load_confirmed_relationship_pairs`` (DAT-409): one overlay type, undirected, and
    ``superseded_at IS NULL`` filters undone teaches out.

    Overlay shape: ``ConfigOverlay(type='expected_dependency',
    payload={'column_ids': [col_a, col_b], 'rule': <text>})``.
    """
    from dataraum.storage import ConfigOverlay

    rows = list(
        session.execute(
            select(ConfigOverlay).where(
                ConfigOverlay.type == "expected_dependency",
                ConfigOverlay.superseded_at.is_(None),
            )
        ).scalars()
    )
    out: set[frozenset[str]] = set()
    for row in rows:
        cols = (row.payload or {}).get("column_ids") or []
        if len(cols) == 2 and all(cols):
            out.add(frozenset(cols))
    return out


def load_structural_reconciliation(
    session: Session, column_id: str, run_id: str | None
) -> dict[str, Any] | None:
    """The column's reconciled aggregation lineage for THIS run (DAT-491).

    Exact-run by design, NO pinned fallback: lineage rows are written by the
    begin_session ``aggregation_lineage`` phase under the session run's
    ``run_id``, so the ``structural_reconciliation`` witness fires at that run's
    ``session_detect`` and abstains everywhere else (every add_source detect in
    particular — the two-witness behaviour there is unchanged). A cross-run
    fallback would re-introduce the stale-immortal-artifact failure mode the
    slice definitions hit (DAT-405); revisit only with head-promotion semantics
    for lineage.
    """
    from dataraum.analysis.lineage.db_models import MeasureAggregationLineage

    if run_id is None:
        return None
    row = session.execute(
        select(MeasureAggregationLineage).where(
            MeasureAggregationLineage.measure_column_id == column_id,
            MeasureAggregationLineage.run_id == run_id,
        )
    ).scalar_one_or_none()
    if row is None:
        return None
    return {
        "pattern": row.pattern,
        "match_rate": row.match_rate,
        "event_table_id": row.event_table_id,
        "r_flow_median": row.r_flow_median,
        "r_stock_median": row.r_stock_median,
    }
