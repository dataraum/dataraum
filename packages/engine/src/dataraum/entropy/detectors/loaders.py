"""Loader helpers for detector data loading.

Each helper extracts data from the DB for a specific analysis domain,
returning dict structures matching what detectors expect in
context.analysis_results. Extracted 1:1 from snapshot.load_column_analysis()
and snapshot.load_table_analysis().
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from dataraum.core.duckdb_types import is_numeric

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sqlalchemy.orm import Session


def resolve_base_runs(session: Session, table_ids: Sequence[str]) -> dict[str, str]:
    """Pin the promoted base-run map for a detect run (DAT-448 / DAT-506).

    Resolves the per-table generation head ``(table:{id}, GENERATION_STAGE)``
    ONCE per table at detect start. add_source seals a table's whole run (typing
    → semantic_per_column → detect) under ONE generation head (DAT-506), so a
    single per-table lookup pins every upstream stage. The detect orchestrator
    threads the map onto every ``DetectorContext``, so all loader reads in the
    run resolve to the same base runs regardless of concurrent promotes — per-call
    head resolution allowed a mid-run promote to tear reads. Unpromoted tables are
    simply absent: readers fail closed, never guess a run.
    """
    from dataraum.storage.snapshot_head import GENERATION_STAGE, head_run_id

    pinned: dict[str, str] = {}
    for table_id in table_ids:
        rid = head_run_id(session, f"table:{table_id}", GENERATION_STAGE)
        if rid is not None:
            pinned[table_id] = rid
    return pinned


def _pinned_base_run(
    session: Session,
    column_id: str,
    base_runs: Mapping[str, str] | None,
) -> str | None:
    """The pinned base ``run_id`` for the column's table (DAT-448 / DAT-506).

    Session-detect reads carry the begin_session run's ``run_id``, but the
    per-column analysis rows (semantic, statistics, …) were written — and
    promoted — by the add_source run that produced the typed table (DAT-405).
    The fallback consults the run-start pin (the table's generation head)
    instead of re-resolving the moving head per call. No pin → ``None`` — the
    caller keeps its no-data behaviour (fail closed, never guess).
    """
    if not base_runs:
        return None
    from dataraum.storage import Column

    col = session.get(Column, column_id)
    if col is None:
        return None
    return base_runs.get(col.table_id)


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
    base_runs: Mapping[str, str] | None = None,
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
            pinned = _pinned_base_run(session, column_id, base_runs)
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
            pinned = _pinned_base_run(session, column_id, base_runs)
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
            # The benford detector reads the full benford_analysis dict — the
            # typed status (DAT-843) rides inside it; no separate key.
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
    base_runs: Mapping[str, str] | None = None,
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
    from dataraum.analysis.semantic.db_models import ColumnConcept, SemanticAnnotation

    sa_stmt = select(SemanticAnnotation).where(SemanticAnnotation.column_id == column_id)
    if run_id is not None:
        sa = session.execute(
            sa_stmt.where(SemanticAnnotation.run_id == run_id)
        ).scalar_one_or_none()
        if sa is None:
            pinned = _pinned_base_run(session, column_id, base_runs)
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

    # OBJECT-grain fields from the per-column annotation (DAT-637).
    semantic_dict: dict[str, Any] = {
        "semantic_role": sa.semantic_role,
        "entity_type": sa.entity_type,
        "business_name": sa.business_name,
        "business_description": sa.business_description,
        "confidence": sa.confidence,
    }
    # The LLM stock/flow witness (DAT-445), an object-grain single-column read.
    if sa.temporal_behavior_claim:
        semantic_dict["temporal_behavior_claim"] = sa.temporal_behavior_claim
    if sa.temporal_behavior_claim_confidence is not None:
        semantic_dict["temporal_behavior_claim_confidence"] = sa.temporal_behavior_claim_confidence

    # CATALOGUE-grain fields from ColumnConcept (DAT-637): authored by the table
    # agent under the begin_session run. At session_detect ``run_id`` IS that run,
    # so they are present; at add_source detect no ColumnConcept exists under the
    # add_source run, so the catalogue fields are simply absent — the grain
    # boundary the architecture enforces, not a lookup miss to paper over.
    cc = None
    if run_id is not None:
        cc = session.execute(
            select(ColumnConcept).where(
                ColumnConcept.column_id == column_id,
                ColumnConcept.run_id == run_id,
            )
        ).scalar_one_or_none()
    if cc is not None:
        semantic_dict["meaning"] = cc.meaning
        if cc.unit_source_column:
            semantic_dict["unit_source_column"] = cc.unit_source_column
        if cc.temporal_behavior:
            semantic_dict["temporal_behavior"] = cc.temporal_behavior
        # The LLM formula-hypothesis witness (derived_value second witness, ADR-0009).
        if cc.derived_formula_hypothesis:
            semantic_dict["derived_formula_hypothesis"] = cc.derived_formula_hypothesis
        if cc.derived_formula_confidence is not None:
            semantic_dict["derived_formula_confidence"] = cc.derived_formula_confidence
    else:
        semantic_dict["meaning"] = None
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
    "left_key_coverage",
    "right_key_coverage",
    "left_orphan_count",
    "right_orphan_count",
    "left_total_count",
    "right_total_count",
    "cardinality_verified",
)


def load_relationship_for_pair(
    session: Session,
    from_column_id: str,
    to_column_id: str,
    run_id: str | None = None,
) -> dict[str, Any] | None:
    """The representative relationship for one directional column pair (DAT-408).

    Several method-rows (candidate / llm / manual / keeper) may share a pair; the
    representative is the highest-precedence one (manual > keeper > llm > candidate)
    — that is the relationship the readiness measures. ``run_id`` scopes to this
    run's catalog (rows coexist across runs).

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


def load_relationship_rows_for_pair(
    session: Session,
    from_column_id: str,
    to_column_id: str,
    run_id: str | None = None,
) -> dict[str, dict[str, Any]]:
    """ALL method-rows for one column pair, keyed by detection method.

    The relationship_discovery adjudication reads every witness class the pair
    carries — the structural ``candidate`` (value-overlap statistics), the
    ``llm`` confirmation, and the teach-materialized ``manual``/``keeper`` rows
    — so it needs the per-method rows side by side, NOT the single
    highest-precedence representative :func:`load_relationship_for_pair`
    returns. Same run scoping.

    Direction-AGNOSTIC by design (wave-2 cal finding): candidate rows can be
    persisted parent→child while the defined pair is child→parent, and the
    one-way match left the value_overlap witness silently uniform (dropped
    before persisting) on every such pair — a false-negative machine. The same
    column pair is one relationship; when both directions carry a row for the
    same method, the exact requested direction wins.
    """
    from sqlalchemy import and_, or_

    from dataraum.analysis.relationships.db_models import Relationship
    from dataraum.storage import Table

    stmt = select(Relationship).where(
        or_(
            and_(
                Relationship.from_column_id == from_column_id,
                Relationship.to_column_id == to_column_id,
            ),
            and_(
                Relationship.from_column_id == to_column_id,
                Relationship.to_column_id == from_column_id,
            ),
        )
    )
    if run_id is not None:
        stmt = stmt.where(Relationship.run_id == run_id)
    rels = list(session.execute(stmt).scalars())
    if not rels:
        return {}
    table_ids = {r.from_table_id for r in rels} | {r.to_table_id for r in rels}
    table_names = dict(
        session.execute(
            select(Table.table_id, Table.table_name).where(Table.table_id.in_(table_ids))
        )
        .tuples()
        .all()
    )
    out: dict[str, dict[str, Any]] = {}
    exact_direction: dict[str, bool] = {}
    for rel in rels:
        if not rel.detection_method:
            continue
        method = str(rel.detection_method)
        is_exact = rel.from_column_id == from_column_id
        if method in out and exact_direction[method] and not is_exact:
            continue
        out[method] = _relationship_to_dict(rel, table_names)
        exact_direction[method] = is_exact
    return out


def load_session_relationships(session: Session, run_id: str | None = None) -> list[dict[str, Any]]:
    """This run's **defined** relationships (DAT-408) — the join-path set.

    "Defined" = ``detection_method != 'candidate'`` (the catalog contract in
    db_models.py / relationships.utils): join-path ambiguity is measured among the
    LLM-selected relationships, not the ephemeral structural candidates — two bare
    candidates between the same two tables are not a real ambiguous join. Scoped to
    ``run_id`` (the current run's catalog) so coexisting prior-run rows don't
    double-count; ``None`` (tests) reads unscoped.
    """
    from dataraum.analysis.relationships.db_models import Relationship
    from dataraum.storage import Table

    stmt = select(Relationship).where(
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


def load_hypothesis_match_rate(
    session: Session,
    column_id: str,
    duckdb_conn: Any,
    source_columns: tuple[str, str],
    operation: str,
) -> dict[str, Any] | None:
    """Row-grade an LLM-hypothesized formula over the column's typed table.

    The derived_value second witness (ADR-0009): the LLM hypothesizes
    ``column = col1 op col2``; the data grounds it with the SAME row statistic
    the formula discovery uses (``formula_match_counts`` — shared tolerance,
    shared zero-target exclusion). Source names are resolved case-insensitively
    against the table's actual columns — an unknown name (hallucinated source)
    returns ``None`` so the data witness abstains instead of guessing; the same
    for a non-numeric target (nothing gradable → total 0).

    Returns ``{"match_rate", "matches", "total"}`` or ``None`` when ungradable.
    """
    from dataraum.analysis.correlation.within_table.derived_columns import formula_match_counts
    from dataraum.core.duckdb_naming import schema_for_layer
    from dataraum.entropy.measurements.derived_value import OPERATION_SYMBOL
    from dataraum.server.storage import LAKE_CATALOG_ALIAS
    from dataraum.storage import Column, Table

    op = OPERATION_SYMBOL.get(operation)
    if duckdb_conn is None or op is None:
        return None
    col = session.get(Column, column_id)
    if col is None:
        return None
    table = session.get(Table, col.table_id)
    if table is None or not table.duckdb_path:
        return None

    # Resolve hypothesis source names against the table's REAL columns (the
    # actual stored spelling is what gets quoted into SQL — never the LLM text)
    # — NUMERIC columns only, mirroring the discovery sweep's filter
    # (derived_columns.py numeric_cols). Without the type gate a hypothesis
    # over DATE/VARCHAR sources (e.g. "end_date - start_date" for a duration
    # column) TRY_CASTs every row to NULL, grades match_rate 0.0, and a
    # perfectly clean column scores 1.0 (review wave-1 blocker).
    if not is_numeric(col.resolved_type):
        return None
    siblings = {
        c.column_name.strip().lower(): c.column_name
        for c in session.execute(select(Column).where(Column.table_id == col.table_id)).scalars()
        if is_numeric(c.resolved_type)
    }
    resolved = [siblings.get(name.strip().lower()) for name in source_columns]
    if any(name is None for name in resolved) or len(resolved) != 2:
        return None

    fqn = f'{LAKE_CATALOG_ALIAS}.{schema_for_layer("typed")}."{table.duckdb_path}"'
    matches, total = formula_match_counts(
        duckdb_conn, fqn, col.column_name, str(resolved[0]), str(resolved[1]), op
    )
    if total == 0:
        return None
    return {"match_rate": matches / total, "matches": matches, "total": total}


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


def load_declared_formula(
    session: Session, table_name: str, column_name: str
) -> dict[str, Any] | None:
    """The user-declared expected formula for one column (DAT-447, Option B).

    The derived_value teach rides the EXISTING ``validation`` overlay type: a
    declared expected formula is a spec-shaped ``validation`` teach row with
    ``check_type: "expected_formula"`` and ``parameters: {table, column,
    formula}`` (full shape documented on ``core.overlay._apply_validation``).
    The validation phase executes the same row as a declared check every run;
    this read pools it as the ``human_declaration`` witness on the matching
    formula claim. Mirrors ``load_documented_dependencies``: a direct
    ``config_overlay`` read, ``superseded_at IS NULL`` filters undone teaches,
    ``created_at`` ASC + last write wins (the applier's upsert convention).

    Returns ``{"formula": <str>}`` or ``None`` when no declaration targets the
    column. Identity matching is case-insensitive on table + column names;
    formula canonicalization stays the measurement's job (``parse_formula``).
    """
    from dataraum.storage import ConfigOverlay

    rows = list(
        session.execute(
            select(ConfigOverlay)
            .where(
                ConfigOverlay.type == "validation",
                ConfigOverlay.superseded_at.is_(None),
            )
            .order_by(ConfigOverlay.created_at.asc())
        ).scalars()
    )
    target_table = table_name.strip().lower()
    target_column = column_name.strip().lower()
    declared: str | None = None
    for row in rows:
        payload = row.payload or {}
        if payload.get("check_type") != "expected_formula":
            continue
        params = payload.get("parameters") or {}
        if (
            str(params.get("table") or "").strip().lower() != target_table
            or str(params.get("column") or "").strip().lower() != target_column
        ):
            continue
        formula = params.get("formula")
        if formula:
            declared = str(formula)  # rows are created_at ASC → last write wins
    if declared is None:
        return None
    return {"formula": declared}


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
