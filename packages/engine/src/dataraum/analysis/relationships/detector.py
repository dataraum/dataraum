"""Entry point for relationship detection."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.relationships.db_models import (
    Relationship as RelationshipDB,
)
from dataraum.analysis.relationships.evaluator import evaluate_candidates
from dataraum.analysis.relationships.finder import find_relationships
from dataraum.analysis.relationships.models import (
    JoinCandidate,
    RelationshipCandidate,
    RelationshipDetectionResult,
)
from dataraum.analysis.relationships.utils import load_suppressed_relationship_pairs
from dataraum.analysis.semantic.utils import load_column_mappings, load_table_mappings
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.storage import Table
from dataraum.storage.upsert import upsert

logger = get_logger(__name__)


def detect_relationships(
    table_ids: list[str],
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
    min_confidence: float = 0.3,
    evaluate: bool = True,
    run_id: str | None = None,
) -> Result[RelationshipDetectionResult]:
    """Detect relationships between tables and store as candidates.

    Uses value overlap (Jaccard/containment) to find joinable column pairs.
    Candidates are stored for semantic analysis to confirm/reject.
    Fully deterministic (DAT-794): no sampling anywhere below 1M distinct
    values, so repeated runs over the same data produce identical candidates.

    Args:
        table_ids: List of table IDs to analyze
        duckdb_conn: DuckDB connection
        session: SQLAlchemy async session
        min_confidence: Minimum join_confidence threshold (default 0.3)
        evaluate: Whether to evaluate candidates with quality metrics (default True)

    Returns:
        Result containing RelationshipDetectionResult
    """
    start_time = time.time()
    logger.debug(
        "relationship_detection_started",
        table_count=len(table_ids),
        min_confidence=min_confidence,
    )

    try:
        # Load table paths + column metadata (no row data — uniqueness is computed in SQL)
        tables_data = _load_tables(session, table_ids)

        # A single table can still carry a self-referential FK (DAT-763), so one
        # table is enough to detect; only an empty scope has nothing to probe.
        if not tables_data:
            return Result.ok(
                RelationshipDetectionResult(
                    candidates=[],
                    total_tables=len(tables_data),
                    computed_at=datetime.now(UTC),
                    duration_seconds=time.time() - start_time,
                )
            )

        # Find relationships via value overlap
        raw_results = find_relationships(duckdb_conn, tables_data, min_confidence)

        # Convert to typed models
        candidates = [
            RelationshipCandidate(
                table1=r["table1"],
                table2=r["table2"],
                join_candidates=[
                    JoinCandidate(
                        column1=j["column1"],
                        column2=j["column2"],
                        join_confidence=j["join_confidence"],
                        cardinality=j["cardinality"],
                        left_uniqueness=j["left_uniqueness"],
                        right_uniqueness=j["right_uniqueness"],
                        statistical_confidence=j.get("statistical_confidence", 1.0),
                        algorithm=j.get("algorithm", "exact"),
                    )
                    for j in r["join_columns"]
                ],
            )
            for r in raw_results
        ]

        # Evaluate candidates with quality metrics (referential integrity, etc.)
        if evaluate and candidates:
            table_paths = {name: path for name, (path, _cols, _types) in tables_data.items()}
            candidates = evaluate_candidates(candidates, table_paths, duckdb_conn)

        # Store candidates in database
        _store_candidates(session, table_ids, candidates, run_id=run_id)

        # Count high confidence candidates
        high_conf_count = sum(
            1 for c in candidates for jc in c.join_candidates if jc.join_confidence > 0.7
        )

        return Result.ok(
            RelationshipDetectionResult(
                candidates=candidates,
                total_tables=len(tables_data),
                total_candidates=len(candidates),
                high_confidence_count=high_conf_count,
                computed_at=datetime.now(UTC),
                duration_seconds=time.time() - start_time,
            )
        )

    except Exception as e:
        return Result.fail(f"Relationship detection failed: {e}")


def _store_candidates(
    session: Session,
    table_ids: list[str],
    candidates: list[RelationshipCandidate],
    *,
    run_id: str | None = None,
) -> None:
    """Store this run's relationship candidates (DAT-408 run-versioned).

    ``candidate`` rows are ephemeral structural detections, re-derived every run
    and stamped with ``run_id``. They coexist with prior runs (non-destructive).
    Idempotent form-(a) writer (DAT-502): rows dedup in-batch on the
    ``uq_relationship_columns_method`` key, then UPSERT — a Temporal
    success-redelivery (same ``run_id``) converges on the same rows instead of
    needing a run-scoped clear. Reads scope to the current run, so coexistence
    is invisible to consumers.
    """
    # Load mappings
    column_map = load_column_mappings(session, table_ids)
    table_map = load_table_mappings(session, table_ids)

    # A user-dropped relationship must not be re-created on re-run (DAT-408).
    suppressed = load_suppressed_relationship_pairs(session)

    # In-batch dedup on the unique key (the same column pair can surface from
    # two table-pair contexts in one batch); last write wins.
    rows: dict[tuple[str | None, str, str, str], dict[str, Any]] = {}

    for candidate in candidates:
        table1_id = table_map.get(candidate.table1)
        table2_id = table_map.get(candidate.table2)

        if not table1_id or not table2_id:
            continue

        # Store each join candidate as a relationship
        for jc in candidate.join_candidates:
            col1_id = column_map.get((candidate.table1, jc.column1))
            col2_id = column_map.get((candidate.table2, jc.column2))

            if not col1_id or not col2_id:
                continue

            if frozenset((col1_id, col2_id)) in suppressed:
                # User dropped this relationship — honor the suppression overlay
                # (undirected: a reject holds whichever way the pair is named).
                continue

            # Build evidence with value overlap and column characteristics
            evidence = {
                "join_confidence": jc.join_confidence,
                "cardinality": jc.cardinality,
                "left_uniqueness": jc.left_uniqueness,
                "right_uniqueness": jc.right_uniqueness,
                "statistical_confidence": jc.statistical_confidence,
                "algorithm": jc.algorithm,
                "source": "value_overlap",
            }

            # Add evaluation metrics if available
            if jc.left_referential_integrity is not None:
                evidence["left_referential_integrity"] = jc.left_referential_integrity
            if jc.right_referential_integrity is not None:
                evidence["right_referential_integrity"] = jc.right_referential_integrity
            if jc.left_orphan_count is not None:
                evidence["left_orphan_count"] = jc.left_orphan_count
            if jc.cardinality_verified is not None:
                evidence["cardinality_verified"] = jc.cardinality_verified

            # Add relationship-level evaluation metrics
            if candidate.left_join_success_rate is not None:
                evidence["left_join_success_rate"] = candidate.left_join_success_rate
            if candidate.introduces_duplicates is not None:
                evidence["introduces_duplicates"] = candidate.introduces_duplicates

            # Build through the model's single orientation chokepoint (DAT-777):
            # a candidate is stored many→one child→parent like every other write
            # path, and the PK is omitted so the model's Python-side default
            # applies (upsert contract, storage/upsert.py). A structural candidate
            # is unconfirmed by definition (DAT-776). Dedup keys on the ORIENTED
            # pair so two inputs orienting onto the same pair don't collide at
            # upsert ("cannot affect row a second time").
            row = RelationshipDB.oriented_row(
                run_id=run_id,
                from_table_id=table1_id,
                from_column_id=col1_id,
                to_table_id=table2_id,
                to_column_id=col2_id,
                relationship_type="candidate",
                cardinality=jc.cardinality,
                confidence=jc.join_confidence,
                detection_method="candidate",
                confirmation_source="unconfirmed",
                evidence=evidence,
            )
            rows[(run_id, row["from_column_id"], row["to_column_id"], "candidate")] = row

    upsert(
        session,
        RelationshipDB,
        list(rows.values()),
        index_elements=[
            "run_id",
            "from_column_id",
            "to_column_id",
            "detection_method",
        ],
    )


def _load_tables(
    session: Session,
    table_ids: list[str],
) -> dict[str, tuple[str, list[str], dict[str, str | None]]]:
    """Load each table's ``(duckdb_path, column_names, column_types)`` from the catalog.

    No row data is materialized: join detection and the uniqueness ratio both run in SQL
    over ``duckdb_path``. ``column_names`` are the catalog columns (ordered by position);
    ``column_types`` maps column_name → resolved_type for type-aware comparison.

    Catalog-only — this never touches DuckDB, so it can't fail on a stale ``duckdb_path``.
    A bad path now surfaces later, in the SQL join/uniqueness step, and fails the whole
    detection via the caller's ``except`` (fail-loud). This replaces the old per-table
    try/except that materialized each table and skipped a failing one — there is no longer
    a partial-results path: every table's ``duckdb_path`` must be readable.
    """
    from dataraum.storage import Column

    # Load tables with their columns. ORDERED BY NAME so a run is reproducible:
    # the mapping's insertion order becomes ``finder.find_relationships``'s
    # ``table_names``, whose upper-triangle enumeration fixes which side of each
    # pair is presented as "left", and an unordered scan made that Postgres
    # physical row order. Same intent as the column ordering below.
    # This is reproducibility ONLY — it is deliberately NOT how orientation is
    # decided, and must never be mistaken for a correctness guarantee. Note the
    # order DOES reach the judge: candidates are served from already-oriented
    # stored rows, so this fixes which side it sees as "left".
    stmt = (
        select(Table.table_id, Table.table_name, Table.duckdb_path)
        .where(Table.table_id.in_(table_ids))
        .order_by(Table.table_name)
    )
    table_rows = session.execute(stmt).all()
    table_info: dict[str, tuple[str, str]] = {
        table_id: (table_name, duckdb_path) for table_id, table_name, duckdb_path in table_rows
    }

    # Load column names + types per table (ordered so the candidate column list is stable)
    col_stmt = (
        select(Column.table_id, Column.column_name, Column.resolved_type)
        .where(Column.table_id.in_(table_ids))
        .order_by(Column.column_position)
    )
    column_types_by_table: dict[str, dict[str, str | None]] = {tid: {} for tid in table_ids}
    for table_id, column_name, resolved_type in session.execute(col_stmt).all():
        column_types_by_table[table_id][column_name] = resolved_type

    return {
        table_name: (
            duckdb_path,
            list(column_types_by_table.get(table_id, {}).keys()),
            column_types_by_table.get(table_id, {}),
        )
        for table_id, (table_name, duckdb_path) in table_info.items()
    }
