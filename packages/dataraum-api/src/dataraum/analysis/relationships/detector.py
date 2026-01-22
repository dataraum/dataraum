"""Entry point for relationship detection."""

import time
from datetime import UTC, datetime
from uuid import uuid4

import duckdb
import pandas as pd
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
from dataraum.analysis.semantic.utils import load_column_mappings, load_table_mappings
from dataraum.core.models.base import Result
from dataraum.storage import Table


def detect_relationships(
    table_ids: list[str],
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
    min_confidence: float = 0.3,
    sample_percent: float = 10.0,
    evaluate: bool = True,
) -> Result[RelationshipDetectionResult]:
    """Detect relationships between tables and store as candidates.

    Uses value overlap (Jaccard/containment) to find joinable column pairs.
    Candidates are stored for semantic analysis to confirm/reject.

    Args:
        table_ids: List of table IDs to analyze
        duckdb_conn: DuckDB connection
        session: SQLAlchemy async session
        min_confidence: Minimum join_confidence threshold (default 0.3)
        sample_percent: Percentage of rows to sample for uniqueness calculation
        evaluate: Whether to evaluate candidates with quality metrics (default True)

    Returns:
        Result containing RelationshipDetectionResult
    """
    start_time = time.time()

    try:
        # Load tables with paths and sampled data
        tables_data = _load_tables(session, duckdb_conn, table_ids, sample_percent)

        if len(tables_data) < 2:
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
            table_paths = {name: path for name, (path, _df) in tables_data.items()}
            candidates = evaluate_candidates(candidates, table_paths, duckdb_conn)

        # Store candidates in database
        _store_candidates(session, table_ids, candidates)

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
) -> None:
    """Store relationship candidates in the database.

    Each join candidate is stored as a separate relationship with
    detection_method='candidate' to distinguish from confirmed relationships.
    """
    # Load mappings
    column_map = load_column_mappings(session, table_ids)
    table_map = load_table_mappings(session, table_ids)

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
            if jc.orphan_count is not None:
                evidence["orphan_count"] = jc.orphan_count
            if jc.cardinality_verified is not None:
                evidence["cardinality_verified"] = jc.cardinality_verified

            # Add relationship-level evaluation metrics
            if candidate.join_success_rate is not None:
                evidence["join_success_rate"] = candidate.join_success_rate
            if candidate.introduces_duplicates is not None:
                evidence["introduces_duplicates"] = candidate.introduces_duplicates

            db_rel = RelationshipDB(
                relationship_id=str(uuid4()),
                from_table_id=table1_id,
                from_column_id=col1_id,
                to_table_id=table2_id,
                to_column_id=col2_id,
                relationship_type="candidate",
                cardinality=jc.cardinality,
                confidence=jc.join_confidence,
                detection_method="candidate",
                evidence=evidence,
                is_confirmed=False,
            )
            session.add(db_rel)


def _load_tables(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_ids: list[str],
    sample_percent: float,
) -> dict[str, tuple[str, pd.DataFrame]]:
    """Load table data from DuckDB.

    Returns duckdb_path for join detection via SQL,
    plus sampled DataFrame for uniqueness calculation.
    """
    stmt = select(Table.table_name, Table.duckdb_path).where(Table.table_id.in_(table_ids))
    result = session.execute(stmt)

    tables_data: dict[str, tuple[str, pd.DataFrame]] = {}
    for table_name, duckdb_path in result.all():
        try:
            # Sample for uniqueness calculation (join detection uses full data via SQL)
            df = duckdb_conn.execute(
                f"SELECT * FROM {duckdb_path} USING SAMPLE {sample_percent}%"
            ).df()
            tables_data[table_name] = (duckdb_path, df)
        except Exception:
            continue

    return tables_data
