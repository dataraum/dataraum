"""Relationship evaluation - quality metrics for relationship candidates.

Evaluates relationship candidates BEFORE semantic agent confirmation:
- Per-JoinCandidate: referential integrity, cardinality verification
- Per-RelationshipCandidate: join success rate, duplicate detection

This module enriches candidates with quality metrics that help the semantic
agent make better decisions and provide evidence for relationship quality.
"""

from __future__ import annotations

import duckdb

from dataraum_context.analysis.relationships.models import (
    JoinCandidate,
    RelationshipCandidate,
)


def evaluate_join_candidate(
    join_candidate: JoinCandidate,
    table1_path: str,
    table2_path: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> JoinCandidate:
    """Evaluate a single join candidate with quality metrics.

    Computes:
    - left_referential_integrity: % of table1 values with match in table2
    - right_referential_integrity: % of table2 values referenced by table1
    - orphan_count: table1 values with no match
    - cardinality_verified: whether detected cardinality matches actual

    Args:
        join_candidate: The join candidate to evaluate
        table1_path: DuckDB path to first table
        table2_path: DuckDB path to second table
        duckdb_conn: DuckDB connection

    Returns:
        JoinCandidate with evaluation metrics populated
    """
    col1 = join_candidate.column1
    col2 = join_candidate.column2

    # Left referential integrity: % of table1 values with match in table2
    left_query = f"""
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE t2."{col2}" IS NOT NULL) as matched
        FROM {table1_path} t1
        LEFT JOIN {table2_path} t2 ON t1."{col1}" = t2."{col2}"
        WHERE t1."{col1}" IS NOT NULL
    """
    left_result = duckdb_conn.execute(left_query).fetchone()
    if left_result and left_result[0] > 0:
        left_ri = (left_result[1] / left_result[0]) * 100
        orphan_count = left_result[0] - left_result[1]
    else:
        left_ri = 0.0
        orphan_count = 0

    # Right referential integrity: % of table2 values that are referenced
    right_query = f"""
        SELECT
            COUNT(DISTINCT t2."{col2}") as total_pk,
            COUNT(DISTINCT t1."{col1}") as referenced_pk
        FROM {table2_path} t2
        LEFT JOIN {table1_path} t1 ON t2."{col2}" = t1."{col1}"
        WHERE t2."{col2}" IS NOT NULL
    """
    right_result = duckdb_conn.execute(right_query).fetchone()
    if right_result and right_result[0] > 0:
        right_ri = (right_result[1] / right_result[0]) * 100
    else:
        right_ri = 0.0

    # Cardinality verification
    cardinality_verified = _verify_cardinality(
        join_candidate.cardinality,
        table1_path,
        table2_path,
        col1,
        col2,
        duckdb_conn,
    )

    # Return updated candidate, preserving topology and join confidence
    return JoinCandidate(
        column1=join_candidate.column1,
        column2=join_candidate.column2,
        confidence=join_candidate.confidence,
        cardinality=join_candidate.cardinality,
        topology_similarity=join_candidate.topology_similarity,
        join_confidence=join_candidate.join_confidence,
        left_referential_integrity=round(left_ri, 2),
        right_referential_integrity=round(right_ri, 2),
        orphan_count=orphan_count,
        cardinality_verified=cardinality_verified,
    )


def _verify_cardinality(
    detected_cardinality: str,
    table1_path: str,
    table2_path: str,
    col1: str,
    col2: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> bool:
    """Verify if detected cardinality matches actual data.

    Args:
        detected_cardinality: The cardinality detected by join detection
        table1_path: DuckDB path to first table
        table2_path: DuckDB path to second table
        col1: Column name in table1
        col2: Column name in table2
        duckdb_conn: DuckDB connection

    Returns:
        True if cardinality matches, False otherwise
    """
    # Check if col1 values are unique (one side of 1:1 or 1:N)
    col1_unique_query = f"""
        SELECT COUNT(*) = COUNT(DISTINCT "{col1}")
        FROM {table1_path}
        WHERE "{col1}" IS NOT NULL
    """
    col1_result = duckdb_conn.execute(col1_unique_query).fetchone()
    col1_is_unique = bool(col1_result[0]) if col1_result else False

    # Check if col2 values are unique (one side of 1:1 or N:1)
    col2_unique_query = f"""
        SELECT COUNT(*) = COUNT(DISTINCT "{col2}")
        FROM {table2_path}
        WHERE "{col2}" IS NOT NULL
    """
    col2_result = duckdb_conn.execute(col2_unique_query).fetchone()
    col2_is_unique = bool(col2_result[0]) if col2_result else False

    # Determine actual cardinality
    if col1_is_unique and col2_is_unique:
        actual = "one-to-one"
    elif col1_is_unique and not col2_is_unique:
        actual = "one-to-many"
    elif not col1_is_unique and col2_is_unique:
        actual = "many-to-one"
    else:
        actual = "many-to-many"

    return detected_cardinality == actual


def evaluate_relationship_candidate(
    candidate: RelationshipCandidate,
    table1_path: str,
    table2_path: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> RelationshipCandidate:
    """Evaluate a relationship candidate with quality metrics.

    Computes:
    - join_success_rate: % of table1 rows that match in table2
    - introduces_duplicates: whether join multiplies rows (fan trap)
    - Also evaluates all join candidates

    Args:
        candidate: The relationship candidate to evaluate
        table1_path: DuckDB path to first table
        table2_path: DuckDB path to second table
        duckdb_conn: DuckDB connection

    Returns:
        RelationshipCandidate with evaluation metrics populated
    """
    # Evaluate all join candidates
    evaluated_joins = []
    for jc in candidate.join_candidates:
        evaluated_jc = evaluate_join_candidate(jc, table1_path, table2_path, duckdb_conn)
        evaluated_joins.append(evaluated_jc)

    # Use the best join candidate for relationship-level metrics
    if not evaluated_joins:
        return RelationshipCandidate(
            table1=candidate.table1,
            table2=candidate.table2,
            confidence=candidate.confidence,
            relationship_type=candidate.relationship_type,
            join_candidates=evaluated_joins,
            join_success_rate=None,
            introduces_duplicates=None,
        )

    # Use best join (highest confidence) for relationship metrics
    best_join = max(evaluated_joins, key=lambda j: j.confidence)

    # Join success rate = left referential integrity of best join
    join_success_rate = best_join.left_referential_integrity

    # Check for duplicate introduction (fan trap)
    introduces_duplicates = _check_duplicate_introduction(
        table1_path,
        table2_path,
        best_join.column1,
        best_join.column2,
        duckdb_conn,
    )

    return RelationshipCandidate(
        table1=candidate.table1,
        table2=candidate.table2,
        confidence=candidate.confidence,
        relationship_type=candidate.relationship_type,
        join_candidates=evaluated_joins,
        join_success_rate=join_success_rate,
        introduces_duplicates=introduces_duplicates,
    )


def _check_duplicate_introduction(
    table1_path: str,
    table2_path: str,
    col1: str,
    col2: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> bool:
    """Check if joining introduces duplicate rows (fan trap).

    A fan trap occurs when joining through a relationship causes
    row multiplication, which can inflate aggregates.

    Args:
        table1_path: DuckDB path to first table
        table2_path: DuckDB path to second table
        col1: Join column in table1
        col2: Join column in table2
        duckdb_conn: DuckDB connection

    Returns:
        True if join introduces duplicates, False otherwise
    """
    query = f"""
        SELECT
            (SELECT COUNT(*) FROM {table1_path}) as before_count,
            (SELECT COUNT(*) FROM {table1_path} t1
             LEFT JOIN {table2_path} t2 ON t1."{col1}" = t2."{col2}") as after_count
    """
    result = duckdb_conn.execute(query).fetchone()
    if result:
        before, after = result
        return bool(after > before)
    return False


def evaluate_candidates(
    candidates: list[RelationshipCandidate],
    table_paths: dict[str, str],
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> list[RelationshipCandidate]:
    """Evaluate all relationship candidates with quality metrics.

    Args:
        candidates: List of relationship candidates to evaluate
        table_paths: Mapping of table names to DuckDB paths
        duckdb_conn: DuckDB connection

    Returns:
        List of RelationshipCandidate with evaluation metrics populated
    """
    evaluated = []
    for candidate in candidates:
        table1_path = table_paths.get(candidate.table1)
        table2_path = table_paths.get(candidate.table2)

        if not table1_path or not table2_path:
            # Can't evaluate without table paths, keep as-is
            evaluated.append(candidate)
            continue

        evaluated_candidate = evaluate_relationship_candidate(
            candidate, table1_path, table2_path, duckdb_conn
        )
        evaluated.append(evaluated_candidate)

    return evaluated
