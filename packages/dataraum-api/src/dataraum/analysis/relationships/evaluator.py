"""Relationship evaluation - quality metrics for relationship candidates.

Evaluates relationship candidates BEFORE semantic agent confirmation:
- Per-JoinCandidate: referential integrity, cardinality verification
- Per-RelationshipCandidate: join success rate, duplicate detection

This module enriches candidates with quality metrics that help the semantic
agent make better decisions and provide evidence for relationship quality.

Uses parallel processing for large relationship sets to speed up evaluation.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import duckdb

from dataraum.analysis.relationships.models import (
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

    # Return updated candidate, preserving original values
    return JoinCandidate(
        column1=join_candidate.column1,
        column2=join_candidate.column2,
        join_confidence=join_candidate.join_confidence,
        cardinality=join_candidate.cardinality,
        left_uniqueness=join_candidate.left_uniqueness,
        right_uniqueness=join_candidate.right_uniqueness,
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
) -> bool | None:
    """Verify if detected cardinality matches actual join behavior.

    Cardinality is about ROW relationships in joins:
    - one-to-one: Each t1 row matches at most one t2 row
    - one-to-many: Each t1 row can match multiple t2 rows
    - many-to-one: Multiple t1 rows can match one t2 row
    - many-to-many: Multiple t1 rows can match multiple t2 rows

    We check by counting how many t2 ROWS match each t1 value (and vice versa).

    Args:
        detected_cardinality: The cardinality detected/declared
        table1_path: DuckDB path to first table
        table2_path: DuckDB path to second table
        col1: Column name in table1
        col2: Column name in table2
        duckdb_conn: DuckDB connection

    Returns:
        True if cardinality matches, False if mismatch, None if inconclusive
    """
    try:
        # Check if each distinct col1 value matches at most one ROW in t2
        # (Determines "one" vs "many" on the right side of relationship)
        col1_max_matches_query = f"""
            SELECT MAX(match_count) <= 1
            FROM (
                SELECT t1."{col1}", COUNT(*) as match_count
                FROM (SELECT DISTINCT "{col1}" FROM {table1_path} WHERE "{col1}" IS NOT NULL) t1
                INNER JOIN {table2_path} t2 ON t1."{col1}" = t2."{col2}"
                GROUP BY t1."{col1}"
            )
        """
        result1 = duckdb_conn.execute(col1_max_matches_query).fetchone()
        right_side_is_one = bool(result1[0]) if result1 and result1[0] is not None else None

        # Check if each distinct col2 value matches at most one ROW in t1
        # (Determines "one" vs "many" on the left side of relationship)
        col2_max_matches_query = f"""
            SELECT MAX(match_count) <= 1
            FROM (
                SELECT t2."{col2}", COUNT(*) as match_count
                FROM (SELECT DISTINCT "{col2}" FROM {table2_path} WHERE "{col2}" IS NOT NULL) t2
                INNER JOIN {table1_path} t1 ON t2."{col2}" = t1."{col1}"
                GROUP BY t2."{col2}"
            )
        """
        result2 = duckdb_conn.execute(col2_max_matches_query).fetchone()
        left_side_is_one = bool(result2[0]) if result2 and result2[0] is not None else None

        # If we couldn't determine either side, return None (inconclusive)
        if right_side_is_one is None or left_side_is_one is None:
            return None

        # Determine actual cardinality from join behavior
        # From t1's perspective: t1 -> t2
        # - left_side_is_one: each t1 value matches at most one t2 row (t1 side is "one")
        # - right_side_is_one: each t2 value matches at most one t1 row (t2 side is "one")
        if left_side_is_one and right_side_is_one:
            actual = "one-to-one"
        elif left_side_is_one and not right_side_is_one:
            # Each t1 value has one match, but t2 values have multiple matches
            # From t1->t2: many t1 rows -> one t2 row = many-to-one
            actual = "many-to-one"
        elif not left_side_is_one and right_side_is_one:
            # Each t1 value has multiple matches, but t2 values have one match
            # From t1->t2: one t1 row -> many t2 rows = one-to-many
            actual = "one-to-many"
        else:
            actual = "many-to-many"

        return detected_cardinality == actual

    except Exception:
        # If verification fails, return None (inconclusive) rather than False
        return None


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
            join_candidates=evaluated_joins,
            join_success_rate=None,
            introduces_duplicates=None,
        )

    # Use best join (highest join_confidence) for relationship metrics
    best_join = max(evaluated_joins, key=lambda j: j.join_confidence)

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


def _evaluate_relationship_candidate_parallel(
    duckdb_conn: duckdb.DuckDBPyConnection,
    candidate: RelationshipCandidate,
    table1_path: str,
    table2_path: str,
) -> RelationshipCandidate:
    """Evaluate a relationship candidate in a worker thread.

    Runs in its own thread using a cursor from the shared DuckDB connection.
    DuckDB cursors are thread-safe for read operations.
    """
    cursor = duckdb_conn.cursor()
    try:
        return evaluate_relationship_candidate(candidate, table1_path, table2_path, cursor)
    finally:
        cursor.close()


def compute_ri_metrics(
    from_table: str,
    from_column: str,
    to_table: str,
    to_column: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    cardinality: str | None = None,
) -> dict[str, float | int | bool | None]:
    """Compute referential integrity metrics for a relationship.

    Standalone function for computing RI metrics without needing
    JoinCandidate objects. Useful for evaluating LLM-discovered
    relationships that weren't in the original candidate set.

    Args:
        from_table: DuckDB path to source table (e.g., "typed_orders")
        from_column: Column name in source table
        to_table: DuckDB path to target table (e.g., "typed_customers")
        to_column: Column name in target table
        duckdb_conn: DuckDB connection
        cardinality: Optional cardinality to verify (e.g., "one-to-many")

    Returns:
        Dict with RI metrics:
        - left_referential_integrity: % of from_table values with match
        - right_referential_integrity: % of to_table values referenced
        - orphan_count: from_table values with no match
        - cardinality_verified: whether cardinality matches (if provided)
    """
    # Left referential integrity
    left_query = f'''
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE t2."{to_column}" IS NOT NULL) as matched
        FROM {from_table} t1
        LEFT JOIN {to_table} t2 ON t1."{from_column}" = t2."{to_column}"
        WHERE t1."{from_column}" IS NOT NULL
    '''
    try:
        left_result = duckdb_conn.execute(left_query).fetchone()
        if left_result and left_result[0] > 0:
            left_ri = (left_result[1] / left_result[0]) * 100
            orphan_count = left_result[0] - left_result[1]
        else:
            left_ri = 0.0
            orphan_count = 0
    except Exception:
        left_ri = None
        orphan_count = None

    # Right referential integrity
    right_query = f'''
        SELECT
            COUNT(DISTINCT t2."{to_column}") as total_pk,
            COUNT(DISTINCT t1."{from_column}") as referenced_pk
        FROM {to_table} t2
        LEFT JOIN {from_table} t1 ON t2."{to_column}" = t1."{from_column}"
        WHERE t2."{to_column}" IS NOT NULL
    '''
    try:
        right_result = duckdb_conn.execute(right_query).fetchone()
        if right_result and right_result[0] > 0:
            right_ri = (right_result[1] / right_result[0]) * 100
        else:
            right_ri = 0.0
    except Exception:
        right_ri = None

    # Cardinality verification (if requested)
    cardinality_verified = None
    if cardinality:
        cardinality_verified = _verify_cardinality(
            cardinality, from_table, to_table, from_column, to_column, duckdb_conn
        )

    return {
        "left_referential_integrity": round(left_ri, 2) if left_ri is not None else None,
        "right_referential_integrity": round(right_ri, 2) if right_ri is not None else None,
        "orphan_count": orphan_count,
        "cardinality_verified": cardinality_verified,
    }


def evaluate_candidates(
    candidates: list[RelationshipCandidate],
    table_paths: dict[str, str],
    duckdb_conn: duckdb.DuckDBPyConnection,
    max_workers: int = 4,
) -> list[RelationshipCandidate]:
    """Evaluate all relationship candidates with quality metrics.

    Uses parallel processing for file-based DBs to speed up evaluation.

    Args:
        candidates: List of relationship candidates to evaluate
        table_paths: Mapping of table names to DuckDB paths
        duckdb_conn: DuckDB connection
        max_workers: Maximum parallel workers

    Returns:
        List of RelationshipCandidate with evaluation metrics populated
    """
    # Separate candidates into evaluable and non-evaluable
    evaluable = []
    non_evaluable = []
    for candidate in candidates:
        table1_path = table_paths.get(candidate.table1)
        table2_path = table_paths.get(candidate.table2)
        if table1_path and table2_path:
            evaluable.append((candidate, table1_path, table2_path))
        else:
            non_evaluable.append(candidate)

    evaluated = []

    # Use parallel processing with cursors from shared connection
    # DuckDB cursors are thread-safe for read operations
    if evaluable:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [
                pool.submit(
                    _evaluate_relationship_candidate_parallel,
                    duckdb_conn,
                    candidate,
                    table1_path,
                    table2_path,
                )
                for candidate, table1_path, table2_path in evaluable
            ]

            for future in futures:
                evaluated.append(future.result())

    # Add non-evaluable candidates (missing table paths)
    evaluated.extend(non_evaluable)

    return evaluated
