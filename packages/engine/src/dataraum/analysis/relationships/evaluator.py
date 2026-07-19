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
from dataraum.core.logging import get_logger

logger = get_logger(__name__)


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
    - left_orphan_count: table1 values with no match
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
        left_orphan_count = left_result[0] - left_result[1]
    else:
        left_ri = 0.0
        left_orphan_count = 0

    # Right referential integrity: % of table2 values that are referenced.
    #
    # NOT the mirror of ``left_referential_integrity``: this one is
    # DISTINCT-weighted (what share of the to-side's keys is used — coverage),
    # while the left one is ROW-weighted (what share of the from-side's rows
    # resolves). The two answer different questions, so exchanging their names
    # on an endpoint flip yields a number that is not the metric it then claims
    # to be — measured on a real pair: 60.0 stored where the direction's own
    # row-weighted RI is 75.0, which ``relationship_entropy`` turns into a 0.40
    # score instead of 0.25. Making the flip exact means measuring BOTH
    # directions the same way and giving coverage its own name; that changes
    # what the judge is shown, so it is a lead decision, not a drive-by
    # (DAT-725 parked).
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
        left_orphan_count=left_orphan_count,
        cardinality_verified=cardinality_verified,
    )


def compute_actual_cardinality(
    table1_path: str,
    table2_path: str,
    col1: str,
    col2: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> str | None:
    """Compute actual single-column join cardinality from data.

    Thin wrapper over :func:`compute_composite_cardinality` for the single-pair
    case (``t1.col1 = t2.col2``).

    Returns:
        Cardinality string ("one-to-one", "one-to-many", "many-to-one",
        "many-to-many") or None if inconclusive.
    """
    return compute_composite_cardinality(table1_path, table2_path, [(col1, col2)], duckdb_conn)


def compute_composite_cardinality(
    table1_path: str,
    table2_path: str,
    column_pairs: list[tuple[str, str]],
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> str | None:
    """Compute join cardinality for a (possibly multi-column) key, from data.

    Generalizes :func:`compute_actual_cardinality` to a COMPOSITE key: the join
    matches on every ``(t1_col, t2_col)`` pair (``t1.a = t2.b AND t1.c = t2.d``),
    and the key on each side is the column TUPLE. This is the math that lets a
    single-column many-to-many fan-out be *rescued* — adding a scoping column to
    the key can collapse it to many-to-one (DAT-277).

    Checks multiplicity in both directions:
    - For each distinct t1 key-tuple, how many t2 rows match?
    - For each distinct t2 key-tuple, how many t1 rows match?

    Args:
        table1_path: DuckDB path to the first table.
        table2_path: DuckDB path to the second table.
        column_pairs: ordered ``(table1_column, table2_column)`` key components.
            A single pair reproduces ``compute_actual_cardinality``.
        duckdb_conn: DuckDB connection.

    Returns:
        Cardinality string ("one-to-one", "one-to-many", "many-to-one",
        "many-to-many") or None if inconclusive / empty key.
    """
    if not column_pairs:
        return None

    t1_cols = [a for a, _b in column_pairs]
    t2_cols = [b for _a, b in column_pairs]
    on_clause = " AND ".join(f't1."{a}" = t2."{b}"' for a, b in column_pairs)
    t1_not_null = " AND ".join(f'"{a}" IS NOT NULL' for a in t1_cols)
    t2_not_null = " AND ".join(f'"{b}" IS NOT NULL' for b in t2_cols)
    t1_distinct = ", ".join(f'"{a}"' for a in t1_cols)
    t2_distinct = ", ".join(f'"{b}"' for b in t2_cols)
    t1_group = ", ".join(f't1."{a}"' for a in t1_cols)
    t2_group = ", ".join(f't2."{b}"' for b in t2_cols)

    try:
        # For each distinct t1 key-tuple, how many t2 rows match? max <= 1 → unique.
        t1_to_t2_query = f"""
            SELECT MAX(match_count) <= 1
            FROM (
                SELECT COUNT(*) as match_count
                FROM (SELECT DISTINCT {t1_distinct} FROM {table1_path} WHERE {t1_not_null}) t1
                INNER JOIN {table2_path} t2 ON {on_clause}
                GROUP BY {t1_group}
            )
        """
        result1 = duckdb_conn.execute(t1_to_t2_query).fetchone()
        each_t1_has_one_t2 = bool(result1[0]) if result1 and result1[0] is not None else None

        # For each distinct t2 key-tuple, how many t1 rows match? max <= 1 → unique.
        t2_to_t1_query = f"""
            SELECT MAX(match_count) <= 1
            FROM (
                SELECT COUNT(*) as match_count
                FROM (SELECT DISTINCT {t2_distinct} FROM {table2_path} WHERE {t2_not_null}) t2
                INNER JOIN {table1_path} t1 ON {on_clause}
                GROUP BY {t2_group}
            )
        """
        result2 = duckdb_conn.execute(t2_to_t1_query).fetchone()
        each_t2_has_one_t1 = bool(result2[0]) if result2 and result2[0] is not None else None

        if each_t1_has_one_t2 is None or each_t2_has_one_t1 is None:
            return None

        # "one-to-many" means: each t1 value can match many t2 rows,
        #   but each t2 value matches at most one t1 row.
        if each_t1_has_one_t2 and each_t2_has_one_t1:
            return "one-to-one"
        elif not each_t1_has_one_t2 and each_t2_has_one_t1:
            return "one-to-many"
        elif each_t1_has_one_t2 and not each_t2_has_one_t1:
            return "many-to-one"
        else:
            return "many-to-many"

    except Exception as e:
        logger.warning(
            "cardinality_computation_failed",
            column_pairs=column_pairs,
            error=str(e),
        )
        return None


def compute_join_coverage(
    table1_path: str,
    table2_path: str,
    column_pairs: list[tuple[str, str]],
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> float | None:
    """Share of table1 rows (non-NULL on every key component) with a table2 match.

    Cardinality proves match MULTIPLICITY; this proves match COVERAGE — the two
    are independent, and a key can be perfectly many-to-one while matching
    almost nothing (DAT-695: a lookalike dimension whose values barely overlap
    the fact's verified many-to-one at 0.3% coverage). Serves as EVIDENCE for
    the LLM judges (relationship confirmation, enrichment), never a gate.

    Returns:
        Matched fraction in [0, 1], or ``None`` when table1 has no non-NULL
        key rows or the probe fails.
    """
    if not column_pairs:
        return None
    on_clause = " AND ".join(f't1."{a}" = t2."{b}"' for a, b in column_pairs)
    t1_not_null = " AND ".join(f't1."{a}" IS NOT NULL' for a, _b in column_pairs)
    try:
        row = duckdb_conn.execute(
            f"""
            SELECT
                COUNT(*) FILTER (WHERE EXISTS (
                    SELECT 1 FROM {table2_path} t2 WHERE {on_clause})) AS matched,
                COUNT(*) AS total
            FROM {table1_path} t1
            WHERE {t1_not_null}
            """
        ).fetchone()
        if not row or not row[1]:
            return None
        return float(row[0]) / float(row[1])
    except Exception as e:
        logger.warning("join_coverage_failed", column_pairs=column_pairs, error=str(e))
        return None


def _verify_cardinality(
    detected_cardinality: str,
    table1_path: str,
    table2_path: str,
    col1: str,
    col2: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> bool | None:
    """Verify if detected cardinality matches actual join behavior.

    Returns:
        True if cardinality matches, False if mismatch, None if inconclusive
    """
    actual = compute_actual_cardinality(table1_path, table2_path, col1, col2, duckdb_conn)
    if actual is None:
        return None
    return detected_cardinality == actual


def evaluate_relationship_candidate(
    candidate: RelationshipCandidate,
    table1_path: str,
    table2_path: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> RelationshipCandidate:
    """Evaluate a relationship candidate with quality metrics.

    Computes:
    - left_join_success_rate: % of table1 rows that match in table2
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
            left_join_success_rate=None,
            introduces_duplicates=None,
        )

    # Use best join (highest join_confidence) for relationship metrics
    best_join = max(evaluated_joins, key=lambda j: j.join_confidence)

    # Join success rate = left referential integrity of best join
    left_join_success_rate = best_join.left_referential_integrity

    # Check for duplicate introduction (fan trap)
    introduces_duplicates = compute_introduces_duplicates(
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
        left_join_success_rate=left_join_success_rate,
        introduces_duplicates=introduces_duplicates,
    )


def compute_introduces_duplicates(
    table1_path: str,
    table2_path: str,
    col1: str,
    col2: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> bool:
    """Check if joining introduces duplicate rows (fan trap).

    A fan trap occurs when joining through a relationship causes
    row multiplication, which can inflate aggregates. Public (used by the
    structural evaluator AND the LLM-synthesis processor) so synthesized
    relationships carry the same empirical fan-trap signal as structural ones.

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
    with duckdb_conn.cursor() as cursor:
        return evaluate_relationship_candidate(candidate, table1_path, table2_path, cursor)


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
        from_table: Fully-qualified DuckDB path to source table
            (e.g., ``lake.typed."orders"`` — narrow, DAT-639).
        from_column: Column name in source table
        to_table: Fully-qualified DuckDB path to target table
            (e.g., ``lake.typed."customers"`` — narrow, DAT-639).
        to_column: Column name in target table
        duckdb_conn: DuckDB connection
        cardinality: Optional cardinality to verify (e.g., "one-to-many")

    Returns:
        Dict with RI metrics:
        - left_referential_integrity: % of from_table ROWS with a match
          (row-weighted — duplicate rows of an orphan value each count)
        - left_value_containment: % of from_table's DISTINCT values with a
          match (distinct-weighted — the containment of the from-side VALUE
          SET in the to side, insensitive to row duplication). Nothing orients
          on it: it is EVIDENCE, and no consumer currently carries it as far
          as the judge (DAT-725).
        - right_referential_integrity: % of to_table values referenced
        - left_orphan_count: from_table ROWS with no match — a from-side
          measurement, hence the prefix. It must flip with the pair; see
          ``db_models.swap_directional_evidence``.
        - cardinality_verified: whether cardinality matches (if provided)
    """
    # Left referential integrity — row-weighted AND distinct-weighted in one
    # scan. The two diverge exactly when the from side carries DUPLICATE rows
    # of unmatched (orphan) values: row-weighted then under-states containment
    # (a biased estimator of the value-set relation — same lesson as the
    # DAT-794 uniqueness ratio), while COUNT(DISTINCT) is immune to the LEFT
    # JOIN's row multiplication and to orphan duplication alike.
    left_query = f'''
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE t2."{to_column}" IS NOT NULL) as matched,
            COUNT(DISTINCT t1."{from_column}") as distinct_total,
            COUNT(DISTINCT t1."{from_column}") FILTER (WHERE t2."{to_column}" IS NOT NULL)
                as distinct_matched
        FROM {from_table} t1
        LEFT JOIN {to_table} t2 ON t1."{from_column}" = t2."{to_column}"
        WHERE t1."{from_column}" IS NOT NULL
    '''
    left_total_count = None
    left_containment = None
    try:
        left_result = duckdb_conn.execute(left_query).fetchone()
        if left_result and left_result[0] > 0:
            left_ri = (left_result[1] / left_result[0]) * 100
            left_orphan_count = left_result[0] - left_result[1]
            left_total_count = left_result[0]
            if left_result[2] > 0:
                left_containment = (left_result[3] / left_result[2]) * 100
        else:
            left_ri = 0.0
            left_orphan_count = 0
            left_total_count = 0
    except Exception:
        left_ri = None
        left_orphan_count = None

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
        "left_value_containment": (
            round(left_containment, 2) if left_containment is not None else None
        ),
        "right_referential_integrity": round(right_ri, 2) if right_ri is not None else None,
        "left_orphan_count": left_orphan_count,
        "left_total_count": left_total_count,
        "cardinality_verified": cardinality_verified,
    }


def evaluate_candidates(
    candidates: list[RelationshipCandidate],
    table_paths: dict[str, str],
    duckdb_conn: duckdb.DuckDBPyConnection,
    max_workers: int = 8,
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
