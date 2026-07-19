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
from typing import NamedTuple

import duckdb

from dataraum.analysis.relationships.models import (
    JoinCandidate,
    RelationshipCandidate,
)
from dataraum.core.logging import get_logger

logger = get_logger(__name__)


class DirectionMetrics(NamedTuple):
    """How well one side's key resolves against the other's, one direction only.

    Both weightings of the same question, because they answer different ones
    and the codebase needs both:

    - ``referential_integrity`` — ROW-weighted: what share of this side's ROWS
      resolve. This is the data-quality number; a value repeated on a thousand
      orphan rows is a thousand broken rows.
    - ``key_coverage`` — DISTINCT-weighted: what share of this side's VALUE SET
      appears on the other. This is the set-containment number, immune to row
      duplication, and it is what "is one side's key set inside the other's"
      actually means.
    - ``orphan_count`` / ``total_count`` — the row counts behind the first.
    """

    referential_integrity: float
    key_coverage: float
    orphan_count: int
    total_count: int


def _measure_direction(
    from_table: str,
    from_column: str,
    to_table: str,
    to_column: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> DirectionMetrics | None:
    """Measure ``from`` → ``to`` resolution. THE one measurement, run per side.

    Called twice with the arguments swapped, so ``left_*`` and ``right_*`` are
    genuinely the same metric on opposite endpoints. That symmetry is what makes
    ``db_models.swap_directional_evidence`` a correct RELABELING: before DAT-725
    the left number was row-weighted and the right one distinct-weighted, so a
    flip renamed a coverage figure into an RI slot and every reader downstream
    believed it — measured, 60.0 stored where that direction's own RI is 75.0,
    scored by ``relationship_entropy`` as 0.40 instead of 0.25.

    A correlated ``EXISTS``, never ``LEFT JOIN`` + ``COUNT(*)``. The old form
    multiplied this side's rows whenever the other side held duplicates of a
    key, so the "row-weighted" ratio was taken over join output rows rather than
    over this table's rows. Three properties of this exact form are load-bearing,
    each verified by a test in ``test_direction_metrics.py``:

    - ``EXISTS`` with ``=``, not ``IN``. DuckDB's ``IN``/``ANY`` refuses
      cross-family comparison where a join's ``=`` coerces, so an ``IN`` form
      raised on a castable VARCHAR key against a numeric one — killing the
      structural phase outright, and on the judge path silently returning every
      metric ``None`` (DAT-725 review).
    - The subquery table is ALIASED and its column qualified. Unqualified, a
      ``to_column`` missing from ``to_table`` binds to the outer row instead of
      failing: a schema-drift error turned into fabricated "100% broken"
      evidence, which the orphan-rate detector then scores at 1.0.
    - ``matched`` is computed ONCE per row in the inner select. DuckDB does not
      common-subexpression-eliminate two identical semi-join predicates — it
      plans two MARK joins — so folding them costs ~25% of this function.

    Returns ``None`` when there is nothing to measure (no non-NULL rows on this
    side). Absence is ignorance: a fabricated 0.0 reads as total breakage and
    scores 1.0 on a relationship with zero broken rows.
    """
    query = f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(*) FILTER (WHERE matched) AS matched_rows,
            COUNT(DISTINCT k) AS total_keys,
            COUNT(DISTINCT k) FILTER (WHERE matched) AS matched_keys
        FROM (
            SELECT
                t."{from_column}" AS k,
                EXISTS (
                    SELECT 1 FROM {to_table} probe
                    WHERE probe."{to_column}" = t."{from_column}"
                ) AS matched
            FROM {from_table} t
            WHERE t."{from_column}" IS NOT NULL
        )
    """
    row = duckdb_conn.execute(query).fetchone()
    if not row or not row[0]:
        return None
    total_rows, matched_rows, total_keys, matched_keys = row
    return DirectionMetrics(
        referential_integrity=round((matched_rows / total_rows) * 100, 2),
        key_coverage=round((matched_keys / total_keys) * 100, 2),
        orphan_count=total_rows - matched_rows,
        total_count=total_rows,
    )


def evaluate_join_candidate(
    join_candidate: JoinCandidate,
    table1_path: str,
    table2_path: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> JoinCandidate:
    """Evaluate a single join candidate with quality metrics.

    Measures BOTH directions with the same function, so every ``left_*`` metric
    has a real ``right_*`` mirror and an endpoint flip is exact relabeling:

    - left/right_referential_integrity: % of that side's ROWS that resolve
    - left/right_key_coverage: % of that side's DISTINCT values on the other
    - left/right_orphan_count: that side's rows that do not resolve
    - cardinality_verified: whether detected cardinality matches actual

    A side with no non-NULL rows leaves its metrics ``None`` — nothing was
    measurable there, and a 0.0 would read as total breakage.

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

    left = _measure_direction(table1_path, col1, table2_path, col2, duckdb_conn)
    right = _measure_direction(table2_path, col2, table1_path, col1, duckdb_conn)

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
        left_referential_integrity=left.referential_integrity if left else None,
        right_referential_integrity=right.referential_integrity if right else None,
        left_key_coverage=left.key_coverage if left else None,
        right_key_coverage=right.key_coverage if right else None,
        left_orphan_count=left.orphan_count if left else None,
        right_orphan_count=right.orphan_count if right else None,
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
            introduces_duplicates=None,
        )

    # Use best join (highest join_confidence) for relationship metrics
    best_join = max(evaluated_joins, key=lambda j: j.join_confidence)

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
        Dict with the same per-side metrics :func:`evaluate_join_candidate`
        produces, from the same :func:`_measure_direction` — both directions
        measured identically, so an endpoint flip is exact relabeling:
        - left/right_referential_integrity: % of that side's ROWS that resolve
        - left/right_key_coverage: % of that side's DISTINCT values present on
          the other — the value-set containment, immune to row duplication
        - left/right_orphan_count, left/right_total_count: the row counts behind
          the referential-integrity ratio
        - cardinality_verified: whether cardinality matches (if provided)

        A measurement that raises leaves its whole side ``None``: absence is
        ignorance, and a fabricated 0.0 would read as total breakage.
    """
    try:
        left: DirectionMetrics | None = _measure_direction(
            from_table, from_column, to_table, to_column, duckdb_conn
        )
    except Exception:
        left = None
    try:
        right: DirectionMetrics | None = _measure_direction(
            to_table, to_column, from_table, from_column, duckdb_conn
        )
    except Exception:
        right = None

    # Cardinality verification (if requested)
    cardinality_verified = None
    if cardinality:
        cardinality_verified = _verify_cardinality(
            cardinality, from_table, to_table, from_column, to_column, duckdb_conn
        )

    return {
        "left_referential_integrity": left.referential_integrity if left else None,
        "right_referential_integrity": right.referential_integrity if right else None,
        "left_key_coverage": left.key_coverage if left else None,
        "right_key_coverage": right.key_coverage if right else None,
        "left_orphan_count": left.orphan_count if left else None,
        "right_orphan_count": right.orphan_count if right else None,
        "left_total_count": left.total_count if left else None,
        "right_total_count": right.total_count if right else None,
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
