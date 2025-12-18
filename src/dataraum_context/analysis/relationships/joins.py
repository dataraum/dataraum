"""Join column detection using value overlap."""

from typing import Any

import duckdb


def find_join_columns(
    conn: duckdb.DuckDBPyConnection,
    table1_path: str,
    table2_path: str,
    columns1: list[str],
    columns2: list[str],
    min_score: float = 0.3,
    max_distinct: int = 100000,
) -> list[dict[str, Any]]:
    """Find join columns using DuckDB for accurate distinct value comparison.

    Queries distinct values directly instead of sampling rows, which preserves
    the true value overlap for join detection. Returns ALL candidates above
    min_score for the LLM to evaluate.

    Returns dicts with:
    - column1, column2: column names
    - join_confidence: value overlap score (Jaccard/containment)
    - cardinality: one-to-one, one-to-many, etc.

    Note: topology_similarity is computed separately in finder.py where
    DataFrame access is available.
    """
    candidates = []

    for col1 in columns1:
        for col2 in columns2:
            score, cardinality = _compute_join_score(
                conn, table1_path, table2_path, col1, col2, max_distinct
            )
            if score > min_score:
                candidates.append(
                    {
                        "column1": col1,
                        "column2": col2,
                        "join_confidence": score,
                        "cardinality": cardinality,
                    }
                )

    def _get_conf(x: dict[str, Any]) -> float:
        c = x["join_confidence"]
        return float(c) if isinstance(c, (int, float)) else 0.0

    candidates.sort(key=_get_conf, reverse=True)
    return candidates


def _compute_join_score(
    conn: duckdb.DuckDBPyConnection,
    table1: str,
    table2: str,
    col1: str,
    col2: str,
    max_distinct: int,
) -> tuple[float, str]:
    """Compute join score using DuckDB SQL for efficient set operations."""
    try:
        # Get distinct counts, intersection, and uniqueness checks
        # Cardinality depends on whether values are unique (key columns), not count ratios
        result = conn.execute(f"""
            WITH
            vals1 AS (SELECT DISTINCT "{col1}" AS v FROM {table1} WHERE "{col1}" IS NOT NULL LIMIT {max_distinct}),
            vals2 AS (SELECT DISTINCT "{col2}" AS v FROM {table2} WHERE "{col2}" IS NOT NULL LIMIT {max_distinct}),
            stats AS (
                SELECT
                    (SELECT COUNT(*) FROM vals1) AS count1,
                    (SELECT COUNT(*) FROM vals2) AS count2,
                    (SELECT COUNT(*) FROM vals1 WHERE v IN (SELECT v FROM vals2)) AS intersection,
                    -- Check if col1 values are unique in table1 (potential key)
                    (SELECT COUNT(*) FROM {table1} WHERE "{col1}" IS NOT NULL) AS total1,
                    -- Check if col2 values are unique in table2 (potential key)
                    (SELECT COUNT(*) FROM {table2} WHERE "{col2}" IS NOT NULL) AS total2
            )
            SELECT count1, count2, intersection, total1, total2 FROM stats
        """).fetchone()

        if result is None:
            return 0.0, "unknown"

        count1, count2, intersection, total1, total2 = result

        if count1 == 0 or count2 == 0:
            return 0.0, "unknown"

        union = count1 + count2 - intersection
        jaccard = intersection / union if union > 0 else 0.0

        # Check containment
        containment = 1.0 if (intersection == count1 or intersection == count2) else 0.0

        score = max(jaccard, containment)

        # Determine cardinality based on uniqueness (not count ratios)
        # A column is "unique" if distinct_count == total_count (it's a key)
        col1_is_unique = count1 == total1
        col2_is_unique = count2 == total2

        if col1_is_unique and col2_is_unique:
            cardinality = "one-to-one"
        elif col1_is_unique and not col2_is_unique:
            cardinality = "one-to-many"
        elif not col1_is_unique and col2_is_unique:
            cardinality = "many-to-one"
        else:
            cardinality = "many-to-many"

        return score, cardinality

    except Exception:
        return 0.0, "unknown"
