"""Composite-key rescue of many-to-many fan-out edges (DAT-277).

The structural detector finds every value-overlapping column pair between two
tables as a separate ``JoinCandidate``. When the best (highest-confidence) pair is
**many-to-many**, joining on it alone fans out and silently over-counts
aggregates. The true key is often composite — a real FK plus one or more shared
scoping columns present in both tables. This module greedily fuses the
co-present candidate columns until the composite join collapses out of
many-to-many, or reports failure (a genuine many-to-many / bridge situation the
caller must flag and abstain on).

DATA decides, not names: a composite is accepted ONLY when the cardinality
actually collapses. A greedy miss returns ``None`` (abstain) — never a forced
join — so the worst case is a missed rescue, never a silent over-count.
"""

from __future__ import annotations

import duckdb

from dataraum.analysis.relationships.evaluator import compute_composite_cardinality
from dataraum.analysis.relationships.models import CompositeKey, RelationshipCandidate
from dataraum.core.logging import get_logger

logger = get_logger(__name__)

_MANY_TO_MANY = "many-to-many"


def rescue_fanout_to_composite(
    candidate: RelationshipCandidate,
    table1_path: str,
    table2_path: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    *,
    max_key_columns: int = 4,
) -> CompositeKey | None:
    """Greedily fuse co-present join columns to collapse a many-to-many fan-out.

    Anchors on the highest-confidence ``JoinCandidate`` (the real FK we want to keep
    scopable). If that pair alone is already not many-to-many there is nothing to
    rescue. Otherwise, repeatedly add the remaining co-present pair that most
    reduces the join's row multiplication, re-checking the composite cardinality
    after each add, until the join is no longer many-to-many (rescued) or the
    candidates / ``max_key_columns`` cap are exhausted (genuine many-to-many).

    Args:
        candidate: the table-pair candidate carrying all value-overlap join pairs.
        table1_path: DuckDB path to ``candidate.table1``.
        table2_path: DuckDB path to ``candidate.table2``.
        duckdb_conn: DuckDB connection.
        max_key_columns: cap on composite width (keeps the greedy search bounded).

    Returns:
        The rescuing :class:`CompositeKey` (cardinality never many-to-many), or
        ``None`` when nothing needs rescuing or no composite collapses the fan-out.
    """
    if len(candidate.join_candidates) < 2:
        return None  # need at least one scoping column to fuse with the anchor

    ordered = sorted(candidate.join_candidates, key=lambda j: j.join_confidence, reverse=True)
    anchor = ordered[0]
    chosen: list[tuple[str, str]] = [(anchor.column1, anchor.column2)]
    remaining: list[tuple[str, str]] = [(j.column1, j.column2) for j in ordered[1:]]

    card = compute_composite_cardinality(table1_path, table2_path, chosen, duckdb_conn)
    if card is None or card != _MANY_TO_MANY:
        return None  # the anchor alone is not a fan-out — nothing to rescue

    while remaining and len(chosen) < max_key_columns:
        # Add the co-present pair that most reduces the join's row multiplication —
        # the most-disambiguating scoping column.
        best_next = min(
            remaining,
            key=lambda p: _join_multiplication(table1_path, table2_path, [*chosen, p], duckdb_conn),
        )
        chosen.append(best_next)
        remaining.remove(best_next)

        card = compute_composite_cardinality(table1_path, table2_path, chosen, duckdb_conn)
        if card is not None and card != _MANY_TO_MANY:
            logger.info(
                "composite_key_rescued",
                table1=candidate.table1,
                table2=candidate.table2,
                key_columns=len(chosen),
                cardinality=card,
            )
            return CompositeKey(column_pairs=chosen, cardinality=card)

    return None  # genuine many-to-many — no composite of these columns collapses it


def _join_multiplication(
    table1_path: str,
    table2_path: str,
    column_pairs: list[tuple[str, str]],
    duckdb_conn: duckdb.DuckDBPyConnection,
) -> float:
    """Worst-direction match multiplicity for a composite join (1 = unique both ways).

    The largest number of matches a single key-tuple draws, taken over both join
    directions. Lower means closer to a clean one-to-one — the ranking signal for
    "which scoping column disambiguates most". Returns ``inf`` on error so a
    failing candidate is never preferred.
    """
    on_clause = " AND ".join(f't1."{a}" = t2."{b}"' for a, b in column_pairs)
    t1_cols = [a for a, _b in column_pairs]
    t2_cols = [b for _a, b in column_pairs]
    t1_not_null = " AND ".join(f'"{a}" IS NOT NULL' for a in t1_cols)
    t2_not_null = " AND ".join(f'"{b}" IS NOT NULL' for b in t2_cols)
    t1_group = ", ".join(f't1."{a}"' for a in t1_cols)
    t2_group = ", ".join(f't2."{b}"' for b in t2_cols)
    t1_distinct = ", ".join(f'"{a}"' for a in t1_cols)
    t2_distinct = ", ".join(f'"{b}"' for b in t2_cols)

    try:
        q = f"""
            SELECT GREATEST(
                (SELECT COALESCE(MAX(c), 0) FROM (
                    SELECT COUNT(*) c
                    FROM (SELECT DISTINCT {t1_distinct} FROM {table1_path} WHERE {t1_not_null}) t1
                    INNER JOIN {table2_path} t2 ON {on_clause}
                    GROUP BY {t1_group})),
                (SELECT COALESCE(MAX(c), 0) FROM (
                    SELECT COUNT(*) c
                    FROM (SELECT DISTINCT {t2_distinct} FROM {table2_path} WHERE {t2_not_null}) t2
                    INNER JOIN {table1_path} t1 ON {on_clause}
                    GROUP BY {t2_group}))
            )
        """
        result = duckdb_conn.execute(q).fetchone()
        return float(result[0]) if result and result[0] is not None else float("inf")
    except Exception as e:
        logger.warning("join_multiplication_failed", column_pairs=column_pairs, error=str(e))
        return float("inf")
