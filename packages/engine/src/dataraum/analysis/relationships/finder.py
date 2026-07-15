"""Find relationships between tables.

Uses value overlap (Jaccard/containment) to detect joinable column pairs.
Enriches candidates with column uniqueness for context.
"""

from typing import Any

import duckdb

from dataraum.analysis.relationships.joins import find_join_columns
from dataraum.core.logging import get_logger

logger = get_logger(__name__)

# Type alias for table data: (duckdb_path, column_names, column_types)
TableData = tuple[str, list[str], dict[str, str | None]]


def find_relationships(
    conn: duckdb.DuckDBPyConnection,
    tables: dict[str, TableData],  # name -> (duckdb_path, column_names, column_types)
    min_confidence: float = 0.3,
) -> list[dict[str, Any]]:
    """Find relationships between tables via value overlap.

    Args:
        conn: DuckDB connection
        tables: Dict of table_name -> (duckdb_path, column_names, column_types)
            where column_types maps column_name -> resolved_type
        min_confidence: Minimum join_confidence threshold (default 0.3)

    Returns:
        List of relationship candidates with join columns
    """
    relationships = []
    table_names = list(tables.keys())
    # Uniqueness is computed in SQL; cache per (path, column) — a column recurs across
    # every table-pair it participates in, and the ratio doesn't depend on the pair.
    uniqueness_cache: dict[tuple[str, str], float] = {}

    # ``table_names[i:]`` includes the DIAGONAL (name1 == name1): a self-referential
    # FK (``chart_of_accounts.parent_id -> account_id``) lives inside ONE table, so it
    # is only ever a candidate when the finder probes a table against itself. The
    # ``same_table`` flag restricts the self-probe to distinct-column pairs (the
    # upper triangle) — deterministic Layer-A detection, no LLM needed to propose it
    # (DAT-763).
    for i, name1 in enumerate(table_names):
        for name2 in table_names[i:]:
            same = name1 == name2
            path1, cols1, types1 = tables[name1]
            path2, cols2, types2 = tables[name2]

            # Find join candidates via value overlap
            join_candidates = find_join_columns(
                conn,
                path1,
                path2,
                cols1,
                cols2,
                min_score=min_confidence,
                column_types1=types1,
                column_types2=types2,
                same_table=same,
            )

            # Enrich with uniqueness ratios (exact SQL)
            enriched_candidates = []
            for jc in join_candidates:
                col1_name, col2_name = jc["column1"], jc["column2"]

                enriched_candidates.append(
                    {
                        "column1": col1_name,
                        "column2": col2_name,
                        "join_confidence": jc["join_confidence"],
                        "cardinality": jc["cardinality"],
                        "left_uniqueness": _uniqueness_ratio(
                            conn, path1, col1_name, uniqueness_cache
                        ),
                        "right_uniqueness": _uniqueness_ratio(
                            conn, path2, col2_name, uniqueness_cache
                        ),
                        "statistical_confidence": jc.get("statistical_confidence", 1.0),
                        "algorithm": jc.get("algorithm", "exact"),
                    }
                )

            if enriched_candidates:
                # Sort by join_confidence
                enriched_candidates.sort(key=lambda x: x["join_confidence"], reverse=True)

                relationships.append(
                    {
                        "table1": name1,
                        "table2": name2,
                        "join_columns": enriched_candidates,
                    }
                )

    return relationships


def _uniqueness_ratio(
    conn: duckdb.DuckDBPyConnection,
    duckdb_path: str,
    column: str,
    cache: dict[tuple[str, str], float],
) -> float:
    """Distinct values / total rows for a column, computed exactly in SQL.

    Exact by design (DAT-794): a row-sampled ratio is a biased estimator of the
    wrong quantity — sample-distinct/sample-rows systematically overstates the
    uniqueness of repeated-value (FK-like) columns at ANY sample rate (measured
    0.93–0.95 for a true 0.47 at 10%), and the value is served to the semantic
    LLM as key-vs-measure evidence. A full COUNT(DISTINCT) costs ~26ms at 1M
    rows. ``COUNT(DISTINCT)`` ignores NULLs, so the ratio is over observed
    values; an empty table yields 0.0.
    """
    key = (duckdb_path, column)
    if key in cache:
        return cache[key]
    quoted = '"' + column.replace('"', '""') + '"'
    row = conn.execute(
        f"SELECT COUNT(DISTINCT {quoted})::DOUBLE / NULLIF(COUNT(*), 0) "  # noqa: S608 — catalog identifiers
        f"FROM {duckdb_path}"
    ).fetchone()
    # fetchone() on a bare aggregate always returns one row; row[0] is None only when
    # NULLIF zeroes an empty table.
    ratio = round(row[0], 4) if row and row[0] is not None else 0.0
    cache[key] = ratio
    return ratio
