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
    sample_percent: float = 10.0,
) -> list[dict[str, Any]]:
    """Find relationships between tables via value overlap.

    Args:
        conn: DuckDB connection
        tables: Dict of table_name -> (duckdb_path, column_names, column_types)
            where column_types maps column_name -> resolved_type
        min_confidence: Minimum join_confidence threshold (default 0.3)
        sample_percent: Row-sample percentage for the uniqueness ratio (default 10%)

    Returns:
        List of relationship candidates with join columns
    """
    relationships = []
    table_names = list(tables.keys())
    # Uniqueness is computed in SQL; cache per (path, column) — a column recurs across
    # every table-pair it participates in, and the ratio doesn't depend on the pair.
    uniqueness_cache: dict[tuple[str, str], float] = {}

    for i, name1 in enumerate(table_names):
        for name2 in table_names[i + 1 :]:
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
            )

            # Enrich with uniqueness ratios (SQL, sampled)
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
                            conn, path1, col1_name, sample_percent, uniqueness_cache
                        ),
                        "right_uniqueness": _uniqueness_ratio(
                            conn, path2, col2_name, sample_percent, uniqueness_cache
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
    sample_percent: float,
    cache: dict[tuple[str, str], float],
) -> float:
    """Distinct values / total rows for a column, over a Bernoulli row sample.

    Computed in SQL (no DataFrame materialization). ``COUNT(DISTINCT)`` already ignores
    NULLs, so the ratio is over observed values; an empty sample yields 0.0.
    """
    key = (duckdb_path, column)
    if key in cache:
        return cache[key]
    quoted = '"' + column.replace('"', '""') + '"'
    row = conn.execute(
        f"SELECT COUNT(DISTINCT {quoted})::DOUBLE / NULLIF(COUNT(*), 0) "  # noqa: S608 — catalog identifiers
        f"FROM (SELECT {quoted} FROM {duckdb_path} USING SAMPLE {sample_percent}% (bernoulli))"
    ).fetchone()
    # fetchone() on a bare aggregate always returns one row; row[0] is None only when
    # NULLIF zeroes an empty sample.
    ratio = round(row[0], 4) if row and row[0] is not None else 0.0
    cache[key] = ratio
    return ratio
