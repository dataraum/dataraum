"""Find relationships between tables.

Uses value overlap (Jaccard/containment) to detect joinable column pairs.
Enriches candidates with column uniqueness for context.
"""

from typing import Any

import duckdb
import pandas as pd

from dataraum_context.analysis.relationships.joins import find_join_columns


def find_relationships(
    conn: duckdb.DuckDBPyConnection,
    tables: dict[str, tuple[str, pd.DataFrame]],  # name -> (duckdb_path, sampled_df)
    min_confidence: float = 0.3,
) -> list[dict[str, Any]]:
    """Find relationships between tables via value overlap.

    Args:
        conn: DuckDB connection
        tables: Dict of table_name -> (duckdb_path, sampled_df)
        min_confidence: Minimum join_confidence threshold (default 0.3)

    Returns:
        List of relationship candidates with join columns
    """
    relationships = []
    table_names = list(tables.keys())

    for i, name1 in enumerate(table_names):
        for name2 in table_names[i + 1 :]:
            path1, df1 = tables[name1]
            path2, df2 = tables[name2]

            # Find join candidates via value overlap
            join_candidates = find_join_columns(
                conn,
                path1,
                path2,
                list(df1.columns),
                list(df2.columns),
                min_score=min_confidence,
            )

            # Enrich with uniqueness ratios from sampled data
            enriched_candidates = []
            for jc in join_candidates:
                col1_name, col2_name = jc["column1"], jc["column2"]

                enriched_candidates.append(
                    {
                        "column1": col1_name,
                        "column2": col2_name,
                        "join_confidence": jc["join_confidence"],
                        "cardinality": jc["cardinality"],
                        "left_uniqueness": _uniqueness_ratio(df1[col1_name]),
                        "right_uniqueness": _uniqueness_ratio(df2[col2_name]),
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


def _uniqueness_ratio(col: pd.Series) -> float:
    """Compute uniqueness ratio (distinct values / total rows)."""
    if len(col) == 0:
        return 0.0
    return round(col.nunique() / len(col), 4)
