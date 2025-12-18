"""Find relationships between tables.

Combines TDA (column-level feature similarity) and value overlap (join detection).
Each join candidate gets: confidence = max(topology_similarity, join_confidence)
"""

from typing import Any

import duckdb
import pandas as pd

from dataraum_context.analysis.relationships.joins import find_join_columns
from dataraum_context.analysis.relationships.topology import column_feature_similarity


def _are_types_compatible(col1: pd.Series, col2: pd.Series) -> bool:
    """Check if two columns have compatible types for joining.

    Only columns of similar types can be meaningfully joined:
    - numeric with numeric
    - string/object with string/object
    - datetime with datetime
    - bool with bool
    """
    # Both numeric (int, float)
    if pd.api.types.is_numeric_dtype(col1) and pd.api.types.is_numeric_dtype(col2):
        # But not bool - bool is technically numeric but shouldn't join with int/float
        if pd.api.types.is_bool_dtype(col1) or pd.api.types.is_bool_dtype(col2):
            return pd.api.types.is_bool_dtype(col1) and pd.api.types.is_bool_dtype(col2)
        return True

    # Both string/object
    if (pd.api.types.is_string_dtype(col1) or pd.api.types.is_object_dtype(col1)) and (
        pd.api.types.is_string_dtype(col2) or pd.api.types.is_object_dtype(col2)
    ):
        return True

    # Both datetime
    if pd.api.types.is_datetime64_any_dtype(col1) and pd.api.types.is_datetime64_any_dtype(col2):
        return True

    # Both bool
    if pd.api.types.is_bool_dtype(col1) and pd.api.types.is_bool_dtype(col2):
        return True

    return False


def find_relationships(
    conn: duckdb.DuckDBPyConnection,
    tables: dict[str, tuple[str, pd.DataFrame]],  # name -> (duckdb_path, sampled_df for TDA)
    min_confidence: float = 0.3,
) -> list[dict[str, Any]]:
    """Find relationships between tables.

    Uses two signals per column pair:
    - Column topology similarity: distributional feature comparison (uses sampled data)
    - Value overlap: Jaccard/containment (uses DuckDB SQL on full data)

    Each join candidate gets: confidence = max(topology_similarity, join_confidence)

    Args:
        conn: DuckDB connection
        tables: Dict of table_name -> (duckdb_path, sampled_df)
                sampled_df is used for topology feature extraction
        min_confidence: Minimum confidence threshold

    Returns:
        List of relationship candidates with ALL join columns above threshold
    """
    relationships = []
    table_names = list(tables.keys())

    for i, name1 in enumerate(table_names):
        for name2 in table_names[i + 1 :]:
            path1, df1 = tables[name1]
            path2, df2 = tables[name2]

            # Value overlap: use DuckDB for accurate distinct value comparison
            # Returns candidates with join_confidence (value overlap score)
            join_candidates = find_join_columns(
                conn,
                path1,
                path2,
                list(df1.columns),
                list(df2.columns),
                min_score=0.0,  # Get all pairs, filter by combined confidence later
            )

            # Compute column-level topology similarity for each candidate
            # and set confidence = max(topology, join_confidence)
            enriched_candidates = []
            for jc in join_candidates:
                col1_name, col2_name = jc["column1"], jc["column2"]

                # Compute topology similarity between this specific column pair
                topo_sim = column_feature_similarity(df1[col1_name], df2[col2_name])

                # Combined confidence = max of both signals
                join_conf = jc["join_confidence"]
                confidence = max(topo_sim, join_conf)

                if confidence >= min_confidence:
                    enriched_candidates.append(
                        {
                            "column1": col1_name,
                            "column2": col2_name,
                            "confidence": confidence,
                            "topology_similarity": topo_sim,
                            "join_confidence": join_conf,
                            "cardinality": jc["cardinality"],
                        }
                    )

            # # Also check column pairs not found by value overlap
            # # (high topology similarity but low value overlap - like zip codes)
            # existing_pairs = {(c["column1"], c["column2"]) for c in enriched_candidates}
            # for col1_name in df1.columns:
            #     for col2_name in df2.columns:
            #         if (col1_name, col2_name) in existing_pairs:
            #             continue

            #         # Skip type-incompatible pairs (can't join string with bool, etc.)
            #         if not _are_types_compatible(df1[col1_name], df2[col2_name]):
            #             continue

            #         topo_sim = column_feature_similarity(df1[col1_name], df2[col2_name])
            #         if topo_sim >= min_confidence:
            #             # Get cardinality for this pair
            #             _, cardinality = _get_cardinality(conn, path1, path2, col1_name, col2_name)
            #             enriched_candidates.append(
            #                 {
            #                     "column1": col1_name,
            #                     "column2": col2_name,
            #                     "confidence": topo_sim,
            #                     "topology_similarity": topo_sim,
            #                     "join_confidence": 0.0,  # No value overlap
            #                     "cardinality": cardinality,
            #                 }
            #             )

            # Sort by confidence
            enriched_candidates.sort(key=lambda x: x["confidence"], reverse=True)

            if enriched_candidates:
                best_confidence = enriched_candidates[0]["confidence"]
                relationships.append(
                    {
                        "table1": name1,
                        "table2": name2,
                        "confidence": best_confidence,
                        "join_columns": enriched_candidates,
                        "relationship_type": _classify_relationship(enriched_candidates),
                    }
                )

    return relationships


# def _get_cardinality(
#     conn: duckdb.DuckDBPyConnection,
#     table1: str,
#     table2: str,
#     col1: str,
#     col2: str,
# ) -> tuple[float, str]:
#     """Get cardinality for a column pair."""
#     try:
#         result = conn.execute(f"""
#             SELECT
#                 (SELECT COUNT(*) FROM {table1} WHERE "{col1}" IS NOT NULL) AS total1,
#                 (SELECT COUNT(DISTINCT "{col1}") FROM {table1} WHERE "{col1}" IS NOT NULL) AS distinct1,
#                 (SELECT COUNT(*) FROM {table2} WHERE "{col2}" IS NOT NULL) AS total2,
#                 (SELECT COUNT(DISTINCT "{col2}") FROM {table2} WHERE "{col2}" IS NOT NULL) AS distinct2
#         """).fetchone()

#         if result is None:
#             return 0.0, "unknown"

#         total1, distinct1, total2, distinct2 = result
#         col1_is_unique = distinct1 == total1
#         col2_is_unique = distinct2 == total2

#         if col1_is_unique and col2_is_unique:
#             return 0.0, "one-to-one"
#         elif col1_is_unique:
#             return 0.0, "one-to-many"
#         elif col2_is_unique:
#             return 0.0, "many-to-one"
#         else:
#             return 0.0, "many-to-many"
#     except Exception:
#         return 0.0, "unknown"


def _classify_relationship(
    join_candidates: list[dict[str, Any]],
) -> str:
    """Classify relationship type - structural only or has join candidates.

    Does NOT pick a winner. All join_candidates are passed through for
    the semantic analysis LLM to decide which are real relationships.
    """
    if not join_candidates:
        return "structural"  # Related by topology only
    return "join_candidates"  # Has candidates - LLM decides
