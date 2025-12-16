"""Find relationships between tables.

Combines TDA (structural similarity) and value overlap (join detection).
Confidence = max(topology_similarity, join_confidence)
"""

from typing import Any

import duckdb
import pandas as pd

from dataraum_context.analysis.relationships.joins import find_join_columns
from dataraum_context.analysis.relationships.topology import (
    compute_persistence,
    persistence_similarity,
)


def find_relationships(
    conn: duckdb.DuckDBPyConnection,
    tables: dict[str, tuple[str, pd.DataFrame]],  # name -> (duckdb_path, sampled_df for TDA)
    min_confidence: float = 0.3,
) -> list[dict[str, Any]]:
    """Find relationships between tables.

    Uses two signals:
    - TDA persistence similarity: structural similarity (uses sampled data)
    - Value overlap: join column detection (uses DuckDB SQL on full data)

    Confidence = max(topology_similarity, best_join_confidence)

    Args:
        conn: DuckDB connection
        tables: Dict of table_name -> (duckdb_path, sampled_df)
                sampled_df is used for TDA only
        min_confidence: Minimum confidence threshold

    Returns:
        List of relationship candidates with ALL join columns above threshold
    """
    # Compute persistence for each table (using sampled data for TDA)
    persistence_cache = {name: compute_persistence(df) for name, (_, df) in tables.items()}

    relationships = []
    table_names = list(tables.keys())

    for i, name1 in enumerate(table_names):
        for name2 in table_names[i + 1 :]:
            path1, df1 = tables[name1]
            path2, df2 = tables[name2]

            # TDA: structural similarity (uses sampled data)
            topo_sim = persistence_similarity(
                persistence_cache[name1],
                persistence_cache[name2],
            )

            # Value overlap: use DuckDB for accurate distinct value comparison
            join_candidates = find_join_columns(
                conn,
                path1,
                path2,
                list(df1.columns),
                list(df2.columns),
                min_score=min_confidence,
            )

            # Confidence = max of both signals
            best_join_conf = max((j["confidence"] for j in join_candidates), default=0.0)
            confidence = max(topo_sim, best_join_conf)

            if confidence >= min_confidence:
                relationships.append(
                    {
                        "table1": name1,
                        "table2": name2,
                        "confidence": confidence,
                        "topology_similarity": topo_sim,
                        "join_columns": join_candidates,
                        "relationship_type": _classify_relationship(join_candidates),
                    }
                )

    return relationships


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
