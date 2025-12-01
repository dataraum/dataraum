# enrichment/tda/relationship_finder.py
from typing import Any

import numpy as np
import pandas as pd

from dataraum_context.enrichment.tda.topology_extractor import TableTopologyExtractor


class TableRelationshipFinder:
    """
    Find relationships between tables using topology
    """

    def __init__(self):
        self.extractor = TableTopologyExtractor()

    def find_relationships(self, tables: dict[str, pd.DataFrame]) -> dict[str, Any]:
        """
        Find all relationships between tables
        """
        # Extract topology for each table
        topologies = {}
        for name, df in tables.items():
            topologies[name] = self.extractor.extract_topology(df)

        # Find pairwise relationships
        relationships = []

        for name1, topo1 in topologies.items():
            for name2, topo2 in topologies.items():
                if name1 < name2:  # Only compute once per pair
                    rel = self.compare_topologies(
                        name1, tables[name1], topo1, name2, tables[name2], topo2
                    )

                    if rel["confidence"] > 0.3:  # Threshold
                        relationships.append(rel)

        return {
            "relationships": relationships,
            "join_graph": self.build_join_graph(relationships),
            "suggested_joins": self.suggest_joins(relationships, tables),
        }

    def compare_topologies(self, name1, df1, topo1, name2, df2, topo2):
        """
        Compare two table topologies to find relationships
        """
        relationship = {
            "table1": name1,
            "table2": name2,
            "confidence": 0,
            "join_columns": [],
            "relationship_type": "unknown",
        }

        # Compare persistence diagrams
        persistence_similarity = self.compare_persistence_diagrams(
            topo1["global_persistence"], topo2["global_persistence"]
        )

        # Find potential join columns
        join_candidates = self.find_join_columns(df1, df2)

        if join_candidates:
            relationship["join_columns"] = join_candidates
            relationship["confidence"] = max(
                persistence_similarity, max(j["confidence"] for j in join_candidates)
            )

            # Determine relationship type
            relationship["relationship_type"] = self.determine_relationship_type(
                df1, df2, join_candidates
            )

        return relationship

    def compare_persistence_diagrams(self, persistence1, persistence2):
        """
        Compare two persistence diagrams using Wasserstein distance
        """
        if not persistence1.get("diagrams") or not persistence2.get("diagrams"):
            return 0

        from scipy.stats import wasserstein_distance

        similarities = []

        for dim in range(min(len(persistence1["diagrams"]), len(persistence2["diagrams"]))):
            dgm1 = persistence1["diagrams"][dim]
            dgm2 = persistence2["diagrams"][dim]

            if len(dgm1) > 0 and len(dgm2) > 0:
                # Use birth times for comparison
                births1 = dgm1[:, 0][dgm1[:, 1] < np.inf]
                births2 = dgm2[:, 0][dgm2[:, 1] < np.inf]

                if len(births1) > 0 and len(births2) > 0:
                    # Normalize and compute distance
                    max_birth = max(births1.max(), births2.max())
                    if max_birth > 0:
                        births1_norm = births1 / max_birth
                        births2_norm = births2 / max_birth

                        # Pad to same length
                        max_len = max(len(births1_norm), len(births2_norm))
                        births1_padded = np.pad(births1_norm, (0, max_len - len(births1_norm)))
                        births2_padded = np.pad(births2_norm, (0, max_len - len(births2_norm)))

                        distance = wasserstein_distance(births1_padded, births2_padded)
                        similarity = 1 / (1 + distance)
                        similarities.append(similarity)

        return np.mean(similarities) if similarities else 0

    def find_join_columns(self, df1, df2):
        """
        Find potential join columns between two tables
        """
        join_candidates = []

        for col1 in df1.columns:
            for col2 in df2.columns:
                score = self.compute_join_score(df1[col1], df2[col2])

                if score > 0.3:  # Threshold
                    join_candidates.append(
                        {
                            "column1": col1,
                            "column2": col2,
                            "confidence": score,
                            "join_type": self.determine_join_type(df1[col1], df2[col2]),
                        }
                    )

        # Sort by confidence
        join_candidates.sort(key=lambda x: x["confidence"], reverse=True)

        return join_candidates[:5]  # Return top 5

    def compute_join_score(self, col1, col2):
        """
        Compute likelihood that two columns can be joined
        """
        # Type compatibility
        if not self.are_types_compatible(col1, col2):
            return 0

        score = 0

        # For string/categorical columns
        if pd.api.types.is_string_dtype(col1) and pd.api.types.is_string_dtype(col2):
            set1 = set(col1.dropna())
            set2 = set(col2.dropna())

            if len(set1) > 0 and len(set2) > 0:
                # Jaccard similarity
                intersection = len(set1.intersection(set2))
                union = len(set1.union(set2))
                jaccard = intersection / union if union > 0 else 0

                # Containment (one is subset of other - strong FK indicator)
                containment = 0
                if set1.issubset(set2):
                    containment = 0.9
                elif set2.issubset(set1):
                    containment = 0.9

                score = max(jaccard, containment)

        # For numeric columns
        elif pd.api.types.is_numeric_dtype(col1) and pd.api.types.is_numeric_dtype(col2):
            # Check range overlap
            range1 = (col1.min(), col1.max())
            range2 = (col2.min(), col2.max())

            overlap = self.compute_range_overlap(range1, range2)

            # Score based on range overlap only
            score = overlap * 0.7

        return min(score, 1.0)

    def are_types_compatible(self, col1, col2):
        """Check if column types are compatible for joining"""
        # Both numeric
        if pd.api.types.is_numeric_dtype(col1) and pd.api.types.is_numeric_dtype(col2):
            return True

        # Both string/object
        if (pd.api.types.is_string_dtype(col1) or pd.api.types.is_object_dtype(col1)) and (
            pd.api.types.is_string_dtype(col2) or pd.api.types.is_object_dtype(col2)
        ):
            return True

        # Both datetime
        if pd.api.types.is_datetime64_dtype(col1) and pd.api.types.is_datetime64_dtype(col2):
            return True

        return False

    def compute_range_overlap(self, range1, range2):
        """Compute overlap between two numeric ranges"""
        min1, max1 = range1
        min2, max2 = range2

        # No overlap
        if max1 < min2 or max2 < min1:
            return 0

        # Compute overlap
        overlap_min = max(min1, min2)
        overlap_max = min(max1, max2)

        overlap_size = overlap_max - overlap_min
        total_size = max(max1, max2) - min(min1, min2)

        return overlap_size / total_size if total_size > 0 else 0

    def determine_join_type(self, col1, col2):
        """Determine the type of join relationship"""
        card1 = col1.nunique()
        card2 = col2.nunique()

        ratio = card1 / card2 if card2 > 0 else float("inf")

        if abs(ratio - 1) < 0.1:
            return "one-to-one"
        elif ratio > 10:
            return "many-to-one"
        elif ratio < 0.1:
            return "one-to-many"
        else:
            return "many-to-many"

    def determine_relationship_type(self, df1, df2, join_candidates):
        """Determine overall relationship type between tables"""
        if not join_candidates:
            return "unrelated"

        # Check cardinalities
        best_join = join_candidates[0]
        col1 = df1[best_join["column1"]]
        col2 = df2[best_join["column2"]]

        # Check for parent-child relationship
        if col1.nunique() == len(df1) and col1.nunique() < col2.nunique():
            return "parent-child"
        elif col2.nunique() == len(df2) and col2.nunique() < col1.nunique():
            return "child-parent"

        # Check for lookup relationship
        if len(df1) < 100 and len(df2) > 1000:
            return "lookup-fact"
        elif len(df2) < 100 and len(df1) > 1000:
            return "fact-lookup"

        return best_join["join_type"]

    def build_join_graph(self, relationships):
        """Build a graph of table relationships"""
        import networkx as nx

        G = nx.Graph()

        for rel in relationships:
            G.add_edge(
                rel["table1"],
                rel["table2"],
                weight=rel["confidence"],
                join_columns=rel["join_columns"],
            )

        return (
            {
                "nodes": list(G.nodes()),
                "edges": [
                    {
                        "source": u,
                        "target": v,
                        "weight": d["weight"],
                        "join_columns": d["join_columns"],
                    }
                    for u, v, d in G.edges(data=True)
                ],
                "is_connected": nx.is_connected(G),
                "components": list(nx.connected_components(G)),
            }
            if G.number_of_nodes() > 0
            else {"nodes": [], "edges": [], "is_connected": False, "components": []}
        )

    def suggest_joins(self, relationships, tables):
        """Suggest optimal join paths"""
        suggestions = []

        for rel in relationships:
            if rel["confidence"] > 0.7:  # High confidence
                suggestion = {
                    "tables": (rel["table1"], rel["table2"]),
                    "join_columns": rel["join_columns"][0] if rel["join_columns"] else None,
                    "confidence": rel["confidence"],
                    "sql": self.generate_join_sql(rel, tables),
                }
                suggestions.append(suggestion)

        return suggestions

    def generate_join_sql(self, relationship, tables):
        """Generate SQL for the suggested join"""
        if not relationship["join_columns"]:
            return None

        best_join = relationship["join_columns"][0]

        sql = f"""
SELECT *
FROM {relationship["table1"]} t1
{best_join["join_type"].upper().replace("-", "_")} JOIN {relationship["table2"]} t2
    ON t1.{best_join["column1"]} = t2.{best_join["column2"]}
        """.strip()

        return sql
