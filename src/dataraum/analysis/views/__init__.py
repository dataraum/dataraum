"""Enriched views module.

Creates grain-preserving DuckDB views that join fact tables with their
confirmed dimension tables. These views materialize the semantic understanding
of relationships for downstream consumption (slicing, correlations, etc.).
"""

from dataraum.analysis.views.builder import DimensionJoin, build_enriched_view_sql
from dataraum.analysis.views.db_models import EnrichedView

__all__ = [
    "DimensionJoin",
    "build_enriched_view_sql",
    "EnrichedView",
]
