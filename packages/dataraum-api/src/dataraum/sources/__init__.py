"""Data source loaders.

This module provides loaders for different source types:
- csv: Untyped sources (VARCHAR-first approach)
- parquet: Strongly typed sources (future)
- sqlite: Weakly typed sources (future)
"""

from dataraum.sources.base import ColumnInfo, LoaderBase, TypeSystemStrength

__all__ = [
    "LoaderBase",
    "TypeSystemStrength",
    "ColumnInfo",
]
