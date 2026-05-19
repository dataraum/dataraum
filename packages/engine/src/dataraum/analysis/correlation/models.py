"""Correlation Analysis Pydantic Models.

This module contains all Pydantic models for the correlation analysis module.

Within-Table Analysis:
- NumericCorrelation: Pearson and Spearman correlations
- DerivedColumn: Detected derived columns

Result Containers:
- CorrelationAnalysisResult: Complete per-table analysis result
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

# =============================================================================
# Within-Table Analysis Models
# =============================================================================


class NumericCorrelation(BaseModel):
    """Pearson and Spearman correlation between two numeric columns."""

    correlation_id: str
    table_id: str
    column1_id: str
    column2_id: str
    column1_name: str
    column2_name: str

    # Pearson (linear relationship)
    pearson_r: float | None = None
    pearson_p_value: float | None = None

    # Spearman (monotonic relationship)
    spearman_rho: float | None = None
    spearman_p_value: float | None = None

    # Metadata
    sample_size: int
    computed_at: datetime

    # Interpretation
    correlation_strength: str  # 'none', 'weak', 'moderate', 'strong', 'very_strong'
    is_significant: bool  # p_value < 0.05


class DerivedColumn(BaseModel):
    """A column that appears to be derived from other columns."""

    derived_id: str
    table_id: str

    # Derived column
    derived_column_id: str
    derived_column_name: str

    # Source columns
    source_column_ids: list[str]
    source_column_names: list[str]

    # Derivation
    derivation_type: str  # 'sum', 'difference', 'product', 'ratio', 'concat', etc.
    formula: str  # Human-readable formula

    # Match quality
    match_rate: float  # 0 to 1
    total_rows: int
    matching_rows: int

    # Evidence
    mismatch_examples: list[dict[str, Any]] | None = None

    # Metadata
    computed_at: datetime


# =============================================================================
# Result Container Models
# =============================================================================


class CorrelationAnalysisResult(BaseModel):
    """Complete correlation analysis result for a single table."""

    table_id: str
    table_name: str

    # Numeric correlations
    numeric_correlations: list[NumericCorrelation] = Field(default_factory=list)

    # Derived columns
    derived_columns: list[DerivedColumn] = Field(default_factory=list)

    # Summary stats
    total_column_pairs: int
    significant_correlations: int
    strong_correlations: int  # |r| > 0.7

    # Performance
    duration_seconds: float
    computed_at: datetime
