"""Temporal analysis module.

Provides temporal analysis for time columns including:
- Granularity detection (day, week, month, etc.)
- Completeness and gap analysis
- Seasonality and trend detection
- Change point detection
- Staleness assessment

Main Entry Point:
    profile_temporal(table_id, duckdb_conn, session) -> Result[TemporalProfileResult]

Example:
    from dataraum.analysis.temporal import profile_temporal

    result = await profile_temporal(table_id, duckdb_conn, session)
    if result.success:
        for profile in result.unwrap().column_profiles:
            print(f"{profile.column_name}: {profile.detected_granularity}")
"""

from dataraum.analysis.temporal.db_models import (
    TemporalColumnProfile,
)
from dataraum.analysis.temporal.db_models import (
    TemporalTableSummary as DBTemporalTableSummary,
)
from dataraum.analysis.temporal.models import (
    TemporalAnalysisResult,
    TemporalProfileResult,
    TemporalTableSummary,
)
from dataraum.analysis.temporal.processor import profile_temporal

__all__ = [
    # Main entry point
    "profile_temporal",
    # Result models
    "TemporalProfileResult",
    "TemporalAnalysisResult",
    "TemporalTableSummary",
    # DB models
    "TemporalColumnProfile",
    "DBTemporalTableSummary",
]
