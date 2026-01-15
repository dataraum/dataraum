"""Temporal slicing analysis module.

Provides temporal analysis capabilities for slice data including:
- Period completeness analysis (data cutoffs)
- Distribution drift detection (JS divergence)
- Cross-slice temporal comparison
- Volume anomaly detection
- Temporal topology analysis (correlation structure drift)
"""

from dataraum_context.analysis.temporal_slicing.analyzer import (
    TemporalSliceAnalyzer,
    TemporalSliceContext,
    aggregate_temporal_data,
    analyze_temporal_slices,
    analyze_temporal_topology,
)
from dataraum_context.analysis.temporal_slicing.db_models import (
    SliceTimeMatrixEntry,
    TemporalDriftAnalysis,
    TemporalSliceAnalysis,
    TemporalSliceRun,
    TemporalTopologyAnalysis,
)
from dataraum_context.analysis.temporal_slicing.models import (
    AggregatedTemporalData,
    CompletenessResult,
    DistributionDriftResult,
    PeriodMetrics,
    PeriodTopology,
    SliceTimeCell,
    SliceTimeMatrix,
    TemporalAnalysisResult,
    TemporalSliceConfig,
    TemporalTopologyResult,
    TimeGrain,
    TopologyDrift,
    VolumeAnomalyResult,
)

__all__ = [
    # Main entry points
    "analyze_temporal_slices",
    "analyze_temporal_topology",
    "aggregate_temporal_data",
    "TemporalSliceAnalyzer",
    "TemporalSliceContext",
    # Config
    "TemporalSliceConfig",
    "TimeGrain",
    # Models
    "PeriodMetrics",
    "CompletenessResult",
    "DistributionDriftResult",
    "SliceTimeCell",
    "SliceTimeMatrix",
    "VolumeAnomalyResult",
    "TemporalAnalysisResult",
    "AggregatedTemporalData",
    # Temporal Topology Models
    "PeriodTopology",
    "TopologyDrift",
    "TemporalTopologyResult",
    # DB Models
    "TemporalSliceAnalysis",
    "TemporalSliceRun",
    "TemporalDriftAnalysis",
    "SliceTimeMatrixEntry",
    "TemporalTopologyAnalysis",
]
