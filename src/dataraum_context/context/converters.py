"""Model converters - SQLAlchemy storage models → Pydantic business models.

This module provides conversion functions that transform database models
(SQLAlchemy ORM) into business models (Pydantic) for each pillar.

Pattern:
- Input: SQLAlchemy model (from storage/models_v2/)
- Output: Pydantic model (from core/models/)
- Purpose: Isolate storage layer from business logic layer
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dataraum_context.core.models.statistical import (
    BenfordTestResult,
    DistributionStabilityResult,
    EntropyStats,
    HistogramBucket,
    NumericStats,
    OrderStats,
    OutlierDetectionResult,
    StatisticalProfile,
    StatisticalQualityMetrics,
    StringStats,
    UniquenessStats,
    ValueCount,
    VIFResult,
)
from dataraum_context.core.models.topological import (
    BettiNumbers,
    HomologicalStability,
    PersistenceDiagram,
    PersistencePoint,
    StructuralComplexity,
    TopologicalQualityResult,
)

if TYPE_CHECKING:
    from dataraum_context.storage.models_v2.statistical_context import (
        StatisticalProfile as StatisticalProfileDB,
    )
    from dataraum_context.storage.models_v2.statistical_context import (
        StatisticalQualityMetrics as StatisticalQualityMetricsDB,
    )
    from dataraum_context.storage.models_v2.topological_context import (
        TopologicalQualityMetrics as TopologicalQualityMetricsDB,
    )


# ==================== Pillar 1: Statistical Context Converters ====================


def convert_statistical_profile(db_profile: StatisticalProfileDB) -> StatisticalProfile:
    """Convert SQLAlchemy StatisticalProfile to Pydantic StatisticalProfile.

    Args:
        db_profile: Database statistical profile

    Returns:
        Pydantic statistical profile
    """
    # Convert numeric stats if present
    numeric_stats = None
    if db_profile.min_value is not None:
        numeric_stats = NumericStats(
            min_value=db_profile.min_value,
            max_value=db_profile.max_value,
            mean=db_profile.mean_value,
            stddev=db_profile.stddev_value,
            skewness=db_profile.skewness,
            kurtosis=db_profile.kurtosis,
            cv=db_profile.cv,
            percentiles=db_profile.percentiles or {},
        )

    # Convert string stats if present
    string_stats = None
    if db_profile.min_length is not None:
        string_stats = StringStats(
            min_length=db_profile.min_length,
            max_length=db_profile.max_length,
            avg_length=db_profile.avg_length,
        )

    # Convert entropy stats if present
    entropy_stats = None
    if db_profile.shannon_entropy is not None:
        entropy_stats = EntropyStats(
            shannon_entropy=db_profile.shannon_entropy,
            normalized_entropy=db_profile.normalized_entropy or 0.0,
        )

    # Convert uniqueness stats if present
    uniqueness_stats = None
    if hasattr(db_profile, "is_unique") and db_profile.is_unique is not None:
        uniqueness_stats = UniquenessStats(
            is_unique=db_profile.is_unique,
            is_primary_key_candidate=getattr(db_profile, "is_primary_key_candidate", False),
            duplicate_count=getattr(db_profile, "duplicate_count", 0),
        )

    # Convert order stats if present
    order_stats = None
    if hasattr(db_profile, "is_sorted") and db_profile.is_sorted is not None:
        order_stats = OrderStats(
            is_sorted=db_profile.is_sorted,
            sort_direction=getattr(db_profile, "sort_direction", None),
            inversions=getattr(db_profile, "inversions", None),
        )

    # Convert histogram
    histogram = None
    if db_profile.histogram:
        histogram = [
            HistogramBucket(
                bucket_min=bucket["bucket_min"],
                bucket_max=bucket["bucket_max"],
                count=bucket["count"],
            )
            for bucket in db_profile.histogram
        ]

    # Convert top values
    top_values = None
    if db_profile.top_values:
        top_values = [
            ValueCount(
                value=val["value"],
                count=val["count"],
                percentage=val.get("percentage", 0.0),
            )
            for val in db_profile.top_values
        ]

    return StatisticalProfile(
        profile_id=db_profile.profile_id,
        column_id=db_profile.column_id,
        profiled_at=db_profile.profiled_at,
        total_count=db_profile.total_count,
        null_count=db_profile.null_count,
        distinct_count=db_profile.distinct_count or 0,
        null_ratio=db_profile.null_ratio or 0.0,
        cardinality_ratio=db_profile.cardinality_ratio or 0.0,
        numeric_stats=numeric_stats,
        string_stats=string_stats,
        entropy_stats=entropy_stats,
        uniqueness_stats=uniqueness_stats,
        order_stats=order_stats,
        histogram=histogram,
        top_values=top_values,
    )


def convert_statistical_quality_metrics(
    db_metrics: StatisticalQualityMetricsDB,
) -> StatisticalQualityMetrics:
    """Convert SQLAlchemy StatisticalQualityMetrics to Pydantic.

    Args:
        db_metrics: Database statistical quality metrics

    Returns:
        Pydantic statistical quality metrics
    """
    # Convert Benford test if present
    benford_test = None
    if hasattr(db_metrics, "benford_chi_square") and db_metrics.benford_chi_square is not None:
        benford_test = BenfordTestResult(
            chi_square=db_metrics.benford_chi_square,
            p_value=db_metrics.benford_p_value,
            compliant=db_metrics.benford_compliant,
            interpretation=db_metrics.benford_interpretation or "",
        )

    # Convert distribution stability if present
    distribution_stability = None
    if hasattr(db_metrics, "ks_statistic") and db_metrics.ks_statistic is not None:
        distribution_stability = DistributionStabilityResult(
            ks_statistic=db_metrics.ks_statistic,
            p_value=db_metrics.ks_p_value,
            is_stable=db_metrics.distribution_stable,
            interpretation=getattr(db_metrics, "ks_interpretation", ""),
        )

    # Convert outlier detection if present
    outlier_detection = None
    if (
        hasattr(db_metrics, "isolation_forest_score")
        and db_metrics.isolation_forest_score is not None
    ):
        outlier_detection = OutlierDetectionResult(
            anomaly_score=db_metrics.isolation_forest_score,
            is_anomalous=db_metrics.is_anomalous,
            method="isolation_forest",
            threshold=getattr(db_metrics, "anomaly_threshold", 0.5),
        )

    # Convert VIF if present
    vif_result = None
    if hasattr(db_metrics, "vif_score") and db_metrics.vif_score is not None:
        vif_result = VIFResult(
            vif_score=db_metrics.vif_score,
            has_multicollinearity=db_metrics.vif_score > 10,
            interpretation=f"VIF={db_metrics.vif_score:.2f}",
        )

    # Convert quality issues
    quality_issues = db_metrics.quality_issues or []

    return StatisticalQualityMetrics(
        benford_test=benford_test,
        distribution_stability=distribution_stability,
        outlier_detection=outlier_detection,
        vif_result=vif_result,
        quality_issues=quality_issues,
    )


# ==================== Pillar 2: Topological Context Converters ====================


def convert_topological_metrics(
    db_metrics: TopologicalQualityMetricsDB,
) -> TopologicalQualityResult:
    """Convert SQLAlchemy TopologicalQualityMetrics to Pydantic.

    Args:
        db_metrics: Database topological quality metrics

    Returns:
        Pydantic topological quality result
    """
    # Convert Betti numbers
    betti_numbers = BettiNumbers(
        betti_0=db_metrics.betti_0 or 0,
        betti_1=db_metrics.betti_1 or 0,
        betti_2=db_metrics.betti_2 or 0,
    )

    # Convert persistence diagrams
    persistence_diagrams = []
    if db_metrics.persistence_diagrams:
        for dim, points in db_metrics.persistence_diagrams.items():
            diagram_points = [
                PersistencePoint(
                    birth=pt["birth"],
                    death=pt["death"],
                    dimension=int(dim),
                    persistence=pt.get("persistence", pt["death"] - pt["birth"]),
                )
                for pt in points
            ]
            persistence_diagrams.append(
                PersistenceDiagram(
                    dimension=int(dim),
                    points=diagram_points,
                )
            )

    # Convert homological stability
    homological_stability = None
    if db_metrics.bottleneck_distance is not None:
        homological_stability = HomologicalStability(
            bottleneck_distance=db_metrics.bottleneck_distance,
            is_stable=db_metrics.homologically_stable,
            threshold=getattr(db_metrics, "stability_threshold", 0.2),
        )

    # Convert structural complexity
    structural_complexity = None
    if db_metrics.structural_complexity is not None:
        structural_complexity = StructuralComplexity(
            total_complexity=db_metrics.structural_complexity,
            complexity_trend=db_metrics.complexity_trend or "unknown",
            within_bounds=db_metrics.complexity_within_bounds,
        )

    # Extract anomalous cycles and orphaned components
    anomalous_cycles = db_metrics.anomalous_cycles or []
    orphaned_components = db_metrics.orphaned_components or 0

    return TopologicalQualityResult(
        betti_numbers=betti_numbers,
        persistence_diagrams=persistence_diagrams,
        homological_stability=homological_stability,
        structural_complexity=structural_complexity,
        anomalous_cycles=anomalous_cycles,
        orphaned_components=orphaned_components,
        persistent_entropy=db_metrics.persistent_entropy,
    )


# ==================== Helper Functions ====================


def safe_get(obj: object, attr: str, default=None):
    """Safely get attribute from object, return default if not present.

    Args:
        obj: Object to get attribute from
        attr: Attribute name
        default: Default value if attribute not present

    Returns:
        Attribute value or default
    """
    return getattr(obj, attr, default)
