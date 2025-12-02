"""Model converters - SQLAlchemy storage models → Pydantic business models.

This module provides conversion functions that transform database models
(SQLAlchemy ORM) into business models (Pydantic) for each pillar.

Pattern:
- Input: SQLAlchemy model (from storage/models_v2/)
- Output: Pydantic model (from core/models/)
- Purpose: Isolate storage layer from business logic layer
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

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
        # Determine entropy category based on normalized entropy
        norm_entropy = db_profile.normalized_entropy or 0.0
        if norm_entropy < 0.3:
            category = "low"
        elif norm_entropy < 0.7:
            category = "medium"
        else:
            category = "high"

        entropy_stats = EntropyStats(
            shannon_entropy=db_profile.shannon_entropy,
            normalized_entropy=norm_entropy,
            entropy_category=category,
        )

    # Convert uniqueness stats if present
    uniqueness_stats = None
    if hasattr(db_profile, "is_unique") and db_profile.is_unique is not None:
        duplicate_count = getattr(db_profile, "duplicate_count", 0)
        distinct_count = db_profile.distinct_count or 0
        total_count = db_profile.total_count or 1

        # Calculate if near unique (> 99% unique)
        uniqueness_ratio = distinct_count / total_count if total_count > 0 else 0.0
        is_near_unique = uniqueness_ratio > 0.99

        uniqueness_stats = UniquenessStats(
            is_unique=db_profile.is_unique,
            is_near_unique=is_near_unique,
            duplicate_count=duplicate_count,
            most_duplicated_value=getattr(db_profile, "most_duplicated_value", None),
            most_duplicated_count=getattr(db_profile, "most_duplicated_count", None),
        )

    # Convert order stats if present
    order_stats = None
    if hasattr(db_profile, "is_sorted") and db_profile.is_sorted is not None:
        sort_direction = getattr(db_profile, "sort_direction", None)
        inversions_count = getattr(db_profile, "inversions", 0)
        total_count = db_profile.total_count or 1

        # Calculate inversions ratio
        inversions_ratio = inversions_count / max(total_count - 1, 1) if total_count > 1 else 0.0

        order_stats = OrderStats(
            is_sorted=db_profile.is_sorted,
            is_monotonic_increasing=sort_direction == "asc" if sort_direction else False,
            is_monotonic_decreasing=sort_direction == "desc" if sort_direction else False,
            sort_direction=sort_direction,
            inversions_ratio=inversions_ratio,
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
        # Extract digit distribution from quality issues or create default
        if (
            hasattr(db_metrics, "benford_digit_distribution")
            and db_metrics.benford_digit_distribution
        ):
            # Convert string keys to int if needed
            raw_dist = db_metrics.benford_digit_distribution
            digit_distribution: dict[int, float] = {int(k): float(v) for k, v in raw_dist.items()}
        else:
            # Create default distribution for digits 1-9
            digit_distribution = dict.fromkeys(range(1, 10), 0.0)

        benford_test = BenfordTestResult(
            chi_square=db_metrics.benford_chi_square,
            p_value=db_metrics.benford_p_value,
            compliant=db_metrics.benford_compliant,
            interpretation=db_metrics.benford_interpretation or "",
            digit_distribution=digit_distribution,
        )

    # Convert distribution stability if present
    distribution_stability = None
    if hasattr(db_metrics, "ks_statistic") and db_metrics.ks_statistic is not None:
        from datetime import datetime, timedelta

        # Get comparison period dates or use defaults
        computed_at = getattr(db_metrics, "computed_at", datetime.now())
        comparison_period_end = getattr(db_metrics, "comparison_period_end", computed_at)
        comparison_period_start = getattr(
            db_metrics, "comparison_period_start", computed_at - timedelta(days=30)
        )

        distribution_stability = DistributionStabilityResult(
            ks_statistic=db_metrics.ks_statistic,
            p_value=db_metrics.ks_p_value,
            stable=db_metrics.distribution_stable,
            comparison_period_start=comparison_period_start,
            comparison_period_end=comparison_period_end,
        )

    # Convert outlier detection if present
    outlier_detection = None
    if (
        hasattr(db_metrics, "isolation_forest_score")
        and db_metrics.isolation_forest_score is not None
    ):
        # Get outlier count and ratio
        outlier_count = getattr(db_metrics, "outlier_count", 0)
        total_count = getattr(db_metrics, "total_count", 1)
        outlier_ratio = outlier_count / total_count if total_count > 0 else 0.0

        outlier_detection = OutlierDetectionResult(
            method="isolation_forest",
            outlier_count=outlier_count,
            outlier_ratio=outlier_ratio,
            average_anomaly_score=db_metrics.isolation_forest_score,
            outlier_samples=getattr(db_metrics, "outlier_samples", None),
        )

    # Convert VIF if present
    vif_result = None
    if hasattr(db_metrics, "vif_score") and db_metrics.vif_score is not None:
        column_id = getattr(db_metrics, "column_id", "unknown")
        correlated_columns = getattr(db_metrics, "correlated_columns", [])

        vif_result = VIFResult(
            column_id=column_id,
            vif_score=db_metrics.vif_score,
            has_multicollinearity=db_metrics.vif_score > 10,
            correlated_columns=correlated_columns,
        )

    # Convert quality issues
    quality_issues = db_metrics.quality_issues or []

    # Get required fields
    from datetime import datetime

    metric_id = getattr(db_metrics, "metric_id", f"metric_{id(db_metrics)}")
    column_id = getattr(db_metrics, "column_id", "unknown")
    computed_at = getattr(db_metrics, "computed_at", datetime.now())

    return StatisticalQualityMetrics(
        metric_id=metric_id,
        column_id=column_id,
        computed_at=computed_at,
        benford_test=benford_test,
        distribution_stability=distribution_stability,
        outlier_detection=outlier_detection,
        vif_result=vif_result,
        quality_score=getattr(db_metrics, "quality_score", None),
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
    betti_0 = db_metrics.betti_0 or 0
    betti_1 = db_metrics.betti_1 or 0
    betti_2 = db_metrics.betti_2 or 0

    betti_numbers = BettiNumbers(
        betti_0=betti_0,
        betti_1=betti_1,
        betti_2=betti_2,
        total_complexity=betti_0 + betti_1 + betti_2,
        is_connected=betti_0 == 1,
        has_cycles=betti_1 > 0,
        has_voids=betti_2 > 0,
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

            # Calculate max persistence and num features
            max_persistence = max((pt.persistence for pt in diagram_points), default=0.0)
            num_features = len(diagram_points)

            persistence_diagrams.append(
                PersistenceDiagram(
                    dimension=int(dim),
                    points=diagram_points,
                    max_persistence=max_persistence,
                    num_features=num_features,
                )
            )

    # Convert homological stability
    homological_stability = None
    if db_metrics.bottleneck_distance is not None:
        threshold = getattr(db_metrics, "stability_threshold", 0.2)
        bottleneck_dist = db_metrics.bottleneck_distance
        is_stable = db_metrics.homologically_stable

        # Determine stability level
        if bottleneck_dist < threshold * 0.5:
            stability_level = "stable"
        elif bottleneck_dist < threshold:
            stability_level = "minor_changes"
        elif bottleneck_dist < threshold * 2:
            stability_level = "significant_changes"
        else:
            stability_level = "unstable"

        homological_stability = HomologicalStability(
            bottleneck_distance=bottleneck_dist,
            is_stable=is_stable,
            threshold=threshold,
            components_added=getattr(db_metrics, "components_added", 0),
            components_removed=getattr(db_metrics, "components_removed", 0),
            cycles_added=getattr(db_metrics, "cycles_added", 0),
            cycles_removed=getattr(db_metrics, "cycles_removed", 0),
            stability_level=stability_level,
        )

    # Convert structural complexity
    structural_complexity = StructuralComplexity(
        total_complexity=betti_0 + betti_1 + betti_2,
        betti_numbers=betti_numbers,
        persistent_entropy=db_metrics.persistent_entropy,
        complexity_mean=getattr(db_metrics, "complexity_mean", None),
        complexity_std=getattr(db_metrics, "complexity_std", None),
        complexity_z_score=getattr(db_metrics, "complexity_z_score", None),
        complexity_trend=getattr(db_metrics, "complexity_trend", None),
        within_bounds=getattr(db_metrics, "complexity_within_bounds", True),
    )

    # Extract anomalous cycles and orphaned components
    raw_anomalous_cycles = db_metrics.anomalous_cycles
    if raw_anomalous_cycles is None:
        anomalous_cycles: list[dict[str, Any]] = []
    elif isinstance(raw_anomalous_cycles, list):
        anomalous_cycles = raw_anomalous_cycles
    else:
        # If it's a dict, wrap it in a list
        anomalous_cycles = [raw_anomalous_cycles] if raw_anomalous_cycles else []

    orphaned_components = db_metrics.orphaned_components or 0

    # Get required fields
    from datetime import datetime

    metric_id = getattr(db_metrics, "metric_id", f"topo_metric_{id(db_metrics)}")
    table_id = getattr(db_metrics, "table_id", "unknown")
    table_name = getattr(db_metrics, "table_name", "unknown")
    computed_at = getattr(db_metrics, "computed_at", datetime.now())

    # Create topology description
    topology_description = (
        f"{betti_0} connected component{'s' if betti_0 != 1 else ''}, "
        f"{betti_1} cycle{'s' if betti_1 != 1 else ''}"
    )
    if betti_2 > 0:
        topology_description += f", {betti_2} void{'s' if betti_2 != 1 else ''}"

    # Calculate quality score based on stability and complexity
    quality_score = 1.0
    if homological_stability and not homological_stability.is_stable:
        quality_score *= 0.8
    if not structural_complexity.within_bounds:
        quality_score *= 0.7

    return TopologicalQualityResult(
        metric_id=metric_id,
        table_id=table_id,
        table_name=table_name,
        computed_at=computed_at,
        betti_numbers=betti_numbers,
        persistence_diagrams=persistence_diagrams,
        stability=homological_stability,
        complexity=structural_complexity,
        persistent_cycles=getattr(db_metrics, "persistent_cycles", []),
        anomalies=getattr(db_metrics, "anomalies", []),
        orphaned_components=orphaned_components,
        topology_description=topology_description,
        quality_warnings=getattr(db_metrics, "quality_warnings", []),
        quality_score=quality_score,
        has_issues=len(anomalous_cycles) > 0 or orphaned_components > 0,
        persistent_entropy=db_metrics.persistent_entropy,
    )
