"""Quality Synthesis (Pillar 5).

Aggregates quality metrics from all 4 pillars into dimensional quality scores
and unified quality assessment.

Architecture:
- Pillar 1 (Statistical): Benford, outliers, VIF, distribution stability
- Pillar 2 (Topological): Betti numbers, persistence, structural complexity
- Pillar 3 (Semantic): Used for labeling/context, not quality assessment
- Pillar 4 (Temporal): Seasonality, trends, completeness, freshness
- Pillar 5 (Quality Synthesis): Aggregates 1, 2, 4 + domain quality

Quality Dimensions:
- Completeness: Null ratios, temporal gaps, missing periods
- Validity: Type inference confidence, parse rates, outliers
- Consistency: Correlation violations, functional dependency breaks
- Uniqueness: Cardinality analysis, duplicate detection
- Timeliness: Temporal freshness, update frequency
- Accuracy: Benford compliance, domain rule violations
"""

import time
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models.base import Result
from dataraum_context.core.models.quality_synthesis import (
    ColumnQualityAssessment,
    DimensionScore,
    QualityDimension,
    QualitySeverity,
    QualitySynthesisResult,
    TableQualityAssessment,
)
from dataraum_context.core.models.quality_synthesis import (
    QualityIssue as QualitySynthesisIssue,
)
from dataraum_context.storage.models_v2.core import Column, Table
from dataraum_context.storage.models_v2.correlation_context import (
    ColumnCorrelation,
    FunctionalDependency,
)
from dataraum_context.storage.models_v2.domain_quality import (
    DomainQualityMetrics,
    FinancialQualityMetrics,
)
from dataraum_context.storage.models_v2.statistical_context import (
    StatisticalProfile,
    StatisticalQualityMetrics,
)
from dataraum_context.storage.models_v2.temporal_context import TemporalQualityMetrics
from dataraum_context.storage.models_v2.topological_context import (
    TopologicalQualityMetrics,
)

# ============================================================================
# Dimension Scoring Functions
# ============================================================================


def _compute_completeness_score(
    null_ratio: float | None,
    temporal_completeness: float | None,
) -> tuple[float, str]:
    """Compute completeness score from null ratio and temporal completeness.

    Args:
        null_ratio: Ratio of null values (0-1)
        temporal_completeness: Temporal completeness ratio (0-1)

    Returns:
        Tuple of (score, explanation)
    """
    # Start with perfect score
    score = 1.0
    factors = []

    if null_ratio is not None:
        # Penalize based on null ratio
        score *= 1.0 - null_ratio
        factors.append(f"{(1.0 - null_ratio) * 100:.1f}% non-null")

    if temporal_completeness is not None:
        # Penalize based on temporal gaps
        score *= temporal_completeness
        factors.append(f"{temporal_completeness * 100:.1f}% temporally complete")

    if not factors:
        return 1.0, "No completeness metrics available"

    explanation = "Completeness based on: " + ", ".join(factors)
    return score, explanation


def _compute_validity_score(
    parse_success_rate: float | None,
    outlier_ratio: float | None,
) -> tuple[float, str]:
    """Compute validity score from parse success and outlier detection.

    Args:
        parse_success_rate: % of values that parse to inferred type (0-1)
        outlier_ratio: Ratio of outliers detected (0-1)

    Returns:
        Tuple of (score, explanation)
    """
    score = 1.0
    factors = []

    if parse_success_rate is not None:
        score *= parse_success_rate
        factors.append(f"{parse_success_rate * 100:.1f}% parse successfully")

    if outlier_ratio is not None:
        # Penalize if >5% outliers
        if outlier_ratio > 0.05:
            penalty = min((outlier_ratio - 0.05) / 0.10, 0.5)  # Max 50% penalty
            score *= 1.0 - penalty
            factors.append(f"{outlier_ratio * 100:.1f}% outliers")

    if not factors:
        return 1.0, "No validity metrics available"

    explanation = "Validity based on: " + ", ".join(factors)
    return score, explanation


def _compute_consistency_score(
    vif_score: float | None,
    functional_dep_violations: int | None,
    orphaned_components: int | None,
    anomalous_cycles_count: int | None,
    high_correlations_count: int | None,
) -> tuple[float, str]:
    """Compute consistency score from multicollinearity, FD violations, and topology.

    Args:
        vif_score: VIF score (1-10+, higher = worse)
        functional_dep_violations: Number of FD violations
        orphaned_components: Number of disconnected subgraphs (structural issues)
        anomalous_cycles_count: Number of unexpected cycles
        high_correlations_count: Number of high correlations (>0.9)

    Returns:
        Tuple of (score, explanation)
    """
    score = 1.0
    factors = []

    if vif_score is not None:
        # VIF > 10 is problematic
        if vif_score > 10:
            penalty = min((vif_score - 10) / 20, 0.5)  # Max 50% penalty
            score *= 1.0 - penalty
            factors.append(f"VIF={vif_score:.1f}")

    if functional_dep_violations is not None and functional_dep_violations > 0:
        # Penalize FD violations
        penalty = min(functional_dep_violations * 0.1, 0.5)  # Max 50% penalty
        score *= 1.0 - penalty
        factors.append(f"{functional_dep_violations} FD violations")

    if orphaned_components is not None and orphaned_components > 0:
        # Orphaned components indicate structural inconsistencies
        penalty = min(orphaned_components * 0.15, 0.4)  # Max 40% penalty
        score *= 1.0 - penalty
        factors.append(f"{orphaned_components} disconnected components")

    if anomalous_cycles_count is not None and anomalous_cycles_count > 0:
        # Anomalous cycles indicate unexpected relationships
        penalty = min(anomalous_cycles_count * 0.1, 0.3)  # Max 30% penalty
        score *= 1.0 - penalty
        factors.append(f"{anomalous_cycles_count} anomalous cycles")

    if high_correlations_count is not None and high_correlations_count > 0:
        # High correlations may indicate redundancy or derived columns
        penalty = min(high_correlations_count * 0.05, 0.3)  # Max 30% penalty
        score *= 1.0 - penalty
        factors.append(f"{high_correlations_count} high correlations")

    if not factors:
        return 1.0, "No consistency metrics available"

    explanation = "Consistency based on: " + ", ".join(factors)
    return score, explanation


def _compute_uniqueness_score(
    cardinality_ratio: float | None,
    duplicate_count: int | None,
    total_count: int | None,
) -> tuple[float, str]:
    """Compute uniqueness score from cardinality and duplicates.

    Args:
        cardinality_ratio: distinct_count / total_count (0-1)
        duplicate_count: Number of duplicated values
        total_count: Total number of values

    Returns:
        Tuple of (score, explanation)
    """
    score = 1.0
    factors = []

    if cardinality_ratio is not None:
        # High cardinality is good for uniqueness
        score = cardinality_ratio
        factors.append(f"{cardinality_ratio * 100:.1f}% unique values")

    if duplicate_count is not None and total_count is not None and total_count > 0:
        dup_ratio = duplicate_count / total_count
        if dup_ratio > 0:
            factors.append(f"{dup_ratio * 100:.1f}% duplicates")

    if not factors:
        return 1.0, "No uniqueness metrics available"

    explanation = "Uniqueness based on: " + ", ".join(factors)
    return score, explanation


def _compute_timeliness_score(
    is_stale: bool | None,
    data_freshness_days: int | None,
) -> tuple[float, str]:
    """Compute timeliness score from staleness and freshness.

    Args:
        is_stale: Whether data is considered stale
        data_freshness_days: Days since last update

    Returns:
        Tuple of (score, explanation)
    """
    if is_stale is None and data_freshness_days is None:
        return 1.0, "No timeliness metrics available"

    score = 1.0
    factors = []

    if is_stale is True:
        score = 0.5  # Stale data gets 50% score
        factors.append("Data is stale")
    elif is_stale is False:
        factors.append("Data is fresh")

    if data_freshness_days is not None:
        # Penalize based on staleness (exponential decay)
        # 7 days = 100%, 30 days = 75%, 90 days = 50%, 180 days = 25%
        if data_freshness_days > 7:
            freshness_score = max(0.25, 1.0 - (data_freshness_days - 7) / 180)
            score *= freshness_score
            factors.append(f"{data_freshness_days} days old")

    explanation = "Timeliness based on: " + ", ".join(factors)
    return score, explanation


def _compute_accuracy_score(
    benford_compliant: bool | None,
    domain_compliance_score: float | None,
) -> tuple[float, str]:
    """Compute accuracy score from Benford and domain compliance.

    Args:
        benford_compliant: Whether Benford's Law is satisfied
        domain_compliance_score: Domain-specific compliance (0-1)

    Returns:
        Tuple of (score, explanation)
    """
    score = 1.0
    factors = []

    if benford_compliant is False:
        score *= 0.7  # 30% penalty for Benford violation
        factors.append("Benford's Law violated")
    elif benford_compliant is True:
        factors.append("Benford's Law satisfied")

    if domain_compliance_score is not None:
        score *= domain_compliance_score
        factors.append(f"{domain_compliance_score * 100:.1f}% domain compliant")

    if not factors:
        return 1.0, "No accuracy metrics available"

    explanation = "Accuracy based on: " + ", ".join(factors)
    return score, explanation


# ============================================================================
# Issue Aggregation
# ============================================================================


def _aggregate_statistical_issues(
    stat_quality: StatisticalQualityMetrics | None,
    column_id: str,
    column_name: str,
) -> list[QualitySynthesisIssue]:
    """Extract quality issues from statistical quality metrics."""
    if not stat_quality or not stat_quality.quality_issues:
        return []

    # Handle both list and dict formats
    issues_list = stat_quality.quality_issues
    if isinstance(issues_list, dict):
        issues_list = issues_list.get("issues", [])

    issues = []
    for issue_dict in issues_list:
        # Map severity
        severity_map = {
            "critical": QualitySeverity.CRITICAL,
            "error": QualitySeverity.ERROR,
            "warning": QualitySeverity.WARNING,
            "info": QualitySeverity.INFO,
        }
        severity = severity_map.get(issue_dict.get("severity", "warning"), QualitySeverity.WARNING)

        # Map to dimension
        issue_type = issue_dict.get("issue_type", "unknown")
        dimension_map = {
            "benford_violation": QualityDimension.ACCURACY,
            "outliers": QualityDimension.VALIDITY,
            "multicollinearity": QualityDimension.CONSISTENCY,
        }
        dimension = dimension_map.get(issue_type, QualityDimension.VALIDITY)

        issue = QualitySynthesisIssue(
            issue_id=str(uuid4()),
            issue_type=issue_type,
            severity=severity,
            dimension=dimension,
            column_id=column_id,
            column_name=column_name,
            description=issue_dict.get("description", "Statistical quality issue"),
            recommendation=None,
            evidence=issue_dict.get("evidence", {}),
            source_pillar=1,  # Statistical
            source_module="statistical_quality",
            detected_at=stat_quality.computed_at,
        )
        issues.append(issue)

    return issues


def _aggregate_temporal_issues(
    temp_quality: TemporalQualityMetrics | None,
    column_id: str,
    column_name: str,
) -> list[QualitySynthesisIssue]:
    """Extract quality issues from temporal quality metrics."""
    if not temp_quality or not temp_quality.quality_issues:
        return []

    # Handle both list and dict formats
    issues_list = temp_quality.quality_issues
    if isinstance(issues_list, dict):
        issues_list = issues_list.get("issues", [])

    issues = []
    for issue_dict in issues_list:
        severity_map = {
            "critical": QualitySeverity.CRITICAL,
            "error": QualitySeverity.ERROR,
            "warning": QualitySeverity.WARNING,
            "info": QualitySeverity.INFO,
        }
        severity = severity_map.get(issue_dict.get("severity", "warning"), QualitySeverity.WARNING)

        # Map to dimension
        issue_type = issue_dict.get("issue_type", "unknown")
        dimension_map = {
            "low_completeness": QualityDimension.COMPLETENESS,
            "large_gap": QualityDimension.COMPLETENESS,
            "stale_data": QualityDimension.TIMELINESS,
            "many_change_points": QualityDimension.CONSISTENCY,
            "unstable_distribution": QualityDimension.CONSISTENCY,
        }
        dimension = dimension_map.get(issue_type, QualityDimension.COMPLETENESS)

        issue = QualitySynthesisIssue(
            issue_id=str(uuid4()),
            issue_type=issue_type,
            severity=severity,
            dimension=dimension,
            column_id=column_id,
            column_name=column_name,
            description=issue_dict.get("description", "Temporal quality issue"),
            recommendation=None,
            evidence=issue_dict.get("evidence", {}),
            source_pillar=4,  # Temporal
            source_module="temporal_quality",
            detected_at=temp_quality.computed_at,
        )
        issues.append(issue)

    return issues


def _aggregate_topological_issues(
    topo_quality: TopologicalQualityMetrics | None,
    table_id: str,
    table_name: str,
) -> list[QualitySynthesisIssue]:
    """Extract quality issues from topological quality metrics (table-level)."""
    if not topo_quality:
        return []

    issues = []

    # Check for orphaned components
    if topo_quality.orphaned_components and topo_quality.orphaned_components > 0:
        issues.append(
            QualitySynthesisIssue(
                issue_id=str(uuid4()),
                issue_type="orphaned_components",
                severity=QualitySeverity.WARNING,
                dimension=QualityDimension.CONSISTENCY,
                table_id=table_id,
                column_id=None,
                column_name=None,
                description=(
                    f"{topo_quality.orphaned_components} disconnected structural "
                    "components detected"
                ),
                recommendation="Investigate structural relationships and data integrity",
                evidence={"orphaned_count": topo_quality.orphaned_components},
                source_pillar=2,  # Topological
                source_module="topological_quality",
                detected_at=topo_quality.computed_at,
            )
        )

    # Check for anomalous cycles
    if topo_quality.anomalous_cycles:
        cycle_count = len(topo_quality.anomalous_cycles.get("cycles", []))
        if cycle_count > 0:
            issues.append(
                QualitySynthesisIssue(
                    issue_id=str(uuid4()),
                    issue_type="anomalous_cycles",
                    severity=QualitySeverity.WARNING,
                    dimension=QualityDimension.CONSISTENCY,
                    table_id=table_id,
                    column_id=None,
                    column_name=None,
                    description=f"{cycle_count} anomalous relationship cycles detected",
                    recommendation="Review circular relationships for logical correctness",
                    evidence={"cycle_count": cycle_count},
                    source_pillar=2,  # Topological
                    source_module="topological_quality",
                    detected_at=topo_quality.computed_at,
                )
            )

    # Check for homological instability
    if topo_quality.homologically_stable is False:
        issues.append(
            QualitySynthesisIssue(
                issue_id=str(uuid4()),
                issue_type="structural_instability",
                severity=QualitySeverity.INFO,
                dimension=QualityDimension.CONSISTENCY,
                table_id=table_id,
                column_id=None,
                column_name=None,
                description="Structural topology has changed significantly from baseline",
                recommendation="Review for data schema changes or relationship drift",
                evidence={"bottleneck_distance": topo_quality.bottleneck_distance},
                source_pillar=2,  # Topological
                source_module="topological_quality",
                detected_at=topo_quality.computed_at,
            )
        )

    return issues


def _aggregate_correlation_issues(
    column_id: str,
    column_name: str,
    high_correlations: list[ColumnCorrelation],
    fd_violations: list[FunctionalDependency],
) -> list[QualitySynthesisIssue]:
    """Extract quality issues from correlation metrics."""
    issues = []

    # High correlations indicate potential redundancy
    if high_correlations:
        for corr in high_correlations[:3]:  # Top 3
            other_col_id = corr.column2_id if corr.column1_id == column_id else corr.column1_id
            corr_value = corr.pearson_r if corr.pearson_r else corr.spearman_rho
            issues.append(
                QualitySynthesisIssue(
                    issue_id=str(uuid4()),
                    issue_type="high_correlation",
                    severity=QualitySeverity.INFO,
                    dimension=QualityDimension.CONSISTENCY,
                    table_id=corr.table_id,
                    column_id=column_id,
                    column_name=column_name,
                    description=f"Very high correlation ({corr_value:.2f}) with another column",
                    recommendation="Consider if one column is derived or redundant",
                    evidence={"other_column_id": other_col_id, "correlation": corr_value},
                    source_pillar=1,  # Statistical (correlation is part of statistical analysis)
                    source_module="correlation",
                    detected_at=corr.computed_at,
                )
            )

    # Functional dependency violations
    if fd_violations:
        for fd in fd_violations[:3]:  # Top 3
            issues.append(
                QualitySynthesisIssue(
                    issue_id=str(uuid4()),
                    issue_type="fd_violation",
                    severity=QualitySeverity.WARNING,
                    dimension=QualityDimension.CONSISTENCY,
                    table_id=fd.table_id,
                    column_id=column_id,
                    column_name=column_name,
                    description=f"Functional dependency violated {fd.violation_count} times",
                    recommendation="Investigate data integrity and normalization",
                    evidence={
                        "violation_count": fd.violation_count,
                        "confidence": fd.confidence,
                    },
                    source_pillar=1,  # Statistical
                    source_module="correlation",
                    detected_at=fd.computed_at,
                )
            )

    return issues


def _aggregate_domain_quality_issues(
    domain_quality: DomainQualityMetrics | None,
    column_id: str,
    column_name: str,
) -> list[QualitySynthesisIssue]:
    """Extract quality issues from domain quality metrics (table-level).

    Note: Domain quality is table-level, so column_id/column_name may be None.
    """
    if not domain_quality or not domain_quality.violations:
        return []

    # Handle both list and dict formats
    violations_list = domain_quality.violations
    if isinstance(violations_list, dict):
        violations_list = violations_list.get("violations", [])

    issues = []
    for violation in violations_list[:5]:  # Top 5
        severity_map = {
            "critical": QualitySeverity.CRITICAL,
            "high": QualitySeverity.ERROR,
            "medium": QualitySeverity.WARNING,
            "low": QualitySeverity.INFO,
        }
        severity = severity_map.get(violation.get("severity", "medium"), QualitySeverity.WARNING)

        issue = QualitySynthesisIssue(
            issue_id=str(uuid4()),
            issue_type="domain_rule_violation",
            severity=severity,
            dimension=QualityDimension.ACCURACY,
            table_id=str(domain_quality.table_id),  # Convert UUID to string
            column_id=column_id,
            column_name=column_name,
            description=violation.get("description", "Domain rule violation"),
            recommendation=violation.get("recommendation"),
            evidence=violation.get("evidence", {}),
            source_pillar=5,  # Quality (domain quality is part of quality pillar)
            source_module="domain_quality",
            detected_at=domain_quality.computed_at,
        )
        issues.append(issue)

    return issues


# ============================================================================
# Column Quality Assessment
# ============================================================================


async def assess_column_quality(
    column: Column,
    session: AsyncSession,
) -> Result[ColumnQualityAssessment]:
    """Assess quality for a single column by aggregating all pillar metrics.

    Args:
        column: Column to assess
        session: Database session

    Returns:
        Result containing ColumnQualityAssessment
    """
    try:
        # Fetch statistical profile and quality
        stmt = (
            select(StatisticalProfile)
            .where(StatisticalProfile.column_id == column.column_id)
            .order_by(StatisticalProfile.profiled_at.desc())
            .limit(1)
        )
        stat_profile = (await session.execute(stmt)).scalar_one_or_none()

        stmt = (
            select(StatisticalQualityMetrics)
            .where(StatisticalQualityMetrics.column_id == column.column_id)
            .order_by(StatisticalQualityMetrics.computed_at.desc())
            .limit(1)
        )
        stat_quality = (await session.execute(stmt)).scalar_one_or_none()

        # Fetch temporal quality
        stmt = (
            select(TemporalQualityMetrics)
            .where(TemporalQualityMetrics.column_id == column.column_id)
            .order_by(TemporalQualityMetrics.computed_at.desc())
            .limit(1)
        )
        temp_quality = (await session.execute(stmt)).scalar_one_or_none()

        # Fetch correlation metrics for this column
        # Count high correlations (>0.9)
        stmt = select(ColumnCorrelation).where(
            (ColumnCorrelation.column1_id == column.column_id)
            | (ColumnCorrelation.column2_id == column.column_id)
        )
        correlations = (await session.execute(stmt)).scalars().all()
        high_correlations_count = sum(
            1
            for corr in correlations
            if (corr.pearson_r and abs(corr.pearson_r) > 0.9)
            or (corr.spearman_rho and abs(corr.spearman_rho) > 0.9)
        )

        # Count functional dependency violations for this column
        # Note: FunctionalDependency uses determinant_column_ids (JSON list)
        # For simplicity, we check if this column is the dependent
        stmt = select(FunctionalDependency).where(
            FunctionalDependency.dependent_column_id == column.column_id
        )
        fd_results = (await session.execute(stmt)).scalars().all()
        # Also check if column is in determinant list
        all_fds = (await session.execute(select(FunctionalDependency))).scalars().all()
        fd_results_with_determinant = [
            fd for fd in all_fds if column.column_id in fd.determinant_column_ids
        ]
        fd_results = list(fd_results) + fd_results_with_determinant
        fd_violations = sum(1 for fd in fd_results if fd.violation_count and fd.violation_count > 0)

        # Compute dimensional scores
        dimension_scores = []

        # Completeness
        null_ratio = stat_profile.null_ratio if stat_profile else None
        temporal_completeness = temp_quality.completeness_ratio if temp_quality else None
        score, explanation = _compute_completeness_score(null_ratio, temporal_completeness)
        dimension_scores.append(
            DimensionScore(
                dimension=QualityDimension.COMPLETENESS,
                score=score,
                completeness_ratio=temporal_completeness,
                null_ratio=null_ratio,
                explanation=explanation,
            )
        )

        # Validity
        parse_rate = None  # TODO: Get from type inference
        outlier_ratio = stat_quality.iqr_outlier_ratio if stat_quality else None
        score, explanation = _compute_validity_score(parse_rate, outlier_ratio)
        dimension_scores.append(
            DimensionScore(
                dimension=QualityDimension.VALIDITY,
                score=score,
                parse_success_rate=parse_rate,
                explanation=explanation,
            )
        )

        # Consistency
        vif_score = stat_quality.vif_score if stat_quality else None
        # Note: Topological metrics are table-level, handled in table assessment
        score, explanation = _compute_consistency_score(
            vif_score,
            fd_violations if fd_violations > 0 else None,
            None,  # orphaned_components - table-level
            None,  # anomalous_cycles_count - table-level
            high_correlations_count if high_correlations_count > 0 else None,
        )
        dimension_scores.append(
            DimensionScore(
                dimension=QualityDimension.CONSISTENCY,
                score=score,
                explanation=explanation,
            )
        )

        # Uniqueness
        cardinality_ratio = stat_profile.cardinality_ratio if stat_profile else None
        duplicate_count = stat_profile.duplicate_count if stat_profile else None
        total_count = stat_profile.total_count if stat_profile else None
        score, explanation = _compute_uniqueness_score(
            cardinality_ratio, duplicate_count, total_count
        )
        dimension_scores.append(
            DimensionScore(
                dimension=QualityDimension.UNIQUENESS,
                score=score,
                explanation=explanation,
            )
        )

        # Timeliness
        is_stale = temp_quality.is_stale if temp_quality else None
        freshness_days = temp_quality.data_freshness_days if temp_quality else None
        score, explanation = _compute_timeliness_score(is_stale, freshness_days)
        dimension_scores.append(
            DimensionScore(
                dimension=QualityDimension.TIMELINESS,
                score=score,
                explanation=explanation,
            )
        )

        # Accuracy (domain quality is table-level, not column-level)
        benford_compliant = stat_quality.benford_compliant if stat_quality else None
        # Note: Domain compliance is aggregated at table level, not used here
        score, explanation = _compute_accuracy_score(benford_compliant, None)
        dimension_scores.append(
            DimensionScore(
                dimension=QualityDimension.ACCURACY,
                score=score,
                explanation=explanation,
            )
        )

        # Aggregate issues from all sources
        issues = []
        issues.extend(
            _aggregate_statistical_issues(stat_quality, column.column_id, column.column_name)
        )
        issues.extend(
            _aggregate_temporal_issues(temp_quality, column.column_id, column.column_name)
        )

        # Correlation issues
        high_corr_list = [
            corr
            for corr in correlations
            if (corr.pearson_r and abs(corr.pearson_r) > 0.9)
            or (corr.spearman_rho and abs(corr.spearman_rho) > 0.9)
        ]
        fd_violation_list = [
            fd for fd in fd_results if fd.violation_count and fd.violation_count > 0
        ]
        issues.extend(
            _aggregate_correlation_issues(
                column.column_id, column.column_name, high_corr_list, fd_violation_list
            )
        )

        # Note: Domain quality is table-level, handled in table assessment

        # Update issue counts in dimension scores
        for dim_score in dimension_scores:
            dim_issues = [i for i in issues if i.dimension == dim_score.dimension]
            dim_score.issue_count = len(dim_issues)
            dim_score.critical_issues = len(
                [i for i in dim_issues if i.severity == QualitySeverity.CRITICAL]
            )

        # Compute overall score (weighted average)
        overall_score = sum(ds.score for ds in dimension_scores) / len(dimension_scores)

        assessment = ColumnQualityAssessment(
            column_id=column.column_id,
            column_name=column.column_name,
            dimension_scores=dimension_scores,
            overall_score=overall_score,
            issues=issues,
            has_statistical_quality=stat_quality is not None,
            has_temporal_quality=temp_quality is not None,
            has_semantic_context=False,  # TODO: Check semantic annotations
            assessed_at=datetime.now(UTC),
        )

        return Result.ok(assessment)

    except Exception as e:
        return Result.fail(f"Column quality assessment failed: {e}")


# ============================================================================
# Table Quality Assessment
# ============================================================================


async def assess_table_quality(
    table_id: str,
    session: AsyncSession,
) -> Result[QualitySynthesisResult]:
    """Assess quality for a table and all its columns.

    Args:
        table_id: Table ID to assess
        session: Database session

    Returns:
        Result containing QualitySynthesisResult
    """
    start_time = time.time()

    try:
        # Get table
        table = await session.get(Table, table_id)
        if not table:
            return Result.fail(f"Table not found: {table_id}")

        # Get all columns
        stmt = select(Column).where(Column.table_id == table_id)
        columns = (await session.execute(stmt)).scalars().all()

        # Assess each column
        column_assessments = []
        for column in columns:
            result = await assess_column_quality(column, session)
            if result.success:
                column_assessments.append(result.value)

        # Aggregate column scores for table-level scores
        if column_assessments:
            avg_scores_by_dimension = {}
            for dim in QualityDimension:
                dim_scores = []
                for col_assessment in column_assessments:
                    for dim_score in col_assessment.dimension_scores:
                        if dim_score.dimension == dim:
                            dim_scores.append(dim_score.score)
                if dim_scores:
                    avg_scores_by_dimension[dim] = sum(dim_scores) / len(dim_scores)

            table_dimension_scores = [
                DimensionScore(
                    dimension=dim,
                    score=avg_scores_by_dimension.get(dim, 1.0),
                    explanation=f"Average {dim.value} across {len(column_assessments)} columns",
                )
                for dim in QualityDimension
            ]

            table_overall_score = sum(ds.score for ds in table_dimension_scores) / len(
                table_dimension_scores
            )
        else:
            table_dimension_scores = []
            table_overall_score = 1.0

        # Get table-level quality metrics
        stmt = (
            select(TopologicalQualityMetrics)
            .where(TopologicalQualityMetrics.table_id == table_id)
            .order_by(TopologicalQualityMetrics.computed_at.desc())
            .limit(1)
        )
        topo_quality = (await session.execute(stmt)).scalar_one_or_none()

        stmt = (
            select(FinancialQualityMetrics)
            .where(FinancialQualityMetrics.table_id == table_id)
            .order_by(FinancialQualityMetrics.computed_at.desc())
            .limit(1)
        )
        financial_quality = (await session.execute(stmt)).scalar_one_or_none()

        # Add table-level topological issues
        table_level_issues = []
        if topo_quality:
            table_level_issues.extend(
                _aggregate_topological_issues(topo_quality, table_id, table.table_name)
            )

        # Adjust table-level consistency score based on topological metrics
        if topo_quality and table_dimension_scores:
            # Find consistency dimension score
            for dim_score in table_dimension_scores:
                if dim_score.dimension == QualityDimension.CONSISTENCY:
                    # Apply topological penalties
                    orphaned = topo_quality.orphaned_components or 0
                    anomalous = (
                        len(topo_quality.anomalous_cycles.get("cycles", []))
                        if topo_quality.anomalous_cycles
                        else 0
                    )

                    topological_penalty = 0.0
                    if orphaned > 0:
                        topological_penalty += min(orphaned * 0.15, 0.4)
                    if anomalous > 0:
                        topological_penalty += min(anomalous * 0.1, 0.3)

                    if topological_penalty > 0:
                        dim_score.score = max(0.0, dim_score.score * (1.0 - topological_penalty))
                        dim_score.explanation += (
                            f" (adjusted for topological issues: {orphaned} orphaned, "
                            f"{anomalous} anomalous cycles)"
                        )
                    break

        # Recalculate overall score if we adjusted consistency
        if table_dimension_scores:
            table_overall_score = sum(ds.score for ds in table_dimension_scores) / len(
                table_dimension_scores
            )

        # Create table assessment
        table_assessment = TableQualityAssessment(
            table_id=table_id,
            table_name=table.table_name,
            dimension_scores=table_dimension_scores,
            overall_score=table_overall_score,
            column_assessments=column_assessments,
            issues=table_level_issues,
            has_statistical_quality=any(ca.has_statistical_quality for ca in column_assessments),
            has_topological_quality=topo_quality is not None,
            has_temporal_quality=any(ca.has_temporal_quality for ca in column_assessments),
            has_domain_quality=financial_quality is not None,
            assessed_at=datetime.now(UTC),
        )

        # Aggregate all issues (column-level + table-level)
        all_issues = []
        for col_assessment in column_assessments:
            all_issues.extend(col_assessment.issues)
        all_issues.extend(table_level_issues)

        # Count issues
        total_issues = len(all_issues)
        critical_issues = len([i for i in all_issues if i.severity == QualitySeverity.CRITICAL])
        warnings = len([i for i in all_issues if i.severity == QualitySeverity.WARNING])

        # Issues by dimension
        issues_by_dimension = {}
        for issue in all_issues:
            dim_name = issue.dimension.value
            issues_by_dimension[dim_name] = issues_by_dimension.get(dim_name, 0) + 1

        # Issues by pillar
        issues_by_pillar = {}
        for issue in all_issues:
            pillar = issue.source_pillar
            issues_by_pillar[pillar] = issues_by_pillar.get(pillar, 0) + 1

        # Create synthesis result
        synthesis = QualitySynthesisResult(
            table_id=table_id,
            table_name=table.table_name,
            table_assessment=table_assessment,
            total_columns=len(columns),
            columns_assessed=len(column_assessments),
            total_issues=total_issues,
            critical_issues=critical_issues,
            warnings=warnings,
            issues_by_dimension=issues_by_dimension,
            issues_by_pillar=issues_by_pillar,
            quality_summary=None,  # TODO: LLM-generated summary
            top_recommendations=[],  # TODO: Prioritize recommendations
            synthesis_duration_seconds=time.time() - start_time,
            synthesized_at=datetime.now(UTC),
        )

        return Result.ok(synthesis)

    except Exception as e:
        return Result.fail(f"Table quality synthesis failed: {e}")
