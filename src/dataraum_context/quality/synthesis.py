"""Quality Synthesis (Pillar 5).

Aggregates quality metrics from all 4 pillars into dimensional quality scores
and unified quality assessment.

Architecture:
- Pillar 1 (Statistical): Benford, outliers (IQR + Isolation Forest), multicollinearity (VIF, Tolerance, Condition Index)
- Pillar 2 (Topological): Betti numbers, persistence, structural complexity
- Pillar 3 (Semantic): Used for labeling/context, not quality assessment
- Pillar 4 (Temporal): Seasonality, trends, completeness, freshness, distribution stability
- Pillar 5 (Quality Synthesis): Aggregates 1, 2, 4 + domain quality

Quality Dimensions:
- Completeness: Null ratios, temporal gaps, missing periods
- Validity: Type inference confidence, parse rates, outliers
- Consistency: Multicollinearity (VIF, Condition Index), correlation violations, functional dependency breaks
- Uniqueness: Cardinality analysis, duplicate detection
- Timeliness: Temporal freshness, update frequency
- Accuracy: Benford compliance, domain rule violations
"""

import logging
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import duckdb
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models.base import Result
from dataraum_context.quality.models import (
    ColumnQualityAssessment,
    DatasetQualitySynthesisResult,
    DimensionScore,
    QualityDimension,
    QualitySynthesisIssue,
    QualitySynthesisResult,
    QualitySynthesisSeverity,
    TableQualityAssessment,
)
from dataraum_context.storage.models_v2.core import Column, Table
from dataraum_context.storage.models_v2.correlation_context import (
    ColumnCorrelation,
    FunctionalDependency,
    MulticollinearityMetrics,
)
from dataraum_context.storage.models_v2.domain_quality import (
    DomainQualityMetrics,
    FinancialQualityMetrics,
)
from dataraum_context.storage.models_v2.semantic_context import SemanticAnnotation
from dataraum_context.storage.models_v2.statistical_context import (
    StatisticalProfile,
    StatisticalQualityMetrics,
)
from dataraum_context.storage.models_v2.temporal_context import TemporalQualityMetrics
from dataraum_context.storage.models_v2.topological_context import (
    TopologicalQualityMetrics,
)
from dataraum_context.storage.models_v2.type_inference import TypeCandidate, TypeDecision

if TYPE_CHECKING:
    from dataraum_context.llm import LLMService

logger = logging.getLogger(__name__)

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
    skewness: float | None = None,
    kurtosis: float | None = None,
    cv: float | None = None,
) -> tuple[float, str]:
    """Compute validity score from parse success and outlier detection.

    Distribution shape metrics (skewness, kurtosis, CV) provide context for outliers
    but do not penalize the score. They help explain whether outliers are expected.

    Args:
        parse_success_rate: % of values that parse to inferred type (0-1)
        outlier_ratio: Ratio of outliers detected (0-1)
        skewness: Distribution asymmetry (Phase 2 metric)
        kurtosis: Tail heaviness (Phase 2 metric)
        cv: Coefficient of variation - dispersion (Phase 2 metric)

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

    # Phase 2 distribution shape context (informational, no penalty)
    if skewness is not None and abs(skewness) > 2.0:
        direction = "right" if skewness > 0 else "left"
        factors.append(f"skewness={skewness:.2f} ({direction}-skewed)")

    if kurtosis is not None and abs(kurtosis) > 5.0:
        factors.append(f"kurtosis={kurtosis:.2f} (heavy-tailed)")

    if cv is not None and cv > 1.0:
        factors.append(f"CV={cv:.2f} (high dispersion)")

    if not factors:
        return 1.0, "No validity metrics available"

    explanation = "Validity based on: " + ", ".join(factors)
    return score, explanation


def _compute_consistency_score(
    multicollinearity_severity: str | None,  # "none", "moderate", "severe"
    max_vif: float | None,
    condition_index: float | None,
    functional_dep_violations: int | None,
    orphaned_components: int | None,
    anomalous_cycles_count: int | None,
    high_correlations_count: int | None,
) -> tuple[float, str]:
    """Compute consistency score from multicollinearity, FD violations, and topology.

    Args:
        multicollinearity_severity: Overall severity ("none", "moderate", "severe")
        max_vif: Maximum VIF value across columns
        condition_index: Table-level Condition Index
        functional_dep_violations: Number of FD violations
        orphaned_components: Number of disconnected subgraphs (structural issues)
        anomalous_cycles_count: Number of unexpected cycles
        high_correlations_count: Number of high correlations (>0.9)

    Returns:
        Tuple of (score, explanation)
    """
    score = 1.0
    factors = []

    # Multicollinearity penalty (table-level severity)
    if multicollinearity_severity == "severe":
        score *= 0.5  # 50% penalty
        factors.append("Severe multicollinearity")
        if condition_index:
            factors.append(f"CI={condition_index:.1f}")
    elif multicollinearity_severity == "moderate":
        score *= 0.75  # 25% penalty
        factors.append("Moderate multicollinearity")

    # Column-level VIF (for context)
    if max_vif and max_vif > 10:
        if "multicollinearity" not in " ".join(factors).lower():
            factors.append(f"Max VIF={max_vif:.1f}")

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
    """Extract quality issues from statistical quality metrics using Pydantic deserialization."""
    if not stat_quality or not stat_quality.quality_data:
        return []

    # Deserialize JSONB to Pydantic model
    from dataraum_context.profiling.models import StatisticalQualityResult

    try:
        quality_result = StatisticalQualityResult.model_validate(stat_quality.quality_data)
        issues_list = quality_result.quality_issues
    except Exception:
        # Fallback to dict access if deserialization fails
        issues_list = stat_quality.quality_data.get("quality_issues", [])

    if not issues_list:
        return []

    issues = []
    for quality_issue in issues_list:
        # Handle both dict (from Pydantic) and QualityIssue object (shouldn't happen but be defensive)
        if isinstance(quality_issue, dict):
            issue_type = quality_issue.get("issue_type", "unknown")
            severity_str = quality_issue.get("severity", "warning")
            description = quality_issue.get("description", "Statistical quality issue")
            evidence = quality_issue.get("evidence", {})
        else:
            # Shouldn't happen, but handle it
            issue_type = getattr(quality_issue, "issue_type", "unknown")
            severity_str = getattr(quality_issue, "severity", "warning")
            description = getattr(quality_issue, "description", "Statistical quality issue")
            evidence = getattr(quality_issue, "evidence", {})

        # Map severity
        severity_map = {
            "critical": QualitySynthesisSeverity.CRITICAL,
            "error": QualitySynthesisSeverity.ERROR,
            "warning": QualitySynthesisSeverity.WARNING,
            "info": QualitySynthesisSeverity.INFO,
        }
        severity = severity_map.get(severity_str, QualitySynthesisSeverity.WARNING)

        # Map to dimension
        dimension_map = {
            "benford_violation": QualityDimension.ACCURACY,
            "outliers": QualityDimension.VALIDITY,
            "outliers_iqr": QualityDimension.VALIDITY,
            "outliers_isolation_forest": QualityDimension.VALIDITY,
            "distribution_shift": QualityDimension.CONSISTENCY,
            "multicollinearity": QualityDimension.CONSISTENCY,
        }
        dimension = dimension_map.get(issue_type, QualityDimension.VALIDITY)

        issue = QualitySynthesisIssue(
            issue_id=str(uuid4()),
            issue_type=issue_type,
            severity=severity,
            dimension=dimension,
            table_id=None,
            column_id=column_id,
            column_name=column_name,
            description=description,
            recommendation=None,
            evidence=evidence,
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
    """Extract quality issues from temporal quality metrics using Pydantic deserialization."""
    if not temp_quality or not temp_quality.temporal_data:
        return []

    # Deserialize JSONB to Pydantic model
    from dataraum_context.quality.models import TemporalQualityResult

    try:
        quality_result = TemporalQualityResult.model_validate(temp_quality.temporal_data)
        issues_list = quality_result.quality_issues
    except Exception:
        # Fallback to dict access if deserialization fails
        issues_list = temp_quality.temporal_data.get("quality_issues", [])

    if not issues_list:
        return []

    issues = []
    for quality_issue in issues_list:
        # Handle both QualityIssue object and dict
        from dataraum_context.quality.models import QualityIssue as QualityIssueModel

        if isinstance(quality_issue, QualityIssueModel):
            # It's a Pydantic QualityIssue
            issue_type = quality_issue.issue_type
            severity_str = quality_issue.severity
            description = quality_issue.description
            evidence = quality_issue.evidence
        else:
            # It's a dict - cast to dict for type safety
            issue_dict: dict[str, Any] = quality_issue  # type: ignore[assignment]
            issue_type = issue_dict.get("issue_type", "unknown")
            severity_str = issue_dict.get("severity", "warning")
            description = issue_dict.get("description", "Temporal quality issue")
            evidence = issue_dict.get("evidence", {})

        severity_map = {
            "critical": QualitySynthesisSeverity.CRITICAL,
            "error": QualitySynthesisSeverity.ERROR,
            "warning": QualitySynthesisSeverity.WARNING,
            "info": QualitySynthesisSeverity.INFO,
        }
        severity = severity_map.get(severity_str, QualitySynthesisSeverity.WARNING)

        # Map to dimension
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
            table_id=None,
            column_id=column_id,
            column_name=column_name,
            description=description,
            recommendation=None,
            evidence=evidence,
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
                severity=QualitySynthesisSeverity.WARNING,
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

    # Check for anomalous cycles (deserialize from JSONB)
    if topo_quality.topology_data:
        try:
            from dataraum_context.quality.models import TopologicalQualityResult

            topology_result = TopologicalQualityResult.model_validate(topo_quality.topology_data)
            anomalous_cycles = topology_result.anomalous_cycles
        except Exception:
            # Fallback to dict access
            anomalous_cycles = topo_quality.topology_data.get("anomalous_cycles", [])

        if anomalous_cycles and len(anomalous_cycles) > 0:
            cycle_count = len(anomalous_cycles)
            issues.append(
                QualitySynthesisIssue(
                    issue_id=str(uuid4()),
                    issue_type="anomalous_cycles",
                    severity=QualitySynthesisSeverity.WARNING,
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
        # Extract bottleneck_distance from JSONB topology_data
        bottleneck_distance = topo_quality.topology_data.get("bottleneck_distance")

        issues.append(
            QualitySynthesisIssue(
                issue_id=str(uuid4()),
                issue_type="structural_instability",
                severity=QualitySynthesisSeverity.INFO,
                dimension=QualityDimension.CONSISTENCY,
                table_id=table_id,
                column_id=None,
                column_name=None,
                description="Structural topology has changed significantly from baseline",
                recommendation="Review for data schema changes or relationship drift",
                evidence={"bottleneck_distance": bottleneck_distance},
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
                    severity=QualitySynthesisSeverity.INFO,
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
                    severity=QualitySynthesisSeverity.WARNING,
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
            "critical": QualitySynthesisSeverity.CRITICAL,
            "high": QualitySynthesisSeverity.ERROR,
            "medium": QualitySynthesisSeverity.WARNING,
            "low": QualitySynthesisSeverity.INFO,
        }
        severity = severity_map.get(
            violation.get("severity", "medium"), QualitySynthesisSeverity.WARNING
        )

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

        stat_quality_stmt = (
            select(StatisticalQualityMetrics)
            .where(StatisticalQualityMetrics.column_id == column.column_id)
            .order_by(StatisticalQualityMetrics.computed_at.desc())
            .limit(1)
        )
        stat_quality = (await session.execute(stat_quality_stmt)).scalar_one_or_none()

        # Fetch temporal quality
        temp_quality_stmt = (
            select(TemporalQualityMetrics)
            .where(TemporalQualityMetrics.column_id == column.column_id)
            .order_by(TemporalQualityMetrics.computed_at.desc())
            .limit(1)
        )
        temp_quality = (await session.execute(temp_quality_stmt)).scalar_one_or_none()

        # Fetch correlation metrics for this column
        # Count high correlations (>0.9)
        corr_stmt = select(ColumnCorrelation).where(
            (ColumnCorrelation.column1_id == column.column_id)
            | (ColumnCorrelation.column2_id == column.column_id)
        )
        correlations = (await session.execute(corr_stmt)).scalars().all()
        high_correlations_count = sum(
            1
            for corr in correlations
            if (corr.pearson_r and abs(corr.pearson_r) > 0.9)
            or (corr.spearman_rho and abs(corr.spearman_rho) > 0.9)
        )

        # Count functional dependency violations for this column
        # Note: FunctionalDependency uses determinant_column_ids (JSON list)
        # For simplicity, we check if this column is the dependent
        fd_stmt = select(FunctionalDependency).where(
            FunctionalDependency.dependent_column_id == column.column_id
        )
        fd_results = (await session.execute(fd_stmt)).scalars().all()
        # Also check if column is in determinant list
        all_fds = (await session.execute(select(FunctionalDependency))).scalars().all()
        fd_results_with_determinant = [
            fd for fd in all_fds if column.column_id in fd.determinant_column_ids
        ]
        fd_results = list(fd_results) + fd_results_with_determinant
        fd_violations = sum(1 for fd in fd_results if fd.violation_count and fd.violation_count > 0)

        # Fetch type decision to get parse rate
        type_decision_stmt = select(TypeDecision).where(TypeDecision.column_id == column.column_id)
        type_decision = (await session.execute(type_decision_stmt)).scalar_one_or_none()

        # If we have a type decision, get the parse rate from the winning type candidate
        parse_rate = None
        if type_decision:
            # Find the type candidate that matches the decided type
            type_candidate_stmt = (
                select(TypeCandidate)
                .where(TypeCandidate.column_id == column.column_id)
                .where(TypeCandidate.data_type == type_decision.decided_type)
                .order_by(TypeCandidate.confidence.desc())
                .limit(1)
            )
            type_candidate = (await session.execute(type_candidate_stmt)).scalar_one_or_none()
            if type_candidate and type_candidate.parse_success_rate is not None:
                parse_rate = type_candidate.parse_success_rate

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
                parse_success_rate=None,  # TODO: Calculate this
                null_ratio=null_ratio,
                explanation=explanation,
            )
        )

        # Validity
        # parse_rate was already fetched above from type_candidate.parse_success_rate
        # Get outlier ratio from outlier detection metrics
        outlier_ratio = None
        if stat_quality:
            # Use IQR outlier ratio if available, else isolation forest
            outlier_ratio = (
                stat_quality.iqr_outlier_ratio or stat_quality.isolation_forest_anomaly_ratio
            )

        # Get Phase 2 distribution shape metrics from JSONB profile_data
        # Deserialize JSONB into Pydantic model for type-safe access
        skewness = None
        kurtosis = None
        cv = None
        if stat_profile and stat_profile.profile_data:
            from dataraum_context.profiling.models import ColumnProfile

            # Pydantic handles deserialization automatically
            try:
                profile = ColumnProfile.model_validate(stat_profile.profile_data)
                if profile.numeric_stats:
                    skewness = profile.numeric_stats.skewness
                    kurtosis = profile.numeric_stats.kurtosis
                    cv = profile.numeric_stats.cv
            except Exception:
                # Fallback to dict access if model validation fails
                numeric_stats = stat_profile.profile_data.get("numeric_stats")
                if numeric_stats:
                    skewness = numeric_stats.get("skewness")
                    kurtosis = numeric_stats.get("kurtosis")
                    cv = numeric_stats.get("cv")

        score, explanation = _compute_validity_score(
            parse_rate, outlier_ratio, skewness, kurtosis, cv
        )
        dimension_scores.append(
            DimensionScore(
                dimension=QualityDimension.VALIDITY,
                score=score,
                completeness_ratio=None,  # TODO: Calculate this
                null_ratio=None,  # TODO: Calculate this
                parse_success_rate=parse_rate,
                explanation=explanation,
            )
        )

        # Consistency
        # Get multicollinearity analysis for table
        multicollinearity_stmt = (
            select(MulticollinearityMetrics)
            .where(MulticollinearityMetrics.table_id == column.table_id)
            .order_by(MulticollinearityMetrics.computed_at.desc())
        )
        multicollinearity_result = await session.execute(multicollinearity_stmt)
        multicollinearity = multicollinearity_result.scalar_one_or_none()

        # Extract severity and metrics
        if multicollinearity and multicollinearity.analysis_data:
            multicollinearity_severity = multicollinearity.analysis_data.get("overall_severity")
            condition_index_val = multicollinearity.condition_index
            max_vif_val = multicollinearity.max_vif
        else:
            multicollinearity_severity = None
            condition_index_val = None
            max_vif_val = None

        # Note: Topological metrics are table-level, handled in table assessment
        score, explanation = _compute_consistency_score(
            multicollinearity_severity,
            max_vif_val,
            condition_index_val,
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
                completeness_ratio=None,  # TODO: Calculate this
                null_ratio=None,  # TODO: Calculate this
                parse_success_rate=None,
            )
        )

        # Uniqueness
        cardinality_ratio = stat_profile.cardinality_ratio if stat_profile else None
        total_count = stat_profile.total_count if stat_profile else None

        # Calculate duplicate_count from cardinality
        duplicate_count = None
        if stat_profile and stat_profile.distinct_count is not None and total_count is not None:
            non_null_count = total_count - stat_profile.null_count
            duplicate_count = (
                non_null_count - stat_profile.distinct_count if non_null_count > 0 else 0
            )
        score, explanation = _compute_uniqueness_score(
            cardinality_ratio, duplicate_count, total_count
        )
        dimension_scores.append(
            DimensionScore(
                dimension=QualityDimension.UNIQUENESS,
                score=score,
                explanation=explanation,
                completeness_ratio=None,  # TODO: Calculate this
                null_ratio=None,  # TODO: Calculate this
                parse_success_rate=None,
            )
        )

        # Timeliness
        # Get timeliness metrics from temporal quality
        is_stale = temp_quality.is_stale if temp_quality else None
        freshness_days = None
        if temp_quality and temp_quality.temporal_data:
            # Extract from JSONB
            freshness_days = temp_quality.temporal_data.get("data_freshness_days")
        score, explanation = _compute_timeliness_score(is_stale, freshness_days)
        dimension_scores.append(
            DimensionScore(
                dimension=QualityDimension.TIMELINESS,
                score=score,
                explanation=explanation,
                completeness_ratio=None,  # TODO: Calculate this
                null_ratio=None,  # TODO: Calculate this
                parse_success_rate=None,
            )
        )

        # Accuracy (domain quality is table-level, not column-level)
        # Get Benford compliance from statistical quality metrics
        benford_compliant = None
        if stat_quality:
            benford_compliant = stat_quality.benford_compliant
        # Note: Domain compliance is aggregated at table level, not used here
        score, explanation = _compute_accuracy_score(benford_compliant, None)
        dimension_scores.append(
            DimensionScore(
                dimension=QualityDimension.ACCURACY,
                score=score,
                explanation=explanation,
                completeness_ratio=None,  # TODO: Calculate this
                null_ratio=None,  # TODO: Calculate this
                parse_success_rate=None,
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

        # Check for semantic annotations
        semantic_stmt = select(SemanticAnnotation).where(
            SemanticAnnotation.column_id == column.column_id
        )
        has_semantic_annotation = (
            await session.execute(semantic_stmt)
        ).scalar_one_or_none() is not None

        # Update issue counts in dimension scores
        for dim_score in dimension_scores:
            dim_issues = [i for i in issues if i.dimension == dim_score.dimension]
            dim_score.issue_count = len(dim_issues)
            dim_score.critical_issues = len(
                [i for i in dim_issues if i.severity == QualitySynthesisSeverity.CRITICAL]
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
            has_semantic_context=has_semantic_annotation,
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
    duckdb_conn: duckdb.DuckDBPyConnection | None = None,
    llm_service: LLMService | None = None,
) -> Result[QualitySynthesisResult]:
    """Assess quality for a table and all its columns.

    Args:
        table_id: Table ID to assess
        session: Database session
        duckdb_conn: Optional DuckDB connection for financial cycle analysis
        llm_service: Optional LLM service for summaries and recommendations

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
                column_assessments.append(result.unwrap())

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
                    completeness_ratio=None,  # TODO: Calculate this
                    null_ratio=None,  # TODO: Calculate this
                    parse_success_rate=None,
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
        topo_stmt = (
            select(TopologicalQualityMetrics)
            .where(TopologicalQualityMetrics.table_id == table_id)
            .order_by(TopologicalQualityMetrics.computed_at.desc())
            .limit(1)
        )
        topo_quality = (await session.execute(topo_stmt)).scalar_one_or_none()

        financial_stmt = (
            select(FinancialQualityMetrics)
            .where(FinancialQualityMetrics.table_id == table_id)
            .order_by(FinancialQualityMetrics.computed_at.desc())
            .limit(1)
        )
        financial_quality = (await session.execute(financial_stmt)).scalar_one_or_none()

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
                    # Extract anomalous_cycles from JSONB topology_data
                    anomalous_cycles_data = topo_quality.topology_data.get("anomalous_cycles", {})
                    anomalous = (
                        len(anomalous_cycles_data.get("cycles", []))
                        if isinstance(anomalous_cycles_data, dict)
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
        critical_issues = len(
            [i for i in all_issues if i.severity == QualitySynthesisSeverity.CRITICAL]
        )
        warnings = len([i for i in all_issues if i.severity == QualitySynthesisSeverity.WARNING])

        # Issues by dimension
        issues_by_dimension: dict[str, int] = {}
        for issue in all_issues:
            dim_name = issue.dimension.value
            issues_by_dimension[dim_name] = issues_by_dimension.get(dim_name, 0) + 1

        # Issues by pillar
        issues_by_pillar: dict[int, int] = {}
        for issue in all_issues:
            pillar = issue.source_pillar
            issues_by_pillar[pillar] = issues_by_pillar.get(pillar, 0) + 1

        # Create synthesis result (initially without LLM fields)
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
            quality_summary=None,
            top_recommendations=[],
            synthesis_duration_seconds=time.time() - start_time,
            synthesized_at=datetime.now(UTC),
        )

        # LLM-enhanced analysis (optional)
        if llm_service is not None:
            # Generate quality summary
            try:
                from dataraum_context.quality.llm_quality import generate_quality_summary

                summary_result = await generate_quality_summary(synthesis, llm_service)
                if summary_result.success and summary_result.value:
                    synthesis.quality_summary = summary_result.value
            except Exception as e:
                logger.warning(f"Failed to generate quality summary: {e}")

            # Generate recommendations
            try:
                from dataraum_context.quality.llm_quality import (
                    generate_quality_recommendations,
                )

                recs_result = await generate_quality_recommendations(synthesis, llm_service)
                if recs_result.success and recs_result.value:
                    synthesis.top_recommendations = [
                        f"[{r.get('priority', 'MEDIUM')}] {r.get('recommendation', '')}"
                        for r in recs_result.value
                    ]
            except Exception as e:
                logger.warning(f"Failed to generate recommendations: {e}")

            # Run financial domain cycle analysis if duckdb available
            if duckdb_conn is not None:
                try:
                    from dataraum_context.quality.domains.financial_orchestrator import (
                        analyze_complete_financial_quality,
                    )

                    financial_result = await analyze_complete_financial_quality(
                        table_id=table_id,
                        duckdb_conn=duckdb_conn,
                        session=session,
                        llm_service=llm_service,
                    )

                    if financial_result.success and financial_result.value:
                        # Store LLM interpretation in the quality summary if available
                        llm_interp = financial_result.value.get("llm_interpretation")
                        if llm_interp and synthesis.quality_summary:
                            synthesis.quality_summary += (
                                f"\n\nFinancial Domain Analysis: {llm_interp.get('summary', '')}"
                            )
                        elif llm_interp:
                            synthesis.quality_summary = (
                                f"Financial Domain Analysis: {llm_interp.get('summary', '')}"
                            )

                        # Add financial recommendations
                        if llm_interp and llm_interp.get("recommendations"):
                            synthesis.top_recommendations.extend(
                                [
                                    f"[FINANCIAL] {rec}"
                                    for rec in llm_interp.get("recommendations", [])
                                ]
                            )
                except Exception as e:
                    logger.warning(f"Failed to run financial domain analysis: {e}")

        return Result.ok(synthesis)

    except Exception as e:
        return Result.fail(f"Table quality synthesis failed: {e}")


# ============================================================================
# Dataset Quality Assessment (Cross-Table Integration)
# ============================================================================


async def assess_dataset_quality(
    table_ids: list[str],
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    llm_service: LLMService | None = None,
) -> Result[DatasetQualitySynthesisResult]:
    """Assess quality across multiple related tables.

    Extends table-level assessment with cross-table metrics:
    - Cross-table multicollinearity (via unified correlation matrix)
    - Cross-table consistency violations
    - Dataset-level quality aggregation
    - LLM-enhanced cycle analysis and summaries (if llm_service provided)

    Args:
        table_ids: List of table IDs to assess
        duckdb_conn: DuckDB connection for cross-table analysis
        session: Database session
        llm_service: Optional LLM service for enhanced analysis

    Returns:
        Result containing DatasetQualitySynthesisResult
    """
    from dataraum_context.enrichment.cross_table_multicollinearity import (
        compute_cross_table_multicollinearity,
    )

    start_time = time.time()

    try:
        # 1. Get individual table assessments (with LLM if available)
        table_assessments = []
        for table_id in table_ids:
            result = await assess_table_quality(
                table_id, session, duckdb_conn=duckdb_conn, llm_service=llm_service
            )
            if result.success and result.value:
                table_assessments.append(result.value)

        if not table_assessments:
            return Result.fail("No tables could be assessed")

        # 2. Run cross-table multicollinearity analysis (if multiple tables)
        cross_table_issues: list[QualitySynthesisIssue] = []
        has_cross_table_analysis = False
        cross_table_dependencies = 0
        cross_table_severity = "none"

        if len(table_ids) >= 2:
            # Try to query stored results first (avoid expensive recomputation)
            from dataraum_context.enrichment.cross_table_multicollinearity import (
                get_stored_cross_table_analysis,
            )

            stored_result = await get_stored_cross_table_analysis(
                table_ids=table_ids,
                session=session,
            )

            # Use stored results if available, otherwise compute fresh
            if stored_result.success and stored_result.value is not None:
                multicollinearity_result = Result.ok(stored_result.value)
            else:
                multicollinearity_result = await compute_cross_table_multicollinearity(
                    table_ids=table_ids,
                    duckdb_conn=duckdb_conn,
                    session=session,
                )

            if multicollinearity_result.success and multicollinearity_result.value:
                has_cross_table_analysis = True
                analysis = multicollinearity_result.value
                cross_table_dependencies = analysis.num_cross_table_dependencies
                cross_table_severity = analysis.overall_severity

                # Convert cross-table multicollinearity groups to quality issues
                for group in analysis.cross_table_groups:
                    # Determine severity based on condition index
                    if group.severity == "severe":
                        severity = QualitySynthesisSeverity.ERROR
                    else:
                        severity = QualitySynthesisSeverity.WARNING

                    issue = QualitySynthesisIssue(
                        issue_id=str(uuid4()),
                        issue_type="cross_table_multicollinearity",
                        severity=severity,
                        dimension=QualityDimension.CONSISTENCY,
                        table_id=None,  # Spans multiple tables
                        column_id=None,
                        column_name=None,
                        description=(
                            f"{len(group.involved_columns)} columns across "
                            f"{group.num_tables} tables share high variance "
                            f"(CI={group.condition_index:.1f})"
                        ),
                        recommendation=(
                            "Review for redundant columns, denormalization, or "
                            "derived columns across tables"
                        ),
                        evidence={
                            "condition_index": group.condition_index,
                            "involved_columns": [
                                f"{table}.{col}" for table, col in group.involved_columns
                            ],
                            "variance_proportions": group.variance_proportions,
                            "num_tables": group.num_tables,
                        },
                        source_pillar=1,  # Statistical (correlation-based)
                        source_module="cross_table_multicollinearity",
                        detected_at=datetime.now(UTC),
                    )
                    cross_table_issues.append(issue)

                # 3. Adjust consistency scores based on cross-table issues
                if cross_table_dependencies > 0:
                    for table_assessment in table_assessments:
                        # Find consistency dimension score
                        for dim_score in table_assessment.table_assessment.dimension_scores:
                            if dim_score.dimension == QualityDimension.CONSISTENCY:
                                # Calculate penalty based on severity
                                if cross_table_severity == "severe":
                                    penalty = 0.3  # 30% penalty
                                elif cross_table_severity == "moderate":
                                    penalty = 0.15  # 15% penalty
                                else:
                                    penalty = 0.0

                                if penalty > 0:
                                    original_score = dim_score.score
                                    dim_score.score = max(0.0, original_score * (1.0 - penalty))
                                    dim_score.explanation += (
                                        f" (adjusted for {cross_table_dependencies} cross-table "
                                        f"dependencies, {cross_table_severity} severity)"
                                    )
                                break

                        # Recalculate table overall score
                        table_dim_scores = table_assessment.table_assessment.dimension_scores
                        if table_dim_scores:
                            table_assessment.table_assessment.overall_score = sum(
                                ds.score for ds in table_dim_scores
                            ) / len(table_dim_scores)

        # 4. Aggregate metrics across tables
        total_tables = len(table_assessments)
        total_columns = sum(a.total_columns for a in table_assessments)
        columns_assessed = sum(a.columns_assessed for a in table_assessments)

        # Average quality scores
        table_scores = [a.table_assessment.overall_score for a in table_assessments]
        average_table_quality = sum(table_scores) / len(table_scores) if table_scores else 0.0

        # Get all column scores
        all_column_scores = []
        for table_assessment in table_assessments:
            for col_assessment in table_assessment.table_assessment.column_assessments:
                all_column_scores.append(col_assessment.overall_score)

        average_column_quality = (
            sum(all_column_scores) / len(all_column_scores) if all_column_scores else 0.0
        )

        # 5. Aggregate issues
        all_table_issues = []
        for table_assessment in table_assessments:
            all_table_issues.extend(table_assessment.table_assessment.issues)
            for col_assessment in table_assessment.table_assessment.column_assessments:
                all_table_issues.extend(col_assessment.issues)

        all_issues = all_table_issues + cross_table_issues

        total_issues = len(all_issues)
        critical_issues = len(
            [i for i in all_issues if i.severity == QualitySynthesisSeverity.CRITICAL]
        )
        warnings = len([i for i in all_issues if i.severity == QualitySynthesisSeverity.WARNING])

        # Issues by dimension
        issues_by_dimension: dict[str, int] = {}
        for issue in all_issues:
            dim_name = issue.dimension.value
            issues_by_dimension[dim_name] = issues_by_dimension.get(dim_name, 0) + 1

        # Issues by pillar
        issues_by_pillar: dict[int, int] = {}
        for issue in all_issues:
            pillar = issue.source_pillar
            issues_by_pillar[pillar] = issues_by_pillar.get(pillar, 0) + 1

        # 6. Get table names
        table_names = [a.table_name for a in table_assessments]

        # 7. Create dataset synthesis result (initially without LLM fields)
        dataset_result = DatasetQualitySynthesisResult(
            table_ids=table_ids,
            table_names=table_names,
            table_assessments=table_assessments,
            total_tables=total_tables,
            total_columns=total_columns,
            columns_assessed=columns_assessed,
            average_table_quality=average_table_quality,
            average_column_quality=average_column_quality,
            total_issues=total_issues,
            critical_issues=critical_issues,
            warnings=warnings,
            issues_by_dimension=issues_by_dimension,
            issues_by_pillar=issues_by_pillar,
            has_cross_table_analysis=has_cross_table_analysis,
            cross_table_dependencies=cross_table_dependencies,
            cross_table_multicollinearity_severity=cross_table_severity,
            cross_table_issues=cross_table_issues,
            dataset_summary=None,
            top_recommendations=[],
            synthesis_duration_seconds=time.time() - start_time,
            synthesized_at=datetime.now(UTC),
        )

        # 8. Multi-table financial cycle analysis (cross-table business processes)
        cross_table_financial_analysis = None
        if len(table_ids) >= 2 and duckdb_conn is not None:
            try:
                from dataraum_context.quality.domains.financial_orchestrator import (
                    analyze_complete_financial_dataset_quality,
                )

                financial_result = await analyze_complete_financial_dataset_quality(
                    table_ids=table_ids,
                    duckdb_conn=duckdb_conn,
                    session=session,
                    llm_service=llm_service,
                )

                if financial_result.success and financial_result.value:
                    cross_table_financial_analysis = financial_result.value
                    logger.info(
                        f"Cross-table financial analysis detected "
                        f"{len(cross_table_financial_analysis.get('cross_table_cycles', []))} cycles"
                    )

                    # Add cross-table cycle issues
                    for cycle_classification in cross_table_financial_analysis.get(
                        "classified_cycles", []
                    ):
                        cycle_type = cycle_classification.get("primary_type", "unknown_cycle")
                        explanation = cycle_classification.get("explanation", "")
                        business_value = cycle_classification.get("business_value", "medium")

                        severity = (
                            QualitySynthesisSeverity.INFO
                            if business_value == "high"
                            else QualitySynthesisSeverity.WARNING
                        )

                        issue = QualitySynthesisIssue(
                            issue_id=str(uuid4()),
                            issue_type=f"financial_cycle_{cycle_type}",
                            severity=severity,
                            dimension=QualityDimension.CONSISTENCY,
                            table_id=None,  # Spans multiple tables
                            column_id=None,
                            column_name=None,
                            description=f"Business cycle detected: {explanation}",
                            recommendation=(
                                "Review business process data flow for completeness"
                                if business_value == "high"
                                else None
                            ),
                            evidence={
                                "cycle_type": cycle_type,
                                "business_value": business_value,
                                "cycle_types": cycle_classification.get("cycle_types", []),
                            },
                            source_pillar=5,  # Quality (domain quality)
                            source_module="financial_orchestrator",
                            detected_at=datetime.now(UTC),
                        )
                        cross_table_issues.append(issue)

            except Exception as e:
                logger.warning(f"Failed to run cross-table financial analysis: {e}")

        # 9. LLM-enhanced dataset summary (optional)
        if llm_service is not None:
            try:
                # Aggregate summaries from all tables
                table_summaries = [
                    f"- {a.table_name}: {a.quality_summary}"
                    for a in table_assessments
                    if a.quality_summary
                ]

                if table_summaries:
                    dataset_result.dataset_summary = (
                        f"Dataset quality assessment across {total_tables} tables:\n"
                        + "\n".join(table_summaries)
                    )

                # Add cross-table financial cycle interpretation
                if cross_table_financial_analysis:
                    interpretation = cross_table_financial_analysis.get("interpretation")
                    if interpretation:
                        interp_summary = interpretation.get("summary", "")
                        if interp_summary:
                            if dataset_result.dataset_summary:
                                dataset_result.dataset_summary += (
                                    f"\n\nBusiness Process Analysis:\n{interp_summary}"
                                )
                            else:
                                dataset_result.dataset_summary = (
                                    f"Business Process Analysis:\n{interp_summary}"
                                )

                # Aggregate recommendations from all tables
                all_recommendations = []
                for assessment in table_assessments:
                    all_recommendations.extend(assessment.top_recommendations)

                # Add cross-table financial recommendations
                if cross_table_financial_analysis:
                    interpretation = cross_table_financial_analysis.get("interpretation")
                    if interpretation and interpretation.get("recommendations"):
                        for rec in interpretation["recommendations"]:
                            if isinstance(rec, dict):
                                priority = rec.get("priority", "MEDIUM")
                                action = rec.get("action", rec.get("recommendation", ""))
                            else:
                                priority = "MEDIUM"
                                action = str(rec)
                            all_recommendations.append(f"[{priority}] {action}")

                # Deduplicate and prioritize
                seen = set()
                unique_recs = []
                for rec in all_recommendations:
                    if rec not in seen:
                        seen.add(rec)
                        unique_recs.append(rec)

                dataset_result.top_recommendations = unique_recs[:10]  # Top 10

            except Exception as e:
                logger.warning(f"Failed to generate dataset summary: {e}")

        return Result.ok(dataset_result)

    except Exception as e:
        return Result.fail(f"Dataset quality synthesis failed: {e}")
