"""Quality context synthesis (Pillar 5).

Aggregates quality metrics from all pillars into a unified quality assessment
with standard data quality dimensions.
"""

import logging
from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models import Result
from dataraum_context.core.models.quality_context import (
    QualityContextResult,
    QualityDimensionDetail,
    QualityIssue,
    QualitySummary,
    QualityWeights,
)
from dataraum_context.storage.models_v2 import (
    FinancialQualityMetrics,
    Source,
    StatisticalQualityMetrics,
    Table,
    TemporalQualityMetrics,
    TopologicalQualityMetrics,
)
from dataraum_context.storage.models_v2 import (
    QualityContext as DBQualityContext,
)
from dataraum_context.storage.models_v2 import (
    QualityDimensionDetail as DBQualityDimensionDetail,
)
from dataraum_context.storage.models_v2 import (
    QualityIssueAggregate as DBQualityIssueAggregate,
)

logger = logging.getLogger(__name__)


async def calculate_completeness_score(
    source_id: str,
    session: AsyncSession,
) -> Result[tuple[float, dict[str, float]]]:
    """Calculate completeness score from statistical and temporal metrics.

    Completeness considers:
    - Null/missing value rates (statistical)
    - Temporal gaps (temporal)
    - Required fields present

    Args:
        source_id: UUID of the source
        session: SQLAlchemy async session
        tables_result = await session.execute(select(Table).where(Table.source_id == source_id))
        tables = tables_result.scalars().all()

        if not tables:
            return Result.fail("No tables found for source")

        component_scores = {}

        # 1. Statistical completeness (null rates)
        statistical_scores = []
        for table in tables:
            stats_result = await session.execute(
                select(StatisticalQualityMetrics)
                .where(StatisticalQualityMetrics.table_id == str(table.table_id))
                .order_by(StatisticalQualityMetrics.computed_at.desc())
                .limit(1)
            )
            stats = stats_result.scalar_one_or_none()

            if stats and stats.null_rate is not None:
                # Completeness = 1 - null_rate
                statistical_scores.append(1.0 - stats.null_rate)

        if statistical_scores:
            component_scores["statistical_completeness"] = sum(statistical_scores) / len(
                statistical_scores
            )
        else:
            component_scores["statistical_completeness"] = 1.0  # Assume complete if no data

        # 2. Temporal completeness (gap analysis)
        temporal_scores = []
        for table in tables:
            temp_result = await session.execute(
                select(TemporalQualityMetrics)
                .where(TemporalQualityMetrics.table_id == str(table.table_id))
                .order_by(TemporalQualityMetrics.computed_at.desc())
                .limit(1)
            )
            temp = temp_result.scalar_one_or_none()

            if temp and temp.completeness_ratio is not None:
                temporal_scores.append(temp.completeness_ratio)

        if temporal_scores:
            component_scores["temporal_completeness"] = sum(temporal_scores) / len(temporal_scores)
        else:
            component_scores["temporal_completeness"] = 1.0  # Assume complete if no temporal data

        # Calculate overall completeness score (weighted average)
        weights = {"statistical_completeness": 0.6, "temporal_completeness": 0.4}
        completeness_score = sum(component_scores[k] * weights[k] for k in component_scores)

        return Result.ok((completeness_score, component_scores))

    except Exception as e:
        logger.error(f"Completeness calculation failed: {e}")
        return Result.fail(f"Completeness calculation failed: {str(e)}")


async def calculate_consistency_score(
    source_id: str,
    session: AsyncSession,
) -> Result[tuple[float, dict[str, float]]]:
    """Calculate consistency score from semantic and topological metrics.

    Consistency considers:
    - Referential integrity (topological)
    - Naming conventions (semantic)
    - Data type consistency (statistical)

    Args:
        source_id: UUID of the source
        session: SQLAlchemy async session

    Returns:
        Result containing (score, component_scores)
    """
    try:
        tables_result = await session.execute(select(Table).where(Table.source_id == source_id))
        tables = tables_result.scalars().all()

        if not tables:
            return Result.fail("No tables found for source")

        component_scores = {}

        # 1. Topological consistency (structural integrity)
        topo_scores = []
        for table in tables:
            topo_result = await session.execute(
                select(TopologicalQualityMetrics)
                .where(TopologicalQualityMetrics.table_id == str(table.table_id))
                .order_by(TopologicalQualityMetrics.computed_at.desc())
                .limit(1)
            )
            topo = topo_result.scalar_one_or_none()

            if topo:
                # Use homological stability as a proxy for consistency
                if topo.homologically_stable:
                    topo_scores.append(1.0)
                else:
                    topo_scores.append(0.7)  # Penalize unstable topology

        if topo_scores:
            component_scores["topological_consistency"] = sum(topo_scores) / len(topo_scores)
        else:
            component_scores["topological_consistency"] = 0.8  # Neutral if no data

        # 2. Statistical consistency (type consistency)
        stat_scores = []
        for table in tables:
            stats_result = await session.execute(
                select(StatisticalQualityMetrics)
                .where(StatisticalQualityMetrics.table_id == str(table.table_id))
                .order_by(StatisticalQualityMetrics.computed_at.desc())
                .limit(1)
            )
            stats = stats_result.scalar_one_or_none()

            if stats and stats.outlier_ratio is not None:
                # Consistency = 1 - outlier_ratio (outliers suggest inconsistency)
                stat_scores.append(1.0 - stats.outlier_ratio)

        if stat_scores:
            component_scores["statistical_consistency"] = sum(stat_scores) / len(stat_scores)
        else:
            component_scores["statistical_consistency"] = 0.9  # Neutral if no data

        # Calculate overall consistency score
        weights = {"topological_consistency": 0.5, "statistical_consistency": 0.5}
        consistency_score = sum(component_scores[k] * weights[k] for k in component_scores)

        return Result.ok((consistency_score, component_scores))

    except Exception as e:
        logger.error(f"Consistency calculation failed: {e}")
        return Result.fail(f"Consistency calculation failed: {str(e)}")


async def calculate_accuracy_score(
    source_id: str,
    session: AsyncSession,
) -> Result[tuple[float, dict[str, float]]]:
    """Calculate accuracy score from domain-specific quality rules.

    Accuracy considers:
    - Domain rule compliance (financial, etc.)
    - Business logic validation
    - Constraint satisfaction

    Args:
        source_id: UUID of the source
        session: SQLAlchemy async session

    Returns:
        Result containing (score, component_scores)
    """
    try:
        tables_result = await session.execute(select(Table).where(Table.source_id == source_id))
        tables = tables_result.scalars().all()

        if not tables:
            return Result.fail("No tables found for source")

        component_scores = {}

        # Financial accuracy (if applicable)
        financial_scores = []
        for table in tables:
            fin_result = await session.execute(
                select(FinancialQualityMetrics)
                .where(FinancialQualityMetrics.table_id == str(table.table_id))
                .order_by(FinancialQualityMetrics.computed_at.desc())
                .limit(1)
            )
            fin = fin_result.scalar_one_or_none()

            if fin:
                financial_scores.append(fin.financial_quality_score)

        if financial_scores:
            component_scores["financial_accuracy"] = sum(financial_scores) / len(financial_scores)
            accuracy_score = component_scores["financial_accuracy"]
        else:
            # No domain-specific metrics, assume neutral accuracy
            component_scores["general_accuracy"] = 0.85
            accuracy_score = 0.85

        return Result.ok((accuracy_score, component_scores))

    except Exception as e:
        logger.error(f"Accuracy calculation failed: {e}")
        return Result.fail(f"Accuracy calculation failed: {str(e)}")


async def calculate_timeliness_score(
    source_id: str,
    session: AsyncSession,
) -> Result[tuple[float, dict[str, float]]]:
    """Calculate timeliness score from temporal metrics.

    Timeliness considers:
    - Data freshness (time since last update)
    - Update frequency regularity
    - Temporal quality overall

    Args:
        source_id: UUID of the source
        session: SQLAlchemy async session

    Returns:
        Result containing (score, component_scores)
    """
    try:
        tables_result = await session.execute(select(Table).where(Table.source_id == source_id))
        tables = tables_result.scalars().all()

        if not tables:
            return Result.fail("No tables found for source")

        component_scores = {}

        # Temporal timeliness
        temporal_scores = []
        for table in tables:
            temp_result = await session.execute(
                select(TemporalQualityMetrics)
                .where(TemporalQualityMetrics.table_id == str(table.table_id))
                .order_by(TemporalQualityMetrics.computed_at.desc())
                .limit(1)
            )
            temp = temp_result.scalar_one_or_none()

            if temp:
                # Use update frequency score and overall temporal quality
                if temp.update_frequency_score is not None:
                    temporal_scores.append(temp.update_frequency_score)
                elif temp.temporal_quality_score is not None:
                    temporal_scores.append(temp.temporal_quality_score)

        if temporal_scores:
            component_scores["temporal_timeliness"] = sum(temporal_scores) / len(temporal_scores)
            timeliness_score = component_scores["temporal_timeliness"]
        else:
            component_scores["temporal_timeliness"] = 0.8  # Neutral if no data
            timeliness_score = 0.8

        return Result.ok((timeliness_score, component_scores))

    except Exception as e:
        logger.error(f"Timeliness calculation failed: {e}")
        return Result.fail(f"Timeliness calculation failed: {str(e)}")


async def calculate_uniqueness_score(
    source_id: str,
    session: AsyncSession,
) -> Result[tuple[float, dict[str, float]]]:
    """Calculate uniqueness score from statistical metrics.

    Uniqueness considers:
    - Duplicate detection
    - Primary key integrity
    - Cardinality analysis

    Args:
        source_id: UUID of the source
        session: SQLAlchemy async session

    Returns:
        Result containing (score, component_scores)
    """
    try:
        tables_result = await session.execute(select(Table).where(Table.source_id == source_id))
        tables = tables_result.scalars().all()

        if not tables:
            return Result.fail("No tables found for source")

        component_scores = {}

        # Statistical uniqueness (duplicate rates)
        stat_scores = []
        for table in tables:
            stats_result = await session.execute(
                select(StatisticalQualityMetrics)
                .where(StatisticalQualityMetrics.table_id == str(table.table_id))
                .order_by(StatisticalQualityMetrics.computed_at.desc())
                .limit(1)
            )
            stats = stats_result.scalar_one_or_none()

            if stats and stats.duplicate_ratio is not None:
                # Uniqueness = 1 - duplicate_ratio
                stat_scores.append(1.0 - stats.duplicate_ratio)

        if stat_scores:
            component_scores["statistical_uniqueness"] = sum(stat_scores) / len(stat_scores)
            uniqueness_score = component_scores["statistical_uniqueness"]
        else:
            component_scores["statistical_uniqueness"] = 0.95  # Assume mostly unique if no data
            uniqueness_score = 0.95

        return Result.ok((uniqueness_score, component_scores))

    except Exception as e:
        logger.error(f"Uniqueness calculation failed: {e}")
        return Result.fail(f"Uniqueness calculation failed: {str(e)}")


async def aggregate_quality_issues(
    source_id: str,
    session: AsyncSession,
) -> Result[list[QualityIssue]]:
    """Aggregate quality issues from all pillars.

    Collects issues from:
    - Statistical quality metrics
    - Topological quality metrics
    - Temporal quality metrics
    - Domain quality metrics

    Args:
        source_id: UUID of the source
        session: SQLAlchemy async session

    Returns:
        Result containing list of aggregated quality issues
    """
    try:
        issues = []

        # Get all tables for this source
        tables_result = await session.execute(select(Table).where(Table.source_id == source_id))
        tables = tables_result.scalars().all()

        # Aggregate financial quality issues
        for table in tables:
            fin_result = await session.execute(
                select(FinancialQualityMetrics)
                .where(FinancialQualityMetrics.table_id == str(table.table_id))
                .order_by(FinancialQualityMetrics.computed_at.desc())
                .limit(1)
            )
            fin = fin_result.scalar_one_or_none()

            if fin:
                # Double-entry imbalance
                if not fin.double_entry_balanced:
                    issues.append(
                        QualityIssue(
                            issue_id=uuid4(),
                            issue_type="double_entry_imbalance",
                            severity="critical",
                            category="domain",
                            description=f"Double-entry imbalance in {table.table_name}: difference of {fin.balance_difference}",
                            affected_entities=[table.table_name],
                            source_pillar="financial",
                            source_metric_id=fin.metric_id,
                            recommendation="Review all transactions and ensure debits equal credits",
                            auto_fixable=False,
                        )
                    )

                # Trial balance issues
                if not fin.trial_balance_check:
                    issues.append(
                        QualityIssue(
                            issue_id=uuid4(),
                            issue_type="trial_balance_imbalance",
                            severity="critical",
                            category="domain",
                            description=f"Trial balance does not hold in {table.table_name}",
                            affected_entities=[table.table_name],
                            source_pillar="financial",
                            source_metric_id=fin.metric_id,
                            recommendation="Review account classifications and balances",
                            auto_fixable=False,
                        )
                    )

                # Sign convention violations
                if fin.sign_convention_compliance < 0.9:
                    issues.append(
                        QualityIssue(
                            issue_id=uuid4(),
                            issue_type="sign_convention_violations",
                            severity="moderate",
                            category="domain",
                            description=f"Sign convention compliance at {fin.sign_convention_compliance:.1%} in {table.table_name}",
                            affected_entities=[table.table_name],
                            source_pillar="financial",
                            source_metric_id=fin.metric_id,
                            recommendation="Review account types and ensure correct sign conventions",
                            auto_fixable=False,
                        )
                    )

        # Sort issues by severity
        severity_order = {"critical": 0, "severe": 1, "moderate": 2, "minor": 3}
        issues.sort(key=lambda x: severity_order.get(x.severity, 99))

        return Result.ok(issues)

    except Exception as e:
        logger.error(f"Issue aggregation failed: {e}")
        return Result.fail(f"Issue aggregation failed: {str(e)}")


async def synthesize_quality_context(
    source_id: str,
    session: AsyncSession,
    weights: QualityWeights | None = None,
) -> Result[QualityContextResult]:
    """Synthesize unified quality context from all pillars.

    Main entry point for quality synthesis. Calculates all dimension scores,
    aggregates issues, and produces a comprehensive quality assessment.

    Args:
        source_id: UUID of the source to assess
        session: SQLAlchemy async session
        weights: Optional custom weights for dimension scoring

    Returns:
        Result containing complete quality context
    """
    if weights is None:
        weights = QualityWeights()

    if not weights.validate_sum():
        return Result.fail("Quality weights must sum to 1.0")

    try:
        context_id = uuid4()
        computed_at = datetime.now(timezone.utc)

        # Calculate all dimension scores
        completeness_result = await calculate_completeness_score(source_id, session)
        if not completeness_result.success:
            return Result.fail(f"Completeness calculation failed: {completeness_result.error}")
        completeness_score, completeness_components = completeness_result.value

        consistency_result = await calculate_consistency_score(source_id, session)
        if not consistency_result.success:
            return Result.fail(f"Consistency calculation failed: {consistency_result.error}")
        consistency_score, consistency_components = consistency_result.value

        accuracy_result = await calculate_accuracy_score(source_id, session)
        if not accuracy_result.success:
            return Result.fail(f"Accuracy calculation failed: {accuracy_result.error}")
        accuracy_score, accuracy_components = accuracy_result.value

        timeliness_result = await calculate_timeliness_score(source_id, session)
        if not timeliness_result.success:
            return Result.fail(f"Timeliness calculation failed: {timeliness_result.error}")
        timeliness_score, timeliness_components = timeliness_result.value

        uniqueness_result = await calculate_uniqueness_score(source_id, session)
        if not uniqueness_result.success:
            return Result.fail(f"Uniqueness calculation failed: {uniqueness_result.error}")
        uniqueness_score, uniqueness_components = uniqueness_result.value

        # Calculate overall score (weighted average)
        overall_score = (
            completeness_score * weights.completeness
            + consistency_score * weights.consistency
            + accuracy_score * weights.accuracy
            + timeliness_score * weights.timeliness
            + uniqueness_score * weights.uniqueness
        )

        # Aggregate quality issues
        issues_result = await aggregate_quality_issues(source_id, session)
        if not issues_result.success:
            return Result.fail(f"Issue aggregation failed: {issues_result.error}")
        issues = issues_result.value

        # Separate critical issues
        critical_issues = [i for i in issues if i.severity == "critical"]
        warnings = [i.description for i in issues if i.severity in ["moderate", "minor"]]

        # Generate recommendations
        recommendations = []
        if completeness_score < 0.8:
            recommendations.append(
                "Address data completeness issues - high null rates or temporal gaps detected"
            )
        if consistency_score < 0.8:
            recommendations.append(
                "Improve data consistency - referential integrity or type issues detected"
            )
        if accuracy_score < 0.8:
            recommendations.append(
                "Review domain-specific quality rules - validation failures detected"
            )
        if timeliness_score < 0.8:
            recommendations.append("Improve data freshness and update frequency")
        if uniqueness_score < 0.9:
            recommendations.append("Address duplicate records")

        # Create dimension details
        dimension_details = [
            QualityDimensionDetail(
                detail_id=uuid4(),
                dimension="completeness",
                dimension_score=completeness_score,
                component_scores=completeness_components,
                calculation_method="Weighted average of statistical null rates and temporal gap analysis",
                contributing_metrics=list(completeness_components.keys()),
            ),
            QualityDimensionDetail(
                detail_id=uuid4(),
                dimension="consistency",
                dimension_score=consistency_score,
                component_scores=consistency_components,
                calculation_method="Weighted average of topological stability and statistical consistency",
                contributing_metrics=list(consistency_components.keys()),
            ),
            QualityDimensionDetail(
                detail_id=uuid4(),
                dimension="accuracy",
                dimension_score=accuracy_score,
                component_scores=accuracy_components,
                calculation_method="Domain-specific rule compliance scores",
                contributing_metrics=list(accuracy_components.keys()),
            ),
            QualityDimensionDetail(
                detail_id=uuid4(),
                dimension="timeliness",
                dimension_score=timeliness_score,
                component_scores=timeliness_components,
                calculation_method="Temporal update frequency and freshness metrics",
                contributing_metrics=list(timeliness_components.keys()),
            ),
            QualityDimensionDetail(
                detail_id=uuid4(),
                dimension="uniqueness",
                dimension_score=uniqueness_score,
                component_scores=uniqueness_components,
                calculation_method="Statistical duplicate detection and cardinality analysis",
                contributing_metrics=list(uniqueness_components.keys()),
            ),
        ]

        # Build result
        result = QualityContextResult(
            context_id=context_id,
            source_id=UUID(source_id),
            computed_at=computed_at,
            completeness_score=completeness_score,
            consistency_score=consistency_score,
            accuracy_score=accuracy_score,
            timeliness_score=timeliness_score,
            uniqueness_score=uniqueness_score,
            overall_score=overall_score,
            dimension_details=dimension_details,
            critical_issues=critical_issues,
            warnings=warnings,
            recommendations=recommendations,
            has_critical_issues=len(critical_issues) > 0,
            has_warnings=len(warnings) > 0,
            quality_grade=QualityContextResult.calculate_grade(None),  # Will be set by Pydantic
        )

        # Calculate grade
        result.quality_grade = result.calculate_grade()

        # Store in database
        db_context = DBQualityContext(
            context_id=context_id,
            source_id=UUID(source_id),
            computed_at=computed_at,
            completeness_score=completeness_score,
            consistency_score=consistency_score,
            accuracy_score=accuracy_score,
            timeliness_score=timeliness_score,
            uniqueness_score=uniqueness_score,
            overall_score=overall_score,
            critical_issues=[
                {
                    "issue_id": str(i.issue_id),
                    "issue_type": i.issue_type,
                    "severity": i.severity,
                    "description": i.description,
            critical_issues=[
                {
                    'issue_id': str(i.issue_id),
                    'issue_type': i.issue_type,
                    'severity': i.severity,
                    'description': i.description,
                }
                for i in critical_issues
            ],
            warnings=warnings,
            recommendations=recommendations,
        )
        session.add(db_context)

        # Store dimension details
        for detail in dimension_details:
            db_detail = DBQualityDimensionDetail(
                detail_id=detail.detail_id,
                context_id=context_id,
                dimension=detail.dimension,
                dimension_score=detail.dimension_score,
                component_scores=detail.component_scores,
                calculation_method=detail.calculation_method,
                contributing_metrics=detail.contributing_metrics,
            )
            session.add(db_detail)

        # Store issues
        for issue in issues:
            db_issue = DBQualityIssueAggregate(
                issue_id=issue.issue_id,
                context_id=context_id,
                issue_type=issue.issue_type,
                severity=issue.severity,
                category=issue.category,
                description=issue.description,
                affected_entities=issue.affected_entities,
                source_pillar=issue.source_pillar,
                source_metric_id=str(issue.source_metric_id) if issue.source_metric_id else None,
                recommendation=issue.recommendation,
                auto_fixable=issue.auto_fixable,
            )
            session.add(db_issue)

        await session.commit()

        return Result.ok(result)

    except Exception as e:
        logger.error(f"Quality synthesis failed: {e}")
        return Result.fail(f"Quality synthesis failed: {str(e)}")
