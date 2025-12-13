"""LLM-powered quality features (Phase 6).

This module provides LLM analysis of quality metrics:
1. Quality summaries - Natural language quality descriptions
2. Quality recommendations - Prioritized action items

For business cycle classification, use:
- dataraum_context.quality.domains.financial_llm.classify_financial_cycle_with_llm()
- dataraum_context.quality.domains.financial_orchestrator.analyze_complete_financial_quality()

These features are OPTIONAL and gracefully degrade if LLM is unavailable.
"""

import json
import logging
from typing import Any

import yaml

from dataraum_context.core.models.base import Result
from dataraum_context.llm import LLMService
from dataraum_context.llm.providers.base import LLMRequest
from dataraum_context.quality.models import (
    QualityDimension,
    QualitySynthesisResult,
)

logger = logging.getLogger(__name__)

# Cache for loaded prompts
_PROMPTS_CACHE: dict[str, Any] | None = None


def _load_prompts() -> dict[str, Any]:
    """Load quality analysis prompts from YAML config.

    Returns:
        Dictionary of prompt templates

    Raises:
        FileNotFoundError: If prompts file not found
        yaml.YAMLError: If prompts file is invalid
    """
    global _PROMPTS_CACHE
    if _PROMPTS_CACHE is not None:
        return _PROMPTS_CACHE

    from pathlib import Path

    # Try common locations
    prompt_paths = [
        Path("config/prompts/quality_analysis.yaml"),
        Path.cwd() / "config/prompts/quality_analysis.yaml",
    ]

    for prompt_path in prompt_paths:
        if prompt_path.exists():
            with open(prompt_path) as f:
                _PROMPTS_CACHE = yaml.safe_load(f)
                return _PROMPTS_CACHE

    raise FileNotFoundError("quality_analysis.yaml prompts file not found")


async def generate_quality_summary(
    quality_result: QualitySynthesisResult,
    llm_service: LLMService | None,
) -> Result[str]:
    """Generate natural language summary of quality assessment.

    Args:
        quality_result: Quality synthesis result with metrics
        llm_service: LLM service (optional - returns generic summary if None)

    Returns:
        Result containing natural language quality summary (2-3 sentences)

    Example:
        >>> summary = await generate_quality_summary(quality_result, llm_service)
        >>> if summary.success:
        ...     print(summary.value)
        "Overall quality is good (0.82/1.0). Completeness and validity are excellent,
         but 3 critical topological issues require attention (circular relationships).
         Recommend addressing cycles before production use."
    """
    try:
        # If no LLM, return generic summary
        if llm_service is None:
            overall = quality_result.table_assessment.overall_score
            total = quality_result.total_issues
            critical = quality_result.critical_issues

            generic = (
                f"Quality score: {overall:.2f}/1.0. "
                f"{total} issues found ({critical} critical). "
                f"Review detailed metrics for more information."
            )
            return Result.ok(generic)

        # Load prompts
        try:
            prompts = _load_prompts()
            quality_summary_prompt = prompts.get("quality_summary", {})
        except Exception as e:
            logger.warning(f"Failed to load prompts, using generic summary: {e}")
            return await generate_quality_summary(quality_result, None)  # Fallback

        # Extract dimension scores
        dimension_scores = {
            score.dimension: score.score
            for score in quality_result.table_assessment.dimension_scores
        }

        # Build key findings
        key_findings = []
        for issue in quality_result.table_assessment.issues[:5]:  # Top 5 issues
            key_findings.append(f"- {issue.description}")

        # Format context
        system_prompt = quality_summary_prompt.get("system", "")
        user_template = quality_summary_prompt.get("user", "")

        user_prompt = user_template.format(
            table_name=quality_result.table_assessment.table_name,
            column_count=quality_result.total_columns,
            columns_assessed=quality_result.columns_assessed,
            overall_score=quality_result.table_assessment.overall_score,
            completeness_score=dimension_scores.get(QualityDimension.COMPLETENESS, 0.0),
            validity_score=dimension_scores.get(QualityDimension.VALIDITY, 0.0),
            consistency_score=dimension_scores.get(QualityDimension.CONSISTENCY, 0.0),
            uniqueness_score=dimension_scores.get(QualityDimension.UNIQUENESS, 0.0),
            timeliness_score=dimension_scores.get(QualityDimension.TIMELINESS, 0.0),
            accuracy_score=dimension_scores.get(QualityDimension.ACCURACY, 0.0),
            total_issues=quality_result.total_issues,
            critical_issues=quality_result.critical_issues,
            warnings=quality_result.warnings,
            key_findings="\n".join(key_findings) if key_findings else "No critical issues",
        )

        # Call LLM
        full_prompt = f"{system_prompt}\n\n{user_prompt}"

        request = LLMRequest(
            prompt=full_prompt,
            max_tokens=200,  # Summary should be brief
            temperature=0.7,
            response_format="text",
        )

        response_result = await llm_service.provider.complete(request)

        if not response_result.success or not response_result.value:
            # Fallback to generic
            return await generate_quality_summary(quality_result, None)

        summary = response_result.value.content.strip()
        return Result.ok(summary)

    except Exception as e:
        logger.error(f"Failed to generate quality summary: {e}")
        return Result.fail(f"Quality summary generation failed: {e}")


async def generate_quality_recommendations(
    quality_result: QualitySynthesisResult,
    llm_service: LLMService | None,
) -> Result[list[dict[str, str]]]:
    """Generate prioritized recommendations to improve data quality.

    Args:
        quality_result: Quality synthesis result with detailed issues
        llm_service: LLM service (optional - returns generic recommendations if None)

    Returns:
        Result containing list of recommendations:
        [
            {
                "priority": "CRITICAL",
                "action": "Fix circular relationships...",
                "rationale": "Cycles cause infinite loops...",
                "impact": "Will improve consistency score by ~0.3"
            },
            ...
        ]

    Example:
        >>> recs = await generate_quality_recommendations(quality_result, llm_service)
        >>> for rec in recs.value:
        ...     print(f"{rec['priority']}: {rec['action']}")
    """
    try:
        # If no LLM, return generic recommendations
        if llm_service is None:
            generic_recs = [
                {
                    "priority": "HIGH",
                    "action": "Review detailed quality metrics and address critical issues",
                    "rationale": "LLM unavailable for detailed analysis",
                    "impact": "Manual review required",
                }
            ]
            return Result.ok(generic_recs)

        # Load prompts
        try:
            prompts = _load_prompts()
            recs_prompt = prompts.get("quality_recommendations", {})
        except Exception as e:
            logger.warning(f"Failed to load prompts, using generic recommendations: {e}")
            return await generate_quality_recommendations(quality_result, None)

        # Format issues by dimension
        issues_by_dim = []
        for dim, count in quality_result.issues_by_dimension.items():
            if count > 0:
                issues_by_dim.append(f"- {dim}: {count} issues")

        # Format detailed issues
        detailed_issues = []
        for issue in quality_result.table_assessment.issues:
            detailed_issues.append(
                f"- [{issue.severity.upper()}] {issue.description} (Source: {issue.source_pillar})"
            )

        # Format context
        system_prompt = recs_prompt.get("system", "")
        user_template = recs_prompt.get("user", "")

        user_prompt = user_template.format(
            table_name=quality_result.table_assessment.table_name,
            overall_score=quality_result.table_assessment.overall_score,
            issues_by_dimension="\n".join(issues_by_dim) if issues_by_dim else "No issues",
            statistical_issues=quality_result.issues_by_pillar.get(1, 0),
            topological_issues=quality_result.issues_by_pillar.get(2, 0),
            temporal_issues=quality_result.issues_by_pillar.get(4, 0),
            domain_issues=quality_result.issues_by_pillar.get(5, 0),
            detailed_issues="\n".join(detailed_issues[:10])
            if detailed_issues
            else "No detailed issues",
            column_problems="See column assessments for details",
        )

        # Call LLM
        full_prompt = f"{system_prompt}\n\n{user_prompt}"

        request = LLMRequest(
            prompt=full_prompt,
            max_tokens=800,  # Recommendations need more space
            temperature=0.5,  # Slightly lower for more focused recommendations
            response_format="text",
        )

        response_result = await llm_service.provider.complete(request)

        if not response_result.success or not response_result.value:
            return await generate_quality_recommendations(quality_result, None)

        # Parse recommendations (expect numbered list or JSON)
        response_text = response_result.value.content.strip()

        # Try JSON parse first
        try:
            recs = json.loads(response_text)
            if isinstance(recs, list):
                return Result.ok(recs)
        except json.JSONDecodeError:
            pass

        # Fallback: Parse as text (simple heuristic)
        # Return as-is wrapped in dict
        generic_rec = [
            {
                "priority": "MEDIUM",
                "action": response_text,
                "rationale": "LLM-generated recommendations",
                "impact": "See detailed analysis",
            }
        ]
        return Result.ok(generic_rec)

    except Exception as e:
        logger.error(f"Failed to generate recommendations: {e}")
        return Result.fail(f"Recommendation generation failed: {e}")


# NOTE: classify_business_cycle() has been removed.
# For LLM-based cycle classification, use:
#   from dataraum_context.quality.domains.financial_llm import classify_financial_cycle_with_llm
#   from dataraum_context.quality.domains.financial_orchestrator import analyze_complete_financial_quality
#
# The financial_orchestrator provides the complete flow:
#   Layer 1: Compute metrics (topological analysis)
#   Layer 2: LLM classification of individual cycles
#   Layer 3: LLM holistic interpretation
