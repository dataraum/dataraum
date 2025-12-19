"""LLM-based quality interpretation.

Uses LLM to interpret complete financial quality results.
"""

import json
import logging
from typing import Any

import yaml

from dataraum_context.core.models.base import Result
from dataraum_context.domains.financial.config import load_financial_config
from dataraum_context.llm.providers.base import LLMProvider, LLMRequest

logger = logging.getLogger(__name__)


async def interpret_financial_quality_with_llm(
    financial_metrics: dict[str, Any],
    topological_metrics: dict[str, Any],
    classified_cycles: list[dict[str, Any]],
    llm_provider: LLMProvider,
    domain_analysis: dict[str, Any] | None = None,
) -> Result[dict[str, Any]]:
    """Interpret complete financial quality using LLM.

    LLM sees ALL computed metrics and provides holistic interpretation.

    Args:
        financial_metrics: Results from financial.py (double-entry, trial balance, etc.)
        topological_metrics: Results from topological.py (Betti numbers, cycles)
        classified_cycles: Cycles classified by classify_financial_cycle_with_llm()
        llm_provider: LLM provider for making API calls
        domain_analysis: Optional domain rule results (anomalies, quality score, fiscal stability)

    Returns:
        Result containing interpretation dict with:
        - overall_quality_score: float (0-1)
        - critical_issues: list[str]
        - recommendations: list[dict]
        - business_process_health: dict
        - summary: str
    """
    try:
        # Load config for thresholds
        config = load_financial_config()
        quality_thresholds = config.get("quality_thresholds", {})
        expected_cycles = config.get("expected_cycles", [])

        # Build comprehensive context
        metrics_context = f"""
=== FINANCIAL ACCOUNTING METRICS ===

Double-Entry Balance:
- Balanced: {financial_metrics.get("double_entry_balanced", "Unknown")}
- Net Difference: ${financial_metrics.get("net_difference", 0):,.2f}
- Total Debits: ${financial_metrics.get("total_debits", 0):,.2f}
- Total Credits: ${financial_metrics.get("total_credits", 0):,.2f}

Trial Balance:
- Assets: ${financial_metrics.get("total_assets", 0):,.2f}
- Liabilities: ${financial_metrics.get("total_liabilities", 0):,.2f}
- Equity: ${financial_metrics.get("total_equity", 0):,.2f}
- Balance Difference: ${financial_metrics.get("trial_balance_difference", 0):,.2f}
- Holds: {financial_metrics.get("trial_balance_holds", "Unknown")}

Sign Conventions:
- Compliance Rate: {financial_metrics.get("sign_compliance_rate", 0):.1%}
- Violations: {financial_metrics.get("sign_violations", 0)} accounts

Fiscal Period Integrity:
- Start Date: {financial_metrics.get("period_start", "Unknown")}
- End Date: {financial_metrics.get("period_end", "Unknown")}
- Missing Days: {financial_metrics.get("missing_days", 0)}
- Has Cutoff Issues: {financial_metrics.get("has_cutoff_issues", False)}

=== TOPOLOGICAL STRUCTURE ===

Betti Numbers:
- Connected Components (b0): {topological_metrics.get("betti_0", "Unknown")}
- Cycles (b1): {topological_metrics.get("betti_1", "Unknown")}
- Voids (b2): {topological_metrics.get("betti_2", "Unknown")}

Complexity:
- Total Complexity: {topological_metrics.get("total_complexity", "Unknown")}
- Persistent Entropy: {topological_metrics.get("persistent_entropy", "Unknown")}
- Orphaned Components: {topological_metrics.get("orphaned_components", 0)}

Detected Cycles: {len(classified_cycles)}

=== CLASSIFIED BUSINESS CYCLES ===
"""

        for i, cycle_class in enumerate(classified_cycles, 1):
            metrics_context += f"\nCycle {i}:\n"
            metrics_context += f"  Type: {cycle_class.get('cycle_type', 'UNKNOWN')}\n"
            metrics_context += f"  Confidence: {cycle_class.get('confidence', 0):.0%}\n"
            metrics_context += f"  Business Value: {cycle_class.get('business_value', 'unknown')}\n"
            metrics_context += f"  Expected: {cycle_class.get('is_expected', False)}\n"
            metrics_context += f"  Explanation: {cycle_class.get('explanation', 'N/A')}\n"

        # Add domain analysis if provided
        if domain_analysis:
            metrics_context += """

=== DOMAIN RULE ANALYSIS (Deterministic) ===
"""
            # Fiscal stability
            fiscal = domain_analysis.get("fiscal_stability", {})
            metrics_context += f"""
Fiscal Stability:
- Original Level: {fiscal.get("original_stability_level", "unknown")}
- Fiscal Context: {fiscal.get("fiscal_context", "none")}
- Is Fiscal Period Effect: {fiscal.get("is_fiscal_period_effect", False)}
- Pattern Type: {fiscal.get("pattern_type", "unknown")}
- Interpretation: {fiscal.get("interpretation", "N/A")}
"""

            # Anomalies
            anomalies = domain_analysis.get("anomalies", [])
            if anomalies:
                metrics_context += f"\nDetected Anomalies ({len(anomalies)}):\n"
                for anomaly in anomalies:
                    metrics_context += f"  - [{anomaly.get('severity', 'unknown').upper()}] {anomaly.get('type', 'unknown')}: {anomaly.get('description', '')}\n"
            else:
                metrics_context += "\nDetected Anomalies: None\n"

            # Quality score
            quality_score = domain_analysis.get("quality_score", {})
            metrics_context += f"""
Domain Quality Score: {quality_score.get("score", 0):.2f}
- Penalties Applied: {len(quality_score.get("penalties", []))}
- Bonuses Applied: {len(quality_score.get("bonuses", []))}
"""

        metrics_context += f"""

=== QUALITY THRESHOLDS (from config) ===
{yaml.dump(quality_thresholds, default_flow_style=False)}

=== EXPECTED CYCLES (from config) ===
{", ".join(expected_cycles) if expected_cycles else "None specified"}
"""

        # LLM prompt
        system_prompt = """You are a financial data quality expert providing holistic assessment.

Your task: Interpret ALL the metrics above and provide actionable insights.

Consider:
- Accounting integrity (double-entry, trial balance, sign conventions)
- Data structure health (connectivity, fragmentation)
- Business process completeness (expected cycles present?)
- Anomalies and risks

Provide:
1. Overall quality score (0-1) based on ALL metrics
2. Critical issues (anything that breaks accounting rules)
3. Prioritized recommendations
4. Business process health assessment

Return JSON:
{
  "overall_quality_score": 0.75,
  "critical_issues": ["$1,250 double-entry imbalance", ...],
  "recommendations": [
    {"priority": "CRITICAL", "action": "...", "rationale": "..."},
    {"priority": "HIGH", "action": "...", "rationale": "..."}
  ],
  "business_process_health": {
    "accounts_receivable": "healthy",
    "accounts_payable": "degraded",
    "missing_processes": ["payroll_cycle"]
  },
  "summary": "Overall assessment in 2-3 sentences"
}
"""

        user_prompt = (
            f"{metrics_context}\n\nProvide comprehensive financial quality interpretation."
        )

        # Call LLM
        full_prompt = f"{system_prompt}\n\n{user_prompt}"
        request = LLMRequest(
            prompt=full_prompt,
            max_tokens=1500,
            temperature=0.3,
            response_format="json",
        )

        response_result = await llm_provider.complete(request)

        if not response_result.success or not response_result.value:
            return Result.fail(f"LLM interpretation failed: {response_result.error}")

        # Parse JSON
        response_text = response_result.value.content.strip()
        try:
            interpretation = json.loads(response_text)
            return Result.ok(interpretation)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response: {e}")
            logger.debug(f"Response text: {response_text[:500]}")
            return Result.fail(f"Invalid JSON from LLM: {e}")

    except Exception as e:
        logger.error(f"Financial quality interpretation failed: {e}")
        return Result.fail(f"Interpretation failed: {e}")
