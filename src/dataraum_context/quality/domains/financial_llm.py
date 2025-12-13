"""Financial Domain LLM Interpretation Layer.

This module uses LLM to interpret computed financial metrics.

Design Principles:
1. Python computes ALL numbers (financial.py + topological.py)
2. LLM interprets results with domain context from config
3. NO pattern matching - LLM understands business cycles directly

Key Architecture:
- financial.py computes: double-entry balance, trial balance, sign conventions
- topological.py computes: Betti numbers, cycles, persistence
- THIS MODULE: LLM explains what those numbers mean in financial context
"""

import logging
from pathlib import Path
from typing import Any

import yaml

from dataraum_context.core.models.base import Result
from dataraum_context.llm import LLMService
from dataraum_context.llm.providers.base import LLMRequest
from dataraum_context.quality.models import CycleDetection

logger = logging.getLogger(__name__)

# Cache for config
_FINANCIAL_CONFIG_CACHE: dict[str, Any] | None = None


def _load_financial_config() -> dict[str, Any]:
    """Load financial domain configuration.

    Returns:
        Dictionary with cycle_patterns, quality_thresholds, etc.
    """
    global _FINANCIAL_CONFIG_CACHE
    if _FINANCIAL_CONFIG_CACHE is not None:
        return _FINANCIAL_CONFIG_CACHE

    config_paths = [
        Path("config/domains/financial.yaml"),
        Path.cwd() / "config/domains/financial.yaml",
    ]

    for config_path in config_paths:
        if config_path.exists():
            with open(config_path) as f:
                _FINANCIAL_CONFIG_CACHE = yaml.safe_load(f)
                return _FINANCIAL_CONFIG_CACHE

    # Return empty config if not found
    logger.warning("financial.yaml not found, using empty config")
    return {}


async def classify_financial_cycle_with_llm(
    cycle: CycleDetection,
    table_name: str,
    column_names: list[str],
    semantic_roles: dict[str, str] | None,
    llm_service: LLMService,
) -> Result[dict[str, Any]]:
    """Classify a single cycle using LLM with config context.

    LLM sees:
    - Cycle structure (birth, death, persistence)
    - Column names involved
    - Semantic roles from enrichment
    - Financial cycle vocabulary from config

    Args:
        cycle: CycleDetection with computed metrics
        table_name: Table name for context
        column_names: Columns involved in cycle
        semantic_roles: Column → role mapping from semantic enrichment
        llm_service: LLM service

    Returns:
        Result containing classification dict with:
        - cycle_type: str (e.g., "accounts_receivable_cycle")
        - confidence: float (0-1)
        - explanation: str
        - business_value: str ("high", "medium", "low")
        - is_expected: bool
        - recommendation: str | None
    """
    try:
        # Load config for context
        config = _load_financial_config()
        cycle_patterns = config.get("cycle_patterns", {})

        # Build context for LLM
        cycle_info = f"""
Table: {table_name}
Cycle Structure:
- Birth: {cycle.birth:.3f}
- Death: {cycle.death:.3f}
- Persistence: {cycle.persistence:.3f}
- Dimension: {cycle.dimension}

Columns Involved: {", ".join(column_names) if column_names else "Unknown"}

Semantic Roles (from enrichment):
{yaml.dump(semantic_roles, default_flow_style=False) if semantic_roles else "None detected"}
"""

        # Format cycle patterns as context (vocabulary, not rules)
        patterns_context = "Known Financial Cycle Types:\n"
        for cycle_type, pattern_def in cycle_patterns.items():
            patterns_context += f"\n{cycle_type}:\n"
            patterns_context += f"  Description: {pattern_def.get('description', 'N/A')}\n"
            patterns_context += (
                f"  Common columns: {', '.join(pattern_def.get('column_patterns', []))}\n"
            )
            patterns_context += f"  Business value: {pattern_def.get('business_value', 'medium')}\n"

        # LLM prompt
        system_prompt = """You are a financial data quality expert analyzing topological cycles in accounting data.

Your task: Classify this cycle and explain its business meaning.

Guidelines:
- Use the cycle structure (persistence, column names, semantic roles)
- Reference known financial cycle types as vocabulary, not strict rules
- If columns suggest multiple interpretations, choose the most likely
- Explain your reasoning clearly
- Assess business value (high/medium/low)
- Determine if this is an expected or anomalous cycle

Return JSON with:
{
  "cycle_type": "accounts_receivable_cycle",  // or "CUSTOM" with explanation
  "confidence": 0.85,  // 0-1 confidence score
  "explanation": "This cycle connects customer, invoice, and payment tables...",
  "business_value": "high",  // high/medium/low
  "is_expected": true,  // expected in financial data?
  "recommendation": "Monitor for completeness"  // or null
}
"""

        user_prompt = f"""{cycle_info}

{patterns_context}

Classify this cycle and explain its financial significance."""

        # Call LLM
        full_prompt = f"{system_prompt}\n\n{user_prompt}"
        request = LLMRequest(
            prompt=full_prompt,
            max_tokens=800,
            temperature=0.3,
            response_format="json",
        )

        response_result = await llm_service.provider.complete(request)

        if not response_result.success or not response_result.value:
            return Result.fail(f"LLM classification failed: {response_result.error}")

        # Parse JSON
        import json

        response_text = response_result.value.content.strip()
        try:
            classification = json.loads(response_text)
            return Result.ok(classification)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response: {e}")
            logger.debug(f"Response text: {response_text[:500]}")
            return Result.fail(f"Invalid JSON from LLM: {e}")

    except Exception as e:
        logger.error(f"Financial cycle classification failed: {e}")
        return Result.fail(f"Classification failed: {e}")


async def interpret_financial_quality_with_llm(
    financial_metrics: dict[str, Any],
    topological_metrics: dict[str, Any],
    classified_cycles: list[dict[str, Any]],
    llm_service: LLMService,
) -> Result[dict[str, Any]]:
    """Interpret complete financial quality using LLM.

    LLM sees ALL computed metrics and provides holistic interpretation.

    Args:
        financial_metrics: Results from financial.py (double-entry, trial balance, etc.)
        topological_metrics: Results from topological.py (Betti numbers, cycles)
        classified_cycles: Cycles classified by classify_financial_cycle_with_llm()
        llm_service: LLM service

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
        config = _load_financial_config()
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
- Connected Components (β₀): {topological_metrics.get("betti_0", "Unknown")}
- Cycles (β₁): {topological_metrics.get("betti_1", "Unknown")}
- Voids (β₂): {topological_metrics.get("betti_2", "Unknown")}

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

        response_result = await llm_service.provider.complete(request)

        if not response_result.success or not response_result.value:
            return Result.fail(f"LLM interpretation failed: {response_result.error}")

        # Parse JSON
        import json

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
