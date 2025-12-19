"""LLM-based cycle classification.

Uses LLM to classify financial cycles and cross-table cycles.
"""

import json
import logging
from typing import Any

import yaml

from dataraum_context.analysis.topology.models import CycleDetection
from dataraum_context.core.models.base import Result
from dataraum_context.domains.financial.config import load_financial_config
from dataraum_context.enrichment.relationships import EnrichedRelationship
from dataraum_context.llm.providers.base import LLMProvider, LLMRequest

logger = logging.getLogger(__name__)


async def classify_financial_cycle_with_llm(
    cycle: CycleDetection,
    table_name: str,
    column_names: list[str],
    semantic_roles: dict[str, str] | None,
    llm_provider: LLMProvider,
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
        semantic_roles: Column -> role mapping from semantic enrichment
        llm_provider: LLM provider for making API calls

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
        config = load_financial_config()
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

        response_result = await llm_provider.complete(request)

        if not response_result.success or not response_result.value:
            return Result.fail(f"LLM classification failed: {response_result.error}")

        # Parse JSON
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


async def classify_cross_table_cycle_with_llm(
    cycle_table_ids: list[str],
    relationships: list[EnrichedRelationship],
    table_semantics: dict[str, dict[str, Any]],
    llm_provider: LLMProvider,
) -> Result[dict[str, Any]]:
    """Classify a cross-table cycle as a business process using LLM.

    LLM receives:
    - Tables involved in the cycle
    - Relationships connecting them (with types, cardinality)
    - Semantic context from enrichment
    - Config patterns as vocabulary (not rules)

    Args:
        cycle_table_ids: List of table IDs forming the cycle
        relationships: Enriched relationships between tables in the cycle
        table_semantics: Semantic context per table {table_id: {columns, roles, ...}}
        llm_provider: LLM provider

    Returns:
        Result containing classification dict with:
        - cycle_types: list[dict] with type, confidence for each classification
        - primary_type: str (highest confidence type)
        - explanation: str
        - business_value: str (high/medium/low)
        - completeness: str (complete/partial/incomplete)
        - missing_elements: list[str] | None
    """
    try:
        # Load config for vocabulary
        config = load_financial_config()
        cycle_patterns = config.get("cycle_patterns", {})
        cross_table_patterns = config.get("cross_table_cycle_patterns", {})

        # Build cycle context
        cycle_tables_info = []
        for table_id in cycle_table_ids:
            semantics = table_semantics.get(table_id, {})
            cycle_tables_info.append(
                {
                    "table_id": table_id,
                    "table_name": semantics.get("table_name", "unknown"),
                    "key_columns": semantics.get("key_columns", []),
                    "semantic_roles": semantics.get("semantic_roles", {}),
                }
            )

        # Build relationships context
        relationships_info = []
        for rel in relationships:
            if rel.from_table_id in cycle_table_ids and rel.to_table_id in cycle_table_ids:
                relationships_info.append(
                    {
                        "from_table": rel.from_table,
                        "from_column": rel.from_column,
                        "to_table": rel.to_table,
                        "to_column": rel.to_column,
                        "relationship_type": rel.relationship_type.value
                        if hasattr(rel.relationship_type, "value")
                        else str(rel.relationship_type),
                        "cardinality": rel.cardinality.value
                        if rel.cardinality and hasattr(rel.cardinality, "value")
                        else str(rel.cardinality)
                        if rel.cardinality
                        else None,
                        "confidence": rel.confidence,
                    }
                )

        # Build config vocabulary context
        patterns_context = "Known Business Cycle Types:\n"

        # Single-table patterns (for reference)
        for cycle_type, pattern_def in cycle_patterns.items():
            patterns_context += f"\n{cycle_type}:\n"
            patterns_context += f"  Description: {pattern_def.get('description', 'N/A')}\n"
            patterns_context += (
                f"  Column indicators: {', '.join(pattern_def.get('column_patterns', []))}\n"
            )
            patterns_context += f"  Business value: {pattern_def.get('business_value', 'medium')}\n"

        # Cross-table patterns (primary reference)
        if cross_table_patterns:
            patterns_context += "\n\nCross-Table Cycle Patterns:\n"
            for cycle_type, pattern_def in cross_table_patterns.items():
                patterns_context += f"\n{cycle_type}:\n"
                patterns_context += f"  Description: {pattern_def.get('description', 'N/A')}\n"
                patterns_context += f"  Table patterns: {pattern_def.get('table_patterns', [])}\n"
                patterns_context += (
                    f"  Business value: {pattern_def.get('business_value', 'medium')}\n"
                )

        # LLM prompt
        system_prompt = """You are a financial data expert analyzing business process cycles in multi-table datasets.

Your task: Classify this cross-table cycle as one or more business processes.

A cross-table cycle is a loop in the table relationship graph, e.g.:
- transactions -> customers -> transactions (AR cycle)
- transactions -> vendors -> transactions (AP cycle)
- customers -> orders -> invoices -> payments -> customers (Revenue cycle)

Guidelines:
- Analyze table names, column semantics, and relationship types
- A cycle can represent MULTIPLE business processes (multi-label)
- Use the config patterns as vocabulary, not strict rules
- Assess completeness: is this a full cycle or partial?
- Explain your reasoning

Return JSON with:
{
  "cycle_types": [
    {"type": "accounts_receivable_cycle", "confidence": 0.85},
    {"type": "revenue_cycle", "confidence": 0.70}
  ],
  "primary_type": "accounts_receivable_cycle",
  "explanation": "This cycle connects customer and transaction tables via Customer name...",
  "business_value": "high",
  "completeness": "complete",
  "missing_elements": null
}"""

        user_prompt = f"""Cross-Table Cycle Analysis:

Tables in Cycle:
{yaml.dump(cycle_tables_info, default_flow_style=False)}

Relationships:
{yaml.dump(relationships_info, default_flow_style=False)}

{patterns_context}

Classify this cross-table cycle. What business process(es) does it represent?"""

        # Call LLM
        request = LLMRequest(
            prompt=f"{system_prompt}\n\n{user_prompt}",
            max_tokens=1000,
            temperature=0.3,
            response_format="json",
        )

        response_result = await llm_provider.complete(request)

        if not response_result.success or not response_result.value:
            return Result.fail(f"LLM classification failed: {response_result.error}")

        # Parse JSON
        response_text = response_result.value.content.strip()
        try:
            classification = json.loads(response_text)
            return Result.ok(classification)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response: {e}")
            return Result.fail(f"Invalid JSON from LLM: {e}")

    except Exception as e:
        logger.error(f"Cross-table cycle classification failed: {e}")
        return Result.fail(f"Classification failed: {e}")
