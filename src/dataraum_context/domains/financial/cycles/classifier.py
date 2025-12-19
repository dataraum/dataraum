"""LLM-based cycle classification.

Uses LLM to classify business process cycles in financial datasets.
"""

import json
import logging
from typing import Any

import yaml

from dataraum_context.core.models.base import Result
from dataraum_context.domains.financial.config import load_financial_config
from dataraum_context.domains.financial.cycles.relationships import (
    CyclePath,
    RelationshipStructure,
)
from dataraum_context.llm.providers.base import LLMProvider, LLMRequest

logger = logging.getLogger(__name__)


async def classify_business_cycle_with_llm(
    cycle: CyclePath,
    structure: RelationshipStructure,
    table_semantics: dict[str, dict[str, Any]],
    llm_provider: LLMProvider,
) -> Result[dict[str, Any]]:
    """Classify a business cycle using rich relationship structure context.

    LLM receives:
    - The specific cycle being classified (tables in sequence)
    - Full relationship structure (pattern, all relationships)
    - Semantic context per table
    - Config patterns as vocabulary (not rules)

    Args:
        cycle: The cycle path to classify
        structure: Full relationship structure for context
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
        cross_table_patterns = config.get("cross_table_cycle_patterns", {})

        # Build cycle context with semantic info
        cycle_tables_info = []
        for i, table_id in enumerate(cycle.table_ids):
            semantics = table_semantics.get(table_id, {})
            cycle_tables_info.append(
                {
                    "position": i + 1,
                    "table_name": cycle.tables[i] if i < len(cycle.tables) else "unknown",
                    "key_columns": semantics.get("key_columns", []),
                    "semantic_roles": semantics.get("semantic_roles", {}),
                }
            )

        # Get relationships involved in this cycle
        cycle_relationships = []
        cycle_table_set = set(cycle.tables)
        for rel in structure.relationships:
            if rel.from_table in cycle_table_set and rel.to_table in cycle_table_set:
                cycle_relationships.append(
                    {
                        "from": f"{rel.from_table}.{rel.from_column}",
                        "to": f"{rel.to_table}.{rel.to_column}",
                        "type": rel.relationship_type,
                        "cardinality": rel.cardinality,
                        "confidence": rel.confidence,
                    }
                )

        # Build vocabulary context from config
        patterns_context = ""
        if cross_table_patterns:
            patterns_context = "Known Business Cycle Types:\n"
            for cycle_type, pattern_def in cross_table_patterns.items():
                patterns_context += f"\n{cycle_type}:\n"
                patterns_context += f"  Description: {pattern_def.get('description', 'N/A')}\n"
                patterns_context += f"  Table patterns: {pattern_def.get('table_patterns', [])}\n"
                patterns_context += (
                    f"  Business value: {pattern_def.get('business_value', 'medium')}\n"
                )

        # LLM prompt
        system_prompt = """You are a financial data expert analyzing business process cycles.

Your task: Classify this cycle as one or more business processes.

Business cycles in financial data represent process flows:
- AR Cycle: Customer → Sale Transaction → Payment → Cash Application
- AP Cycle: Vendor → Purchase Transaction → Bill → Payment
- Revenue Cycle: Order → Invoice → Payment → GL Entry
- Expense Cycle: Purchase Order → Receipt → Bill → Disbursement

Guidelines:
- A cycle can represent MULTIPLE business processes (multi-label)
- Use table names, column semantics, and relationship types to classify
- Assess if the cycle is complete or missing expected steps
- Explain your reasoning

Return JSON:
{
  "cycle_types": [
    {"type": "accounts_receivable_cycle", "confidence": 0.85},
    {"type": "revenue_cycle", "confidence": 0.70}
  ],
  "primary_type": "accounts_receivable_cycle",
  "explanation": "This cycle connects customer and transaction tables...",
  "business_value": "high",
  "completeness": "complete",
  "missing_elements": null
}"""

        user_prompt = f"""Cycle to Classify:
Tables: {" → ".join(cycle.tables)} → {cycle.tables[0]} (loop)
Length: {cycle.length} tables

Tables in Cycle:
{yaml.dump(cycle_tables_info, default_flow_style=False)}

Relationships in Cycle:
{yaml.dump(cycle_relationships, default_flow_style=False)}

Overall Graph Pattern: {structure.pattern}
{structure.pattern_description}

{patterns_context}

Classify this cycle. What business process(es) does it represent?"""

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
            # Add cycle info to result
            classification["cycle_tables"] = cycle.tables
            classification["cycle_length"] = cycle.length
            return Result.ok(classification)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response: {e}")
            return Result.fail(f"Invalid JSON from LLM: {e}")

    except Exception as e:
        logger.error(f"Business cycle classification failed: {e}")
        return Result.fail(f"Classification failed: {e}")


async def interpret_relationship_structure_with_llm(
    structure: RelationshipStructure,
    table_semantics: dict[str, dict[str, Any]],
    classified_cycles: list[dict[str, Any]],
    llm_provider: LLMProvider,
) -> Result[dict[str, Any]]:
    """Generate holistic interpretation of relationship structure.

    Provides business-level summary of:
    - What business processes exist (from classified cycles)
    - What might be missing (expected but not found)
    - Data model quality observations

    Args:
        structure: Full relationship structure
        table_semantics: Semantic context per table
        classified_cycles: Already-classified cycles
        llm_provider: LLM provider

    Returns:
        Result containing interpretation dict
    """
    try:
        # Build structure summary
        structure_summary = {
            "pattern": structure.pattern,
            "description": structure.pattern_description,
            "total_tables": structure.total_tables,
            "total_relationships": structure.total_relationships,
            "hub_tables": structure.hub_tables,
            "leaf_tables": structure.leaf_tables,
            "isolated_tables": structure.isolated_tables,
            "cycles_detected": len(structure.cycles),
            "connected_components": structure.connected_components,
        }

        # Build table roles summary
        table_roles = [
            {
                "table": t.table_name,
                "role": t.role,
                "connections": t.connection_count,
                "semantic_info": table_semantics.get(t.table_id, {}).get("semantic_roles", {}),
            }
            for t in structure.tables
        ]

        # Build classified cycles summary
        cycles_summary = [
            {
                "tables": c.get("cycle_tables", []),
                "primary_type": c.get("primary_type", "unknown"),
                "business_value": c.get("business_value", "unknown"),
            }
            for c in classified_cycles
        ]

        system_prompt = """You are a financial data architect reviewing a multi-table dataset structure.

Provide a business-level interpretation of:
1. What business processes are represented by the relationships
2. What expected processes might be missing
3. Data model quality observations

Return JSON:
{
  "summary": "Brief 2-3 sentence overview",
  "business_processes": ["List of identified business processes"],
  "missing_processes": ["Expected processes not found, or null"],
  "data_model_observations": ["Quality/structure observations"],
  "recommendations": ["Actionable recommendations, or null"]
}"""

        user_prompt = f"""Dataset Structure:
{yaml.dump(structure_summary, default_flow_style=False)}

Table Roles:
{yaml.dump(table_roles, default_flow_style=False)}

Classified Business Cycles:
{yaml.dump(cycles_summary, default_flow_style=False) if cycles_summary else "No cycles detected"}

Provide a business interpretation of this financial dataset structure."""

        request = LLMRequest(
            prompt=f"{system_prompt}\n\n{user_prompt}",
            max_tokens=1000,
            temperature=0.3,
            response_format="json",
        )

        response_result = await llm_provider.complete(request)

        if not response_result.success or not response_result.value:
            return Result.fail(f"LLM interpretation failed: {response_result.error}")

        response_text = response_result.value.content.strip()
        try:
            interpretation = json.loads(response_text)
            return Result.ok(interpretation)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response: {e}")
            return Result.fail(f"Invalid JSON from LLM: {e}")

    except Exception as e:
        logger.error(f"Structure interpretation failed: {e}")
        return Result.fail(f"Interpretation failed: {e}")


# =============================================================================
# Within-Table Cycle Classification
# =============================================================================


async def classify_financial_cycle_with_llm(
    cycle: Any,
    table_name: str,
    column_names: list[str],
    semantic_roles: dict[str, str] | None,
    llm_provider: LLMProvider,
) -> Result[dict[str, Any]]:
    """Legacy function for within-table cycle classification.

    Note: Within-table cycles from TDA are data quality indicators,
    not business process cycles. Consider if this classification is needed.
    """
    try:
        config = load_financial_config()
        cycle_patterns = config.get("cycle_patterns", {})

        cycle_info = f"""
Table: {table_name}
Cycle Structure:
- Birth: {cycle.birth:.3f}
- Death: {cycle.death:.3f}
- Persistence: {cycle.persistence:.3f}
- Dimension: {cycle.dimension}

Columns Involved: {", ".join(column_names) if column_names else "Unknown"}

Semantic Roles:
{yaml.dump(semantic_roles, default_flow_style=False) if semantic_roles else "None detected"}
"""

        patterns_context = "Known Cycle Types:\n"
        for cycle_type, pattern_def in cycle_patterns.items():
            patterns_context += f"\n{cycle_type}:\n"
            patterns_context += f"  Description: {pattern_def.get('description', 'N/A')}\n"

        system_prompt = """You are a financial data expert analyzing column correlation cycles.

Note: These are WITHIN-TABLE cycles showing column relationships, not cross-table business processes.

Return JSON:
{
  "cycle_type": "data_quality_indicator",
  "confidence": 0.85,
  "explanation": "Description of what this correlation means",
  "business_value": "medium",
  "is_expected": true,
  "recommendation": null
}"""

        user_prompt = f"""{cycle_info}

{patterns_context}

Classify this within-table cycle."""

        request = LLMRequest(
            prompt=f"{system_prompt}\n\n{user_prompt}",
            max_tokens=800,
            temperature=0.3,
            response_format="json",
        )

        response_result = await llm_provider.complete(request)

        if not response_result.success or not response_result.value:
            return Result.fail(f"LLM classification failed: {response_result.error}")

        response_text = response_result.value.content.strip()
        try:
            classification = json.loads(response_text)
            return Result.ok(classification)
        except json.JSONDecodeError as e:
            return Result.fail(f"Invalid JSON from LLM: {e}")

    except Exception as e:
        return Result.fail(f"Classification failed: {e}")
