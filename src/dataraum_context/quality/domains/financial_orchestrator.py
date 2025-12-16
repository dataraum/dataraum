"""Financial Quality Orchestrator - Complete Financial Domain Analysis.

This is the main entry point for financial quality analysis.

Architecture:
    Layer 1:   Compute metrics (financial.py + topological.py)
    Layer 1.5: Domain rules (fiscal stability, anomalies, quality score)
    Layer 2:   LLM classification (cycle interpretation)
    Layer 3:   LLM interpretation (holistic assessment)

The domain rules in Layer 1.5 provide deterministic, auditable outputs
that complement the LLM's interpretive capabilities.
"""

import logging
from pathlib import Path
from typing import Any

import duckdb
import yaml
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models.base import Result
from dataraum_context.enrichment.db_models import (
    BusinessCycleClassification,
    MultiTableTopologyMetrics,
    SemanticAnnotation,
)
from dataraum_context.enrichment.relationships import (
    EnrichedRelationship,
    analyze_relationship_graph,
    gather_relationships,
)
from dataraum_context.llm import LLMService
from dataraum_context.llm.providers.base import LLMRequest
from dataraum_context.quality.domains.financial import analyze_financial_quality
from dataraum_context.quality.domains.financial_llm import (
    classify_financial_cycle_with_llm,
    interpret_financial_quality_with_llm,
)
from dataraum_context.quality.models import TopologicalAnomaly
from dataraum_context.quality.topological import analyze_topological_quality
from dataraum_context.storage.models_v2.core import Column, Table

logger = logging.getLogger(__name__)

# =============================================================================
# Configuration Loading
# =============================================================================

_FINANCIAL_CONFIG_CACHE: dict[str, Any] | None = None


def _load_financial_config() -> dict[str, Any]:
    """Load financial domain configuration from YAML."""
    global _FINANCIAL_CONFIG_CACHE
    if _FINANCIAL_CONFIG_CACHE is not None:
        return _FINANCIAL_CONFIG_CACHE

    config_paths = [
        Path("config/domains/financial.yaml"),
        Path(__file__).parent.parent.parent.parent.parent / "config/domains/financial.yaml",
    ]

    for path in config_paths:
        if path.exists():
            with open(path) as f:
                _FINANCIAL_CONFIG_CACHE = yaml.safe_load(f)
                return _FINANCIAL_CONFIG_CACHE

    logger.warning("Financial config not found, using empty config")
    return {}


# =============================================================================
# Layer 1.5: Domain Rules (Deterministic, Auditable)
# =============================================================================


def assess_fiscal_stability(
    stability: Any,  # StabilityAnalysis from topological analysis
    temporal_context: dict[str, Any],
) -> dict[str, Any]:
    """Enhance stability analysis with fiscal period awareness.

    Distinguishes between:
    - Fiscal period effects (expected, recurring changes)
    - Structural changes (unexpected, permanent topology shifts)

    Args:
        stability: StabilityAnalysis object from topological analysis
        temporal_context: Dict with fiscal calendar information

    Returns:
        Dict with enhanced stability assessment including fiscal context
    """
    if stability is None:
        return {
            "stability_level": "unknown",
            "fiscal_context": None,
            "is_fiscal_period_effect": False,
            "pattern_type": "unknown",
        }

    # Extract fiscal calendar info
    current_period = temporal_context.get("current_fiscal_period")
    is_period_end = temporal_context.get("is_period_end", False)
    is_quarter_end = temporal_context.get("is_quarter_end", False)
    is_year_end = temporal_context.get("is_year_end", False)

    # Determine if changes are fiscal period effects
    fiscal_context = None
    is_fiscal_period_effect = False
    pattern_type = "structural_change"  # Default assumption

    stability_level = getattr(stability, "stability_level", "unknown")

    if is_year_end and stability_level in ["significant_changes", "unstable"]:
        fiscal_context = "fiscal_year_end_close"
        is_fiscal_period_effect = True
        pattern_type = "recurring_spike"

    elif is_quarter_end and stability_level in ["minor_changes", "significant_changes"]:
        fiscal_context = "quarter_end_close"
        is_fiscal_period_effect = True
        pattern_type = "recurring_spike"

    elif is_period_end and stability_level == "minor_changes":
        fiscal_context = "month_end_close"
        is_fiscal_period_effect = True
        pattern_type = "recurring_spike"

    elif stability_level in ["significant_changes", "unstable"]:
        fiscal_context = "mid_period"
        is_fiscal_period_effect = False
        pattern_type = "structural_change"

    # Generate interpretation
    interpretation = _interpret_fiscal_stability(
        stability_level, is_fiscal_period_effect, fiscal_context
    )

    return {
        "original_stability_level": stability_level,
        "fiscal_context": fiscal_context,
        "is_fiscal_period_effect": is_fiscal_period_effect,
        "pattern_type": pattern_type,
        "affected_periods": [current_period] if current_period else [],
        "components_added": getattr(stability, "components_added", 0),
        "components_removed": getattr(stability, "components_removed", 0),
        "cycles_added": getattr(stability, "cycles_added", 0),
        "cycles_removed": getattr(stability, "cycles_removed", 0),
        "interpretation": interpretation,
    }


def _interpret_fiscal_stability(
    stability_level: str, is_fiscal_effect: bool, fiscal_context: str | None
) -> str:
    """Generate human-readable interpretation of stability assessment."""
    if is_fiscal_effect:
        if fiscal_context == "fiscal_year_end_close":
            return (
                "Expected topology changes due to fiscal year-end close. "
                "Increased activity and relationship complexity is normal."
            )
        elif fiscal_context == "quarter_end_close":
            return (
                "Recurring topology changes due to quarter-end close. "
                "Period-end spikes are expected."
            )
        elif fiscal_context == "month_end_close":
            return "Minor topology changes due to month-end close. Normal recurring pattern."
        else:
            return "Changes appear related to fiscal period effects."
    else:
        if stability_level == "unstable":
            return (
                "ALERT: Significant structural changes detected outside normal fiscal periods. "
                "Investigate data quality or business process changes."
            )
        elif stability_level == "significant_changes":
            return (
                "WARNING: Notable structural changes detected mid-period. "
                "May indicate data quality issues or business changes."
            )
        else:
            return "Topology is stable with minor expected variations."


def detect_financial_anomalies(
    topological_result: Any,  # TopologicalQualityResult
    classified_cycles: list[dict[str, Any]],
) -> list[TopologicalAnomaly]:
    """Detect financial-specific topological anomalies.

    Anomaly types:
    - excessive_financial_cycles: Too many cycles for financial data
    - unclassified_financial_cycles: Cycles couldn't be classified
    - financial_data_fragmentation: Disconnected components
    - missing_financial_cycles: Expected cycles not found
    - cost_center_isolation: Orphaned components

    Args:
        topological_result: TopologicalQualityResult from analysis
        classified_cycles: List of LLM-classified cycle dicts

    Returns:
        List of TopologicalAnomaly objects with financial context
    """
    anomalies: list[TopologicalAnomaly] = []

    if topological_result is None:
        return anomalies

    # Get table name for affected_tables
    table_name = getattr(topological_result, "table_name", "unknown")

    # Anomaly 1: Unusual cycle complexity
    cycle_count = len(classified_cycles)
    unclassified = [c for c in classified_cycles if c.get("cycle_type") in [None, "UNKNOWN"]]

    if cycle_count > 15:
        anomalies.append(
            TopologicalAnomaly(
                anomaly_type="excessive_financial_cycles",
                severity="high",
                description=f"Unusually high number of cycles ({cycle_count}) for financial data",
                evidence={"cycle_count": cycle_count, "expected_max": 10},
                affected_tables=[table_name],
                affected_columns=[],
            )
        )

    if len(unclassified) > 5:
        anomalies.append(
            TopologicalAnomaly(
                anomaly_type="unclassified_financial_cycles",
                severity="medium",
                description=f"{len(unclassified)} cycles could not be classified",
                evidence={"unclassified_count": len(unclassified), "total_count": cycle_count},
                affected_tables=[table_name],
                affected_columns=[],
            )
        )

    # Anomaly 2: Disconnected components
    if hasattr(topological_result, "betti_numbers"):
        betti_0 = topological_result.betti_numbers.betti_0
        if betti_0 > 3:
            anomalies.append(
                TopologicalAnomaly(
                    anomaly_type="financial_data_fragmentation",
                    severity="high",
                    description=f"Financial data has {betti_0} disconnected components. Expected: 1-2",
                    evidence={
                        "component_count": betti_0,
                        "expected_max": 2,
                        "interpretation": "Accounts or entities are not properly linked",
                    },
                    affected_tables=[table_name],
                    affected_columns=[],
                )
            )

    # Anomaly 3: Missing expected financial cycles
    cycle_types = {c.get("cycle_type") for c in classified_cycles if c.get("cycle_type")}
    expected_cycles = {"accounts_receivable_cycle", "expense_cycle", "revenue_cycle"}
    missing_cycles = expected_cycles - cycle_types

    if missing_cycles and cycle_count > 0:
        anomalies.append(
            TopologicalAnomaly(
                anomaly_type="missing_financial_cycles",
                severity="medium",
                description=f"Expected financial cycles not detected: {', '.join(missing_cycles)}",
                evidence={
                    "missing_cycles": list(missing_cycles),
                    "detected_cycles": list(cycle_types),
                },
                affected_tables=[table_name],
                affected_columns=[],
            )
        )

    # Anomaly 4: Cost center isolation
    orphaned = getattr(topological_result, "orphaned_components", 0)
    if orphaned > 0:
        anomalies.append(
            TopologicalAnomaly(
                anomaly_type="cost_center_isolation",
                severity="medium",
                description=f"{orphaned} isolated components detected",
                evidence={"orphaned_count": orphaned},
                affected_tables=[table_name],
                affected_columns=[],
            )
        )

    return anomalies


async def analyze_complete_financial_quality(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    llm_service: LLMService | None = None,
) -> Result[dict[str, Any]]:
    """Complete financial quality analysis with LLM interpretation.

    Correct architecture:
    1. Python computes ALL metrics
    2. LLM interprets results with domain context
    3. Returns comprehensive assessment

    Args:
        table_id: Table to analyze
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session
        llm_service: LLM service (optional - returns metrics only if None)

    Returns:
        Result containing:
        - financial_metrics: Computed numbers from financial.py
        - topological_metrics: Computed structure from topological.py
        - classified_cycles: LLM classifications (if LLM available)
        - interpretation: LLM interpretation (if LLM available)

    Example:
        result = await analyze_complete_financial_quality(
            table_id="table-123",
            duckdb_conn=conn,
            session=session,
            llm_service=llm_service
        )

        if result.success:
            data = result.value
            print(f"Quality Score: {data['interpretation']['overall_quality_score']}")
            print(f"Critical Issues: {data['interpretation']['critical_issues']}")
    """
    try:
        # ============================================================================
        # LAYER 1: COMPUTE ALL METRICS (Pure Python + SQL)
        # ============================================================================

        logger.info(f"Computing financial metrics for table {table_id}")

        # 1.1: Financial accounting metrics
        financial_result = await analyze_financial_quality(
            table_id=table_id, duckdb_conn=duckdb_conn, session=session
        )

        if not financial_result.success:
            logger.error(f"Financial analysis failed: {financial_result.error}")
            return Result.fail(f"Financial analysis failed: {financial_result.error}")

        financial_quality = financial_result.unwrap()

        # Build financial metrics dict for LLM
        financial_metrics = {
            "double_entry_balanced": financial_quality.double_entry_balanced,
            "net_difference": financial_quality.balance_difference or 0.0,
            "total_debits": (
                financial_quality.double_entry_details.total_debits
                if financial_quality.double_entry_details
                else 0.0
            ),
            "total_credits": (
                financial_quality.double_entry_details.total_credits
                if financial_quality.double_entry_details
                else 0.0
            ),
            "trial_balance_holds": financial_quality.accounting_equation_holds,
            "total_assets": financial_quality.assets_total or 0.0,
            "total_liabilities": financial_quality.liabilities_total or 0.0,
            "total_equity": financial_quality.equity_total or 0.0,
            "trial_balance_difference": (financial_quality.assets_total or 0.0)
            - (
                (financial_quality.liabilities_total or 0.0)
                + (financial_quality.equity_total or 0.0)
            ),
            "sign_compliance_rate": financial_quality.sign_convention_compliance or 0.0,
            "sign_violations": len(financial_quality.sign_violations)
            if financial_quality.sign_violations
            else 0,
            "period_start": (
                financial_quality.period_integrity_details[0].period_start.isoformat()
                if financial_quality.period_integrity_details
                else None
            ),
            "period_end": (
                financial_quality.period_integrity_details[0].period_end.isoformat()
                if financial_quality.period_integrity_details
                else None
            ),
            "missing_days": (
                financial_quality.period_integrity_details[0].missing_days
                if financial_quality.period_integrity_details
                else 0
            ),
            "has_cutoff_issues": not financial_quality.period_end_cutoff_clean,
        }

        # 1.2: Topological structure metrics
        logger.info(f"Computing topological metrics for table {table_id}")

        topological_result = await analyze_topological_quality(
            table_id=table_id, duckdb_conn=duckdb_conn, session=session
        )

        if not topological_result.success:
            logger.warning(f"Topological analysis failed: {topological_result.error}")
            # Continue with None - not critical for financial analysis
            topological_quality = None
            topological_metrics: dict[str, Any] = {
                "betti_0": None,
                "betti_1": None,
                "betti_2": None,
                "total_complexity": None,
                "persistent_entropy": None,
                "orphaned_components": None,
                "cycles": [],
            }
        else:
            topological_quality = topological_result.unwrap()
            topological_metrics = {
                "betti_0": topological_quality.betti_numbers.betti_0,
                "betti_1": topological_quality.betti_numbers.betti_1,
                "betti_2": topological_quality.betti_numbers.betti_2,
                "total_complexity": topological_quality.betti_numbers.total_complexity,
                "persistent_entropy": topological_quality.persistent_entropy,
                "orphaned_components": topological_quality.orphaned_components,
                "cycles": topological_quality.persistent_cycles,
            }

        # If no LLM, still run domain rules and return
        if llm_service is None:
            logger.info("No LLM service - running domain rules only")

            # Run Layer 1.5 without LLM
            stability = (
                getattr(topological_quality, "stability", None) if topological_quality else None
            )
            fiscal_stability = assess_fiscal_stability(stability, {})
            financial_anomalies = detect_financial_anomalies(topological_quality, [])

            domain_analysis = {
                "fiscal_stability": fiscal_stability,
                "anomalies": [
                    {
                        "type": a.anomaly_type,
                        "severity": a.severity,
                        "description": a.description,
                        "evidence": a.evidence,
                    }
                    for a in financial_anomalies
                ],
                "anomaly_count": len(financial_anomalies),
            }

            return Result.ok(
                {
                    "financial_metrics": financial_metrics,
                    "topological_metrics": topological_metrics,
                    "classified_cycles": [],
                    "domain_analysis": domain_analysis,
                    "llm_interpretation": None,
                    "llm_available": False,
                }
            )

        # ============================================================================
        # LAYER 2: LLM CLASSIFICATION (Cycle interpretation with config context)
        # ============================================================================

        logger.info("Classifying cycles with LLM")

        # Get table and column info for context
        table = await session.get(Table, table_id)
        if not table:
            return Result.fail(f"Table {table_id} not found")

        # Get columns
        stmt = select(Column).where(Column.table_id == table_id)
        columns_result = await session.execute(stmt)
        columns = list(columns_result.scalars().all())

        # Get semantic annotations (join through columns)
        stmt_semantic = (
            select(SemanticAnnotation, Column.column_name)
            .join(Column, SemanticAnnotation.column_id == Column.column_id)
            .where(Column.table_id == table_id)
        )
        semantic_result = await session.execute(stmt_semantic)
        semantic_annotations = {
            col_name: ann.semantic_role
            for ann, col_name in semantic_result.all()
            if ann.semantic_role
        }

        # Classify each cycle with LLM
        classified_cycles = []

        cycles_list = topological_metrics.get("cycles", [])
        if cycles_list:
            for cycle in cycles_list:
                # Determine involved columns (simplified - would need graph analysis)
                # For now, use all columns as potential context
                column_names = [col.column_name for col in columns]

                classification_result = await classify_financial_cycle_with_llm(
                    cycle=cycle,
                    table_name=table.table_name,
                    column_names=column_names,
                    semantic_roles=semantic_annotations,
                    llm_service=llm_service,
                )

                if classification_result.success:
                    classification = classification_result.unwrap()
                    classified_cycles.append(classification)
                else:
                    logger.warning(f"Cycle classification failed: {classification_result.error}")
                    # Include unclassified cycle
                    classified_cycles.append(
                        {
                            "cycle_type": "UNKNOWN",
                            "confidence": 0.0,
                            "explanation": f"Classification failed: {classification_result.error}",
                            "business_value": "unknown",
                            "is_expected": False,
                            "recommendation": None,
                        }
                    )

        # ============================================================================
        # LAYER 1.5: DOMAIN RULES (Deterministic, Auditable)
        # ============================================================================

        logger.info("Applying domain rules for anomaly detection and scoring")

        # Assess fiscal stability (if stability info available)
        stability = getattr(topological_quality, "stability", None) if topological_quality else None
        temporal_context: dict[str, Any] = {}  # Could be passed as parameter in future
        fiscal_stability = assess_fiscal_stability(stability, temporal_context)

        # Detect financial-specific anomalies
        financial_anomalies = detect_financial_anomalies(topological_quality, classified_cycles)

        # Build domain analysis summary (no scores - just anomalies and metrics)
        domain_analysis = {
            "fiscal_stability": fiscal_stability,
            "anomalies": [
                {
                    "type": a.anomaly_type,
                    "severity": a.severity,
                    "description": a.description,
                    "evidence": a.evidence,
                }
                for a in financial_anomalies
            ],
            "cycle_count": len(classified_cycles),
            "anomaly_count": len(financial_anomalies),
        }

        # ============================================================================
        # LAYER 3: LLM INTERPRETATION (Holistic assessment)
        # ============================================================================

        logger.info("Generating LLM interpretation of all metrics")

        interpretation_result = await interpret_financial_quality_with_llm(
            financial_metrics=financial_metrics,
            topological_metrics=topological_metrics,
            classified_cycles=classified_cycles,
            llm_service=llm_service,
            domain_analysis=domain_analysis,  # Pass domain analysis as context
        )

        if not interpretation_result.success:
            logger.error(f"LLM interpretation failed: {interpretation_result.error}")
            # Return with domain analysis even if LLM fails
            return Result.ok(
                {
                    "financial_metrics": financial_metrics,
                    "topological_metrics": topological_metrics,
                    "classified_cycles": classified_cycles,
                    "domain_analysis": domain_analysis,
                    "llm_interpretation": None,
                    "interpretation_error": interpretation_result.error,
                    "llm_available": True,
                }
            )

        llm_interpretation = interpretation_result.unwrap()

        # ============================================================================
        # RETURN COMPLETE RESULT
        # ============================================================================

        return Result.ok(
            {
                "financial_metrics": financial_metrics,
                "topological_metrics": topological_metrics,
                "classified_cycles": classified_cycles,
                "domain_analysis": domain_analysis,
                "llm_interpretation": llm_interpretation,
                "llm_available": True,
            }
        )

    except Exception as e:
        logger.error(f"Complete financial quality analysis failed: {e}")
        return Result.fail(f"Analysis failed: {e}")


# =============================================================================
# Multi-Table Business Cycle Analysis
# =============================================================================


async def classify_cross_table_cycle_with_llm(
    cycle_table_ids: list[str],
    relationships: list[EnrichedRelationship],
    table_semantics: dict[str, dict[str, Any]],
    llm_service: LLMService,
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
        llm_service: LLM service

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
        config = _load_financial_config()
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
- transactions → customers → transactions (AR cycle)
- transactions → vendors → transactions (AP cycle)
- customers → orders → invoices → payments → customers (Revenue cycle)

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
            return Result.fail(f"Invalid JSON from LLM: {e}")

    except Exception as e:
        logger.error(f"Cross-table cycle classification failed: {e}")
        return Result.fail(f"Classification failed: {e}")


async def analyze_complete_financial_dataset_quality(
    table_ids: list[str],
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    llm_service: LLMService | None = None,
) -> Result[dict[str, Any]]:
    """Complete financial quality analysis for a multi-table dataset.

    This is the main entry point for analyzing business process cycles
    across multiple related tables.

    Architecture:
        Layer 1:   Per-table accounting checks (financial.py)
        Layer 2:   Relationship gathering (cross_table_multicollinearity.py)
        Layer 3:   Cross-table cycle detection (topological.py)
        Layer 4:   LLM business cycle classification (multi-label)
        Layer 5:   LLM holistic interpretation

    Args:
        table_ids: List of table IDs in the dataset
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session
        llm_service: LLM service (optional - returns raw cycles if None)

    Returns:
        Result containing:
        - per_table_metrics: Dict of table_id → financial metrics
        - relationships: List of detected relationships
        - cross_table_cycles: List of detected cycles (table ID lists)
        - classified_cycles: List of LLM classifications (if LLM available)
        - dataset_quality: Overall dataset assessment
        - interpretation: LLM interpretation (if LLM available)

    Example:
        result = await analyze_complete_financial_dataset_quality(
            table_ids=["transactions", "customers", "vendors", "products"],
            duckdb_conn=conn,
            session=session,
            llm_service=llm_service
        )

        if result.success:
            data = result.value
            for cycle in data["classified_cycles"]:
                print(f"{cycle['primary_type']}: {cycle['explanation']}")
    """
    try:
        logger.info(f"Analyzing financial dataset with {len(table_ids)} tables")

        # =====================================================================
        # LAYER 1: PER-TABLE ACCOUNTING CHECKS
        # =====================================================================

        per_table_metrics: dict[str, dict[str, Any]] = {}
        per_table_topological: dict[str, Any] = {}

        for table_id in table_ids:
            # Financial accounting checks
            financial_result = await analyze_financial_quality(
                table_id=table_id, duckdb_conn=duckdb_conn, session=session
            )

            if financial_result.success:
                fq = financial_result.unwrap()
                per_table_metrics[table_id] = {
                    "double_entry_balanced": fq.double_entry_balanced,
                    "balance_difference": fq.balance_difference,
                    "accounting_equation_holds": fq.accounting_equation_holds,
                    "sign_convention_compliance": fq.sign_convention_compliance,
                }
            else:
                logger.warning(
                    f"Financial analysis failed for {table_id}: {financial_result.error}"
                )
                per_table_metrics[table_id] = {"error": financial_result.error}

            # Single-table topological analysis (for data quality, not business cycles)
            topo_result = await analyze_topological_quality(
                table_id=table_id, duckdb_conn=duckdb_conn, session=session
            )

            if topo_result.success:
                tq = topo_result.unwrap()
                per_table_topological[table_id] = {
                    "betti_0": tq.betti_numbers.betti_0,
                    "betti_1": tq.betti_numbers.betti_1,
                    "structural_complexity": tq.structural_complexity,
                    "has_anomalies": tq.has_anomalies,
                }

        # =====================================================================
        # LAYER 2: RELATIONSHIP GATHERING
        # =====================================================================

        logger.info("Gathering relationships between tables")

        relationships = await gather_relationships(table_ids, session)

        logger.info(f"Found {len(relationships)} relationships")

        # =====================================================================
        # LAYER 3: CROSS-TABLE CYCLE DETECTION
        # =====================================================================

        logger.info("Detecting cross-table cycles")

        # analyze_relationship_graph uses duck typing - EnrichedRelationship has
        # the same attributes as RelationshipModel (from_table_id, to_table_id, etc.)
        graph_analysis = analyze_relationship_graph(
            table_ids,
            relationships,  # type: ignore[arg-type]
        )

        cross_table_cycles: list[list[str]] = graph_analysis.get("cycles", [])  # type: ignore[assignment]
        betti_0: int = graph_analysis.get("betti_0", 1)  # type: ignore[assignment]

        logger.info(
            f"Detected {len(cross_table_cycles)} cross-table cycles, {betti_0} connected components"
        )

        # If no LLM, return raw results
        if llm_service is None:
            logger.info("No LLM service - returning raw cycle detection results")
            return Result.ok(
                {
                    "per_table_metrics": per_table_metrics,
                    "per_table_topological": per_table_topological,
                    "relationships": [
                        {
                            "from_table": r.from_table,
                            "to_table": r.to_table,
                            "from_column": r.from_column,
                            "to_column": r.to_column,
                            "relationship_type": str(r.relationship_type),
                            "confidence": r.confidence,
                        }
                        for r in relationships
                    ],
                    "cross_table_cycles": cross_table_cycles,
                    "graph_betti_0": betti_0,
                    "classified_cycles": [],
                    "llm_available": False,
                }
            )

        # =====================================================================
        # LAYER 4: LLM BUSINESS CYCLE CLASSIFICATION
        # =====================================================================

        logger.info("Classifying cross-table cycles with LLM")

        # Build table semantics context
        table_semantics: dict[str, dict[str, Any]] = {}

        for table_id in table_ids:
            table = await session.get(Table, table_id)
            if not table:
                continue

            # Get columns
            stmt = select(Column).where(Column.table_id == table_id)
            columns_result = await session.execute(stmt)
            columns = list(columns_result.scalars().all())

            # Get semantic annotations
            stmt_semantic = (
                select(SemanticAnnotation, Column.column_name)
                .join(Column, SemanticAnnotation.column_id == Column.column_id)
                .where(Column.table_id == table_id)
            )
            semantic_result = await session.execute(stmt_semantic)
            semantic_roles = {
                col_name: ann.semantic_role
                for ann, col_name in semantic_result.all()
                if ann.semantic_role
            }

            table_semantics[table_id] = {
                "table_name": table.table_name,
                "key_columns": [
                    c.column_name
                    for c in columns
                    if "id" in c.column_name.lower() or "_key" in c.column_name.lower()
                ],
                "all_columns": [c.column_name for c in columns],
                "semantic_roles": semantic_roles,
            }

        # Classify each cross-table cycle
        classified_cycles: list[dict[str, Any]] = []

        for cycle in cross_table_cycles:
            classification_result = await classify_cross_table_cycle_with_llm(
                cycle_table_ids=cycle,
                relationships=relationships,
                table_semantics=table_semantics,
                llm_service=llm_service,
            )

            if classification_result.success:
                classification = classification_result.unwrap()
                classification["cycle_tables"] = cycle
                classified_cycles.append(classification)
            else:
                logger.warning(f"Cycle classification failed: {classification_result.error}")
                classified_cycles.append(
                    {
                        "cycle_tables": cycle,
                        "cycle_types": [],
                        "primary_type": "UNKNOWN",
                        "explanation": f"Classification failed: {classification_result.error}",
                        "business_value": "unknown",
                        "completeness": "unknown",
                    }
                )

        # =====================================================================
        # LAYER 5: LLM HOLISTIC INTERPRETATION
        # =====================================================================

        logger.info("Generating holistic dataset interpretation")

        # Build interpretation context
        interpretation_context = f"""
=== FINANCIAL DATASET ANALYSIS ===

Tables Analyzed: {len(table_ids)}
Relationships Detected: {len(relationships)}
Connected Components: {betti_0}
Cross-Table Cycles: {len(cross_table_cycles)}

=== PER-TABLE ACCOUNTING HEALTH ===
"""
        for table_id, metrics in per_table_metrics.items():
            table_name = table_semantics.get(table_id, {}).get("table_name", table_id)
            interpretation_context += f"\n{table_name}:\n"
            for key, value in metrics.items():
                interpretation_context += f"  {key}: {value}\n"

        interpretation_context += "\n=== CLASSIFIED BUSINESS CYCLES ===\n"
        for i, classified in enumerate(classified_cycles, 1):
            interpretation_context += f"\nCycle {i}:\n"
            interpretation_context += f"  Tables: {classified.get('cycle_tables', [])}\n"
            interpretation_context += f"  Type: {classified.get('primary_type', 'UNKNOWN')}\n"
            interpretation_context += f"  Confidence: {classified.get('cycle_types', [{}])[0].get('confidence', 0) if classified.get('cycle_types') else 0:.0%}\n"
            interpretation_context += (
                f"  Business Value: {classified.get('business_value', 'unknown')}\n"
            )
            interpretation_context += (
                f"  Completeness: {classified.get('completeness', 'unknown')}\n"
            )

        # Expected cycles from config
        config = _load_financial_config()
        expected_cycles: list[str] = config.get("expected_cycles", [])
        detected_types: set[str] = {
            c.get("primary_type", "UNKNOWN") for c in classified_cycles if c.get("primary_type")
        }
        missing_expected = set(expected_cycles) - detected_types

        interpretation_context += "\n=== EXPECTED VS DETECTED ===\n"
        interpretation_context += f"Expected cycles: {', '.join(expected_cycles)}\n"
        interpretation_context += f"Detected cycles: {', '.join(detected_types)}\n"
        interpretation_context += (
            f"Missing: {', '.join(missing_expected) if missing_expected else 'None'}\n"
        )

        # Generate LLM interpretation
        system_prompt = """You are a financial data quality expert providing holistic assessment of a multi-table dataset.

Your task: Interpret the business process health of this dataset based on:
1. Per-table accounting integrity
2. Detected business cycles (AR, AP, Revenue, etc.)
3. Missing expected cycles
4. Overall data connectivity

Provide:
1. Overall dataset quality score (0-1)
2. Business process health assessment
3. Critical issues (blocking problems)
4. Recommendations (prioritized)
5. Executive summary (2-3 sentences)

Return JSON:
{
  "overall_quality_score": 0.75,
  "business_process_health": {
    "accounts_receivable": "healthy",
    "accounts_payable": "partial",
    "revenue": "missing"
  },
  "critical_issues": ["No revenue cycle detected"],
  "recommendations": [
    {"priority": "HIGH", "action": "...", "rationale": "..."}
  ],
  "summary": "..."
}"""

        request = LLMRequest(
            prompt=f"{system_prompt}\n\n{interpretation_context}\n\nProvide holistic assessment.",
            max_tokens=1500,
            temperature=0.3,
            response_format="json",
        )

        interpretation_response = await llm_service.provider.complete(request)

        interpretation = None
        if interpretation_response.success and interpretation_response.value:
            import json

            try:
                interpretation = json.loads(interpretation_response.value.content.strip())
            except json.JSONDecodeError:
                logger.warning("Failed to parse interpretation JSON")

        # =====================================================================
        # PERSIST MULTI-TABLE TOPOLOGY AND BUSINESS CYCLES
        # =====================================================================

        logger.info("Persisting multi-table topology and business cycles")

        # Create MultiTableTopologyMetrics record
        topology_metrics = MultiTableTopologyMetrics(
            table_ids=table_ids,
            cross_table_cycles=len(cross_table_cycles),
            graph_betti_0=betti_0,
            relationship_count=len(relationships),
            has_cross_table_cycles=len(cross_table_cycles) > 0,
            is_connected_graph=betti_0 == 1,
            analysis_data={
                "per_table_topological": per_table_topological,
                "relationships": [
                    {
                        "from_table": r.from_table,
                        "to_table": r.to_table,
                        "from_column": r.from_column,
                        "to_column": r.to_column,
                    }
                    for r in relationships
                ],
                "cross_table_cycles": cross_table_cycles,
            },
        )
        session.add(topology_metrics)
        await session.flush()  # Get the analysis_id

        # Persist each classified business cycle
        persisted_cycle_ids = []
        for classified in classified_cycles:
            # Get confidence from first cycle_type if available
            cycle_types = classified.get("cycle_types", [])
            confidence = cycle_types[0].get("confidence", 0.0) if cycle_types else 0.0

            cycle_record = BusinessCycleClassification(
                analysis_id=topology_metrics.analysis_id,
                cycle_type=classified.get("primary_type", "UNKNOWN"),
                confidence=confidence,
                business_value=classified.get("business_value", "unknown"),
                completeness=classified.get("completeness", "unknown"),
                table_ids=classified.get("cycle_tables", []),
                explanation=classified.get("explanation"),
                missing_elements=classified.get("missing_elements"),
                llm_model=llm_service.provider.__class__.__name__ if llm_service else None,
            )
            session.add(cycle_record)
            persisted_cycle_ids.append(cycle_record.cycle_id)

        await session.commit()
        logger.info(
            f"Persisted {len(persisted_cycle_ids)} business cycle classifications "
            f"for analysis {topology_metrics.analysis_id}"
        )

        # =====================================================================
        # RETURN COMPLETE RESULT
        # =====================================================================

        return Result.ok(
            {
                "per_table_metrics": per_table_metrics,
                "per_table_topological": per_table_topological,
                "relationships": [
                    {
                        "from_table": r.from_table,
                        "to_table": r.to_table,
                        "from_column": r.from_column,
                        "to_column": r.to_column,
                        "relationship_type": str(r.relationship_type),
                        "confidence": r.confidence,
                    }
                    for r in relationships
                ],
                "cross_table_cycles": cross_table_cycles,
                "graph_betti_0": betti_0,
                "classified_cycles": classified_cycles,
                "missing_expected_cycles": list(missing_expected),
                "interpretation": interpretation,
                "llm_available": True,
                # Persistence references
                "analysis_id": topology_metrics.analysis_id,
                "persisted_cycle_ids": persisted_cycle_ids,
            }
        )

    except Exception as e:
        logger.error(f"Complete financial dataset analysis failed: {e}")
        return Result.fail(f"Dataset analysis failed: {e}")
