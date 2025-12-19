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
from typing import Any

import duckdb
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.analysis.topology import analyze_topological_quality
from dataraum_context.analysis.topology.db_models import (
    BusinessCycleClassification,
    MultiTableTopologyMetrics,
)
from dataraum_context.core.models.base import Result
from dataraum_context.domains.financial import analyze_financial_quality
from dataraum_context.domains.financial.config import load_financial_config
from dataraum_context.domains.financial.cycles import (
    assess_fiscal_stability,
    classify_cross_table_cycle_with_llm,
    classify_financial_cycle_with_llm,
    detect_financial_anomalies,
    interpret_financial_quality_with_llm,
)
from dataraum_context.enrichment.db_models import SemanticAnnotation
from dataraum_context.enrichment.relationships import (
    analyze_relationship_graph,
    gather_relationships,
)
from dataraum_context.llm.providers.base import LLMProvider, LLMRequest
from dataraum_context.storage import Column, Table

logger = logging.getLogger(__name__)


async def analyze_complete_financial_quality(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    llm_provider: LLMProvider | None = None,
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
        llm_provider: LLM provider (optional - returns metrics only if None)

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
            llm_provider=llm_provider
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
        if llm_provider is None:
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
                    llm_provider=llm_provider,
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
            llm_provider=llm_provider,
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


async def analyze_complete_financial_dataset_quality(
    table_ids: list[str],
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    llm_provider: LLMProvider | None = None,
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
        llm_provider: LLM provider (optional - returns raw cycles if None)

    Returns:
        Result containing:
        - per_table_metrics: Dict of table_id -> financial metrics
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
            llm_provider=llm_provider
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
        if llm_provider is None:
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
                llm_provider=llm_provider,
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
        config = load_financial_config()
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

        interpretation_response = await llm_provider.complete(request)

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
                llm_model=llm_provider.__class__.__name__ if llm_provider else None,
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
