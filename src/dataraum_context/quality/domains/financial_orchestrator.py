"""Financial Quality Orchestrator - Correct Architecture.

This is the main entry point for financial quality analysis.

Flow:
1. Compute ALL numbers (financial.py + topological.py)
2. LLM interprets with config context (financial_llm.py)
3. Return comprehensive result

NO pattern matching - LLM does all interpretation.
"""

import logging
from typing import Any

import duckdb
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models.base import Result
from dataraum_context.llm import LLMService
from dataraum_context.quality.domains.financial import analyze_financial_quality
from dataraum_context.quality.domains.financial_llm import (
    classify_financial_cycle_with_llm,
    interpret_financial_quality_with_llm,
)
from dataraum_context.quality.topological import analyze_topological_quality
from dataraum_context.storage.models_v2.core import Column, Table
from dataraum_context.storage.models_v2.semantic_context import SemanticAnnotation

logger = logging.getLogger(__name__)


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

        # If no LLM, return computed metrics only
        if llm_service is None:
            logger.info("No LLM service - returning computed metrics only")
            return Result.ok(
                {
                    "financial_metrics": financial_metrics,
                    "topological_metrics": topological_metrics,
                    "classified_cycles": [],
                    "interpretation": None,
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
        # LAYER 3: LLM INTERPRETATION (Holistic assessment)
        # ============================================================================

        logger.info("Generating LLM interpretation of all metrics")

        interpretation_result = await interpret_financial_quality_with_llm(
            financial_metrics=financial_metrics,
            topological_metrics=topological_metrics,
            classified_cycles=classified_cycles,
            llm_service=llm_service,
        )

        if not interpretation_result.success:
            logger.error(f"LLM interpretation failed: {interpretation_result.error}")
            # Return without interpretation
            return Result.ok(
                {
                    "financial_metrics": financial_metrics,
                    "topological_metrics": topological_metrics,
                    "classified_cycles": classified_cycles,
                    "interpretation": None,
                    "interpretation_error": interpretation_result.error,
                    "llm_available": True,
                }
            )

        interpretation = interpretation_result.unwrap()

        # ============================================================================
        # RETURN COMPLETE RESULT
        # ============================================================================

        return Result.ok(
            {
                "financial_metrics": financial_metrics,
                "topological_metrics": topological_metrics,
                "classified_cycles": classified_cycles,
                "interpretation": interpretation,
                "llm_available": True,
            }
        )

    except Exception as e:
        logger.error(f"Complete financial quality analysis failed: {e}")
        return Result.fail(f"Analysis failed: {e}")
