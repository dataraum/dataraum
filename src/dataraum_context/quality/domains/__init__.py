"""Domain-specific quality modules.

Each domain provides specialized quality checks and rules:

**Accounting Quality Checks:**
- financial: Double-entry, trial balance, sign conventions, fiscal periods

**Topological Domain Analysis:**
- financial: Fiscal stability, financial anomalies, domain-weighted scoring

**LLM-Enhanced Analysis:**
- financial_orchestrator: Complete flow with LLM cycle classification
- financial_llm: LLM classification and interpretation functions

Usage:
    # Accounting quality checks
    from dataraum_context.quality.domains import financial
    result = await financial.analyze_financial_quality(table_id, conn, session)

    # Basic domain analysis (no LLM)
    from dataraum_context.quality.domains.financial import FinancialDomainAnalyzer
    analyzer = FinancialDomainAnalyzer()
    enhanced = analyzer.analyze(topological_result, temporal_context)

    # LLM-enhanced analysis (recommended)
    from dataraum_context.quality.domains.financial_orchestrator import (
        analyze_complete_financial_quality,
    )
    result = await analyze_complete_financial_quality(table_id, conn, session, llm_service)
"""

from dataraum_context.quality.domains import financial
from dataraum_context.quality.domains.financial import (
    FinancialDomainAnalyzer,
    assess_fiscal_stability,
    compute_financial_quality_score,
    detect_financial_anomalies,
)

__all__ = [
    "financial",
    "FinancialDomainAnalyzer",
    "assess_fiscal_stability",
    "detect_financial_anomalies",
    "compute_financial_quality_score",
]
