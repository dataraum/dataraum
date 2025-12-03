"""Domain-specific quality modules.

Each domain provides specialized quality checks and rules:

**Accounting Quality Checks:**
- financial: Double-entry, trial balance, sign conventions, fiscal periods

**Topological Domain Analysis:**
- financial: Cycle classification, fiscal stability, financial anomalies

Usage:
    # Accounting quality checks
    from dataraum_context.quality.domains import financial
    result = await financial.analyze_financial_quality(table_id, conn, session)

    # Topological domain analysis
    from dataraum_context.quality.domains.financial import FinancialDomainAnalyzer
    analyzer = FinancialDomainAnalyzer()
    enhanced = analyzer.analyze(topological_result, temporal_context)
"""

from dataraum_context.quality.domains import financial
from dataraum_context.quality.domains.financial import (
    FinancialDomainAnalyzer,
    assess_fiscal_stability,
    compute_financial_quality_score,
    detect_financial_anomalies,
    detect_financial_cycles,
)

__all__ = [
    "financial",
    "FinancialDomainAnalyzer",
    "detect_financial_cycles",
    "assess_fiscal_stability",
    "detect_financial_anomalies",
    "compute_financial_quality_score",
]
