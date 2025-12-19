"""Financial domain quality module.

Provides financial accounting-specific quality checks:
- Double-entry balance validation
- Trial balance checks (Assets = Liabilities + Equity)
- Sign convention validation
- Fiscal period integrity

Also provides cycle analysis with LLM interpretation:
- Cross-table business cycle detection
- LLM cycle classification
- Fiscal stability rules

Usage:
    # Using the analyzer (recommended)
    from dataraum_context.domains.financial import FinancialDomainAnalyzer

    analyzer = FinancialDomainAnalyzer()
    result = await analyzer.analyze(table_id, conn, session)
    issues = analyzer.get_issues(result.unwrap())

    # Using the convenience function
    from dataraum_context.domains.financial import analyze_financial_quality

    result = await analyze_financial_quality(table_id, conn, session)

    # Complete analysis with LLM
    from dataraum_context.domains.financial import (
        analyze_complete_financial_quality,
        analyze_complete_financial_dataset_quality,
    )

    # Using individual checks
    from dataraum_context.domains.financial.checks import (
        check_double_entry_balance,
        check_trial_balance,
    )

    # Using cycle analysis
    from dataraum_context.domains.financial.cycles import (
        assess_fiscal_stability,
        detect_financial_anomalies,
        classify_financial_cycle_with_llm,
    )
"""

from dataraum_context.domains.financial.analyzer import (
    FinancialDomainAnalyzer,
    analyze_financial_quality,
)
from dataraum_context.domains.financial.checks import (
    check_double_entry_balance,
    check_fiscal_period_integrity,
    check_sign_conventions,
    check_trial_balance,
)
from dataraum_context.domains.financial.config import (
    clear_config_cache,
    load_financial_config,
)
from dataraum_context.domains.financial.cycles import (
    assess_fiscal_stability,
    classify_cross_table_cycle_with_llm,
    classify_financial_cycle_with_llm,
    detect_financial_anomalies,
    interpret_financial_quality_with_llm,
)
from dataraum_context.domains.financial.db_models import (
    DoubleEntryCheck,
    FinancialQualityMetrics,
    FiscalPeriodIntegrity,
    SignConventionViolation,
    TrialBalanceCheck,
)
from dataraum_context.domains.financial.models import (
    DoubleEntryResult,
    FinancialQualityConfig,
    FinancialQualityIssue,
    FinancialQualityResult,
    FiscalPeriodIntegrityCheck,
    SignConventionConfig,
    TrialBalanceResult,
)
from dataraum_context.domains.financial.models import (
    SignConventionViolation as SignConventionViolationModel,
)
from dataraum_context.domains.financial.orchestrator import (
    analyze_complete_financial_dataset_quality,
    analyze_complete_financial_quality,
)

__all__ = [
    # Analyzer
    "FinancialDomainAnalyzer",
    "analyze_financial_quality",
    # Orchestrator (complete analysis with LLM)
    "analyze_complete_financial_quality",
    "analyze_complete_financial_dataset_quality",
    # Config
    "load_financial_config",
    "clear_config_cache",
    # Check functions
    "check_double_entry_balance",
    "check_trial_balance",
    "check_sign_conventions",
    "check_fiscal_period_integrity",
    # Cycle functions
    "assess_fiscal_stability",
    "detect_financial_anomalies",
    "classify_financial_cycle_with_llm",
    "classify_cross_table_cycle_with_llm",
    "interpret_financial_quality_with_llm",
    # DB Models
    "FinancialQualityMetrics",
    "DoubleEntryCheck",
    "TrialBalanceCheck",
    "SignConventionViolation",
    "FiscalPeriodIntegrity",
    # Pydantic Models
    "FinancialQualityConfig",
    "SignConventionConfig",
    "DoubleEntryResult",
    "TrialBalanceResult",
    "SignConventionViolationModel",
    "FiscalPeriodIntegrityCheck",
    "FinancialQualityIssue",
    "FinancialQualityResult",
]
