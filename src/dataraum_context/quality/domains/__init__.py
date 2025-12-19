"""Domain-specific quality orchestration.

This module re-exports from domains/financial/ for backward compatibility.

For new code, import directly from dataraum_context.domains.financial:
    from dataraum_context.domains.financial import (
        analyze_complete_financial_quality,
        analyze_complete_financial_dataset_quality,
        assess_fiscal_stability,
        detect_financial_anomalies,
        classify_financial_cycle_with_llm,
        interpret_financial_quality_with_llm,
    )

For domain analyzers and registry, import from dataraum_context.domains:
    from dataraum_context.domains import get_analyzer, list_domains
    from dataraum_context.domains.financial import analyze_financial_quality
"""

# Re-export from new canonical locations for backward compatibility
from dataraum_context.domains.financial.cycles import (  # noqa: F401
    assess_fiscal_stability,
    classify_cross_table_cycle_with_llm,
    classify_financial_cycle_with_llm,
    detect_financial_anomalies,
    interpret_financial_quality_with_llm,
)
from dataraum_context.domains.financial.orchestrator import (  # noqa: F401
    analyze_complete_financial_dataset_quality,
    analyze_complete_financial_quality,
)

__all__ = [
    # Financial orchestrator
    "analyze_complete_financial_quality",
    "analyze_complete_financial_dataset_quality",
    # Cycle rules and detection
    "assess_fiscal_stability",
    "detect_financial_anomalies",
    # LLM classification
    "classify_financial_cycle_with_llm",
    "classify_cross_table_cycle_with_llm",
    "interpret_financial_quality_with_llm",
]
