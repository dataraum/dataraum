"""Financial cycle analysis module.

This module provides:
- Fiscal stability rules (deterministic, auditable)
- Anomaly detection for financial data
- LLM cycle classification
- LLM quality interpretation

Usage:
    from dataraum_context.domains.financial.cycles import (
        assess_fiscal_stability,
        detect_financial_anomalies,
        classify_financial_cycle_with_llm,
        classify_cross_table_cycle_with_llm,
        interpret_financial_quality_with_llm,
    )
"""

from dataraum_context.domains.financial.cycles.classifier import (
    classify_cross_table_cycle_with_llm,
    classify_financial_cycle_with_llm,
)
from dataraum_context.domains.financial.cycles.detector import detect_financial_anomalies
from dataraum_context.domains.financial.cycles.interpreter import (
    interpret_financial_quality_with_llm,
)
from dataraum_context.domains.financial.cycles.rules import assess_fiscal_stability

__all__ = [
    "assess_fiscal_stability",
    "detect_financial_anomalies",
    "classify_financial_cycle_with_llm",
    "classify_cross_table_cycle_with_llm",
    "interpret_financial_quality_with_llm",
]
