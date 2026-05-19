"""Column eligibility analysis module.

Evaluates columns against configurable quality thresholds and
marks ineligible columns for removal from typed tables.
"""

from dataraum.analysis.eligibility.config import (
    EligibilityConfig,
    EligibilityRule,
    load_eligibility_config,
)
from dataraum.analysis.eligibility.db_models import ColumnEligibilityRecord
from dataraum.analysis.eligibility.evaluator import (
    evaluate_condition,
    evaluate_rules,
    extract_metrics,
    format_reason,
    is_likely_key,
    quarantine_and_drop_columns,
)

__all__ = [
    "ColumnEligibilityRecord",
    "EligibilityConfig",
    "EligibilityRule",
    "evaluate_condition",
    "evaluate_rules",
    "extract_metrics",
    "format_reason",
    "is_likely_key",
    "load_eligibility_config",
    "quarantine_and_drop_columns",
]
