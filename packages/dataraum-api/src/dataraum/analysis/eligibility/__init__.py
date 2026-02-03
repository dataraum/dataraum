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

__all__ = [
    "ColumnEligibilityRecord",
    "EligibilityConfig",
    "EligibilityRule",
    "load_eligibility_config",
]
