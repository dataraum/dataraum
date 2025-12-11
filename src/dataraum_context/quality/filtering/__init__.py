"""Data filtering module - System 1 (separate from quality assessment).

This module implements intelligent data filtering based on:
1. LLM analysis of quality metrics (Phase 8)
2. User-defined rules with priority system (Phase 9)
3. Execution of merged filtering logic (Phase 10)

Key Design:
- Filtering is SEPARATE from quality measurement
- Quality metrics (System 2) inform filtering decisions
- LLM acts as bridge between measurement and filtering
- Users have control via priority-based rules
"""

from dataraum_context.quality.filtering.models import (
    FilterAction,
    FilteringRecommendations,
    FilteringResult,
    FilteringRule,
    FilteringRulesConfig,
    RuleAppliesTo,
    RulePriority,
)
from dataraum_context.quality.filtering.rules_loader import (
    FilteringRulesLoadError,
    load_default_filtering_rules,
    load_filtering_rules,
)

__all__ = [
    # Models
    "FilteringRule",
    "FilteringRulesConfig",
    "FilteringRecommendations",
    "FilteringResult",
    "RuleAppliesTo",
    "RulePriority",
    "FilterAction",
    # Loader
    "load_filtering_rules",
    "load_default_filtering_rules",
    "FilteringRulesLoadError",
]
