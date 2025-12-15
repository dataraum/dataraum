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

from dataraum_context.quality.filtering.executor import execute_filtering
from dataraum_context.quality.filtering.llm_filter_agent import analyze_quality_for_filtering
from dataraum_context.quality.filtering.models import (
    CalculationImpact,
    FilterAction,
    FilterDefinition,
    FilteringRecommendations,
    FilteringResult,
    FilteringRule,
    FilteringRulesConfig,
    FilterType,
    QualityFlag,
    RuleAppliesTo,
    RulePriority,
)
from dataraum_context.quality.filtering.rules_loader import (
    FilteringRulesLoadError,
    load_default_filtering_rules,
    load_filtering_rules,
)
from dataraum_context.quality.filtering.rules_merger import merge_filtering_rules

__all__ = [
    # Core models
    "FilteringRule",
    "FilteringRulesConfig",
    "FilteringRecommendations",
    "FilteringResult",
    "RuleAppliesTo",
    "RulePriority",
    "FilterAction",
    # Extended filter models
    "FilterType",
    "FilterDefinition",
    "QualityFlag",
    "CalculationImpact",
    # Phase 1: Loader
    "load_filtering_rules",
    "load_default_filtering_rules",
    "FilteringRulesLoadError",
    # Phase 8: LLM Agent
    "analyze_quality_for_filtering",
    # Phase 9: Rules Merger
    "merge_filtering_rules",
    # Phase 10: Executor
    "execute_filtering",
]
