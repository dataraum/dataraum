"""Core formatting utilities.

Provides reusable patterns for transforming raw metrics into
structured, interpretable context for LLM consumption.

These utilities are used by:
- quality/formatting/ - Quality metric formatting
- entropy/ - Entropy score formatting (planned)
"""

from dataraum_context.core.formatting.base import (
    THRESHOLDS,
    CommonThresholds,
    InterpretationTemplate,
    RecommendationConfig,
    SeverityLevel,
    ThresholdConfig,
    format_list_with_overflow,
    generate_interpretation,
    generate_recommendation,
    map_to_severity,
    severity_emoji,
)
from dataraum_context.core.formatting.config import (
    FormatterConfig,
    MetricGroupConfig,
    get_default_config,
    load_formatter_config,
)

__all__ = [
    # Base utilities
    "CommonThresholds",
    "InterpretationTemplate",
    "RecommendationConfig",
    "SeverityLevel",
    "ThresholdConfig",
    "THRESHOLDS",
    "format_list_with_overflow",
    "generate_interpretation",
    "generate_recommendation",
    "map_to_severity",
    "severity_emoji",
    # Configuration
    "FormatterConfig",
    "MetricGroupConfig",
    "get_default_config",
    "load_formatter_config",
]
