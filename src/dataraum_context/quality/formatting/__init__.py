"""Quality formatting utilities.

Provides reusable utilities for formatting quality metrics and issues
into structured, interpretable context for LLM consumption.

Note: Calculation graphs and schema mapping have moved to
      dataraum_context.calculations
"""

from dataraum_context.quality.formatting.base import (
    THRESHOLDS,
    CommonThresholds,
    SeverityLevel,
    ThresholdConfig,
    format_list_with_overflow,
    generate_interpretation,
    generate_recommendation,
    map_to_severity,
    severity_emoji,
)
from dataraum_context.quality.formatting.business_cycles import (
    BusinessCycleContext,
    BusinessCyclesOutput,
    format_business_cycles_as_context_string,
    format_business_cycles_for_llm,
    format_single_cycle,
)
from dataraum_context.quality.formatting.config import (
    FormatterConfig,
    MetricGroupConfig,
    get_default_config,
    load_formatter_config,
)
from dataraum_context.quality.formatting.domain import (
    format_compliance_group,
    format_domain_quality,
    format_financial_balance_group,
    format_fiscal_period_group,
    format_sign_convention_group,
)
from dataraum_context.quality.formatting.multicollinearity import (
    format_cross_table_multicollinearity_for_llm,
)
from dataraum_context.quality.formatting.statistical import (
    format_benford_group,
    format_completeness_group,
    format_outliers_group,
    format_statistical_quality,
)
from dataraum_context.quality.formatting.temporal import (
    format_freshness_group,
    format_patterns_group,
    format_stability_group,
    format_temporal_completeness_group,
    format_temporal_quality,
)
from dataraum_context.quality.formatting.topological import (
    format_complexity_group,
    format_cycles_group,
    format_structure_group,
    format_topological_quality,
    format_topological_stability_group,
)

__all__ = [
    # Base utilities
    "CommonThresholds",
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
    # Multicollinearity formatters (cross-table only)
    "format_cross_table_multicollinearity_for_llm",
    # Statistical formatters
    "format_completeness_group",
    "format_outliers_group",
    "format_benford_group",
    "format_statistical_quality",
    # Temporal formatters
    "format_freshness_group",
    "format_temporal_completeness_group",
    "format_patterns_group",
    "format_stability_group",
    "format_temporal_quality",
    # Topological formatters
    "format_structure_group",
    "format_cycles_group",
    "format_complexity_group",
    "format_topological_stability_group",
    "format_topological_quality",
    # Domain formatters
    "format_compliance_group",
    "format_financial_balance_group",
    "format_sign_convention_group",
    "format_fiscal_period_group",
    "format_domain_quality",
    # Business cycles formatters
    "BusinessCycleContext",
    "BusinessCyclesOutput",
    "format_business_cycles_for_llm",
    "format_business_cycles_as_context_string",
    "format_single_cycle",
]
