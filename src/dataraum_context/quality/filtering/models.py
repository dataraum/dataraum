"""Pydantic models for filtering rules configuration.

This module defines models for user-defined filtering rules that override/extend
LLM-generated filtering recommendations. These rules are NOT evaluated as part
of quality assessment - they're used by the filtering executor (System 1).
"""

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class RulePriority(str, Enum):
    """Priority level for user rules vs LLM recommendations."""

    OVERRIDE = "override"  # User rule replaces LLM recommendation
    EXTEND = "extend"  # User rule added to LLM recommendations
    SUGGEST = "suggest"  # User rule only if no LLM recommendation


class FilterAction(str, Enum):
    """Action to take when rule matches."""

    INCLUDE_IN_CLEAN = "include_in_clean"  # Row passes to clean view
    QUARANTINE = "quarantine"  # Row goes to quarantine table
    EXCLUDE_FROM_QUARANTINE = "exclude_from_quarantine"  # Never quarantine


class RuleAppliesTo(BaseModel):
    """Criteria for when a rule applies."""

    role: str | None = Field(None, description="Semantic role (e.g., 'primary_key', 'foreign_key')")
    type: str | None = Field(None, description="DuckDB type (e.g., 'VARCHAR', 'DOUBLE')")
    pattern: str | None = Field(None, description="Column name regex pattern")
    columns: list[str] | None = Field(None, description="Explicit column names")
    table_pattern: str | None = Field(None, description="Table name regex pattern")


class FilteringRule(BaseModel):
    """A single filtering rule."""

    name: str = Field(..., description="Unique rule identifier")
    priority: RulePriority = Field(..., description="Priority vs LLM recommendations")
    filter: str | None = Field(
        None,
        description="SQL WHERE clause expression (e.g., 'price > 0')",
    )
    action: FilterAction = Field(
        default=FilterAction.INCLUDE_IN_CLEAN,
        description="Action when rule matches",
    )
    applies_to: RuleAppliesTo | None = Field(None, description="When this rule applies")
    description: str | None = Field(None, description="Human-readable description")
    template_variables: dict[str, Any] | None = Field(
        None,
        description="Variables for template substitution (e.g., {column}, {start_column})",
    )

    def matches_column(
        self,
        column_name: str,
        column_type: str | None = None,
        semantic_role: str | None = None,
    ) -> bool:
        """Check if rule applies to given column.

        Args:
            column_name: Column name to check
            column_type: DuckDB type (optional)
            semantic_role: Semantic role from enrichment (optional)

        Returns:
            True if rule applies to this column
        """
        if self.applies_to is None:
            return True  # Global rule

        # Check explicit column names
        if self.applies_to.columns and column_name in self.applies_to.columns:
            return True

        # Check column pattern
        if self.applies_to.pattern:
            import re

            if re.match(self.applies_to.pattern, column_name):
                return True

        # Check semantic role
        if self.applies_to.role and semantic_role == self.applies_to.role:
            return True

        # Check type
        if self.applies_to.type and column_type == self.applies_to.type:
            return True

        return False


class FilteringRulesConfig(BaseModel):
    """Complete filtering rules configuration."""

    name: str = Field(default="default", description="Configuration name")
    version: str = Field(default="1.0.0", description="Configuration version")
    description: str | None = Field(None, description="Configuration description")
    filtering_rules: list[FilteringRule] = Field(
        default_factory=list, description="User-defined filtering rules"
    )

    def get_rules_for_column(
        self,
        column_name: str,
        column_type: str | None = None,
        semantic_role: str | None = None,
    ) -> list[FilteringRule]:
        """Get all rules that apply to a specific column.

        Args:
            column_name: Column name
            column_type: DuckDB type (optional)
            semantic_role: Semantic role (optional)

        Returns:
            List of applicable rules, sorted by priority (override first)
        """
        applicable = [
            rule
            for rule in self.filtering_rules
            if rule.matches_column(column_name, column_type, semantic_role)
        ]

        # Sort by priority: override > extend > suggest
        priority_order = {
            RulePriority.OVERRIDE: 0,
            RulePriority.EXTEND: 1,
            RulePriority.SUGGEST: 2,
        }
        return sorted(applicable, key=lambda r: priority_order[r.priority])


class FilterType(str, Enum):
    """Type of filter - scope vs quality."""

    SCOPE = "scope"  # Row selection for calculations (e.g., type = 'sale')
    QUALITY = "quality"  # Data cleaning (e.g., amount IS NOT NULL)


class FilterDefinition(BaseModel):
    """A single filter with metadata."""

    column: str = Field(..., description="Column this filter applies to")
    condition: str = Field(..., description="SQL WHERE clause fragment")
    filter_type: FilterType = Field(..., description="Scope or quality filter")
    reason: str = Field(..., description="Why this filter was generated")
    rows_affected_pct: float | None = Field(
        None, description="Estimated percentage of rows affected"
    )
    auto_approve: bool = Field(default=True, description="Whether this filter can be auto-approved")
    review_note: str | None = Field(None, description="Note for human reviewer")


class QualityFlag(BaseModel):
    """Issue that can't be filtered, only flagged for awareness."""

    issue_type: str = Field(..., description="Type of issue (e.g., 'benford_violation')")
    column: str = Field(..., description="Affected column")
    description: str = Field(..., description="Human-readable description")
    severity: str = Field(default="moderate", description="none, low, moderate, high, severe")
    recommendation: str | None = Field(None, description="Suggested action")


class CalculationImpact(BaseModel):
    """Impact of quality issues on downstream calculations."""

    calculation_id: str = Field(..., description="Affected calculation (e.g., 'dso')")
    abstract_field: str = Field(..., description="Affected abstract field (e.g., 'revenue')")
    impact_severity: str = Field(..., description="critical, high, moderate, low")
    explanation: str = Field(..., description="Why this matters for the calculation")


class FilteringRecommendations(BaseModel):
    """LLM-generated filtering recommendations.

    Extended to support:
    - Scope filters (row selection for calculations)
    - Quality filters (data cleaning)
    - Flags (issues that can't be filtered)
    - Calculation impact context
    """

    # New structured filters
    scope_filters: list[FilterDefinition] = Field(
        default_factory=list,
        description="Filters for calculation scope (e.g., type = 'sale' for revenue)",
    )
    quality_filters: list[FilterDefinition] = Field(
        default_factory=list,
        description="Filters for data quality (e.g., amount IS NOT NULL)",
    )
    flags: list[QualityFlag] = Field(
        default_factory=list,
        description="Issues that can't be filtered, only flagged",
    )

    # Calculation context
    calculation_impacts: list[CalculationImpact] = Field(
        default_factory=list,
        description="How quality issues affect downstream calculations",
    )

    # Backward-compatible fields (populated from scope_filters + quality_filters)
    clean_view_filters: list[str] = Field(
        default_factory=list,
        description="SQL WHERE clauses for clean view (all filters combined)",
    )
    quarantine_criteria: list[str] = Field(
        default_factory=list,
        description="SQL criteria for quarantine (inverse of clean)",
    )
    column_exclusions: list[str] = Field(
        default_factory=list,
        description="Columns to exclude from default SELECT",
    )
    rationale: dict[str, str] = Field(
        default_factory=dict,
        description="Explanation for each recommendation",
    )

    # Metadata
    source: str = Field(default="llm", description="Source: llm, user_rule, or merged")
    confidence: float = Field(default=0.0, description="LLM confidence (0.0 to 1.0)")
    requires_acknowledgment: bool = Field(
        default=False, description="Whether human must acknowledge before use"
    )

    # Business context
    business_cycles: list[str] = Field(
        default_factory=list,
        description="Business cycles this table participates in",
    )

    def get_all_filter_conditions(self) -> list[str]:
        """Get all filter conditions as SQL WHERE clauses."""
        conditions = []
        for f in self.scope_filters:
            conditions.append(f.condition)
        for f in self.quality_filters:
            conditions.append(f.condition)
        # Include legacy filters
        conditions.extend(self.clean_view_filters)
        return list(set(conditions))  # Deduplicate

    def get_filters_by_column(self, column: str) -> list[FilterDefinition]:
        """Get all filters that apply to a specific column."""
        return [f for f in self.scope_filters + self.quality_filters if f.column == column]

    def has_critical_impacts(self) -> bool:
        """Check if any calculation impacts are critical."""
        return any(i.impact_severity == "critical" for i in self.calculation_impacts)


class FilteringResult(BaseModel):
    """Result of filtering execution (output of Phase 10)."""

    table_id: str
    clean_table_name: str
    quarantine_table_name: str
    rows_in_clean: int
    rows_in_quarantine: int
    applied_filters: list[str] = Field(
        default_factory=list, description="Filters applied to clean view"
    )
    quarantine_reasons: dict[str, int] = Field(
        default_factory=dict,
        description="Count of rows per quarantine reason",
    )
