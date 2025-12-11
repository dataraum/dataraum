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


class FilteringRecommendations(BaseModel):
    """LLM-generated filtering recommendations (output of Phase 8)."""

    clean_view_filters: list[str] = Field(
        default_factory=list,
        description="SQL WHERE clauses for clean view",
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
    source: str = Field(default="llm", description="Source of recommendations (llm or user_rule)")


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
