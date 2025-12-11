"""Tests for filtering rules YAML loader (Phase 1)."""

from pathlib import Path

import pytest

from dataraum_context.quality.filtering import (
    FilterAction,
    FilteringRule,
    FilteringRulesLoadError,
    RulePriority,
    load_filtering_rules,
)


def test_load_basic_filtering_rules(tmp_path: Path):
    """Test loading basic filtering rules configuration."""
    config_file = tmp_path / "rules.yaml"
    config_file.write_text("""
name: test
version: "1.0.0"
description: Test filtering rules

filtering_rules:
  - name: "test_rule"
    priority: "override"
    filter: "price > 0"
    description: "Prices must be positive"
""")

    config = load_filtering_rules(config_file)

    assert config.name == "test"
    assert config.version == "1.0.0"
    assert config.description == "Test filtering rules"
    assert len(config.filtering_rules) == 1

    rule = config.filtering_rules[0]
    assert rule.name == "test_rule"
    assert rule.priority == RulePriority.OVERRIDE
    assert rule.filter == "price > 0"
    assert rule.action == FilterAction.INCLUDE_IN_CLEAN
    assert rule.description == "Prices must be positive"


def test_load_rules_with_applies_to(tmp_path: Path):
    """Test loading rules with applies_to criteria."""
    config_file = tmp_path / "rules.yaml"
    config_file.write_text("""
filtering_rules:
  - name: "email_validation"
    priority: "override"
    filter: "email ~ '^[^@]+@[^@]+$'"
    applies_to:
      pattern: '.*_email$'
      role: "contact_info"
    description: "Email format validation"
""")

    config = load_filtering_rules(config_file)
    rule = config.filtering_rules[0]

    assert rule.applies_to is not None
    assert rule.applies_to.pattern == ".*_email$"
    assert rule.applies_to.role == "contact_info"


def test_load_rules_with_template_variables(tmp_path: Path):
    """Test loading rules with template variables."""
    config_file = tmp_path / "rules.yaml"
    config_file.write_text("""
filtering_rules:
  - name: "date_order"
    priority: "extend"
    filter: "{start_column} <= {end_column}"
    template_variables:
      start_column: "start_date"
      end_column: "end_date"
""")

    config = load_filtering_rules(config_file)
    rule = config.filtering_rules[0]

    assert rule.template_variables is not None
    assert rule.template_variables["start_column"] == "start_date"
    assert rule.template_variables["end_column"] == "end_date"


def test_load_rules_with_multiple_priorities(tmp_path: Path):
    """Test loading rules with different priorities."""
    config_file = tmp_path / "rules.yaml"
    config_file.write_text("""
filtering_rules:
  - name: "override_rule"
    priority: "override"
    filter: "a > 0"

  - name: "extend_rule"
    priority: "extend"
    filter: "b > 0"

  - name: "suggest_rule"
    priority: "suggest"
    filter: "c > 0"
""")

    config = load_filtering_rules(config_file)

    assert len(config.filtering_rules) == 3
    assert config.filtering_rules[0].priority == RulePriority.OVERRIDE
    assert config.filtering_rules[1].priority == RulePriority.EXTEND
    assert config.filtering_rules[2].priority == RulePriority.SUGGEST


def test_load_rules_with_actions(tmp_path: Path):
    """Test loading rules with different actions."""
    config_file = tmp_path / "rules.yaml"
    config_file.write_text("""
filtering_rules:
  - name: "include_rule"
    priority: "override"
    action: "include_in_clean"

  - name: "quarantine_rule"
    priority: "override"
    action: "quarantine"

  - name: "exclude_quarantine_rule"
    priority: "override"
    action: "exclude_from_quarantine"
    applies_to:
      role: "primary_key"
""")

    config = load_filtering_rules(config_file)

    assert config.filtering_rules[0].action == FilterAction.INCLUDE_IN_CLEAN
    assert config.filtering_rules[1].action == FilterAction.QUARANTINE
    assert config.filtering_rules[2].action == FilterAction.EXCLUDE_FROM_QUARANTINE


def test_load_empty_config(tmp_path: Path):
    """Test loading empty configuration file."""
    config_file = tmp_path / "empty.yaml"
    config_file.write_text("")

    config = load_filtering_rules(config_file)

    assert config.name == "default"
    assert len(config.filtering_rules) == 0


def test_load_nonexistent_file():
    """Test loading nonexistent file raises error."""
    with pytest.raises(FilteringRulesLoadError, match="not found"):
        load_filtering_rules("/nonexistent/path.yaml")


def test_load_invalid_yaml(tmp_path: Path):
    """Test loading invalid YAML raises error."""
    config_file = tmp_path / "invalid.yaml"
    config_file.write_text("""
filtering_rules:
  - name: "broken
    priority: "override
""")

    with pytest.raises(FilteringRulesLoadError, match="Invalid YAML"):
        load_filtering_rules(config_file)


def test_load_invalid_priority(tmp_path: Path):
    """Test loading rule with invalid priority value."""
    config_file = tmp_path / "invalid_priority.yaml"
    config_file.write_text("""
filtering_rules:
  - name: "test"
    priority: "invalid_priority"
    filter: "x > 0"
""")

    # Should skip invalid rule and continue
    config = load_filtering_rules(config_file)
    assert len(config.filtering_rules) == 0


def test_get_rules_for_column():
    """Test filtering rules by column criteria."""
    rule1 = FilteringRule(
        name="email_rule",
        priority=RulePriority.OVERRIDE,
        filter="email ~ '^[^@]+@[^@]+$'",
    )
    rule1.applies_to = type(
        "AppliesTo",
        (),
        {
            "pattern": ".*_email$",
            "columns": None,
            "role": None,
            "type": None,
            "table_pattern": None,
        },
    )()

    rule2 = FilteringRule(
        name="pk_rule",
        priority=RulePriority.OVERRIDE,
        action=FilterAction.EXCLUDE_FROM_QUARANTINE,
    )
    rule2.applies_to = type(
        "AppliesTo",
        (),
        {
            "pattern": None,
            "columns": None,
            "role": "primary_key",
            "type": None,
            "table_pattern": None,
        },
    )()

    # Test pattern matching
    assert rule1.matches_column("user_email")
    assert not rule1.matches_column("user_name")

    # Test role matching
    assert rule2.matches_column("id", semantic_role="primary_key")
    assert not rule2.matches_column("id", semantic_role="foreign_key")


def test_get_rules_for_column_sorted_by_priority():
    """Test that rules are returned sorted by priority."""
    from dataraum_context.quality.filtering.models import FilteringRulesConfig

    rule1 = FilteringRule(
        name="suggest_rule",
        priority=RulePriority.SUGGEST,
        filter="a > 0",
    )
    rule2 = FilteringRule(
        name="override_rule",
        priority=RulePriority.OVERRIDE,
        filter="b > 0",
    )
    rule3 = FilteringRule(
        name="extend_rule",
        priority=RulePriority.EXTEND,
        filter="c > 0",
    )

    config = FilteringRulesConfig(filtering_rules=[rule1, rule2, rule3])

    rules = config.get_rules_for_column("test_column")

    # Should be sorted: override, extend, suggest
    assert len(rules) == 3
    assert rules[0].priority == RulePriority.OVERRIDE
    assert rules[1].priority == RulePriority.EXTEND
    assert rules[2].priority == RulePriority.SUGGEST


def test_rule_matches_explicit_columns():
    """Test rule matching with explicit column names."""
    from dataraum_context.quality.filtering.models import RuleAppliesTo

    rule = FilteringRule(
        name="test",
        priority=RulePriority.OVERRIDE,
        filter="x > 0",
        applies_to=RuleAppliesTo(columns=["price", "amount", "cost"]),
    )

    assert rule.matches_column("price")
    assert rule.matches_column("amount")
    assert rule.matches_column("cost")
    assert not rule.matches_column("quantity")


def test_rule_matches_type():
    """Test rule matching by DuckDB type."""
    from dataraum_context.quality.filtering.models import RuleAppliesTo

    rule = FilteringRule(
        name="test",
        priority=RulePriority.OVERRIDE,
        filter="x IS NOT NULL",
        applies_to=RuleAppliesTo(type="VARCHAR"),
    )

    assert rule.matches_column("name", column_type="VARCHAR")
    assert not rule.matches_column("count", column_type="INTEGER")


def test_global_rule_matches_all_columns():
    """Test that rules without applies_to match all columns."""
    rule = FilteringRule(
        name="global_rule",
        priority=RulePriority.EXTEND,
        filter="x IS NOT NULL",
        # No applies_to
    )

    assert rule.matches_column("any_column")
    assert rule.matches_column("another_column")
