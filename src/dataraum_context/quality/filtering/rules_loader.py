"""YAML loader for filtering rules configuration.

This module loads user-defined filtering rules from YAML configuration files.
Rules are NOT evaluated here - they're just loaded and validated.
Evaluation happens in Phase 9 (rules merger) and Phase 10 (filtering executor).
"""

import logging
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from dataraum_context.quality.filtering.models import (
    FilterAction,
    FilteringRule,
    FilteringRulesConfig,
    RuleAppliesTo,
    RulePriority,
)

logger = logging.getLogger(__name__)


class FilteringRulesLoadError(Exception):
    """Error loading filtering rules configuration."""

    pass


def load_filtering_rules(config_path: Path | str) -> FilteringRulesConfig:
    """Load filtering rules from YAML file.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        FilteringRulesConfig with parsed and validated rules

    Raises:
        FilteringRulesLoadError: If file not found, invalid YAML, or validation fails
    """
    config_path = Path(config_path)

    if not config_path.exists():
        raise FilteringRulesLoadError(f"Configuration file not found: {config_path}")

    try:
        with open(config_path, encoding="utf-8") as f:
            raw_config = yaml.safe_load(f)

        if not raw_config:
            logger.warning(f"Empty configuration file: {config_path}")
            return FilteringRulesConfig()

        # Parse and validate
        config = _parse_filtering_config(raw_config)

        logger.info(f"Loaded {len(config.filtering_rules)} filtering rules from {config_path}")
        return config

    except yaml.YAMLError as e:
        raise FilteringRulesLoadError(f"Invalid YAML in {config_path}: {e}") from e
    except ValidationError as e:
        raise FilteringRulesLoadError(f"Validation error in {config_path}: {e}") from e
    except Exception as e:
        raise FilteringRulesLoadError(f"Unexpected error loading {config_path}: {e}") from e


def _parse_filtering_config(raw_config: dict[str, Any]) -> FilteringRulesConfig:
    """Parse raw YAML dict into FilteringRulesConfig.

    Args:
        raw_config: Raw dictionary from YAML

    Returns:
        Validated FilteringRulesConfig
    """
    # Extract metadata
    name = raw_config.get("name", "default")
    version = raw_config.get("version", "1.0.0")
    description = raw_config.get("description")

    # Parse filtering rules
    filtering_rules = []
    raw_rules = raw_config.get("filtering_rules", [])

    for raw_rule in raw_rules:
        try:
            rule = _parse_filtering_rule(raw_rule)
            filtering_rules.append(rule)
        except Exception as e:
            logger.warning(f"Skipping invalid rule {raw_rule.get('name')}: {e}")

    return FilteringRulesConfig(
        name=name,
        version=version,
        description=description,
        filtering_rules=filtering_rules,
    )


def _parse_filtering_rule(raw_rule: dict[str, Any]) -> FilteringRule:
    """Parse a single filtering rule.

    Args:
        raw_rule: Raw rule dictionary from YAML

    Returns:
        Validated FilteringRule
    """
    # Required fields
    name = raw_rule["name"]
    priority = RulePriority(raw_rule["priority"])

    # Optional fields
    filter_expr = raw_rule.get("filter")
    action = FilterAction(raw_rule.get("action", "include_in_clean"))
    description = raw_rule.get("description")
    template_variables = raw_rule.get("template_variables")

    # Parse applies_to criteria
    applies_to = None
    if "applies_to" in raw_rule:
        raw_applies = raw_rule["applies_to"]
        applies_to = RuleAppliesTo(
            role=raw_applies.get("role"),
            type=raw_applies.get("type"),
            pattern=raw_applies.get("pattern"),
            columns=raw_applies.get("columns"),
            table_pattern=raw_applies.get("table_pattern"),
        )

    return FilteringRule(
        name=name,
        priority=priority,
        filter=filter_expr,
        action=action,
        applies_to=applies_to,
        description=description,
        template_variables=template_variables,
    )


def load_default_filtering_rules() -> FilteringRulesConfig:
    """Load default filtering rules configuration.

    Returns:
        Default FilteringRulesConfig

    Raises:
        FilteringRulesLoadError: If default config not found or invalid
    """
    # Try common config locations
    config_locations = [
        Path("config/filtering/default.yaml"),
        Path("config/filtering/filtering_rules.yaml"),
        Path.cwd() / "config/filtering/default.yaml",
    ]

    for config_path in config_locations:
        if config_path.exists():
            return load_filtering_rules(config_path)

    # Return empty config if no default found
    logger.warning("No default filtering rules configuration found, using empty config")
    return FilteringRulesConfig()
