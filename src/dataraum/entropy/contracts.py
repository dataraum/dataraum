"""Entropy contracts: Data readiness thresholds by use case.

Contracts define acceptable entropy levels for different use cases.
Each contract specifies dimension thresholds, blocking conditions,
and provides confidence level calculation (traffic light model).

Usage:
    from dataraum.entropy.contracts import (
        evaluate_contract,
        list_contracts,
        ConfidenceLevel,
    )

    # Evaluate a contract
    evaluation = evaluate_contract(entropy_data, "executive_dashboard")

    # Get traffic light confidence
    level = evaluation.confidence_level  # GREEN, YELLOW, ORANGE, RED

See docs/ENTROPY_CONTRACTS.md for detailed specification.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from dataraum.core.config import get_config_file
from dataraum.core.logging import get_logger
from dataraum.entropy.config import get_dimension_label
from dataraum.entropy.models import ResolutionOption

if TYPE_CHECKING:
    from dataraum.entropy.analysis.aggregator import ColumnSummary

logger = get_logger(__name__)

ENTROPY_CONTRACTS_CONFIG = "entropy/contracts.yaml"


class ConfidenceLevel(str, Enum):
    """Traffic light confidence levels for query responses."""

    GREEN = "green"  # All thresholds pass
    YELLOW = "yellow"  # Minor issues (within 20% of threshold)
    ORANGE = "orange"  # Significant issues (some dimensions exceed)
    RED = "red"  # Critical issues (blocking violations)

    @property
    def emoji(self) -> str:
        """Return emoji for this confidence level."""
        return {
            ConfidenceLevel.GREEN: "🟢",
            ConfidenceLevel.YELLOW: "🟡",
            ConfidenceLevel.ORANGE: "🟠",
            ConfidenceLevel.RED: "🔴",
        }[self]

    @property
    def label(self) -> str:
        """Return human-readable label."""
        return {
            ConfidenceLevel.GREEN: "GOOD",
            ConfidenceLevel.YELLOW: "MARGINAL",
            ConfidenceLevel.ORANGE: "ISSUES",
            ConfidenceLevel.RED: "HIGH RISK",
        }[self]


@dataclass
class BlockingCondition:
    """A condition that blocks contract compliance."""

    condition_type: str  # e.g., "any_dimension_exceeds", "has_blocked_columns"
    parameters: dict[str, Any] = field(default_factory=dict)
    description: str = ""

    def evaluate(
        self,
        column_summaries: dict[str, ColumnSummary],
        evaluation: ContractEvaluation,
    ) -> bool:
        """Check if this blocking condition is triggered.

        Returns True if condition is triggered (blocking).
        """
        if self.condition_type == "any_dimension_exceeds":
            threshold = self.parameters.get("threshold", 0.5)
            for _dim, score in evaluation.dimension_scores.items():
                if score > threshold:
                    return True
            return False

        elif self.condition_type == "critical_entropy_count_exceeds":
            threshold = int(self.parameters.get("threshold", 0))
            # Count columns with blocked readiness (network-derived)
            critical_count = sum(1 for s in column_summaries.values() if s.readiness == "blocked")
            return critical_count > threshold

        # Unknown condition type - don't block
        logger.warning(f"Unknown blocking condition type: {self.condition_type}")
        return False


@dataclass
class Violation:
    """A violation of contract threshold."""

    violation_type: str  # "dimension", "overall", "blocking_condition"
    severity: str  # "warning", "blocking"

    # For dimension violations
    dimension: str | None = None
    max_allowed: float | None = None
    actual: float | None = None

    # For blocking condition violations
    condition: str | None = None

    # Details
    details: str = ""
    affected_columns: list[str] = field(default_factory=list)


@dataclass
class ContractProfile:
    """Definition of a data readiness contract."""

    name: str  # e.g., "regulatory_reporting"
    display_name: str  # e.g., "Regulatory Reporting"
    description: str

    # Thresholds
    overall_threshold: float
    dimension_thresholds: dict[str, float] = field(default_factory=dict)

    # Blocking conditions
    blocking_conditions: list[BlockingCondition] = field(default_factory=list)

    # Warning threshold (for YELLOW status)
    warning_margin: float = 0.2  # Within 20% of threshold = warning


@dataclass
class ContractEvaluation:
    """Result of evaluating entropy against a contract."""

    contract_name: str
    contract_display_name: str
    is_compliant: bool

    # Confidence level (traffic light)
    confidence_level: ConfidenceLevel

    # Scores
    overall_score: float
    dimension_scores: dict[str, float] = field(default_factory=dict)

    # Violations (blocking and non-blocking)
    violations: list[Violation] = field(default_factory=list)

    # Warnings (approaching threshold)
    warnings: list[Violation] = field(default_factory=list)

    # Recommendations to achieve compliance
    recommendations: list[ResolutionOption] = field(default_factory=list)

    # Metadata
    evaluated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    # Summary for UI
    compliance_percentage: float = 0.0  # % of dimensions within threshold
    worst_dimension: str | None = None
    worst_dimension_score: float = 0.0
    estimated_effort_to_comply: str = "unknown"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "contract_name": self.contract_name,
            "contract_display_name": self.contract_display_name,
            "is_compliant": self.is_compliant,
            "confidence_level": self.confidence_level.value,
            "confidence_emoji": self.confidence_level.emoji,
            "confidence_label": self.confidence_level.label,
            "overall_score": round(self.overall_score, 3),
            "dimension_scores": {k: round(v, 3) for k, v in self.dimension_scores.items()},
            "violations": [
                {
                    "type": v.violation_type,
                    "severity": v.severity,
                    "dimension": v.dimension,
                    "max_allowed": v.max_allowed,
                    "actual": round(v.actual, 3) if v.actual else None,
                    "details": v.details,
                    "affected_columns": v.affected_columns,
                }
                for v in self.violations
            ],
            "warnings": [
                {
                    "type": w.violation_type,
                    "dimension": w.dimension,
                    "max_allowed": w.max_allowed,
                    "actual": round(w.actual, 3) if w.actual else None,
                    "details": w.details,
                }
                for w in self.warnings
            ],
            "compliance_percentage": round(self.compliance_percentage * 100, 1),
            "worst_dimension": self.worst_dimension,
            "worst_dimension_score": round(self.worst_dimension_score, 3),
            "estimated_effort_to_comply": self.estimated_effort_to_comply,
            "evaluated_at": self.evaluated_at.isoformat(),
        }


# Module-level cache for contracts
_contracts_cache: dict[str, ContractProfile] | None = None
_contracts_path_cache: Path | None = None


def load_contracts(config_path: Path | None = None) -> dict[str, ContractProfile]:
    """Load contract definitions from YAML file.

    Args:
        config_path: Path to contracts.yaml. Defaults to config/entropy/contracts.yaml.

    Returns:
        Dict mapping contract name to ContractProfile.

    Raises:
        FileNotFoundError: If config file doesn't exist.
        ValueError: If config file is invalid.
    """
    if config_path is None:
        config_path = get_config_file(ENTROPY_CONTRACTS_CONFIG)

    if not config_path.exists():
        raise FileNotFoundError(f"Contracts config not found: {config_path}.")

    try:
        with open(config_path) as f:
            raw = yaml.safe_load(f) or {}

        contracts = _parse_contracts(raw)

        if not contracts:
            raise ValueError(
                f"No contracts defined in {config_path}. "
                f"Add at least one contract under 'contracts:' key."
            )

        return contracts

    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML in contracts config {config_path}: {e}") from e


def _parse_contracts(raw: dict[str, Any]) -> dict[str, ContractProfile]:
    """Parse raw YAML config into contract profiles."""
    contracts: dict[str, ContractProfile] = {}

    for name, definition in raw.get("contracts", {}).items():
        blocking_conditions: list[BlockingCondition] = []
        for bc in definition.get("blocking_conditions", []):
            blocking_conditions.append(
                BlockingCondition(
                    condition_type=bc.get("type", ""),
                    parameters={k: v for k, v in bc.items() if k != "type"},
                    description=bc.get("description", ""),
                )
            )

        contracts[name] = ContractProfile(
            name=name,
            display_name=definition.get("display_name", name.replace("_", " ").title()),
            description=definition.get("description", ""),
            overall_threshold=definition.get("overall_threshold", 0.5),
            dimension_thresholds=definition.get("dimension_thresholds", {}),
            blocking_conditions=blocking_conditions,
            warning_margin=definition.get("warning_margin", 0.2),
        )

    return contracts


def get_contracts(config_path: Path | None = None) -> dict[str, ContractProfile]:
    """Get contracts, using cache if available.

    Args:
        config_path: Optional path to override default config location.

    Returns:
        Dict mapping contract name to ContractProfile.
    """
    global _contracts_cache, _contracts_path_cache

    if config_path is not None:
        path = config_path
    elif _contracts_path_cache is not None:
        path = _contracts_path_cache
    else:
        path = get_config_file(ENTROPY_CONTRACTS_CONFIG)

    if _contracts_cache is not None and _contracts_path_cache == path:
        return _contracts_cache

    _contracts_cache = load_contracts(path)
    _contracts_path_cache = path
    return _contracts_cache


def list_contracts(config_path: Path | None = None) -> list[dict[str, Any]]:
    """List all available contracts.

    Returns:
        List of contract summaries for API/CLI.
    """
    contracts = get_contracts(config_path)
    return [
        {
            "name": c.name,
            "display_name": c.display_name,
            "description": c.description,
            "overall_threshold": c.overall_threshold,
        }
        for c in contracts.values()
    ]


def get_contract(name: str, config_path: Path | None = None) -> ContractProfile | None:
    """Get a specific contract by name.

    Args:
        name: Contract name (e.g., "executive_dashboard")
        config_path: Optional path to contracts config

    Returns:
        ContractProfile or None if not found.
    """
    contracts = get_contracts(config_path)
    return contracts.get(name)


def _get_dimension_score(
    column_summaries: dict[str, ColumnSummary],
    dimension: str,
) -> float:
    """Get the score for a specific dimension from column summaries.

    Aggregates scores across all columns for the dimension.
    Only includes columns that actually have data for this dimension.
    Supports prefix matching: "semantic.dimensional" matches
    "semantic.dimensional.overall_score", etc.

    Args:
        column_summaries: Dict mapping column key to summary
        dimension: Dimension path like "structural.types" or "semantic.dimensional"

    Returns:
        Average score for this dimension across columns that have it (0.0 if no data).
    """
    scores: list[float] = []

    for summary in column_summaries.values():
        # First check dimension_scores for exact match
        if dimension in summary.dimension_scores:
            scores.append(summary.dimension_scores[dimension])
        else:
            # Check for prefix match (e.g., "semantic.dimensional" matches
            # "semantic.dimensional.overall_score")
            matching_scores = [
                score
                for dim_path, score in summary.dimension_scores.items()
                if dim_path.startswith(dimension + ".")
            ]
            if matching_scores:
                # Use the average of all sub-dimensions
                scores.append(sum(matching_scores) / len(matching_scores))
            # No score for this dimension on this column — skip it
            # (detector didn't run, not a reason to use layer average)

    return sum(scores) / len(scores) if scores else 0.0


def _find_affected_columns(
    column_summaries: dict[str, ColumnSummary],
    dimension: str,
    threshold: float,
) -> list[str]:
    """Find columns that exceed threshold for a dimension.

    Only considers columns that actually have data for this dimension.
    Supports prefix matching: "semantic.dimensional" matches
    "semantic.dimensional.overall_score", etc.

    Args:
        column_summaries: Dict mapping column key to summary
        dimension: Dimension path (e.g., "structural.types" or "semantic.dimensional")
        threshold: Threshold score

    Returns:
        List of column names (table.column format) exceeding threshold.
    """
    affected: list[str] = []

    for key, summary in column_summaries.items():
        score: float | None = None

        if dimension in summary.dimension_scores:
            score = summary.dimension_scores[dimension]
        else:
            # Check for prefix match
            matching_scores = [
                s
                for dim_path, s in summary.dimension_scores.items()
                if dim_path.startswith(dimension + ".")
            ]
            if matching_scores:
                score = sum(matching_scores) / len(matching_scores)

        if score is not None and score > threshold:
            affected.append(key)

    return sorted(affected)


def evaluate_contract(
    column_summaries: dict[str, ColumnSummary],
    contract_name: str,
    config_path: Path | None = None,
) -> ContractEvaluation:
    """Evaluate entropy against a contract.

    Args:
        column_summaries: Dict mapping column key to ColumnSummary
        contract_name: Name of the contract to evaluate against
        config_path: Optional path to contracts config

    Returns:
        ContractEvaluation with compliance status, violations, and recommendations.

    Raises:
        ValueError: If contract not found.
    """
    contract = get_contract(contract_name, config_path)
    if contract is None:
        raise ValueError(f"Contract not found: {contract_name}")

    violations: list[Violation] = []
    warnings: list[Violation] = []
    dimension_scores: dict[str, float] = {}

    # Check each dimension threshold
    dimensions_checked = 0
    dimensions_passing = 0
    worst_dimension: str | None = None
    worst_excess = 0.0

    for dimension, max_score in contract.dimension_thresholds.items():
        actual_score = _get_dimension_score(column_summaries, dimension)
        dimension_scores[dimension] = actual_score
        dimensions_checked += 1

        if actual_score > max_score:
            # Violation
            excess = actual_score - max_score
            affected = _find_affected_columns(column_summaries, dimension, max_score)

            # Determine severity
            blocking_threshold = max_score * (1 + contract.warning_margin * 2)
            severity = "blocking" if actual_score > blocking_threshold else "warning"

            label = get_dimension_label(dimension)
            col_list = ", ".join(affected[:5])
            violation = Violation(
                violation_type="dimension",
                severity=severity,
                dimension=dimension,
                max_allowed=max_score,
                actual=actual_score,
                details=(
                    f"{label} ({actual_score:.2f}/{max_score:.2f}) — "
                    f"too uncertain for {contract.display_name}. "
                    f"Affected: {col_list}"
                ),
                affected_columns=affected,
            )

            if severity == "blocking":
                violations.append(violation)
            else:
                warnings.append(violation)

            # Track worst dimension
            if excess > worst_excess:
                worst_excess = excess
                worst_dimension = dimension
        elif actual_score > max_score * (1 - contract.warning_margin):
            # Approaching threshold - warning
            affected = _find_affected_columns(
                column_summaries, dimension, max_score * (1 - contract.warning_margin)
            )
            label = get_dimension_label(dimension)
            col_list = ", ".join(affected[:5])
            warnings.append(
                Violation(
                    violation_type="dimension",
                    severity="warning",
                    dimension=dimension,
                    max_allowed=max_score,
                    actual=actual_score,
                    details=(
                        f"{label} approaching limit ({actual_score:.2f}/{max_score:.2f}) "
                        f"for {contract.display_name}. Affected: {col_list}"
                    ),
                    affected_columns=affected,
                )
            )
            dimensions_passing += 1
        else:
            dimensions_passing += 1

    # Calculate overall score
    overall_score = (
        sum(dimension_scores.values()) / len(dimension_scores) if dimension_scores else 0.0
    )

    # Check overall threshold
    if overall_score > contract.overall_threshold:
        violations.append(
            Violation(
                violation_type="overall",
                severity="blocking",
                max_allowed=contract.overall_threshold,
                actual=overall_score,
                details=(
                    f"Overall uncertainty ({overall_score:.2f}) exceeds "
                    f"{contract.display_name} threshold ({contract.overall_threshold:.2f})"
                ),
            )
        )

    # Create preliminary evaluation for blocking condition checks
    evaluation = ContractEvaluation(
        contract_name=contract.name,
        contract_display_name=contract.display_name,
        is_compliant=False,  # Will be set below
        confidence_level=ConfidenceLevel.RED,  # Will be set below
        overall_score=overall_score,
        dimension_scores=dimension_scores,
        violations=violations,
        warnings=warnings,
    )

    # Check blocking conditions
    for condition in contract.blocking_conditions:
        if condition.evaluate(column_summaries, evaluation):
            violations.append(
                Violation(
                    violation_type="blocking_condition",
                    severity="blocking",
                    condition=condition.condition_type,
                    details=condition.description
                    or f"Data readiness blocked: {condition.condition_type.replace('_', ' ')}",
                )
            )

    # Check if any column has blocked readiness (from Bayesian network).
    # This ensures contract evaluation is consistent with overall readiness:
    # if readiness=BLOCKED, no contract should pass.
    blocked_columns = sorted(key for key, s in column_summaries.items() if s.readiness == "blocked")
    if blocked_columns:
        col_list = ", ".join(blocked_columns[:5])
        extra = f" (+{len(blocked_columns) - 5} more)" if len(blocked_columns) > 5 else ""
        violations.append(
            Violation(
                violation_type="blocking_condition",
                severity="blocking",
                condition="blocked_columns",
                details=(
                    f"{len(blocked_columns)} column(s) have blocked readiness: {col_list}{extra}"
                ),
                affected_columns=blocked_columns,
            )
        )

    # Determine compliance
    blocking_violations = [v for v in violations if v.severity == "blocking"]
    is_compliant = len(blocking_violations) == 0

    # Calculate confidence level
    confidence_level = _calculate_confidence_level(
        is_compliant=is_compliant,
        violations=violations,
        warnings=warnings,
        dimension_scores=dimension_scores,
        contract=contract,
    )

    # Calculate compliance percentage
    compliance_percentage = (
        dimensions_passing / dimensions_checked if dimensions_checked > 0 else 1.0
    )

    # Estimate effort to comply
    if is_compliant:
        effort = "none"
    elif len(blocking_violations) <= 2:
        effort = "low"
    elif len(blocking_violations) <= 5:
        effort = "medium"
    else:
        effort = "high"

    # Build final evaluation
    evaluation.is_compliant = is_compliant
    evaluation.confidence_level = confidence_level
    evaluation.violations = violations
    evaluation.warnings = warnings
    evaluation.compliance_percentage = compliance_percentage
    evaluation.worst_dimension = worst_dimension
    evaluation.worst_dimension_score = (
        dimension_scores.get(worst_dimension, 0.0) if worst_dimension else 0.0
    )
    evaluation.estimated_effort_to_comply = effort

    return evaluation


def _calculate_confidence_level(
    is_compliant: bool,
    violations: list[Violation],
    warnings: list[Violation],
    dimension_scores: dict[str, float],
    contract: ContractProfile,
) -> ConfidenceLevel:
    """Calculate traffic light confidence level.

    Args:
        is_compliant: Whether contract is compliant
        violations: List of violations
        warnings: List of warnings
        dimension_scores: Scores by dimension
        contract: The contract being evaluated

    Returns:
        ConfidenceLevel (GREEN, YELLOW, ORANGE, RED)
    """
    blocking_violations = [v for v in violations if v.severity == "blocking"]

    if not is_compliant:
        # Check for critical blocking issues
        if len(blocking_violations) >= 3:
            return ConfidenceLevel.RED

        # Check for blocking conditions (e.g., blocked columns exist)
        blocking_types = [v.violation_type for v in blocking_violations]
        if "blocking_condition" in blocking_types:
            return ConfidenceLevel.RED

        # Has blocking violations but not critical
        return ConfidenceLevel.ORANGE

    # Compliant - check for warnings
    if warnings:
        return ConfidenceLevel.YELLOW

    return ConfidenceLevel.GREEN


def evaluate_all_contracts(
    column_summaries: dict[str, ColumnSummary],
    config_path: Path | None = None,
) -> dict[str, ContractEvaluation]:
    """Evaluate entropy against all available contracts.

    Useful for finding the strictest passing contract.

    Args:
        column_summaries: Dict mapping column key to ColumnSummary
        config_path: Optional path to contracts config

    Returns:
        Dict mapping contract name to evaluation.
    """
    contracts = get_contracts(config_path)
    evaluations: dict[str, ContractEvaluation] = {}

    for name in contracts:
        evaluations[name] = evaluate_contract(column_summaries, name, config_path)

    return evaluations


def find_best_contract(
    column_summaries: dict[str, ColumnSummary],
    config_path: Path | None = None,
) -> tuple[str | None, ContractEvaluation | None]:
    """Find the strictest contract that passes.

    Args:
        column_summaries: Dict mapping column key to ColumnSummary
        config_path: Optional path to contracts config

    Returns:
        Tuple of (contract_name, evaluation) for strictest passing contract.
        Returns (None, None) if no contracts pass.
    """
    evaluations = evaluate_all_contracts(column_summaries, config_path)
    contracts = get_contracts(config_path)

    # Sort by strictness (lower overall_threshold = stricter)
    sorted_names = sorted(
        evaluations.keys(),
        key=lambda n: contracts[n].overall_threshold,
    )

    # Find strictest passing
    for name in sorted_names:
        if evaluations[name].is_compliant:
            return name, evaluations[name]

    # None pass
    return None, None
