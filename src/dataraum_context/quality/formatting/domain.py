"""Domain quality formatter.

Formats domain-specific quality metrics into contextualized output for LLM consumption.
Groups related metrics into interpretation units:
- Compliance: General rule compliance scores and violation counts
- Financial Balancing: Double-entry balance, trial balance, balance differences
- Sign Conventions: Sign compliance, violations by account type
- Fiscal Periods: Period completeness, cutoff cleanliness
- Issues: Quality issues summary

Usage:
    from dataraum_context.quality.formatting.domain import (
        format_domain_quality,
        format_compliance_group,
        format_financial_balance_group,
        format_sign_convention_group,
        format_fiscal_period_group,
    )

    result = format_domain_quality(
        compliance_score=0.95,
        double_entry_balanced=True,
        sign_convention_compliance=0.98,
        config=formatter_config,
    )
"""

from dataclasses import dataclass
from typing import Any

from dataraum_context.quality.formatting.config import FormatterConfig, get_default_config


@dataclass
class MetricContext:
    """Contextualized metric with severity and interpretation."""

    value: Any
    severity: str
    interpretation: str
    details: dict[str, Any] | None = None


@dataclass
class GroupContext:
    """Contextualized metric group."""

    group_name: str
    overall_severity: str
    interpretation: str
    metrics: dict[str, MetricContext]
    samples: list[Any] | None = None


# =============================================================================
# Interpretation Templates
# =============================================================================

COMPLIANCE_INTERPRETATIONS = {
    "none": "Full compliance with domain rules",
    "low": "Minor compliance deviations, mostly acceptable",
    "moderate": "Moderate compliance issues, review recommended",
    "high": "Significant compliance gaps, action required",
    "severe": "Severe compliance failures, immediate attention needed",
    "critical": "Critical compliance violations, regulatory risk",
}

FINANCIAL_BALANCE_INTERPRETATIONS = {
    "none": "Books are balanced, accounting integrity verified",
    "low": "Minor balance discrepancies within tolerance",
    "moderate": "Moderate imbalances detected, investigation needed",
    "high": "Significant balance discrepancies, potential errors",
    "severe": "Severe accounting imbalances, integrity concern",
    "critical": "Critical balance failures, audit required",
}

SIGN_CONVENTION_INTERPRETATIONS = {
    "none": "Sign conventions followed correctly",
    "low": "Minor sign convention deviations",
    "moderate": "Moderate sign violations, review account postings",
    "high": "Significant sign convention issues",
    "severe": "Severe sign convention violations, potential misclassification",
    "critical": "Critical sign convention failures, account structure issues",
}

FISCAL_PERIOD_INTERPRETATIONS = {
    "none": "Fiscal periods complete with clean cutoffs",
    "low": "Minor period irregularities",
    "moderate": "Moderate period issues, some gaps or cutoff concerns",
    "high": "Significant period integrity issues",
    "severe": "Severe period integrity failures",
    "critical": "Critical fiscal period issues, reporting integrity at risk",
}


# =============================================================================
# Group Formatters
# =============================================================================


def format_compliance_group(
    compliance_score: float | None = None,
    violation_count: int | None = None,
    violations: list[dict[str, Any]] | None = None,
    rules_checked: int | None = None,
    rules_passed: int | None = None,
    config: FormatterConfig | None = None,
    table_name: str | None = None,
) -> GroupContext:
    """Format compliance metrics into contextualized group.

    Args:
        compliance_score: Overall compliance rate (0-1)
        violation_count: Number of rule violations
        violations: Violation details
        rules_checked: Total rules evaluated
        rules_passed: Rules that passed
        config: Formatter configuration
        table_name: Table name for context

    Returns:
        GroupContext with compliance analysis
    """
    config = config or get_default_config()
    metrics: dict[str, MetricContext] = {}
    severities: list[str] = []

    # Compliance score
    if compliance_score is not None:
        severity = config.get_severity("domain", "compliance_score", compliance_score, table_name)
        severities.append(severity)

        pct = compliance_score * 100
        if rules_checked is not None and rules_passed is not None:
            interp = f"{pct:.1f}% compliance ({rules_passed} of {rules_checked} rules passed)"
        else:
            interp = f"{pct:.1f}% compliance rate"

        metrics["compliance_score"] = MetricContext(
            value=compliance_score,
            severity=severity,
            interpretation=interp,
            details={
                "rules_checked": rules_checked,
                "rules_passed": rules_passed,
            },
        )

    # Violation count
    if violation_count is not None:
        severity = config.get_severity("domain", "violation_count", violation_count, table_name)
        severities.append(severity)

        if violation_count == 0:
            interp = "No rule violations detected"
        elif violation_count == 1:
            interp = "1 rule violation detected"
        else:
            interp = f"{violation_count} rule violations detected"

        # Format violation samples
        violation_samples = None
        if violations:
            violation_samples = [
                {
                    "rule": v.get("rule_name") or v.get("rule_type"),
                    "severity": v.get("severity"),
                    "description": v.get("description"),
                }
                for v in violations[:5]
            ]

        metrics["violation_count"] = MetricContext(
            value=violation_count,
            severity=severity,
            interpretation=interp,
            details={"violations": violation_samples} if violation_samples else None,
        )

    # Determine overall severity
    severity_order = ["none", "low", "moderate", "high", "severe", "critical"]
    overall_severity = "none"
    for sev in severities:
        if sev in severity_order and severity_order.index(sev) > severity_order.index(
            overall_severity
        ):
            overall_severity = sev

    return GroupContext(
        group_name="compliance",
        overall_severity=overall_severity,
        interpretation=COMPLIANCE_INTERPRETATIONS.get(
            overall_severity, "Unknown compliance status"
        ),
        metrics=metrics,
    )


def format_financial_balance_group(
    double_entry_balanced: bool | None = None,
    balance_difference: float | None = None,
    balance_tolerance: float | None = None,
    total_debits: float | None = None,
    total_credits: float | None = None,
    trial_balance_check: bool | None = None,
    accounting_equation_holds: bool | None = None,
    assets_total: float | None = None,
    liabilities_total: float | None = None,
    equity_total: float | None = None,
    config: FormatterConfig | None = None,
    table_name: str | None = None,
) -> GroupContext:
    """Format financial balancing metrics into contextualized group.

    Args:
        double_entry_balanced: Whether double-entry balances
        balance_difference: Net difference between debits and credits
        balance_tolerance: Acceptable tolerance
        total_debits: Sum of debits
        total_credits: Sum of credits
        trial_balance_check: Trial balance passed
        accounting_equation_holds: Assets = Liabilities + Equity
        assets_total: Total assets
        liabilities_total: Total liabilities
        equity_total: Total equity
        config: Formatter configuration
        table_name: Table name for context

    Returns:
        GroupContext with balance analysis
    """
    config = config or get_default_config()
    metrics: dict[str, MetricContext] = {}
    severities: list[str] = []

    # Double-entry balance
    if double_entry_balanced is not None:
        if double_entry_balanced:
            severity = "none"
            interp = "Double-entry accounting balanced"
        else:
            # Use balance_difference to determine severity
            if balance_difference is not None:
                abs_diff = abs(balance_difference)
                severity = config.get_severity("domain", "balance_difference", abs_diff, table_name)
                interp = f"Double-entry imbalance: {balance_difference:,.2f}"
            else:
                severity = "high"
                interp = "Double-entry accounting not balanced"

        severities.append(severity)

        metrics["double_entry_balanced"] = MetricContext(
            value=double_entry_balanced,
            severity=severity,
            interpretation=interp,
            details={
                "balance_difference": balance_difference,
                "tolerance": balance_tolerance,
                "total_debits": total_debits,
                "total_credits": total_credits,
            },
        )

    # Trial balance
    if trial_balance_check is not None:
        if trial_balance_check:
            severity = "none"
            interp = "Trial balance verified"
        else:
            severity = "high"
            interp = "Trial balance check failed"

        severities.append(severity)

        metrics["trial_balance_check"] = MetricContext(
            value=trial_balance_check,
            severity=severity,
            interpretation=interp,
            details={
                "accounting_equation_holds": accounting_equation_holds,
            },
        )

    # Accounting equation (Assets = Liabilities + Equity)
    if accounting_equation_holds is not None:
        if accounting_equation_holds:
            severity = "none"
            if assets_total is not None:
                interp = f"Accounting equation holds: A({assets_total:,.0f}) = L + E"
            else:
                interp = "Accounting equation holds"
        else:
            severity = "high"
            if (
                assets_total is not None
                and liabilities_total is not None
                and equity_total is not None
            ):
                diff = assets_total - (liabilities_total + equity_total)
                interp = f"Accounting equation fails: A - (L + E) = {diff:,.2f}"
            else:
                interp = "Accounting equation does not hold"

        severities.append(severity)

        metrics["accounting_equation_holds"] = MetricContext(
            value=accounting_equation_holds,
            severity=severity,
            interpretation=interp,
            details={
                "assets_total": assets_total,
                "liabilities_total": liabilities_total,
                "equity_total": equity_total,
            },
        )

    # Determine overall severity
    severity_order = ["none", "low", "moderate", "high", "severe", "critical"]
    overall_severity = "none"
    for sev in severities:
        if sev in severity_order and severity_order.index(sev) > severity_order.index(
            overall_severity
        ):
            overall_severity = sev

    return GroupContext(
        group_name="financial_balance",
        overall_severity=overall_severity,
        interpretation=FINANCIAL_BALANCE_INTERPRETATIONS.get(
            overall_severity, "Unknown balance status"
        ),
        metrics=metrics,
    )


def format_sign_convention_group(
    sign_convention_compliance: float | None = None,
    sign_violations: list[dict[str, Any]] | None = None,
    violation_count: int | None = None,
    config: FormatterConfig | None = None,
    table_name: str | None = None,
) -> GroupContext:
    """Format sign convention metrics into contextualized group.

    Args:
        sign_convention_compliance: Compliance rate (0-1)
        sign_violations: List of sign violations
        violation_count: Number of sign violations
        config: Formatter configuration
        table_name: Table name for context

    Returns:
        GroupContext with sign convention analysis
    """
    config = config or get_default_config()
    metrics: dict[str, MetricContext] = {}
    severities: list[str] = []

    # Sign compliance
    if sign_convention_compliance is not None:
        severity = config.get_severity(
            "domain", "sign_compliance", sign_convention_compliance, table_name
        )
        severities.append(severity)

        pct = sign_convention_compliance * 100
        if pct >= 99.9:
            interp = "Sign conventions fully compliant"
        elif pct >= 99:
            interp = f"{pct:.1f}% sign convention compliance (minor deviations)"
        else:
            interp = f"{pct:.1f}% sign convention compliance"

        # Count violations if provided
        v_count = violation_count or (len(sign_violations) if sign_violations else None)

        metrics["sign_convention_compliance"] = MetricContext(
            value=sign_convention_compliance,
            severity=severity,
            interpretation=interp,
            details={"violation_count": v_count},
        )

    # Sign violations
    if sign_violations:
        # Group violations by account type
        by_type: dict[str, int] = {}
        for v in sign_violations:
            account_type = v.get("account_type", "unknown")
            by_type[account_type] = by_type.get(account_type, 0) + 1

        # Format samples
        violation_samples = [
            {
                "account": v.get("account_identifier"),
                "account_type": v.get("account_type"),
                "expected": v.get("expected_sign"),
                "actual": v.get("actual_sign"),
                "amount": v.get("amount"),
            }
            for v in sign_violations[:5]
        ]

        count = len(sign_violations)
        if count == 1:
            interp = "1 sign convention violation"
        else:
            interp = f"{count} sign convention violations"

        metrics["sign_violations"] = MetricContext(
            value=count,
            severity="none" if count == 0 else ("low" if count <= 5 else "moderate"),
            interpretation=interp,
            details={
                "by_account_type": by_type,
                "samples": violation_samples,
            },
        )

    # Determine overall severity
    severity_order = ["none", "low", "moderate", "high", "severe", "critical"]
    overall_severity = "none"
    for sev in severities:
        if sev in severity_order and severity_order.index(sev) > severity_order.index(
            overall_severity
        ):
            overall_severity = sev

    return GroupContext(
        group_name="sign_conventions",
        overall_severity=overall_severity,
        interpretation=SIGN_CONVENTION_INTERPRETATIONS.get(
            overall_severity, "Unknown sign convention status"
        ),
        metrics=metrics,
    )


def format_fiscal_period_group(
    fiscal_period_complete: bool | None = None,
    period_end_cutoff_clean: bool | None = None,
    missing_days: int | None = None,
    late_transactions: int | None = None,
    early_transactions: int | None = None,
    periods: list[dict[str, Any]] | None = None,
    config: FormatterConfig | None = None,
    table_name: str | None = None,
) -> GroupContext:
    """Format fiscal period metrics into contextualized group.

    Args:
        fiscal_period_complete: Whether period is complete
        period_end_cutoff_clean: Whether period cutoff is clean
        missing_days: Days missing from period
        late_transactions: Transactions posted after period end
        early_transactions: Transactions dated before period start
        periods: Period details
        config: Formatter configuration
        table_name: Table name for context

    Returns:
        GroupContext with fiscal period analysis
    """
    config = config or get_default_config()
    metrics: dict[str, MetricContext] = {}
    severities: list[str] = []

    # Period completeness
    if fiscal_period_complete is not None:
        if fiscal_period_complete:
            severity = "none"
            interp = "Fiscal period complete"
        else:
            if missing_days is not None and missing_days > 0:
                severity = "moderate" if missing_days <= 5 else "high"
                interp = f"Fiscal period incomplete ({missing_days} days missing)"
            else:
                severity = "moderate"
                interp = "Fiscal period incomplete"

        severities.append(severity)

        metrics["fiscal_period_complete"] = MetricContext(
            value=fiscal_period_complete,
            severity=severity,
            interpretation=interp,
            details={"missing_days": missing_days},
        )

    # Period cutoff
    if period_end_cutoff_clean is not None:
        if period_end_cutoff_clean:
            severity = "none"
            interp = "Period cutoff is clean"
        else:
            total_cutoff_issues = (late_transactions or 0) + (early_transactions or 0)
            if total_cutoff_issues <= 5:
                severity = "low"
            elif total_cutoff_issues <= 20:
                severity = "moderate"
            else:
                severity = "high"

            parts = []
            if late_transactions:
                parts.append(f"{late_transactions} late")
            if early_transactions:
                parts.append(f"{early_transactions} early")

            if parts:
                interp = f"Period cutoff issues: {', '.join(parts)} transactions"
            else:
                interp = "Period cutoff has issues"

        severities.append(severity)

        metrics["period_end_cutoff_clean"] = MetricContext(
            value=period_end_cutoff_clean,
            severity=severity,
            interpretation=interp,
            details={
                "late_transactions": late_transactions,
                "early_transactions": early_transactions,
            },
        )

    # Period samples
    if periods:
        period_samples = [
            {
                "period": p.get("fiscal_period"),
                "is_complete": p.get("is_complete"),
                "cutoff_clean": p.get("cutoff_clean"),
            }
            for p in periods[:3]
        ]

        metrics["period_summary"] = MetricContext(
            value=len(periods),
            severity="none",  # Informational
            interpretation=f"{len(periods)} fiscal period(s) analyzed",
            details={"periods": period_samples},
        )

    # Determine overall severity
    severity_order = ["none", "low", "moderate", "high", "severe", "critical"]
    overall_severity = "none"
    for sev in severities:
        if sev in severity_order and severity_order.index(sev) > severity_order.index(
            overall_severity
        ):
            overall_severity = sev

    return GroupContext(
        group_name="fiscal_periods",
        overall_severity=overall_severity,
        interpretation=FISCAL_PERIOD_INTERPRETATIONS.get(
            overall_severity, "Unknown fiscal period status"
        ),
        metrics=metrics,
    )


# =============================================================================
# Main Formatter
# =============================================================================


def format_domain_quality(
    # Compliance metrics
    compliance_score: float | None = None,
    violation_count: int | None = None,
    violations: list[dict[str, Any]] | None = None,
    rules_checked: int | None = None,
    rules_passed: int | None = None,
    # Financial balance metrics
    double_entry_balanced: bool | None = None,
    balance_difference: float | None = None,
    balance_tolerance: float | None = None,
    total_debits: float | None = None,
    total_credits: float | None = None,
    trial_balance_check: bool | None = None,
    accounting_equation_holds: bool | None = None,
    assets_total: float | None = None,
    liabilities_total: float | None = None,
    equity_total: float | None = None,
    # Sign convention metrics
    sign_convention_compliance: float | None = None,
    sign_violations: list[dict[str, Any]] | None = None,
    # Fiscal period metrics
    fiscal_period_complete: bool | None = None,
    period_end_cutoff_clean: bool | None = None,
    missing_days: int | None = None,
    late_transactions: int | None = None,
    early_transactions: int | None = None,
    periods: list[dict[str, Any]] | None = None,
    # Quality issues
    quality_issues: list[dict[str, Any]] | None = None,
    has_issues: bool | None = None,
    # Configuration
    config: FormatterConfig | None = None,
    table_name: str | None = None,
    domain: str | None = None,
) -> dict[str, Any]:
    """Format all domain quality metrics into contextualized output.

    This is the main entry point for domain quality formatting. It groups
    related metrics and produces a structured output suitable for LLM
    consumption or human review.

    Args:
        compliance_score: Overall compliance (0-1)
        violation_count: Number of violations
        violations: Violation details
        rules_checked: Total rules evaluated
        rules_passed: Rules passed
        double_entry_balanced: Double-entry balanced
        balance_difference: Balance difference
        balance_tolerance: Tolerance
        total_debits: Sum of debits
        total_credits: Sum of credits
        trial_balance_check: Trial balance passed
        accounting_equation_holds: Equation holds
        assets_total: Total assets
        liabilities_total: Total liabilities
        equity_total: Total equity
        sign_convention_compliance: Sign compliance
        sign_violations: Sign violations
        fiscal_period_complete: Period complete
        period_end_cutoff_clean: Cutoff clean
        missing_days: Missing days
        late_transactions: Late transactions
        early_transactions: Early transactions
        periods: Period details
        quality_issues: Quality issues
        has_issues: Has issues flag
        config: Formatter configuration
        table_name: Table name for context
        domain: Domain name (e.g., 'financial')

    Returns:
        Dict with contextualized domain quality assessment
    """
    config = config or get_default_config()

    # Format each group
    compliance = format_compliance_group(
        compliance_score=compliance_score,
        violation_count=violation_count,
        violations=violations,
        rules_checked=rules_checked,
        rules_passed=rules_passed,
        config=config,
        table_name=table_name,
    )

    financial_balance = format_financial_balance_group(
        double_entry_balanced=double_entry_balanced,
        balance_difference=balance_difference,
        balance_tolerance=balance_tolerance,
        total_debits=total_debits,
        total_credits=total_credits,
        trial_balance_check=trial_balance_check,
        accounting_equation_holds=accounting_equation_holds,
        assets_total=assets_total,
        liabilities_total=liabilities_total,
        equity_total=equity_total,
        config=config,
        table_name=table_name,
    )

    sign_conventions = format_sign_convention_group(
        sign_convention_compliance=sign_convention_compliance,
        sign_violations=sign_violations,
        config=config,
        table_name=table_name,
    )

    fiscal_periods = format_fiscal_period_group(
        fiscal_period_complete=fiscal_period_complete,
        period_end_cutoff_clean=period_end_cutoff_clean,
        missing_days=missing_days,
        late_transactions=late_transactions,
        early_transactions=early_transactions,
        periods=periods,
        config=config,
        table_name=table_name,
    )

    # Determine overall severity
    severity_order = ["none", "low", "moderate", "high", "severe", "critical"]
    all_severities = [
        compliance.overall_severity,
        financial_balance.overall_severity,
        sign_conventions.overall_severity,
        fiscal_periods.overall_severity,
    ]
    overall_severity = max(
        all_severities, key=lambda s: severity_order.index(s) if s in severity_order else 0
    )

    # Include quality issues if present
    issues_summary = None
    if quality_issues:
        issues_summary = [
            {
                "type": issue.get("issue_type"),
                "severity": issue.get("severity"),
                "description": issue.get("description"),
            }
            for issue in quality_issues[:5]
        ]

    # Build output
    dq_result: dict[str, Any] = {
        "overall_severity": overall_severity,
        "domain": domain or "general",
        "groups": {
            "compliance": _group_to_dict(compliance),
            "financial_balance": _group_to_dict(financial_balance),
            "sign_conventions": _group_to_dict(sign_conventions),
            "fiscal_periods": _group_to_dict(fiscal_periods),
        },
        "table_name": table_name,
    }

    if issues_summary:
        dq_result["quality_issues"] = issues_summary

    if has_issues is not None:
        dq_result["has_issues"] = has_issues

    return {"domain_quality": dq_result}


def _group_to_dict(group: GroupContext) -> dict[str, Any]:
    """Convert GroupContext to dictionary."""
    result: dict[str, Any] = {
        "severity": group.overall_severity,
        "interpretation": group.interpretation,
        "metrics": {},
    }

    for name, metric in group.metrics.items():
        result["metrics"][name] = {
            "value": metric.value,
            "severity": metric.severity,
            "interpretation": metric.interpretation,
        }
        if metric.details:
            result["metrics"][name]["details"] = metric.details

    if group.samples:
        result["samples"] = group.samples

    return result
