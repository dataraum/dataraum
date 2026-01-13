"""Tests for domain quality formatter."""

from dataraum_context.core.formatting.base import ThresholdConfig
from dataraum_context.core.formatting.config import (
    FormatterConfig,
    MetricGroupConfig,
)
from dataraum_context.quality.formatting.domain import (
    format_compliance_group,
    format_domain_quality,
    format_financial_balance_group,
    format_fiscal_period_group,
    format_sign_convention_group,
)


class TestComplianceGroup:
    """Tests for compliance metric group formatting."""

    def test_format_full_compliance(self):
        """Test formatting with full compliance."""
        result = format_compliance_group(
            compliance_score=1.0,
            violation_count=0,
            rules_checked=50,
            rules_passed=50,
        )

        assert result.group_name == "compliance"
        assert result.overall_severity == "none"
        assert "100.0%" in result.metrics["compliance_score"].interpretation

    def test_format_partial_compliance(self):
        """Test formatting with partial compliance."""
        result = format_compliance_group(
            compliance_score=0.85,
            violation_count=15,
            rules_checked=100,
            rules_passed=85,
        )

        assert result.overall_severity in ["low", "moderate", "high", "severe"]
        assert "85 of 100" in result.metrics["compliance_score"].interpretation

    def test_format_with_violations(self):
        """Test formatting includes violation details."""
        violations = [
            {"rule_name": "not_null_check", "severity": "high", "description": "Null values found"},
            {
                "rule_name": "range_check",
                "severity": "moderate",
                "description": "Values out of range",
            },
        ]

        result = format_compliance_group(
            compliance_score=0.9,
            violation_count=2,
            violations=violations,
        )

        assert "violation_count" in result.metrics
        assert result.metrics["violation_count"].details is not None

    def test_format_no_violations(self):
        """Test formatting with no violations."""
        result = format_compliance_group(
            violation_count=0,
        )

        assert "No rule violations" in result.metrics["violation_count"].interpretation


class TestFinancialBalanceGroup:
    """Tests for financial balance group formatting."""

    def test_format_balanced_books(self):
        """Test formatting with balanced double-entry."""
        result = format_financial_balance_group(
            double_entry_balanced=True,
            balance_difference=0.0,
            total_debits=1000000.0,
            total_credits=1000000.0,
        )

        assert result.group_name == "financial_balance"
        assert result.overall_severity == "none"
        assert "balanced" in result.metrics["double_entry_balanced"].interpretation.lower()

    def test_format_imbalanced_books(self):
        """Test formatting with imbalanced double-entry."""
        result = format_financial_balance_group(
            double_entry_balanced=False,
            balance_difference=500.50,
        )

        assert result.overall_severity in ["moderate", "high", "severe"]
        assert "500.50" in result.metrics["double_entry_balanced"].interpretation

    def test_format_trial_balance_pass(self):
        """Test formatting with passing trial balance."""
        result = format_financial_balance_group(
            trial_balance_check=True,
            accounting_equation_holds=True,
        )

        assert "trial_balance_check" in result.metrics
        assert result.metrics["trial_balance_check"].severity == "none"

    def test_format_trial_balance_fail(self):
        """Test formatting with failing trial balance."""
        result = format_financial_balance_group(
            trial_balance_check=False,
            accounting_equation_holds=False,
            assets_total=1000000.0,
            liabilities_total=600000.0,
            equity_total=350000.0,  # Diff = 50000
        )

        assert result.metrics["trial_balance_check"].severity == "high"
        assert result.metrics["accounting_equation_holds"].severity == "high"

    def test_format_accounting_equation(self):
        """Test formatting accounting equation."""
        result = format_financial_balance_group(
            accounting_equation_holds=True,
            assets_total=1000000.0,
            liabilities_total=600000.0,
            equity_total=400000.0,
        )

        assert "accounting_equation_holds" in result.metrics
        assert "A(" in result.metrics["accounting_equation_holds"].interpretation


class TestSignConventionGroup:
    """Tests for sign convention group formatting."""

    def test_format_full_compliance(self):
        """Test formatting with full sign compliance."""
        result = format_sign_convention_group(
            sign_convention_compliance=1.0,
        )

        assert result.group_name == "sign_conventions"
        assert result.overall_severity == "none"
        assert "fully compliant" in result.metrics["sign_convention_compliance"].interpretation

    def test_format_minor_violations(self):
        """Test formatting with minor sign violations."""
        result = format_sign_convention_group(
            sign_convention_compliance=0.995,
        )

        assert "minor deviations" in result.metrics["sign_convention_compliance"].interpretation

    def test_format_with_violations(self):
        """Test formatting includes violation details."""
        sign_violations = [
            {
                "account_identifier": "A-001",
                "account_type": "asset",
                "expected_sign": "debit",
                "actual_sign": "credit",
                "amount": -5000.0,
            },
            {
                "account_identifier": "L-002",
                "account_type": "liability",
                "expected_sign": "credit",
                "actual_sign": "debit",
                "amount": 3000.0,
            },
        ]

        result = format_sign_convention_group(
            sign_convention_compliance=0.98,
            sign_violations=sign_violations,
        )

        assert "sign_violations" in result.metrics
        assert result.metrics["sign_violations"].details["by_account_type"] == {
            "asset": 1,
            "liability": 1,
        }


class TestFiscalPeriodGroup:
    """Tests for fiscal period group formatting."""

    def test_format_complete_period(self):
        """Test formatting with complete fiscal period."""
        result = format_fiscal_period_group(
            fiscal_period_complete=True,
            period_end_cutoff_clean=True,
        )

        assert result.group_name == "fiscal_periods"
        assert result.overall_severity == "none"
        assert "complete" in result.metrics["fiscal_period_complete"].interpretation.lower()

    def test_format_incomplete_period(self):
        """Test formatting with incomplete period."""
        result = format_fiscal_period_group(
            fiscal_period_complete=False,
            missing_days=10,
        )

        assert result.overall_severity in ["moderate", "high"]
        assert "10 days missing" in result.metrics["fiscal_period_complete"].interpretation

    def test_format_cutoff_issues(self):
        """Test formatting with cutoff issues."""
        result = format_fiscal_period_group(
            fiscal_period_complete=True,
            period_end_cutoff_clean=False,
            late_transactions=15,
            early_transactions=5,
        )

        assert result.metrics["period_end_cutoff_clean"].severity != "none"
        assert "15 late" in result.metrics["period_end_cutoff_clean"].interpretation
        assert "5 early" in result.metrics["period_end_cutoff_clean"].interpretation

    def test_format_with_periods(self):
        """Test formatting includes period details."""
        periods = [
            {"fiscal_period": "Q1-2024", "is_complete": True, "cutoff_clean": True},
            {"fiscal_period": "Q2-2024", "is_complete": True, "cutoff_clean": False},
        ]

        result = format_fiscal_period_group(
            fiscal_period_complete=True,
            periods=periods,
        )

        assert "period_summary" in result.metrics
        assert "2 fiscal period" in result.metrics["period_summary"].interpretation


class TestDomainQualityMain:
    """Tests for main format_domain_quality function."""

    def test_combines_all_groups(self):
        """Test main function includes all groups."""
        result = format_domain_quality(
            compliance_score=0.95,
            double_entry_balanced=True,
            sign_convention_compliance=0.99,
            fiscal_period_complete=True,
        )

        assert "domain_quality" in result
        dq = result["domain_quality"]

        assert "overall_severity" in dq
        assert "groups" in dq
        assert "compliance" in dq["groups"]
        assert "financial_balance" in dq["groups"]
        assert "sign_conventions" in dq["groups"]
        assert "fiscal_periods" in dq["groups"]

    def test_overall_severity_is_worst(self):
        """Test overall severity is worst of all groups."""
        result = format_domain_quality(
            compliance_score=0.99,  # none
            double_entry_balanced=False,  # high
            balance_difference=10000.0,  # makes it worse
            sign_convention_compliance=0.99,  # none
        )

        dq = result["domain_quality"]
        assert dq["overall_severity"] in ["moderate", "high", "severe", "critical"]

    def test_table_name_passed_through(self):
        """Test table name is included in output."""
        result = format_domain_quality(
            compliance_score=0.95,
            table_name="journal_entries",
        )

        assert result["domain_quality"]["table_name"] == "journal_entries"

    def test_domain_name_included(self):
        """Test domain name is included in output."""
        result = format_domain_quality(
            compliance_score=0.95,
            domain="financial",
        )

        assert result["domain_quality"]["domain"] == "financial"

    def test_custom_config(self):
        """Test custom configuration is respected."""
        strict_config = FormatterConfig(
            groups={
                "domain": MetricGroupConfig(
                    thresholds={
                        "compliance_score": ThresholdConfig(
                            thresholds={"none": 0.999},  # Very strict
                            default_severity="critical",
                            ascending=False,
                        ),
                    }
                ),
            }
        )

        result = format_domain_quality(
            compliance_score=0.99,  # Would be "none" with default, but "critical" with strict
            config=strict_config,
        )

        compliance = result["domain_quality"]["groups"]["compliance"]
        assert compliance["metrics"]["compliance_score"]["severity"] == "critical"

    def test_handles_missing_data(self):
        """Test handles missing optional fields gracefully."""
        result = format_domain_quality(
            compliance_score=0.95,
            # All other fields missing
        )

        assert "domain_quality" in result
        assert result["domain_quality"]["overall_severity"] is not None

    def test_includes_quality_issues(self):
        """Test quality issues are included when present."""
        issues = [
            {
                "issue_type": "double_entry_imbalance",
                "severity": "high",
                "description": "Debits and credits do not balance",
            },
        ]

        result = format_domain_quality(
            compliance_score=0.95,
            quality_issues=issues,
            has_issues=True,
        )

        assert "quality_issues" in result["domain_quality"]
        assert result["domain_quality"]["has_issues"] is True

    def test_output_structure(self):
        """Test output has expected structure."""
        result = format_domain_quality(
            compliance_score=0.9,
            violation_count=5,
            double_entry_balanced=True,
            sign_convention_compliance=0.98,
            fiscal_period_complete=True,
            period_end_cutoff_clean=False,
            late_transactions=10,
        )

        dq = result["domain_quality"]

        # Check compliance group structure
        compliance = dq["groups"]["compliance"]
        assert "severity" in compliance
        assert "interpretation" in compliance
        assert "metrics" in compliance
        assert "compliance_score" in compliance["metrics"]
        assert "value" in compliance["metrics"]["compliance_score"]
        assert "severity" in compliance["metrics"]["compliance_score"]
        assert "interpretation" in compliance["metrics"]["compliance_score"]

    def test_empty_groups_still_present(self):
        """Test that groups appear even with no data."""
        result = format_domain_quality()

        dq = result["domain_quality"]
        assert "compliance" in dq["groups"]
        assert "financial_balance" in dq["groups"]
        assert "sign_conventions" in dq["groups"]
        assert "fiscal_periods" in dq["groups"]
