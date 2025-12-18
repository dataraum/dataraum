"""Financial domain analyzer.

Implements the DomainAnalyzer interface for financial quality checks.
This is the main entry point for financial domain analysis.
"""

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import duckdb
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models.base import Result
from dataraum_context.domains.base import DomainAnalyzer
from dataraum_context.domains.financial.checks import (
    check_double_entry_balance,
    check_fiscal_period_integrity,
    check_sign_conventions,
    check_trial_balance,
)
from dataraum_context.domains.financial.db_models import (
    DoubleEntryCheck as DBDoubleEntryCheck,
)
from dataraum_context.domains.financial.db_models import (
    FinancialQualityMetrics as DBFinancialQualityMetrics,
)
from dataraum_context.domains.financial.db_models import (
    FiscalPeriodIntegrity as DBFiscalPeriodIntegrity,
)
from dataraum_context.domains.financial.db_models import (
    SignConventionViolation as DBSignConventionViolation,
)
from dataraum_context.domains.financial.db_models import (
    TrialBalanceCheck as DBTrialBalanceCheck,
)
from dataraum_context.domains.financial.models import (
    FinancialQualityConfig,
    FinancialQualityIssue,
    FinancialQualityResult,
    FiscalPeriodIntegrityCheck,
)
from dataraum_context.domains.registry import register_domain


@register_domain("financial")
class FinancialDomainAnalyzer(DomainAnalyzer):
    """Financial domain analyzer - standard accounting checks.

    Implements double-entry balance, trial balance, sign conventions,
    and fiscal period integrity checks.

    Note: Business cycle detection is handled separately and requires
    topology analysis.
    """

    @property
    def domain_name(self) -> str:
        return "financial"

    async def analyze(
        self,
        table_id: str,
        duckdb_conn: duckdb.DuckDBPyConnection,
        session: AsyncSession,
        config: FinancialQualityConfig | None = None,  # type: ignore[override]
    ) -> Result[dict[str, Any]]:
        """Run standard financial quality checks.

        Checks:
        - Double-entry balance (debits = credits)
        - Trial balance (Assets = Liabilities + Equity)
        - Sign conventions (correct debit/credit signs)
        - Fiscal period integrity (completeness, cutoff)

        Does NOT include:
        - Business cycle detection (requires topology)
        - LLM interpretation (separate concern)

        Args:
            table_id: Table to analyze
            duckdb_conn: DuckDB connection
            session: SQLAlchemy session
            config: Optional financial configuration

        Returns:
            Result containing metrics dict with all check results
        """
        if config is None:
            config = FinancialQualityConfig()

        metric_id = str(uuid4())
        computed_at = datetime.now(UTC)
        quality_issues: list[FinancialQualityIssue] = []

        # 1. Double-entry balance check
        double_entry_result = await check_double_entry_balance(
            table_id, duckdb_conn, session, config
        )
        if double_entry_result.success:
            double_entry = double_entry_result.unwrap()
            double_entry_balanced = double_entry.is_balanced
            balance_difference = double_entry.net_difference

            if not double_entry_balanced:
                quality_issues.append(
                    FinancialQualityIssue(
                        issue_type="double_entry_imbalance",
                        severity="critical",
                        description=f"Double-entry imbalance: difference of {balance_difference:.2f}",
                        recommendation="Review all transactions and ensure debits equal credits",
                    )
                )
        else:
            double_entry = None
            double_entry_balanced = False
            balance_difference = 0.0
            quality_issues.append(
                FinancialQualityIssue(
                    issue_type="double_entry_check_failed",
                    severity="moderate",
                    description=f"Could not perform double-entry check: {double_entry_result.error}",
                )
            )

        # 2. Trial balance check
        trial_balance_result = await check_trial_balance(table_id, duckdb_conn, session, config)
        if trial_balance_result.success:
            trial_balance = trial_balance_result.unwrap()
            trial_balance_check = trial_balance.equation_holds
            accounting_equation_holds = trial_balance.equation_holds
            assets_total = trial_balance.assets
            liabilities_total = trial_balance.liabilities
            equity_total = trial_balance.equity

            if not trial_balance_check:
                quality_issues.append(
                    FinancialQualityIssue(
                        issue_type="trial_balance_imbalance",
                        severity="critical",
                        description=f"Trial balance does not hold: Assets={assets_total:.2f}, Liabilities+Equity={liabilities_total + equity_total:.2f}",
                        recommendation="Review account classifications and balances",
                    )
                )
        else:
            trial_balance = None
            trial_balance_check = False
            accounting_equation_holds = None
            assets_total = None
            liabilities_total = None
            equity_total = None

        # 3. Sign convention check
        sign_result = await check_sign_conventions(table_id, duckdb_conn, session, config)
        if sign_result.success:
            sign_compliance, sign_violations = sign_result.unwrap()

            if sign_compliance < 0.95:
                quality_issues.append(
                    FinancialQualityIssue(
                        issue_type="sign_convention_violations",
                        severity="moderate" if sign_compliance > 0.90 else "severe",
                        description=f"Sign convention compliance: {sign_compliance:.1%}. Found {len(sign_violations)} violations",
                        affected_accounts=[v.account_identifier for v in sign_violations[:10]],
                        recommendation="Review account types and ensure correct sign conventions",
                    )
                )
        else:
            sign_compliance = 0.0
            sign_violations = []

        # 4. Fiscal period integrity
        period_result = await check_fiscal_period_integrity(table_id, duckdb_conn, session, config)
        period_checks: list[FiscalPeriodIntegrityCheck] = []
        if period_result.success:
            period_checks = period_result.unwrap()
            fiscal_period_complete = all(c.is_complete for c in period_checks)
            period_end_cutoff_clean = all(c.cutoff_clean for c in period_checks)

            for check in period_checks:
                if not check.is_complete:
                    quality_issues.append(
                        FinancialQualityIssue(
                            issue_type="fiscal_period_incomplete",
                            severity="moderate",
                            description=f"Fiscal period {check.fiscal_period} incomplete: {check.missing_days} days missing",
                            recommendation="Verify transaction completeness for the period",
                        )
                    )

                if not check.cutoff_clean:
                    quality_issues.append(
                        FinancialQualityIssue(
                            issue_type="period_cutoff_violation",
                            severity="minor",
                            description=f"Period cutoff issues: {check.late_transactions} late, {check.early_transactions} early transactions",
                            recommendation="Review transaction dating and period close procedures",
                        )
                    )
        else:
            fiscal_period_complete = True
            period_end_cutoff_clean = True

        # Persist to database
        db_metric = DBFinancialQualityMetrics(
            metric_id=metric_id,
            table_id=table_id,
            computed_at=computed_at,
            double_entry_balanced=double_entry_balanced,
            balance_difference=balance_difference,
            balance_tolerance=config.double_entry_tolerance,
            trial_balance_check=trial_balance_check,
            accounting_equation_holds=accounting_equation_holds,
            assets_total=assets_total,
            liabilities_total=liabilities_total,
            equity_total=equity_total,
            sign_convention_compliance=sign_compliance,
            sign_violations=[
                {
                    "account": v.account_identifier,
                    "type": v.account_type,
                    "amount": v.amount,
                    "severity": v.violation_severity,
                }
                for v in sign_violations
            ],
            intercompany_elimination_rate=None,
            orphaned_intercompany=None,
            fiscal_period_complete=fiscal_period_complete,
            period_end_cutoff_clean=period_end_cutoff_clean,
        )
        session.add(db_metric)
        await session.flush()  # Flush parent before adding children with FK references

        # Store detailed checks
        if double_entry:
            db_double_entry = DBDoubleEntryCheck(
                check_id=double_entry.check_id,
                metric_id=metric_id,
                computed_at=computed_at,
                total_debits=double_entry.total_debits,
                debit_account_count=double_entry.debit_account_count,
                total_credits=double_entry.total_credits,
                credit_account_count=double_entry.credit_account_count,
                net_difference=double_entry.net_difference,
                is_balanced=double_entry.is_balanced,
            )
            session.add(db_double_entry)

        if trial_balance:
            db_trial_balance = DBTrialBalanceCheck(
                check_id=trial_balance.check_id,
                metric_id=metric_id,
                computed_at=computed_at,
                assets=trial_balance.assets,
                liabilities=trial_balance.liabilities,
                equity=trial_balance.equity,
                equation_difference=trial_balance.equation_difference,
                equation_holds=trial_balance.equation_holds,
                tolerance=trial_balance.tolerance,
            )
            session.add(db_trial_balance)

        for violation in sign_violations:
            db_violation = DBSignConventionViolation(
                violation_id=violation.violation_id,
                metric_id=metric_id,
                detected_at=computed_at,
                account_identifier=violation.account_identifier,
                account_type=violation.account_type,
                expected_sign=violation.expected_sign,
                actual_sign=violation.actual_sign,
                amount=violation.amount,
                violation_severity=violation.violation_severity,
                description=violation.description,
                transaction_date=violation.transaction_date,
            )
            session.add(db_violation)

        for check in period_checks:
            db_check = DBFiscalPeriodIntegrity(
                check_id=check.check_id,
                metric_id=metric_id,
                computed_at=computed_at,
                fiscal_period=check.fiscal_period,
                period_start=check.period_start,
                period_end=check.period_end,
                is_complete=check.is_complete,
                missing_days=check.missing_days,
                cutoff_clean=check.cutoff_clean,
                late_transactions=check.late_transactions,
                early_transactions=check.early_transactions,
                transaction_count=check.transaction_count,
                total_amount=check.total_amount,
            )
            session.add(db_check)

        await session.commit()

        # Build Pydantic result
        result = FinancialQualityResult(
            metric_id=metric_id,
            table_id=table_id,
            computed_at=computed_at,
            double_entry_balanced=double_entry_balanced,
            balance_difference=balance_difference,
            balance_tolerance=config.double_entry_tolerance,
            double_entry_details=double_entry,
            trial_balance_check=trial_balance_check,
            accounting_equation_holds=accounting_equation_holds,
            assets_total=assets_total,
            liabilities_total=liabilities_total,
            equity_total=equity_total,
            trial_balance_details=trial_balance,
            sign_convention_compliance=sign_compliance,
            sign_violations=sign_violations,
            intercompany_elimination_rate=None,
            orphaned_intercompany=None,
            fiscal_period_complete=fiscal_period_complete,
            period_end_cutoff_clean=period_end_cutoff_clean,
            period_integrity_details=period_checks,
            quality_issues=quality_issues,
            has_issues=len(quality_issues) > 0,
        )

        return Result.ok(result.model_dump(mode="json"))

    def get_issues(self, metrics: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract quality issues from financial metrics."""
        issues = []

        # Double-entry issues
        if not metrics.get("double_entry_balanced"):
            issues.append(
                {
                    "type": "double_entry_imbalance",
                    "severity": "critical",
                    "description": f"Debits/credits imbalanced by {metrics.get('balance_difference', 0):.2f}",
                    "recommendation": "Review all transactions for missing entries",
                }
            )

        # Trial balance issues
        if (
            not metrics.get("trial_balance_check")
            and metrics.get("accounting_equation_holds") is not None
        ):
            issues.append(
                {
                    "type": "trial_balance_imbalance",
                    "severity": "critical",
                    "description": "Accounting equation does not hold",
                    "recommendation": "Review account classifications",
                }
            )

        # Sign convention issues
        sign_compliance = metrics.get("sign_convention_compliance", 1.0)
        if sign_compliance < 0.95:
            issues.append(
                {
                    "type": "sign_convention_violations",
                    "severity": "moderate",
                    "description": f"Sign compliance: {sign_compliance:.1%}",
                    "recommendation": "Review account type assignments",
                }
            )

        # Period integrity
        if not metrics.get("fiscal_period_complete"):
            issues.append(
                {
                    "type": "fiscal_period_incomplete",
                    "severity": "moderate",
                    "description": "Fiscal period has incomplete data",
                    "recommendation": "Check for missing transactions",
                }
            )

        return issues


# Convenience function for backward compatibility
async def analyze_financial_quality(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    config: FinancialQualityConfig | None = None,
) -> Result[FinancialQualityResult]:
    """Comprehensive financial quality analysis.

    This is a convenience wrapper around FinancialDomainAnalyzer.analyze()
    that returns the typed FinancialQualityResult.

    Args:
        table_id: UUID of the table to analyze
        duckdb_conn: DuckDB connection
        session: SQLAlchemy async session
        config: Financial quality configuration (uses defaults if None)

    Returns:
        Result containing FinancialQualityResult
    """
    analyzer = FinancialDomainAnalyzer()
    result = await analyzer.analyze(table_id, duckdb_conn, session, config)

    if not result.success:
        return Result.fail(result.error or "Unknown error")

    # Convert dict back to typed result
    return Result.ok(FinancialQualityResult.model_validate(result.unwrap()))
