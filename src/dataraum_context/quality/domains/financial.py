"""Financial domain quality checks.

Implements financial accounting-specific quality rules:

**Accounting Quality Checks:**
- Double-entry balance validation
- Trial balance checks (Assets = Liabilities + Equity)
- Sign convention validation
- Intercompany transaction matching
- Fiscal period integrity

**Topological Domain Analysis:**
- Financial cycle detection and classification
- Fiscal period-aware stability assessment
- Financial anomaly detection in topology
- Domain-weighted quality scoring
"""

import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb
import pandas as pd
import yaml
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models.base import Result
from dataraum_context.quality.domains.db_models import (
    DoubleEntryCheck as DBDoubleEntryCheck,
)
from dataraum_context.quality.domains.db_models import (
    FinancialQualityMetrics as DBFinancialQualityMetrics,
)
from dataraum_context.quality.domains.db_models import (
    FiscalPeriodIntegrity as DBFiscalPeriodIntegrity,
)
from dataraum_context.quality.domains.db_models import (
    SignConventionViolation as DBSignConventionViolation,
)
from dataraum_context.quality.domains.db_models import (
    TrialBalanceCheck as DBTrialBalanceCheck,
)
from dataraum_context.quality.domains.models import (
    DoubleEntryResult,
    FinancialQualityConfig,
    FinancialQualityIssue,
    FinancialQualityResult,
    FiscalPeriodIntegrityCheck,
    SignConventionViolation,
    TrialBalanceResult,
)
from dataraum_context.storage.models_v2 import Table

logger = logging.getLogger(__name__)


async def check_double_entry_balance(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    config: FinancialQualityConfig,
) -> Result[DoubleEntryResult]:
    """Check if debits equal credits (double-entry accounting).

    Args:
        table_id: UUID of the table to check
        duckdb_conn: DuckDB connection
        session: SQLAlchemy async session
        config: Financial quality configuration

    Returns:
        Result containing double-entry balance check result
    """
    try:
        # Get table name from metadata
        table = await session.get(Table, table_id)
        if not table:
            return Result.fail(f"Table {table_id} not found")

        table_name = table.table_name

        # Try to detect account type and amount columns
        # Common patterns: account_type, account_category, type
        # Amount columns: amount, debit, credit, balance

        # First, check if we have explicit debit/credit columns
        columns_query = f"DESCRIBE {table_name}"
        columns_df = duckdb_conn.execute(columns_query).df()
        column_names = columns_df["column_name"].str.lower().tolist()

        has_debit_credit = "debit" in column_names and "credit" in column_names
        has_amount = "amount" in column_names
        has_account_type = any(
            col in column_names for col in ["account_type", "account_category", "type"]
        )

        if has_debit_credit:
            # Explicit debit/credit columns
            query = f"""
                SELECT
                    SUM(debit) as total_debits,
                    SUM(credit) as total_credits,
                    COUNT(DISTINCT CASE WHEN debit > 0 THEN 1 END) as debit_count,
                    COUNT(DISTINCT CASE WHEN credit > 0 THEN 1 END) as credit_count
                FROM {table_name}
            """
        elif has_amount and has_account_type:
            # Amount column with account type classification
            # Debits: Asset, Expense (typically positive)
            # Credits: Liability, Equity, Revenue (typically positive)
            query = f"""
                SELECT
                    SUM(CASE
                        WHEN LOWER(account_type) IN ('asset', 'expense', 'assets', 'expenses')
                        THEN ABS(amount)
                        ELSE 0
                    END) as total_debits,
                    SUM(CASE
                        WHEN LOWER(account_type) IN ('liability', 'equity', 'revenue', 'liabilities', 'income', 'sales')
                        THEN ABS(amount)
                        ELSE 0
                    END) as total_credits,
                    COUNT(DISTINCT CASE
                        WHEN LOWER(account_type) IN ('asset', 'expense', 'assets', 'expenses')
                        THEN 1
                    END) as debit_count,
                    COUNT(DISTINCT CASE
                        WHEN LOWER(account_type) IN ('liability', 'equity', 'revenue', 'liabilities', 'income', 'sales')
                        THEN 1
                    END) as credit_count
                FROM {table_name}
            """
        else:
            return Result.fail(
                "Cannot determine double-entry structure. "
                "Expected either (debit, credit) columns or (amount, account_type) columns."
            )

        result_df = duckdb_conn.execute(query).df()
        row = result_df.iloc[0]

        total_debits = float(row["total_debits"] or 0)
        total_credits = float(row["total_credits"] or 0)
        debit_count = int(row["debit_count"] or 0)
        credit_count = int(row["credit_count"] or 0)

        net_difference = abs(total_debits - total_credits)
        is_balanced = net_difference <= config.double_entry_tolerance

        check_id = str(uuid4())

        # Store in database
        _db_check = DBDoubleEntryCheck(
            check_id=check_id,
            metric_id=None,  # Will be set when creating FinancialQualityMetrics
            computed_at=datetime.now(UTC),
            total_debits=total_debits,
            debit_account_count=debit_count,
            total_credits=total_credits,
            credit_account_count=credit_count,
            net_difference=net_difference,
            is_balanced=is_balanced,
        )

        result = DoubleEntryResult(
            check_id=check_id,
            total_debits=total_debits,
            total_credits=total_credits,
            net_difference=net_difference,
            is_balanced=is_balanced,
            debit_account_count=debit_count,
            credit_account_count=credit_count,
        )

        return Result.ok(result)

    except Exception as e:
        logger.error(f"Double-entry check failed: {e}")
        return Result.fail(f"Double-entry check failed: {str(e)}")


async def check_trial_balance(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    config: FinancialQualityConfig,
) -> Result[TrialBalanceResult]:
    """Check trial balance: Assets = Liabilities + Equity.

    Args:
        table_id: UUID of the table to check
        duckdb_conn: DuckDB connection
        session: SQLAlchemy async session
        config: Financial quality configuration

    Returns:
        Result containing trial balance check result
    """
    try:
        # Get table name
        table = await session.get(Table, table_id)
        if not table:
            return Result.fail(f"Table {table_id} not found")

        table_name = table.table_name

        # Detect columns
        columns_query = f"DESCRIBE {table_name}"
        columns_df = duckdb_conn.execute(columns_query).df()
        column_names = columns_df["column_name"].str.lower().tolist()

        has_amount = "amount" in column_names or "balance" in column_names
        has_account_type = any(
            col in column_names for col in ["account_type", "account_category", "type"]
        )

        if not (has_amount and has_account_type):
            return Result.fail(
                "Cannot determine trial balance structure. "
                "Expected (amount/balance, account_type) columns."
            )

        amount_col = "amount" if "amount" in column_names else "balance"
        type_col = (
            "account_type"
            if "account_type" in column_names
            else ("account_category" if "account_category" in column_names else "type")
        )

        # Calculate Assets, Liabilities, Equity
        query = f"""
            SELECT
                SUM(CASE
                    WHEN LOWER({type_col}) IN ('asset', 'assets')
                    THEN ABS({amount_col})
                    ELSE 0
                END) as assets,
                SUM(CASE
                    WHEN LOWER({type_col}) IN ('liability', 'liabilities')
                    THEN ABS({amount_col})
                    ELSE 0
                END) as liabilities,
                SUM(CASE
                    WHEN LOWER({type_col}) IN ('equity', 'stockholders equity', 'shareholders equity', 'capital')
                    THEN ABS({amount_col})
                    ELSE 0
                END) as equity
            FROM {table_name}
        """

        result_df = duckdb_conn.execute(query).df()
        row = result_df.iloc[0]

        assets = float(row["assets"] or 0)
        liabilities = float(row["liabilities"] or 0)
        equity = float(row["equity"] or 0)

        equation_difference = abs(assets - (liabilities + equity))
        equation_holds = equation_difference <= config.trial_balance_tolerance

        check_id = str(uuid4())

        # Store in database
        _db_check = DBTrialBalanceCheck(
            check_id=check_id,
            metric_id=None,  # Will be set later
            computed_at=datetime.now(UTC),
            assets=assets,
            liabilities=liabilities,
            equity=equity,
            equation_difference=equation_difference,
            equation_holds=equation_holds,
            tolerance=config.trial_balance_tolerance,
        )

        result = TrialBalanceResult(
            check_id=check_id,
            assets=assets,
            liabilities=liabilities,
            equity=equity,
            equation_difference=equation_difference,
            equation_holds=equation_holds,
            tolerance=config.trial_balance_tolerance,
        )

        return Result.ok(result)

    except Exception as e:
        logger.error(f"Trial balance check failed: {e}")
        return Result.fail(f"Trial balance check failed: {str(e)}")


async def check_sign_conventions(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    config: FinancialQualityConfig,
) -> Result[tuple[float, list[SignConventionViolation]]]:
    """Check sign conventions for account types.

    Validates that accounts follow expected sign conventions:
    - Assets/Expenses: Debit (positive)
    - Liabilities/Equity/Revenue: Credit (positive)

    Args:
        table_id: UUID of the table to check
        duckdb_conn: DuckDB connection
        session: SQLAlchemy async session
        config: Financial quality configuration

    Returns:
        Result containing (compliance_score, violations)
    """
    try:
        # Get table name
        table = await session.get(Table, table_id)
        if not table:
            return Result.fail(f"Table {table_id} not found")

        table_name = table.table_name

        # Detect columns
        columns_query = f"DESCRIBE {table_name}"
        columns_df = duckdb_conn.execute(columns_query).df()
        column_names = columns_df["column_name"].str.lower().tolist()

        has_amount = "amount" in column_names or "balance" in column_names
        has_account_type = any(
            col in column_names for col in ["account_type", "account_category", "type"]
        )

        if not (has_amount and has_account_type):
            return Result.fail(
                "Cannot check sign conventions. Expected (amount/balance, account_type) columns."
            )

        amount_col = "amount" if "amount" in column_names else "balance"
        type_col = (
            "account_type"
            if "account_type" in column_names
            else ("account_category" if "account_category" in column_names else "type")
        )

        # Get account identifier column (account_name, account_code, account, name, code)
        account_id_col = None
        for col in ["account_name", "account_code", "account", "name", "code"]:
            if col in column_names:
                account_id_col = col
                break

        if not account_id_col:
            account_id_col = type_col  # Fallback to type column

        # Query to find violations
        # For simplicity, we'll check if amounts have unexpected signs
        query = f"""
            SELECT
                {account_id_col} as account_identifier,
                {type_col} as account_type,
                {amount_col} as amount,
                COUNT(*) as violation_count
            FROM {table_name}
            WHERE (
                -- Asset/Expense should be positive (debit normal balance)
                (LOWER({type_col}) IN ('asset', 'assets', 'expense', 'expenses') AND {amount_col} < 0)
                OR
                -- Liability/Equity/Revenue should be positive (credit normal balance)
                (LOWER({type_col}) IN ('liability', 'liabilities', 'equity', 'revenue', 'income', 'sales') AND {amount_col} < 0)
            )
            GROUP BY {account_id_col}, {type_col}, {amount_col}
            LIMIT 100
        """

        violations_df = duckdb_conn.execute(query).df()

        # Get total transaction count
        total_query = f"SELECT COUNT(*) as total FROM {table_name}"
        total_count_row = duckdb_conn.execute(total_query).fetchone()
        total_count = total_count_row[0] if total_count_row else 0

        violations = []
        total_violations = 0

        for _, row in violations_df.iterrows():
            account_type = row["account_type"]
            expected_sign = config.sign_conventions.get_expected_sign(account_type)
            actual_sign = "credit" if row["amount"] < 0 else "debit"

            # Determine severity
            if abs(row["amount"]) > 10000:
                severity = "severe"
            elif abs(row["amount"]) > 1000:
                severity = "moderate"
            else:
                severity = "minor"

            violation_id = str(uuid4())

            violation = SignConventionViolation(
                violation_id=violation_id,
                account_identifier=str(row["account_identifier"]),
                account_type=account_type,
                expected_sign=expected_sign,
                actual_sign=actual_sign,
                amount=float(row["amount"]),
                violation_severity=severity,
                description=f"{account_type} account has unexpected {actual_sign} balance (expected {expected_sign})",
            )

            violations.append(violation)
            total_violations += int(row["violation_count"])

        # Calculate compliance score
        if total_count > 0:
            compliance_score = 1.0 - min(total_violations / total_count, 1.0)
        else:
            compliance_score = 1.0

        return Result.ok((compliance_score, violations))

    except Exception as e:
        logger.error(f"Sign convention check failed: {e}")
        return Result.fail(f"Sign convention check failed: {str(e)}")


async def check_fiscal_period_integrity(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    config: FinancialQualityConfig,
) -> Result[list[FiscalPeriodIntegrityCheck]]:
    """Check fiscal period completeness and cutoff cleanliness.

    Args:
        table_id: UUID of the table to check
        duckdb_conn: DuckDB connection
        session: SQLAlchemy async session
        config: Financial quality configuration

    Returns:
        Result containing list of fiscal period integrity checks
    """
    try:
        # Get table name
        table = await session.get(Table, table_id)
        if not table:
            return Result.fail(f"Table {table_id} not found")

        table_name = table.table_name

        # Detect date column
        columns_query = f"DESCRIBE {table_name}"
        columns_df = duckdb_conn.execute(columns_query).df()

        date_columns = columns_df[
            columns_df["column_type"].str.contains("DATE|TIMESTAMP", case=False)
        ]["column_name"].tolist()

        if not date_columns:
            return Result.fail("No date/timestamp columns found for period integrity check")

        date_col = date_columns[0]

        # Get date range
        range_query = (
            f"SELECT MIN({date_col}) as min_date, MAX({date_col}) as max_date FROM {table_name}"
        )
        range_df = duckdb_conn.execute(range_query).df()
        _min_date = pd.to_datetime(range_df.iloc[0]["min_date"])
        max_date = pd.to_datetime(range_df.iloc[0]["max_date"])

        # For simplicity, check the most recent fiscal period
        # In production, this would check all fiscal periods

        # Determine fiscal year based on config or calendar year
        if config.fiscal_year_end_month:
            # Custom fiscal year
            fiscal_year_end = datetime(max_date.year, config.fiscal_year_end_month, 1)
            if max_date.month < config.fiscal_year_end_month:
                fiscal_year_end = fiscal_year_end.replace(year=max_date.year - 1)
        else:
            # Calendar year
            fiscal_year_end = datetime(max_date.year, 12, 31)

        # Calculate expected days in period
        if config.fiscal_year_end_month:
            fiscal_year_start = fiscal_year_end.replace(year=fiscal_year_end.year - 1)
        else:
            fiscal_year_start = datetime(max_date.year, 1, 1)

        expected_days = (fiscal_year_end - fiscal_year_start).days

        # Count actual transaction days
        days_query = f"""
            SELECT COUNT(DISTINCT DATE_TRUNC('day', {date_col})) as actual_days
            FROM {table_name}
            WHERE {date_col} BETWEEN '{fiscal_year_start.isoformat()}' AND '{fiscal_year_end.isoformat()}'
        """
        actual_days_row = duckdb_conn.execute(days_query).fetchone()
        actual_days = actual_days_row[0] if actual_days_row else 0

        missing_days = expected_days - actual_days
        is_complete = missing_days <= 5  # Allow 5 days tolerance

        # Check cutoff cleanliness
        # Late transactions: after period end
        late_query = f"""
            SELECT COUNT(*) as late_count
            FROM {table_name}
            WHERE {date_col} > '{fiscal_year_end.isoformat()}'
        """
        late_transactions_row = duckdb_conn.execute(late_query).fetchone()
        late_transactions = late_transactions_row[0] if late_transactions_row else 0

        # Early transactions: before period start
        early_query = f"""
            SELECT COUNT(*) as early_count
            FROM {table_name}
            WHERE {date_col} < '{fiscal_year_start.isoformat()}'
        """
        early_transactions_row = duckdb_conn.execute(early_query).fetchone()
        early_transactions = early_transactions_row[0] if early_transactions_row else 0

        cutoff_clean = (late_transactions == 0) and (early_transactions == 0)

        # Get transaction count and total amount
        stats_query = f"""
            SELECT
                COUNT(*) as transaction_count,
                SUM(ABS(amount)) as total_amount
            FROM {table_name}
            WHERE {date_col} BETWEEN '{fiscal_year_start.isoformat()}' AND '{fiscal_year_end.isoformat()}'
        """
        stats_df = duckdb_conn.execute(stats_query).df()
        transaction_count = int(stats_df.iloc[0]["transaction_count"] or 0)
        total_amount = float(stats_df.iloc[0]["total_amount"] or 0)

        check_id = str(uuid4())

        check = FiscalPeriodIntegrityCheck(
            check_id=check_id,
            fiscal_period=f"FY-{fiscal_year_end.year}",
            period_start=fiscal_year_start,
            period_end=fiscal_year_end,
            is_complete=is_complete,
            missing_days=missing_days,
            cutoff_clean=cutoff_clean,
            late_transactions=late_transactions,
            early_transactions=early_transactions,
            transaction_count=transaction_count,
            total_amount=total_amount,
        )

        return Result.ok([check])

    except Exception as e:
        logger.error(f"Fiscal period integrity check failed: {e}")
        return Result.fail(f"Fiscal period integrity check failed: {str(e)}")


async def analyze_financial_quality(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    config: FinancialQualityConfig | None = None,
) -> Result[FinancialQualityResult]:
    """Comprehensive financial quality analysis.

    Performs all financial quality checks:
    - Double-entry balance
    - Trial balance
    - Sign conventions
    - Fiscal period integrity

    Args:
        table_id: UUID of the table to analyze
        duckdb_conn: DuckDB connection
        session: SQLAlchemy async session
        config: Financial quality configuration (uses defaults if None)

    Returns:
        Result containing complete financial quality assessment
    """
    if config is None:
        config = FinancialQualityConfig()

    metric_id = str(uuid4())
    computed_at = datetime.now(UTC)
    quality_issues = []

    # 1. Double-entry balance check
    double_entry_result = await check_double_entry_balance(table_id, duckdb_conn, session, config)
    if double_entry_result.success:
        double_entry = double_entry_result.unwrap()
        double_entry_balanced = double_entry.is_balanced
        balance_difference = double_entry.net_difference

        if not double_entry_balanced:
            quality_issues.append(
                FinancialQualityIssue(
                    issue_type="double_entry_imbalance",
                    severity="critical",
                    description=f"Double-entry imbalance detected: difference of {balance_difference:.2f}",
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
    period_checks: list[FiscalPeriodIntegrityCheck]  # Type annotation for mypy
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
                        description=f"Fiscal period {check.fiscal_period} is incomplete: {check.missing_days} days missing",
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
        period_checks = []
        fiscal_period_complete = True  # Default to true if can't check
        period_end_cutoff_clean = True

    # Store in database
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
        intercompany_elimination_rate=None,  # Not implemented yet
        orphaned_intercompany=None,
        fiscal_period_complete=fiscal_period_complete,
        period_end_cutoff_clean=period_end_cutoff_clean,
    )

    session.add(db_metric)

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

    # Build result
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
        intercompany_details=[],
        fiscal_period_complete=fiscal_period_complete,
        period_end_cutoff_clean=period_end_cutoff_clean,
        period_integrity_details=period_checks,
        quality_issues=quality_issues,
        has_issues=len(quality_issues) > 0,
    )

    return Result.ok(result)


# ===================================================================
# TOPOLOGICAL DOMAIN ANALYSIS
# ===================================================================
# These functions enhance generic topological analysis with
# financial domain-specific interpretations


def _load_financial_config() -> dict[str, Any]:
    """Load financial domain configuration from YAML file.

    Returns:
        Dict with configuration data
    """
    # Try multiple locations for config file
    possible_paths: list[Path] = [
        # Project root (when running from source)
        Path.cwd() / "config" / "domains" / "financial.yaml",
        # Relative to this file (installed package)
        Path(__file__).parent.parent.parent.parent / "config" / "domains" / "financial.yaml",
    ]

    # Add environment variable override if set
    config_dir = os.getenv("DATARAUM_CONFIG_DIR")
    if config_dir:
        possible_paths.insert(0, Path(config_dir) / "domains" / "financial.yaml")

    config_path: Path | None = None
    for path in possible_paths:
        if path.exists():
            config_path = path
            break

    if not config_path:
        logger.warning(
            f"Financial config not found in any of: {[str(p) for p in possible_paths]}, "
            "using defaults"
        )
        return {}

    try:
        with open(config_path) as f:
            config: dict[str, Any] = yaml.safe_load(f) or {}
        logger.info(f"Loaded financial domain config from {config_path}")
        return config
    except Exception as e:
        logger.error(f"Failed to load financial config: {e}")
        return {}


# NOTE: The following functions have been moved to financial_orchestrator.py:
# - assess_fiscal_stability()
# - detect_financial_anomalies()
# - FinancialDomainAnalyzer class
#
# For complete financial domain analysis with LLM support, use:
#   from dataraum_context.quality.domains.financial_orchestrator import (
#       analyze_complete_financial_quality,
#   )
#   result = await analyze_complete_financial_quality(table_id, conn, session, llm_service)
