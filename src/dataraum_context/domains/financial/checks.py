"""Financial domain quality checks.

Individual check functions for financial accounting quality:
- Double-entry balance validation
- Trial balance checks (Assets = Liabilities + Equity)
- Sign convention validation
- Fiscal period integrity
"""

import logging
from datetime import datetime
from uuid import uuid4

import duckdb
import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models.base import Result
from dataraum_context.domains.financial.models import (
    DoubleEntryResult,
    FinancialQualityConfig,
    FiscalPeriodIntegrityCheck,
    SignConventionViolation,
    TrialBalanceResult,
)
from dataraum_context.storage import Table

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
        # Get table from metadata
        table = await session.get(Table, table_id)
        if not table:
            return Result.fail(f"Table {table_id} not found")
        if not table.duckdb_path:
            return Result.fail(f"Table has no DuckDB path: {table_id}")

        table_name = table.duckdb_path

        # Detect columns
        columns_query = f"DESCRIBE {table_name}"
        columns_df = duckdb_conn.execute(columns_query).df()
        column_names = columns_df["column_name"].str.lower().tolist()

        has_debit_credit = "debit" in column_names and "credit" in column_names
        has_amount = "amount" in column_names
        has_account_type = any(
            col in column_names for col in ["account_type", "account_category", "type"]
        )

        if has_debit_credit:
            query = f"""
                SELECT
                    SUM(debit) as total_debits,
                    SUM(credit) as total_credits,
                    COUNT(DISTINCT CASE WHEN debit > 0 THEN 1 END) as debit_count,
                    COUNT(DISTINCT CASE WHEN credit > 0 THEN 1 END) as credit_count
                FROM {table_name}
            """
        elif has_amount and has_account_type:
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

        result = DoubleEntryResult(
            check_id=str(uuid4()),
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
        table = await session.get(Table, table_id)
        if not table:
            return Result.fail(f"Table {table_id} not found")
        if not table.duckdb_path:
            return Result.fail(f"Table has no DuckDB path: {table_id}")

        table_name = table.duckdb_path

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

        result = TrialBalanceResult(
            check_id=str(uuid4()),
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

    Args:
        table_id: UUID of the table to check
        duckdb_conn: DuckDB connection
        session: SQLAlchemy async session
        config: Financial quality configuration

    Returns:
        Result containing (compliance_score, violations)
    """
    try:
        table = await session.get(Table, table_id)
        if not table:
            return Result.fail(f"Table {table_id} not found")
        if not table.duckdb_path:
            return Result.fail(f"Table has no DuckDB path: {table_id}")

        table_name = table.duckdb_path

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

        account_id_col = None
        for col in ["account_name", "account_code", "account", "name", "code"]:
            if col in column_names:
                account_id_col = col
                break

        if not account_id_col:
            account_id_col = type_col

        query = f"""
            SELECT
                {account_id_col} as account_identifier,
                {type_col} as account_type,
                {amount_col} as amount,
                COUNT(*) as violation_count
            FROM {table_name}
            WHERE (
                (LOWER({type_col}) IN ('asset', 'assets', 'expense', 'expenses') AND {amount_col} < 0)
                OR
                (LOWER({type_col}) IN ('liability', 'liabilities', 'equity', 'revenue', 'income', 'sales') AND {amount_col} < 0)
            )
            GROUP BY {account_id_col}, {type_col}, {amount_col}
            LIMIT 100
        """

        violations_df = duckdb_conn.execute(query).df()

        total_query = f"SELECT COUNT(*) as total FROM {table_name}"
        total_count_row = duckdb_conn.execute(total_query).fetchone()
        total_count = total_count_row[0] if total_count_row else 0

        violations = []
        total_violations = 0

        for _, row in violations_df.iterrows():
            account_type = row["account_type"]
            expected_sign = config.sign_conventions.get_expected_sign(account_type)
            actual_sign = "credit" if row["amount"] < 0 else "debit"

            if abs(row["amount"]) > 10000:
                severity = "severe"
            elif abs(row["amount"]) > 1000:
                severity = "moderate"
            else:
                severity = "minor"

            violation = SignConventionViolation(
                violation_id=str(uuid4()),
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
        table = await session.get(Table, table_id)
        if not table:
            return Result.fail(f"Table {table_id} not found")
        if not table.duckdb_path:
            return Result.fail(f"Table has no DuckDB path: {table_id}")

        table_name = table.duckdb_path

        columns_query = f"DESCRIBE {table_name}"
        columns_df = duckdb_conn.execute(columns_query).df()

        date_columns = columns_df[
            columns_df["column_type"].str.contains("DATE|TIMESTAMP", case=False)
        ]["column_name"].tolist()

        if not date_columns:
            return Result.fail("No date/timestamp columns found for period integrity check")

        date_col = date_columns[0]
        # Quote column name to handle spaces and special characters
        quoted_date_col = f'"{date_col}"'

        range_query = f"SELECT MIN({quoted_date_col}) as min_date, MAX({quoted_date_col}) as max_date FROM {table_name}"
        range_df = duckdb_conn.execute(range_query).df()
        _min_date = pd.to_datetime(range_df.iloc[0]["min_date"])
        max_date = pd.to_datetime(range_df.iloc[0]["max_date"])

        if config.fiscal_year_end_month:
            fiscal_year_end = datetime(max_date.year, config.fiscal_year_end_month, 1)
            if max_date.month < config.fiscal_year_end_month:
                fiscal_year_end = fiscal_year_end.replace(year=max_date.year - 1)
        else:
            fiscal_year_end = datetime(max_date.year, 12, 31)

        if config.fiscal_year_end_month:
            fiscal_year_start = fiscal_year_end.replace(year=fiscal_year_end.year - 1)
        else:
            fiscal_year_start = datetime(max_date.year, 1, 1)

        expected_days = (fiscal_year_end - fiscal_year_start).days

        days_query = f"""
            SELECT COUNT(DISTINCT DATE_TRUNC('day', {quoted_date_col})) as actual_days
            FROM {table_name}
            WHERE {quoted_date_col} BETWEEN '{fiscal_year_start.isoformat()}' AND '{fiscal_year_end.isoformat()}'
        """
        actual_days_row = duckdb_conn.execute(days_query).fetchone()
        actual_days = actual_days_row[0] if actual_days_row else 0

        missing_days = expected_days - actual_days
        is_complete = missing_days <= 5

        late_query = f"""
            SELECT COUNT(*) as late_count
            FROM {table_name}
            WHERE {quoted_date_col} > '{fiscal_year_end.isoformat()}'
        """
        late_transactions_row = duckdb_conn.execute(late_query).fetchone()
        late_transactions = late_transactions_row[0] if late_transactions_row else 0

        early_query = f"""
            SELECT COUNT(*) as early_count
            FROM {table_name}
            WHERE {quoted_date_col} < '{fiscal_year_start.isoformat()}'
        """
        early_transactions_row = duckdb_conn.execute(early_query).fetchone()
        early_transactions = early_transactions_row[0] if early_transactions_row else 0

        cutoff_clean = (late_transactions == 0) and (early_transactions == 0)

        stats_query = f"""
            SELECT
                COUNT(*) as transaction_count,
                SUM(ABS(amount)) as total_amount
            FROM {table_name}
            WHERE {quoted_date_col} BETWEEN '{fiscal_year_start.isoformat()}' AND '{fiscal_year_end.isoformat()}'
        """
        stats_df = duckdb_conn.execute(stats_query).df()
        transaction_count = int(stats_df.iloc[0]["transaction_count"] or 0)
        total_amount = float(stats_df.iloc[0]["total_amount"] or 0)

        check = FiscalPeriodIntegrityCheck(
            check_id=str(uuid4()),
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
