"""Tests for financial domain quality checks.

Tests financial accounting-specific quality rules:
- Double-entry balance validation
- Trial balance checks
- Sign convention validation
- Fiscal period integrity
"""

from datetime import datetime, timedelta
from uuid import uuid4

import duckdb
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from dataraum_context.domains.financial import (
    analyze_financial_quality,
    check_double_entry_balance,
    check_fiscal_period_integrity,
    check_sign_conventions,
    check_trial_balance,
)
from dataraum_context.domains.financial.models import FinancialQualityConfig
from dataraum_context.storage import Source, Table, init_database


@pytest.fixture
async def db_session():
    """Create an in-memory SQLite async session for testing."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)

    # Use init_database to properly register all db_models
    await init_database(engine)

    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        # Create a test source and table
        source = Source(
            source_id=str(uuid4()),
            name="test_financial_source",
            source_type="csv",
            connection_config={},
        )
        session.add(source)
        await session.commit()

        table = Table(
            table_id=str(uuid4()),
            source_id=source.source_id,
            table_name="test_ledger",
            duckdb_path="test_ledger",  # Must match the DuckDB table name
            layer="raw",
            row_count=0,
        )
        session.add(table)
        await session.commit()

        yield session, table.table_id


@pytest.fixture
def duckdb_conn():
    """Create an in-memory DuckDB connection."""
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


@pytest.mark.asyncio
async def test_check_double_entry_balanced(db_session, duckdb_conn):
    """Test double-entry balance check with balanced ledger."""
    session, table_id = db_session

    # Create balanced ledger with explicit debit/credit columns
    duckdb_conn.execute("""
        CREATE TABLE test_ledger (
            transaction_id INTEGER,
            account VARCHAR,
            debit DOUBLE,
            credit DOUBLE
        )
    """)

    # Insert balanced transactions (debits = credits)
    duckdb_conn.execute("""
        INSERT INTO test_ledger VALUES
            (1, 'Cash', 1000, 0),
            (1, 'Revenue', 0, 1000),
            (2, 'Expense', 500, 0),
            (2, 'Cash', 0, 500)
    """)

    config = FinancialQualityConfig(double_entry_tolerance=0.01)
    result = await check_double_entry_balance(str(table_id), duckdb_conn, session, config)

    assert result.success
    assert result.value.is_balanced
    assert result.value.total_debits == 1500
    assert result.value.total_credits == 1500
    assert result.value.net_difference == 0


@pytest.mark.asyncio
async def test_check_double_entry_imbalanced(db_session, duckdb_conn):
    """Test double-entry balance check with imbalanced ledger."""
    session, table_id = db_session

    # Create imbalanced ledger
    duckdb_conn.execute("""
        CREATE TABLE test_ledger (
            transaction_id INTEGER,
            account VARCHAR,
            debit DOUBLE,
            credit DOUBLE
        )
    """)

    # Insert imbalanced transactions
    duckdb_conn.execute("""
        INSERT INTO test_ledger VALUES
            (1, 'Cash', 1000, 0),
            (1, 'Revenue', 0, 900)
    """)

    config = FinancialQualityConfig(double_entry_tolerance=0.01)
    result = await check_double_entry_balance(str(table_id), duckdb_conn, session, config)

    assert result.success
    assert not result.value.is_balanced
    assert result.value.net_difference == 100


@pytest.mark.asyncio
async def test_check_double_entry_with_account_types(db_session, duckdb_conn):
    """Test double-entry check using account types instead of debit/credit columns."""
    session, table_id = db_session

    # Create ledger with account_type and amount columns
    duckdb_conn.execute("""
        CREATE TABLE test_ledger (
            transaction_id INTEGER,
            account_type VARCHAR,
            account VARCHAR,
            amount DOUBLE
        )
    """)

    # Insert transactions with proper account types
    # Transaction 1: Cash (asset) increases by 1000, Sales (revenue) 1000
    # Transaction 2: Rent (expense) 500, Liability (accounts payable) 500
    duckdb_conn.execute("""
        INSERT INTO test_ledger VALUES
            (1, 'Asset', 'Cash', 1000),
            (1, 'Revenue', 'Sales', 1000),
            (2, 'Expense', 'Rent', 500),
            (2, 'Liability', 'Accounts Payable', 500)
    """)

    config = FinancialQualityConfig(double_entry_tolerance=0.01)
    result = await check_double_entry_balance(str(table_id), duckdb_conn, session, config)

    assert result.success
    assert result.value.is_balanced
    assert result.value.total_debits == 1500  # Asset + Expense
    assert result.value.total_credits == 1500  # Revenue + Liability


@pytest.mark.asyncio
async def test_check_trial_balance_valid(db_session, duckdb_conn):
    """Test trial balance with valid accounting equation."""
    session, table_id = db_session

    # Create balance sheet
    duckdb_conn.execute("""
        CREATE TABLE test_ledger (
            account_type VARCHAR,
            account VARCHAR,
            amount DOUBLE
        )
    """)

    # Insert accounts following Assets = Liabilities + Equity
    duckdb_conn.execute("""
        INSERT INTO test_ledger VALUES
            ('Asset', 'Cash', 50000),
            ('Asset', 'Inventory', 30000),
            ('Liability', 'Accounts Payable', 20000),
            ('Equity', 'Retained Earnings', 60000)
    """)

    config = FinancialQualityConfig(trial_balance_tolerance=0.01)
    result = await check_trial_balance(str(table_id), duckdb_conn, session, config)

    assert result.success
    assert result.value.equation_holds
    assert result.value.assets == 80000
    assert result.value.liabilities == 20000
    assert result.value.equity == 60000
    assert result.value.equation_difference == 0


@pytest.mark.asyncio
async def test_check_trial_balance_invalid(db_session, duckdb_conn):
    """Test trial balance with invalid accounting equation."""
    session, table_id = db_session

    duckdb_conn.execute("""
        CREATE TABLE test_ledger (
            account_type VARCHAR,
            account VARCHAR,
            amount DOUBLE
        )
    """)

    # Insert imbalanced accounts
    duckdb_conn.execute("""
        INSERT INTO test_ledger VALUES
            ('Asset', 'Cash', 100000),
            ('Liability', 'Accounts Payable', 20000),
            ('Equity', 'Retained Earnings', 50000)
    """)

    config = FinancialQualityConfig(trial_balance_tolerance=0.01)
    result = await check_trial_balance(str(table_id), duckdb_conn, session, config)

    assert result.success
    assert not result.value.equation_holds
    assert result.value.equation_difference == 30000  # 100000 - (20000 + 50000)


@pytest.mark.asyncio
async def test_check_sign_conventions_compliant(db_session, duckdb_conn):
    """Test sign convention check with compliant accounts."""
    session, table_id = db_session

    duckdb_conn.execute("""
        CREATE TABLE test_ledger (
            account_type VARCHAR,
            account VARCHAR,
            amount DOUBLE
        )
    """)

    # Insert accounts with correct signs
    duckdb_conn.execute("""
        INSERT INTO test_ledger VALUES
            ('Asset', 'Cash', 1000),
            ('Expense', 'Rent', 500),
            ('Revenue', 'Sales', 2000),
            ('Liability', 'Loan', 5000),
            ('Equity', 'Capital', 3000)
    """)

    config = FinancialQualityConfig()
    result = await check_sign_conventions(str(table_id), duckdb_conn, session, config)

    assert result.success
    compliance, violations = result.value
    assert compliance == 1.0  # 100% compliant
    assert len(violations) == 0


@pytest.mark.asyncio
async def test_check_sign_conventions_violations(db_session, duckdb_conn):
    """Test sign convention check with violations."""
    session, table_id = db_session

    duckdb_conn.execute("""
        CREATE TABLE test_ledger (
            account_type VARCHAR,
            account VARCHAR,
            amount DOUBLE
        )
    """)

    # Insert accounts with incorrect signs
    duckdb_conn.execute("""
        INSERT INTO test_ledger VALUES
            ('Asset', 'Cash', 1000),
            ('Asset', 'Bad Asset', -500),
            ('Revenue', 'Sales', 2000),
            ('Revenue', 'Bad Revenue', -300),
            ('Expense', 'Rent', 400)
    """)

    config = FinancialQualityConfig()
    result = await check_sign_conventions(str(table_id), duckdb_conn, session, config)

    assert result.success
    compliance, violations = result.value
    assert compliance < 1.0  # Some violations
    assert len(violations) == 2  # Two negative amounts where positive expected


@pytest.mark.asyncio
async def test_check_fiscal_period_integrity_complete(db_session, duckdb_conn):
    """Test fiscal period integrity with complete period."""
    session, table_id = db_session

    duckdb_conn.execute("""
        CREATE TABLE test_ledger (
            transaction_date TIMESTAMP,
            account VARCHAR,
            amount DOUBLE
        )
    """)

    # Insert transactions spanning a full year
    base_date = datetime(2024, 1, 1)
    for i in range(360):  # Most days covered
        date = base_date + timedelta(days=i)
        duckdb_conn.execute(f"""
            INSERT INTO test_ledger VALUES
                (timestamp '{date.isoformat()}', 'Account', 100)
        """)

    config = FinancialQualityConfig(fiscal_year_end_month=None)  # Use calendar year
    result = await check_fiscal_period_integrity(str(table_id), duckdb_conn, session, config)

    assert result.success
    checks = result.value
    assert len(checks) > 0
    # With 360 days covered, we should have <= 10 days missing (allowing for weekends/holidays)
    assert checks[0].missing_days <= 10


@pytest.mark.asyncio
async def test_check_fiscal_period_integrity_with_cutoff_violations(db_session, duckdb_conn):
    """Test fiscal period integrity with cutoff violations."""
    session, table_id = db_session

    duckdb_conn.execute("""
        CREATE TABLE test_ledger (
            transaction_date TIMESTAMP,
            account VARCHAR,
            amount DOUBLE
        )
    """)

    # Insert transactions with some outside the period
    base_date = datetime(2024, 1, 1)

    # Normal transactions
    for i in range(100):
        date = base_date + timedelta(days=i)
        duckdb_conn.execute(f"""
            INSERT INTO test_ledger VALUES
                (timestamp '{date.isoformat()}', 'Account', 100)
        """)

    # Early transaction (before period)
    early_date = datetime(2023, 12, 1)
    duckdb_conn.execute(f"""
        INSERT INTO test_ledger VALUES
            (timestamp '{early_date.isoformat()}', 'Account', 50)
    """)

    # Late transaction (after period)
    late_date = datetime(2025, 1, 15)
    duckdb_conn.execute(f"""
        INSERT INTO test_ledger VALUES
            (timestamp '{late_date.isoformat()}', 'Account', 75)
    """)

    config = FinancialQualityConfig(fiscal_year_end_month=12)
    result = await check_fiscal_period_integrity(str(table_id), duckdb_conn, session, config)

    assert result.success
    checks = result.value
    assert len(checks) > 0
    assert checks[0].early_transactions > 0 or checks[0].late_transactions > 0
    assert not checks[0].cutoff_clean


@pytest.mark.asyncio
async def test_analyze_financial_quality_complete(db_session, duckdb_conn):
    """Test complete financial quality analysis."""
    session, table_id = db_session

    # Create a complete financial ledger
    duckdb_conn.execute("""
        CREATE TABLE test_ledger (
            transaction_id INTEGER,
            transaction_date TIMESTAMP,
            account_type VARCHAR,
            account VARCHAR,
            amount DOUBLE
        )
    """)

    # Insert balanced transactions
    base_date = datetime(2024, 1, 1)
    duckdb_conn.execute(f"""
        INSERT INTO test_ledger VALUES
            (1, timestamp '{base_date.isoformat()}', 'Asset', 'Cash', 10000),
            (1, timestamp '{base_date.isoformat()}', 'Equity', 'Capital', 10000),
            (2, timestamp '{(base_date + timedelta(days=1)).isoformat()}', 'Expense', 'Rent', 1000),
            (2, timestamp '{(base_date + timedelta(days=1)).isoformat()}', 'Liability', 'Accounts Payable', 1000),
            (3, timestamp '{(base_date + timedelta(days=2)).isoformat()}', 'Asset', 'Receivables', 5000),
            (3, timestamp '{(base_date + timedelta(days=2)).isoformat()}', 'Revenue', 'Sales', 5000)
    """)

    config = FinancialQualityConfig()
    result = await analyze_financial_quality(str(table_id), duckdb_conn, session, config)

    assert result.success

    financial_quality = result.value
    assert financial_quality.metric_id is not None
    assert financial_quality.double_entry_balanced


@pytest.mark.asyncio
async def test_analyze_financial_quality_with_issues(db_session, duckdb_conn):
    """Test financial quality analysis with quality issues."""
    session, table_id = db_session

    # Create ledger with issues
    duckdb_conn.execute("""
        CREATE TABLE test_ledger (
            transaction_date TIMESTAMP,
            account_type VARCHAR,
            account VARCHAR,
            amount DOUBLE
        )
    """)

    # Imbalanced transactions with sign violations
    base_date = datetime(2024, 1, 1)
    duckdb_conn.execute(f"""
        INSERT INTO test_ledger VALUES
            (timestamp '{base_date.isoformat()}', 'Asset', 'Cash', -5000),
            (timestamp '{base_date.isoformat()}', 'Revenue', 'Sales', -3000)
    """)

    config = FinancialQualityConfig()
    result = await analyze_financial_quality(str(table_id), duckdb_conn, session, config)

    assert result.success

    financial_quality = result.value
    assert financial_quality.has_issues
    assert len(financial_quality.quality_issues) > 0
    assert financial_quality.sign_convention_compliance < 1.0


@pytest.mark.asyncio
async def test_pydantic_financial_quality_result():
    """Test FinancialQualityResult Pydantic model validation."""
    from dataraum_context.domains.financial.models import (
        DoubleEntryResult,
        FinancialQualityResult,
    )

    double_entry = DoubleEntryResult(
        check_id=str(uuid4()),
        total_debits=1000.0,
        total_credits=1000.0,
        net_difference=0.0,
        is_balanced=True,
        debit_account_count=5,
        credit_account_count=5,
    )

    result = FinancialQualityResult(
        metric_id=str(uuid4()),
        table_id=str(uuid4()),
        computed_at=datetime.now(),
        double_entry_balanced=True,
        balance_difference=0.0,
        balance_tolerance=0.01,
        double_entry_details=double_entry,
        trial_balance_check=True,
        sign_convention_compliance=1.0,
        fiscal_period_complete=True,
        period_end_cutoff_clean=True,
        has_issues=False,
    )

    assert result.double_entry_balanced
    assert result.double_entry_details.is_balanced


@pytest.mark.asyncio
async def test_pydantic_sign_convention_config():
    """Test SignConventionConfig model."""
    from dataraum_context.domains.financial.models import SignConventionConfig

    config = SignConventionConfig()

    assert config.get_expected_sign("Asset") == "debit"
    assert config.get_expected_sign("Liability") == "credit"
    assert config.get_expected_sign("Revenue") == "credit"
    assert config.get_expected_sign("Expense") == "debit"
    assert config.get_expected_sign("Equity") == "credit"

    # Test case insensitivity
    assert config.get_expected_sign("asset") == "debit"
    assert config.get_expected_sign("REVENUE") == "credit"
