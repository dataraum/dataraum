"""Tests for the validation agent."""

from unittest.mock import MagicMock, patch

import pytest

from dataraum.analysis.validation.agent import ValidationAgent
from dataraum.analysis.validation.models import (
    ValidationSeverity,
    ValidationSpec,
    ValidationStatus,
)
from dataraum.core.models.base import Result


@pytest.fixture
def mock_llm_config():
    """Create a mock LLM config."""
    config = MagicMock()
    config.features.validation = MagicMock()
    config.features.validation.enabled = True
    config.features.validation.model_tier = "fast"
    return config


@pytest.fixture
def mock_provider():
    """Create a mock LLM provider."""
    provider = MagicMock()
    provider.get_model_for_tier = MagicMock(return_value="claude-3-haiku")
    provider.converse = MagicMock()
    return provider


def _make_tool_response(tool_input: dict, tool_name: str = "generate_validation_sql"):
    """Create a mock LLM response with tool calls."""
    tool_call = MagicMock()
    tool_call.name = tool_name
    tool_call.input = tool_input

    response = MagicMock()
    response.tool_calls = [tool_call]
    response.content = None
    return response


@pytest.fixture
def mock_prompt_renderer():
    """Create a mock prompt renderer that returns valid prompts."""
    renderer = MagicMock()
    # Configure render_split to return (system_prompt, user_prompt, temperature)
    renderer.render_split.return_value = (
        "You are an SQL expert.",
        "Generate SQL for this validation.",
        0.0,
    )
    return renderer


@pytest.fixture
def mock_cache():
    """Create a mock LLM cache."""
    return MagicMock()


@pytest.fixture
def validation_agent(mock_llm_config, mock_provider, mock_prompt_renderer, mock_cache):
    """Create a validation agent with mocked dependencies."""
    return ValidationAgent(
        config=mock_llm_config,
        provider=mock_provider,
        prompt_renderer=mock_prompt_renderer,
        cache=mock_cache,
    )


@pytest.fixture
def table_with_data(session, duckdb_conn):
    """Create a test table with data in both SQLite and DuckDB."""
    from dataraum.analysis.semantic.db_models import (
        SemanticAnnotation as SemanticAnnotationDB,
    )
    from dataraum.storage import Column, Source, Table

    # Create source and table in SQLite
    source = Source(name="test_source", source_type="csv")
    session.add(source)
    session.flush()

    table = Table(
        source_id=source.source_id,
        table_name="journal_entries",
        layer="typed",
        row_count=4,
        duckdb_path="typed_journal_entries",
    )
    session.add(table)
    session.flush()

    # Create columns
    col_debit = Column(
        table_id=table.table_id,
        column_name="debit",
        column_position=0,
        raw_type="DECIMAL",
        resolved_type="DECIMAL(18,2)",
    )
    col_credit = Column(
        table_id=table.table_id,
        column_name="credit",
        column_position=1,
        raw_type="DECIMAL",
        resolved_type="DECIMAL(18,2)",
    )
    session.add_all([col_debit, col_credit])
    session.flush()

    # Add semantic annotations
    ann_debit = SemanticAnnotationDB(
        column_id=col_debit.column_id,
        semantic_role="measure",
        entity_type="debit",
    )
    ann_credit = SemanticAnnotationDB(
        column_id=col_credit.column_id,
        semantic_role="measure",
        entity_type="credit",
    )
    session.add_all([ann_debit, ann_credit])
    session.commit()

    # Create matching table in DuckDB with balanced data
    duckdb_conn.execute("""
        CREATE TABLE typed_journal_entries (
            debit DECIMAL(18,2),
            credit DECIMAL(18,2)
        )
    """)
    duckdb_conn.execute("""
        INSERT INTO typed_journal_entries VALUES
            (100.00, 0.00),
            (50.00, 0.00),
            (0.00, 100.00),
            (0.00, 50.00)
    """)

    return table


class TestValidationAgentEvaluateResult:
    """Tests for the _evaluate_result method."""

    def test_evaluate_balance_check_passed(self, validation_agent):
        """Test balance check evaluation when balanced."""
        spec = ValidationSpec(
            validation_id="test",
            name="Test",
            description="Test",
            category="test",
            check_type="balance",
            parameters={"tolerance": 0.01},
        )

        result_rows = [{"total_debits": 150.00, "total_credits": 150.00, "difference": 0.00}]

        passed, message, details = validation_agent._evaluate_result(spec, result_rows, 1)

        assert passed is True
        assert "diff=0.00" in message

    def test_evaluate_balance_check_failed(self, validation_agent):
        """Test balance check evaluation when not balanced."""
        spec = ValidationSpec(
            validation_id="test",
            name="Test",
            description="Test",
            category="test",
            check_type="balance",
            parameters={"tolerance": 0.01},
        )

        result_rows = [{"total_debits": 150.00, "total_credits": 100.00, "difference": 50.00}]

        passed, message, details = validation_agent._evaluate_result(spec, result_rows, 1)

        assert passed is False
        assert details["difference"] == 50.0

    def test_evaluate_constraint_check_no_violations(self, validation_agent):
        """Test constraint check with no violations."""
        spec = ValidationSpec(
            validation_id="test",
            name="Test",
            description="Test",
            category="test",
            check_type="constraint",
        )

        passed, message, details = validation_agent._evaluate_result(spec, [], 0)

        assert passed is True
        assert "No constraint violations" in message

    def test_evaluate_constraint_check_with_violations(self, validation_agent):
        """Test constraint check with violations."""
        spec = ValidationSpec(
            validation_id="test",
            name="Test",
            description="Test",
            category="test",
            check_type="constraint",
        )

        result_rows = [{"id": 1, "violation": "negative amount"}]

        passed, message, details = validation_agent._evaluate_result(spec, result_rows, 1)

        assert passed is False
        assert "1 constraint violations" in message

    def test_evaluate_comparison_check_equation_holds(self, validation_agent):
        """Test comparison check with equation_holds column."""
        spec = ValidationSpec(
            validation_id="test",
            name="Test",
            description="Test",
            category="test",
            check_type="comparison",
        )

        result_rows = [{"assets": 1000, "liabilities": 600, "equity": 400, "equation_holds": True}]

        passed, message, details = validation_agent._evaluate_result(spec, result_rows, 1)

        assert passed is True
        assert "passed" in message

    def test_evaluate_comparison_check_equation_fails(self, validation_agent):
        """Test comparison check when equation doesn't hold."""
        spec = ValidationSpec(
            validation_id="test",
            name="Test",
            description="Test",
            category="test",
            check_type="comparison",
        )

        result_rows = [{"assets": 1000, "liabilities": 600, "equity": 300, "equation_holds": False}]

        passed, message, details = validation_agent._evaluate_result(spec, result_rows, 1)

        assert passed is False

    def test_evaluate_aggregate_check(self, validation_agent):
        """Test aggregate check evaluation."""
        spec = ValidationSpec(
            validation_id="test",
            name="Test",
            description="Test",
            category="test",
            check_type="aggregate",
        )

        result_rows = [{"min_date": "2024-01-01", "max_date": "2024-12-31", "total_records": 1000}]

        passed, message, details = validation_agent._evaluate_result(spec, result_rows, 1)

        assert passed is True
        assert "Aggregate check completed" in message


class TestValidationAgentGenerateSQL:
    """Tests for SQL generation via LLM."""

    def test_generate_sql_success(self, validation_agent, mock_provider):
        """Test successful SQL generation."""
        spec = ValidationSpec(
            validation_id="test_check",
            name="Test Check",
            description="A test validation",
            category="test",
            check_type="balance",
            sql_hints="Sum debits and credits",
        )

        schema = {
            "table_name": "transactions",
            "duckdb_path": "typed_transactions",
            "columns": [
                {"column_name": "debit", "data_type": "DECIMAL"},
                {"column_name": "credit", "data_type": "DECIMAL"},
            ],
        }

        # Mock LLM response with tool call
        tool_input = {
            "sql": "SELECT SUM(debit) as total_debits, SUM(credit) as total_credits FROM typed_transactions",
            "explanation": "Sums debit and credit columns",
            "columns_used": ["debit", "credit"],
            "tables_used": ["typed_transactions"],
            "can_validate": True,
            "skip_reason": None,
        }
        mock_response = _make_tool_response(tool_input)
        mock_provider.converse.return_value = Result.ok(mock_response)

        result = validation_agent._generate_sql(spec, schema)

        assert result.success
        generated = result.value
        assert "SELECT" in generated.sql_query
        assert generated.columns_used == ["debit", "credit"]
        assert generated.is_valid is True

    def test_generate_sql_cannot_validate(self, validation_agent, mock_provider):
        """Test when LLM indicates validation cannot be performed."""
        spec = ValidationSpec(
            validation_id="test_check",
            name="Test Check",
            description="Check debit/credit balance",
            category="financial",
            check_type="balance",
        )

        schema = {
            "table_name": "customers",
            "duckdb_path": "typed_customers",
            "columns": [
                {"column_name": "customer_id", "data_type": "VARCHAR"},
                {"column_name": "name", "data_type": "VARCHAR"},
            ],
        }

        # Mock LLM response indicating cannot validate
        tool_input = {
            "sql": None,
            "explanation": "No debit/credit columns found",
            "columns_used": [],
            "tables_used": [],
            "can_validate": False,
            "skip_reason": "Missing required columns: debit, credit",
        }
        mock_response = _make_tool_response(tool_input)
        mock_provider.converse.return_value = Result.ok(mock_response)

        result = validation_agent._generate_sql(spec, schema)

        assert result.success
        generated = result.value
        assert generated.is_valid is False
        assert "Missing required columns" in generated.validation_error

    def test_generate_sql_llm_error(self, validation_agent, mock_provider):
        """Test handling of LLM errors."""
        spec = ValidationSpec(
            validation_id="test",
            name="Test",
            description="Test",
            category="test",
            check_type="balance",
        )

        schema = {"table_name": "test", "duckdb_path": "test", "columns": []}

        mock_provider.converse.return_value = Result.fail("API error")

        result = validation_agent._generate_sql(spec, schema)

        assert not result.success
        assert "API error" in result.error

    def test_generate_sql_disabled_feature(self, validation_agent):
        """Test when validation feature is disabled."""
        validation_agent.config.features.validation.enabled = False

        spec = ValidationSpec(
            validation_id="test",
            name="Test",
            description="Test",
            category="test",
            check_type="balance",
        )

        schema = {
            "table_name": "test",
            "duckdb_path": "test_path",
            "columns": [],
        }

        result = validation_agent._generate_sql(spec, schema)

        assert not result.success
        assert "disabled" in result.error


class TestValidationAgentRunValidations:
    """Tests for running full validation flows (multi-table)."""

    def test_run_validations_tables_not_found(self, session, duckdb_conn, validation_agent):
        """Test running validations on nonexistent tables."""
        result = validation_agent.run_validations(
            session=session,
            duckdb_conn=duckdb_conn,
            table_ids=["nonexistent-id"],
        )

        assert not result.success
        assert "not found" in result.error.lower() or "No tables" in result.error

    def test_run_validations_no_specs(
        self, session, duckdb_conn, validation_agent, table_with_data
    ):
        """Test running validations with no matching specs."""
        table = table_with_data

        # Patch to return empty specs
        with patch("dataraum.analysis.validation.agent.load_all_validation_specs") as mock_load:
            mock_load.return_value = {}

            result = validation_agent.run_validations(
                session=session,
                duckdb_conn=duckdb_conn,
                table_ids=[table.table_id],
            )

        assert result.success
        run_result = result.value
        assert run_result.total_checks == 0

    def test_run_single_validation_success(
        self, session, duckdb_conn, validation_agent, mock_provider, table_with_data
    ):
        """Test running a single validation that passes."""
        table = table_with_data

        spec = ValidationSpec(
            validation_id="balance_check",
            name="Balance Check",
            description="Check debit equals credit",
            category="financial",
            check_type="balance",
            severity=ValidationSeverity.CRITICAL,
            parameters={"tolerance": 0.01},
        )

        # Get multi-table schema
        from dataraum.analysis.validation.resolver import (
            get_multi_table_schema_for_llm,
        )

        schema = get_multi_table_schema_for_llm(session, [table.table_id])

        # Mock LLM to return valid SQL with tool call
        tool_input = {
            "sql": "SELECT SUM(debit) as total_debits, SUM(credit) as total_credits, ABS(SUM(debit) - SUM(credit)) as difference FROM typed_journal_entries",
            "explanation": "Sums and compares debits and credits",
            "columns_used": ["journal_entries.debit", "journal_entries.credit"],
            "tables_used": ["journal_entries"],
            "can_validate": True,
            "skip_reason": None,
        }
        mock_response = _make_tool_response(tool_input)
        mock_provider.converse.return_value = Result.ok(mock_response)

        result = validation_agent._run_single_validation(
            duckdb_conn=duckdb_conn,
            table_ids=[table.table_id],
            spec=spec,
            schema=schema,
        )

        assert result.status == ValidationStatus.PASSED
        assert result.passed is True
        assert result.sql_used is not None

    def test_run_single_validation_skipped(
        self, session, duckdb_conn, validation_agent, mock_provider, table_with_data
    ):
        """Test running a validation that gets skipped."""
        table = table_with_data

        spec = ValidationSpec(
            validation_id="missing_cols_check",
            name="Missing Columns Check",
            description="Check that requires columns we don't have",
            category="test",
            check_type="balance",
        )

        from dataraum.analysis.validation.resolver import (
            get_multi_table_schema_for_llm,
        )

        schema = get_multi_table_schema_for_llm(session, [table.table_id])

        # Mock LLM to indicate cannot validate
        tool_input = {
            "sql": None,
            "explanation": "Required columns not found",
            "columns_used": [],
            "tables_used": [],
            "can_validate": False,
            "skip_reason": "Missing account_type column",
        }
        mock_response = _make_tool_response(tool_input)
        mock_provider.converse.return_value = Result.ok(mock_response)

        result = validation_agent._run_single_validation(
            duckdb_conn=duckdb_conn,
            table_ids=[table.table_id],
            spec=spec,
            schema=schema,
        )

        assert result.status == ValidationStatus.SKIPPED
        assert "Missing account_type" in result.message
