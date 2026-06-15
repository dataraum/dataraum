"""Tests for the validation agent."""

from unittest.mock import MagicMock

import pytest

from dataraum.analysis.validation.agent import ValidationAgent
from dataraum.analysis.validation.models import (
    ValidationSeverity,
    ValidationSpec,
    ValidationStatus,
)
from dataraum.core.models.base import Result
from dataraum.lifecycle import BaseRunMap
from dataraum.llm.providers.base import TransientProviderError


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
def validation_agent(mock_llm_config, mock_provider, mock_prompt_renderer):
    """Create a validation agent with mocked dependencies."""
    return ValidationAgent(
        config=mock_llm_config,
        provider=mock_provider,
        prompt_renderer=mock_prompt_renderer,
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


def _eval_spec(check_type: str, **parameters) -> ValidationSpec:
    return ValidationSpec(
        validation_id="test",
        name="Test",
        description="Test",
        category="test",
        check_type=check_type,
        parameters=parameters,
    )


class TestValidationAgentEvaluateResult:
    """Tests for the _evaluate_result method.

    Returns (status, message, details): PASSED/FAILED is a judged
    measurement; ERROR means INCONCLUSIVE — the result shape could not be
    judged. Inconclusive must never surface as FAILED (DAT-439).
    """

    def test_evaluate_balance_check_passed(self, validation_agent):
        """Test balance check evaluation when balanced."""
        spec = _eval_spec("balance", tolerance=0.01)
        result_rows = [{"total_debits": 150.00, "total_credits": 150.00, "difference": 0.00}]

        status, message, details = validation_agent._evaluate_result(spec, result_rows, 1)

        assert status == ValidationStatus.PASSED
        assert "0.00" in message

    def test_evaluate_balance_check_failed(self, validation_agent):
        """Test balance check evaluation when not balanced."""
        spec = _eval_spec("balance", tolerance=0.01)
        result_rows = [{"total_debits": 150.00, "total_credits": 100.00, "difference": 50.00}]

        status, message, details = validation_agent._evaluate_result(spec, result_rows, 1)

        assert status == ValidationStatus.FAILED
        assert details["difference"] == 50.0

    def test_evaluate_balance_unrecognizable_columns_is_error_not_failed(self, validation_agent):
        """A balance result without judgeable columns is inconclusive → ERROR."""
        spec = _eval_spec("balance")
        result_rows = [{"some_col": 1, "other_col": 2}]

        status, message, details = validation_agent._evaluate_result(spec, result_rows, 1)

        assert status == ValidationStatus.ERROR
        assert "inconclusive" in message
        assert "some_col" in message

    def test_evaluate_balance_zero_rows_is_error(self, validation_agent):
        """A balance summary query returning no rows cannot be judged → ERROR."""
        status, message, _ = validation_agent._evaluate_result(_eval_spec("balance"), [], 0)

        assert status == ValidationStatus.ERROR
        assert "inconclusive" in message

    def test_evaluate_constraint_check_no_violations(self, validation_agent):
        """Constraint: an empty result IS the judgement (no violations) → PASSED."""
        status, message, details = validation_agent._evaluate_result(
            _eval_spec("constraint"), [], 0
        )

        assert status == ValidationStatus.PASSED
        assert "No constraint violations" in message

    def test_evaluate_constraint_check_with_violations(self, validation_agent):
        """Test constraint check with violations."""
        result_rows = [{"id": 1, "violation": "negative amount"}]

        status, message, details = validation_agent._evaluate_result(
            _eval_spec("constraint"), result_rows, 1
        )

        assert status == ValidationStatus.FAILED
        assert "1 constraint violations" in message

    def test_evaluate_comparison_check_equation_holds(self, validation_agent):
        """Test comparison check with equation_holds column."""
        result_rows = [{"assets": 1000, "liabilities": 600, "equity": 400, "equation_holds": True}]

        status, message, details = validation_agent._evaluate_result(
            _eval_spec("comparison"), result_rows, 1
        )

        assert status == ValidationStatus.PASSED
        assert "passed" in message

    def test_evaluate_comparison_check_equation_fails(self, validation_agent):
        """Test comparison check when equation doesn't hold."""
        result_rows = [{"assets": 1000, "liabilities": 600, "equity": 300, "equation_holds": False}]

        status, message, details = validation_agent._evaluate_result(
            _eval_spec("comparison"), result_rows, 1
        )

        assert status == ValidationStatus.FAILED

    def test_evaluate_comparison_inconclusive_is_error_not_failed(self, validation_agent):
        """The smoke-proven three_way_match shape (DAT-439): a comparison
        result without equation_holds/is_valid/difference columns is
        inconclusive — it must be ERROR, never FAILED."""
        result_rows = [{"po_count": 5, "invoice_count": 3, "receipt_count": 4}]

        status, message, details = validation_agent._evaluate_result(
            _eval_spec("comparison"), result_rows, 1
        )

        assert status == ValidationStatus.ERROR
        assert "Comparison check inconclusive" in message
        assert "could not identify comparison columns" in message
        assert details["check_type"] == "comparison"

    def test_evaluate_comparison_zero_rows_is_error(self, validation_agent):
        """A comparison query returning no rows cannot be judged → ERROR."""
        status, message, _ = validation_agent._evaluate_result(_eval_spec("comparison"), [], 0)

        assert status == ValidationStatus.ERROR
        assert "inconclusive" in message

    def test_evaluate_aggregate_check(self, validation_agent):
        """DAT-439 decision pin: aggregate without a rate metric stays PASSED —
        the prompt contract is 'summary values for review' (no rate required);
        the rate judgement is opportunistic."""
        result_rows = [{"min_date": "2024-01-01", "max_date": "2024-12-31", "total_records": 1000}]

        status, message, details = validation_agent._evaluate_result(
            _eval_spec("aggregate"), result_rows, 1
        )

        assert status == ValidationStatus.PASSED
        assert "Aggregate check completed" in message

    def test_evaluate_aggregate_rate_above_tolerance_fails(self, validation_agent):
        """An aggregate WITH a rate metric is judged against tolerance."""
        result_rows = [{"orphan_rate": 0.5, "total": 100}]

        status, message, _ = validation_agent._evaluate_result(
            _eval_spec("aggregate", tolerance=0.01), result_rows, 1
        )

        assert status == ValidationStatus.FAILED

    def test_evaluate_aggregate_zero_rows_is_error(self, validation_agent):
        """An aggregate summary query returning no rows cannot be judged → ERROR."""
        status, message, _ = validation_agent._evaluate_result(_eval_spec("aggregate"), [], 0)

        assert status == ValidationStatus.ERROR
        assert "inconclusive" in message

    def test_evaluate_unknown_check_type_is_error(self, validation_agent):
        """An unrecognized check type has no evaluation semantics — ERROR,
        never the old row_count>0 guess (DAT-439 sweep)."""
        result_rows = [{"anything": 1}]

        status, message, details = validation_agent._evaluate_result(
            _eval_spec("referential"), result_rows, 1
        )

        assert status == ValidationStatus.ERROR
        assert "Cannot evaluate check_type 'referential'" in message
        assert details["row_count"] == 1


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

    def test_generate_sql_llm_error_propagates(self, validation_agent, mock_provider):
        """A provider API failure raises a typed ProviderError (DAT-503).

        converse no longer folds the error into a Result.fail; the agent lets
        the typed exception propagate so retryability rides it to the worker's
        durable boundary, not a substring of a Result the agent would re-wrap.
        """
        spec = ValidationSpec(
            validation_id="test",
            name="Test",
            description="Test",
            category="test",
            check_type="balance",
        )

        schema = {"table_name": "test", "duckdb_path": "test", "columns": []}

        mock_provider.converse.side_effect = TransientProviderError("API error")

        with pytest.raises(TransientProviderError, match="API error"):
            validation_agent._generate_sql(spec, schema)

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

    def test_generate_sql_no_tool_call_fails(self, validation_agent, mock_provider):
        """No tool call = bind ERROR with reason — the JSON-parse-from-text
        fallback is deleted (DAT-439): even parseable text content must NOT
        be rescued into a GeneratedSQL."""
        spec = _eval_spec("balance")
        schema = {"table_name": "test", "duckdb_path": "test", "columns": []}

        response = MagicMock()
        response.tool_calls = []
        # Valid JSON the old fallback would have silently rescued.
        response.content = (
            '{"sql": "SELECT 1", "can_validate": true, "columns_used": [], "skip_reason": null}'
        )
        mock_provider.converse.return_value = Result.ok(response)

        result = validation_agent._generate_sql(spec, schema)

        assert not result.success
        assert "did not use the generate_validation_sql tool" in result.error

    def test_generate_sql_can_validate_without_sql_fails(self, validation_agent, mock_provider):
        """can_validate=true with no SQL is a degraded generation, not a
        legitimate skip — it must fail the bind, never surface as SKIPPED
        (DAT-439 sweep)."""
        spec = _eval_spec("balance")
        schema = {"table_name": "test", "duckdb_path": "test", "columns": []}

        tool_input = {
            "sql": None,
            "explanation": "confused response",
            "columns_used": [],
            "can_validate": True,
            "skip_reason": None,
        }
        mock_provider.converse.return_value = Result.ok(_make_tool_response(tool_input))

        result = validation_agent._generate_sql(spec, schema)

        assert not result.success
        assert "returned no SQL" in result.error


class TestValidationAgentBindExecute:
    """Tests for the bind/execute lifecycle operations (DAT-438).

    The phase-level concerns the old run_validations covered — schema errors,
    zero-spec outcomes, persistence — live in test_validation_phase.py now.
    """

    def test_bind_and_execute_success(
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

        schema = get_multi_table_schema_for_llm(session, [table.table_id], base_runs=BaseRunMap())

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

        generated, bind_failure = validation_agent.bind_validation(
            duckdb_conn, [table.table_id], spec, schema
        )
        assert bind_failure is None
        assert generated is not None
        assert generated.sql_query

        result = validation_agent.execute_validation(
            duckdb_conn, [table.table_id], spec, schema, generated
        )

        assert result.status == ValidationStatus.PASSED
        assert result.passed is True
        assert result.sql_used is not None

    def test_bind_ungroundable_returns_skip_failure(
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

        schema = get_multi_table_schema_for_llm(session, [table.table_id], base_runs=BaseRunMap())

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

        generated, bind_failure = validation_agent.bind_validation(
            duckdb_conn, [table.table_id], spec, schema
        )

        # Ungroundable: no generated SQL; the failure result carries the
        # reason the phase records on the still-declared artifact.
        assert generated is None
        assert bind_failure is not None
        assert bind_failure.status == ValidationStatus.SKIPPED
        assert "Missing account_type" in bind_failure.message

    def test_bind_missing_lake_table_fails_explain(
        self, session, duckdb_conn, validation_agent, mock_provider, table_with_data
    ):
        """Pins the downstream catch behind the resolver's row-count swallow
        (DAT-439 item 4): a table missing from the lake surfaces at bind —
        EXPLAIN fails → bind ERROR with reason, never a silent half-context
        run."""
        table = table_with_data

        spec = ValidationSpec(
            validation_id="missing_table_check",
            name="Missing Table Check",
            description="References a table that is not in the lake",
            category="test",
            check_type="balance",
        )

        from dataraum.analysis.validation.resolver import (
            get_multi_table_schema_for_llm,
        )

        schema = get_multi_table_schema_for_llm(session, [table.table_id], base_runs=BaseRunMap())

        tool_input = {
            "sql": "SELECT SUM(debit) AS total_debits FROM typed_table_not_in_lake",
            "explanation": "Sums debits from a table that does not exist",
            "columns_used": ["debit"],
            "can_validate": True,
            "skip_reason": None,
        }
        mock_provider.converse.return_value = Result.ok(_make_tool_response(tool_input))

        generated, bind_failure = validation_agent.bind_validation(
            duckdb_conn, [table.table_id], spec, schema
        )

        assert generated is None
        assert bind_failure is not None
        assert bind_failure.status == ValidationStatus.ERROR
        assert "Generated SQL is invalid" in bind_failure.message

    def test_execute_inconclusive_result_is_error(
        self, session, duckdb_conn, validation_agent, mock_provider, table_with_data
    ):
        """End-to-end execute pin: SQL runs but returns an unjudgeable shape
        → status ERROR (inconclusive), never FAILED (DAT-439 item 1)."""
        table = table_with_data

        spec = ValidationSpec(
            validation_id="three_way_match",
            name="Three Way Match",
            description="PO = invoice = receipt",
            category="financial",
            check_type="comparison",
        )

        from dataraum.analysis.validation.resolver import (
            get_multi_table_schema_for_llm,
        )

        schema = get_multi_table_schema_for_llm(session, [table.table_id], base_runs=BaseRunMap())

        tool_input = {
            "sql": "SELECT 5 AS po_count, 3 AS invoice_count FROM typed_journal_entries LIMIT 1",
            "explanation": "Counts without a judgeable comparison column",
            "columns_used": [],
            "can_validate": True,
            "skip_reason": None,
        }
        mock_provider.converse.return_value = Result.ok(_make_tool_response(tool_input))

        generated, bind_failure = validation_agent.bind_validation(
            duckdb_conn, [table.table_id], spec, schema
        )
        assert bind_failure is None
        assert generated is not None

        result = validation_agent.execute_validation(
            duckdb_conn, [table.table_id], spec, schema, generated
        )

        assert result.status == ValidationStatus.ERROR
        assert result.passed is False
        assert "Comparison check inconclusive" in result.message
