"""Tests for the validation agent."""

import json
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
    # A real value, not a Mock attribute: the agent forwards effort into
    # ConversationRequest, which validates it as str | None.
    config.features.validation.effort = "low"
    return config


@pytest.fixture
def mock_provider():
    """Create a mock LLM provider."""
    provider = MagicMock()
    provider.get_model_for_tier = MagicMock(return_value="claude-3-haiku")
    provider.converse = MagicMock()
    return provider


def _make_output_response(payload: dict):
    """A finished turn: the structured-output JSON as message content (DAT-807)."""
    response = MagicMock()
    response.tool_calls = []
    response.content = json.dumps(payload)
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

        # Mock LLM structured output
        payload = {
            "sql": "SELECT SUM(debit) as total_debits, SUM(credit) as total_credits FROM typed_transactions",
            "columns_used": ["debit", "credit"],
            "can_validate": True,
            "skip_reason": "",
        }
        mock_response = _make_output_response(payload)
        mock_provider.converse.return_value = Result.ok(mock_response)

        result = validation_agent._generate_sql(spec, schema)

        assert result.success
        generated = result.value
        assert "SELECT" in generated.sql_query
        assert generated.columns_used == ["debit", "credit"]
        assert generated.is_valid is True

    def test_generate_sql_pipes_conventions(self, validation_agent, mock_provider):
        """DAT-645: the vertical's conventions reach the prompt context verbatim."""
        spec = ValidationSpec(
            validation_id="sign_conventions",
            name="Sign",
            description="d",
            category="financial",
            check_type="constraint",
        )
        schema = {
            "table_name": "transactions",
            "duckdb_path": "typed_transactions",
            "columns": [{"column_name": "debit", "data_type": "DECIMAL"}],
        }
        mock_provider.converse.return_value = Result.ok(
            _make_output_response(
                {
                    "sql": "SELECT 1 AS x",
                    "columns_used": [],
                    "can_validate": True,
                    "skip_reason": "",
                }
            )
        )

        validation_agent._generate_sql(spec, schema, conventions="CREDIT-NORMAL = credit - debit")

        rendered_context = validation_agent.renderer.render_split.call_args.args[1]
        assert rendered_context["conventions"] == "CREDIT-NORMAL = credit - debit"

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

        # Mock LLM structured output indicating it cannot validate. sql and
        # skip_reason are BOTH required (DAT-807): exactly one is populated and
        # the other is the documented "" sentinel — never a union.
        payload = {
            "sql": "",
            "columns_used": [],
            "can_validate": False,
            "skip_reason": "Missing required columns: debit, credit",
        }
        mock_response = _make_output_response(payload)
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

    def test_generate_sql_unparseable_output_fails(self, validation_agent, mock_provider):
        """An unparseable payload = bind ERROR with reason, never a silent rescue
        (DAT-439). Constrained decoding makes a SHAPE failure unreachable, so the
        live causes are a turn that did not finish (max_tokens / refusal) or a
        genuine contract break — the error must name which."""
        spec = _eval_spec("balance")
        schema = {"table_name": "test", "duckdb_path": "test", "columns": []}

        response = MagicMock()
        response.tool_calls = []
        response.content = "I could not do this."
        response.stop_reason = "end_turn"
        response.output_tokens = 7
        mock_provider.converse.return_value = Result.ok(response)

        result = validation_agent._generate_sql(spec, schema)

        assert not result.success
        assert "Failed to parse the validation_sql output" in result.error
        # The diagnosis names the stop_reason so a truncation is never
        # misattributed to a broken API contract (DAT-807).
        assert "stop_reason=" in result.error

    def test_generate_sql_can_validate_without_sql_fails(self, validation_agent, mock_provider):
        """can_validate=true with no SQL is a degraded generation, not a
        legitimate skip — it must fail the bind, never surface as SKIPPED
        (DAT-439 sweep)."""
        spec = _eval_spec("balance")
        schema = {"table_name": "test", "duckdb_path": "test", "columns": []}

        payload = {
            "sql": "",
            "columns_used": [],
            "can_validate": True,
            "skip_reason": "",
        }
        mock_provider.converse.return_value = Result.ok(_make_output_response(payload))

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

        # Mock LLM to return valid SQL with tool call — the contracted output
        # (ADR-0017): one row with `deviation` + `magnitude`. The data is
        # balanced (debits == credits) so deviation = 0 → PASSED.
        tool_input = {
            "sql": "SELECT ABS(SUM(debit) - SUM(credit)) AS deviation, GREATEST(ABS(SUM(debit)), ABS(SUM(credit))) AS magnitude FROM typed_journal_entries",
            "columns_used": ["journal_entries.debit", "journal_entries.credit"],
            "can_validate": True,
            "skip_reason": "",
        }
        mock_response = _make_output_response(tool_input)
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
            "sql": "",
            "columns_used": [],
            "can_validate": False,
            "skip_reason": "Missing account_type column",
        }
        mock_response = _make_output_response(tool_input)
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
            "columns_used": ["debit"],
            "can_validate": True,
            "skip_reason": "",
        }
        mock_provider.converse.return_value = Result.ok(_make_output_response(tool_input))

        generated, bind_failure = validation_agent.bind_validation(
            duckdb_conn, [table.table_id], spec, schema
        )

        assert generated is None
        assert bind_failure is not None
        assert bind_failure.status == ValidationStatus.ERROR
        assert "Generated SQL is invalid" in bind_failure.message

    def test_bind_reserved_word_identifier_fails_explain(
        self, session, duckdb_conn, validation_agent, mock_provider, table_with_data
    ):
        """DAT-858: a bare reserved-word identifier (DuckDB's ``natural``,
        contextually reserved for ``NATURAL JOIN``) used unquoted as a CTE
        name fails to parse. There is NO repair turn on this path — bind
        must still fail loud with ERROR, the same fail-closed contract as
        any other unplannable SQL, never a crash and never a silent rewrite
        of the LLM's emitted text."""
        table = table_with_data

        spec = ValidationSpec(
            validation_id="reserved_word_check",
            name="Reserved Word Check",
            description="LLM emits an unquoted reserved-word identifier",
            category="test",
            check_type="balance",
        )

        from dataraum.analysis.validation.resolver import (
            get_multi_table_schema_for_llm,
        )

        schema = get_multi_table_schema_for_llm(session, [table.table_id], base_runs=BaseRunMap())

        # `natural` is unquoted here — DuckDB reserves it for NATURAL JOIN
        # grammar, so an unquoted CTE/alias named `natural` fails to parse
        # even though it reads like an ordinary identifier.
        tool_input = {
            "sql": (
                "WITH natural AS ("
                "SELECT SUM(debit) - SUM(credit) AS total FROM typed_journal_entries"
                ") SELECT ABS(total) AS deviation, ABS(total) AS magnitude FROM natural"
            ),
            "columns_used": ["debit", "credit"],
            "can_validate": True,
            "skip_reason": "",
        }
        mock_provider.converse.return_value = Result.ok(_make_output_response(tool_input))

        generated, bind_failure = validation_agent.bind_validation(
            duckdb_conn, [table.table_id], spec, schema
        )

        assert generated is None
        assert bind_failure is not None
        assert bind_failure.status == ValidationStatus.ERROR
        # Pin the actual parse reason, not just "some EXPLAIN failure" (which
        # the sibling missing-table test already covers) — DuckDB 1.5.4's
        # parser names the offending token. If a future DuckDB unreserves
        # NATURAL this assertion fails loud, which is the correct signal to
        # revisit the fix, not a flake to silence.
        assert 'syntax error at or near "natural"' in bind_failure.message

    def test_execute_inconclusive_result_is_error(
        self, session, duckdb_conn, validation_agent, mock_provider, table_with_data
    ):
        """End-to-end execute pin: SQL runs but ignores the output contract
        (no ``deviation`` column) → status ERROR (inconclusive), never FAILED
        (DAT-439 item 1 / ADR-0017)."""
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
            "columns_used": [],
            "can_validate": True,
            "skip_reason": "",
        }
        mock_provider.converse.return_value = Result.ok(_make_output_response(tool_input))

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
        assert "inconclusive" in result.message
        assert "deviation" in result.message
