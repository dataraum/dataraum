"""Tests for SQL Snippet Library."""

from dataraum.query.snippet_library import SnippetLibrary
from dataraum.query.snippet_models import SQLSnippetRecord

WORKSPACE_ID = "test"


class TestSnippetLibraryFindById:
    """Tests for primary key lookup."""

    def test_find_existing_snippet(self, session):
        """Find a snippet by its primary key."""
        library = SnippetLibrary(session, workspace_id=WORKSPACE_ID)

        record = library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(amount) AS value FROM typed_orders",
            description="Sum of revenue",
            schema_mapping_id="schema_abc",
            source="graph:dso",
            standard_field="revenue",
        )
        session.flush()

        found = library.find_by_id(record.snippet_id)
        assert found is not None
        assert found.snippet_id == record.snippet_id
        assert found.sql == "SELECT SUM(amount) AS value FROM typed_orders"

    def test_find_nonexistent_returns_none(self, session):
        """Unknown snippet_id returns None."""
        library = SnippetLibrary(session, workspace_id=WORKSPACE_ID)

        found = library.find_by_id("nonexistent-id")
        assert found is None


class TestSnippetLibraryFindByKey:
    """Tests for exact key lookup."""

    def test_find_extract_snippet(self, session):
        """Find an extract snippet by exact key."""
        library = SnippetLibrary(session, workspace_id=WORKSPACE_ID)

        # Save a snippet
        library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(amount) AS value FROM typed_orders",
            description="Sum of revenue",
            schema_mapping_id="schema_abc",
            source="graph:dso",
            standard_field="revenue",
            statement="income_statement",
            aggregation="sum",
        )
        session.flush()

        # Find it
        match = library.find_by_key(
            snippet_type="extract",
            schema_mapping_id="schema_abc",
            standard_field="revenue",
            statement="income_statement",
            aggregation="sum",
        )

        assert match is not None
        assert match.match_confidence == 1.0
        assert match.match_strategy == "exact_key"
        assert match.snippet.standard_field == "revenue"
        assert match.snippet.sql == "SELECT SUM(amount) AS value FROM typed_orders"

    def test_find_no_match(self, session):
        """No snippet for this key."""
        library = SnippetLibrary(session, workspace_id=WORKSPACE_ID)

        match = library.find_by_key(
            snippet_type="extract",
            schema_mapping_id="schema_abc",
            standard_field="nonexistent",
        )
        assert match is None

    def test_find_different_schema(self, session):
        """Same field but different schema doesn't match."""
        library = SnippetLibrary(session, workspace_id=WORKSPACE_ID)

        library.save_snippet(
            snippet_type="extract",
            sql="SELECT 1",
            description="test",
            schema_mapping_id="schema_abc",
            source="graph:test",
            standard_field="revenue",
            statement="income_statement",
            aggregation="sum",
        )
        session.flush()

        match = library.find_by_key(
            snippet_type="extract",
            schema_mapping_id="schema_xyz",
            standard_field="revenue",
            statement="income_statement",
            aggregation="sum",
        )
        assert match is None

    def test_find_constant_snippet(self, session):
        """Find a constant snippet including parameter value."""
        library = SnippetLibrary(session, workspace_id=WORKSPACE_ID)

        library.save_snippet(
            snippet_type="constant",
            sql="SELECT 30 AS value",
            description="30 day period",
            schema_mapping_id="schema_abc",
            source="graph:dso",
            standard_field="days_in_period",
            parameter_value="30",
        )
        session.flush()

        # Find with matching parameter value
        match = library.find_by_key(
            snippet_type="constant",
            schema_mapping_id="schema_abc",
            standard_field="days_in_period",
            parameter_value="30",
        )
        assert match is not None
        assert match.snippet.parameter_value == "30"

        # Different parameter value doesn't match
        match2 = library.find_by_key(
            snippet_type="constant",
            schema_mapping_id="schema_abc",
            standard_field="days_in_period",
            parameter_value="365",
        )
        assert match2 is None

    def test_null_fields_match_correctly(self, session):
        """Null fields in key must match null (not anything)."""
        library = SnippetLibrary(session, workspace_id=WORKSPACE_ID)

        # Snippet with no statement
        library.save_snippet(
            snippet_type="extract",
            sql="SELECT 1",
            description="test",
            schema_mapping_id="schema_abc",
            source="graph:test",
            standard_field="total_assets",
            # statement=None (default)
            aggregation="sum",
        )
        session.flush()

        # Match with no statement
        match = library.find_by_key(
            snippet_type="extract",
            schema_mapping_id="schema_abc",
            standard_field="total_assets",
            aggregation="sum",
        )
        assert match is not None

        # Should NOT match if we ask for a specific statement
        match2 = library.find_by_key(
            snippet_type="extract",
            schema_mapping_id="schema_abc",
            standard_field="total_assets",
            statement="balance_sheet",
            aggregation="sum",
        )
        assert match2 is None


class TestSnippetLibrarySave:
    """Tests for snippet save with upsert semantics."""

    def test_save_new_snippet(self, session):
        """Save creates a new record."""
        library = SnippetLibrary(session, workspace_id=WORKSPACE_ID)

        record = library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(x) AS value FROM t",
            description="Sum of x",
            schema_mapping_id="schema_abc",
            source="graph:metric_a",
            standard_field="revenue",
            statement="income_statement",
            aggregation="sum",
        )
        session.flush()

        assert record.snippet_id is not None
        assert record.sql == "SELECT SUM(x) AS value FROM t"

    def test_save_keeps_first_writer(self, session):
        """Save with same key keeps original (first writer wins)."""
        library = SnippetLibrary(session, workspace_id=WORKSPACE_ID)

        # First save
        record1 = library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(x) AS value FROM t",
            description="Original",
            schema_mapping_id="schema_abc",
            source="graph:v1",
            standard_field="revenue",
            aggregation="sum",
        )
        session.flush()
        snippet_id_1 = record1.snippet_id

        # Second save with same key — should return original, not overwrite
        record2 = library.save_snippet(
            snippet_type="extract",
            sql="SELECT SUM(y) AS value FROM t2",
            description="Updated",
            schema_mapping_id="schema_abc",
            source="graph:v2",
            standard_field="revenue",
            aggregation="sum",
        )
        session.flush()

        # Should be same record, unchanged
        assert record2.snippet_id == snippet_id_1
        assert record2.sql == "SELECT SUM(x) AS value FROM t"
        assert record2.description == "Original"
        assert record2.source == "graph:v1"

    def test_redelivered_save_converges_across_commits(self, session):
        """Success-redelivery (same key, committed prior write) converges (DAT-502).

        metrics_phase commits once PER METRIC (the sanctioned multi-commit
        exception): attempt 1's snippet is durable when the redelivered
        attempt re-saves the same key — first-writer-wins must hold across
        the commit, leaving exactly one healthy row.
        """
        from sqlalchemy import func, select

        from dataraum.query.snippet_models import SQLSnippetRecord

        library = SnippetLibrary(session, workspace_id=WORKSPACE_ID)
        kwargs = {
            "snippet_type": "extract",
            "description": "metric snippet",
            "schema_mapping_id": "schema_abc",
            "source": "graph:dso",
            "standard_field": "revenue",
            "aggregation": "sum",
        }
        first = library.save_snippet(sql="SELECT SUM(x) AS value FROM t", **kwargs)
        session.commit()  # the per-metric commit; ack lost

        again = library.save_snippet(sql="SELECT SUM(z) AS value FROM t3", **kwargs)
        session.commit()

        assert again.snippet_id == first.snippet_id
        assert again.sql == "SELECT SUM(x) AS value FROM t"  # first writer won
        total = session.scalar(select(func.count()).select_from(SQLSnippetRecord))
        assert total == 1

    def test_save_formula_snippet(self, session):
        """Save a formula snippet with normalized expression."""
        library = SnippetLibrary(session, workspace_id=WORKSPACE_ID)

        record = library.save_snippet(
            snippet_type="formula",
            sql="SELECT (SELECT value FROM ar) / (SELECT value FROM rev) * 30 AS value",
            description="DSO calculation",
            schema_mapping_id="schema_abc",
            source="graph:dso",
            normalized_expression="({A} / {B}) * {C}",
            input_fields=["accounts_receivable", "days_in_period", "revenue"],
        )
        session.flush()

        assert record.normalized_expression == "({A} / {B}) * {C}"
        assert record.input_fields == ["accounts_receivable", "days_in_period", "revenue"]

    def test_save_with_column_mappings(self, session):
        """Column mappings are persisted."""
        library = SnippetLibrary(session, workspace_id=WORKSPACE_ID)

        record = library.save_snippet(
            snippet_type="extract",
            sql="SELECT 1",
            description="test",
            schema_mapping_id="schema_abc",
            source="graph:test",
            standard_field="revenue",
            column_mappings={"revenue": "Betrag", "type": "Kontoart"},
        )
        session.flush()

        fetched = session.get(SQLSnippetRecord, record.snippet_id)
        assert fetched.column_mappings == {"revenue": "Betrag", "type": "Kontoart"}


class TestSnippetLibraryFormulaPerMetric:
    """Formula snippets are identified PER-METRIC by source, not by shape (DAT-646)."""

    _COMMON = {
        "snippet_type": "formula",
        "description": "margin",
        "schema_mapping_id": "schema_abc",
        "normalized_expression": "{A} / {B}",
        "input_fields": ["a", "b"],
    }

    def test_same_shape_different_source_are_distinct_rows(self, session):
        """Two metrics sharing an arithmetic shape each get their OWN formula row — no
        cross-metric aliasing (was: net_margin reused ebitda_margin's formula snippet)."""
        from sqlalchemy import func, select

        library = SnippetLibrary(session, workspace_id=WORKSPACE_ID)

        a = library.save_snippet(
            sql="WITH ebitda AS (SELECT 1) SELECT 1 AS value",
            source="graph:ebitda_margin",
            **self._COMMON,
        )
        b = library.save_snippet(
            sql="WITH net_income AS (SELECT 1) SELECT 1 AS value",
            source="graph:net_margin",
            **self._COMMON,
        )
        session.flush()

        assert a.snippet_id != b.snippet_id
        total = session.scalar(
            select(func.count())
            .select_from(SQLSnippetRecord)
            .where(SQLSnippetRecord.snippet_type == "formula")
        )
        assert total == 2

    def test_same_source_same_expression_is_idempotent(self, session):
        """Re-saving a metric's own formula is a no-op (insert-if-not-exists, first wins)."""
        from sqlalchemy import func, select

        library = SnippetLibrary(session, workspace_id=WORKSPACE_ID)

        first = library.save_snippet(
            sql="SELECT 1 AS value", source="graph:net_margin", **self._COMMON
        )
        session.flush()
        again = library.save_snippet(
            sql="SELECT 2 AS value", source="graph:net_margin", **self._COMMON
        )
        session.flush()

        assert again.snippet_id == first.snippet_id
        assert again.sql == "SELECT 1 AS value"  # first writer won
        total = session.scalar(
            select(func.count())
            .select_from(SQLSnippetRecord)
            .where(SQLSnippetRecord.snippet_type == "formula")
        )
        assert total == 1


class TestSnippetLibraryRecordUsage:
    """Tests for usage tracking."""

    def test_record_exact_reuse(self, session):
        """Record an exact reuse and update snippet stats."""
        library = SnippetLibrary(session, workspace_id=WORKSPACE_ID)

        snippet = library.save_snippet(
            snippet_type="extract",
            sql="SELECT 1",
            description="test",
            schema_mapping_id="schema_abc",
            source="graph:test",
            standard_field="revenue",
        )
        session.flush()
        assert snippet.execution_count == 0

        usage = library.record_usage(
            execution_id="exec_001",
            execution_type="graph",
            usage_type="exact_reuse",
            snippet_id=snippet.snippet_id,
            match_confidence=1.0,
            sql_match_ratio=1.0,
            step_id="revenue",
        )
        session.flush()

        assert usage.usage_type == "exact_reuse"
        assert usage.step_id == "revenue"

        # Snippet stats should be updated
        session.refresh(snippet)
        assert snippet.execution_count == 1
        assert snippet.last_used_at is not None

    def test_record_newly_generated(self, session):
        """Record a newly generated step (no snippet)."""
        library = SnippetLibrary(session, workspace_id=WORKSPACE_ID)

        usage = library.record_usage(
            execution_id="exec_002",
            execution_type="query",
            usage_type="newly_generated",
            step_id="monthly_revenue",
        )
        session.flush()

        assert usage.snippet_id is None
        assert usage.usage_type == "newly_generated"

    def test_record_provided_not_used(self, session):
        """Record when snippet was provided but LLM ignored it."""
        library = SnippetLibrary(session, workspace_id=WORKSPACE_ID)

        snippet = library.save_snippet(
            snippet_type="extract",
            sql="SELECT 1",
            description="test",
            schema_mapping_id="schema_abc",
            source="graph:test",
            standard_field="revenue",
        )
        session.flush()

        library.record_usage(
            execution_id="exec_003",
            execution_type="query",
            usage_type="provided_not_used",
            snippet_id=snippet.snippet_id,
            match_confidence=0.7,
            sql_match_ratio=0.3,
        )
        session.flush()

        # provided_not_used should NOT increment execution_count
        session.refresh(snippet)
        assert snippet.execution_count == 0
