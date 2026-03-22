"""Tests for run_sql MCP tool."""

from __future__ import annotations

from collections.abc import Generator
from uuid import uuid4

import duckdb
import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from dataraum.mcp.cte_parser import decompose_ctes
from dataraum.mcp.formatters import format_run_sql_result
from dataraum.mcp.sql_executor import _build_column_quality, _snippet_key_for_step, run_sql
from dataraum.query.execution import SQLStep, StepExecutionResult
from dataraum.storage import init_database


def _id() -> str:
    return str(uuid4())


@pytest.fixture
def cursor() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB connection."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE orders (id INT, amount DOUBLE, region VARCHAR)")
    conn.execute("INSERT INTO orders VALUES (1, 100.0, 'US'), (2, 200.0, 'EU'), (3, 150.0, 'US')")
    return conn


@pytest.fixture
def session() -> Generator[Session]:
    """In-memory SQLite session with all tables created."""
    engine = create_engine("sqlite:///:memory:", echo=False)

    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        c = dbapi_conn.cursor()
        c.execute("PRAGMA foreign_keys=OFF")
        c.close()

    init_database(engine)
    factory = sessionmaker(bind=engine)
    with factory() as s:
        yield s


def _insert_source_and_table(
    session: Session,
    table_name: str = "orders",
    column_names: list[str] | None = None,
) -> tuple[str, str, list[str]]:
    """Insert Source, Table, and Columns. Returns (source_id, table_id, [column_ids])."""
    from dataraum.storage import Column, Source, Table

    source_id = _id()
    table_id = _id()
    cols = column_names or ["amount", "region"]

    session.add(Source(source_id=source_id, name="test_source", source_type="csv"))
    session.add(
        Table(
            table_id=table_id,
            source_id=source_id,
            table_name=table_name,
            layer="typed",
            duckdb_path=f"typed_{table_name}",
        )
    )
    col_ids = []
    for i, col_name in enumerate(cols):
        col_id = _id()
        col_ids.append(col_id)
        session.add(
            Column(
                column_id=col_id,
                table_id=table_id,
                column_name=col_name,
                column_position=i,
            )
        )
    session.flush()
    return source_id, table_id, col_ids


# --- Phase 2a: Basic execution ---


class TestRawSqlExecutes:
    def test_simple_select(self, cursor: duckdb.DuckDBPyConnection) -> None:
        result = run_sql(cursor, sql="SELECT 42 AS x")
        assert "error" not in result
        assert result["columns"] == ["x"]
        assert result["row_count"] == 1
        assert result["rows"] == [{"x": 42}]
        assert result["truncated"] is False

    def test_select_from_table(self, cursor: duckdb.DuckDBPyConnection) -> None:
        result = run_sql(cursor, sql="SELECT * FROM orders WHERE region = 'US'")
        assert result["row_count"] == 2
        assert all(r["region"] == "US" for r in result["rows"])


class TestStructuredStepsExecuted:
    def test_two_steps_with_reference(self, cursor: duckdb.DuckDBPyConnection) -> None:
        steps = [
            {"step_id": "us_orders", "sql": "SELECT * FROM orders WHERE region = 'US'"},
            {
                "step_id": "us_total",
                "sql": "SELECT SUM(amount) AS total FROM us_orders",
                "description": "Sum US orders",
            },
        ]
        result = run_sql(cursor, steps=steps)
        assert "error" not in result
        assert result["row_count"] == 1
        assert result["rows"][0]["total"] == 250.0
        assert len(result["steps_executed"]) == 2
        assert result["steps_executed"][0]["step_id"] == "us_orders"
        assert result["steps_executed"][1]["step_id"] == "us_total"


class TestInputValidation:
    def test_rejects_both_steps_and_sql(self, cursor: duckdb.DuckDBPyConnection) -> None:
        result = run_sql(cursor, steps=[{"step_id": "q", "sql": "SELECT 1"}], sql="SELECT 1")
        assert "error" in result
        assert "not both" in result["error"]

    def test_rejects_neither_steps_nor_sql(self, cursor: duckdb.DuckDBPyConnection) -> None:
        result = run_sql(cursor)
        assert "error" in result
        assert "Provide either" in result["error"]


class TestRowLimit:
    def test_default_limit_applied(self, cursor: duckdb.DuckDBPyConnection) -> None:
        # Insert enough rows to exceed default limit (100)
        cursor.execute("CREATE TABLE big AS SELECT i AS id FROM generate_series(1, 200) t(i)")
        result = run_sql(cursor, sql="SELECT * FROM big")
        assert result["row_count"] == 100
        assert result["truncated"] is True

    def test_custom_limit(self, cursor: duckdb.DuckDBPyConnection) -> None:
        result = run_sql(cursor, sql="SELECT * FROM orders", limit=2)
        assert result["row_count"] == 2
        assert result["truncated"] is True

    def test_max_limit_enforced(self, cursor: duckdb.DuckDBPyConnection) -> None:
        # Limit > 10000 is capped to 10000
        result = run_sql(cursor, sql="SELECT * FROM orders", limit=99999)
        # With only 3 rows, truncated should be False — the cap is 10000
        assert result["truncated"] is False
        assert result["row_count"] == 3


class TestSqlError:
    def test_bad_sql_returns_error(self, cursor: duckdb.DuckDBPyConnection) -> None:
        result = run_sql(cursor, sql="SELECT * FROM nonexistent_table")
        assert "error" in result


class TestFormatRunSqlResult:
    def test_basic_format(self) -> None:
        step_results = [
            StepExecutionResult(step_id="q", sql_executed="SELECT 1 AS x"),
        ]
        result = format_run_sql_result(
            columns=["x"],
            rows=[{"x": 1}],
            step_results=step_results,
            limit=100,
            total_rows=1,
        )
        assert result["columns"] == ["x"]
        assert result["row_count"] == 1
        assert result["truncated"] is False
        assert result["steps_executed"] == [{"step_id": "q", "sql": "SELECT 1 AS x"}]
        assert "column_quality" not in result
        assert "quality_caveat" not in result

    def test_truncated_flag(self) -> None:
        step_results = [
            StepExecutionResult(step_id="q", sql_executed="SELECT 1"),
        ]
        result = format_run_sql_result(
            columns=["x"],
            rows=[{"x": 1}] * 10,
            step_results=step_results,
            limit=10,
            total_rows=50,
        )
        assert result["truncated"] is True

    def test_quality_fields_included_when_provided(self) -> None:
        step_results = [
            StepExecutionResult(step_id="q", sql_executed="SELECT 1"),
        ]
        quality = {"revenue": {"quality_grade": "A"}}
        result = format_run_sql_result(
            columns=["revenue"],
            rows=[{"revenue": 100}],
            step_results=step_results,
            limit=100,
            total_rows=1,
            column_quality=quality,
            quality_caveat="entropy phase not run",
        )
        assert result["column_quality"] == quality
        assert result["quality_caveat"] == "entropy phase not run"


# --- Phase 2b: Quality metadata ---


class TestQualityMetadataForMappedColumns:
    def test_quality_attached_via_mapping(self, session: Session) -> None:
        from dataraum.analysis.quality_summary.db_models import ColumnQualityReport

        source_id, table_id, col_ids = _insert_source_and_table(
            session, "orders", ["Betrag", "Region"]
        )
        # Add quality report for Betrag
        session.add(
            ColumnQualityReport(
                report_id=_id(),
                source_column_id=col_ids[0],
                slice_column_id=col_ids[0],
                column_name="Betrag",
                source_table_name="orders",
                slice_column_name="Betrag",
                slice_count=1,
                overall_quality_score=0.72,
                quality_grade="B",
                summary="Moderate quality",
                report_data={},
                investigation_views=[],
            )
        )
        session.flush()

        quality, caveat = _build_column_quality(
            session,
            table_ids=[table_id],
            output_columns=["revenue", "region"],
            column_mappings={"revenue": "Betrag"},
        )

        assert quality["revenue"] is not None
        assert quality["revenue"]["source_column"] == "orders.Betrag"
        assert quality["revenue"]["quality_grade"] == "B"
        assert quality["revenue"]["quality_score"] == 0.72


class TestQualityViaSlicingView:
    def test_quality_resolved_via_slicing_view(self, session: Session) -> None:
        """ColumnQualityReport.source_column_id points to slicing_view columns.

        Verify that _build_column_quality resolves through the slicing_view layer
        to find quality grades — the same pattern as ColumnQualityDetector.
        """
        from dataraum.analysis.quality_summary.db_models import ColumnQualityReport
        from dataraum.storage import Column, Source, Table

        source_id = _id()
        session.add(Source(source_id=source_id, name="test", source_type="csv"))

        # Typed table
        typed_id = _id()
        session.add(
            Table(
                table_id=typed_id,
                source_id=source_id,
                table_name="orders",
                layer="typed",
                duckdb_path="typed_orders",
            )
        )
        typed_col_id = _id()
        session.add(
            Column(
                column_id=typed_col_id, table_id=typed_id, column_name="amount", column_position=0
            )
        )

        # Slicing view table (what quality reports actually reference)
        sv_id = _id()
        session.add(
            Table(
                table_id=sv_id,
                source_id=source_id,
                table_name="slicing_orders",
                layer="slicing_view",
                duckdb_path="slicing_orders",
            )
        )
        sv_col_id = _id()
        session.add(
            Column(column_id=sv_col_id, table_id=sv_id, column_name="amount", column_position=0)
        )

        # Quality report references slicing_view column (production behavior)
        session.add(
            ColumnQualityReport(
                report_id=_id(),
                source_column_id=sv_col_id,
                slice_column_id=sv_col_id,
                column_name="amount",
                source_table_name="slicing_orders",
                slice_column_name="amount",
                slice_count=1,
                overall_quality_score=0.85,
                quality_grade="A",
                summary="Good quality",
                report_data={},
                investigation_views=[],
            )
        )
        session.flush()

        quality, _ = _build_column_quality(
            session,
            table_ids=[typed_id],
            output_columns=["amount"],
            column_mappings={},
        )

        assert quality["amount"] is not None
        assert quality["amount"]["source_column"] == "orders.amount"
        assert quality["amount"]["quality_grade"] == "A"
        assert quality["amount"]["quality_score"] == 0.85


class TestQualifiedColumnMappings:
    def test_qualified_mapping_resolves_correctly(self, session: Session) -> None:
        """column_mappings with 'table.column' format should resolve directly."""
        source_id, table_id, col_ids = _insert_source_and_table(
            session, "orders", ["Betrag", "Region"]
        )

        quality, _ = _build_column_quality(
            session,
            table_ids=[table_id],
            output_columns=["revenue"],
            column_mappings={"revenue": "orders.Betrag"},
        )
        assert quality["revenue"] is not None
        assert quality["revenue"]["source_column"] == "orders.Betrag"

    def test_ambiguous_column_returns_candidates(self, session: Session) -> None:
        """Same column in multiple tables should return ambiguous marker."""
        from dataraum.storage import Column, Source, Table

        source_id = _id()
        session.add(Source(source_id=source_id, name="test", source_type="csv"))

        # Two typed tables both with "date" column
        t1_id = _id()
        session.add(
            Table(
                table_id=t1_id,
                source_id=source_id,
                table_name="orders",
                layer="typed",
                duckdb_path="typed_orders",
            )
        )
        session.add(Column(column_id=_id(), table_id=t1_id, column_name="date", column_position=0))

        t2_id = _id()
        session.add(
            Table(
                table_id=t2_id,
                source_id=source_id,
                table_name="invoices",
                layer="typed",
                duckdb_path="typed_invoices",
            )
        )
        session.add(Column(column_id=_id(), table_id=t2_id, column_name="date", column_position=0))
        session.flush()

        quality, _ = _build_column_quality(
            session,
            table_ids=[t1_id, t2_id],
            output_columns=["date"],
            column_mappings={},
        )
        assert quality["date"] is not None
        assert quality["date"]["ambiguous"] is True
        assert len(quality["date"]["candidates"]) == 2

    def test_ambiguous_resolved_by_qualified_mapping(self, session: Session) -> None:
        """Qualified mapping disambiguates when column exists in multiple tables."""
        from dataraum.storage import Column, Source, Table

        source_id = _id()
        session.add(Source(source_id=source_id, name="test", source_type="csv"))

        t1_id = _id()
        session.add(
            Table(
                table_id=t1_id,
                source_id=source_id,
                table_name="orders",
                layer="typed",
                duckdb_path="typed_orders",
            )
        )
        session.add(Column(column_id=_id(), table_id=t1_id, column_name="date", column_position=0))

        t2_id = _id()
        session.add(
            Table(
                table_id=t2_id,
                source_id=source_id,
                table_name="invoices",
                layer="typed",
                duckdb_path="typed_invoices",
            )
        )
        session.add(Column(column_id=_id(), table_id=t2_id, column_name="date", column_position=0))
        session.flush()

        quality, _ = _build_column_quality(
            session,
            table_ids=[t1_id, t2_id],
            output_columns=["invoice_date"],
            column_mappings={"invoice_date": "invoices.date"},
        )
        assert quality["invoice_date"] is not None
        assert quality["invoice_date"]["source_column"] == "invoices.date"
        assert "ambiguous" not in quality["invoice_date"]


class TestUnmappedColumnsGetNull:
    def test_computed_column_returns_null(self, session: Session) -> None:
        _, table_id, _ = _insert_source_and_table(session, "orders", ["amount"])

        quality, _ = _build_column_quality(
            session,
            table_ids=[table_id],
            output_columns=["total_revenue"],
            column_mappings={},
        )
        assert quality["total_revenue"] is None


class TestQualityCaveatWhenIncomplete:
    def test_caveat_when_entropy_not_run(self, session: Session) -> None:
        _, table_id, _ = _insert_source_and_table(session, "orders", ["amount"])

        _, caveat = _build_column_quality(
            session,
            table_ids=[table_id],
            output_columns=["amount"],
            column_mappings={},
        )
        assert caveat is not None
        assert "entropy phase has not run" in caveat

    def test_no_caveat_when_entropy_completed(self, session: Session) -> None:
        from dataraum.entropy.db_models import EntropyObjectRecord

        source_id, table_id, _ = _insert_source_and_table(session, "orders", ["amount"])

        # Insert an entropy object so the check finds entropy data
        session.add(
            EntropyObjectRecord(
                object_id=_id(),
                layer="structural",
                dimension="types",
                sub_dimension="type_fidelity",
                target="column:orders.amount",
                source_id=source_id,
                table_id=table_id,
                score=0.1,
                detector_id="test_detector",
            )
        )
        session.flush()

        _, caveat = _build_column_quality(
            session,
            table_ids=[table_id],
            output_columns=["amount"],
            column_mappings={},
        )
        assert caveat is None


class TestQualityGracefulOnNoPipeline:
    def test_no_session_returns_no_quality(self, cursor: duckdb.DuckDBPyConnection) -> None:
        """Without session/table_ids, quality fields are absent."""
        result = run_sql(cursor, sql="SELECT 42 AS x")
        assert "column_quality" not in result
        assert "quality_caveat" not in result


# --- Phase 2c: Snippet integration ---


def _make_snippet(
    session: Session,
    source_id: str,
    step_id: str,
    sql_text: str,
) -> str:
    """Helper: insert a snippet record and return its ID."""
    from dataraum.query.snippet_library import SnippetLibrary

    library = SnippetLibrary(session)
    record = library.save_snippet(
        snippet_type="query",
        sql=sql_text,
        description=f"test snippet for {step_id}",
        schema_mapping_id=source_id,
        source="test",
        standard_field=step_id,
    )
    session.flush()
    return record.snippet_id


class TestSnippetReuseDetected:
    def test_exact_reuse(self, cursor: duckdb.DuckDBPyConnection, session: Session) -> None:
        source_id, table_id, _ = _insert_source_and_table(session, "orders", ["amount"])
        sql_text = "SELECT 42 AS x"
        # Use content-hash key (matches what run_sql generates for raw SQL)
        step = SQLStep(step_id="query", sql=sql_text, description="")
        key = _snippet_key_for_step(
            step,
            [
                {
                    "step_id": "query",
                    "_snippet_key": f"query_{__import__('hashlib').sha256(sql_text.encode()).hexdigest()[:12]}",
                }
            ],
        )
        snippet_id = _make_snippet(session, source_id, key, sql_text)

        result = run_sql(
            cursor,
            session=session,
            source_id=source_id,
            table_ids=[table_id],
            sql=sql_text,
        )
        assert "error" not in result
        step_info = result["steps_executed"][0]
        assert step_info["snippet_status"] == "exact_reuse"
        assert step_info["snippet_id"] == snippet_id

    def test_adapted_reuse_with_structured_steps(
        self, cursor: duckdb.DuckDBPyConnection, session: Session
    ) -> None:
        """Structured steps use step_id directly — adapted when SQL differs."""
        source_id, table_id, _ = _insert_source_and_table(session, "orders", ["amount"])
        _make_snippet(session, source_id, "my_step", "SELECT 1 AS old_query")

        result = run_sql(
            cursor,
            session=session,
            source_id=source_id,
            table_ids=[table_id],
            steps=[{"step_id": "my_step", "sql": "SELECT 42 AS x"}],
        )
        assert "error" not in result
        step_info = result["steps_executed"][0]
        assert step_info["snippet_status"] == "adapted"


class TestSnippetSavedAfterSuccess:
    def test_novel_step_saved(self, cursor: duckdb.DuckDBPyConnection, session: Session) -> None:
        import hashlib as _hashlib

        from dataraum.query.snippet_library import SnippetLibrary

        source_id, table_id, _ = _insert_source_and_table(session, "orders", ["amount"])
        sql_text = "SELECT 42 AS x"

        result = run_sql(
            cursor,
            session=session,
            source_id=source_id,
            table_ids=[table_id],
            sql=sql_text,
        )
        assert "error" not in result
        assert result["snippet_summary"]["saved"] == 1
        assert result["snippet_summary"]["session_source"].startswith("mcp:session_")

        # Verify snippet was saved with content-hash key
        expected_key = f"query_{_hashlib.sha256(sql_text.encode()).hexdigest()[:12]}"
        library = SnippetLibrary(session)
        match = library.find_by_key(
            snippet_type="query",
            schema_mapping_id=source_id,
            standard_field=expected_key,
        )
        assert match is not None
        assert match.snippet.sql == sql_text


class TestSnippetNotSavedOnFailure:
    def test_bad_sql_no_snippet(self, cursor: duckdb.DuckDBPyConnection, session: Session) -> None:
        import hashlib as _hashlib

        from dataraum.query.snippet_library import SnippetLibrary

        source_id, table_id, _ = _insert_source_and_table(session, "orders", ["amount"])
        bad_sql = "SELECT * FROM nonexistent_table"

        result = run_sql(
            cursor,
            session=session,
            source_id=source_id,
            table_ids=[table_id],
            sql=bad_sql,
        )
        assert "error" in result

        key = f"query_{_hashlib.sha256(bad_sql.encode()).hexdigest()[:12]}"
        library = SnippetLibrary(session)
        match = library.find_by_key(
            snippet_type="query",
            schema_mapping_id=source_id,
            standard_field=key,
        )
        assert match is None


class TestSnippetSummaryAccurate:
    def test_reused_and_saved_counts(
        self, cursor: duckdb.DuckDBPyConnection, session: Session
    ) -> None:
        source_id, table_id, _ = _insert_source_and_table(session, "orders", ["amount"])
        # Pre-save a snippet for step "step_a"
        _make_snippet(session, source_id, "step_a", "SELECT 1 AS a")

        steps = [
            {"step_id": "step_a", "sql": "SELECT 1 AS a"},
            {"step_id": "step_b", "sql": "SELECT a + 1 AS b FROM step_a"},
        ]
        result = run_sql(
            cursor,
            session=session,
            source_id=source_id,
            table_ids=[table_id],
            steps=steps,
        )
        assert "error" not in result
        summary = result["snippet_summary"]
        assert summary["reused"] == 1
        assert summary["saved"] == 1


class TestSnippetIntegrationWithRawSql:
    def test_raw_sql_creates_snippet(
        self, cursor: duckdb.DuckDBPyConnection, session: Session
    ) -> None:
        source_id, table_id, _ = _insert_source_and_table(session, "orders", ["amount"])

        result = run_sql(
            cursor,
            session=session,
            source_id=source_id,
            table_ids=[table_id],
            sql="SELECT 99 AS val",
        )
        assert "error" not in result
        assert result["snippet_summary"]["saved"] == 1
        # step_id for raw sql mode is "query"
        assert result["steps_executed"][0]["step_id"] == "query"

    def test_different_raw_sql_no_collision(
        self, cursor: duckdb.DuckDBPyConnection, session: Session
    ) -> None:
        """Two different raw SQL strings should not share a snippet key."""
        source_id, table_id, _ = _insert_source_and_table(session, "orders", ["amount"])

        r1 = run_sql(
            cursor,
            session=session,
            source_id=source_id,
            table_ids=[table_id],
            sql="SELECT 1 AS a",
        )
        r2 = run_sql(
            cursor,
            session=session,
            source_id=source_id,
            table_ids=[table_id],
            sql="SELECT 2 AS b",
        )
        assert r1["snippet_summary"]["saved"] == 1
        assert r2["snippet_summary"]["saved"] == 1
        # Both saved independently, neither shows as reused
        assert r1["snippet_summary"]["reused"] == 0
        assert r2["snippet_summary"]["reused"] == 0


# --- CTE auto-decomposition ---


class TestCteDecomposition:
    def test_cte_decomposed_into_steps(self, cursor: duckdb.DuckDBPyConnection) -> None:
        sql = (
            "WITH revenue AS (SELECT region, SUM(amount) AS total FROM orders GROUP BY region), "
            "costs AS (SELECT region, COUNT(*) AS n FROM orders GROUP BY region) "
            "SELECT * FROM revenue JOIN costs USING (region)"
        )
        result = run_sql(cursor, sql=sql)
        assert "error" not in result
        assert len(result["steps_executed"]) == 2

    def test_cte_names_used_as_step_ids(self, cursor: duckdb.DuckDBPyConnection) -> None:
        sql = (
            "WITH revenue AS (SELECT region, SUM(amount) AS total FROM orders GROUP BY region), "
            "costs AS (SELECT region, COUNT(*) AS n FROM orders GROUP BY region) "
            "SELECT * FROM revenue JOIN costs USING (region)"
        )
        result = run_sql(cursor, sql=sql)
        assert "error" not in result
        step_ids = [s["step_id"] for s in result["steps_executed"]]
        assert step_ids == ["revenue", "costs"]

    def test_cte_snippets_saved_individually(
        self, cursor: duckdb.DuckDBPyConnection, session: Session
    ) -> None:
        from dataraum.query.snippet_library import SnippetLibrary

        source_id, table_id, _ = _insert_source_and_table(session, "orders", ["amount", "region"])
        sql = (
            "WITH revenue AS (SELECT region, SUM(amount) AS total FROM orders GROUP BY region), "
            "costs AS (SELECT region, COUNT(*) AS n FROM orders GROUP BY region) "
            "SELECT * FROM revenue JOIN costs USING (region)"
        )
        result = run_sql(
            cursor,
            session=session,
            source_id=source_id,
            table_ids=[table_id],
            sql=sql,
        )
        assert "error" not in result
        assert result["snippet_summary"]["saved"] == 2

        library = SnippetLibrary(session)
        rev = library.find_by_key(
            snippet_type="query", schema_mapping_id=source_id, standard_field="revenue"
        )
        assert rev is not None
        costs = library.find_by_key(
            snippet_type="query", schema_mapping_id=source_id, standard_field="costs"
        )
        assert costs is not None

    def test_recursive_cte_falls_back(self, cursor: duckdb.DuckDBPyConnection) -> None:
        sql = (
            "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 5) "
            "SELECT * FROM cnt"
        )
        result = run_sql(cursor, sql=sql)
        assert "error" not in result
        # Falls back to monolithic — single step with step_id "query"
        assert len(result["steps_executed"]) == 1
        assert result["steps_executed"][0]["step_id"] == "query"

    def test_no_cte_passes_through(self, cursor: duckdb.DuckDBPyConnection) -> None:
        result = run_sql(cursor, sql="SELECT * FROM orders")
        assert "error" not in result
        assert len(result["steps_executed"]) == 1
        assert result["steps_executed"][0]["step_id"] == "query"

    def test_column_mappings_distributed(self) -> None:
        sql = (
            "WITH revenue AS (SELECT region, SUM(amount) AS total FROM orders GROUP BY region), "
            "costs AS (SELECT region, COUNT(*) AS n FROM orders GROUP BY region) "
            "SELECT * FROM revenue JOIN costs USING (region)"
        )
        result = decompose_ctes(sql, column_mappings={"rev": "amount", "cnt": "n"})
        assert result is not None
        # "amount" is referenced in the revenue CTE
        rev_step = result.steps[0]
        assert rev_step["step_id"] == "revenue"
        assert rev_step.get("column_mappings") == {"rev": "amount"}

    def test_parse_error_falls_back(self) -> None:
        result = decompose_ctes("THIS IS NOT SQL AT ALL !!!")
        assert result is None

    def test_quoted_alias_falls_back(self, cursor: duckdb.DuckDBPyConnection) -> None:
        """CTE aliases with spaces are not safe for CREATE TEMP VIEW."""
        sql = 'WITH "my cte" AS (SELECT 1 AS x) SELECT * FROM "my cte"'
        result = decompose_ctes(sql)
        assert result is None
        # Also verify run_sql still executes it via monolithic fallback
        run_result = run_sql(cursor, sql=sql)
        assert "error" not in run_result
        assert run_result["steps_executed"][0]["step_id"] == "query"

    def test_qualified_column_mappings_distributed(self) -> None:
        """Qualified 'table.column' mappings should match by column name."""
        sql = (
            "WITH revenue AS (SELECT region, SUM(amount) AS total FROM orders GROUP BY region) "
            "SELECT * FROM revenue"
        )
        result = decompose_ctes(sql, column_mappings={"rev": "orders.amount"})
        assert result is not None
        assert result.steps[0].get("column_mappings") == {"rev": "orders.amount"}

    def test_empty_steps_returns_error(self, cursor: duckdb.DuckDBPyConnection) -> None:
        result = run_sql(cursor, steps=[])
        assert "error" in result
        assert "empty" in result["error"]
