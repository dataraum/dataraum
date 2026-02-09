"""Tests for the reports router (HTML query reports)."""

from datetime import UTC, datetime

from fastapi.testclient import TestClient


class TestQueryReport:
    """Tests for GET /reports/query/{execution_id}."""

    def test_404_for_nonexistent_execution(self, test_client: TestClient):
        """Returns 404 for a non-existent execution ID."""
        response = test_client.get("/reports/query/nonexistent-id")
        assert response.status_code == 404

    def test_renders_html_with_seeded_execution(self, test_client: TestClient, seeded_source: dict):
        """Returns HTML report for a seeded execution record."""
        from dataraum.core.connections import get_connection_manager
        from dataraum.query.db_models import QueryExecutionRecord

        manager = get_connection_manager()
        with manager.session_scope() as session:
            execution = QueryExecutionRecord(
                execution_id="test-exec-1",
                source_id=seeded_source["source_id"],
                question="What is the total value?",
                sql_executed="SELECT SUM(value) FROM test_data",
                success=True,
                row_count=1,
                confidence_level="GREEN",
                executed_at=datetime.now(UTC),
            )
            session.add(execution)
            session.commit()

        response = test_client.get("/reports/query/test-exec-1")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

        # Check key content in HTML
        html = response.text
        assert "What is the total value?" in html
        assert "High Confidence" in html
        assert "DataRaum" in html  # Base template rendered

    def test_graceful_handling_no_library_entry(self, test_client: TestClient, seeded_source: dict):
        """Renders report even when execution has no linked library entry."""
        from dataraum.core.connections import get_connection_manager
        from dataraum.query.db_models import QueryExecutionRecord

        manager = get_connection_manager()
        with manager.session_scope() as session:
            execution = QueryExecutionRecord(
                execution_id="test-exec-2",
                source_id=seeded_source["source_id"],
                question="Simple query",
                sql_executed="SELECT 1",
                success=True,
                confidence_level="YELLOW",
                executed_at=datetime.now(UTC),
            )
            session.add(execution)
            session.commit()

        response = test_client.get("/reports/query/test-exec-2")
        assert response.status_code == 200
        assert "Simple query" in response.text
        assert "Moderate Confidence" in response.text

    def test_renders_entropy_action_alert(self, test_client: TestClient, seeded_source: dict):
        """Renders entropy action alert card when entropy_action is set."""
        from dataraum.core.connections import get_connection_manager
        from dataraum.query.db_models import QueryExecutionRecord

        manager = get_connection_manager()
        with manager.session_scope() as session:
            execution = QueryExecutionRecord(
                execution_id="test-exec-ea",
                source_id=seeded_source["source_id"],
                question="Revenue with assumptions",
                sql_executed="SELECT SUM(value) FROM test_data",
                success=True,
                confidence_level="YELLOW",
                entropy_action="answer_with_assumptions",
                executed_at=datetime.now(UTC),
            )
            session.add(execution)
            session.commit()

        response = test_client.get("/reports/query/test-exec-ea")
        assert response.status_code == 200
        html = response.text
        assert "Some assumptions were needed due to data uncertainty" in html
        assert "alert-warning" in html

    def test_renders_with_library_entry_and_assumptions(
        self, test_client: TestClient, seeded_source: dict
    ):
        """Renders assumptions from linked library entry."""
        from dataraum.core.connections import get_connection_manager
        from dataraum.query.db_models import QueryExecutionRecord, QueryLibraryEntry

        manager = get_connection_manager()
        with manager.session_scope() as session:
            # Create library entry with assumptions
            entry = QueryLibraryEntry(
                query_id="lib-entry-1",
                source_id=seeded_source["source_id"],
                original_question="Revenue question",
                summary="Calculate total revenue",
                steps_json=[],
                final_sql="SELECT SUM(value) FROM test_data",
                column_mappings={},
                assumptions=[
                    {
                        "dimension": "semantic.units",
                        "target": "column:value",
                        "assumption": "Currency is EUR",
                        "basis": "inferred",
                        "confidence": 0.8,
                    }
                ],
                embedding_text="total revenue",
                confidence_level="GREEN",
            )
            session.add(entry)
            session.flush()

            execution = QueryExecutionRecord(
                execution_id="test-exec-3",
                source_id=seeded_source["source_id"],
                library_entry_id="lib-entry-1",
                question="What is total revenue?",
                sql_executed="SELECT SUM(value) FROM test_data",
                success=True,
                confidence_level="GREEN",
                executed_at=datetime.now(UTC),
            )
            session.add(execution)
            session.commit()

        response = test_client.get("/reports/query/test-exec-3")
        assert response.status_code == 200
        html = response.text
        assert "Currency is EUR" in html
        assert "semantic.units" in html
