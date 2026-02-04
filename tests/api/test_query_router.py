"""Tests for query router endpoints."""

from fastapi.testclient import TestClient


class TestExecuteQuery:
    """Tests for POST /api/v1/query."""

    def test_simple_select(self, test_client: TestClient):
        """Executes a simple SELECT query."""
        response = test_client.post(
            "/api/v1/query",
            json={"sql": "SELECT 1 as num, 'hello' as msg"},
        )
        assert response.status_code == 200

        data = response.json()
        assert data["columns"] == ["num", "msg"]
        assert len(data["rows"]) == 1
        assert data["rows"][0]["num"] == 1
        assert data["rows"][0]["msg"] == "hello"
        assert data["row_count"] == 1
        assert data["truncated"] is False

    def test_rejects_non_select(self, test_client: TestClient):
        """Rejects non-SELECT queries."""
        # INSERT
        response = test_client.post(
            "/api/v1/query",
            json={"sql": "INSERT INTO foo VALUES (1)"},
        )
        assert response.status_code == 400
        assert "SELECT" in response.json()["detail"]

        # UPDATE
        response = test_client.post(
            "/api/v1/query",
            json={"sql": "UPDATE foo SET x = 1"},
        )
        assert response.status_code == 400

        # DELETE
        response = test_client.post(
            "/api/v1/query",
            json={"sql": "DELETE FROM foo"},
        )
        assert response.status_code == 400

    def test_rejects_dangerous_keywords(self, test_client: TestClient):
        """Rejects queries with dangerous keywords."""
        dangerous_queries = [
            "SELECT * FROM foo; DROP TABLE foo",
            "SELECT * FROM foo; CREATE TABLE bar (x INT)",
            "SELECT * FROM foo; ALTER TABLE foo ADD COLUMN y INT",
            "SELECT * FROM foo; TRUNCATE foo",
        ]
        for sql in dangerous_queries:
            response = test_client.post("/api/v1/query", json={"sql": sql})
            assert response.status_code == 400
            assert "forbidden keyword" in response.json()["detail"].lower()

    def test_respects_limit(self, test_client: TestClient):
        """Respects the limit parameter."""
        response = test_client.post(
            "/api/v1/query",
            json={
                "sql": "SELECT * FROM generate_series(1, 100) as t(n)",
                "limit": 10,
            },
        )
        assert response.status_code == 200

        data = response.json()
        assert data["row_count"] == 10
        # Note: truncated is only True if the DB returned more than limit
        # Since we add LIMIT to the SQL, the DB only returns limit rows

    def test_adds_default_limit(self, test_client: TestClient):
        """Adds default limit when not specified in query."""
        response = test_client.post(
            "/api/v1/query",
            json={"sql": "SELECT * FROM generate_series(1, 10) as t(n)"},
        )
        assert response.status_code == 200
        # Should work without hitting the default 1000 limit
        assert response.json()["row_count"] == 10

    def test_invalid_sql(self, test_client: TestClient):
        """Returns error for invalid SQL."""
        response = test_client.post(
            "/api/v1/query",
            json={"sql": "SELECT * FROM nonexistent_table_xyz"},
        )
        assert response.status_code == 400
        assert "execution failed" in response.json()["detail"].lower()

    def test_syntax_error(self, test_client: TestClient):
        """Returns error for SQL syntax errors."""
        response = test_client.post(
            "/api/v1/query",
            json={"sql": "SELECT FROM WHERE"},
        )
        assert response.status_code == 400
