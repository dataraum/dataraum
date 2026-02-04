"""Tests for sources router endpoints."""

from fastapi.testclient import TestClient


class TestListSources:
    """Tests for GET /api/v1/sources."""

    def test_empty_list(self, test_client: TestClient):
        """Returns empty list when no sources exist."""
        response = test_client.get("/api/v1/sources")
        assert response.status_code == 200

        data = response.json()
        assert data["sources"] == []
        assert data["total"] == 0

    def test_list_with_source(self, test_client: TestClient, seeded_source: dict):
        """Returns sources when they exist."""
        response = test_client.get("/api/v1/sources")
        assert response.status_code == 200

        data = response.json()
        assert data["total"] == 1
        assert len(data["sources"]) == 1
        assert data["sources"][0]["source_id"] == seeded_source["source_id"]
        assert data["sources"][0]["name"] == "Test Source"
        assert data["sources"][0]["source_type"] == "csv"

    def test_pagination(self, test_client: TestClient):
        """Pagination parameters work correctly."""
        # Create multiple sources
        for i in range(5):
            test_client.post(
                "/api/v1/sources",
                json={"name": f"Source {i}", "source_type": "csv"},
            )

        # Test skip
        response = test_client.get("/api/v1/sources?skip=2&limit=2")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 5
        assert len(data["sources"]) == 2


class TestGetSource:
    """Tests for GET /api/v1/sources/{source_id}."""

    def test_not_found(self, test_client: TestClient):
        """Returns 404 for non-existent source."""
        response = test_client.get("/api/v1/sources/nonexistent")
        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_get_existing(self, test_client: TestClient, seeded_source: dict):
        """Returns source details when it exists."""
        response = test_client.get(f"/api/v1/sources/{seeded_source['source_id']}")
        assert response.status_code == 200

        data = response.json()
        assert data["source_id"] == seeded_source["source_id"]
        assert data["name"] == "Test Source"
        assert data["source_type"] == "csv"
        assert data["table_count"] == 0


class TestCreateSource:
    """Tests for POST /api/v1/sources."""

    def test_create_minimal(self, test_client: TestClient):
        """Creates source with minimal fields."""
        response = test_client.post(
            "/api/v1/sources",
            json={"name": "New Source", "source_type": "csv"},
        )
        assert response.status_code == 201

        data = response.json()
        assert data["name"] == "New Source"
        assert data["source_type"] == "csv"
        assert data["source_id"] is not None
        assert data["path"] is None
        assert data["table_count"] == 0

    def test_create_with_path(self, test_client: TestClient):
        """Creates source with path."""
        response = test_client.post(
            "/api/v1/sources",
            json={
                "name": "Source With Path",
                "source_type": "parquet",
                "path": "/data/files",
            },
        )
        assert response.status_code == 201

        data = response.json()
        assert data["name"] == "Source With Path"
        assert data["source_type"] == "parquet"
        assert data["path"] == "/data/files"

    def test_create_missing_required(self, test_client: TestClient):
        """Returns 422 when required fields are missing."""
        response = test_client.post(
            "/api/v1/sources",
            json={"name": "Only Name"},
        )
        assert response.status_code == 422
