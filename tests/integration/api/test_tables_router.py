"""Tests for tables router endpoints."""

from fastapi.testclient import TestClient


class TestListTables:
    """Tests for GET /api/v1/tables."""

    def test_empty_list(self, test_client: TestClient):
        """Returns empty list when no tables exist."""
        response = test_client.get("/api/v1/tables")
        assert response.status_code == 200

        data = response.json()
        assert data["tables"] == []
        assert data["total"] == 0

    def test_list_with_tables(self, test_client: TestClient, seeded_tables: dict):
        """Returns tables when they exist."""
        response = test_client.get("/api/v1/tables")
        assert response.status_code == 200

        data = response.json()
        assert data["total"] == 2
        assert len(data["tables"]) == 2

        # Check table names are present
        table_names = {t["name"] for t in data["tables"]}
        assert "customers" in table_names
        assert "orders" in table_names

    def test_filter_by_source(self, test_client: TestClient, seeded_tables: dict):
        """Filters tables by source_id."""
        response = test_client.get(f"/api/v1/tables?source_id={seeded_tables['source_id']}")
        assert response.status_code == 200
        assert response.json()["total"] == 2

        # Filter by non-existent source
        response = test_client.get("/api/v1/tables?source_id=nonexistent")
        assert response.status_code == 200
        assert response.json()["total"] == 0

    def test_includes_columns(self, test_client: TestClient, seeded_tables: dict):
        """Tables include their columns."""
        response = test_client.get("/api/v1/tables")
        assert response.status_code == 200

        data = response.json()
        customers_table = next(t for t in data["tables"] if t["name"] == "customers")
        assert len(customers_table["columns"]) == 2

        # Check column order
        assert customers_table["columns"][0]["name"] == "id"
        assert customers_table["columns"][1]["name"] == "name"


class TestGetTable:
    """Tests for GET /api/v1/tables/{table_id}."""

    def test_not_found(self, test_client: TestClient):
        """Returns 404 for non-existent table."""
        response = test_client.get("/api/v1/tables/nonexistent")
        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_get_existing(self, test_client: TestClient, seeded_tables: dict):
        """Returns table details when it exists."""
        response = test_client.get("/api/v1/tables/table-1")
        assert response.status_code == 200

        data = response.json()
        assert data["table_id"] == "table-1"
        assert data["name"] == "customers"
        assert data["row_count"] == 100
        assert len(data["columns"]) == 2


class TestGetColumn:
    """Tests for GET /api/v1/columns/{column_id}."""

    def test_not_found(self, test_client: TestClient):
        """Returns 404 for non-existent column."""
        response = test_client.get("/api/v1/columns/nonexistent")
        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_get_existing(self, test_client: TestClient, seeded_tables: dict):
        """Returns column details when it exists."""
        response = test_client.get("/api/v1/columns/col-1")
        assert response.status_code == 200

        data = response.json()
        assert data["column_id"] == "col-1"
        assert data["name"] == "id"
        assert data["position"] == 0
        assert data["resolved_type"] == "INTEGER"
