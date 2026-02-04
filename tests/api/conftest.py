"""Pytest fixtures for API tests."""

import os
from collections.abc import Generator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from dataraum.api.main import create_app
from dataraum.core.connections import close_default_manager


@pytest.fixture
def test_output_dir(tmp_path: Path) -> Path:
    """Create a temporary output directory for tests."""
    output_dir = tmp_path / "pipeline_output"
    output_dir.mkdir()
    return output_dir


@pytest.fixture
def test_client(test_output_dir: Path) -> Generator[TestClient]:
    """FastAPI test client with isolated database.

    Creates a fresh app instance with its own SQLite database
    for each test function.
    """
    # Set environment for the app
    os.environ["DATARAUM_OUTPUT_DIR"] = str(test_output_dir)

    app = create_app(output_dir=test_output_dir)

    with TestClient(app) as client:
        yield client

    # Cleanup
    close_default_manager()


@pytest.fixture
def seeded_source(test_client: TestClient, test_output_dir: Path) -> dict:
    """Create a test source with sample data.

    Returns dict with source_id and path to data.
    """
    from dataraum.core.connections import get_connection_manager
    from dataraum.storage import Source

    # Create a simple CSV file
    data_dir = test_output_dir / "data"
    data_dir.mkdir()
    csv_path = data_dir / "test.csv"
    csv_path.write_text("id,name,value\n1,Alice,100\n2,Bob,200\n3,Carol,300\n")

    # Create source in database
    manager = get_connection_manager()
    with manager.session_scope() as session:
        source = Source(
            source_id="test-source",
            name="Test Source",
            source_type="csv",
            connection_config={"path": str(data_dir)},
        )
        session.add(source)
        session.commit()

    return {
        "source_id": "test-source",
        "path": str(data_dir),
        "csv_path": str(csv_path),
    }


@pytest.fixture
def seeded_tables(test_client: TestClient, seeded_source: dict) -> dict:
    """Create test tables with columns.

    Returns dict with table_ids and column_ids.
    """
    from dataraum.core.connections import get_connection_manager
    from dataraum.storage import Column, Table

    manager = get_connection_manager()
    with manager.session_scope() as session:
        # Create tables
        table1 = Table(
            table_id="table-1",
            table_name="customers",
            source_id=seeded_source["source_id"],
            layer="typed",
            row_count=100,
        )
        table2 = Table(
            table_id="table-2",
            table_name="orders",
            source_id=seeded_source["source_id"],
            layer="typed",
            row_count=500,
        )
        session.add_all([table1, table2])
        session.flush()

        # Create columns for table1
        col1 = Column(
            column_id="col-1",
            table_id="table-1",
            column_name="id",
            column_position=0,
            resolved_type="INTEGER",
        )
        col2 = Column(
            column_id="col-2",
            table_id="table-1",
            column_name="name",
            column_position=1,
            resolved_type="VARCHAR",
        )
        col3 = Column(
            column_id="col-3",
            table_id="table-2",
            column_name="order_id",
            column_position=0,
            resolved_type="INTEGER",
        )
        session.add_all([col1, col2, col3])
        session.commit()

    return {
        "source_id": seeded_source["source_id"],
        "table_ids": ["table-1", "table-2"],
        "column_ids": ["col-1", "col-2", "col-3"],
    }
