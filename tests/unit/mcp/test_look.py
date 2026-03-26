"""Tests for look MCP tool."""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch
from uuid import uuid4

import duckdb
from sqlalchemy.orm import Session

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.semantic.db_models import SemanticAnnotation, TableEntity
from dataraum.analysis.statistics.db_models import StatisticalProfile
from dataraum.analysis.typing.db_models import TypeCandidate, TypeDecision
from dataraum.storage import Column, Source, Table


def _id() -> str:
    return str(uuid4())


@contextmanager
def _mock_manager(session: Session, duckdb_conn=None):
    """Mock get_manager_for_directory with test session and optional DuckDB cursor."""
    manager = MagicMock()
    manager.session_scope.return_value.__enter__ = lambda _: session
    manager.session_scope.return_value.__exit__ = lambda *_: None
    if duckdb_conn is not None:
        manager.duckdb_cursor.return_value.__enter__ = lambda _: duckdb_conn
        manager.duckdb_cursor.return_value.__exit__ = lambda *_: None

    with patch("dataraum.core.connections.get_manager_for_directory", return_value=manager):
        yield manager


def _setup_source_and_table(
    session: Session,
    table_name: str = "orders",
    columns: list[str] | None = None,
    row_count: int = 100,
) -> tuple[str, str, list[tuple[str, str]]]:
    """Insert Source + Table + Columns. Returns (source_id, table_id, [(col_id, col_name)])."""
    source_id = _id()
    table_id = _id()
    cols = columns or ["id", "amount", "region"]

    session.add(Source(source_id=source_id, name="test_source", source_type="csv"))
    session.add(
        Table(
            table_id=table_id,
            source_id=source_id,
            table_name=table_name,
            layer="typed",
            duckdb_path=f"typed_{table_name}",
            row_count=row_count,
        )
    )

    col_ids = []
    for i, name in enumerate(cols):
        col_id = _id()
        col_ids.append((col_id, name))
        session.add(
            Column(
                column_id=col_id,
                table_id=table_id,
                column_name=name,
                column_position=i,
                resolved_type="BIGINT" if name == "amount" else "VARCHAR",
            )
        )

    session.flush()
    return source_id, table_id, col_ids


class TestLookDataset:
    def test_returns_tables_with_columns(self, session: Session, tmp_path) -> None:
        source_id, table_id, col_ids = _setup_source_and_table(session)

        with (
            _mock_manager(session),
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
        ):
            from dataraum.mcp.server import _look

            result = _look(tmp_path)

        assert "tables" in result
        assert len(result["tables"]) == 1
        tbl = result["tables"][0]
        assert tbl["name"] == "orders"
        assert tbl["row_count"] == 100
        assert len(tbl["columns"]) == 3
        col_names = [c["name"] for c in tbl["columns"]]
        assert "amount" in col_names

    def test_includes_semantic_annotations(self, session: Session, tmp_path) -> None:
        source_id, table_id, col_ids = _setup_source_and_table(session)
        amount_id = col_ids[1][0]  # "amount"

        session.add(
            SemanticAnnotation(
                column_id=amount_id,
                semantic_role="measure",
                business_name="Order Amount",
                business_concept="revenue",
                temporal_behavior="additive",
            )
        )
        session.flush()

        with (
            _mock_manager(session),
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
        ):
            from dataraum.mcp.server import _look

            result = _look(tmp_path)

        amount_col = next(c for c in result["tables"][0]["columns"] if c["name"] == "amount")
        assert amount_col["semantic_role"] == "measure"
        assert amount_col["business_name"] == "Order Amount"
        assert amount_col["business_concept"] == "revenue"
        assert amount_col["temporal_behavior"] == "additive"

    def test_includes_table_entity(self, session: Session, tmp_path) -> None:
        source_id, table_id, col_ids = _setup_source_and_table(session)

        session.add(
            TableEntity(
                table_id=table_id,
                detected_entity_type="transaction",
                is_fact_table=True,
                description="Customer orders",
                time_column="created_at",
            )
        )
        session.flush()

        with (
            _mock_manager(session),
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
        ):
            from dataraum.mcp.server import _look

            result = _look(tmp_path)

        tbl = result["tables"][0]
        assert tbl["entity_type"] == "transaction"
        assert tbl["is_fact_table"] is True
        assert tbl["description"] == "Customer orders"
        assert tbl["time_column"] == "created_at"

    def test_only_llm_relationships(self, session: Session, tmp_path) -> None:
        """Only LLM-confirmed relationships are included, not candidates."""
        source_id, table_id, col_ids = _setup_source_and_table(session)
        # Create a second table for relationships
        table2_id = _id()
        session.add(
            Table(
                table_id=table2_id,
                source_id=source_id,
                table_name="customers",
                layer="typed",
                duckdb_path="typed_customers",
            )
        )
        cust_col_id = _id()
        session.add(
            Column(
                column_id=cust_col_id,
                table_id=table2_id,
                column_name="customer_id",
                column_position=0,
            )
        )
        session.flush()

        orders_id_col = col_ids[0][0]  # "id"

        # LLM relationship — should appear
        session.add(
            Relationship(
                from_table_id=table_id,
                from_column_id=orders_id_col,
                to_table_id=table2_id,
                to_column_id=cust_col_id,
                relationship_type="foreign_key",
                cardinality="many-to-one",
                confidence=0.95,
                detection_method="llm",
            )
        )
        # Candidate relationship — should NOT appear
        session.add(
            Relationship(
                from_table_id=table_id,
                from_column_id=orders_id_col,
                to_table_id=table2_id,
                to_column_id=cust_col_id,
                relationship_type="candidate",
                cardinality="many-to-one",
                confidence=0.6,
                detection_method="candidate",
            )
        )
        session.flush()

        with (
            _mock_manager(session),
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
        ):
            from dataraum.mcp.server import _look

            result = _look(tmp_path)

        assert len(result["relationships"]) == 1
        rel = result["relationships"][0]
        assert rel["type"] == "foreign_key"
        assert rel["confidence"] == 0.95

    def test_includes_unit_source_column(self, session: Session, tmp_path) -> None:
        """unit_source_column from SemanticAnnotation is included at dataset level."""
        source_id, table_id, col_ids = _setup_source_and_table(session)
        amount_id = col_ids[1][0]  # "amount"

        session.add(
            SemanticAnnotation(
                column_id=amount_id,
                semantic_role="measure",
                unit_source_column="currency",
            )
        )
        session.flush()

        with (
            _mock_manager(session),
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
        ):
            from dataraum.mcp.server import _look

            result = _look(tmp_path)

        amount_col = next(c for c in result["tables"][0]["columns"] if c["name"] == "amount")
        assert amount_col["unit_source_column"] == "currency"

    def test_no_entropy_in_response(self, session: Session, tmp_path) -> None:
        """look never returns entropy scores or readiness."""
        source_id, table_id, col_ids = _setup_source_and_table(session)

        with (
            _mock_manager(session),
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
        ):
            from dataraum.mcp.server import _look

            result = _look(tmp_path)

        result_str = str(result)
        assert "entropy" not in result_str.lower()
        assert "readiness" not in result_str.lower()


class TestLookTable:
    def test_includes_column_stats(self, session: Session, tmp_path) -> None:
        source_id, table_id, col_ids = _setup_source_and_table(session)
        amount_id = col_ids[1][0]

        session.add(
            StatisticalProfile(
                column_id=amount_id,
                layer="typed",
                total_count=100,
                null_count=5,
                distinct_count=80,
                cardinality_ratio=0.8,
                profile_data={
                    "numeric_stats": {"min": 0, "max": 1000, "mean": 250.5},
                    "top_values": [
                        {"value": "100", "count": 10, "pct": 0.1},
                        {"value": "200", "count": 8, "pct": 0.08},
                    ],
                },
            )
        )
        session.flush()

        with (
            _mock_manager(session),
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
        ):
            from dataraum.mcp.server import _look

            result = _look(tmp_path, target="orders")

        assert result["name"] == "orders"
        amount_col = next(c for c in result["columns"] if c["name"] == "amount")
        assert "stats" in amount_col
        assert amount_col["stats"]["total_count"] == 100
        assert amount_col["stats"]["null_count"] == 5
        assert "numeric" in amount_col["stats"]
        assert amount_col["stats"]["numeric"]["mean"] == 250.5

    def test_includes_nullable_from_profile(self, session: Session, tmp_path) -> None:
        """nullable derived from StatisticalProfile.null_count > 0."""
        source_id, table_id, col_ids = _setup_source_and_table(session)
        amount_id = col_ids[1][0]
        region_id = col_ids[2][0]

        # amount has nulls
        session.add(
            StatisticalProfile(
                column_id=amount_id,
                layer="typed",
                total_count=100,
                null_count=5,
                distinct_count=80,
                cardinality_ratio=0.8,
                profile_data={},
            )
        )
        # region has no nulls
        session.add(
            StatisticalProfile(
                column_id=region_id,
                layer="typed",
                total_count=100,
                null_count=0,
                distinct_count=3,
                cardinality_ratio=0.03,
                profile_data={},
            )
        )
        session.flush()

        with (
            _mock_manager(session),
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
        ):
            from dataraum.mcp.server import _look

            result = _look(tmp_path, target="orders")

        amount_col = next(c for c in result["columns"] if c["name"] == "amount")
        region_col = next(c for c in result["columns"] if c["name"] == "region")
        assert amount_col["nullable"] is True
        assert region_col["nullable"] is False

    def test_includes_unit_source_column(self, session: Session, tmp_path) -> None:
        """unit_source_column included at table level."""
        source_id, table_id, col_ids = _setup_source_and_table(session)
        amount_id = col_ids[1][0]

        session.add(
            SemanticAnnotation(
                column_id=amount_id,
                semantic_role="measure",
                unit_source_column="currency",
            )
        )
        session.flush()

        with (
            _mock_manager(session),
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
        ):
            from dataraum.mcp.server import _look

            result = _look(tmp_path, target="orders")

        amount_col = next(c for c in result["columns"] if c["name"] == "amount")
        assert amount_col["unit_source_column"] == "currency"


class TestLookColumn:
    def test_includes_type_candidates(self, session: Session, tmp_path) -> None:
        source_id, table_id, col_ids = _setup_source_and_table(session)
        amount_id = col_ids[1][0]

        session.add(
            TypeCandidate(
                column_id=amount_id,
                data_type="BIGINT",
                confidence=0.95,
                parse_success_rate=0.99,
                detected_pattern="integer",
            )
        )
        session.add(
            TypeDecision(
                column_id=amount_id,
                decided_type="BIGINT",
                decision_source="automatic",
                decision_reason="highest confidence candidate",
            )
        )
        session.flush()

        with (
            _mock_manager(session),
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
        ):
            from dataraum.mcp.server import _look

            result = _look(tmp_path, target="orders.amount")

        assert result["name"] == "amount"
        assert result["table"] == "orders"
        assert result["type"] == "BIGINT"
        assert len(result["type_candidates"]) == 1
        assert result["type_candidates"][0]["type"] == "BIGINT"
        assert result["type_decision"]["type"] == "BIGINT"
        assert result["type_decision"]["source"] == "automatic"

    def test_includes_semantic_and_quality(self, session: Session, tmp_path) -> None:
        source_id, table_id, col_ids = _setup_source_and_table(session)
        amount_id = col_ids[1][0]

        session.add(
            SemanticAnnotation(
                column_id=amount_id,
                semantic_role="measure",
                business_name="Amount",
            )
        )
        session.flush()

        with (
            _mock_manager(session),
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
        ):
            from dataraum.mcp.server import _look

            result = _look(tmp_path, target="orders.amount")

        assert result["semantic"]["role"] == "measure"
        assert result["semantic"]["business_name"] == "Amount"


class TestLookSample:
    def test_returns_rows(self, session: Session, tmp_path) -> None:
        source_id, table_id, col_ids = _setup_source_and_table(session)

        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE typed_orders (id INT, amount DOUBLE, region VARCHAR)")
        conn.execute("INSERT INTO typed_orders VALUES (1, 100.0, 'US'), (2, 200.0, 'EU')")

        with (
            _mock_manager(session, duckdb_conn=conn),
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
        ):
            from dataraum.mcp.server import _look

            result = _look(tmp_path, target="orders", sample=10)

        assert result["table"] == "orders"
        assert result["row_count"] == 2
        assert len(result["columns"]) == 3
        assert len(result["rows"]) == 2
        conn.close()


class TestLookErrors:
    def test_unknown_table(self, session: Session, tmp_path) -> None:
        source_id, table_id, col_ids = _setup_source_and_table(session)

        with (
            _mock_manager(session),
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
        ):
            from dataraum.mcp.server import _look

            result = _look(tmp_path, target="nonexistent")

        assert "error" in result
        assert "nonexistent" in result["error"]
        assert "orders" in str(result["error"])  # Shows available tables

    def test_unknown_column(self, session: Session, tmp_path) -> None:
        source_id, table_id, col_ids = _setup_source_and_table(session)

        with (
            _mock_manager(session),
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
        ):
            from dataraum.mcp.server import _look

            result = _look(tmp_path, target="orders.nonexistent")

        assert "error" in result

    def test_sample_without_table(self, session: Session, tmp_path) -> None:
        source_id, table_id, col_ids = _setup_source_and_table(session)

        with (
            _mock_manager(session),
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
        ):
            from dataraum.mcp.server import _look

            result = _look(tmp_path, sample=10)

        assert "error" in result

    def test_target_parsing(self, session: Session, tmp_path) -> None:
        """No target = dataset, 'table' = table, 'table.col' = column."""
        source_id, table_id, col_ids = _setup_source_and_table(session)

        with (
            _mock_manager(session),
            patch(
                "dataraum.mcp.server._get_pipeline_source",
                return_value=session.get(Source, source_id),
            ),
        ):
            from dataraum.mcp.server import _look

            # Dataset level
            dataset = _look(tmp_path)
            assert "tables" in dataset
            assert "relationships" in dataset

            # Table level
            table = _look(tmp_path, target="orders")
            assert "columns" in table
            assert table.get("name") == "orders"
            assert "tables" not in table  # Not dataset shape

            # Column level
            column = _look(tmp_path, target="orders.amount")
            assert column.get("name") == "amount"
            assert column.get("table") == "orders"
            assert "tables" not in column
