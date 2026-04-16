"""Tests for metric teach type (DAT-253).

Tests the teach(type="metric") handler: writes metric YAML, idempotent
overwrites, creates DataFix record.
"""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import yaml
from sqlalchemy.orm import Session

from dataraum.mcp.teach import handle_teach
from dataraum.pipeline.fixes.models import DataFix
from dataraum.storage import Column, Source, Table


def _id() -> str:
    return str(uuid4())


def _setup_typed_tables(session: Session) -> str:
    """Create minimal Source + Table + Column for teach dispatch."""
    source_id = _id()
    session.add(Source(source_id=source_id, name="test_source", source_type="csv"))
    table_id = _id()
    session.add(
        Table(
            table_id=table_id,
            source_id=source_id,
            table_name="orders",
            layer="typed",
            duckdb_path="typed_orders",
            row_count=100,
        )
    )
    session.add(
        Column(
            column_id=_id(),
            table_id=table_id,
            column_name="amount",
            column_position=0,
            resolved_type="DOUBLE",
        )
    )
    session.flush()
    return source_id


def _make_config_root(tmp_path: Path) -> Path:
    """Create a config root with metrics directory."""
    config_root = tmp_path / "config"
    config_root.mkdir()
    # Verticals
    verticals_dir = config_root / "verticals" / "_adhoc"
    verticals_dir.mkdir(parents=True)
    (verticals_dir / "metrics").mkdir()
    return config_root


class TestTeachMetric:
    def test_writes_metric_yaml(self, session: Session, tmp_path: Path) -> None:
        source_id = _setup_typed_tables(session)
        config_root = _make_config_root(tmp_path)

        result = handle_teach(
            "metric",
            {
                "graph_id": "gross_margin",
                "name": "Gross Margin",
                "description": "Revenue minus cost of goods sold as a percentage",
                "category": "profitability",
                "unit": "percent",
                "dependencies": {
                    "revenue": {
                        "level": 1,
                        "type": "extract",
                        "source": {"standard_field": "revenue"},
                        "aggregation": "sum",
                    },
                    "cogs": {
                        "level": 1,
                        "type": "extract",
                        "source": {"standard_field": "cost_of_goods_sold"},
                        "aggregation": "sum",
                    },
                    "gross_margin": {
                        "level": 2,
                        "type": "formula",
                        "expression": "((revenue - cogs) / revenue) * 100",
                        "depends_on": ["revenue", "cogs"],
                        "output_step": True,
                    },
                },
            },
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            config_root=config_root,
        )

        assert result["status"] == "applied"
        assert result["type"] == "metric"
        assert "teaching_id" in result
        assert "graph_execution" in result["measurement_hint"]

        # Verify YAML was written
        metric_path = (
            config_root / "verticals" / "_adhoc" / "metrics" / "profitability" / "gross_margin.yaml"
        )
        assert metric_path.exists()
        metric = yaml.safe_load(metric_path.read_text())
        assert metric["graph_id"] == "gross_margin"
        assert metric["graph_type"] == "metric"
        assert metric["metadata"]["name"] == "Gross Margin"
        assert metric["metadata"]["source"] == "teach"
        assert "revenue" in metric["dependencies"]
        assert metric["output"]["unit"] == "percent"

    def test_idempotent_overwrites(self, session: Session, tmp_path: Path) -> None:
        source_id = _setup_typed_tables(session)
        config_root = _make_config_root(tmp_path)

        params = {
            "graph_id": "my_metric",
            "name": "My Metric",
            "description": "v1",
            "dependencies": {
                "val": {
                    "level": 1,
                    "type": "extract",
                    "source": {"standard_field": "amount"},
                    "aggregation": "sum",
                    "output_step": True,
                },
            },
        }

        handle_teach(
            "metric",
            params,
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            config_root=config_root,
        )

        params["description"] = "v2"
        handle_teach(
            "metric",
            params,
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            config_root=config_root,
        )

        metric_path = (
            config_root / "verticals" / "_adhoc" / "metrics" / "general" / "my_metric.yaml"
        )
        metric = yaml.safe_load(metric_path.read_text())
        assert metric["metadata"]["description"] == "v2"

    def test_creates_datafix_record(self, session: Session, tmp_path: Path) -> None:
        source_id = _setup_typed_tables(session)
        config_root = _make_config_root(tmp_path)

        handle_teach(
            "metric",
            {
                "graph_id": "simple",
                "name": "Simple",
                "description": "test",
                "dependencies": {
                    "val": {
                        "level": 1,
                        "type": "extract",
                        "source": {"standard_field": "amount"},
                        "aggregation": "sum",
                        "output_step": True,
                    },
                },
            },
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            config_root=config_root,
        )

        fixes = session.query(DataFix).filter_by(source_id=source_id).all()
        assert len(fixes) == 1
        assert fixes[0].action == "metric"
        assert fixes[0].target == "config"
        assert fixes[0].status == "applied"

    def test_with_interpretation(self, session: Session, tmp_path: Path) -> None:
        source_id = _setup_typed_tables(session)
        config_root = _make_config_root(tmp_path)

        result = handle_teach(
            "metric",
            {
                "graph_id": "dso",
                "name": "DSO",
                "description": "Days sales outstanding",
                "category": "working_capital",
                "unit": "days",
                "dependencies": {
                    "ar": {
                        "level": 1,
                        "type": "extract",
                        "source": {"standard_field": "accounts_receivable"},
                        "aggregation": "end_of_period",
                    },
                    "revenue": {
                        "level": 1,
                        "type": "extract",
                        "source": {"standard_field": "revenue"},
                        "aggregation": "sum",
                    },
                    "dso": {
                        "level": 2,
                        "type": "formula",
                        "expression": "(ar / revenue) * 30",
                        "depends_on": ["ar", "revenue"],
                        "output_step": True,
                    },
                },
                "interpretation": {
                    "ranges": [
                        {"min": 0, "max": 30, "label": "GOOD", "description": "Efficient"},
                        {"min": 31, "max": 90, "label": "POOR", "description": "Slow"},
                    ],
                },
            },
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            config_root=config_root,
        )

        assert result["status"] == "applied"
        metric_path = (
            config_root / "verticals" / "_adhoc" / "metrics" / "working_capital" / "dso.yaml"
        )
        metric = yaml.safe_load(metric_path.read_text())
        assert metric["interpretation"]["ranges"][0]["label"] == "GOOD"

    def test_inspiration_snippet_id_in_yaml(self, session: Session, tmp_path: Path) -> None:
        """inspiration_snippet_id flows through to metric YAML metadata."""
        source_id = _setup_typed_tables(session)
        config_root = _make_config_root(tmp_path)

        result = handle_teach(
            "metric",
            {
                "graph_id": "promoted_metric",
                "name": "Promoted",
                "description": "From run_sql",
                "dependencies": {
                    "val": {
                        "level": 1,
                        "type": "extract",
                        "source": {"standard_field": "amount"},
                        "aggregation": "sum",
                        "output_step": True,
                    },
                },
                "inspiration_snippet_id": "snippet-abc-123",
            },
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            config_root=config_root,
        )

        assert result["status"] == "applied"
        metric_path = (
            config_root / "verticals" / "_adhoc" / "metrics" / "general" / "promoted_metric.yaml"
        )
        metric = yaml.safe_load(metric_path.read_text())
        assert metric["metadata"]["inspiration_snippet_id"] == "snippet-abc-123"

    def test_no_inspiration_snippet_id_omitted(self, session: Session, tmp_path: Path) -> None:
        """When inspiration_snippet_id is not provided, it's absent from YAML."""
        source_id = _setup_typed_tables(session)
        config_root = _make_config_root(tmp_path)

        handle_teach(
            "metric",
            {
                "graph_id": "normal_metric",
                "name": "Normal",
                "description": "No promotion",
                "dependencies": {
                    "val": {
                        "level": 1,
                        "type": "extract",
                        "source": {"standard_field": "amount"},
                        "aggregation": "sum",
                        "output_step": True,
                    },
                },
            },
            source_id=source_id,
            session=session,
            vertical="_adhoc",
            config_root=config_root,
        )

        metric_path = (
            config_root / "verticals" / "_adhoc" / "metrics" / "general" / "normal_metric.yaml"
        )
        metric = yaml.safe_load(metric_path.read_text())
        assert "inspiration_snippet_id" not in metric["metadata"]
