"""Tests for metric induction — save_metrics_config."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import yaml

from dataraum.graphs.induction import save_metrics_config


class TestSaveMetricsConfig:
    def test_writes_metric_files(self, tmp_path: Path) -> None:
        metrics_dir = tmp_path / "metrics"
        metrics_dir.mkdir()

        metrics = [
            {
                "graph_id": "gross_margin",
                "metadata": {
                    "name": "Gross Margin",
                    "description": "Margin percentage",
                    "category": "profitability",
                },
                "output": {"type": "scalar", "metric_id": "gross_margin", "unit": "percent"},
                "dependencies": {
                    "revenue": {
                        "level": 1,
                        "type": "extract",
                        "source": {"standard_field": "revenue"},
                        "aggregation": "sum",
                    },
                },
            },
            {
                "graph_id": "dso",
                "metadata": {
                    "name": "DSO",
                    "description": "Days sales outstanding",
                    "category": "working_capital",
                },
                "output": {"type": "scalar", "metric_id": "dso", "unit": "days"},
                "dependencies": {
                    "ar": {
                        "level": 1,
                        "type": "extract",
                        "source": {"standard_field": "accounts_receivable"},
                        "aggregation": "end_of_period",
                    },
                },
            },
        ]

        with patch("dataraum.core.vertical.VerticalConfig") as mock_vc:
            mock_vc.return_value.metrics_dir = metrics_dir
            save_metrics_config("_adhoc", metrics)

        # Check profitability/gross_margin.yaml
        gm_path = metrics_dir / "profitability" / "gross_margin.yaml"
        assert gm_path.exists()
        gm = yaml.safe_load(gm_path.read_text())
        assert gm["graph_id"] == "gross_margin"
        assert gm["graph_type"] == "metric"
        assert gm["metadata"]["source"] == "induced"

        # Check working_capital/dso.yaml
        dso_path = metrics_dir / "working_capital" / "dso.yaml"
        assert dso_path.exists()
        dso = yaml.safe_load(dso_path.read_text())
        assert dso["graph_id"] == "dso"

    def test_creates_category_directories(self, tmp_path: Path) -> None:
        metrics_dir = tmp_path / "metrics"
        metrics_dir.mkdir()

        metrics = [
            {
                "graph_id": "test",
                "metadata": {
                    "name": "Test",
                    "description": "test",
                    "category": "new_category",
                },
                "output": {"type": "scalar", "metric_id": "test", "unit": "ratio"},
                "dependencies": {},
            },
        ]

        with patch("dataraum.core.vertical.VerticalConfig") as mock_vc:
            mock_vc.return_value.metrics_dir = metrics_dir
            save_metrics_config("_adhoc", metrics)

        assert (metrics_dir / "new_category" / "test.yaml").exists()

    def test_handles_empty_list(self, tmp_path: Path) -> None:
        metrics_dir = tmp_path / "metrics"
        metrics_dir.mkdir()

        with patch("dataraum.core.vertical.VerticalConfig") as mock_vc:
            mock_vc.return_value.metrics_dir = metrics_dir
            save_metrics_config("_adhoc", [])

        # No files created
        assert list(metrics_dir.rglob("*.yaml")) == []
