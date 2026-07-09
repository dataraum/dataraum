"""Tests for the overlay-aware metric (transformation-graph) declared-set loader (DAT-456)."""

from __future__ import annotations

from pathlib import Path

import yaml

from dataraum.core.overlay import (
    OverlayRow,
    reset_overlay_resolver_for_tests,
    set_overlay_resolver,
)
from dataraum.graphs.config import get_metric_definitions, get_metrics_config


class TestGetMetricDefinitionsProduction:
    """Production path reads the shipped finance ``metrics/`` directory."""

    def teardown_method(self) -> None:
        reset_overlay_resolver_for_tests()

    def test_returns_all_finance_metrics(self) -> None:
        defs = get_metric_definitions("finance")
        # 13 P&L / working-capital / liquidity metrics + 3 activity metrics
        # (transaction_count, average_transaction_value, active_accounts — DAT-718).
        assert len(defs) == 16

    def test_keyed_by_graph_id_with_known_metrics(self) -> None:
        defs = get_metric_definitions("finance")
        # dio (Days Inventory Outstanding) is a first-class metric (DAT-591) — it
        # completes cash_conversion_cycle = dso + dio - dpo as a real composition.
        expected = {"dso", "dio", "dpo", "cash_conversion_cycle", "current_ratio", "ebitda"}
        assert expected.issubset(defs.keys())

    def test_definitions_carry_their_structure(self) -> None:
        defs = get_metric_definitions("finance")
        dso = defs["dso"]
        assert dso["graph_id"] == "dso"
        assert dso["metadata"]["category"] == "working_capital"
        assert "dependencies" in dso

    def test_unknown_vertical_is_empty_not_an_error(self) -> None:
        # Overlay-aware loader: an unknown vertical resolves to an EMPTY set,
        # never raises — "no declared metrics" is a loud phase-tier outcome.
        assert get_metric_definitions("nonexistent") == {}
        assert get_metrics_config("nonexistent") == {"metrics": []}


class TestGetMetricsConfigOverlay:
    """Production path layers ``metric`` overlay rows over the shipped graphs."""

    def teardown_method(self) -> None:
        reset_overlay_resolver_for_tests()

    def test_overlay_row_adds_a_taught_metric(self) -> None:
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="metric",
                    payload={
                        "vertical": "finance",
                        "graph_id": "custom_kpi",
                        "metadata": {"name": "Custom KPI", "category": "custom"},
                        "output": {"type": "scalar"},
                    },
                )
            ]
        )
        defs = get_metric_definitions("finance")
        assert "custom_kpi" in defs
        assert len(defs) == 17  # the 16 shipped + the taught one
        assert "vertical" not in defs["custom_kpi"]

    def test_overlay_row_replaces_shipped_metric_by_graph_id(self) -> None:
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="metric",
                    payload={
                        "vertical": "finance",
                        "graph_id": "dso",
                        "metadata": {"name": "DSO (taught)", "category": "working_capital"},
                        "output": {"type": "scalar"},
                    },
                )
            ]
        )
        defs = get_metric_definitions("finance")
        assert len(defs) == 16  # replace, not append
        assert defs["dso"]["metadata"]["name"] == "DSO (taught)"


class TestGetMetricsConfigTestPath:
    """Explicit ``verticals_dir`` reads raw YAML and bypasses the overlay."""

    def teardown_method(self) -> None:
        reset_overlay_resolver_for_tests()

    @staticmethod
    def _seed_metric(verticals_dir: Path, vertical: str, graph_id: str) -> None:
        metrics_dir = verticals_dir / vertical / "metrics" / "category"
        metrics_dir.mkdir(parents=True, exist_ok=True)
        (metrics_dir / f"{graph_id}.yaml").write_text(
            yaml.safe_dump(
                {
                    "graph_id": graph_id,
                    "metadata": {"name": graph_id, "category": "category"},
                    "output": {"type": "scalar"},
                }
            )
        )

    def test_reads_directory_recursively(self, tmp_path: Path) -> None:
        self._seed_metric(tmp_path, "testv", "m1")
        self._seed_metric(tmp_path, "testv", "m2")
        defs = get_metric_definitions("testv", verticals_dir=tmp_path)
        assert set(defs) == {"m1", "m2"}

    def test_missing_dir_is_empty(self, tmp_path: Path) -> None:
        assert get_metric_definitions("framed", verticals_dir=tmp_path) == {}

    def test_overlay_is_bypassed_on_test_path(self, tmp_path: Path) -> None:
        # A registered resolver MUST NOT leak into the explicit-dir test path.
        self._seed_metric(tmp_path, "testv", "m1")
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="metric",
                    payload={"vertical": "testv", "graph_id": "ghost", "output": {}},
                )
            ]
        )
        defs = get_metric_definitions("testv", verticals_dir=tmp_path)
        assert set(defs) == {"m1"}  # 'ghost' overlay row not applied
