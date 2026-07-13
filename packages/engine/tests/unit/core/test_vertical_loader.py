"""Tests for the unified VerticalLoader facade (DAT-481).

One loader for all four vertical families (concepts / validations / cycles /
metrics), across the three resolution modes: shipped (on-disk), framed
(overlay-only), and the explicit-``verticals_dir`` test path (raw, no overlay).
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
import yaml

from dataraum.core.overlay import (
    OverlayRow,
    reset_overlay_resolver_for_tests,
    set_overlay_resolver,
)
from dataraum.core.vertical_loader import Family, VerticalLoader


@pytest.fixture(autouse=True)
def _clean_resolver() -> Iterator[None]:
    reset_overlay_resolver_for_tests()
    yield
    reset_overlay_resolver_for_tests()


class TestShipped:
    """The shipped finance vertical resolves all four families off disk."""

    def test_metrics(self) -> None:
        metrics = VerticalLoader("finance").collection(Family.METRICS)["metrics"]
        assert any(m.get("graph_id") == "dso" for m in metrics)

    def test_validations(self) -> None:
        validations = VerticalLoader("finance").collection(Family.VALIDATIONS)["validations"]
        assert len(validations) >= 1

    def test_cycles(self) -> None:
        cycles = VerticalLoader("finance").collection(Family.CYCLES)
        assert cycles.get("cycle_types")

    def test_concepts(self) -> None:
        ontology = VerticalLoader("finance").collection(Family.CONCEPTS)
        assert ontology["name"]
        assert ontology["concepts"]


class TestFramed:
    """A framed vertical (no on-disk dir) resolves overlay rows only."""

    def test_metric_row_resolves(self) -> None:
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="metric",
                    payload={"vertical": "sales", "graph_id": "win_rate", "output": {}},
                )
            ]
        )
        metrics = VerticalLoader("sales").collection(Family.METRICS)["metrics"]
        assert [m["graph_id"] for m in metrics] == ["win_rate"]

    def test_validation_row_resolves(self) -> None:
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="validation",
                    payload={"vertical": "sales", "validation_id": "v_pipeline_stage"},
                )
            ]
        )
        validations = VerticalLoader("sales").collection(Family.VALIDATIONS)["validations"]
        assert [v["validation_id"] for v in validations] == ["v_pipeline_stage"]

    def test_cycle_row_resolves(self) -> None:
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="cycle",
                    payload={"vertical": "sales", "name": "sales_cycle"},
                )
            ]
        )
        cycle_types = VerticalLoader("sales").collection(Family.CYCLES)["cycle_types"]
        assert "sales_cycle" in cycle_types


class TestEmptyAndUnknown:
    """No on-disk source AND no overlay → the family's empty base, never a raise."""

    def test_dir_families_empty_list(self) -> None:
        assert VerticalLoader("nope").collection(Family.METRICS) == {"metrics": []}
        assert VerticalLoader("nope").collection(Family.VALIDATIONS) == {"validations": []}

    def test_cycles_empty_dict(self) -> None:
        assert VerticalLoader("nope").collection(Family.CYCLES) == {}

    def test_concepts_empty_carries_name(self) -> None:
        assert VerticalLoader("nope").collection(Family.CONCEPTS) == {
            "name": "nope",
            "concepts": [],
        }


class TestTestPathBypassesOverlay:
    """An explicit ``verticals_dir`` reads raw YAML and ignores the overlay."""

    @staticmethod
    def _seed(root: Path) -> None:
        vroot = root / "myvert"
        (vroot / "metrics" / "cat").mkdir(parents=True)
        (vroot / "metrics" / "cat" / "m1.yaml").write_text(
            yaml.safe_dump({"graph_id": "m1", "metadata": {"name": "m1"}, "output": {}})
        )
        (vroot / "validations").mkdir()
        (vroot / "validations" / "v1.yaml").write_text(yaml.safe_dump({"validation_id": "v1"}))
        (vroot / "cycles.yaml").write_text(yaml.safe_dump({"cycle_types": {"c1": {}}}))
        (vroot / "ontology.yaml").write_text(
            yaml.safe_dump({"name": "myvert", "concepts": [{"name": "x"}]})
        )

    def test_reads_raw_ignoring_overlay(self, tmp_path: Path) -> None:
        self._seed(tmp_path)
        # A registered resolver MUST NOT leak into the explicit-dir test path.
        set_overlay_resolver(
            lambda: [OverlayRow(type="metric", payload={"vertical": "myvert", "graph_id": "ghost"})]
        )
        loader = VerticalLoader("myvert", verticals_dir=tmp_path)

        assert [m["graph_id"] for m in loader.collection(Family.METRICS)["metrics"]] == ["m1"]
        assert [
            v["validation_id"] for v in loader.collection(Family.VALIDATIONS)["validations"]
        ] == ["v1"]
        assert loader.collection(Family.CYCLES) == {"cycle_types": {"c1": {}}}
        assert loader.collection(Family.CONCEPTS)["concepts"] == [{"name": "x"}]

    def test_missing_test_path_is_empty_base(self, tmp_path: Path) -> None:
        loader = VerticalLoader("absent", verticals_dir=tmp_path)
        assert loader.collection(Family.METRICS) == {"metrics": []}
        assert loader.collection(Family.CYCLES) == {}
        assert loader.collection(Family.CONCEPTS) == {"name": "absent", "concepts": []}
