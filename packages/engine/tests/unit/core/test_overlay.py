"""Tests for the layered config overlay (DAT-343).

The overlay sits between the baked-in YAML in ``packages/dataraum-config``
and the consumers; each teach type binds to one target file and one
merge function. These tests pin the per-type merge semantics and the
loader integration (resolver registered → overlay rows merged; no
resolver → base returned unchanged).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from dataraum.core.config import (
    load_phase_config,
    load_yaml_config,
    reset_config_root,
    set_config_root,
)
from dataraum.core.overlay import (
    OverlayRow,
    apply_overlay,
    reset_overlay_resolver_for_tests,
    set_overlay_resolver,
)


# ---------------------------------------------------------------------------
# Per-type applier semantics — driven through ``apply_overlay`` so the
# dispatcher's path matching is exercised alongside the merge functions.
# ---------------------------------------------------------------------------


class TestApplyTypePattern:
    """``type_pattern`` rows merge into ``phases/typing.yaml`` overrides."""

    def teardown_method(self) -> None:
        reset_overlay_resolver_for_tests()

    def test_adds_pattern_to_empty_overrides(self) -> None:
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="type_pattern",
                    payload={
                        "name": "my_date",
                        "pattern": r"^\d{4}-\d{2}$",
                        "inferred_type": "DATE",
                    },
                )
            ]
        )
        merged = apply_overlay("phases/typing.yaml", {})
        assert merged["overrides"]["patterns"]["my_date"] == {
            "pattern": r"^\d{4}-\d{2}$",
            "inferred_type": "DATE",
        }

    def test_preserves_base_categories(self) -> None:
        base: dict[str, Any] = {
            "date_patterns": [{"name": "iso", "pattern": "^.*$"}],
            "overrides": {"patterns": {"existing": {"pattern": "^x$"}}},
        }
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="type_pattern",
                    payload={"name": "added", "pattern": "^y$"},
                )
            ]
        )
        merged = apply_overlay("phases/typing.yaml", base)
        assert merged["date_patterns"] == base["date_patterns"]
        assert merged["overrides"]["patterns"]["existing"] == {"pattern": "^x$"}
        assert merged["overrides"]["patterns"]["added"] == {"pattern": "^y$"}

    def test_last_write_wins_on_same_name(self) -> None:
        set_overlay_resolver(
            lambda: [
                OverlayRow(type="type_pattern", payload={"name": "k", "pattern": "v1"}),
                OverlayRow(type="type_pattern", payload={"name": "k", "pattern": "v2"}),
            ]
        )
        merged = apply_overlay("phases/typing.yaml", {})
        assert merged["overrides"]["patterns"]["k"] == {"pattern": "v2"}

    def test_row_without_name_is_ignored(self) -> None:
        set_overlay_resolver(
            lambda: [
                OverlayRow(type="type_pattern", payload={"pattern": "^x$"}),
            ]
        )
        merged = apply_overlay("phases/typing.yaml", {})
        assert merged["overrides"]["patterns"] == {}


class TestApplyNullValue:
    """``null_value`` rows append into ``null_values.yaml`` lists by category."""

    def teardown_method(self) -> None:
        reset_overlay_resolver_for_tests()

    def test_appends_to_category(self) -> None:
        base = {"standard_nulls": [{"value": "NA"}]}
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="null_value",
                    payload={
                        "category": "standard_nulls",
                        "value": "TBD",
                        "description": "to be determined",
                    },
                )
            ]
        )
        merged = apply_overlay("null_values.yaml", base)
        assert merged["standard_nulls"] == [
            {"value": "NA"},
            {"value": "TBD", "description": "to be determined"},
        ]

    def test_creates_category_when_missing(self) -> None:
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="null_value",
                    payload={"category": "placeholder_nulls", "value": "-"},
                )
            ]
        )
        merged = apply_overlay("null_values.yaml", {})
        assert merged["placeholder_nulls"] == [{"value": "-"}]

    def test_dedupes_within_category_idempotently(self) -> None:
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="null_value",
                    payload={"category": "standard_nulls", "value": "TBD"},
                ),
                OverlayRow(
                    type="null_value",
                    payload={"category": "standard_nulls", "value": "TBD"},
                ),
            ]
        )
        merged = apply_overlay("null_values.yaml", {"standard_nulls": []})
        assert merged["standard_nulls"] == [{"value": "TBD"}]

    def test_ignores_row_without_category_or_value(self) -> None:
        set_overlay_resolver(
            lambda: [
                OverlayRow(type="null_value", payload={"value": "x"}),  # no category
                OverlayRow(type="null_value", payload={"category": "standard_nulls"}),  # no value
            ]
        )
        merged = apply_overlay("null_values.yaml", {"standard_nulls": []})
        assert merged["standard_nulls"] == []


class TestApplyConceptProperty:
    """``concept_property`` rows patch a field on a named concept entry.

    Path-parameterized by vertical name; the dispatcher routes by parsing
    ``verticals/<v>/ontology.yaml``.
    """

    def teardown_method(self) -> None:
        reset_overlay_resolver_for_tests()

    def test_patches_named_concept(self) -> None:
        base = {
            "name": "finance",
            "concepts": [
                {"name": "revenue", "indicators": ["rev"]},
                {"name": "cost", "indicators": ["cost"]},
            ],
        }
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="concept_property",
                    payload={
                        "vertical": "finance",
                        "concept": "revenue",
                        "property": "typical_role",
                        "value": "measure",
                    },
                )
            ]
        )
        merged = apply_overlay("verticals/finance/ontology.yaml", base)
        revenue = next(c for c in merged["concepts"] if c["name"] == "revenue")
        cost = next(c for c in merged["concepts"] if c["name"] == "cost")
        assert revenue["typical_role"] == "measure"
        # Unrelated concept untouched.
        assert "typical_role" not in cost

    def test_skips_row_targeting_other_vertical(self) -> None:
        base = {"concepts": [{"name": "revenue"}]}
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="concept_property",
                    payload={
                        "vertical": "marketing",
                        "concept": "revenue",
                        "property": "typical_role",
                        "value": "measure",
                    },
                )
            ]
        )
        merged = apply_overlay("verticals/finance/ontology.yaml", base)
        assert "typical_role" not in merged["concepts"][0]

    def test_unknown_concept_ignored_defensively(self) -> None:
        base = {"concepts": [{"name": "revenue"}]}
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="concept_property",
                    payload={
                        "vertical": "finance",
                        "concept": "nonexistent",
                        "property": "x",
                        "value": "y",
                    },
                )
            ]
        )
        merged = apply_overlay("verticals/finance/ontology.yaml", base)
        assert merged["concepts"] == base["concepts"]


# ---------------------------------------------------------------------------
# Dispatcher — short-circuit + path matching.
# ---------------------------------------------------------------------------


class TestApplyOverlayDispatch:
    """``apply_overlay`` short-circuits when no resolver / no matching rows."""

    def teardown_method(self) -> None:
        reset_overlay_resolver_for_tests()

    def test_no_resolver_returns_base_identity(self) -> None:
        base = {"k": "v"}
        result = apply_overlay("phases/typing.yaml", base)
        assert result is base  # short-circuit returns the same object

    def test_resolver_with_no_rows_returns_base_identity(self) -> None:
        set_overlay_resolver(lambda: [])
        base = {"k": "v"}
        result = apply_overlay("phases/typing.yaml", base)
        assert result is base

    def test_unrelated_path_returns_base_unmodified(self) -> None:
        set_overlay_resolver(
            lambda: [OverlayRow(type="type_pattern", payload={"name": "x", "pattern": "y"})]
        )
        base = {"providers": {"anthropic": {}}}
        merged = apply_overlay("llm/config.yaml", base)
        assert merged == base

    def test_rows_of_wrong_type_for_path_skipped(self) -> None:
        # null_value rows targeted at the typing file are simply skipped.
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="null_value",
                    payload={"category": "standard_nulls", "value": "X"},
                )
            ]
        )
        base = {"overrides": {"patterns": {}}}
        merged = apply_overlay("phases/typing.yaml", base)
        assert merged == base


# ---------------------------------------------------------------------------
# Loader integration — load_yaml_config / load_phase_config consult the
# resolver via apply_overlay.
# ---------------------------------------------------------------------------


class TestLoaderIntegration:
    """``load_yaml_config`` and ``load_phase_config`` apply overlay rows."""

    def teardown_method(self) -> None:
        reset_overlay_resolver_for_tests()
        reset_config_root()

    def test_load_yaml_config_applies_overlay(self, tmp_path: Path) -> None:
        root = tmp_path / "cfg"
        root.mkdir()
        (root / "null_values.yaml").write_text("standard_nulls:\n  - value: NA\n")
        set_config_root(root)
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="null_value",
                    payload={"category": "standard_nulls", "value": "TBD"},
                )
            ]
        )

        data = load_yaml_config("null_values.yaml")

        assert data["standard_nulls"] == [{"value": "NA"}, {"value": "TBD"}]

    def test_load_yaml_config_inert_when_no_resolver(self, tmp_path: Path) -> None:
        root = tmp_path / "cfg"
        root.mkdir()
        (root / "null_values.yaml").write_text("standard_nulls:\n  - value: NA\n")
        set_config_root(root)

        data = load_yaml_config("null_values.yaml")

        assert data == {"standard_nulls": [{"value": "NA"}]}

    def test_load_phase_config_applies_overlay(self, tmp_path: Path) -> None:
        root = tmp_path / "cfg"
        (root / "phases").mkdir(parents=True)
        (root / "phases" / "typing.yaml").write_text("min_confidence: 0.5\n")
        set_config_root(root)
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="type_pattern",
                    payload={"name": "my_date", "pattern": r"^\d{4}-\d{2}$"},
                )
            ]
        )

        cfg = load_phase_config("typing")

        assert cfg["min_confidence"] == 0.5
        assert cfg["overrides"]["patterns"]["my_date"] == {"pattern": r"^\d{4}-\d{2}$"}

    def test_load_phase_config_with_explicit_config_root_bypasses_overlay(
        self, tmp_path: Path
    ) -> None:
        """Tests passing an explicit ``config_root`` keep the original semantics:
        the fixture is deterministic; overlay rows must not bleed in."""
        root = tmp_path / "fixture"
        (root / "phases").mkdir(parents=True)
        (root / "phases" / "typing.yaml").write_text("min_confidence: 0.7\n")
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="type_pattern",
                    payload={"name": "should_not_appear", "pattern": "^x$"},
                )
            ]
        )

        cfg = load_phase_config("typing", config_root=root)

        assert cfg == {"min_confidence": 0.7}

    def test_load_phase_config_missing_file_returns_empty(self) -> None:
        # Cleared resolver — but also: a missing phase file shouldn't crash
        # even when an overlay resolver is registered.
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="type_pattern",
                    payload={"name": "x", "pattern": "y"},
                )
            ]
        )

        cfg = load_phase_config("definitely_nonexistent_phase_xyz_dat343")

        assert cfg == {}


# ---------------------------------------------------------------------------
# Resolver lifecycle.
# ---------------------------------------------------------------------------


class TestResolverLifecycle:
    """``set_overlay_resolver`` and the test reset are honored."""

    def teardown_method(self) -> None:
        reset_overlay_resolver_for_tests()

    def test_reset_drops_resolver(self) -> None:
        set_overlay_resolver(
            lambda: [OverlayRow(type="type_pattern", payload={"name": "x", "pattern": "y"})]
        )
        reset_overlay_resolver_for_tests()
        # No resolver → base returned unchanged regardless of registered rows.
        base = {"overrides": {"patterns": {}}}
        assert apply_overlay("phases/typing.yaml", base) is base

    def test_set_to_none_explicit(self) -> None:
        set_overlay_resolver(
            lambda: [OverlayRow(type="type_pattern", payload={"name": "x", "pattern": "y"})]
        )
        set_overlay_resolver(None)
        base = {"overrides": {"patterns": {}}}
        assert apply_overlay("phases/typing.yaml", base) is base


@pytest.fixture(autouse=True)
def _cleanup() -> Any:
    """Belt-and-braces: each test gets a clean resolver + config root."""
    yield
    reset_overlay_resolver_for_tests()
    reset_config_root()
