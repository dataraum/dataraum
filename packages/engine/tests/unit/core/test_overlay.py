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
    appliable_teach_types,
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


class TestApplyUnit:
    """``unit`` rows merge into ``phases/typing.yaml`` ``overrides.units`` (DAT-428)."""

    def teardown_method(self) -> None:
        reset_overlay_resolver_for_tests()

    def test_adds_unit_to_empty_overrides(self) -> None:
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="unit",
                    payload={"table": "invoices", "column": "amount", "unit": "EUR"},
                )
            ]
        )
        merged = apply_overlay("phases/typing.yaml", {})
        assert merged["overrides"]["units"] == {"invoices.amount": {"unit": "EUR"}}

    def test_last_write_wins_on_same_column(self) -> None:
        set_overlay_resolver(
            lambda: [
                OverlayRow(type="unit", payload={"table": "t", "column": "c", "unit": "USD"}),
                OverlayRow(type="unit", payload={"table": "t", "column": "c", "unit": "EUR"}),
            ]
        )
        merged = apply_overlay("phases/typing.yaml", {})
        assert merged["overrides"]["units"]["t.c"] == {"unit": "EUR"}

    def test_composes_with_type_pattern_on_same_file(self) -> None:
        """``unit`` (overrides.units) and ``type_pattern`` (overrides.patterns) share the
        file but write disjoint keys, so the dispatcher applies both without clobbering."""
        set_overlay_resolver(
            lambda: [
                OverlayRow(type="type_pattern", payload={"name": "p", "pattern": "^x$"}),
                OverlayRow(type="unit", payload={"table": "t", "column": "c", "unit": "kg"}),
            ]
        )
        merged = apply_overlay("phases/typing.yaml", {})
        assert merged["overrides"]["patterns"]["p"] == {"pattern": "^x$"}
        assert merged["overrides"]["units"]["t.c"] == {"unit": "kg"}

    def test_ignores_rows_missing_fields(self) -> None:
        set_overlay_resolver(
            lambda: [
                OverlayRow(type="unit", payload={"column": "c", "unit": "EUR"}),  # no table
                OverlayRow(type="unit", payload={"table": "t", "unit": "EUR"}),  # no column
                OverlayRow(type="unit", payload={"table": "t", "column": "c"}),  # no unit
            ]
        )
        merged = apply_overlay("phases/typing.yaml", {})
        assert merged["overrides"]["units"] == {}


class TestApplyConcept:
    """``concept`` rows upsert-replace into a vertical ontology's ``concepts:`` list.

    Used by user teach AND by ``_adhoc`` cold-start induction (DAT-371) —
    induction writes one row per induced concept instead of a YAML file.
    """

    def teardown_method(self) -> None:
        reset_overlay_resolver_for_tests()

    def test_appends_new_concept_to_empty_baseline(self) -> None:
        """Empty ``_adhoc`` baseline + one concept row materializes that concept."""
        base = {"name": "_adhoc", "concepts": []}
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="concept",
                    payload={
                        "vertical": "_adhoc",
                        "name": "revenue",
                        "description": "Total income",
                        "indicators": ["revenue", "sales"],
                        "typical_role": "measure",
                    },
                )
            ]
        )
        merged = apply_overlay("verticals/_adhoc/ontology.yaml", base)
        assert merged["concepts"] == [
            {
                "name": "revenue",
                "description": "Total income",
                "indicators": ["revenue", "sales"],
                "typical_role": "measure",
            }
        ]
        # ``vertical`` is the routing key, never leaks into the merged concept.
        assert "vertical" not in merged["concepts"][0]

    def test_appends_alongside_existing_concepts(self) -> None:
        base = {
            "name": "finance",
            "concepts": [{"name": "revenue", "indicators": ["rev"]}],
        }
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="concept",
                    payload={
                        "vertical": "finance",
                        "name": "cost",
                        "indicators": ["cost", "expense"],
                    },
                )
            ]
        )
        merged = apply_overlay("verticals/finance/ontology.yaml", base)
        assert [c["name"] for c in merged["concepts"]] == ["revenue", "cost"]
        # Existing concept untouched.
        assert merged["concepts"][0] == {"name": "revenue", "indicators": ["rev"]}

    def test_same_name_replaces_in_place(self) -> None:
        """Upsert-replace by name: a later row for the same concept wins."""
        base = {
            "name": "finance",
            "concepts": [{"name": "revenue", "indicators": ["old"], "typical_role": "measure"}],
        }
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="concept",
                    payload={
                        "vertical": "finance",
                        "name": "revenue",
                        "indicators": ["new"],
                    },
                )
            ]
        )
        merged = apply_overlay("verticals/finance/ontology.yaml", base)
        revenue = next(c for c in merged["concepts"] if c["name"] == "revenue")
        # Replaced wholesale — old ``typical_role`` is gone.
        assert revenue == {"name": "revenue", "indicators": ["new"]}

    def test_skips_row_targeting_other_vertical(self) -> None:
        base = {"concepts": []}
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="concept",
                    payload={"vertical": "marketing", "name": "campaign"},
                )
            ]
        )
        merged = apply_overlay("verticals/finance/ontology.yaml", base)
        # No row for finance → identity short-circuit (no concept rows for vertical).
        assert merged is base

    def test_row_without_name_is_ignored(self) -> None:
        base = {"concepts": []}
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="concept",
                    payload={"vertical": "_adhoc", "description": "no name"},
                )
            ]
        )
        merged = apply_overlay("verticals/_adhoc/ontology.yaml", base)
        assert merged["concepts"] == []

    def test_concept_rows_apply_before_property_patches(self) -> None:
        """Dispatcher applies concept rows first, then concept_property patches on top.

        Pins the order documented in :func:`apply_overlay`: a concept
        row may freshly insert a concept, then a property row may patch a
        field on that same just-inserted concept in the same merge pass.
        """
        base = {"name": "_adhoc", "concepts": []}
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="concept",
                    payload={"vertical": "_adhoc", "name": "revenue", "indicators": ["rev"]},
                ),
                OverlayRow(
                    type="concept_property",
                    payload={
                        "vertical": "_adhoc",
                        "concept": "revenue",
                        "property": "typical_role",
                        "value": "measure",
                    },
                ),
            ]
        )
        merged = apply_overlay("verticals/_adhoc/ontology.yaml", base)
        revenue = next(c for c in merged["concepts"] if c["name"] == "revenue")
        assert revenue == {
            "name": "revenue",
            "indicators": ["rev"],
            "typical_role": "measure",
        }


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


class TestApplyRebind:
    """``rebind`` rows append a column name to a concept's ``indicators``.

    The column-grain re-grounding teach (temporal_behavior's ignorance-branch
    suggestion): the appended indicator reaches the next run's grounding
    prompt, steering the LLM witness — never writing ``business_concept``.
    """

    def teardown_method(self) -> None:
        reset_overlay_resolver_for_tests()

    @staticmethod
    def _row(column: str, concept: str, vertical: str = "finance") -> OverlayRow:
        return OverlayRow(
            type="rebind",
            payload={"vertical": vertical, "concept": concept, "column": column},
        )

    def test_appends_column_to_concept_indicators(self) -> None:
        base = {
            "name": "finance",
            "concepts": [{"name": "account_balance", "indicators": ["balance"]}],
        }
        set_overlay_resolver(lambda: [self._row("debit_balance", "account_balance")])
        merged = apply_overlay("verticals/finance/ontology.yaml", base)
        assert merged["concepts"][0]["indicators"] == ["balance", "debit_balance"]
        # Base list untouched (no aliasing).
        assert base["concepts"][0]["indicators"] == ["balance"]

    def test_creates_indicators_when_concept_has_none(self) -> None:
        base = {"concepts": [{"name": "revenue"}]}
        set_overlay_resolver(lambda: [self._row("rev_amt", "revenue")])
        merged = apply_overlay("verticals/finance/ontology.yaml", base)
        assert merged["concepts"][0]["indicators"] == ["rev_amt"]

    def test_duplicate_rebind_is_idempotent(self) -> None:
        base = {"concepts": [{"name": "revenue", "indicators": ["rev_amt"]}]}
        set_overlay_resolver(
            lambda: [
                self._row("rev_amt", "revenue"),
                self._row("rev_amt", "revenue"),
            ]
        )
        merged = apply_overlay("verticals/finance/ontology.yaml", base)
        assert merged["concepts"][0]["indicators"] == ["rev_amt"]

    def test_last_rebind_wins_moves_column_between_concepts(self) -> None:
        """A later rebind for the same column MOVES it — the column lands only
        on its final target among rebind rows."""
        base = {
            "concepts": [
                {"name": "account_balance", "indicators": []},
                {"name": "transaction_amount", "indicators": []},
            ],
        }
        set_overlay_resolver(
            lambda: [
                self._row("debit_balance", "account_balance"),
                self._row("debit_balance", "transaction_amount"),
            ]
        )
        merged = apply_overlay("verticals/finance/ontology.yaml", base)
        balance = next(c for c in merged["concepts"] if c["name"] == "account_balance")
        amount = next(c for c in merged["concepts"] if c["name"] == "transaction_amount")
        assert balance["indicators"] == []
        assert amount["indicators"] == ["debit_balance"]

    def test_unknown_concept_ignored_defensively(self) -> None:
        base = {"concepts": [{"name": "revenue", "indicators": ["rev"]}]}
        set_overlay_resolver(lambda: [self._row("col", "nonexistent")])
        merged = apply_overlay("verticals/finance/ontology.yaml", base)
        assert merged["concepts"] == base["concepts"]

    def test_skips_row_targeting_other_vertical(self) -> None:
        base = {"concepts": [{"name": "revenue", "indicators": []}]}
        set_overlay_resolver(lambda: [self._row("col", "revenue", vertical="marketing")])
        merged = apply_overlay("verticals/finance/ontology.yaml", base)
        assert merged is base  # no matching rows → identity short-circuit

    def test_row_without_column_or_concept_ignored(self) -> None:
        base = {"concepts": [{"name": "revenue", "indicators": []}]}
        set_overlay_resolver(
            lambda: [
                OverlayRow(type="rebind", payload={"vertical": "finance", "column": "c"}),
                OverlayRow(type="rebind", payload={"vertical": "finance", "concept": "revenue"}),
            ]
        )
        merged = apply_overlay("verticals/finance/ontology.yaml", base)
        assert merged["concepts"][0]["indicators"] == []

    def test_rebind_applies_after_concept_rows(self) -> None:
        """Family order: a rebind may pull a column onto a concept defined or
        replaced by a ``concept`` row in the same merge pass."""
        base = {"name": "_adhoc", "concepts": []}
        set_overlay_resolver(
            lambda: [
                self._row("gross_take", "revenue", vertical="_adhoc"),
                OverlayRow(
                    type="concept",
                    payload={"vertical": "_adhoc", "name": "revenue", "indicators": ["rev"]},
                ),
            ]
        )
        merged = apply_overlay("verticals/_adhoc/ontology.yaml", base)
        revenue = next(c for c in merged["concepts"] if c["name"] == "revenue")
        assert revenue["indicators"] == ["rev", "gross_take"]


class TestAppliableTeachTypes:
    """``appliable_teach_types`` derives the executable vocabulary from the registries."""

    def test_contains_every_registered_applier_type(self) -> None:
        assert appliable_teach_types() == frozenset(
            {
                "type_pattern",
                "null_value",
                "unit",
                "concept",
                "concept_property",
                "rebind",
                "validation",
                "cycle",
                "metric",
            }
        )

    def test_excludes_types_without_appliers(self) -> None:
        # ``explanation`` is fully deferred; ``relationship`` and
        # ``expected_dependency`` are direct config_overlay reads, not
        # layered-read appliers.
        assert "explanation" not in appliable_teach_types()
        assert "relationship" not in appliable_teach_types()
        assert "expected_dependency" not in appliable_teach_types()


# ---------------------------------------------------------------------------
# Dispatcher — short-circuit + path matching.
# ---------------------------------------------------------------------------


class TestApplyValidation:
    """``validation`` rows upsert into the logical ``verticals/<v>/validations`` collection."""

    def teardown_method(self) -> None:
        reset_overlay_resolver_for_tests()

    @staticmethod
    def _row(validation_id: str, vertical: str = "finance", **extra: Any) -> OverlayRow:
        payload: dict[str, Any] = {
            "vertical": vertical,
            "validation_id": validation_id,
            "name": validation_id,
            "description": "d",
            "category": "financial",
            "check_type": "balance",
        }
        payload.update(extra)
        return OverlayRow(type="validation", payload=payload)

    def test_adds_to_empty_collection(self) -> None:
        # Framed vertical: empty base + rows IS the declared set.
        set_overlay_resolver(lambda: [self._row("taught")])
        merged = apply_overlay("verticals/finance/validations", {"validations": []})
        assert [s["validation_id"] for s in merged["validations"]] == ["taught"]
        assert "vertical" not in merged["validations"][0]  # stripped from payload

    def test_replaces_base_spec_by_id_last_write_wins(self) -> None:
        base = {"validations": [{"validation_id": "tb", "name": "shipped"}]}
        set_overlay_resolver(
            lambda: [
                self._row("tb", parameters={"tolerance": 1.0}),
                self._row("tb", parameters={"tolerance": 5.0}),
            ]
        )
        merged = apply_overlay("verticals/finance/validations", base)
        assert len(merged["validations"]) == 1
        assert merged["validations"][0]["parameters"] == {"tolerance": 5.0}
        # Base dict untouched (no aliasing)
        assert base["validations"][0]["name"] == "shipped"

    def test_other_vertical_rows_filtered_by_dispatcher(self) -> None:
        set_overlay_resolver(lambda: [self._row("x", vertical="marketing")])
        base: dict[str, Any] = {"validations": []}
        merged = apply_overlay("verticals/finance/validations", base)
        assert merged is base  # no matching rows → identity short-circuit

    def test_row_without_validation_id_ignored(self) -> None:
        set_overlay_resolver(
            lambda: [OverlayRow(type="validation", payload={"vertical": "finance", "name": "n"})]
        )
        merged = apply_overlay("verticals/finance/validations", {"validations": []})
        assert merged["validations"] == []

    def test_per_file_yaml_path_is_inert(self) -> None:
        # The applier binds to the COLLECTION path; an individual spec file's
        # path matches no applier and passes through unchanged.
        set_overlay_resolver(lambda: [self._row("tb")])
        base = {"validation_id": "tb", "name": "shipped"}
        merged = apply_overlay("verticals/finance/validations/trial_balance.yaml", base)
        assert merged == base


class TestApplyCycle:
    """``cycle`` rows upsert into the ``cycle_types`` MAPPING of ``cycles.yaml``."""

    def teardown_method(self) -> None:
        reset_overlay_resolver_for_tests()

    @staticmethod
    def _row(name: str, vertical: str = "finance", **extra: Any) -> OverlayRow:
        payload: dict[str, Any] = {
            "vertical": vertical,
            "name": name,
            "description": "d",
            "business_value": "high",
        }
        payload.update(extra)
        return OverlayRow(type="cycle", payload=payload)

    def test_adds_to_empty_mapping(self) -> None:
        # Framed vertical: empty base + rows IS the declared set.
        set_overlay_resolver(lambda: [self._row("order_to_cash")])
        merged = apply_overlay("verticals/finance/cycles.yaml", {"cycle_types": {}})
        assert list(merged["cycle_types"]) == ["order_to_cash"]
        # name keys the mapping, but is dropped from the value; vertical stripped.
        entry = merged["cycle_types"]["order_to_cash"]
        assert "vertical" not in entry
        assert "name" not in entry
        assert entry["business_value"] == "high"

    def test_replaces_base_cycle_by_name_last_write_wins(self) -> None:
        base = {"cycle_types": {"period_close": {"description": "shipped"}}}
        set_overlay_resolver(
            lambda: [
                self._row("period_close", description="first"),
                self._row("period_close", description="second"),
            ]
        )
        merged = apply_overlay("verticals/finance/cycles.yaml", base)
        assert merged["cycle_types"]["period_close"]["description"] == "second"
        # Base dict untouched (no aliasing)
        assert base["cycle_types"]["period_close"]["description"] == "shipped"

    def test_adds_alongside_existing_cycle_types(self) -> None:
        base = {"cycle_types": {"order_to_cash": {"description": "shipped"}}}
        set_overlay_resolver(lambda: [self._row("custom_cycle")])
        merged = apply_overlay("verticals/finance/cycles.yaml", base)
        assert set(merged["cycle_types"]) == {"order_to_cash", "custom_cycle"}

    def test_other_vertical_rows_filtered_by_dispatcher(self) -> None:
        set_overlay_resolver(lambda: [self._row("x", vertical="marketing")])
        base: dict[str, Any] = {"cycle_types": {}}
        merged = apply_overlay("verticals/finance/cycles.yaml", base)
        assert merged is base  # no matching rows → identity short-circuit

    def test_row_without_name_ignored(self) -> None:
        set_overlay_resolver(
            lambda: [OverlayRow(type="cycle", payload={"vertical": "finance", "description": "d"})]
        )
        merged = apply_overlay("verticals/finance/cycles.yaml", {"cycle_types": {}})
        assert merged["cycle_types"] == {}


class TestApplyMetric:
    """``metric`` rows upsert into the ``metrics:`` LIST of the metrics dir, keyed by graph_id."""

    def teardown_method(self) -> None:
        reset_overlay_resolver_for_tests()

    @staticmethod
    def _row(graph_id: str, vertical: str = "finance", **extra: Any) -> OverlayRow:
        payload: dict[str, Any] = {
            "vertical": vertical,
            "graph_id": graph_id,
            "metadata": {"name": graph_id, "category": "custom"},
            "output": {"type": "scalar"},
        }
        payload.update(extra)
        return OverlayRow(type="metric", payload=payload)

    def test_adds_to_empty_list(self) -> None:
        # Framed vertical: empty base + rows IS the declared set.
        set_overlay_resolver(lambda: [self._row("dso")])
        merged = apply_overlay("verticals/finance/metrics", {"metrics": []})
        assert [m["graph_id"] for m in merged["metrics"]] == ["dso"]
        # vertical is stripped from the stored definition.
        assert "vertical" not in merged["metrics"][0]

    def test_replaces_base_metric_by_graph_id_last_write_wins(self) -> None:
        base = {"metrics": [{"graph_id": "dso", "version": "shipped"}]}
        set_overlay_resolver(
            lambda: [
                self._row("dso", version="first"),
                self._row("dso", version="second"),
            ]
        )
        merged = apply_overlay("verticals/finance/metrics", base)
        assert [m["graph_id"] for m in merged["metrics"]] == ["dso"]
        assert merged["metrics"][0]["version"] == "second"
        # Base dict untouched (no aliasing).
        assert base["metrics"][0]["version"] == "shipped"

    def test_adds_alongside_existing_metrics(self) -> None:
        base = {"metrics": [{"graph_id": "ebitda", "version": "shipped"}]}
        set_overlay_resolver(lambda: [self._row("custom_metric")])
        merged = apply_overlay("verticals/finance/metrics", base)
        assert {m["graph_id"] for m in merged["metrics"]} == {"ebitda", "custom_metric"}

    def test_other_vertical_rows_filtered_by_dispatcher(self) -> None:
        set_overlay_resolver(lambda: [self._row("x", vertical="marketing")])
        base: dict[str, Any] = {"metrics": []}
        merged = apply_overlay("verticals/finance/metrics", base)
        assert merged is base  # no matching rows → identity short-circuit

    def test_row_without_graph_id_ignored(self) -> None:
        set_overlay_resolver(
            lambda: [OverlayRow(type="metric", payload={"vertical": "finance", "output": {}})]
        )
        merged = apply_overlay("verticals/finance/metrics", {"metrics": []})
        assert merged["metrics"] == []


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
