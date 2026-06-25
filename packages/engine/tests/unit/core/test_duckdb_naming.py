"""Tests for core/duckdb_naming — the workspace-typed identifier helper."""

from __future__ import annotations

import pytest

from dataraum.core.duckdb_naming import (
    is_reserved_schema,
    qualified_table,
    sanitize_identifier,
    schema_for_layer,
    workspace_table_name,
)


class TestSanitizeIdentifier:
    def test_lowercases_plain_input(self):
        assert sanitize_identifier("Orders") == "orders"

    def test_collapses_non_id_chars_to_underscore(self):
        assert sanitize_identifier("SalesLT.Customer") == "saleslt_customer"

    def test_collapses_runs_of_underscores(self):
        assert sanitize_identifier("weird---name") == "weird_name"

    def test_strips_leading_and_trailing_whitespace(self):
        assert sanitize_identifier("  orders  ") == "orders"

    def test_prefixes_leading_digit(self):
        assert sanitize_identifier("2024_orders") == "x_2024_orders"

    def test_raises_on_empty_after_sanitize(self):
        with pytest.raises(ValueError, match="empty"):
            sanitize_identifier("---")


class TestSchemaForLayer:
    @pytest.mark.parametrize(
        "layer,expected",
        [("raw", "raw"), ("typed", "typed"), ("quarantine", "quarantine")],
    )
    def test_known_layers(self, layer, expected):
        assert schema_for_layer(layer) == expected

    @pytest.mark.parametrize("layer", ["enriched", "slicing_view", "slice"])
    def test_view_like_layers_fall_back_to_typed(self, layer):
        assert schema_for_layer(layer) == "typed"


class TestWorkspaceTableName:
    def test_is_narrow_no_source_prefix(self):
        # DAT-639: workspace-unique narrow name — no `{source}__` prefix.
        assert workspace_table_name("Orders") == "orders"

    def test_sanitizes(self):
        assert workspace_table_name("Sales.LT Customer") == "sales_lt_customer"

    def test_deterministic(self):
        assert workspace_table_name("Customer") == workspace_table_name("Customer")


class TestQualifiedTable:
    def test_composes_schema_plus_narrow_table(self):
        assert qualified_table("typed", "orders") == "typed.orders"

    def test_uses_raw_schema_for_raw_layer(self):
        assert qualified_table("raw", "Customer") == "raw.customer"

    def test_quarantine_layer_routes_to_quarantine_schema(self):
        assert qualified_table("quarantine", "orders") == "quarantine.orders"

    def test_view_like_layer_falls_back_to_typed_schema(self):
        # Slice 1 keeps enriched/slicing_view artifacts under the typed schema.
        assert qualified_table("enriched", "orders") == "typed.orders"


class TestIsReservedSchema:
    @pytest.mark.parametrize("schema", ["session_abc", "archive_xyz"])
    def test_reserved_prefixes(self, schema):
        assert is_reserved_schema(schema) is True

    @pytest.mark.parametrize("schema", ["raw", "typed", "quarantine", "session"])
    def test_non_reserved_passes(self, schema):
        # Bare "session" (no underscore-suffix) is NOT reserved — only
        # the prefix form ``session_*`` is.
        assert is_reserved_schema(schema) is False
