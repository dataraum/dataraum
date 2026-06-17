"""Tests for the centralized slice naming (DAT-356).

These names are the contract between GENERATION (the slicing agent) and MATCHING.
The single source of truth lives in ``slicing.naming``; these pin the two
properties everything else relies on.
"""

from __future__ import annotations

from dataraum.analysis.slicing.naming import (
    slice_table_name,
    slice_table_prefix,
)


def test_full_name_starts_with_prefix() -> None:
    """Matchers scan by prefix and slice the value off — the full name must extend it."""
    prefix = slice_table_prefix("csv__orders", "region")
    name = slice_table_name("csv__orders", "region", "us")
    assert name.startswith(prefix)
    assert name[len(prefix) :] == "us"


def test_source_qualified_no_collision_across_sources() -> None:
    """Two same-named facts in different sources get DISTINCT names (the DAT-356 fix).

    The bug class: both sources' ``orders`` fact would otherwise produce
    ``slice_orders_region_us`` and overwrite each other in the shared lake.
    Keying off the source-qualified ``duckdb_path`` keeps them apart.
    """
    a = slice_table_name("csv_a__orders", "region", "us")
    b = slice_table_name("csv_b__orders", "region", "us")
    assert a != b


def test_sanitization_and_empty_value() -> None:
    """Non-alnum collapses to single underscores, lowercased; empty value → 'unknown'."""
    assert (
        slice_table_name("Sales.Orders", "Region Code", "N/A")
        == "slice_sales_orders_region_code_n_a"
    )
    assert slice_table_name("x", "c", "") == "slice_x_c_unknown"
