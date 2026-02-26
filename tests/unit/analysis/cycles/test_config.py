"""Tests for business cycle configuration loading."""

from __future__ import annotations

import pytest

from dataraum.analysis.cycles.config import (
    format_cycle_vocabulary_for_context,
    get_cycle_types,
    map_to_canonical_type,
)


class TestGetCycleTypes:
    def test_returns_all_10_cycle_types(self) -> None:
        types = get_cycle_types("finance")
        assert len(types) == 10

    def test_contains_original_8_types(self) -> None:
        types = get_cycle_types("finance")
        expected = {
            "order_to_cash",
            "accounts_receivable",
            "procure_to_pay",
            "accounts_payable",
            "inventory_cycle",
            "hire_to_retire",
            "payroll_cycle",
            "asset_lifecycle",
        }
        assert expected.issubset(types.keys())

    def test_contains_promoted_gl_cycles(self) -> None:
        types = get_cycle_types("finance")
        assert "journal_entry_cycle" in types
        assert "intercompany_cycle" in types

    def test_gl_cycles_have_stages(self) -> None:
        types = get_cycle_types("finance")
        je = types["journal_entry_cycle"]
        assert len(je["typical_stages"]) == 4
        ic = types["intercompany_cycle"]
        assert len(ic["typical_stages"]) == 3

    def test_feeds_into_present(self) -> None:
        types = get_cycle_types("finance")
        # order_to_cash feeds into AR and GL
        assert "accounts_receivable" in types["order_to_cash"]["feeds_into"]
        assert "journal_entry_cycle" in types["order_to_cash"]["feeds_into"]
        # journal_entry_cycle is a hub — many cycles feed into it
        feeders = [
            name for name, defn in types.items()
            if "journal_entry_cycle" in defn.get("feeds_into", [])
        ]
        assert len(feeders) >= 5

    def test_intercompany_is_terminal(self) -> None:
        types = get_cycle_types("finance")
        assert types["intercompany_cycle"].get("feeds_into") is None


class TestMapToCanonicalType:
    def test_direct_match(self) -> None:
        canonical, is_known = map_to_canonical_type("order_to_cash", "finance")
        assert canonical == "order_to_cash"
        assert is_known is True

    def test_alias_ar_cycle(self) -> None:
        canonical, is_known = map_to_canonical_type("ar_cycle", "finance")
        assert canonical == "accounts_receivable"
        assert is_known is True

    def test_alias_o2c(self) -> None:
        canonical, is_known = map_to_canonical_type("o2c", "finance")
        assert canonical == "order_to_cash"
        assert is_known is True

    def test_case_insensitive(self) -> None:
        canonical, is_known = map_to_canonical_type("Order_To_Cash", "finance")
        assert canonical == "order_to_cash"
        assert is_known is True

    def test_unknown_type(self) -> None:
        canonical, is_known = map_to_canonical_type("nonexistent_cycle", "finance")
        assert canonical is None
        assert is_known is False

    def test_empty_string(self) -> None:
        canonical, is_known = map_to_canonical_type("", "finance")
        assert canonical is None
        assert is_known is False

    def test_gl_cycle_direct(self) -> None:
        canonical, is_known = map_to_canonical_type("journal_entry_cycle", "finance")
        assert canonical == "journal_entry_cycle"
        assert is_known is True

    def test_gl_cycle_alias(self) -> None:
        canonical, is_known = map_to_canonical_type("je_cycle", "finance")
        assert canonical == "journal_entry_cycle"
        assert is_known is True

    def test_intercompany_alias(self) -> None:
        canonical, is_known = map_to_canonical_type("ic_cycle", "finance")
        assert canonical == "intercompany_cycle"
        assert is_known is True


class TestFormatCycleVocabulary:
    def test_includes_all_cycle_types(self) -> None:
        output = format_cycle_vocabulary_for_context(vertical="finance")
        assert "order_to_cash" in output
        assert "accounts_receivable" in output
        assert "journal_entry_cycle" in output
        assert "intercompany_cycle" in output

    def test_includes_analysis_hints(self) -> None:
        output = format_cycle_vocabulary_for_context(vertical="finance")
        assert "ANALYSIS GUIDANCE" in output

    def test_no_domain_specifics_section(self) -> None:
        output = format_cycle_vocabulary_for_context(vertical="finance")
        assert "DOMAIN SPECIFICS" not in output

    def test_includes_feeds_into(self) -> None:
        output = format_cycle_vocabulary_for_context(vertical="finance")
        assert "Feeds into: accounts_receivable, journal_entry_cycle" in output

    def test_raises_for_unknown_vertical(self) -> None:
        with pytest.raises(FileNotFoundError):
            format_cycle_vocabulary_for_context(vertical="nonexistent")
