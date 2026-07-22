"""Tests for business cycle configuration loading."""

from __future__ import annotations

from dataraum.analysis.cycles.config import (
    UNDETERMINED_DIRECTION,
    format_cycle_vocabulary_for_context,
    get_cycle_types,
    map_to_canonical_type,
    resolve_cycle_identity,
)

# The finance settlement family, as the seed loads it — the declaration the save-time
# resolution validates the emitted direction against.
_FINANCE_FAMILIES = {
    "settlement": {"incoming": "accounts_receivable", "outgoing": "accounts_payable"}
}


class TestGetCycleTypes:
    def test_returns_all_12_cycle_types(self) -> None:
        types = get_cycle_types("finance")
        assert len(types) == 12

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
        assert "period_close" in types

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
            name
            for name, defn in types.items()
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

    def test_unknown_type_preserved(self) -> None:
        canonical, is_known = map_to_canonical_type("nonexistent_cycle", "finance")
        assert canonical == "nonexistent_cycle"
        assert is_known is False

    def test_unknown_type_normalized(self) -> None:
        canonical, is_known = map_to_canonical_type("Some_Unknown_Cycle", "finance")
        assert canonical == "some_unknown_cycle"
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

    def test_period_close_direct(self) -> None:
        canonical, is_known = map_to_canonical_type("period_close", "finance")
        assert canonical == "period_close"
        assert is_known is True

    def test_period_close_alias_financial_reporting(self) -> None:
        canonical, is_known = map_to_canonical_type("financial_reporting", "finance")
        assert canonical == "period_close"
        assert is_known is True

    def test_period_close_alias_trial_balance(self) -> None:
        canonical, is_known = map_to_canonical_type("trial_balance_reporting", "finance")
        assert canonical == "period_close"
        assert is_known is True


class TestFormatCycleVocabulary:
    def test_includes_all_cycle_types(self) -> None:
        output = format_cycle_vocabulary_for_context(vertical="finance")
        assert "order_to_cash" in output
        assert "accounts_receivable" in output
        assert "journal_entry_cycle" in output
        assert "intercompany_cycle" in output
        assert "period_close" in output

    def test_includes_analysis_hints(self) -> None:
        output = format_cycle_vocabulary_for_context(vertical="finance")
        assert "ANALYSIS GUIDANCE" in output

    def test_no_domain_specifics_section(self) -> None:
        output = format_cycle_vocabulary_for_context(vertical="finance")
        assert "DOMAIN SPECIFICS" not in output

    def test_includes_feeds_into(self) -> None:
        output = format_cycle_vocabulary_for_context(vertical="finance")
        assert "Feeds into: accounts_receivable, journal_entry_cycle" in output

    def test_unknown_vertical_is_empty_not_an_error(self) -> None:
        # Overlay-aware loader (DAT-455): an unknown vertical resolves to an
        # EMPTY config, never raises — "no declared cycles" is a loud phase-tier
        # outcome, not a loader crash.
        assert format_cycle_vocabulary_for_context(vertical="nonexistent") == ""


class TestResolveCycleIdentity:
    """The direction-axis resolution (DAT-856) — the sole producer of (family, direction)."""

    def test_decided_direction_resolves_to_the_member(self) -> None:
        r = resolve_cycle_identity(
            cycle_type="settlement",
            family="settlement",
            direction="outgoing",
            cycle_families=_FINANCE_FAMILIES,
            vertical="finance",
        )
        # A decided direction resolves to its declared member — canonical is the member,
        # keeping the vocabulary is_known_type and the validation-health linkage on it.
        assert r.canonical_type == "accounts_payable"
        assert r.is_known_type is True
        assert r.family == "settlement"
        assert r.direction == "outgoing"

    def test_incoming_resolves_to_receivable(self) -> None:
        r = resolve_cycle_identity(
            cycle_type="settlement",
            family="settlement",
            direction="incoming",
            cycle_families=_FINANCE_FAMILIES,
            vertical="finance",
        )
        assert r.canonical_type == "accounts_receivable"
        assert r.direction == "incoming"

    def test_undetermined_keeps_the_family_as_canonical(self) -> None:
        r = resolve_cycle_identity(
            cycle_type="settlement",
            family="settlement",
            direction=UNDETERMINED_DIRECTION,
            cycle_families=_FINANCE_FAMILIES,
            vertical="finance",
        )
        # The detected-but-undirected state: canonical is the FAMILY, never a coerced
        # member; is_known because the family is declared.
        assert r.canonical_type == "settlement"
        assert r.is_known_type is True
        assert r.family == "settlement"
        assert r.direction == UNDETERMINED_DIRECTION

    def test_off_vocab_direction_degrades_to_undetermined(self) -> None:
        # A direction the family does not declare: keep the family detection, leave the
        # axis honestly undetermined rather than guess a member (recall over coercion).
        r = resolve_cycle_identity(
            cycle_type="settlement",
            family="settlement",
            direction="sideways",
            cycle_families=_FINANCE_FAMILIES,
            vertical="finance",
        )
        assert r.canonical_type == "settlement"
        assert r.family == "settlement"
        assert r.direction == UNDETERMINED_DIRECTION

    def test_undeclared_family_falls_to_the_cycle_type_path(self) -> None:
        # The judge named a family the vertical does not declare → resolve by cycle_type,
        # family/direction NULL (a non-family cycle).
        r = resolve_cycle_identity(
            cycle_type="order_to_cash",
            family="not_a_family",
            direction="incoming",
            cycle_families=_FINANCE_FAMILIES,
            vertical="finance",
        )
        assert r.canonical_type == "order_to_cash"
        assert r.is_known_type is True
        assert r.family is None
        assert r.direction is None

    def test_non_family_cycle_keeps_todays_behavior(self) -> None:
        r = resolve_cycle_identity(
            cycle_type="order_to_cash",
            family="",
            direction="",
            cycle_families=_FINANCE_FAMILIES,
            vertical="finance",
        )
        assert r.canonical_type == "order_to_cash"
        assert r.is_known_type is True
        assert r.family is None
        assert r.direction is None

    def test_non_family_off_vocab_cycle_preserved(self) -> None:
        # The existing off-vocab fallback is untouched for the non-family path.
        r = resolve_cycle_identity(
            cycle_type="incident_resolution",
            family="",
            direction="",
            cycle_families=_FINANCE_FAMILIES,
            vertical="finance",
        )
        assert r.canonical_type == "incident_resolution"
        assert r.is_known_type is False
        assert r.family is None
        assert r.direction is None
