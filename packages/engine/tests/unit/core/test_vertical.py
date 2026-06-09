"""Tests for core/vertical.py — resolve_vertical born-loud discriminator (DAT-480)."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from dataraum.core.overlay import (
    OverlayRow,
    reset_overlay_resolver_for_tests,
    set_overlay_resolver,
)
from dataraum.core.vertical import (
    VerticalKind,
    available_verticals,
    require_known_vertical,
    resolve_vertical,
)


@pytest.fixture(autouse=True)
def _clean_resolver() -> Iterator[None]:
    """Every test starts (and ends) with no overlay resolver registered."""
    reset_overlay_resolver_for_tests()
    yield
    reset_overlay_resolver_for_tests()


def _frame(vertical: str, *, row_type: str = "concept") -> None:
    """Register a resolver exposing one vertical-scoped overlay row."""
    set_overlay_resolver(
        lambda: [OverlayRow(type=row_type, payload={"vertical": vertical, "name": "x"})]
    )


class TestResolveVertical:
    def test_shipped_builtin(self) -> None:
        """An on-disk builtin (finance) is shipped."""
        assert resolve_vertical("finance") is VerticalKind.SHIPPED

    @pytest.mark.parametrize("name", ["_adhoc", "_custom", None, ""])
    def test_placeholder(self, name: str | None) -> None:
        """_adhoc, any leading-underscore name, and a missing name are placeholders.

        _adhoc ships an on-disk dir yet must NOT read as a shipped vertical — the
        underscore convention wins (checked before the on-disk lookup).
        """
        assert resolve_vertical(name) is VerticalKind.PLACEHOLDER

    def test_framed_from_overlay(self) -> None:
        """A name with vertical-scoped overlay rows but no on-disk dir is framed."""
        _frame("sales")
        assert resolve_vertical("sales") is VerticalKind.FRAMED

    @pytest.mark.parametrize("row_type", ["concept", "validation", "cycle", "metric"])
    def test_framed_by_any_family_row(self, row_type: str) -> None:
        """Any vertical-scoped family row (not just concept) makes a vertical framed."""
        _frame("ops", row_type=row_type)
        assert resolve_vertical("ops") is VerticalKind.FRAMED

    def test_unknown_typo(self) -> None:
        """A typo with no on-disk dir and no overlay rows is unknown."""
        assert resolve_vertical("finanace") is VerticalKind.UNKNOWN

    def test_non_vertical_overlay_row_does_not_frame(self) -> None:
        """A non-vertical overlay type (e.g. null_value) never makes a name framed."""
        set_overlay_resolver(lambda: [OverlayRow(type="null_value", payload={"vertical": "ghost"})])
        assert resolve_vertical("ghost") is VerticalKind.UNKNOWN


class TestAvailableVerticals:
    def test_lists_shipped_excludes_placeholder(self) -> None:
        """Available verticals lists shipped builtins but never the _adhoc placeholder."""
        available = available_verticals()
        assert "finance" in available
        assert "_adhoc" not in available

    def test_includes_framed(self) -> None:
        """A framed vertical joins the available list (shipped ∪ framed)."""
        _frame("sales")
        available = available_verticals()
        assert "sales" in available
        assert "finance" in available


class TestRequireKnownVertical:
    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("finance", VerticalKind.SHIPPED),
            ("_adhoc", VerticalKind.PLACEHOLDER),
            (None, VerticalKind.PLACEHOLDER),
        ],
    )
    def test_known_passes_through(self, name: str | None, expected: VerticalKind) -> None:
        """Shipped / placeholder / framed names return their kind, never raise."""
        assert require_known_vertical(name) is expected

    def test_framed_passes_through(self) -> None:
        _frame("sales")
        assert require_known_vertical("sales") is VerticalKind.FRAMED

    def test_unknown_raises_born_loud_naming_available(self) -> None:
        """A typo'd vertical raises, naming the verticals that DO exist.

        RuntimeError (not ValueError) matches the sibling fail-loud raises at the
        operating_model resolve seam.
        """
        with pytest.raises(RuntimeError, match="Unknown vertical 'finanace'"):
            require_known_vertical("finanace")

    def test_unknown_error_lists_finance_not_adhoc(self) -> None:
        """The error names shipped/framed verticals (finance), never _adhoc."""
        with pytest.raises(RuntimeError) as exc:
            require_known_vertical("nope")
        assert "finance" in str(exc.value)
        assert "_adhoc" not in str(exc.value)
