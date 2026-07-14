"""_orient_fk_direction — persist a confirmed FK many→one, child→parent (DAT-758).

The LLM judge intermittently reverses the direction; the measured cardinality is
the reliable signal. Every consumer that reads the stored direction assumes
``from`` = the many/fact side (og_references, the conformed-dim slice identity, the
enrichment prompt's grain-safe marker), so the persist orients it deterministically.
"""

from __future__ import annotations

from dataraum.analysis.semantic.processor import _orient_fk_direction


def test_one_to_many_is_flipped_to_many_to_one() -> None:
    # Judge emitted parent→child (journal_entries → journal_lines): measured
    # one-to-many. Flip so the row is many-to-one child→parent.
    ev = {"left_referential_integrity": 0.6, "right_referential_integrity": 1.0}
    ft, fc, tt, tc, card = _orient_fk_direction(
        "entries_tbl", "entries_col", "lines_tbl", "lines_col", "one-to-many", ev
    )
    assert (ft, fc, tt, tc) == ("lines_tbl", "lines_col", "entries_tbl", "entries_col")
    assert card == "many-to-one"
    # Directional evidence follows the swap; RI(from→to) exchanges.
    assert ev["left_referential_integrity"] == 1.0
    assert ev["right_referential_integrity"] == 0.6
    # A many-to-one child→parent join never fans out.
    assert ev["introduces_duplicates"] is False


def test_many_to_one_is_untouched() -> None:
    ev = {"left_referential_integrity": 1.0}
    result = _orient_fk_direction("a", "ac", "b", "bc", "many-to-one", ev)
    assert result == ("a", "ac", "b", "bc", "many-to-one")
    assert ev == {"left_referential_integrity": 1.0}  # unchanged


def test_one_to_one_is_untouched() -> None:
    # Orientation-agnostic — either endpoint is a valid ``from``.
    result = _orient_fk_direction("a", "ac", "b", "bc", "one-to-one", {})
    assert result == ("a", "ac", "b", "bc", "one-to-one")


def test_unknown_cardinality_is_untouched() -> None:
    # Cannot orient without a measured cardinality (no duckdb / not a candidate).
    result = _orient_fk_direction("a", "ac", "b", "bc", None, {})
    assert result == ("a", "ac", "b", "bc", None)


def test_flip_handles_missing_ri_fields() -> None:
    # No RI in evidence (metrics unavailable) — flip still swaps endpoints and
    # sets the non-fan-out flag without inventing RI keys.
    ev: dict[str, object] = {}
    ft, fc, tt, tc, card = _orient_fk_direction("a", "ac", "b", "bc", "one-to-many", ev)
    assert (ft, fc, tt, tc, card) == ("b", "bc", "a", "ac", "many-to-one")
    assert ev == {"introduces_duplicates": False}
