"""Curated catalog reads and the account they give of themselves (DAT-879/622).

The rule under test: serve what was JUDGED, order it by what was MEASURED, and
say what was left behind. The old behaviour — ``LIMIT 12`` with an alphabetical
tail and no note — is what these replace.
"""

from __future__ import annotations

from dataraum.analysis.slicing.curation import curated_slices
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.storage.base import load_all_models

# Instantiating a mapped class configures the mapper registry, which needs EVERY
# model imported. This file imports one directly, so register them all — a no-op
# when a sibling already did.
load_all_models()


def _row(name: str, *, interest: str | None, relevance: float | None) -> SliceDefinition:
    return SliceDefinition(
        run_id="r",
        table_id="t",
        column_id=f"c_{name}",
        column_name=name,
        slice_interest=interest,
        slice_relevance=relevance,
    )


class TestThreshold:
    def test_serves_judged_rows_only(self) -> None:
        rows = [
            _row("judged_a", interest="primary", relevance=0.5),
            _row("unjudged", interest=None, relevance=0.99),
        ]
        c = curated_slices(rows)
        assert [r.column_name for r in c.served] == ["judged_a"]
        assert c.total == 2
        assert c.dropped_unjudged == 1

    def test_a_high_measuring_unjudged_row_is_still_dropped(self) -> None:
        """Relevance says an axis is USABLE, not that a reader wants it. The
        measured number must never overrule the judgment — including by
        promoting something the judge never looked at."""
        rows = [
            _row("judged", interest="supporting", relevance=0.05),
            _row("unjudged", interest=None, relevance=1.0),
        ]
        assert [r.column_name for r in curated_slices(rows).served] == ["judged"]


class TestOrdering:
    def test_primary_precedes_supporting(self) -> None:
        rows = [
            _row("sup", interest="supporting", relevance=0.99),
            _row("pri", interest="primary", relevance=0.01),
        ]
        assert [r.column_name for r in curated_slices(rows).served] == ["pri", "sup"]

    def test_relevance_orders_within_a_tier_descending(self) -> None:
        rows = [
            _row("low", interest="primary", relevance=0.2),
            _row("high", interest="primary", relevance=0.9),
            _row("mid", interest="primary", relevance=0.5),
        ]
        assert [r.column_name for r in curated_slices(rows).served] == ["high", "mid", "low"]

    def test_unmeasured_sorts_last_within_its_tier(self) -> None:
        """NULL relevance must not win by accident — the failure mode of every
        sort that treats an absent number as zero-and-therefore-first."""
        rows = [
            _row("unmeasured", interest="primary", relevance=None),
            _row("measured", interest="primary", relevance=0.1),
        ]
        assert [r.column_name for r in curated_slices(rows).served] == ["measured", "unmeasured"]

    def test_name_breaks_exact_ties_deterministically(self) -> None:
        rows = [
            _row("b", interest="primary", relevance=0.5),
            _row("a", interest="primary", relevance=0.5),
        ]
        assert [r.column_name for r in curated_slices(rows).served] == ["a", "b"]


class TestTheAccountItGives:
    def test_note_names_the_count_and_the_reason(self) -> None:
        rows = [_row("j", interest="primary", relevance=0.5)] + [
            _row(f"u{i}", interest=None, relevance=0.5) for i in range(9)
        ]
        note = curated_slices(rows).note
        assert "1 of 10" in note
        assert "9" in note
        # The REASON matters as much as the number: "dropped" invites the model
        # to assume the tail was junk; "never judged" is what actually happened.
        assert "NOT judged" in note

    def test_no_note_when_nothing_was_dropped(self) -> None:
        rows = [_row("a", interest="primary", relevance=0.5)]
        assert curated_slices(rows).note == ""

    def test_empty_catalog_says_nothing(self) -> None:
        c = curated_slices([])
        assert c.served == [] and c.note == "" and not c.unjudged_fallback


class TestUnjudgedFallback:
    """When the ranker did not run, no row carries a judgment. Serving nothing
    would hide the catalog; serving it as if curated would be a lie."""

    def test_serves_everything_by_measured_relevance(self) -> None:
        rows = [
            _row("low", interest=None, relevance=0.1),
            _row("high", interest=None, relevance=0.9),
        ]
        c = curated_slices(rows)
        assert [r.column_name for r in c.served] == ["high", "low"]
        assert c.unjudged_fallback
        assert c.dropped_unjudged == 0

    def test_note_says_the_ordering_is_structural_only(self) -> None:
        note = curated_slices([_row("a", interest=None, relevance=0.5)]).note
        assert "did not run" in note
        assert "measured partition quality only" in note
