"""Curating the slice catalog for an LLM-facing surface (DAT-879 / DAT-622).

The catalog is the FULL deterministic dimension inventory (DAT-725). Every
LLM-facing surface wants a curated subset of it, and every one of them used to
get that subset the same silent way: ``ORDER BY slice_priority LIMIT
CURATED_SLICE_BUDGET``, a bare constant of 12. Three things were wrong with
that, and they are why DAT-280/622/827 were one ticket:

* it truncated without telling anyone — the model was handed 12 dimensions and
  no indication that 40 more existed (DAT-622);
* the cut could not be justified, because an ordinal has no scale to threshold
  on (DAT-827);
* and it silently degraded, because every un-ranked row tied at the priority
  floor, so the tiebreak ``column_name`` chose the tail. A surface that
  advertised "the most interesting dimensions" was, past the ranked rows,
  listing them in alphabetical order.

``curated_slices`` replaces all of that with one rule, applied identically at
every surface: serve what the agent JUDGED to be a business axis, ordered by
measured relevance, and state what was left out. There is no budget constant
here to tune, and no cut that cannot be described in a sentence.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from dataraum.analysis.slicing.models import SLICE_INTEREST_RANK

if TYPE_CHECKING:
    from dataraum.analysis.slicing.db_models import SliceDefinition

__all__ = [
    "UNJUDGED_FALLBACK_MAX",
    "CuratedSlices",
    "curated_slices",
]

# Cost bound for the un-judged fallback branch ONLY — NOT a relevance threshold
# and not a judgment. In that branch there is nothing to narrow by, so the read
# would otherwise serve the entire inventory; the cycles and validation
# decorations then run one statistical-profile query and render one value block
# PER served dimension, which on a wide corpus is ~50 of each. This caps that
# work. It has no semantic content, which is exactly why the branch must SAY it
# applied — see ``CuratedSlices.note``. The judged path has no cap at all: its
# size is bounded by what the agent was asked to judge.
UNJUDGED_FALLBACK_MAX = 25


@dataclass(frozen=True)
class CuratedSlices:
    """A curated catalog read, with the truthful account of what it dropped.

    ``served`` is what the surface renders. The counts are what it must SAY it
    is not rendering — a consumer that prints ``served`` without ``note`` has
    reintroduced the silent cap this module exists to remove.
    """

    served: list[SliceDefinition]
    total: int
    dropped_unjudged: int
    #: True when no row carried a judgment at all, so the fallback ordering
    #: (measured relevance over the whole inventory) is in force. That happens
    #: in the documented ranker-skipped operating modes — no LLM config, or the
    #: slicing feature disabled — and the surface must say so rather than
    #: present a structural ordering as a curated one.
    unjudged_fallback: bool

    @property
    def note(self) -> str:
        """One line stating what this read left out; "" when it dropped nothing.

        Written for an LLM reader: it names the count and the REASON, because
        "39 dimensions were dropped" invites the model to assume the tail was
        junk, while "39 were never judged" tells it the truth — that nothing
        looked at them.
        """
        if self.unjudged_fallback:
            if not self.total:
                return ""
            shown = (
                f"All {self.total} catalogued dimensions are shown"
                if len(self.served) == self.total
                else f"Showing {len(self.served)} of {self.total} catalogued dimensions"
            )
            return (
                f"{shown}, ordered by measured partition quality only: the "
                "cataloguing agent did not run for this run, so none carries a "
                "business-relevance judgment."
            )
        if not self.dropped_unjudged:
            return ""
        return (
            f"Showing {len(self.served)} of {self.total} catalogued dimensions. "
            f"The other {self.dropped_unjudged} were catalogued but NOT judged by the "
            "cataloguing agent — they are usable axes that were never assessed, not "
            "axes assessed and rejected."
        )


def _sort_key(row: SliceDefinition) -> tuple[int, float, str]:
    """Order a curated read: judged tier, then measured relevance, then name.

    Relevance descends (higher is better) and NULL — unmeasured, no statistical
    profile — sorts last within its tier rather than winning by accident.
    """
    relevance = row.slice_relevance if row.slice_relevance is not None else -1.0
    return (
        SLICE_INTEREST_RANK.get(row.slice_interest, len(SLICE_INTEREST_RANK)),
        -relevance,
        row.column_name or "",
    )


def curated_slices(rows: list[SliceDefinition]) -> CuratedSlices:
    """Curate a catalog read for an LLM-facing surface.

    Args:
        rows: The full table- and run-scoped inventory. Pass everything —
            the point of this function is that the caller no longer applies a
            ``LIMIT`` it cannot explain.

    Returns:
        The rows to serve plus the account of what was dropped. When no row
        carries a judgment (the ranker did not run), the whole inventory is
        served in measured-relevance order and ``unjudged_fallback`` is set:
        serving nothing would be a worse lie than serving it unranked, and
        silently presenting it as curated would be worse still.
    """
    ordered = sorted(rows, key=_sort_key)
    judged = [r for r in ordered if r.slice_interest is not None]

    if not judged:
        # Nothing to narrow by, so a cost bound applies instead of a threshold
        # (see ``UNJUDGED_FALLBACK_MAX``) — and the note states that it did.
        return CuratedSlices(
            served=ordered[:UNJUDGED_FALLBACK_MAX],
            total=len(ordered),
            dropped_unjudged=0,
            unjudged_fallback=bool(ordered),
        )

    return CuratedSlices(
        served=judged,
        total=len(ordered),
        dropped_unjudged=len(ordered) - len(judged),
        unjudged_fallback=False,
    )
