"""Measured relevance of a slice axis (DAT-879, absorbing DAT-280/622/827).

The catalog used to carry an ORDINAL ``slice_priority`` (1 = most interesting)
with an ``UNRANKED_SLICE_PRIORITY = 1000`` floor for the rows the agent never
looked at. An ordinal cannot be thresholded (rank 3 means nothing without
knowing what it is 3 *of*), cannot compare an axis on one table to an axis on
another, and cannot compare a categorical axis to a banded numeric one. The
floor made it worse: every un-ranked row tied at 1000, so a curated
``ORDER BY slice_priority, column_name LIMIT n`` filled its remaining budget
ALPHABETICALLY and called the result "the most interesting dimensions".

This module replaces that ordinal with a MEASURED score in [0, 1], carried by
every catalog row. It has no free parameters — nothing here is tuned, and there
is no threshold constant to calibrate:

    relevance = coverage x evenness

``coverage`` is the fraction of rows that land in a named bucket. ``evenness``
is Pielou's index — the Shannon entropy of the bucket distribution over its
maximum for that many buckets, ``H / ln(k)``. Read together: *of the data this
axis can speak about, how much of its structure does it actually resolve?*

``H / ln(k)`` rather than the effective-group ratio ``exp(H) / k``, which was
tried first and conflates: it scores a 99/1 near-constant boolean at 0.53, the
same as a merely skewed four-way split, because both resolve about half their
nominal groups. Pielou's index separates them (0.08 vs 0.62) while keeping the
property that matters below — it is scale-free in ``k``, so an even 4-way axis
and an even 800-way axis both score 1.0.

Both factors are computed from a BUCKET DISTRIBUTION — a list of member counts
— and nothing else. That is what makes the score kind-agnostic (DAT-280): a
categorical axis passes its per-value counts, and a banded numeric axis passes
its per-band counts, onto the same scale with the same meaning. The scorer
cannot tell the two apart, which is precisely the property the catalog needs.
(The numeric axis TYPE itself is not built here — this establishes the shared
scale it will land on.)

What the score deliberately does NOT do: prefer few groups to many. ``evenness``
is scale-free in the bucket count, so an even 4-way ``region`` and an even
800-way ``account_id`` both score 1.0. Which of those a reader wants first is a
business judgment, and it stays with the slicing agent (``slice_interest``) —
this number never overrules it. The score answers "is this axis usable, and how
much of the data does it resolve", which is a question the data can answer.

Truncation is reported, never guessed. The profiler stores at most ``top_k``
values per column, so an axis with more distinct values than that arrives with
a partial distribution. Rather than assert a number it cannot know, the scorer
returns a BOUNDED result: the unseen tail is treated as one bucket for the
lower bound and as maximally-even buckets for the upper bound. When the
distribution is complete the two bounds coincide.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

__all__ = [
    "SliceRelevance",
    "score_axis",
]


@dataclass(frozen=True)
class SliceRelevance:
    """A measured relevance score, with its bounds and its inputs.

    ``lower``/``upper`` differ only when the value distribution was truncated by
    the profiler's top-K. ``exact`` says which case this is, so a consumer can
    render "0.42" or "0.31-0.55 (partial distribution)" without re-deriving the
    reason. ``score`` is the value to sort and threshold on: the lower bound,
    because under-claiming an axis is the safe direction — an axis promoted on
    an assumed-even tail that is in fact one dominant value would be a silently
    bad recommendation, which is the failure class this ticket exists to close.
    """

    score: float
    lower: float
    upper: float
    coverage: float
    evenness: float
    exact: bool
    # The bucket count the score was computed over (the axis's distinct groups,
    # including the unseen tail) and how many of them the profile actually
    # carried. Equal ⇒ ``exact``.
    groups: int
    groups_measured: int

    @property
    def truncated(self) -> bool:
        """True when the profile's top-K hid part of the distribution."""
        return not self.exact


def _entropy(probabilities: list[float]) -> float:
    """Shannon entropy in nats over a probability vector (zeros ignored)."""
    return -sum(p * math.log(p) for p in probabilities if p > 0.0)


def score_axis(
    *,
    total_rows: int,
    null_count: int,
    bucket_counts: list[int],
    distinct_groups: int | None = None,
) -> SliceRelevance | None:
    """Score one candidate slice axis from its measured bucket distribution.

    Kind-agnostic by construction: ``bucket_counts`` are per-VALUE counts for a
    categorical axis and per-BAND counts for a numeric one. The scorer never
    learns which it was given.

    Args:
        total_rows: Rows in the table (the coverage denominator).
        null_count: Rows with no value on this axis — they fall outside every
            named bucket, so they reduce coverage rather than forming a group.
        bucket_counts: Measured member count per observed bucket, non-null
            only. May be a top-K PREFIX of the true distribution.
        distinct_groups: The axis's true group count (``COUNT(DISTINCT)``).
            When it exceeds ``len(bucket_counts)`` the distribution is
            truncated and the result is bounded rather than exact. Defaults to
            the number of buckets supplied (i.e. a complete distribution).

    Returns:
        The measured score, or None when the axis cannot be scored at all —
        no rows, or no profile to score. None means "unmeasured", which the
        catalog stores as NULL and the reads report as such; it never
        silently becomes a zero, because "this axis is bad" and "we never
        measured this axis" are different claims.
    """
    if total_rows <= 0 or not bucket_counts:
        return None

    observed = [c for c in bucket_counts if c > 0]
    if not observed:
        return None

    non_null = total_rows - null_count
    if non_null <= 0:
        return None

    groups = max(distinct_groups or len(observed), len(observed))
    coverage = min(non_null / total_rows, 1.0)

    seen = sum(observed)
    # The profile's counts are capped at top-K, so the tail mass is whatever
    # non-null rows they do not account for. Clamp: a profile written in a
    # different run than the row count can disagree slightly, and a negative
    # tail is not a thing we should propagate into a logarithm.
    tail_mass = max(non_null - seen, 0)
    tail_groups = max(groups - len(observed), 0)
    exact = tail_groups == 0 or tail_mass == 0

    # A single group partitions nothing, so its evenness is 0 by definition —
    # ln(1) = 0 would otherwise divide by zero. The pre-filter already drops
    # constants; this keeps the function total rather than relying on that.
    if groups < 2:
        return SliceRelevance(
            score=0.0,
            lower=0.0,
            upper=0.0,
            coverage=coverage,
            evenness=0.0,
            exact=True,
            groups=groups,
            groups_measured=len(observed),
        )

    max_entropy = math.log(groups)
    head = [c / non_null for c in observed]

    if exact:
        evenness_lo = evenness_hi = _entropy(head) / max_entropy
    else:
        # Lower bound: the unseen tail is ONE bucket (maximally concentrated) —
        # the least even the axis could be given what we measured.
        lo_probs = [*head, tail_mass / non_null]
        evenness_lo = _entropy(lo_probs) / max_entropy
        # Upper bound: the tail splits evenly across every unseen group — the
        # most even it could be.
        hi_probs = [*head, *([tail_mass / tail_groups / non_null] * tail_groups)]
        evenness_hi = _entropy(hi_probs) / max_entropy

    lower = coverage * min(evenness_lo, evenness_hi)
    upper = coverage * max(evenness_lo, evenness_hi)

    return SliceRelevance(
        score=lower,
        lower=lower,
        upper=upper,
        coverage=coverage,
        evenness=min(evenness_lo, evenness_hi),
        exact=exact,
        groups=groups,
        groups_measured=len(observed),
    )
