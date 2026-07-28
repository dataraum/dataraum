"""Measured relevance of a slice axis (DAT-879, absorbing DAT-280/622/827).

The catalog used to carry an ORDINAL ``slice_priority`` (1 = most interesting)
with an ``UNRANKED_SLICE_PRIORITY = 1000`` floor for the rows the agent never
looked at. An ordinal cannot be thresholded (rank 3 means nothing without
knowing what it is 3 *of*), cannot compare an axis on one table to an axis on
another, and cannot compare a categorical axis to a banded numeric one. The
floor made it worse: every un-judged row tied at 1000, so a curated
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

What the score deliberately does NOT do: prefer few groups to many. Which of an
even 4-way ``region`` and an even 800-way ``account_id`` a reader wants first is
a business judgment, and it stays with the slicing agent (``slice_interest``) —
this number never overrules it. The score answers "is this axis usable, and how
much of the data does it resolve", which is a question the data can answer.

**Truncation, and what this function does NOT tell you.** The profiler stores at
most ``top_k`` values per column, so a high-cardinality axis arrives with a
partial distribution. The unseen tail is treated as a SINGLE bucket — the least
even it could be — so the number returned is a LOWER BOUND on the axis's true
relevance whenever the distribution was truncated. Under-claiming is the safe
direction: promoting an axis on an assumed-even tail that is really one dominant
value is exactly the silently-bad recommendation this ticket exists to prevent.

Callers cannot tell a bounded score from an exact one, deliberately: an earlier
draft returned the interval and an ``exact`` flag, and nothing consumed either.
Rendering the interval is parked until a surface actually asks for it — a
built-but-unwired field is a liability, not a head start.
"""

from __future__ import annotations

import math

__all__ = [
    "score_axis",
]


def _entropy(probabilities: list[float]) -> float:
    """Shannon entropy in nats over a probability vector (zeros ignored)."""
    return -sum(p * math.log(p) for p in probabilities if p > 0.0)


def score_axis(
    *,
    total_rows: int,
    null_count: int,
    bucket_counts: list[int],
    distinct_groups: int | None = None,
) -> float | None:
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
            truncated and the result is a lower bound (see the module
            docstring). Defaults to the number of buckets supplied.

    Returns:
        The measured score in [0, 1], or None when the axis cannot be scored at
        all — no rows, or no profile to score. None means "unmeasured", which
        the catalog stores as NULL and the reads report as such; it never
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

    coverage = non_null / total_rows
    seen = sum(observed)
    # The profile's counts are capped at top-K, so the tail mass is whatever
    # non-null rows they do not account for. Clamp: a profile written in a
    # different run than the row count can disagree, and a negative tail is not
    # a thing we should propagate into a logarithm.
    tail_mass = max(non_null - seen, 0)

    claimed_groups = max(distinct_groups or len(observed), len(observed))
    # When the observed buckets already account for every non-null row, any
    # groups ``distinct_count`` claims beyond them hold ZERO rows — the two
    # halves of a stale profile disagreeing. Normalizing over the claimed count
    # would then divide an entropy built from 200 buckets by ln(5000) and
    # deflate a perfectly good axis toward zero. The counts ARE the
    # measurement; trust them over a distinct_count they contradict.
    groups = claimed_groups if tail_mass > 0 else len(observed)

    # A single group partitions nothing, so its evenness is 0 by definition —
    # ln(1) = 0 would otherwise divide by zero. The pre-filter already drops
    # constants; this keeps the function total rather than relying on that.
    if groups < 2:
        return 0.0

    probabilities = [c / non_null for c in observed]
    if tail_mass > 0:
        probabilities.append(tail_mass / non_null)

    evenness = _entropy(probabilities) / math.log(groups)

    # Clamp at the boundary rather than trusting the arithmetic. Two REAL
    # inputs land outside [0, 1], and both would raise IntegrityError against
    # this column's CHECK — failing the slicing activity into an endless
    # deterministic Temporal retry, since the same data reproduces it forever:
    #   * an exactly-uniform distribution over k in {5, 13, 19, ...} floats to
    #     1.0000000000000002, because ln(k)/ln(k) is not exactly 1 in binary
    #     floating point;
    #   * a stale profile whose counts exceed the row count (e.g. [120, 5] over
    #     100 rows) gives probabilities > 1, hence a NEGATIVE entropy (-0.0995).
    return min(max(coverage * evenness, 0.0), 1.0)
