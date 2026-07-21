"""Tests for ``analyze_basic_temporal`` — the surviving temporal substrate.

DAT-783 consolidated every served temporal fact onto this single DISTINCT-timestamp
pass (span, granularity + confidence, completeness/gaps, staleness) and deleted the
duplicate-corrupted row-interval path plus the WRONG fiscal/update-frequency analyzers.
These tests lock in the behaviour the cockpit + coverage surfaces now read.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import duckdb
import pytest

from dataraum.analysis.temporal.detection import analyze_basic_temporal
from dataraum.analysis.temporal.models import DATE_TRUNC_GRAINS

# Real thresholds mirrored from config/phases/temporal.yaml (self-contained — no
# config bind-mount needed at unit-test time).
_CONFIG = {
    "granularity": {
        "definitions": [
            ["second", 1, 0.5],
            ["minute", 60, 5],
            ["hour", 3600, 300],
            ["day", 86400, 3600],
            ["week", 604800, 86400],
            ["month", 2592000, 259200],
            ["quarter", 7776000, 777600],
            ["year", 31536000, 3153600],
        ],
        "default_confidence": 0.7,
        "irregular_confidence": 0.3,
        "variation_divisor": 10,
    },
    "gaps": {
        "significant_gap_multiplier": 2.0,
        "severity_severe_multiplier": 10.0,
        "severity_moderate_multiplier": 5.0,
    },
    "staleness": {"stale_multiplier": 2.0},
}


def _table(conn: duckdb.DuckDBPyConnection, dates: list[datetime]) -> None:
    conn.execute("CREATE OR REPLACE TABLE t (ts TIMESTAMP)")
    conn.executemany("INSERT INTO t VALUES (?)", [(d,) for d in dates])


def test_daily_series_is_day_granularity_and_complete() -> None:
    conn = duckdb.connect()
    base = datetime(2024, 1, 1)
    _table(conn, [base + timedelta(days=i) for i in range(30)])

    v = analyze_basic_temporal(conn, "t", "ts", config=_CONFIG).unwrap()

    assert v["granularity"] == "day"
    assert v["granularity_confidence"] > 0.5
    assert v["span_days"] == 29
    assert v["completeness"].completeness_ratio >= 0.95
    assert v["completeness"].gap_count == 0


def test_planted_gap_is_detected_severe() -> None:
    conn = duckdb.connect()
    base = datetime(2024, 1, 1)
    dates = [base + timedelta(days=i) for i in range(30)]
    dates += [base + timedelta(days=50 + i) for i in range(30)]  # 20-day hole
    _table(conn, dates)

    comp = analyze_basic_temporal(conn, "t", "ts", config=_CONFIG).unwrap()["completeness"]

    assert comp.gap_count == 1
    assert comp.largest_gap_days is not None and comp.largest_gap_days >= 19
    assert comp.gaps[0].severity == "severe"


def test_duplicate_heavy_fact_column_still_reads_daily() -> None:
    """A multi-row-per-day fact column stays 'day', not 'irregular'.

    Regression guard for the deleted row-interval path: with more rows than distinct
    days, the median ROW interval was 0 → granularity collapsed to 'irregular'. The
    DISTINCT pass is robust — this is why fiscal/update-frequency were deleted, not wired.
    """
    conn = duckdb.connect()
    base = datetime(2024, 1, 1)
    dates: list[datetime] = []
    for i in range(60):
        day = base + timedelta(days=i)
        dates += [day, day, day]  # three rows per day
    _table(conn, dates)

    v = analyze_basic_temporal(conn, "t", "ts", config=_CONFIG).unwrap()

    assert v["granularity"] == "day"
    assert v["completeness"].actual_periods == 60  # distinct days, not row count


def test_resolution_equals_grain_is_unchanged_and_exact() -> None:
    """The common case: one instant per bucket, at the grain. Ratio is exact.

    When the column's resolution already IS the detected grain, bucket-counting and
    instant-counting agree, so DAT-810 must not move this number. 60 consecutive days,
    no holes: 60 buckets over a 59-day span → expected 60, ratio exactly 1.0 — a
    genuine 1.0 that is *computed*, not clamped into place.
    """
    conn = duckdb.connect()
    base = datetime(2024, 1, 1)
    _table(conn, [base + timedelta(days=i) for i in range(60)])

    c = analyze_basic_temporal(conn, "t", "ts", config=_CONFIG).unwrap()["completeness"]

    assert (c.actual_periods, c.expected_periods) == (60, 60)
    assert c.completeness_ratio == 1.0


def test_subgrain_timestamps_do_not_inflate_completeness_past_one() -> None:
    """Sub-grain instants must not overrun the bucket denominator (DAT-810).

    The shape that triggers the defect: a TIMESTAMP column at day cadence where a
    MINORITY of days carry a second intra-day instant. The minority matters — grain is
    inferred from the *median* gap, so intra-day duplicates have to stay below half the
    gaps or the median drops off 86400 and the column reads 'irregular' instead of
    'day'. (This is also why the finance corpus cannot trigger it: every date column
    there is a plain DATE, so there are no sub-day instants at all.)

    Layout: a 22-day window, days 10 and 16 absent (20 days present), and 5 of the
    present days carrying an extra 17:00 instant beside the 09:00 one.
      - 25 distinct instants, 20 distinct day buckets, span 21d → expected 22 buckets.
      - Gaps sort to 5×28800, 5×57600, 12×86400, 2×172800 → median 86400 → 'day'.
      - OLD: 25 raw instants / 22 buckets = 1.136 → min(…, 1.0) → a reported 1.0.
      - NEW: 20 buckets / 22 buckets = 0.909.
    The clamp did not merely round a wrong number — it certified a column that is
    missing 2 of its 22 days as *perfectly complete*, and gap_count is 0 here (both
    holes are 172800s, exactly at the 2×median threshold, not above it), so
    completeness is the only signal that could have carried the absence.
    """
    conn = duckdb.connect()
    base = datetime(2024, 1, 1, 9, 0)
    missing = {10, 16}
    dup = {2, 4, 6, 12, 14}
    dates: list[datetime] = []
    for i in range(1, 23):
        if i in missing:
            continue
        day = base + timedelta(days=i - 1)
        dates.append(day)
        if i in dup:
            dates.append(day + timedelta(hours=8))
    _table(conn, dates)

    v = analyze_basic_temporal(conn, "t", "ts", config=_CONFIG).unwrap()
    c = v["completeness"]

    assert v["granularity"] == "day"  # the median gap must still land on 'day'
    assert v["distinct_count"] == 25  # raw instants — the old, wrong numerator
    assert c.actual_periods == 20  # day buckets — the correct numerator
    assert c.expected_periods == 22
    assert c.completeness_ratio is not None
    assert c.completeness_ratio == pytest.approx(20 / 22)
    assert c.completeness_ratio < 1.0  # the false "complete" verdict is gone
    assert c.gap_count == 0  # nothing else would have flagged the 2 missing days


def test_month_denominator_counts_calendar_months_not_nominal_seconds() -> None:
    """The denominator is calendar buckets, not elapsed/nominal seconds (DAT-810).

    Config's per-granularity seconds are nominal — a month is 2592000s (30 days) — which
    is fine for *inferring* the grain but wrong for counting it. Jan 1 / Feb 1 / Mar 1
    2023 spans 59 days (Feb has 28), so the old seconds-division denominator gave
    59/30 + 1 = 2 "expected months" against 3 real month buckets → ratio 1.5. Counting
    the denominator on the calendar (``date_diff`` between truncated endpoints) gives 3,
    and the series is exactly complete: 3/3.

    This is the same unit-mismatch defect as the raw-instants numerator, on the other
    side of the ratio — and it is why the clamp could be deleted rather than moved.
    """
    conn = duckdb.connect()
    _table(conn, [datetime(2023, m, 1) for m in (1, 2, 3)])

    v = analyze_basic_temporal(conn, "t", "ts", config=_CONFIG).unwrap()
    c = v["completeness"]

    assert v["granularity"] == "month"
    assert (c.actual_periods, c.expected_periods) == (3, 3)
    assert c.completeness_ratio == 1.0


def _at_cadence(grain: str) -> tuple[list[datetime], timedelta]:
    """20 instants at ``grain`` cadence, plus the sub-grain offset to perturb them with.

    The offset always lands inside its own bucket, so it adds an instant without adding
    a bucket — the sub-grain shape that overran the old raw-instant numerator.
    """
    monday = datetime(2024, 1, 1)  # 2024-01-01 is a Monday, so week buckets align
    per_grain: dict[str, tuple[list[datetime], timedelta]] = {
        "second": ([monday + timedelta(seconds=i) for i in range(20)], timedelta(milliseconds=500)),
        "minute": ([monday + timedelta(minutes=i) for i in range(20)], timedelta(seconds=30)),
        "hour": ([monday + timedelta(hours=i) for i in range(20)], timedelta(minutes=30)),
        "day": ([monday + timedelta(days=i) for i in range(20)], timedelta(hours=9)),
        "week": ([monday + timedelta(weeks=i) for i in range(20)], timedelta(days=3)),
        # Calendar-awkward on purpose: 20 consecutive months (two Februaries, 28/30/31-day
        # months), 20 quarters, and 20 years spanning five leap years.
        "month": ([datetime(2023 + m // 12, m % 12 + 1, 1) for m in range(20)], timedelta(days=10)),
        "quarter": (
            [datetime(2020 + q // 4, (q % 4) * 3 + 1, 1) for q in range(20)],
            timedelta(days=20),
        ),
        "year": ([datetime(2005 + i, 1, 1) for i in range(20)], timedelta(days=100)),
    }
    return per_grain[grain]


def test_completeness_ratio_never_exceeds_one_across_grains() -> None:
    """The invariant the deleted clamp used to fake, now true by construction.

    Sweeps every ``date_trunc`` grain with sub-grain noise (an extra instant inside a
    bucket) over calendar-awkward spans (short/long months, quarter and year boundaries,
    leap years) — the shapes that made both seconds-vs-buckets mismatches fire.
    ``actual`` counts buckets holding data; ``expected`` counts the buckets of the closed
    window that contains them; so ``actual <= expected`` always, with no clamp in the code.

    The noise is deliberately a MINORITY (3 perturbed of 20): each one splits a
    grain-sized gap into two sub-grain gaps, so past ``K < (B-1)/3`` the median gap drops
    off the grain and the column reads 'irregular' instead — which is the honest verdict,
    but not the case under test here.
    """
    cases: list[tuple[str, list[datetime]]] = []
    for grain in sorted(DATE_TRUNC_GRAINS):
        base, offset = _at_cadence(grain)
        cases.append((grain, base + [base[i] + offset for i in (2, 7, 13)]))

    for grain, dates in cases:
        conn = duckdb.connect()
        _table(conn, dates)
        v = analyze_basic_temporal(conn, "t", "ts", config=_CONFIG).unwrap()
        c = v["completeness"]

        assert v["granularity"] == grain, f"{grain}: inferred {v['granularity']!r}"
        assert c.actual_periods is not None and c.expected_periods is not None
        assert c.actual_periods <= c.expected_periods, (
            f"{grain}: {c.actual_periods}>{c.expected_periods}"
        )
        assert c.completeness_ratio is not None
        assert 0.0 < c.completeness_ratio <= 1.0, f"{grain}: ratio {c.completeness_ratio}"


def test_irregular_grain_has_no_completeness_ratio() -> None:
    """An 'irregular' column falls loud: no bucket → no ratio (DAT-810).

    Gaps that match no granularity definition leave the grain as the 'irregular'
    sentinel, which has no ``date_trunc`` part and no config seconds — so there is
    neither a numerator nor a denominator. Completeness must be absent, not a
    plausible default. The old code gave the sentinel a 1-second fallback, making
    ``expected`` the elapsed seconds (a ~1.8M-period denominator) and the ratio a
    meaningless ~0.0.

    Gaps stay populated: they are measured against the median gap, not against a grain.
    """
    conn = duckdb.connect()
    base = datetime(2024, 1, 1)
    # Gaps of ~5 days: between 'day' (86400±3600) and 'week' (604800±86400) — no match.
    _table(conn, [base + timedelta(days=5 * i) for i in range(10)])

    v = analyze_basic_temporal(conn, "t", "ts", config=_CONFIG).unwrap()
    c = v["completeness"]

    assert v["granularity"] == "irregular"
    assert c.completeness_ratio is None
    assert c.actual_periods is None
    assert c.expected_periods is None
    assert c.gap_count == 0  # a regular 5-day cadence has no gap vs its own median


def test_unknown_grain_has_no_completeness_ratio() -> None:
    """A single distinct instant falls loud too: no median gap → no grain → no ratio."""
    conn = duckdb.connect()
    _table(conn, [datetime(2024, 6, 30) for _ in range(50)])

    v = analyze_basic_temporal(conn, "t", "ts", config=_CONFIG).unwrap()
    c = v["completeness"]

    assert v["granularity"] == "unknown"
    assert c.completeness_ratio is None
    assert c.actual_periods is None
    assert c.expected_periods is None


def test_old_data_is_stale_recent_data_is_not() -> None:
    conn = duckdb.connect()

    old = [datetime(2020, 1, 1) + timedelta(days=i) for i in range(30)]
    _table(conn, old)
    assert analyze_basic_temporal(conn, "t", "ts", config=_CONFIG).unwrap()["is_stale"] is True

    today = datetime.now(UTC).replace(tzinfo=None)
    recent = [today - timedelta(days=i) for i in range(30)]
    _table(conn, recent)
    assert analyze_basic_temporal(conn, "t", "ts", config=_CONFIG).unwrap()["is_stale"] is False


def test_single_distinct_timestamp_is_not_stale() -> None:
    """A repeated as_of/period_end date (one distinct value) has no cadence.

    There is no median gap to be stale against, so ``is_stale`` is False — not
    "any age at all" → True, which the None-guarded branch would otherwise produce
    (regression guard for the reviewer-caught default-stale bug).
    """
    conn = duckdb.connect()
    _table(conn, [datetime(2020, 6, 30) for _ in range(500)])  # all same day, very old

    v = analyze_basic_temporal(conn, "t", "ts", config=_CONFIG).unwrap()

    assert v["median_gap_seconds"] is None
    assert v["is_stale"] is False


def test_date_trunc_grains_match_the_real_config_definitions() -> None:
    """DAT-810 contract: ``DATE_TRUNC_GRAINS`` is a hand-typed frozenset, but its whole
    premise is that it equals ``granularity.definitions`` in the REAL
    config/phases/temporal.yaml — the labels ``infer_granularity`` actually mints.

    Nothing structural enforces that (config is YAML; ``db_models`` needs the constant at
    class-body-eval time, so it cannot load config there). Without this test, adding or
    renaming a grain in the YAML makes ``count_grain_periods`` silently return ``None``
    for that grain — completeness quietly vanishes for real data, with no error. That is
    precisely the "absence resolves to a plausible default" failure this ticket removed,
    so the drift must fail LOUDLY in CI instead.

    Also pins the inverse: the two sentinels are NOT grains and must never appear in the
    config definitions.
    """
    from dataraum.core.config import load_phase_config

    config = load_phase_config("temporal")
    config_grains = {d[0] for d in config["granularity"]["definitions"]}

    assert config_grains == set(DATE_TRUNC_GRAINS), (
        "config/phases/temporal.yaml granularity.definitions has drifted from "
        "DATE_TRUNC_GRAINS (analysis/temporal/models.py). Config-only labels silently "
        "lose completeness; constant-only labels are dead. Reconcile both."
    )
    assert not ({"irregular", "unknown"} & config_grains), (
        "irregular/unknown are infer_granularity's no-grain sentinels, not granularities"
    )
