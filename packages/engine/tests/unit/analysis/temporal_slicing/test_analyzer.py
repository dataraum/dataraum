"""Unit tests for temporal slice analyzer pure functions."""

from __future__ import annotations

from datetime import date

import duckdb
import pytest

from dataraum.analysis.temporal_slicing.analyzer import (
    _bin_case,
    _build_drift_evidence,
    _generalized_drift,
    _generate_periods,
    _get_binned_distribution,
    _jensen_shannon_divergence,
    _pooled_quantile_edges,
)
from dataraum.analysis.temporal_slicing.models import TemporalSliceConfig, TimeGrain


class TestJensenShannonDivergence:
    """Tests for _jensen_shannon_divergence."""

    def test_identical_distributions(self):
        """JS divergence is 0 for identical distributions."""
        d = {"A": 0.5, "B": 0.3, "C": 0.2}
        assert _jensen_shannon_divergence(d, d) == pytest.approx(0.0, abs=1e-10)

    def test_completely_disjoint(self):
        """JS divergence is ln(2) for completely disjoint distributions."""
        p = {"A": 1.0}
        q = {"B": 1.0}
        assert _jensen_shannon_divergence(p, q) == pytest.approx(0.6931, abs=0.001)

    def test_symmetric(self):
        """JS divergence is symmetric: JSD(p,q) == JSD(q,p)."""
        p = {"A": 0.7, "B": 0.3}
        q = {"A": 0.4, "B": 0.6}
        assert _jensen_shannon_divergence(p, q) == pytest.approx(
            _jensen_shannon_divergence(q, p), abs=1e-10
        )

    def test_partial_overlap(self):
        """JS divergence is between 0 and ln(2) for partially overlapping."""
        p = {"A": 0.6, "B": 0.4}
        q = {"A": 0.3, "B": 0.5, "C": 0.2}
        result = _jensen_shannon_divergence(p, q)
        assert 0 < result < 0.6932

    def test_slight_change(self):
        """Small distribution change produces small divergence."""
        p = {"A": 0.5, "B": 0.5}
        q = {"A": 0.48, "B": 0.52}
        result = _jensen_shannon_divergence(p, q)
        assert result < 0.01

    def test_empty_distributions(self):
        """Empty distributions produce 0 divergence."""
        assert _jensen_shannon_divergence({}, {}) == pytest.approx(0.0)


class TestGeneralizedDrift:
    """Tests for _generalized_drift — drift as periods' disagreement (DAT-442).

    The size-weighted generalized JSD of per-period distributions vs the pooled
    mixture, normalized by the weight entropy to [0, 1] (= conflict C with periods
    as witnesses). Severity is NOT here — it's in the loss table.
    """

    def test_stationary_periods_zero(self):
        """Identical period distributions → no drift."""
        d = {"a": 0.5, "b": 0.5}
        assert _generalized_drift([d, d, d], [1.0, 1.0, 1.0]) == pytest.approx(0.0, abs=1e-12)

    def test_single_period_zero(self):
        """One period can't disagree with itself."""
        assert _generalized_drift([{"a": 1.0}], [1.0]) == 0.0

    def test_two_disjoint_periods_is_one(self):
        """Two equally-weighted disjoint periods → maximal disagreement."""
        assert _generalized_drift([{"a": 1.0}, {"b": 1.0}], [1.0, 1.0]) == pytest.approx(1.0)

    def test_two_clusters_normalized_by_weight_entropy(self):
        """4 periods in 2 disjoint clusters: 1 bit of value-disagreement over 2
        bits of period-entropy → 0.5 (the weight-entropy normalization)."""
        dists = [{"a": 1.0}, {"a": 1.0}, {"b": 1.0}, {"b": 1.0}]
        assert _generalized_drift(dists, [1.0] * 4) == pytest.approx(0.5)

    def test_partial_overlap_between_zero_and_one(self):
        """Periods that shift but overlap → strictly between 0 and 1."""
        result = _generalized_drift([{"a": 0.7, "b": 0.3}, {"a": 0.3, "b": 0.7}], [1.0, 1.0])
        assert 0.0 < result < 1.0

    def test_size_weights_downweight_a_small_divergent_period(self):
        """A divergent period that is tiny by row count drifts the column less."""
        dists = [{"a": 1.0}, {"a": 1.0}, {"b": 1.0}]
        equal = _generalized_drift(dists, [1.0, 1.0, 1.0])
        skewed = _generalized_drift(dists, [10.0, 10.0, 1.0])
        assert skewed < equal

    def test_clamped_to_unit_interval(self):
        """All results stay within [0, 1]."""
        for weights in ([1.0, 1.0], [100.0, 1.0], [1.0, 1.0, 1.0, 1.0]):
            dists = [{"a": 1.0}, {"b": 1.0}] * (len(weights) // 2)
            assert 0.0 <= _generalized_drift(dists, weights) <= 1.0


class TestBinCase:
    """Tests for _bin_case — the SQL CASE that maps a value to a bin index."""

    def test_edges_produce_sequential_bins(self):
        case = _bin_case("v", [10.0, 20.0, 30.0])
        assert case == "CASE WHEN v < 10 THEN 0 WHEN v < 20 THEN 1 WHEN v < 30 THEN 2 ELSE 3 END"

    def test_no_edges_is_single_bin(self):
        assert _bin_case("v", []) == "CASE  ELSE 0 END"


class TestNumericBinning:
    """In-memory DuckDB tests for the numeric drift binning SQL (DAT-442).

    Exercises the real ``quantile_cont`` / CASE-bin SQL (DuckDB is embedded, no
    docker) end-to-end into ``_generalized_drift``.
    """

    def _conn(self, rows: list[tuple[date, float]]) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE t (ts DATE, amount DOUBLE)")
        conn.executemany("INSERT INTO t VALUES (?, ?)", rows)
        return conn

    def _shift_rows(self) -> list[tuple[date, float]]:
        # Jan amounts 10–19, Feb amounts 100–190 (a disjoint 10× level shift).
        rows: list[tuple[date, float]] = []
        for i in range(50):
            rows.append((date(2024, 1, 15), 10.0 + (i % 10)))
            rows.append((date(2024, 2, 15), 100.0 + (i % 10) * 10))
        return rows

    def test_pooled_edges_sorted_and_unique(self):
        edges = _pooled_quantile_edges(self._conn(self._shift_rows()), "t", "amount")
        assert edges == sorted(edges)
        assert len(set(edges)) == len(edges)
        assert len(edges) >= 1

    def test_binned_distribution_sums_to_one(self):
        conn = self._conn(self._shift_rows())
        edges = _pooled_quantile_edges(conn, "t", "amount")
        res = _get_binned_distribution(
            conn, "t", "amount", "ts", date(2024, 1, 1), date(2024, 2, 1), edges
        )
        assert res is not None
        dist, n = res
        assert n == 50
        assert sum(dist.values()) == pytest.approx(1.0)

    def test_level_shift_drifts_high(self):
        conn = self._conn(self._shift_rows())
        edges = _pooled_quantile_edges(conn, "t", "amount")
        jan, n_jan = _get_binned_distribution(
            conn, "t", "amount", "ts", date(2024, 1, 1), date(2024, 2, 1), edges
        )
        feb, n_feb = _get_binned_distribution(
            conn, "t", "amount", "ts", date(2024, 2, 1), date(2024, 3, 1), edges
        )
        drift = _generalized_drift([jan, feb], [float(n_jan), float(n_feb)])
        assert drift > 0.5  # disjoint level shift → strong period disagreement

    def test_stationary_drifts_low(self):
        rows = [(date(2024, m, 15), 10.0 + (i % 10)) for m in (1, 2) for i in range(50)]
        conn = self._conn(rows)
        edges = _pooled_quantile_edges(conn, "t", "amount")
        jan, n_jan = _get_binned_distribution(
            conn, "t", "amount", "ts", date(2024, 1, 1), date(2024, 2, 1), edges
        )
        feb, n_feb = _get_binned_distribution(
            conn, "t", "amount", "ts", date(2024, 2, 1), date(2024, 3, 1), edges
        )
        drift = _generalized_drift([jan, feb], [float(n_jan), float(n_feb)])
        assert drift < 0.1  # same distribution each period → ~no drift


class TestGeneratePeriods:
    """Tests for _generate_periods."""

    def test_monthly_periods(self):
        """Monthly grain generates correct month boundaries."""
        config = TemporalSliceConfig(
            time_column="ts",
            period_start=date(2024, 1, 1),
            period_end=date(2024, 4, 1),
            time_grain=TimeGrain.MONTHLY,
        )
        periods = _generate_periods(config)
        assert len(periods) == 3
        assert periods[0] == (date(2024, 1, 1), date(2024, 2, 1), "2024-01")
        assert periods[1] == (
            date(2024, 1, 1) + (date(2024, 2, 1) - date(2024, 1, 1)),
            date(2024, 3, 1),
            "2024-02",
        )
        assert periods[2][2] == "2024-03"

    def test_daily_periods(self):
        """Daily grain generates one period per day."""
        config = TemporalSliceConfig(
            time_column="ts",
            period_start=date(2024, 1, 1),
            period_end=date(2024, 1, 4),
            time_grain=TimeGrain.DAILY,
        )
        periods = _generate_periods(config)
        assert len(periods) == 3
        assert periods[0][2] == "2024-01-01"
        assert periods[1][2] == "2024-01-02"
        assert periods[2][2] == "2024-01-03"

    def test_weekly_periods(self):
        """Weekly grain generates 7-day periods."""
        config = TemporalSliceConfig(
            time_column="ts",
            period_start=date(2024, 1, 1),
            period_end=date(2024, 1, 22),
            time_grain=TimeGrain.WEEKLY,
        )
        periods = _generate_periods(config)
        assert len(periods) == 3

    def test_single_period(self):
        """Returns one period when range is exactly one grain."""
        config = TemporalSliceConfig(
            time_column="ts",
            period_start=date(2024, 1, 1),
            period_end=date(2024, 2, 1),
            time_grain=TimeGrain.MONTHLY,
        )
        periods = _generate_periods(config)
        assert len(periods) == 1

    def test_empty_range(self):
        """Returns empty list when start == end."""
        config = TemporalSliceConfig(
            time_column="ts",
            period_start=date(2024, 1, 1),
            period_end=date(2024, 1, 1),
            time_grain=TimeGrain.MONTHLY,
        )
        periods = _generate_periods(config)
        assert len(periods) == 0

    def test_year_boundary(self):
        """Monthly periods cross year boundary correctly."""
        config = TemporalSliceConfig(
            time_column="ts",
            period_start=date(2024, 11, 1),
            period_end=date(2025, 2, 1),
            time_grain=TimeGrain.MONTHLY,
        )
        periods = _generate_periods(config)
        assert len(periods) == 3
        assert periods[0][2] == "2024-11"
        assert periods[1][2] == "2024-12"
        assert periods[2][2] == "2025-01"


class TestBuildDriftEvidence:
    """Tests for _build_drift_evidence."""

    def test_empty_per_period(self):
        """Returns None for empty per_period list."""
        result = _build_drift_evidence([], {"A": 0.5}, 0.1)
        assert result is None

    def test_worst_period_identified(self):
        """Identifies the period with highest JS divergence."""
        per_period = [
            ("2024-02", 0.05, {"A": 0.6}, {"A": 0.55}),
            ("2024-03", 0.3, {"A": 0.55}, {"A": 0.3, "B": 0.7}),
            ("2024-04", 0.1, {"A": 0.3, "B": 0.7}, {"A": 0.35, "B": 0.65}),
        ]
        result = _build_drift_evidence(per_period, {"A": 0.6}, 0.1)
        assert result is not None
        assert result.worst_period == "2024-03"
        assert result.worst_js == pytest.approx(0.3, abs=0.01)

    def test_top_shifts_above_5pp(self):
        """Only shifts > 5 percentage points are included."""
        per_period = [
            ("2024-02", 0.2, {"A": 0.6, "B": 0.4}, {"A": 0.3, "B": 0.5, "C": 0.2}),
        ]
        result = _build_drift_evidence(per_period, {"A": 0.6, "B": 0.4}, 0.1)
        assert result is not None
        # A shifted 60%→30% = 30pp, B shifted 40%→50% = 10pp, C emerged 0%→20% = 20pp
        categories = {s.category for s in result.top_shifts}
        assert "A" in categories  # 30pp shift
        assert "C" in categories  # 20pp shift
        assert "B" in categories  # 10pp shift

    def test_top_shifts_capped_at_10(self):
        """Top shifts are capped at 10 entries."""
        # Create a period with many shifting categories
        prev = {f"cat_{i}": 1.0 / 20 for i in range(20)}
        curr = {f"cat_{i}": (1.0 / 20 + 0.04 * (1 if i % 2 == 0 else -1)) for i in range(20)}
        per_period = [("2024-02", 0.3, prev, curr)]
        result = _build_drift_evidence(per_period, prev, 0.1)
        assert result is not None
        assert len(result.top_shifts) <= 10

    def test_emerged_categories(self):
        """Detects categories that appeared after baseline."""
        baseline = {"A": 0.5, "B": 0.5}
        per_period = [
            ("2024-02", 0.2, {"A": 0.5, "B": 0.5}, {"A": 0.4, "B": 0.4, "C": 0.2}),
        ]
        result = _build_drift_evidence(per_period, baseline, 0.1)
        assert result is not None
        emerged_cats = {e.category for e in result.emerged_categories}
        assert "C" in emerged_cats

    def test_vanished_categories(self):
        """Detects categories that disappeared from baseline."""
        baseline = {"A": 0.5, "B": 0.3, "C": 0.2}
        per_period = [
            ("2024-02", 0.2, {"A": 0.5, "B": 0.3, "C": 0.2}, {"A": 0.7, "B": 0.3}),
        ]
        result = _build_drift_evidence(per_period, baseline, 0.1)
        assert result is not None
        vanished_cats = {v.category for v in result.vanished_categories}
        assert "C" in vanished_cats

    def test_change_points_detect_jumps(self):
        """Change points detected when JS divergence jumps."""
        per_period = [
            ("2024-02", 0.02, {"A": 0.5}, {"A": 0.48}),
            ("2024-03", 0.02, {"A": 0.48}, {"A": 0.46}),
            ("2024-04", 0.3, {"A": 0.46}, {"A": 0.2, "B": 0.8}),  # jump
            ("2024-05", 0.02, {"A": 0.2, "B": 0.8}, {"A": 0.19, "B": 0.81}),
        ]
        result = _build_drift_evidence(per_period, {"A": 0.5}, 0.1)
        assert result is not None
        assert "2024-04" in result.change_points

    def test_no_change_points_for_gradual_drift(self):
        """No change points when drift is gradual (no sudden jumps)."""
        per_period = [
            ("2024-02", 0.15, {"A": 0.5}, {"A": 0.4, "B": 0.6}),
            ("2024-03", 0.16, {"A": 0.4, "B": 0.6}, {"A": 0.35, "B": 0.65}),
            ("2024-04", 0.15, {"A": 0.35, "B": 0.65}, {"A": 0.3, "B": 0.7}),
        ]
        result = _build_drift_evidence(per_period, {"A": 0.5}, 0.1)
        assert result is not None
        # Gradual increase — differences between consecutive JS values are small
        assert len(result.change_points) == 0

    def test_shifts_below_threshold_excluded(self):
        """Periods with JS below threshold don't contribute to top_shifts."""
        per_period = [
            ("2024-02", 0.05, {"A": 0.5, "B": 0.5}, {"A": 0.3, "B": 0.7}),  # below threshold
            ("2024-03", 0.2, {"A": 0.3, "B": 0.7}, {"A": 0.6, "B": 0.4}),  # above threshold
        ]
        result = _build_drift_evidence(per_period, {"A": 0.5, "B": 0.5}, 0.1)
        assert result is not None
        # Only 2024-03 shifts should appear (2024-02 is below threshold)
        for shift in result.top_shifts:
            assert shift.period == "2024-03"

    def test_emerged_deduplicated(self):
        """Emerged categories are deduplicated (first appearance kept)."""
        baseline = {"A": 1.0}
        per_period = [
            ("2024-02", 0.2, {"A": 0.8}, {"A": 0.6, "B": 0.4}),
            ("2024-03", 0.2, {"A": 0.6, "B": 0.4}, {"A": 0.5, "B": 0.3, "C": 0.2}),
            ("2024-04", 0.2, {"A": 0.5}, {"A": 0.4, "B": 0.3, "C": 0.3}),
        ]
        result = _build_drift_evidence(per_period, baseline, 0.1)
        assert result is not None
        b_entries = [e for e in result.emerged_categories if e.category == "B"]
        assert len(b_entries) == 1
        assert b_entries[0].period == "2024-02"
