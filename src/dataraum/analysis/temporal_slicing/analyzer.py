"""Temporal slice analyzer.

Analyzes slices for temporal completeness, distribution drift, and volume anomalies.
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from uuid import uuid4

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.temporal_slicing.db_models import (
    SliceTimeMatrixEntry,
    TemporalDriftAnalysis,
    TemporalSliceAnalysis,
    TemporalSliceRun,
)
from dataraum.analysis.temporal_slicing.models import (
    AggregatedTemporalData,
    CompletenessResult,
    DistributionDriftResult,
    PeriodMetrics,
    PeriodTopology,
    SliceTimeCell,
    SliceTimeMatrix,
    TemporalAnalysisResult,
    TemporalSliceConfig,
    TemporalTopologyResult,
    TimeGrain,
    TopologyDrift,
    VolumeAnomalyResult,
)
from dataraum.core.models.base import Result
from dataraum.storage import Column, Table


@dataclass
class TemporalSliceContext:
    """Context for temporal slice analysis."""

    slice_table_name: str
    slice_table_id: str
    slice_column_name: str
    time_column: str
    config: TemporalSliceConfig
    duckdb_conn: duckdb.DuckDBPyConnection
    session: Session


class TemporalSliceAnalyzer:
    """Analyzer for temporal slice data.

    Implements 4-level temporal analysis:
    1. Period Completeness - coverage ratio, cutoff detection
    2. Distribution Drift - JS divergence, chi-square for categorical columns
    3. Cross-Slice Temporal Comparison - slice × time matrix
    4. Volume Anomaly Detection - z-scores, period-over-period changes
    """

    def __init__(
        self,
        duckdb_conn: duckdb.DuckDBPyConnection,
        session: Session,
    ):
        self.duckdb_conn = duckdb_conn
        self.session = session

    def analyze(
        self,
        slice_table_name: str,
        config: TemporalSliceConfig,
        slice_column_name: str | None = None,
    ) -> Result[TemporalAnalysisResult]:
        """Run full temporal analysis on a slice table.

        Args:
            slice_table_name: Name of the slice table in DuckDB
            config: Temporal analysis configuration
            slice_column_name: Optional column name used for slicing (for Level 3)

        Returns:
            Result containing TemporalAnalysisResult
        """
        try:
            # Get table info
            table = self._get_table(slice_table_name)
            if not table:
                return Result.fail(f"Table not found: {slice_table_name}")

            # Generate period boundaries
            periods = self._generate_periods(config)

            # Level 1: Compute period metrics
            period_metrics = self._compute_period_metrics(
                slice_table_name, config.time_column, periods, config
            )

            # Level 1: Check completeness
            completeness_results = self._analyze_completeness(period_metrics, config)

            # Level 2: Analyze distribution drift for categorical columns
            categorical_columns = self._get_categorical_columns(table.table_id)
            drift_results = self._analyze_distribution_drift(
                slice_table_name, config.time_column, periods, categorical_columns, config
            )

            # Level 3: Build slice × time matrix (if we know the slice column)
            slice_time_matrix = None
            if slice_column_name:
                slice_time_matrix = self._build_slice_time_matrix(
                    slice_table_name, config.time_column, slice_column_name, periods, config
                )

            # Level 4: Detect volume anomalies
            volume_anomalies = self._detect_volume_anomalies(period_metrics, config)

            # Generate investigation queries
            investigation_queries = self._generate_investigation_queries(
                slice_table_name,
                config.time_column,
                completeness_results,
                volume_anomalies,
            )

            result = TemporalAnalysisResult(
                config=config,
                slice_table_name=slice_table_name,
                time_column=config.time_column,
                period_metrics=period_metrics,
                completeness_results=completeness_results,
                drift_results=drift_results,
                slice_time_matrix=slice_time_matrix,
                volume_anomalies=volume_anomalies,
                total_periods=len(periods),
                incomplete_periods=sum(1 for c in completeness_results if not c.is_complete),
                anomaly_count=sum(1 for v in volume_anomalies if v.is_anomaly),
                drift_detected=any(d.has_significant_drift for d in drift_results),
                investigation_queries=investigation_queries,
            )

            return Result.ok(result)

        except Exception as e:
            return Result.fail(f"Temporal analysis failed: {e}")

    def persist_results(
        self,
        result: TemporalAnalysisResult,
    ) -> Result[str]:
        """Persist temporal analysis results to database.

        Args:
            result: Analysis result to persist

        Returns:
            Result containing the run_id
        """
        try:
            # Generate run_id upfront — SQLAlchemy column defaults only evaluate at
            # flush/INSERT time, so reading run.run_id before flush would return None.
            run_id = str(uuid4())

            # Create run record
            run = TemporalSliceRun(
                run_id=run_id,
                slice_table_name=result.slice_table_name,
                time_column=result.time_column,
                period_start=result.config.period_start,
                period_end=result.config.period_end,
                time_grain=result.config.time_grain.value,
                total_periods=result.total_periods,
                incomplete_periods=result.incomplete_periods,
                anomaly_count=result.anomaly_count,
                drift_detected=result.drift_detected,
                config_json=result.config.model_dump(mode="json"),
            )

            # Persist period analyses - use relationship for proper FK ordering
            for period_metric, completeness in zip(
                result.period_metrics, result.completeness_results, strict=False
            ):
                # Find matching volume anomaly
                volume_anomaly = next(
                    (
                        v
                        for v in result.volume_anomalies
                        if v.period_label == period_metric.period_label
                    ),
                    None,
                )

                analysis = TemporalSliceAnalysis(
                    slice_table_name=result.slice_table_name,
                    time_column=result.time_column,
                    period_label=period_metric.period_label,
                    period_start=period_metric.period_start,
                    period_end=period_metric.period_end,
                    row_count=period_metric.row_count,
                    expected_days=period_metric.expected_days,
                    observed_days=period_metric.observed_days,
                    coverage_ratio=period_metric.coverage_ratio,
                    is_complete=completeness.is_complete,
                    has_early_cutoff=completeness.has_early_cutoff,
                    days_missing_at_end=completeness.days_missing_at_end,
                    last_day_ratio=completeness.last_day_ratio,
                    z_score=volume_anomaly.z_score if volume_anomaly else None,
                    rolling_avg=volume_anomaly.rolling_avg if volume_anomaly else None,
                    rolling_std=volume_anomaly.rolling_std if volume_anomaly else None,
                    is_volume_anomaly=volume_anomaly.is_anomaly if volume_anomaly else False,
                    anomaly_type=volume_anomaly.anomaly_type if volume_anomaly else None,
                    period_over_period_change=volume_anomaly.period_over_period_change
                    if volume_anomaly
                    else None,
                    issues_json=completeness.issues
                    + (volume_anomaly.issues if volume_anomaly else []),
                )
                run.analyses.append(analysis)

            # Persist drift analyses - use relationship for proper FK ordering
            for drift in result.drift_results:
                drift_record = TemporalDriftAnalysis(
                    slice_table_name=result.slice_table_name,
                    column_name=drift.column_name,
                    period_label=drift.period_label,
                    js_divergence=drift.jensen_shannon_divergence,
                    chi_square_statistic=drift.chi_square_statistic,
                    chi_square_p_value=drift.chi_square_p_value,
                    new_categories_json=drift.new_categories,
                    missing_categories_json=drift.missing_categories,
                    has_significant_drift=drift.has_significant_drift,
                    has_category_changes=drift.has_category_changes,
                )
                run.drift_analyses.append(drift_record)

            # Persist slice-time matrix - use relationship for proper FK ordering
            if result.slice_time_matrix:
                for _slice_value, periods in result.slice_time_matrix.data.items():
                    for _period_label, cell in periods.items():
                        entry = SliceTimeMatrixEntry(
                            slice_table_name=result.slice_table_name,
                            slice_column=result.slice_time_matrix.slice_column,
                            slice_value=cell.slice_value,
                            period_label=cell.period_label,
                            row_count=cell.row_count,
                            period_over_period_change=cell.period_over_period_change,
                        )
                        run.matrix_entries.append(entry)

            # Add the run with all children - SQLAlchemy handles FK ordering
            self.session.add(run)

            # Note: commit handled by session_scope() in caller
            return Result.ok(run_id)

        except Exception as e:
            return Result.fail(f"Failed to persist results: {e}")

    def _get_table(self, table_name: str) -> Table | None:
        """Get table by DuckDB path."""
        stmt = select(Table).where(Table.duckdb_path == table_name)
        result = self.session.execute(stmt)
        return result.scalar_one_or_none()

    def _get_categorical_columns(self, table_id: str) -> list[Column]:
        """Get categorical columns from table (VARCHAR, low cardinality)."""
        stmt = select(Column).where(
            Column.table_id == table_id,
            Column.resolved_type.in_(["VARCHAR", "TEXT", "STRING"]),
        )
        result = self.session.execute(stmt)
        return list(result.scalars().all())

    def _generate_periods(self, config: TemporalSliceConfig) -> list[tuple[date, date, str]]:
        """Generate period boundaries based on time grain.

        Returns:
            List of (start_date, end_date, period_label) tuples
        """
        periods = []
        current = config.period_start

        while current < config.period_end:
            if config.time_grain == TimeGrain.DAILY:
                next_date = current + timedelta(days=1)
                label = current.isoformat()
            elif config.time_grain == TimeGrain.WEEKLY:
                # Start from Monday
                next_date = current + timedelta(days=7)
                iso_year, iso_week, _ = current.isocalendar()
                label = f"{iso_year}-W{iso_week:02d}"
            else:  # MONTHLY
                if current.month == 12:
                    next_date = date(current.year + 1, 1, 1)
                else:
                    next_date = date(current.year, current.month + 1, 1)
                label = f"{current.year}-{current.month:02d}"

            # Clamp to period_end
            end = min(next_date, config.period_end)
            periods.append((current, end, label))
            current = next_date

        return periods

    def _compute_period_metrics(
        self,
        table_name: str,
        time_column: str,
        periods: list[tuple[date, date, str]],
        config: TemporalSliceConfig,
    ) -> list[PeriodMetrics]:
        """Compute metrics for each period."""
        metrics = []

        for start, end, label in periods:
            # Count rows and distinct days in period
            sql = f"""
                SELECT
                    COUNT(*) as row_count,
                    COUNT(DISTINCT CAST("{time_column}" AS DATE)) as observed_days,
                    MAX(CAST("{time_column}" AS DATE)) as max_date
                FROM "{table_name}"
                WHERE CAST("{time_column}" AS DATE) >= ?
                  AND CAST("{time_column}" AS DATE) < ?
            """
            result = self.duckdb_conn.execute(sql, [start, end]).fetchone()
            row_count = result[0] if result else 0
            observed_days = result[1] if result else 0
            max_date = result[2] if result else None

            expected_days = (end - start).days

            # Calculate coverage
            coverage_ratio = observed_days / expected_days if expected_days > 0 else 0

            # Get last day volume for cutoff detection
            last_day_volume = None
            avg_day_volume = None
            last_day_ratio = None
            days_until_end = None

            if max_date and row_count > 0:
                # Get volume per day
                day_sql = f"""
                    SELECT
                        CAST("{time_column}" AS DATE) as day,
                        COUNT(*) as count
                    FROM "{table_name}"
                    WHERE CAST("{time_column}" AS DATE) >= ?
                      AND CAST("{time_column}" AS DATE) < ?
                    GROUP BY CAST("{time_column}" AS DATE)
                    ORDER BY day
                """
                day_results = self.duckdb_conn.execute(day_sql, [start, end]).fetchall()
                if day_results:
                    volumes = [r[1] for r in day_results]
                    avg_day_volume = sum(volumes) / len(volumes) if volumes else 0
                    last_day_volume = volumes[-1] if volumes else 0
                    last_day_ratio = last_day_volume / avg_day_volume if avg_day_volume > 0 else 0

                    # Check days until end of period
                    if isinstance(max_date, str):
                        max_date = date.fromisoformat(max_date)
                    days_until_end = (end - max_date).days - 1  # -1 because end is exclusive

            metrics.append(
                PeriodMetrics(
                    period_start=start,
                    period_end=end,
                    period_label=label,
                    row_count=row_count,
                    expected_days=expected_days,
                    observed_days=observed_days,
                    coverage_ratio=coverage_ratio,
                    last_day_volume=last_day_volume,
                    avg_day_volume=avg_day_volume,
                    last_day_ratio=last_day_ratio,
                    max_date_in_period=max_date if isinstance(max_date, date) else None,
                    days_until_end=days_until_end,
                )
            )

        # Compute rolling statistics for anomaly detection
        baseline_periods = config.get_baseline_periods()
        for i, m in enumerate(metrics):
            if i >= baseline_periods:
                window = metrics[max(0, i - baseline_periods) : i]
                volumes = [pm.row_count for pm in window]
                if volumes:
                    avg = sum(volumes) / len(volumes)
                    std = (
                        math.sqrt(sum((v - avg) ** 2 for v in volumes) / len(volumes))
                        if len(volumes) > 1
                        else 0
                    )
                    m.volume_rolling_avg = avg
                    m.volume_rolling_std = std
                    m.z_score = (m.row_count - avg) / std if std > 0 else 0

            # Period-over-period change
            if i > 0 and metrics[i - 1].row_count > 0:
                m.period_over_period_change = (m.row_count - metrics[i - 1].row_count) / metrics[
                    i - 1
                ].row_count

        return metrics

    def _analyze_completeness(
        self,
        period_metrics: list[PeriodMetrics],
        config: TemporalSliceConfig,
    ) -> list[CompletenessResult]:
        """Analyze period completeness."""
        results = []

        for m in period_metrics:
            issues = []
            is_complete = True
            has_early_cutoff = False
            has_volume_dropoff = False

            # Check coverage ratio
            if m.coverage_ratio < config.completeness_threshold:
                issues.append(
                    f"Low coverage: {m.coverage_ratio:.1%} (expected >= {config.completeness_threshold:.0%})"
                )
                is_complete = False

            # Check for early cutoff
            if m.days_until_end is not None and m.days_until_end > 0:
                has_early_cutoff = True
                issues.append(f"Data ends {m.days_until_end} days before period end")
                is_complete = False

            # Check for volume dropoff on last day
            if m.last_day_ratio is not None and m.last_day_ratio < config.last_day_ratio_threshold:
                has_volume_dropoff = True
                issues.append(f"Last day volume drop: {m.last_day_ratio:.1%} of average")

            results.append(
                CompletenessResult(
                    period_label=m.period_label,
                    coverage_ratio=m.coverage_ratio,
                    is_complete=is_complete,
                    days_missing_at_end=m.days_until_end or 0,
                    has_early_cutoff=has_early_cutoff,
                    has_volume_dropoff=has_volume_dropoff,
                    last_day_ratio=m.last_day_ratio,
                    issues=issues,
                )
            )

        return results

    def _analyze_distribution_drift(
        self,
        table_name: str,
        time_column: str,
        periods: list[tuple[date, date, str]],
        columns: list[Column],
        config: TemporalSliceConfig,
    ) -> list[DistributionDriftResult]:
        """Analyze distribution drift for categorical columns."""
        results = []

        for col in columns:
            col_name = col.column_name
            if col_name == time_column:
                continue

            previous_distribution: dict[str, float] | None = None
            previous_label: str | None = None

            for start, end, label in periods:
                # Get value distribution for this period
                sql = f"""
                    SELECT
                        "{col_name}" as value,
                        COUNT(*) as count
                    FROM "{table_name}"
                    WHERE CAST("{time_column}" AS DATE) >= ?
                      AND CAST("{time_column}" AS DATE) < ?
                    GROUP BY "{col_name}"
                """
                period_results = self.duckdb_conn.execute(sql, [start, end]).fetchall()

                if not period_results:
                    continue

                # Build distribution
                total = sum(r[1] for r in period_results)
                current_distribution = {
                    str(r[0]) if r[0] is not None else "_NULL_": r[1] / total
                    for r in period_results
                }

                # Compare with previous period
                drift_result = DistributionDriftResult(
                    column_name=col_name,
                    period_label=label,
                    previous_period_label=previous_label,
                )

                if previous_distribution is not None:
                    # Jensen-Shannon divergence
                    js_div = self._jensen_shannon_divergence(
                        previous_distribution, current_distribution
                    )
                    drift_result.jensen_shannon_divergence = js_div

                    if js_div > config.drift_threshold:
                        drift_result.has_significant_drift = True
                        drift_result.issues.append(
                            f"Distribution drift: JS={js_div:.4f} (threshold: {config.drift_threshold})"
                        )

                    # Check for category changes
                    prev_cats = set(previous_distribution.keys())
                    curr_cats = set(current_distribution.keys())

                    new_cats = curr_cats - prev_cats
                    missing_cats = prev_cats - curr_cats

                    if new_cats or missing_cats:
                        drift_result.has_category_changes = True
                        drift_result.new_categories = list(new_cats)[:10]
                        drift_result.missing_categories = list(missing_cats)[:10]
                        if new_cats:
                            drift_result.issues.append(f"New categories: {len(new_cats)}")
                        if missing_cats:
                            drift_result.issues.append(f"Missing categories: {len(missing_cats)}")

                results.append(drift_result)

                previous_distribution = current_distribution
                previous_label = label

        return results

    def _jensen_shannon_divergence(
        self,
        p: dict[str, float],
        q: dict[str, float],
    ) -> float:
        """Compute Jensen-Shannon divergence between two distributions."""
        # Get all keys
        all_keys = set(p.keys()) | set(q.keys())

        # Convert to probability vectors
        p_vec = [p.get(k, 0.0) for k in all_keys]
        q_vec = [q.get(k, 0.0) for k in all_keys]

        # Compute midpoint distribution
        m_vec = [(pi + qi) / 2 for pi, qi in zip(p_vec, q_vec, strict=True)]

        # KL divergences
        def kl_divergence(a: list[float], b: list[float]) -> float:
            result = 0.0
            for ai, bi in zip(a, b, strict=True):
                if ai > 0 and bi > 0:
                    result += ai * math.log(ai / bi)
            return result

        kl_pm = kl_divergence(p_vec, m_vec)
        kl_qm = kl_divergence(q_vec, m_vec)

        return (kl_pm + kl_qm) / 2

    def _build_slice_time_matrix(
        self,
        table_name: str,
        time_column: str,
        slice_column: str,
        periods: list[tuple[date, date, str]],
        config: TemporalSliceConfig,
    ) -> SliceTimeMatrix:
        """Build cross-slice temporal comparison matrix."""
        # Get distinct slice values
        slice_sql = f'SELECT DISTINCT "{slice_column}" FROM "{table_name}" ORDER BY 1'
        slice_values = [str(r[0]) for r in self.duckdb_conn.execute(slice_sql).fetchall()]

        data: dict[str, dict[str, SliceTimeCell]] = defaultdict(dict)
        period_totals: dict[str, int] = {}

        for start, end, label in periods:
            # Get counts per slice value for this period
            sql = f"""
                SELECT
                    "{slice_column}" as slice_value,
                    COUNT(*) as count
                FROM "{table_name}"
                WHERE CAST("{time_column}" AS DATE) >= ?
                  AND CAST("{time_column}" AS DATE) < ?
                GROUP BY "{slice_column}"
            """
            results = self.duckdb_conn.execute(sql, [start, end]).fetchall()

            period_total = 0
            for r in results:
                slice_val = str(r[0]) if r[0] is not None else "_NULL_"
                count = r[1]
                period_total += count

                data[slice_val][label] = SliceTimeCell(
                    slice_value=slice_val,
                    period_label=label,
                    row_count=count,
                )

            period_totals[label] = period_total

        # Compute period-over-period changes per slice
        period_labels = [p[2] for p in periods]
        for _slice_val, periods_data in data.items():
            for i, label in enumerate(period_labels):
                if i > 0 and label in periods_data:
                    prev_label = period_labels[i - 1]
                    if prev_label in periods_data:
                        prev_count = periods_data[prev_label].row_count
                        if prev_count > 0:
                            curr_count = periods_data[label].row_count
                            periods_data[label].period_over_period_change = (
                                curr_count - prev_count
                            ) / prev_count

        # Compute slice trends (overall growth/decline)
        slice_trends = {}
        for slice_val in slice_values:
            if slice_val in data:
                first_period = next(iter(data[slice_val].values()), None)
                last_period = list(data[slice_val].values())[-1] if data[slice_val] else None
                if first_period and last_period and first_period.row_count > 0:
                    slice_trends[slice_val] = (
                        last_period.row_count - first_period.row_count
                    ) / first_period.row_count
                else:
                    slice_trends[slice_val] = 0.0

        # Detect hidden trends
        hidden_trends = []
        compensating_slices = []

        # Check if global totals are stable but individual slices have offsetting trends
        total_volumes = [period_totals.get(p[2], 0) for p in periods]
        if len(total_volumes) > 1:
            first_total = total_volumes[0]
            last_total = total_volumes[-1]
            global_change = (last_total - first_total) / first_total if first_total > 0 else 0

            # If global is stable (<10% change) but slices have big changes
            if abs(global_change) < 0.1:
                growing = [(sv, t) for sv, t in slice_trends.items() if t > 0.2]
                declining = [(sv, t) for sv, t in slice_trends.items() if t < -0.2]

                if growing and declining:
                    hidden_trends.append(
                        f"Global volume stable ({global_change:+.1%}) but {len(growing)} slices growing, "
                        f"{len(declining)} declining"
                    )
                    # Find compensating pairs
                    for g_sv, _g_trend in growing[:3]:
                        for d_sv, _d_trend in declining[:3]:
                            compensating_slices.append((g_sv, d_sv))

        return SliceTimeMatrix(
            slice_column=slice_column,
            periods=period_labels,
            slices=slice_values,
            data=dict(data),
            slice_trends=slice_trends,
            period_totals=period_totals,
            hidden_trends=hidden_trends,
            compensating_slices=compensating_slices[:5],  # Top 5 pairs
        )

    def _detect_volume_anomalies(
        self,
        period_metrics: list[PeriodMetrics],
        config: TemporalSliceConfig,
    ) -> list[VolumeAnomalyResult]:
        """Detect volume anomalies using z-scores."""
        results = []

        for m in period_metrics:
            is_anomaly = False
            anomaly_type = None
            issues = []

            # Check z-score
            if m.z_score is not None and abs(m.z_score) > config.volume_zscore_threshold:
                is_anomaly = True
                if m.z_score > 0:
                    anomaly_type = "spike"
                    issues.append(
                        f"Volume spike: z-score={m.z_score:.2f} (threshold: ±{config.volume_zscore_threshold})"
                    )
                else:
                    anomaly_type = "drop"
                    issues.append(
                        f"Volume drop: z-score={m.z_score:.2f} (threshold: ±{config.volume_zscore_threshold})"
                    )

            # Check for gaps (zero volume)
            if m.row_count == 0:
                is_anomaly = True
                anomaly_type = "gap"
                issues.append("No data in period")

            results.append(
                VolumeAnomalyResult(
                    period_label=m.period_label,
                    volume=m.row_count,
                    rolling_avg=m.volume_rolling_avg or 0,
                    rolling_std=m.volume_rolling_std or 0,
                    z_score=m.z_score or 0,
                    is_anomaly=is_anomaly,
                    anomaly_type=anomaly_type,
                    period_over_period_change=m.period_over_period_change,
                    issues=issues,
                )
            )

        return results

    def _generate_investigation_queries(
        self,
        table_name: str,
        time_column: str,
        completeness_results: list[CompletenessResult],
        volume_anomalies: list[VolumeAnomalyResult],
    ) -> list[dict[str, str]]:
        """Generate SQL queries for investigating issues."""
        queries = []

        # Query for incomplete periods
        incomplete = [c for c in completeness_results if not c.is_complete]
        if incomplete:
            period = incomplete[0]
            queries.append(
                {
                    "description": f"Investigate incomplete period: {period.period_label}",
                    "sql": f"""
SELECT
    CAST("{time_column}" AS DATE) as day,
    COUNT(*) as row_count
FROM "{table_name}"
WHERE CAST("{time_column}" AS DATE) >= '{period.period_label}'  -- Adjust based on grain
GROUP BY 1
ORDER BY 1
""".strip(),
                }
            )

        # Query for anomalies
        anomalies = [v for v in volume_anomalies if v.is_anomaly]
        if anomalies:
            anomaly = anomalies[0]
            queries.append(
                {
                    "description": f"Investigate {anomaly.anomaly_type} anomaly: {anomaly.period_label}",
                    "sql": f"""
SELECT
    CAST("{time_column}" AS DATE) as day,
    COUNT(*) as row_count
FROM "{table_name}"
WHERE CAST("{time_column}" AS DATE) >= '{anomaly.period_label}'  -- Adjust based on grain
GROUP BY 1
ORDER BY 1
""".strip(),
                }
            )

        return queries


def analyze_temporal_slices(
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
    slice_table_name: str,
    config: TemporalSliceConfig,
    slice_column_name: str | None = None,
    persist: bool = True,
) -> Result[TemporalAnalysisResult]:
    """Analyze temporal patterns in a slice table.

    This is the main entry point for temporal slice analysis.

    Args:
        duckdb_conn: DuckDB connection
        session: Database session
        slice_table_name: Name of the slice table
        config: Temporal analysis configuration
        slice_column_name: Optional slice column for cross-slice analysis
        persist: Whether to persist results to database

    Returns:
        Result containing TemporalAnalysisResult
    """
    analyzer = TemporalSliceAnalyzer(duckdb_conn, session)
    result = analyzer.analyze(slice_table_name, config, slice_column_name)

    if result.success and result.value is not None and persist:
        persist_result = analyzer.persist_results(result.value)
        if not persist_result.success:
            return Result.fail(persist_result.error or "Failed to persist results")

    return result


def aggregate_temporal_data(
    result: TemporalAnalysisResult,
    slice_column_name: str,
) -> AggregatedTemporalData:
    """Aggregate temporal analysis results for quality summary.

    Args:
        result: Temporal analysis result
        slice_column_name: Name of slice column

    Returns:
        AggregatedTemporalData for use in quality summary
    """
    # Completeness summary
    incomplete_periods = [c for c in result.completeness_results if not c.is_complete]
    early_cutoffs = [c for c in result.completeness_results if c.has_early_cutoff]
    coverages = [c.coverage_ratio for c in result.completeness_results]

    # Drift summary
    drifts = [d for d in result.drift_results if d.has_significant_drift]
    js_values = [
        d.jensen_shannon_divergence
        for d in result.drift_results
        if d.jensen_shannon_divergence is not None
    ]
    category_change_periods = list(
        {d.period_label for d in result.drift_results if d.has_category_changes}
    )

    # Volume summary
    anomalies = [v for v in result.volume_anomalies if v.is_anomaly]
    gaps = [v for v in result.volume_anomalies if v.anomaly_type == "gap"]
    z_scores = [v.z_score for v in result.volume_anomalies if v.z_score != 0]

    # Slice comparison
    declining = []
    growing = []
    if result.slice_time_matrix:
        declining = [s for s, t in result.slice_time_matrix.slice_trends.items() if t < -0.2]
        growing = [s for s, t in result.slice_time_matrix.slice_trends.items() if t > 0.2]

    return AggregatedTemporalData(
        slice_column_name=slice_column_name,
        time_column=result.time_column,
        total_periods=result.total_periods,
        incomplete_period_count=len(incomplete_periods),
        avg_coverage_ratio=sum(coverages) / len(coverages) if coverages else 0,
        early_cutoff_count=len(early_cutoffs),
        drift_detected_count=len(drifts),
        max_js_divergence=max(js_values) if js_values else None,
        category_change_periods=category_change_periods,
        volume_anomaly_count=len(anomalies),
        max_zscore=max(abs(z) for z in z_scores) if z_scores else None,
        gap_periods=[v.period_label for v in gaps],
        declining_slices=declining[:5],
        growing_slices=growing[:5],
        hidden_trend_insights=result.slice_time_matrix.hidden_trends
        if result.slice_time_matrix
        else [],
    )


# =============================================================================
# LEVEL 5: TEMPORAL TOPOLOGY ANALYSIS (TDA with Bottleneck Distance)
# =============================================================================


def analyze_temporal_topology(
    duck_conn: duckdb.DuckDBPyConnection,
    table_name: str,
    time_column: str,
    numeric_columns: list[str] | None = None,
    period: str = "month",
    min_samples: int = 10,
    bottleneck_threshold: float = 0.5,
) -> TemporalTopologyResult:
    """Analyze how data topology changes over time periods using TDA.

    Uses actual Topological Data Analysis (via ripser) to compute persistence
    diagrams for each period, then uses bottleneck distance to detect structural
    drift between consecutive periods.

    Args:
        duck_conn: DuckDB connection
        table_name: Table to analyze
        time_column: Temporal column for period grouping
        numeric_columns: Columns to include (auto-detected if None)
        period: Time granularity ('day', 'week', 'month', 'quarter', 'year')
        min_samples: Min rows per period for valid analysis
        bottleneck_threshold: Distance threshold for significant drift

    Returns:
        TemporalTopologyResult with per-period topology and drift detection
    """
    import numpy as np

    from dataraum.analysis.topology import compute_bottleneck_distance
    from dataraum.analysis.topology.extraction import compute_persistent_entropy
    from dataraum.analysis.topology.tda.extractor import TableTopologyExtractor

    result = TemporalTopologyResult(
        table_name=table_name,
        time_column=time_column,
    )

    # Get time period boundaries
    period_trunc = {
        "day": "DAY",
        "week": "WEEK",
        "month": "MONTH",
        "quarter": "QUARTER",
        "year": "YEAR",
    }.get(period, "MONTH")

    try:
        periods_df = duck_conn.execute(f"""
            SELECT
                DATE_TRUNC('{period_trunc}', "{time_column}") as period_start,
                DATE_TRUNC('{period_trunc}', "{time_column}") + INTERVAL '1 {period}' as period_end,
                COUNT(*) as row_count
            FROM "{table_name}"
            WHERE "{time_column}" IS NOT NULL
            GROUP BY 1, 2
            HAVING COUNT(*) >= {min_samples}
            ORDER BY 1
        """).fetchdf()
    except Exception:
        return result

    if periods_df.empty:
        return result

    # Initialize TDA extractor
    extractor = TableTopologyExtractor()
    period_topologies: list[PeriodTopology] = []
    period_diagrams: list[list[np.ndarray]] = []  # Store diagrams for bottleneck comparison

    for _, row in periods_df.iterrows():
        period_start = str(row["period_start"])[:10]
        period_end = str(row["period_end"])[:10]
        row_count = int(row["row_count"])

        try:
            # Load period data
            period_df = duck_conn.execute(f"""
                SELECT *
                FROM "{table_name}"
                WHERE "{time_column}" >= '{period_start}'
                  AND "{time_column}" < '{period_end}'
                LIMIT 5000
            """).df()

            if period_df.empty:
                continue

            # Run TDA extraction
            topology = extractor.extract_topology(period_df)
            persistence = topology.get("global_persistence", {})
            diagrams = persistence.get("diagrams", [])

            if not diagrams:
                continue

            # Convert diagrams to numpy arrays
            np_diagrams = [
                np.array(dgm) if not isinstance(dgm, np.ndarray) else dgm for dgm in diagrams
            ]

            # Extract Betti numbers from persistence diagrams
            betti_0 = 1  # At least one component
            betti_1 = 0
            betti_2 = 0

            if len(np_diagrams) > 0 and len(np_diagrams[0]) > 0:
                dgm_0 = np_diagrams[0]
                finite_mask = dgm_0[:, 1] < np.inf
                betti_0 = int(np.sum(finite_mask)) + 1

            if len(np_diagrams) > 1 and len(np_diagrams[1]) > 0:
                dgm_1 = np_diagrams[1]
                finite_mask = dgm_1[:, 1] < np.inf
                betti_1 = int(np.sum(finite_mask))

            if len(np_diagrams) > 2 and len(np_diagrams[2]) > 0:
                dgm_2 = np_diagrams[2]
                finite_mask = dgm_2[:, 1] < np.inf
                betti_2 = int(np.sum(finite_mask))

            structural_complexity = betti_0 + betti_1 + betti_2
            persistent_entropy = compute_persistent_entropy(np_diagrams)

            period_topo = PeriodTopology(
                period_start=period_start,
                period_end=period_end,
                betti_0=betti_0,
                betti_1=betti_1,
                betti_2=betti_2,
                structural_complexity=structural_complexity,
                persistent_entropy=persistent_entropy,
                row_count=row_count,
            )
            period_topologies.append(period_topo)
            period_diagrams.append(np_diagrams)

        except Exception:
            # Skip periods that fail TDA extraction
            continue

    result.period_topologies = period_topologies
    result.periods_analyzed = len(period_topologies)

    if not period_topologies:
        return result

    # Compute drift between consecutive periods using bottleneck distance
    drifts: list[TopologyDrift] = []
    max_bottleneck = 0.0

    for i in range(1, len(period_topologies)):
        prev = period_topologies[i - 1]
        curr = period_topologies[i]
        prev_diagrams = period_diagrams[i - 1]
        curr_diagrams = period_diagrams[i]

        # Compute bottleneck distance for each dimension and take max
        bottleneck_dist = 0.0
        for dim in range(min(len(prev_diagrams), len(curr_diagrams))):
            if len(prev_diagrams[dim]) > 0 and len(curr_diagrams[dim]) > 0:
                dist = compute_bottleneck_distance(prev_diagrams[dim], curr_diagrams[dim])
                bottleneck_dist = max(bottleneck_dist, dist)

        max_bottleneck = max(max_bottleneck, bottleneck_dist)

        # Always record bottleneck distance as a drift metric
        if bottleneck_dist > bottleneck_threshold:
            drifts.append(
                TopologyDrift(
                    period_from=prev.period_start,
                    period_to=curr.period_start,
                    metric="bottleneck_distance",
                    value_from=0.0,
                    value_to=bottleneck_dist,
                    change_pct=0.0,  # Not a percentage metric
                    bottleneck_distance=bottleneck_dist,
                    is_significant=bottleneck_dist > bottleneck_threshold * 2,
                )
            )

        # Also check entropy drift
        if prev.persistent_entropy > 0:
            entropy_change = (
                curr.persistent_entropy - prev.persistent_entropy
            ) / prev.persistent_entropy
            if abs(entropy_change) > 0.3:  # >30% entropy change
                drifts.append(
                    TopologyDrift(
                        period_from=prev.period_start,
                        period_to=curr.period_start,
                        metric="entropy",
                        value_from=prev.persistent_entropy,
                        value_to=curr.persistent_entropy,
                        change_pct=entropy_change * 100,
                        bottleneck_distance=bottleneck_dist,
                        is_significant=abs(entropy_change) > 0.5,
                    )
                )

        # Check complexity drift
        if prev.structural_complexity > 0:
            cx_change = (
                curr.structural_complexity - prev.structural_complexity
            ) / prev.structural_complexity
            if abs(cx_change) > 0.2:
                drifts.append(
                    TopologyDrift(
                        period_from=prev.period_start,
                        period_to=curr.period_start,
                        metric="complexity",
                        value_from=prev.structural_complexity,
                        value_to=curr.structural_complexity,
                        change_pct=cx_change * 100,
                        bottleneck_distance=bottleneck_dist,
                        is_significant=abs(cx_change) > 0.5,
                    )
                )

    result.topology_drifts = drifts
    result.max_bottleneck_distance = max_bottleneck

    # Compute overall statistics
    complexities = [t.structural_complexity for t in period_topologies]
    entropies = [t.persistent_entropy for t in period_topologies]

    result.avg_complexity = sum(complexities) / len(complexities)
    result.avg_entropy = sum(entropies) / len(entropies) if entropies else 0.0

    if len(complexities) > 1:
        mean_c = result.avg_complexity
        result.complexity_variance = sum((c - mean_c) ** 2 for c in complexities) / len(
            complexities
        )

        # Determine trend
        first_half = complexities[: len(complexities) // 2]
        second_half = complexities[len(complexities) // 2 :]

        if first_half and second_half:
            first_avg = sum(first_half) / len(first_half)
            second_avg = sum(second_half) / len(second_half)

            if second_avg > first_avg * 1.2:
                result.trend_direction = "increasing"
            elif second_avg < first_avg * 0.8:
                result.trend_direction = "decreasing"
            elif result.complexity_variance > mean_c * 0.5:
                result.trend_direction = "volatile"
            else:
                result.trend_direction = "stable"

    # Flag anomalous periods (>2 std dev from mean complexity or high bottleneck)
    if result.complexity_variance > 0:
        std_dev = result.complexity_variance**0.5
        for topo in period_topologies:
            if abs(topo.structural_complexity - result.avg_complexity) > 2 * std_dev:
                topo.has_anomalies = True
                result.structural_anomaly_periods.append(topo.period_start)

    return result


__all__ = [
    "TemporalSliceAnalyzer",
    "TemporalSliceContext",
    "analyze_temporal_slices",
    "aggregate_temporal_data",
    "analyze_temporal_topology",
]
