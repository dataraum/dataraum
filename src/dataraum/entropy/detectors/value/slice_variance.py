"""Slice variance entropy detector.

Measures cross-slice statistical variance for a column. High variance
across slices indicates the column behaves differently depending on
context (e.g., null ratio varies by region), which is a signal the
Bayesian network uses for query/aggregation readiness.

Data source: per-slice StatisticalProfile and StatisticalQualityMetrics
records created by slice_analysis (run_statistics_on_slice / run_quality_on_slice).
"""

from __future__ import annotations

from typing import Any

from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject, ResolutionOption
from dataraum.pipeline.fixes.models import FixSchema, FixSchemaField

# Fallback thresholds (matching variance.py defaults)
_DEFAULT_NULL_SPREAD = 0.10
_DEFAULT_DISTINCT_RATIO = 5.0
_DEFAULT_OUTLIER_SPREAD = 0.25
_DEFAULT_BENFORD_SPREAD = 0.30


class SliceVarianceDetector(EntropyDetector):
    """Detector for cross-slice statistical variance.

    Queries per-slice StatisticalProfile and StatisticalQualityMetrics
    for the target column, computes spread metrics (null_spread,
    distinct_ratio, outlier_spread, benford_spread), and produces a
    score = max of normalized spreads.

    Each spread is normalized: min(spread / (2 * threshold), 1.0)
    so that at-threshold = 0.5, at 2x threshold = 1.0.
    """

    # Semantic roles where slice variance is meaningless —
    # keys/FKs are unique by construction, attributes are free text,
    # timestamps and measures naturally vary across categorical slices.
    _SKIP_ROLES = frozenset(
        {"key", "foreign_key", "identifier", "attribute", "timestamp", "measure"}
    )

    detector_id = "slice_variance"
    layer = Layer.VALUE
    dimension = Dimension.VARIANCE
    sub_dimension = SubDimension.SLICE_STABILITY
    scope = "column"
    description = "Measures cross-slice statistical variance for a column"

    @property
    def fix_schemas(self) -> list[FixSchema]:
        """Schema for accepting slice variance findings."""
        return [
            FixSchema(
                action="accept_finding",
                target="config",
                description="Mark slice variance findings as reviewed and accepted",
                config_path="entropy/thresholds.yaml",
                key_path=["detectors", "slice_variance", "accepted_columns"],
                operation="append",
                requires_rerun="quality_review",
                guidance=(
                    "Present ALL affected columns in a numbered list with their key "
                    "variance metrics. For each column show: table.column — which "
                    "spread thresholds were exceeded — spread values.\n"
                    "Ask the user to select columns by number (comma-separated), or 'all'.\n"
                    "Then ask WHY the variance is acceptable (e.g., 'expected regional "
                    "differences', 'known data partitioning')."
                ),
                fields={
                    "reason": FixSchemaField(
                        type="string",
                        required=False,
                        description="Why the finding was accepted",
                    ),
                },
            )
        ]

    def _resolve_source_id(self, context: DetectorContext) -> str | None:
        """Resolve source_id from context, falling back to table_id lookup."""
        if context.source_id:
            return context.source_id
        if context.session is None or context.table_id is None:
            return None
        from sqlalchemy import select

        from dataraum.storage import Table

        table = context.session.execute(
            select(Table.source_id).where(Table.table_id == context.table_id)
        ).scalar_one_or_none()
        return table

    def _load_data(self, context: DetectorContext) -> list[dict[str, Any]]:
        """Load per-slice statistics for this column.

        Queries slice tables (Table.layer == 'slice') for the source,
        finds columns matching context.column_name, and loads their
        StatisticalProfile + StatisticalQualityMetrics.

        Falls back to context.analysis_results["slice_profiles"] when
        no DB session is available (e.g. unit tests).
        """
        if context.session is None or not context.column_name:
            fallback: list[dict[str, Any]] = context.analysis_results.get("slice_profiles", [])
            return fallback

        source_id = self._resolve_source_id(context)
        if source_id is None:
            fallback_2: list[dict[str, Any]] = context.analysis_results.get("slice_profiles", [])
            return fallback_2

        from sqlalchemy import select

        from dataraum.analysis.statistics.db_models import StatisticalProfile
        from dataraum.analysis.statistics.quality_db_models import StatisticalQualityMetrics
        from dataraum.storage import Column, Table

        # 1. Find all slice tables for this source
        slice_tables = (
            context.session.execute(
                select(Table).where(
                    Table.layer == "slice",
                    Table.source_id == source_id,
                )
            )
            .scalars()
            .all()
        )
        if not slice_tables:
            return []

        slice_table_ids = [t.table_id for t in slice_tables]
        slice_table_names = {t.table_id: t.table_name for t in slice_tables}

        # 2. Find columns in those tables matching our column name
        slice_columns = (
            context.session.execute(
                select(Column).where(
                    Column.table_id.in_(slice_table_ids),
                    Column.column_name == context.column_name,
                )
            )
            .scalars()
            .all()
        )
        if not slice_columns:
            return []

        slice_col_ids = [c.column_id for c in slice_columns]
        col_to_table = {c.column_id: c.table_id for c in slice_columns}

        # 3. Load StatisticalProfile for each slice column
        profiles = (
            context.session.execute(
                select(StatisticalProfile).where(
                    StatisticalProfile.column_id.in_(slice_col_ids),
                )
            )
            .scalars()
            .all()
        )
        profile_by_col: dict[str, StatisticalProfile] = {p.column_id: p for p in profiles}

        # 4. Load StatisticalQualityMetrics for each slice column
        quality_metrics = (
            context.session.execute(
                select(StatisticalQualityMetrics).where(
                    StatisticalQualityMetrics.column_id.in_(slice_col_ids),
                )
            )
            .scalars()
            .all()
        )
        quality_by_col: dict[str, StatisticalQualityMetrics] = {
            q.column_id: q for q in quality_metrics
        }

        # 5. Build per-slice metric dicts
        slice_profiles: list[dict[str, Any]] = []
        for col in slice_columns:
            profile = profile_by_col.get(col.column_id)
            if profile is None:
                continue

            null_ratio = profile.null_count / profile.total_count if profile.total_count else 0.0

            entry: dict[str, Any] = {
                "null_ratio": null_ratio,
                "distinct_count": profile.distinct_count or 0,
                "row_count": profile.total_count,
                "slice_table_name": slice_table_names.get(col_to_table[col.column_id], ""),
            }

            qm = quality_by_col.get(col.column_id)
            if qm is not None:
                entry["outlier_ratio"] = qm.iqr_outlier_ratio or 0.0
                qd = qm.quality_data or {}
                benford = qd.get("benford_analysis")
                if isinstance(benford, dict):
                    entry["benford_p_value"] = benford.get("p_value")
                else:
                    entry["benford_p_value"] = None
            else:
                entry["outlier_ratio"] = 0.0
                entry["benford_p_value"] = None

            slice_profiles.append(entry)

        return slice_profiles

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Detect slice variance entropy.

        Args:
            context: Detector context with slice_profiles in analysis_results.

        Returns:
            List with single EntropyObject, or empty if < 2 slices.
        """
        # Skip columns whose semantic role makes variance meaningless
        if context.session is not None and context.column_id is not None:
            from dataraum.entropy.detectors.loaders import load_semantic

            sem = load_semantic(context.session, context.column_id)
            if sem is not None:
                role = sem.get("semantic_role")
                if role in self._SKIP_ROLES:
                    return []

        slice_profiles: list[dict[str, Any]] = self._load_data(context)

        if len(slice_profiles) < 2:
            return []

        # Load configurable thresholds
        config = get_entropy_config()
        detector_config = config.detector("slice_variance")
        null_threshold = detector_config.get("null_spread_threshold", _DEFAULT_NULL_SPREAD)
        distinct_threshold = detector_config.get(
            "distinct_ratio_threshold", _DEFAULT_DISTINCT_RATIO
        )
        outlier_threshold = detector_config.get("outlier_spread_threshold", _DEFAULT_OUTLIER_SPREAD)
        benford_threshold = detector_config.get("benford_spread_threshold", _DEFAULT_BENFORD_SPREAD)
        accepted_columns: list[str] = self.config.get("accepted_columns") or detector_config.get(
            "accepted_columns", []
        )

        # Compute spread metrics
        null_ratios = [p["null_ratio"] for p in slice_profiles]
        distinct_counts = [p["distinct_count"] for p in slice_profiles]
        outlier_ratios = [p["outlier_ratio"] for p in slice_profiles]
        benford_pvalues = [
            p["benford_p_value"] for p in slice_profiles if p.get("benford_p_value") is not None
        ]

        # Null spread
        null_spread = max(null_ratios) - min(null_ratios)
        null_norm = min(null_spread / (2 * null_threshold), 1.0)

        # Distinct ratio (max/min, if min > 0)
        min_distinct = min(distinct_counts)
        max_distinct = max(distinct_counts)
        distinct_ratio: float | None
        if min_distinct > 0:
            ratio = max_distinct / min_distinct
            distinct_ratio = ratio
            # Normalize: ratio of 1.0 = no spread, threshold = 2.0
            # spread = ratio - 1.0, threshold_spread = threshold - 1.0 = 1.0
            distinct_spread = ratio - 1.0
            distinct_norm = min(distinct_spread / (2 * (distinct_threshold - 1.0)), 1.0)
        else:
            # min_distinct == 0: ratio is undefined (e.g., entirely-null slice)
            distinct_ratio = None
            distinct_norm = 0.0

        # Outlier spread
        outlier_spread = max(outlier_ratios) - min(outlier_ratios)
        outlier_norm = min(outlier_spread / (2 * outlier_threshold), 1.0)

        # Benford spread
        if len(benford_pvalues) >= 2:
            benford_spread = max(benford_pvalues) - min(benford_pvalues)
            benford_norm = min(benford_spread / (2 * benford_threshold), 1.0)
        else:
            benford_spread = 0.0
            benford_norm = 0.0

        # Score = max of normalized spreads
        score = max(null_norm, distinct_norm, outlier_norm, benford_norm)

        # Track which thresholds were exceeded
        exceeded: list[str] = []
        if null_spread > null_threshold:
            exceeded.append("null_spread")
        if distinct_ratio is not None and distinct_ratio > distinct_threshold:
            exceeded.append("distinct_ratio")
        if outlier_spread > outlier_threshold:
            exceeded.append("outlier_spread")
        if len(benford_pvalues) >= 2 and benford_spread > benford_threshold:
            exceeded.append("benford_spread")

        evidence = [
            {
                "null_spread": round(null_spread, 4),
                "distinct_ratio": round(distinct_ratio, 2) if distinct_ratio is not None else None,
                "outlier_spread": round(outlier_spread, 4),
                "benford_spread": round(benford_spread, 4),
                "exceeded_thresholds": exceeded,
                "slice_count": len(slice_profiles),
            }
        ]

        resolution_options: list[ResolutionOption] = []
        if score > 0:
            resolution_options.append(
                ResolutionOption(
                    action="accept_finding",
                    parameters={
                        "column": context.column_name,
                        "detector_id": self.detector_id,
                    },
                    effort="low",
                    description="Accept slice variance as expected for this column",
                )
            )

        # Mark as accepted (score stays honest, contract overrule handles gate)
        target_key = f"{context.table_name}.{context.column_name}"
        if target_key in accepted_columns:
            evidence[0]["accepted"] = True

        return [
            self.create_entropy_object(
                context=context,
                score=score,
                evidence=evidence,
                resolution_options=resolution_options,
            )
        ]
