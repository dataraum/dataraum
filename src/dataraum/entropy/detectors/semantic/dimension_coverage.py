"""Dimension coverage entropy detector.

Measures NULL rate per dimension column on enriched views.
A column with 80% NULLs provides unreliable slicing — this detector
quantifies that uncertainty so contracts and the Bayesian network
can factor it in.

Source: EnrichedView.dimension_columns + DuckDB NULL counts
Score = mean NULL rate across dimension columns (0.0 = fully populated, 1.0 = all NULLs).
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select

from dataraum.core.logging import get_logger
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.models import EntropyObject, ResolutionOption

logger = get_logger(__name__)


class DimensionCoverageDetector(EntropyDetector):
    """Detector for dimension column coverage on enriched views.

    Measures how well dimension columns (added via LEFT JOIN) are populated.
    High NULL rates in dimension columns mean unreliable slicing/grouping.

    Source: EnrichedView metadata + DuckDB queries on the view
    Score = mean NULL rate across all dimension columns.
    """

    detector_id = "dimension_coverage"
    layer = "semantic"
    dimension = "coverage"
    sub_dimension = "dimension_coverage"
    scope = "view"
    required_analyses = ["enriched_view"]
    description = "Measures NULL rate per dimension column on enriched views"

    def load_data(self, context: DetectorContext) -> None:
        """Load EnrichedView metadata for the target view."""
        if context.session is None or not context.view_name:
            return

        from dataraum.analysis.views.db_models import EnrichedView

        view = context.session.execute(
            select(EnrichedView).where(EnrichedView.view_name == context.view_name)
        ).scalar_one_or_none()

        if view is not None:
            context.analysis_results["enriched_view"] = view

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Detect dimension coverage entropy.

        For each dimension column on the enriched view, queries DuckDB
        for the NULL rate. The overall score is the mean NULL rate.

        Args:
            context: Detector context with enriched_view in analysis_results

        Returns:
            List with single EntropyObject for dimension coverage
        """
        view = context.get_analysis("enriched_view")
        dimension_columns: list[str] = view.dimension_columns or []

        # No dimension columns → no uncertainty
        if not dimension_columns:
            return [
                self.create_entropy_object(
                    context=context,
                    score=0.0,
                    evidence=[{"reason": "no_dimension_columns"}],
                )
            ]

        evidence: list[dict[str, Any]] = []
        null_rates: list[float] = []

        for col in dimension_columns:
            null_rate = self._query_null_rate(context, col)
            null_rates.append(null_rate)
            evidence.append(
                {
                    "column": col,
                    "null_rate": null_rate,
                }
            )

        score = sum(null_rates) / len(null_rates)

        resolution_options: list[ResolutionOption] = []
        high_null_cols = [e["column"] for e in evidence if e["null_rate"] > 0.5]
        if high_null_cols:
            resolution_options.append(
                ResolutionOption(
                    action="investigate_relationship",
                    parameters={
                        "columns": high_null_cols,
                        "view_name": context.view_name,
                    },
                    effort="medium",
                    description=(
                        "Investigate why dimension columns have high NULL rates — "
                        "the join relationship may be incorrect or the dimension "
                        "table may have missing keys"
                    ),
                )
            )

        return [
            self.create_entropy_object(
                context=context,
                score=score,
                evidence=evidence,
                resolution_options=resolution_options,
            )
        ]

    @staticmethod
    def _query_null_rate(context: DetectorContext, column: str) -> float:
        """Query DuckDB for the NULL rate of a column on the view.

        Returns 1.0 if the query fails (assume worst case).
        """
        if context.duckdb_conn is None:
            return 1.0

        try:
            result = context.duckdb_conn.execute(
                f'SELECT COUNT(*) FILTER (WHERE "{column}" IS NULL) * 1.0 '
                f'/ NULLIF(COUNT(*), 0) FROM "{context.view_name}"'
            ).fetchone()
            return float(result[0]) if result and result[0] is not None else 0.0
        except Exception:
            logger.warning(
                "dimension_coverage_query_failed",
                view=context.view_name,
                column=column,
                exc_info=True,
            )
            return 1.0
