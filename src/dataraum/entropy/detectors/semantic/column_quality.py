"""Column quality entropy detector.

Measures uncertainty from LLM-generated column quality reports.
Converts ColumnQualityReport scores into entropy: score = 1.0 - avg_quality_score.

Produces one EntropyObject per column, using the same logic that was previously
inline in entropy_phase._run_dimensional_entropy().

Source: quality_summary analysis (ColumnQualityReport)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject, ResolutionOption

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class ColumnQualityDetector(EntropyDetector):
    """Detect entropy from column quality reports.

    Table-scoped detector that produces one EntropyObject per column
    based on LLM quality assessments across slices.
    """

    detector_id = "column_quality"
    layer = Layer.SEMANTIC
    dimension = Dimension.DIMENSIONAL
    sub_dimension = SubDimension.COLUMN_QUALITY
    scope = "table"
    required_analyses = [AnalysisKey.COLUMN_QUALITY_REPORTS]
    description = "Column quality entropy from LLM quality reports"

    def load_data(self, context: DetectorContext) -> None:
        """Load column quality reports for the table."""
        if context.session is None or not context.table_id or not context.table_name:
            return

        data = self._load_column_quality_reports(
            context.session,
            context.table_id,
            context.table_name,
        )
        if data is not None:
            context.analysis_results["column_quality_reports"] = data

    @staticmethod
    def _load_column_quality_reports(
        session: Session,
        table_id: str,
        table_name: str,
    ) -> dict[str, Any] | None:
        """Load ColumnQualityReport data grouped by column for a table.

        Resolves slicing_view columns when present.

        Returns dict keyed by column_name with quality metrics,
        or None if no reports exist.
        """
        from dataraum.analysis.quality_summary.db_models import ColumnQualityReport
        from dataraum.storage import Column, Table

        # Get columns for this typed table
        table_columns = list(
            session.execute(select(Column).where(Column.table_id == table_id)).scalars().all()
        )

        # Check for slicing_view table (FK-based scoping)
        sv_table = session.execute(
            select(Table).where(
                Table.table_name == f"slicing_{table_name}",
                Table.layer == "slicing_view",
            )
        ).scalar_one_or_none()

        if sv_table:
            sv_cols = (
                session.execute(select(Column).where(Column.table_id == sv_table.table_id))
                .scalars()
                .all()
            )
            lookup_column_ids = [c.column_id for c in sv_cols]
        else:
            lookup_column_ids = [c.column_id for c in table_columns]

        # Query reports
        reports = list(
            session.execute(
                select(ColumnQualityReport).where(
                    ColumnQualityReport.source_column_id.in_(lookup_column_ids)
                )
            )
            .scalars()
            .all()
        )

        if not reports:
            return None

        # Build column_id lookup from typed table columns
        column_id_lookup = {c.column_name: c.column_id for c in table_columns}

        # Group by column
        grouped: dict[str, dict[str, Any]] = {}
        for report in reports:
            col_name = report.column_name
            if col_name not in grouped:
                # Resolve column_id: prefer typed table, fall back to slicing_view
                col_id = column_id_lookup.get(col_name)
                effective_table_id = table_id
                effective_table_name = table_name
                if col_id is None and sv_table:
                    col_id = report.source_column_id
                    effective_table_id = sv_table.table_id
                    effective_table_name = sv_table.table_name
                if col_id is None:
                    continue

                grouped[col_name] = {
                    "column_id": col_id,
                    "table_id": effective_table_id,
                    "table_name": effective_table_name,
                    "reports": [],
                }
            grouped[col_name]["reports"].append(report)

        if not grouped:
            return None

        # Compute aggregated metrics per column
        result: dict[str, Any] = {}
        for col_name, data in grouped.items():
            col_reports = data["reports"]
            avg_quality_score = sum(r.overall_quality_score for r in col_reports) / len(col_reports)
            grades = [r.quality_grade for r in col_reports]

            all_key_findings: list[str] = []
            all_quality_issues: list[dict[str, Any]] = []
            all_recommendations: list[str] = []

            for report in col_reports:
                rd = report.report_data or {}
                all_key_findings.extend(rd.get("key_findings", []))
                all_quality_issues.extend(rd.get("quality_issues", []))
                all_recommendations.extend(rd.get("recommendations", []))

            result[col_name] = {
                "column_id": data["column_id"],
                "table_id": data["table_id"],
                "table_name": data["table_name"],
                "avg_quality_score": avg_quality_score,
                "grades": grades,
                "slices_analyzed": len(col_reports),
                "key_findings": all_key_findings,
                "quality_issues": all_quality_issues,
                "quality_issues_count": len(all_quality_issues),
                "recommendations": all_recommendations,
                "recommendations_count": len(all_recommendations),
            }

        return result

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Produce one EntropyObject per column from quality reports."""
        reports_data: dict[str, Any] = context.get_analysis("column_quality_reports", {})
        if not reports_data:
            return []

        objects: list[EntropyObject] = []
        for col_name, col_data in reports_data.items():
            avg_quality_score = col_data["avg_quality_score"]
            entropy_score = 1.0 - avg_quality_score

            evidence: list[dict[str, Any]] = [
                {
                    "source": "column_quality_report",
                    "column_id": col_data["column_id"],
                    "table_id": col_data["table_id"],
                    "slices_analyzed": col_data["slices_analyzed"],
                    "avg_quality_score": avg_quality_score,
                    "grades": col_data["grades"],
                    "key_findings": col_data["key_findings"],
                    "quality_issues_count": col_data["quality_issues_count"],
                    "recommendations_count": col_data["recommendations_count"],
                }
            ]

            # No fix_schemas — column_quality is a downstream documentation-debt
            # detector. Quality issues are addressed by fixing upstream detectors
            # (typing, null_ratio, etc.) or by documenting business rules.
            resolution_options: list[ResolutionOption] = []

            # Use the effective table name for the target
            effective_table_name = col_data["table_name"]
            obj = EntropyObject(
                layer=self.layer,
                dimension=self.dimension,
                sub_dimension=self.sub_dimension,
                target=f"column:{effective_table_name}.{col_name}",
                score=entropy_score,
                evidence=evidence,
                resolution_options=resolution_options,
                detector_id=self.detector_id,
                source_analysis_ids=[],
            )
            objects.append(obj)

        return objects
