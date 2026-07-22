"""Cross-table consistency entropy detector.

Consumes ValidationResultRecord (the grounded ``sql_used`` + declared params).
The verdict is **recomputed on demand** (ADR-0017): this detector re-runs each
check's run-versioned ``sql_used`` against current data and scores the fresh
verdict — it never reads a stored pass/fail (a stored verdict goes stale on
re-import, the SQL does not).

Scope: table-level, with COLUMN-grain objects fanned out for failed checks
(DAT-432): a failed reconciliation bands the columns its SQL actually
touched (``columns_used``), so the band reaches the columns deliverable
metrics flow through — not just an aggregate ``table:`` row nothing joins on.

Score semantics (DAT-442 honesty + the scoreboard finding below):
- A failed CRITICAL check is CATEGORICAL: score 1.0. The spec's own tolerance
  already decided pass/fail, so "failed critical" means a declared identity is
  broken beyond its declared tolerance — the magnitude stays in evidence as
  the diagnostic. (Honest rates put the injected 10% TB↔GL break at risk
  0.8×0.10 = 0.08 — invisible below the 0.3 band — while every GL-derived
  deliverable number was measurably wrong: 0 prevented / 8 wrong-delivered.)
- Non-critical failures score the honest relative discrepancy ``deviation /
  magnitude`` (no boost, DAT-442) — uniform across check types now that the SQL
  output is contracted (ADR-0017), no per-check_type rate matching.
- ERROR/inconclusive (or unbound) scores 0.0 + a ``validation_unassessed``
  warning: an unassessed check is ignorance, not measured risk — the old 0.5
  turned LLM SQL-generation nondeterminism into clean-table false alarms.

Aggregation: max() — worst validation failure drives the table's score.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select

from dataraum.analysis.validation.evaluate import (
    DEFAULT_TOLERANCE,
    ValidationVerdict,
    verdict_from_sql,
)
from dataraum.analysis.validation.models import ValidationStatus
from dataraum.core.logging import get_logger
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject

logger = get_logger(__name__)


def _score(verdict: ValidationVerdict, severity: str) -> float:
    """Score a recomputed validation verdict (ADR-0017).

    The verdict is recomputed on demand from the contracted SQL output, so its
    ``details`` carry a uniform ``deviation``/``magnitude`` — no per-check_type
    branching, no column-name guessing.

    Args:
        verdict: The freshly recomputed verdict (deviation/magnitude in details).
        severity: The declared severity (from the result record).

    Returns:
        Score between 0.0 (passed / unassessed) and 1.0 (critical failure).
    """
    if verdict.passed:
        return 0.0

    if verdict.status != ValidationStatus.FAILED:
        # INCONCLUSIVE (the SQL ran but didn't honor the contract) or UNBOUND
        # (no sql_used) — ignorance, never a risk measurement. The old 0.5
        # banded CLEAN tables on nondeterministic SQL failures (DAT-439); the
        # caller logs the unassessed check.
        return 0.0

    if severity == "critical":
        # Categorical: a CRITICAL identity failed beyond its declared
        # tolerance — the books don't reconcile. The relative magnitude stays
        # in evidence; scoring it as a rate hid provably-wrong deliverables.
        return 1.0

    # Honest relative discrepancy (no boost, DAT-442): deviation / magnitude.
    deviation = abs(float(verdict.details.get("deviation", 0) or 0))
    magnitude = abs(float(verdict.details.get("magnitude", 1) or 0)) or 1.0
    return min(1.0, deviation / magnitude)


def _load_run_specs(context: DetectorContext) -> dict[str, Any]:
    """Load this run's validation specs (severity + tolerance) from config.

    The verdict's tolerance and the critical-rule severity are declared config,
    not stored on the record (ADR-0017). The run's vertical is read from a
    validation lifecycle artifact via the shared session — the entropy/detect
    layer is otherwise vertical-free. Returns ``{}`` (graceful, never raises) when
    the run/session/vertical can't be resolved; consumers fall back to defaults.
    """
    if context.session is None or context.run_id is None:
        return {}

    from sqlalchemy import select

    from dataraum.analysis.validation.config import load_all_validation_specs
    from dataraum.lifecycle.db_models import LifecycleArtifact

    artifact = (
        context.session.execute(
            select(LifecycleArtifact)
            .where(
                LifecycleArtifact.artifact_type == "validation",
                LifecycleArtifact.run_id == context.run_id,
            )
            .limit(1)
        )
        .scalars()
        .first()
    )
    vertical = (artifact.teaches or {}).get("vertical") if artifact else None
    return load_all_validation_specs(vertical, context.session) if vertical else {}


class CrossTableConsistencyDetector(EntropyDetector):
    """Detect entropy from cross-table validation failures.

    Table-scoped detector that scores validation check results.
    Produces one EntropyObject per table with the worst validation
    failure as the score.
    """

    detector_id = "cross_table_consistency"
    layer = Layer.COMPUTATIONAL
    dimension = Dimension.RECONCILIATION
    sub_dimension = SubDimension.CROSS_TABLE_CONSISTENCY
    scope = "table"
    required_analyses = [AnalysisKey.VALIDATION]
    description = "Cross-table reconciliation failures from validation checks"

    def load_data(self, context: DetectorContext) -> None:
        """Load validation results that involve this table."""
        if context.session is None or not context.table_id:
            return

        from dataraum.analysis.validation.db_models import ValidationResultRecord

        # ValidationResultRecord.table_ids is a JSON list of table_ids involved.
        # We need results where our table_id appears in that list.
        # SQLAlchemy JSON containment varies by backend; load all and filter.
        # Run-versioned since DAT-438: on the detect path scope to THIS run's
        # rows (the DetectorContext.run_id contract); ``None`` (test/legacy
        # callers outside the workflow) adds no filter.
        stmt = select(ValidationResultRecord)
        if context.run_id is not None:
            stmt = stmt.where(ValidationResultRecord.run_id == context.run_id)
        all_results = list(context.session.execute(stmt).scalars().all())

        matching = [r for r in all_results if context.table_id in (r.table_ids or [])]

        if matching:
            context.analysis_results["validation"] = matching

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Score validation results for this table.

        Returns a single EntropyObject with score = max(per-check scores).
        """
        results: list[Any] = context.get_analysis("validation", [])
        if not results:
            return [
                self.create_entropy_object(
                    context=context,
                    score=0.0,
                    evidence=[{"reason": "no_validation_results"}],
                )
            ]

        # The verdict's tolerance + the critical-rule severity are declared config
        # (ADR-0017), read from the spec — never stored on the record. The run's
        # vertical comes from a validation lifecycle artifact via the shared session.
        specs = _load_run_specs(context)

        scores: list[float] = []
        evidence: list[dict[str, Any]] = []
        # Worst failed-check score + entries per column the failing SQL touched.
        per_column: dict[str, tuple[float, list[dict[str, Any]]]] = {}

        for result in results:
            spec = specs.get(result.validation_id)
            tolerance = (
                spec.tolerance
                if spec is not None and spec.tolerance is not None
                else DEFAULT_TOLERANCE
            )
            severity = spec.severity.value if spec is not None else "info"

            # Recompute the verdict on demand (ADR-0017): re-run the run-versioned
            # ``sql_used`` against current data rather than read a stored pass/fail
            # that goes stale on re-import. check_type isn't needed (the score is
            # uniform deviation/magnitude); the message uses a generic label.
            verdict = verdict_from_sql(context.duckdb_conn, result.sql_used, tolerance=tolerance)
            score = _score(verdict, severity)
            if verdict.status == ValidationStatus.ERROR:
                logger.warning(
                    "validation_unassessed",
                    validation_id=result.validation_id,
                    table=context.table_name,
                )
            scores.append(score)
            entry = {
                "validation_id": result.validation_id,
                "status": verdict.status.value,
                "severity": severity,
                "passed": verdict.passed,
                "score": score,
                "message": verdict.message,
            }
            evidence.append(entry)
            if score > 0.0:
                for col_name in self._own_columns_used(context, result):
                    worst, entries = per_column.get(col_name, (0.0, []))
                    entries.append(dict(entry))
                    per_column[col_name] = (max(worst, score), entries)

        # max() — worst failure drives the score
        final_score = max(scores) if scores else 0.0

        objects = [
            self.create_entropy_object(
                context=context,
                score=final_score,
                evidence=evidence,
            )
        ]
        objects.extend(self._column_objects(context, per_column))
        return objects

    @staticmethod
    def _own_columns_used(context: DetectorContext, result: Any) -> list[str]:
        """The failing check's ``columns_used`` entries that name THIS table.

        Entries are LLM-declared ``"table.column"`` strings. Table names are
        workspace-unique and narrow (DAT-639 — no ``src_<digest>__`` prefix), so a
        single exact match is correct and unambiguous: there is exactly one table
        of a given name in the workspace, so this can't cross-claim another
        source's same-named table.
        """
        table_name = context.table_name or ""
        out: list[str] = []
        for ref in getattr(result, "columns_used", None) or []:
            table_part, _, column_part = ref.partition(".")
            if column_part and table_part == table_name:
                out.append(column_part)
        return out

    def _column_objects(
        self,
        context: DetectorContext,
        per_column: dict[str, tuple[float, list[dict[str, Any]]]],
    ) -> list[EntropyObject]:
        """Column-grain objects for the columns failing checks touched.

        The band must reach the columns deliverable metrics flow through —
        a ``table:`` row joins to nothing downstream. ``column_id`` rides in
        evidence so the engine anchors the record; names the LLM declared but
        the table doesn't have are dropped (hallucination guard).
        """
        if context.session is None or not context.table_id or not per_column:
            return []

        from dataraum.storage import Column as ColumnModel

        col_ids = {
            col.column_name: col.column_id
            for col in context.session.execute(
                select(ColumnModel).where(ColumnModel.table_id == context.table_id)
            ).scalars()
        }
        objects: list[EntropyObject] = []
        for col_name, (worst, entries) in sorted(per_column.items()):
            column_id = col_ids.get(col_name)
            if column_id is None:
                logger.warning(
                    "validation_column_unknown", table=context.table_name, column=col_name
                )
                continue
            objects.append(
                EntropyObject(
                    layer=self.layer,
                    dimension=self.dimension,
                    sub_dimension=self.sub_dimension,
                    target=f"column:{context.table_name}.{col_name}",
                    score=worst,
                    evidence=[
                        {
                            **entry,
                            # BOTH ids: the engine's _extract_column_id anchors a
                            # record only when an entry carries column_id AND
                            # table_id (review wave-1: without table_id every
                            # fan-out row persisted with column_id=NULL and the
                            # cockpit's per-column evidence reads missed them).
                            "column_id": column_id,
                            "table_id": context.table_id,
                            "_table_name": context.table_name,
                            "_column_name": col_name,
                        }
                        for entry in entries
                    ],
                    detector_id=self.detector_id,
                )
            )
        return objects
