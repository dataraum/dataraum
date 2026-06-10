"""Cross-table consistency entropy detector.

Consumes ValidationResultRecord from the validation phase.
The validation phase generates and executes SQL checks — this detector
only scores the results.

Scope: table-level, with COLUMN-grain objects fanned out for failed checks
(DAT-432/L7): a failed reconciliation bands the columns its SQL actually
touched (``columns_used``), so the band reaches the columns deliverable
metrics flow through — not just an aggregate ``table:`` row nothing joins on.

Score semantics (DAT-442 honesty + the L7 scoreboard finding):
- A failed CRITICAL check is CATEGORICAL: score 1.0. The spec's own tolerance
  already decided pass/fail, so "failed critical" means a declared identity is
  broken beyond its declared tolerance — the magnitude stays in evidence as
  the diagnostic. (Honest rates put the injected 10% TB↔GL break at risk
  0.8×0.10 = 0.08 — invisible below the 0.3 band — while every GL-derived
  deliverable number was measurably wrong: 0 prevented / 8 wrong-delivered.)
- Non-critical failures keep the honest rates per check type:
  balance |difference|/magnitude · comparison proportional/binary ·
  aggregate violation_rate · constraint count/total.
- ERROR/inconclusive scores 0.0 + a ``validation_unassessed`` warning: an
  unassessed check is ignorance, not measured risk — the old 0.5 turned LLM
  SQL-generation nondeterminism into clean-table false alarms.

Aggregation: max() — worst validation failure drives the table's score.
"""

from __future__ import annotations

import re
from typing import Any

from sqlalchemy import select

from dataraum.core.logging import get_logger
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject

logger = get_logger(__name__)

# The content-keyed upload prefix (src_<sha1>__) — the ONLY prefix the logical
# table-name fallback strips. A bare '__' split would eat legitimate name parts
# and cross-claim same-named tables from different sources.
_UPLOAD_PREFIX = re.compile(r"^src_[0-9a-f]{40}__")


def _strip_upload_prefix(name: str) -> str:
    return _UPLOAD_PREFIX.sub("", name)


def _score_validation_result(result: Any) -> float:
    """Convert a ValidationResultRecord to an entropy score.

    Args:
        result: ValidationResultRecord with status, severity, details.

    Returns:
        Score between 0.0 (passed) and 1.0 (critical failure).
    """
    # PASSED is the ONLY ``passed=True`` state — SKIPPED and ERROR both carry
    # ``passed=False`` and so fall through to the explicit status branches
    # below (which is why there is no ``status == "passed"`` branch).
    if result.passed:
        return 0.0

    if result.status == "skipped":
        # Bind-time skip: the LLM declared the validation inapplicable to
        # this workspace. Not a data measurement — it must not contribute
        # entropy. Without this branch a skipped row that carries table_ids
        # falls into the check-type scoring below and can score 1.0
        # (comparison's binary branch) — a mislabel (DAT-439).
        return 0.0

    if result.status == "error":
        # Execution error or inconclusive evaluation (DAT-439) — the check
        # could not assess the data. Ignorance, never a risk measurement:
        # the old 0.5 banded CLEAN tables whenever the LLM's generated SQL
        # failed to run (nondeterministic false alarms). The caller logs it.
        return 0.0

    if result.severity == "critical":
        # Categorical (L7): a CRITICAL identity failed beyond its own declared
        # tolerance — the books don't reconcile. The relative magnitude stays
        # in evidence; scoring it as a rate hid provably-wrong deliverables.
        return 1.0

    details = result.details or {}
    check_type = details.get("check_type", "")

    if check_type == "balance":
        difference = abs(float(details.get("difference", 0)))
        magnitude = abs(float(details.get("magnitude", 1)))
        if magnitude == 0:
            return 1.0
        # Honest relative discrepancy (no boost, DAT-442).
        return min(1.0, difference / magnitude)

    if check_type == "comparison":
        # If the comparison has numeric difference, score proportionally
        # like a balance check (e.g., trial_balance equation mismatch).
        comp_difference = details.get("difference")
        if comp_difference is not None:
            diff = abs(float(comp_difference))
            if diff == 0:
                # passed=False but difference=0 is inconsistent — treat as failure
                return 1.0
            # Use left_side as magnitude reference
            magnitude = abs(float(details.get("left_side", details.get("magnitude", 1))))
            if magnitude == 0:
                return 1.0
            return min(1.0, diff / magnitude)
        # Binary: critical checks either hold or don't
        return 1.0

    if check_type == "aggregate":
        rate = float(details.get("violation_rate", details.get("orphan_rate", 0)))
        return min(1.0, rate) if rate > 0 else 0.0

    if check_type == "constraint":
        count = float(details.get("violation_count", 0))
        total = float(details.get("total_rows", 0))
        if total > 0:
            # Honest violation rate (no boost, DAT-442).
            return min(1.0, count / total)
        # No total_rows available — rough magnitude proxy on raw count
        # (no boost): 1 violation ~ 0.01, 10 ~ 0.1, 100+ ~ 1.0.
        return min(1.0, count / 100.0)

    # Unknown check type — use severity as fallback
    severity_scores = {"critical": 1.0, "high": 0.7, "medium": 0.4, "low": 0.1}
    return severity_scores.get(result.severity, 0.5)


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

        scores: list[float] = []
        evidence: list[dict[str, Any]] = []
        # Worst failed-check score + entries per column the failing SQL touched.
        per_column: dict[str, tuple[float, list[dict[str, Any]]]] = {}

        for result in results:
            score = _score_validation_result(result)
            if result.status == "error":
                logger.warning(
                    "validation_unassessed",
                    validation_id=result.validation_id,
                    table=context.table_name,
                )
            scores.append(score)
            entry = {
                "validation_id": result.validation_id,
                "status": result.status,
                "severity": result.severity,
                "passed": result.passed,
                "score": score,
                "message": result.message,
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

        Entries are LLM-declared ``"table.column"`` strings; the table part may
        carry the physical ``src_<digest>__`` upload prefix or the logical name,
        so both sides compare suffix-stripped.
        """

        table_name = context.table_name or ""
        own_logical = _strip_upload_prefix(table_name)
        out: list[str] = []
        for ref in getattr(result, "columns_used", None) or []:
            table_part, _, column_part = ref.partition(".")
            if not column_part:
                continue
            # Exact physical match first; the logical fallback strips ONLY the
            # src_<digest>__ upload prefix (never any '__') and requires the
            # ref to be unprefixed — otherwise a check touching source A's
            # journal_lines would band source B's same-named table (review
            # wave-1, the suffix-matching bug class for the third time).
            if table_part == table_name or (
                table_part == _strip_upload_prefix(table_part) and table_part == own_logical
            ):
                out.append(column_part)
        return out

    def _column_objects(
        self,
        context: DetectorContext,
        per_column: dict[str, tuple[float, list[dict[str, Any]]]],
    ) -> list[EntropyObject]:
        """Column-grain objects for the columns failing checks touched (L7).

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
