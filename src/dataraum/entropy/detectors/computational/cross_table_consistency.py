"""Cross-table consistency entropy detector.

Consumes ValidationResultRecord from the validation phase.
The validation phase generates and executes SQL checks — this detector
only scores the results.

Scope: table-level. Each validation check spans multiple tables; the
score attaches to every table involved in the check.

Score conversion by check type:
- balance: min(1.0, |difference| / magnitude) with sqrt boost
- comparison: 1.0 if failed, 0.0 if passed (binary for critical checks)
- aggregate: violation_rate with sqrt boost
- constraint: min(1.0, violation_count / total_rows) with sqrt boost

Aggregation: max() — worst validation failure drives the table's score.
"""

from __future__ import annotations

import math
from typing import Any

from sqlalchemy import select

from dataraum.core.logging import get_logger
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject, ResolutionOption

logger = get_logger(__name__)


def _score_validation_result(result: Any) -> float:
    """Convert a ValidationResultRecord to an entropy score.

    Args:
        result: ValidationResultRecord with status, severity, details.

    Returns:
        Score between 0.0 (passed) and 1.0 (critical failure).
    """
    if result.passed:
        return 0.0

    if result.status == "error":
        # Execution error — can't assess, treat as moderate uncertainty
        return 0.5

    details = result.details or {}
    check_type = details.get("check_type", "")

    if check_type == "balance":
        difference = abs(float(details.get("difference", 0)))
        magnitude = abs(float(details.get("magnitude", 1)))
        if magnitude == 0:
            return 1.0
        raw = min(1.0, difference / magnitude)
        return min(1.0, math.sqrt(raw)) if raw > 0 else 0.0

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
            raw = min(1.0, diff / magnitude)
            return min(1.0, math.sqrt(raw))
        # Binary: critical checks either hold or don't
        return 1.0

    if check_type == "aggregate":
        rate = float(details.get("violation_rate", details.get("orphan_rate", 0)))
        return min(1.0, math.sqrt(rate)) if rate > 0 else 0.0

    if check_type == "constraint":
        count = float(details.get("violation_count", 0))
        total = float(details.get("total_rows", 0))
        if total > 0:
            raw = min(1.0, count / total)
            return min(1.0, math.sqrt(raw)) if raw > 0 else 0.0
        # No total_rows available — score based on violation count alone.
        # 1 violation ~ 0.1, 10 ~ 0.32, 100+ ~ 1.0
        raw = min(1.0, count / 100.0)
        return min(1.0, math.sqrt(raw)) if raw > 0 else 0.0

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
        all_results = list(context.session.execute(select(ValidationResultRecord)).scalars().all())

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

        for result in results:
            score = _score_validation_result(result)
            scores.append(score)
            evidence.append(
                {
                    "validation_id": result.validation_id,
                    "status": result.status,
                    "severity": result.severity,
                    "passed": result.passed,
                    "score": score,
                    "message": result.message,
                }
            )

        # max() — worst failure drives the score
        final_score = max(scores) if scores else 0.0

        resolution_options: list[ResolutionOption] = []
        if final_score > 0:
            failed_ids = [e["validation_id"] for e in evidence if not e["passed"]]
            resolution_options.append(
                ResolutionOption(
                    action="investigate_reconciliation",
                    parameters={
                        "table": context.table_name,
                        "failed_validations": failed_ids,
                    },
                    effort="high",
                    description=(
                        "Investigate cross-table reconciliation failures — "
                        "these require human review of the underlying data mismatch"
                    ),
                )
            )

        return [
            self.create_entropy_object(
                context=context,
                score=final_score,
                evidence=evidence,
                resolution_options=resolution_options,
            )
        ]
