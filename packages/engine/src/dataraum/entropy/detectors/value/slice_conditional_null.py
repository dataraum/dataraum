"""Slice-conditional null detector — nulls concentrated in specific slices (DAT-473).

The dataset-level ``null_ratio`` is a single fraction: a column 5%-null overall reads as
mild. But if those nulls are not spread evenly — if one cost center is 60% null while the
rest are clean — every aggregate sliced by that dimension is silently biased, and the flat
ratio hides it. This detector reads that concentration.

The statistic is **bias-corrected Cramér's V** (Bergsma) on the 2×K contingency of
``(value IS NULL) × slice`` under the **Cochran validity rule** (any expected cell < 5 →
abstain), grounded in the kill gate (``entropy_eval_architecture.md``; pinned in
dataraum-eval ``test_slice_null_gate.py``). For the target column it scans each sibling
low-cardinality categorical as a candidate slice dimension and emits the MAX association
over the slices that yield a valid table — ``0.0`` when missingness is independent of every
slice (MCAR), when the column has no nulls, or when no slice yields a valid 2×K table.

Teach-closeable: when a user documents that the column's missingness is expected given a
slice (the ``document_business_rule`` → ``ConfigOverlay(type='expected_dependency')``
archetype — the same overlay ``dimensional_entropy`` reads), that ``(column, slice)`` pair
is excluded from the score, so the teach closes the measurement. Like ``null_ratio`` this
is a per-column VALUE/NULLS signal, so it rolls into that column's readiness band.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.entropy import stats
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.detectors.loaders import load_documented_dependencies, load_statistics
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject
from dataraum.storage import Column

logger = get_logger(__name__)

# A categorical with more distinct values than this is not a slice dimension (an
# identifier / free text); the Cochran rule already abstains on too-many-tiny-slices,
# so this only bounds the scan, it does not tune the statistic.
_MAX_SLICE_CARDINALITY = 50
# distinct/total above this ⇒ an identifier (distinct ≈ rowcount) → never a slice.
_NEAR_UNIQUE_RATIO = 0.99
# evidence keeps the strongest driving slices only.
_TOP_K_SLICES = 5


class SliceConditionalNullDetector(EntropyDetector):
    """Scores how strongly a column's nulls concentrate in a slice (bias-corrected V)."""

    detector_id = "slice_conditional_null"
    layer = Layer.VALUE
    dimension = Dimension.NULLS
    sub_dimension = SubDimension.SLICE_CONDITIONAL_NULL
    required_analyses = [AnalysisKey.STATISTICS]
    description = "Nulls concentrated in specific slices (Cramér's V of is-null × slice)"

    def load_data(self, context: DetectorContext) -> None:
        """Load the target column's statistical profile (gates can_run)."""
        if context.session is None or context.column_id is None:
            return
        result = load_statistics(
            context.session, context.column_id, context.run_id, base_runs=context.base_runs
        )
        if result is not None:
            context.analysis_results["statistics"] = result

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Emit one column object: max bias-corrected Cramér's V over valid slice dims."""
        if context.duckdb_conn is None or context.table_id is None or context.session is None:
            return []
        session = context.session

        stats_row = self._as_dict(context.get_analysis("statistics"))
        null_count = int(stats_row.get("null_count") or 0)
        # No nulls → nothing can concentrate; a flat-clean column is not entropy.
        if null_count == 0:
            return [self.create_entropy_object(context=context, score=0.0, evidence=[])]

        slice_cols = self._candidate_slice_columns(context, session)
        if not slice_cols:
            return [self.create_entropy_object(context=context, score=0.0, evidence=[])]

        is_null, slice_values = self._read_columns(context, slice_cols)
        documented = load_documented_dependencies(session)

        scored: list[tuple[float, Column]] = []
        for slice_col in slice_cols:
            if frozenset({context.column_id, slice_col.column_id}) in documented:
                continue  # a teach marked this conditional-missingness expected → not entropy
            # Condition only on rows that carry a slice label (drop slice-null rows), so a
            # column's own missingness is never read as a phantom "null slice" — mirrors the
            # gate reference, which drops nulls from the dimension.
            paired = [
                (flag, val)
                for flag, val in zip(is_null, slice_values[slice_col.column_id], strict=True)
                if val is not None
            ]
            distinct = {val for _, val in paired}
            if not (2 <= len(distinct) <= _MAX_SLICE_CARDINALITY):
                continue  # not a categorical slice dimension on the labelled rows
            value = stats.cramers_v([f for f, _ in paired], [v for _, v in paired])
            if value is not None:  # None = Cochran/degenerate abstention, not a 0
                scored.append((value, slice_col))

        scored.sort(key=lambda p: p[0], reverse=True)
        top_score = scored[0][0] if scored else 0.0
        evidence = [
            {
                "pattern": "slice_conditional_null",
                "slice_column": col.column_name,
                "slice_column_id": col.column_id,
                "cramers_v": round(value, 4),
                "null_count": null_count,
            }
            for value, col in scored[:_TOP_K_SLICES]
        ]
        return [self.create_entropy_object(context=context, score=top_score, evidence=evidence)]

    def _candidate_slice_columns(self, context: DetectorContext, session: Session) -> list[Column]:
        """Sibling categoricals worth scanning as slice dimensions.

        A SOFT pre-filter: it drops identifiers (by name / near-unique cardinality) and
        clearly-too-granular columns when a profile says so, to avoid scanning free text.
        The authoritative cardinality gate is the actual distinct count on the scanned,
        slice-labelled rows (in ``detect``) — so a column with no profile is still kept,
        not silently excluded.
        """
        columns = list(
            session.execute(select(Column).where(Column.table_id == context.table_id))
            .scalars()
            .all()
        )
        candidates: list[Column] = []
        for col in columns:
            if col.column_id == context.column_id:
                continue
            name = (col.column_name or "").lower()
            if name == "id" or name.endswith("_id"):
                continue  # identifiers carry no slice semantics
            srow = self._as_dict(
                load_statistics(session, col.column_id, context.run_id, context.base_runs)
            )
            distinct = srow.get("distinct_count")
            cardinality = srow.get("cardinality_ratio")
            if distinct is not None and int(distinct) > _MAX_SLICE_CARDINALITY:
                continue  # known too-granular for a slice dimension — don't scan it
            if cardinality is not None and cardinality > _NEAR_UNIQUE_RATIO:
                continue  # distinct ≈ rowcount ⇒ an identifier
            candidates.append(col)
        return candidates

    def _read_columns(
        self, context: DetectorContext, slice_cols: list[Column]
    ) -> tuple[list[bool], dict[str, list[Any]]]:
        """Read the target's is-null mask + every slice column in ONE row-aligned scan."""
        from dataraum.core.duckdb_naming import schema_for_layer
        from dataraum.server.storage import LAKE_CATALOG_ALIAS

        def q(name: str) -> str:
            return '"' + name.replace('"', '""') + '"'

        select_list = ", ".join([q(context.column_name)] + [q(c.column_name) for c in slice_cols])
        table_fqn = f"{LAKE_CATALOG_ALIAS}.{schema_for_layer('typed')}.{q(context.table_name)}"
        rows = context.duckdb_conn.execute(f"SELECT {select_list} FROM {table_fqn}").fetchall()
        is_null = [row[0] is None for row in rows]
        slice_values: dict[str, list[Any]] = {}
        for idx, col in enumerate(slice_cols, start=1):
            slice_values[col.column_id] = [row[idx] for row in rows]
        return is_null, slice_values

    @staticmethod
    def _as_dict(stats_row: Any) -> dict[str, Any]:
        """Normalize a ColumnProfile or dict to a plain dict of the fields we read."""
        if stats_row is None:
            return {}
        if isinstance(stats_row, dict):
            return stats_row
        return {
            "null_count": getattr(stats_row, "null_count", 0),
            "distinct_count": getattr(stats_row, "distinct_count", None),
            "cardinality_ratio": getattr(stats_row, "cardinality_ratio", None),
        }
