"""Dimensional entropy detector — undocumented cross-column dependency (NMI).

The measurement is normalized mutual information (``stats.nmi``) between column pairs:
``NMI(X;Y) = MI / sqrt(H(X)·H(Y))`` in ``[0, 1]``. ``1.0`` = one column determines the
other (a strong dependency); ``0.0`` = independent. A column pair that share structure
the data dictionary does NOT record is *undocumented entropy* a teach can close
(``document_business_rule`` → ``ConfigOverlay(type='expected_dependency')`` → the pair
is excluded from the score). The classic case is the double-entry mutex: ``debit ≠ 0``
exactly when ``credit = 0`` — NMI of their non-zero indicators ≈ 1.0 until a teach marks
it expected.

No discretization, no pattern counts, no magic normalization. Each column becomes a
discrete label sequence — numerics as a non-zero indicator (the structural carrier of a
mutex / conditional-presence dependency lives in the zero pattern, not the magnitude),
low-cardinality categoricals as their raw value — and NMI runs over the aligned
sequences. The detector emits ONE table-scoped object whose score is the max NMI over
*undocumented* pairs. Identifiers and near-unique / high-cardinality columns are excluded
(finite-sample NMI inflates on near-unique labels). Derived columns are NOT excluded:
the indicator measures structural co-presence, orthogonal to the value formula
``derived_value`` owns, and an always-present derived column self-excludes (constant
indicator → NMI 0).

This measures INTRINSIC structure — the mutex is real in clean data too — so its
calibration bar is teach-closure (documenting the pair drops the score), not injection
recall.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.entropy import stats
from dataraum.entropy.detectors.base import DetectorContext, EntropyDetector
from dataraum.entropy.detectors.loaders import (
    load_documented_dependencies,
    load_semantic,
    load_statistics,
)
from dataraum.entropy.dimensions import Dimension, Layer, SubDimension
from dataraum.entropy.models import EntropyObject
from dataraum.storage import Column

logger = get_logger(__name__)

# resolved_type prefixes treated as numeric → folded to a non-zero indicator.
_NUMERIC_TYPES = (
    "DECIMAL",
    "NUMERIC",
    "FLOAT",
    "DOUBLE",
    "REAL",
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "SMALLINT",
    "TINYINT",
)

# distinct/total above this ⇒ an identifier (distinct ≈ rowcount) → excluded entirely.
_NEAR_UNIQUE_RATIO = 0.99
# a categorical sparser than this gives an unstable (finite-sample-inflated) NMI → excluded.
# Numerics are exempt: their indicator is binary regardless of value cardinality.
_HIGH_CARD_RATIO = 0.5
# evidence keeps the strongest pairs only.
_TOP_K_PAIRS = 5

_NUMERIC = "numeric"
_CATEGORICAL = "categorical"


def _nonzero(value: Any) -> int:
    """Non-zero indicator: 1 if the cell is present and ≠ 0, else 0.

    A mutex (``debit ≠ 0`` ⇔ ``credit = 0``) or a conditional-presence dependency lives in
    the zero pattern, not the magnitude — so NMI runs on discrete indicators with no
    arbitrary value binning. Missing / unparseable cells fold to 0 (absent).
    """
    if value is None:
        return 0
    try:
        return 1 if float(value) != 0.0 else 0
    except TypeError, ValueError:
        return 0


class DimensionalEntropyDetector(EntropyDetector):
    """Scores the strongest undocumented cross-column dependency on a table via NMI."""

    detector_id = "dimensional_entropy"
    layer = Layer.SEMANTIC
    dimension = Dimension.DIMENSIONAL
    sub_dimension = SubDimension.CROSS_COLUMN_PATTERNS
    scope = "table"
    # required_analyses inherits the empty base default: NMI reads typed VALUES directly via
    # duckdb, so the detector has no slice/analysis dependency (was [SLICE_VARIANCE]).
    description = "Undocumented cross-column dependency (normalized mutual information)"

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        """Emit one table object: max NMI over undocumented candidate-column pairs."""
        if context.duckdb_conn is None or context.table_id is None or context.session is None:
            logger.debug("dimensional_entropy: no duckdb/table/session context — skipping")
            return []
        session = context.session  # narrowed; the metadata helpers take it explicitly

        candidates = self._candidate_columns(context, session)
        if len(candidates) < 2:
            return []

        labels = self._read_labels(context, candidates)
        documented = load_documented_dependencies(session)

        scored: list[tuple[float, Column, Column]] = []
        for i in range(len(candidates)):
            for j in range(i + 1, len(candidates)):
                col_a, col_b = candidates[i][0], candidates[j][0]
                if frozenset({col_a.column_id, col_b.column_id}) in documented:
                    continue  # a teach marked this expected → not entropy
                value = stats.nmi(labels[col_a.column_id], labels[col_b.column_id])
                scored.append((value, col_a, col_b))

        scored.sort(key=lambda p: p[0], reverse=True)
        top_score = scored[0][0] if scored else 0.0  # all pairs documented ⇒ clean (0.0)
        evidence = [
            {
                "pattern": "cross_column_dependency",
                "columns": [a.column_name, b.column_name],
                "column_ids": [a.column_id, b.column_id],
                "nmi": round(value, 4),
            }
            for value, a, b in scored[:_TOP_K_PAIRS]
        ]
        return [self.create_entropy_object(context=context, score=top_score, evidence=evidence)]

    def _candidate_columns(
        self, context: DetectorContext, session: Session
    ) -> list[tuple[Column, str]]:
        """Columns eligible for NMI, tagged numeric (→ indicator) or categorical (→ label)."""
        columns = list(
            session.execute(select(Column).where(Column.table_id == context.table_id))
            .scalars()
            .all()
        )
        candidates: list[tuple[Column, str]] = []
        for col in columns:
            semantic = (
                load_semantic(session, col.column_id, context.run_id, context.base_runs) or {}
            )
            stats_row = (
                load_statistics(session, col.column_id, context.run_id, context.base_runs) or {}
            )
            if self._is_excluded(col, semantic, stats_row):
                continue
            resolved = (col.resolved_type or "").upper()
            if resolved.startswith(_NUMERIC_TYPES):
                candidates.append((col, _NUMERIC))
                continue
            cardinality = stats_row.get("cardinality_ratio")
            if cardinality is not None and cardinality > _HIGH_CARD_RATIO:
                continue  # too sparse for a stable categorical NMI
            candidates.append((col, _CATEGORICAL))
        return candidates

    def _is_excluded(
        self,
        col: Column,
        semantic: dict[str, Any],
        stats_row: dict[str, Any],
    ) -> bool:
        """Identifiers carry no cross-column dependency to score.

        Derived columns are NOT excluded: NMI runs on non-zero INDICATORS, which
        measure structural co-presence (a mutex / conditional presence), orthogonal
        to the *value* formula derived_value owns. An always-present derived column
        (e.g. a signed net_amount that is never 0) has a constant indicator → NMI 0,
        so it self-excludes; one with a real zero-pattern dependency is a genuine
        signal a teach can close. (Excluding them dropped the debit/credit mutex,
        since the correlations dedup flags debit = net_amount + credit.)
        """
        name = (col.column_name or "").lower()
        if name == "id" or name.endswith("_id"):
            return True
        if semantic.get("semantic_role") in {"key", "foreign_key"}:
            return True
        cardinality = stats_row.get("cardinality_ratio")
        if cardinality is not None and cardinality > _NEAR_UNIQUE_RATIO:
            return True  # distinct ≈ rowcount ⇒ an identifier
        return False

    def _read_labels(
        self, context: DetectorContext, candidates: list[tuple[Column, str]]
    ) -> dict[str, list[Any]]:
        """Read all candidate columns in ONE scan so the label sequences stay row-aligned."""
        from dataraum.core.duckdb_naming import schema_for_layer
        from dataraum.server.storage import LAKE_CATALOG_ALIAS

        def q(name: str) -> str:
            return '"' + name.replace('"', '""') + '"'

        select_list = ", ".join(q(col.column_name) for col, _ in candidates)
        table_fqn = f"{LAKE_CATALOG_ALIAS}.{schema_for_layer('typed')}.{q(context.table_name)}"
        sql = f"SELECT {select_list} FROM {table_fqn}"
        rows = context.duckdb_conn.execute(sql).fetchall()
        labels: dict[str, list[Any]] = {}
        for idx, (col, kind) in enumerate(candidates):
            column_values = [row[idx] for row in rows]
            if kind == _NUMERIC:
                labels[col.column_id] = [_nonzero(v) for v in column_values]
            else:
                labels[col.column_id] = column_values  # raw label; None is its own category
        return labels
