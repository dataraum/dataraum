"""The prompt sample budget is applied by the two semantic agents (DAT-890).

The profiler stores ``top_k_values`` (200) per column; ``privacy.max_sample_values``
(10) is what may reach a PROMPT. The deleted ``llm/privacy.py`` sampler served
the full stored set on its non-sensitive branch, so these two agents — and only
these two, every other builder capped — shipped ~20x their budget: a measured
8,722 raw values in one ``column_annotation`` prompt, 88% of its bytes.

These tests assert on the ASSEMBLED prompt, not on the helper, because that is
where the defect was observable and where a future regression would land.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from dataraum.analysis.semantic.agent import SemanticAgent
from dataraum.analysis.semantic.column_agent import ColumnAnnotationAgent
from dataraum.analysis.semantic.utils import prompt_samples
from dataraum.analysis.statistics.models import ColumnProfile, ValueCount
from dataraum.core.models.base import ColumnRef

_STORED = 200  # phases/statistics.yaml: top_k_values
_BUDGET = 10  # llm/config.yaml: privacy.max_sample_values


def _profile(table: str, column: str, *, stored: int = _STORED) -> ColumnProfile:
    return ColumnProfile(
        column_id=f"{table}.{column}",
        column_ref=ColumnRef(table_name=table, column_name=column),
        profiled_at=datetime.now(UTC),
        total_count=1000,
        null_count=0,
        distinct_count=stored,
        null_ratio=0.0,
        cardinality_ratio=0.5,
        top_values=[ValueCount(value=-3900.0 - i, count=2, percentage=0.2) for i in range(stored)],
    )


def test_prompt_samples_applies_the_budget() -> None:
    profiles = [_profile("journal_lines", "net_amount"), _profile("journal_lines", "debit")]
    samples = prompt_samples(profiles, limit=_BUDGET)

    assert set(samples) == {("journal_lines", "net_amount"), ("journal_lines", "debit")}
    assert all(len(v) == _BUDGET for v in samples.values())
    # The cap takes the TOP values, in stored order — not a random slice.
    assert samples[("journal_lines", "net_amount")][0] == -3900.0


def test_prompt_samples_survives_an_unprofiled_column() -> None:
    """No stored top values serves an empty list — absence stays visible."""
    p = _profile("orders", "id")
    p.top_values = None
    assert prompt_samples([p], limit=_BUDGET) == {("orders", "id"): []}


def test_column_annotation_tables_json_carries_at_most_the_budget() -> None:
    """The per-column agent's assembled schema block honours the cap."""
    agent = ColumnAnnotationAgent.__new__(ColumnAnnotationAgent)
    profiles = [_profile("journal_lines", c) for c in ("debit", "credit", "net_amount")]

    tables_json = agent._build_tables_json(profiles, prompt_samples(profiles, limit=_BUDGET))

    (table,) = tables_json
    assert [c["column_name"] for c in table["columns"]] == ["debit", "credit", "net_amount"]
    for col in table["columns"]:
        assert len(col["sample_values"]) <= _BUDGET
    # The whole point is prompt SIZE: 3 columns x 200 stored values is what made
    # this prompt 88% raw corpus bytes.
    assert len(json.dumps(tables_json)) < 1500


def test_semantic_per_table_tables_json_carries_at_most_the_budget() -> None:
    """The per-table agent shares the cut — its prompt was 72% sample bytes."""
    agent = SemanticAgent.__new__(SemanticAgent)
    profiles = [_profile("journal_lines", c) for c in ("debit", "net_amount")]

    tables_json = agent._build_tables_json(profiles, prompt_samples(profiles, limit=_BUDGET))

    (table,) = tables_json
    for col in table["columns"]:
        assert len(col["sample_values"]) <= _BUDGET
