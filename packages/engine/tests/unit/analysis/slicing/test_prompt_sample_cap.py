"""The slicing prompt's per-column value samples are capped (DAT-671 census).

``analysis/slicing/agent.py`` renders ``context_data["tables"]`` straight into
the ``tables_json`` prompt input, and each column there carries the profiler's
FULL stored ``top_values`` (up to the profiler's top-K) — the census's single
largest surviving uncapped data-value site. ``context_data["tables"]`` is also
read AFTER the LLM call by ``slicing_phase.py``'s deterministic post-processing
(``score_axis``'s bucket_counts, the ``distinct_values`` persistence fallback),
so the fix caps a PROMPT-ONLY copy (``_capped_tables_for_prompt``) rather than
the shared structure itself — capping that in place would silently shrink a
real relevance score and the persisted evidence for an un-ranked column.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from dataraum.analysis.slicing.agent import SlicingAgent, _capped_tables_for_prompt


def _table(top_values: list[dict]) -> dict:
    return {
        "table_name": "journal_lines",
        "table_id": "tbl_jl",
        "columns": [
            {
                "column_name": "cost_center",
                "column_id": "col_cc",
                "top_values": top_values,
            }
        ],
    }


def _values(n: int) -> list[dict]:
    return [{"value": f"v{i:02d}", "count": n - i, "percentage": 1.0} for i in range(n)]


def test_capped_tables_for_prompt_bounds_top_values() -> None:
    tables = [_table(_values(50))]

    capped = _capped_tables_for_prompt(tables, limit=10)

    (table,) = capped
    (col,) = table["columns"]
    assert len(col["top_values"]) == 10
    # Head of the stored order, not a random slice — count/percentage intact.
    assert col["top_values"][0] == {"value": "v00", "count": 50, "percentage": 1.0}


def test_capped_tables_for_prompt_does_not_mutate_the_source() -> None:
    """The shared context_data structure survives untouched.

    ``slicing_phase.py``'s deterministic scorer and persistence fallback read
    the SAME ``context_data["tables"]`` after the LLM call — this is the
    invariant that makes capping only a COPY (not the source) correct.
    """
    tables = [_table(_values(50))]

    _capped_tables_for_prompt(tables, limit=10)

    assert len(tables[0]["columns"][0]["top_values"]) == 50


def test_capped_tables_for_prompt_survives_a_column_with_no_top_values() -> None:
    tables = [_table([])]

    capped = _capped_tables_for_prompt(tables, limit=10)

    assert capped[0]["columns"][0]["top_values"] == []


def test_analyze_serializes_the_capped_copy_into_tables_json() -> None:
    """The real seam: ``analyze`` must actually call the capped builder,
    not just have it sitting unused — a spy on the rendered prompt input."""
    config = MagicMock()
    config.features.slicing_analysis.enabled = True
    config.features.slicing_analysis.effort = "medium"
    config.privacy.max_sample_values = 3
    config.limits.max_output_tokens_per_request = 8192

    provider = MagicMock()
    provider.get_model_for_tier.return_value = "claude-test"
    provider.converse.return_value.unwrap.return_value = MagicMock(
        content=json.dumps({"recommendations": [], "time_columns": []}),
        stop_reason="end_turn",
        output_tokens=1,
    )
    renderer = MagicMock()
    captured: dict[str, object] = {}

    def render_split(_name: str, context: dict) -> tuple[str, str]:
        captured.update(context)
        return "sys", "user"

    renderer.render_split.side_effect = render_split

    agent = SlicingAgent(config=config, provider=provider, prompt_renderer=renderer)
    context_data = {"tables": [_table(_values(50))], "constraints": {}}

    agent.analyze(session=MagicMock(), table_ids=["tbl_jl"], context_data=context_data)

    rendered_tables = json.loads(captured["tables_json"])
    assert len(rendered_tables[0]["columns"][0]["top_values"]) == 3
    # The original context_data the caller holds is untouched.
    assert len(context_data["tables"][0]["columns"][0]["top_values"]) == 50
