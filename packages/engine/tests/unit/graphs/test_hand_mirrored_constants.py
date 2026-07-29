"""Engine side of the hand-mirrored cockpit↔engine graph constants (DAT-671 R6).

Two literals are restated by hand in the cockpit because there is no shared
module across the language boundary — the same discipline ``worker/contracts.py``
carries for the Temporal wire models:

* ``_PART_OF_MAX_DEPTH`` — both packages walk the SAME bounded recursive CTE over
  the SAME ``og_concept_edges`` view (PGQ ``MATCH`` is fixed-depth, ADR-0021), so
  two different caps mean two different ancestries of one graph: a capability
  difference that traces to a PATH, not to the data (ADR-0024 d2).
* ``AXIS_KEY_ALL`` — a SERVED VALUE. The cockpit reads additivity rows straight
  off ``current_metric_axis_additivity`` and must recognise the class row to
  resolve most-specific-first; a drift turns every class-level verdict into an
  unrecognised concrete axis, so the drill finds no verdict for an axis that has
  one.

The cockpit pins both against real engine-served rows
(``concept-graph-load.integration.test.ts``). Nothing pinned the ENGINE side —
so an engine-first change could land green and only surface as a wrong answer in
the UI. This closes that direction: the engine's own suite reads the cockpit
sources and fails on drift.

Reading a sibling package's source in a test is the established shape here — the
cockpit's own fixture reads the engine's generated ``schema_graph.sql``
(``packages/cockpit/src/test/fixture-workspace.ts``). The path is resolved
relative to this file, so it holds in a worktree and in the eval repo's vendored
copy alike; a missing cockpit is a broken checkout and fails loudly rather than
skipping.
"""

from __future__ import annotations

import re
from pathlib import Path

from dataraum.graphs.additivity_db_models import AXIS_KEY_ALL
from dataraum.graphs.context_reads import _PART_OF_MAX_DEPTH

_COCKPIT_TOOLS = Path(__file__).resolve().parents[4] / "cockpit" / "src" / "tools"


def _mirrored_literals(file_name: str, const_name: str) -> list[str]:
    """Every ``const <const_name> = <literal>;`` in one cockpit tool module.

    Returns the raw literals (a list, so a duplicated declaration is visible as a
    drift rather than silently taking the first).
    """
    source = (_COCKPIT_TOOLS / file_name).read_text()
    return re.findall(
        rf"^(?:export )?const {const_name} = (.+?);$",
        source,
        flags=re.MULTILINE,
    )


def test_part_of_max_depth_matches_the_cockpit_mirror() -> None:
    mirrored = _mirrored_literals("concept-graph-load.ts", "PART_OF_MAX_DEPTH")
    assert mirrored == [str(_PART_OF_MAX_DEPTH)], (
        f"part_of ancestry depth drifted: engine _PART_OF_MAX_DEPTH="
        f"{_PART_OF_MAX_DEPTH}, cockpit concept-graph-load.ts={mirrored}"
    )


def test_axis_key_all_matches_both_cockpit_mirrors() -> None:
    # Both cockpit readers of the sentinel — the concept-graph load path (which
    # re-exports it) and the drill, which keeps its own copy.
    for file_name in ("concept-graph.ts", "drill-axes.ts"):
        mirrored = _mirrored_literals(file_name, "AXIS_KEY_ALL")
        assert mirrored == [f'"{AXIS_KEY_ALL}"'], (
            f"class-row sentinel drifted: engine AXIS_KEY_ALL={AXIS_KEY_ALL!r}, "
            f"cockpit {file_name}={mirrored}"
        )
