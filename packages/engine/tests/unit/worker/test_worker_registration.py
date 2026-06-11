"""Every activity a workflow executes is registered on the bundled worker.

Workflows call activities by NAME string, so a phase wired into a workflow
chain but missing from ``worker_activities()`` fails only at runtime — a
``NotFoundError`` mid-workflow on a live run. That is exactly how the DAT-491
``aggregation_lineage`` miss surfaced: the phase, activity wrapper, and
pipeline.yaml entry all existed, but ``main.py``'s registration list was never
extended, and the stub-based progress test couldn't see it (it registers its
own stubs).

The executed-name set is extracted from ``workflows.py``'s AST (every string
literal passed to ``workflow.execute_activity``) plus the loop-driven phase
constants, so a newly added chain step is picked up without editing this test.
"""

from __future__ import annotations

import ast
import inspect

from dataraum.worker import workflows as workflows_mod
from dataraum.worker.activities import PhaseActivities
from dataraum.worker.main import worker_activities
from dataraum.worker.workflows import _SESSION_PHASE_ORDER, _SESSION_VALUE_PHASE_ORDER


def _executed_activity_names() -> set[str]:
    """Activity names the workflow bodies execute, read off the source AST."""
    tree = ast.parse(inspect.getsource(workflows_mod))
    names: set[str] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "execute_activity"
            and node.args
        ):
            first = node.args[0]
            if isinstance(first, ast.Constant) and isinstance(first.value, str):
                names.add(first.value)
    # Loop-driven steps pass a variable, not a literal — their domains are the
    # phase-order constants the loops iterate.
    names.update(_SESSION_PHASE_ORDER)
    names.update(_SESSION_VALUE_PHASE_ORDER)
    return names


def test_every_executed_activity_is_registered() -> None:
    # Names live in @activity.defn metadata; the manager is never touched.
    acts = PhaseActivities(manager=None)  # type: ignore[arg-type]
    registered = {
        getattr(fn, "__temporal_activity_definition").name  # noqa: B009 — dunder set by temporalio
        for fn in worker_activities(acts)
    }
    executed = _executed_activity_names()
    assert executed, "AST extraction found no executed activities — extractor broken"
    missing = executed - registered
    assert not missing, (
        f"workflow chains execute activities the worker never registers: {sorted(missing)} "
        "— add them to worker_activities() in worker/main.py"
    )
