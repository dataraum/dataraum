"""Guard the begin_session value layer is wired, not dormant (DAT-403/536).

The value chain (slicing → aggregation_lineage → correlations) was code-complete
but DORMANT (declared in pipeline.yaml, never executed by any workflow) before
DAT-403 revived them; DAT-536 then removed the slice-materialization phases
(slicing_view / slice_analysis / temporal_slice_analysis) — the
structural_reconciliation substrate is now aggregated inline in
aggregation_lineage. This pins the two invariants that keep the chain live:

1. ``_SESSION_VALUE_PHASE_ORDER`` (what ``beginSessionWorkflow`` executes after
   ``enriched_views``) is the agreed value chain, in dependency order.
2. Every name in it resolves to a registered phase class — so the workflow's
   ``execute_activity(name)`` → ``run_session_phase`` → ``get_phase_class(name)``
   dispatch can never silently no-op on a typo or a re-dormancy regression.
"""

from __future__ import annotations

from dataraum.pipeline.registry import get_phase_class
from dataraum.worker.workflows import _SESSION_VALUE_PHASE_ORDER


def test_value_phase_order_is_the_agreed_chain() -> None:
    assert _SESSION_VALUE_PHASE_ORDER == (
        "slicing",
        # DAT-823: catalogue-semantics authoring (table readings + column
        # concepts) over the composed catalogue — after slicing resolves the
        # dimension identities, before the dimension_hierarchies conform judge.
        "catalogue_semantics",
        # DAT-537: deterministic g3 FD pass over slicing's catalog — drill-down
        # hierarchies + aliases. Reads slicing; consumed by the answer agent (DAT-538).
        "dimension_hierarchies",
        # DAT-491/536: lineage aggregates each fact's enriched view inline and
        # reconciles the per-period sums — no slice-materialization phases precede it.
        "aggregation_lineage",
        "correlations",
    )


def test_driver_rankings_is_not_in_the_value_chain() -> None:
    """driver_rankings runs AFTER session_detect, not in the value layer (DAT-543).

    It reads the pool-RESOLVED ``temporal_behavior`` (session_detect overturns the
    table-agent stock claim via the structural witness) to pick its target function
    and persist ``target_type``. Leaving it in the value chain — before detect — made
    it rank a pool-flipped balance against the wrong target and persist a stale
    ``stock`` the graph/answer agents then trusted. It is executed by an explicit
    post-detect activity in ``beginSessionWorkflow``, so it must NOT sit here.
    """
    assert "driver_rankings" not in _SESSION_VALUE_PHASE_ORDER
    # still a registered, runnable phase — just dispatched later in the spine.
    assert get_phase_class("driver_rankings") is not None


def test_every_value_phase_resolves_in_the_registry() -> None:
    """Each wired value phase has a registered class the dispatch can run."""
    for phase in _SESSION_VALUE_PHASE_ORDER:
        cls = get_phase_class(phase)
        assert cls is not None, f"value phase '{phase}' is not registered — dispatch would no-op"
        assert cls().name == phase
