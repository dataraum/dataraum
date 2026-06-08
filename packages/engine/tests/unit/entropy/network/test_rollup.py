"""Unit tests for the deterministic entropy rollup (noisy-OR over network.yaml).

These replace test_cpts.py / test_inference.py. They assert the structural
properties we rely on — monotonicity, no prior leakage, correct banding, and
meaningful fix priorities — against the real shipped network. (Cross-detector
compounding was removed with the computational composites in the DAT-442
flat-table move; severity now lives per-detector in the loss table.)
Recall/precision against ground truth is proven in dataraum-eval, not here.
"""

from __future__ import annotations

import pytest

from dataraum.entropy.network.config import get_network_config
from dataraum.entropy.network.rollup import (
    compute_priorities,
    intent_nodes,
    readiness_from_risk,
    roll_up,
    topo_order,
)


@pytest.fixture(scope="module")
def config():
    return get_network_config()


def _root_names(config) -> list[str]:
    """Roots = nodes with no incoming edge (no parents)."""
    children = {edge.child for edge in config.edges}
    return [name for name in config.nodes if name not in children]


def test_topo_order_is_complete_and_acyclic(config):
    order = topo_order(config)
    assert set(order) == set(config.nodes)
    pos = {n: i for i, n in enumerate(order)}
    for edge in config.edges:
        assert pos[edge.parent] < pos[edge.child], "parent must precede child"


def test_no_prior_leakage_for_unobserved_nodes(config):
    """A node with no observed parents must be absent — never a phantom risk."""
    risk = roll_up(config, {"type_fidelity": 0.8})
    # Only type_fidelity and its reachable descendants appear.
    assert "type_fidelity" in risk
    assert "naming_clarity" not in risk  # unobserved sibling root, no evidence
    # query_intent has type_fidelity as a parent, so it resolves.
    assert "query_intent" in risk


def test_observed_root_keeps_raw_score(config):
    risk = roll_up(config, {"type_fidelity": 0.71})
    assert risk["type_fidelity"] == pytest.approx(0.71)


def test_monotonic_in_evidence(config):
    # time_role feeds both query and aggregation intents directly.
    low = roll_up(config, {"time_role": 0.4})
    high = roll_up(config, {"time_role": 0.9})
    assert high["aggregation_intent"] >= low["aggregation_intent"]
    assert high["query_intent"] >= low["query_intent"]


def test_low_band_evidence_is_dropped(config):
    """Scores in the clean band (<= low_upper) contribute nothing — precision."""
    low_upper = config.discretization.low_upper
    risk = roll_up(config, {"time_role": low_upper})  # exactly at the floor
    assert "time_role" not in risk
    assert "aggregation_intent" not in risk  # no above-floor evidence to resolve it


def test_risk_stays_in_unit_interval(config):
    saturated = dict.fromkeys(_root_names(config), 1.0)
    risk = roll_up(config, saturated)
    assert all(0.0 <= v <= 1.0 for v in risk.values())


def test_readiness_banding(config):
    disc = config.discretization
    assert readiness_from_risk(0.1, disc.low_upper, disc.medium_upper) == "ready"
    assert readiness_from_risk(0.45, disc.low_upper, disc.medium_upper) == "investigate"
    assert readiness_from_risk(0.8, disc.low_upper, disc.medium_upper) == "blocked"


def test_clean_evidence_reads_ready(config):
    """All-low detector scores leave intents unresolved — i.e. 'ready' (precision).

    Low-band scores are gated out, so no intent gets evidence; the caller treats
    an absent intent as ready (its default).
    """
    clean = dict.fromkeys(_root_names(config), 0.05)
    risk = roll_up(config, clean)
    for intent in intent_nodes(config):
        assert intent not in risk, f"{intent} should not resolve from all-clean evidence"


def test_priorities_rank_strongest_driver_first(config):
    scores = {"type_fidelity": 0.8, "naming_clarity": 0.4}
    priorities = compute_priorities(config, scores)
    assert priorities, "expected at least one prioritised fix"
    assert priorities[0].impact_delta >= priorities[-1].impact_delta
    # Fixing a node must not be credited with negative impact.
    assert all(p.impact_delta >= 0.0 for p in priorities)
    # Clean nodes are not prioritised.
    assert all(p.node in scores for p in priorities)
