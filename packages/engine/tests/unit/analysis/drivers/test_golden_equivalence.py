"""DAT-580 golden characterization — pin ``discover_drivers`` output across the port.

The arrow→polars + int-code port (criterion/tree contract flip, bincount entity
aggregation) must not change WHAT the driver engine finds. This test captures the full
:class:`DriverRanking` for six deterministic scenarios — one per code path the port
touches — and asserts equality against a committed golden:

  * **flow / ratio row-wise** — the plain DAT-545 null + ``build_codes`` encoding.
  * **flow / ratio entity-grain** — the cluster collapse (``_collapse_to_entity``) + ICC
    factorize, the ε-sensitive aggregation path.
  * **flow two-driver (high ICC)** — the within-entity residual (``.over``-window port).
  * **two-entity** — N=2 home-grain routing (``_home_grain_partition``).

Comparator (per the refinement, polars#5325 — polars summation can be MORE accurate, so
bitwise equality with pandas is the wrong bar):

  * **structural fields exact** — dim names + order, grain/entity routing, driver paths,
    slice values, support counts, tree shape.
  * **floats `np.allclose(atol=1e-7)`** — gains, slice effects.
  * **p-values within one permutation quantum** ``1/(1+n_perm)`` — an ε gain wobble can
    flip a single ``>=`` in the null; benign unless it crosses α, which the structural
    ``ranked_dimensions`` assertion catches.

The committed golden was captured on the pandas baseline (before the port) and has
NOT been regenerated since — the arrow→polars code passing against it is the equivalence
proof. Only regenerate on a deliberate, reviewed behavior change, with
``DAT580_REGEN=1 uv run pytest .../test_golden_equivalence.py``.
"""

from __future__ import annotations

import json
import os

# Precautionary: all float aggregation in the port goes through numpy bincount (thread-
# independent), so this isn't strictly required — but it pins any future polars-side
# reduction to a single thread. Must precede the engine's first polars import.
os.environ.setdefault("POLARS_MAX_THREADS", "1")

from pathlib import Path  # noqa: E402
from typing import Any  # noqa: E402

import numpy as np  # noqa: E402
import pytest  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402

from dataraum.analysis.drivers.models import DriverNode, DriverRanking, Measure  # noqa: E402
from dataraum.analysis.drivers.processor import discover_drivers  # noqa: E402

from .conftest import (  # noqa: E402
    ALL_DIMS,
    CL_DIMS,
    CL_ENTITY,
    CL_RATIO_DIMS,
    RATIO_DIMS,
    TE_CUST,
    TE_DIMS,
    TE_PROD,
    TWO_DRIVER_DIMS,
    make_clustered_corpus,
    make_clustered_ratio_corpus,
    make_clustered_two_driver_corpus,
    make_corpus,
    make_ratio_corpus,
    make_two_entity_corpus,
)
from .test_grain_e2e import _seed_catalog, _seed_ratio_catalog, _write_view

GOLDEN = Path(__file__).parent / "golden" / "driver_rankings.json"
N_PERM = 200
FLOW = Measure(target_type="flow", column="measure")
RATIO = Measure(target_type="ratio", numerator="numerator", denominator="denominator")

# (id, kind, dims, corpus_fn, gen_seed, run_seed, measure, cluster_keys)
# kind: "flow" seeds a flow catalog, "ratio" a ratio catalog.
SCENARIOS = [
    ("flow_row_wise", "flow", ALL_DIMS, make_corpus, 1, 0, FLOW, None),
    ("ratio_row_wise", "ratio", RATIO_DIMS, make_ratio_corpus, 1, 0, RATIO, None),
    ("flow_entity_grain", "flow", CL_DIMS, make_clustered_corpus, 2, 0, FLOW, [CL_ENTITY]),
    (
        "ratio_entity_grain",
        "ratio",
        CL_RATIO_DIMS,
        make_clustered_ratio_corpus,
        0,
        0,
        RATIO,
        [CL_ENTITY],
    ),
    (
        "flow_two_driver_high_icc",
        "flow",
        TWO_DRIVER_DIMS,
        make_clustered_two_driver_corpus,
        0,
        0,
        FLOW,
        [CL_ENTITY],
    ),
    ("two_entity", "flow", TE_DIMS, make_two_entity_corpus, 0, 0, FLOW, [TE_CUST, TE_PROD]),
]


def _node_to_dict(node: DriverNode | None) -> dict[str, Any] | None:
    if node is None:
        return None
    return {
        "dimension": node.dimension,
        "gain": node.gain,
        "p_value": node.p_value,
        "support": node.support,
        "slices": [[s.dimension, s.value, s.effect, s.support] for s in node.slices],
        "children": [[v, _node_to_dict(c)] for v, c in node.children],
    }


def _ranking_to_dict(r: DriverRanking) -> dict[str, Any]:
    return {
        "measure": r.measure,
        "target_type": r.target_type,
        "n_rows": r.n_rows,
        "grain": r.grain,
        "entity": r.entity,
        "ranked_dimensions": [[d, g] for d, g in r.ranked_dimensions],
        "driver_paths": [list(p) for p in r.driver_paths],
        "interesting_slices": [
            [s.dimension, s.value, s.effect, s.support] for s in r.interesting_slices
        ],
        "secondary_dimensions": [
            [s.dimension, s.gain, s.grain, s.entity] for s in r.secondary_dimensions
        ],
        "root": _node_to_dict(r.root),
    }


def _run_scenario(session: Session, duck: Any, scn: tuple) -> dict[str, Any]:
    _id, kind, dims, corpus_fn, gen_seed, run_seed, measure, cluster_keys = scn
    seed_args = {"dims": dims} if dims is not None else {}
    tid = (_seed_ratio_catalog if kind == "ratio" else _seed_catalog)(session, **seed_args)
    _write_view(duck, corpus_fn(np.random.default_rng(gen_seed)))
    ranking = discover_drivers(
        session,
        duckdb_conn=duck,
        fact_table_id=tid,
        run_id="session-run-1",
        measure=measure,
        cluster_keys=cluster_keys,
        n_perm=N_PERM,
        seed=run_seed,
    )
    return _ranking_to_dict(ranking)


# --- comparator -----------------------------------------------------------------

ATOL = 1e-7
P_QUANTUM = 1.0 / (1 + N_PERM) + 1e-9


def _close(a: float, b: float, atol: float = ATOL) -> bool:
    return bool(np.isclose(a, b, atol=atol, rtol=1e-6))


def _slices_match(a: list, g: list, path: str) -> None:
    assert len(a) == len(g), f"{path}: slice count {len(a)} != {len(g)}"
    for i, (sa, sg) in enumerate(zip(a, g, strict=True)):
        assert sa[0] == sg[0] and sa[1] == sg[1], f"{path}[{i}]: (dim,value) {sa[:2]} != {sg[:2]}"
        assert sa[3] == sg[3], f"{path}[{i}]: support {sa[3]} != {sg[3]}"
        assert _close(sa[2], sg[2]), f"{path}[{i}]: effect {sa[2]} !~ {sg[2]}"


def _node_match(a: dict | None, g: dict | None, path: str) -> None:
    assert (a is None) == (g is None), f"{path}: one node is None"
    if a is None or g is None:
        return
    assert a["dimension"] == g["dimension"], f"{path}: dim {a['dimension']} != {g['dimension']}"
    assert a["support"] == g["support"], f"{path}: support {a['support']} != {g['support']}"
    assert _close(a["gain"], g["gain"]), f"{path}: gain {a['gain']} !~ {g['gain']}"
    assert _close(a["p_value"], g["p_value"], P_QUANTUM), (
        f"{path}: p {a['p_value']} !~ {g['p_value']}"
    )
    _slices_match(a["slices"], g["slices"], f"{path}.slices")
    assert [v for v, _ in a["children"]] == [v for v, _ in g["children"]], f"{path}: child values"
    for (_, ca), (cv, cg) in zip(a["children"], g["children"], strict=True):
        _node_match(ca, cg, f"{path}.child[{cv}]")


def _assert_matches(actual: dict, golden: dict, scn_id: str) -> None:
    for key in ("measure", "target_type", "n_rows", "grain", "entity"):
        assert actual[key] == golden[key], f"{scn_id}.{key}: {actual[key]!r} != {golden[key]!r}"

    a_dims = [d for d, _ in actual["ranked_dimensions"]]
    g_dims = [d for d, _ in golden["ranked_dimensions"]]
    assert a_dims == g_dims, f"{scn_id}.ranked_dimensions order: {a_dims} != {g_dims}"
    for (d, ga), (_, gg) in zip(
        actual["ranked_dimensions"], golden["ranked_dimensions"], strict=True
    ):
        assert _close(ga, gg), f"{scn_id}.ranked[{d}] gain {ga} !~ {gg}"

    assert actual["driver_paths"] == golden["driver_paths"], f"{scn_id}.driver_paths"

    a_sec = [(s[0], s[2], s[3]) for s in actual["secondary_dimensions"]]
    g_sec = [(s[0], s[2], s[3]) for s in golden["secondary_dimensions"]]
    assert a_sec == g_sec, f"{scn_id}.secondary (dim,grain,entity): {a_sec} != {g_sec}"
    for sa, sg in zip(actual["secondary_dimensions"], golden["secondary_dimensions"], strict=True):
        assert _close(sa[1], sg[1]), f"{scn_id}.secondary[{sa[0]}] gain {sa[1]} !~ {sg[1]}"

    _slices_match(
        actual["interesting_slices"], golden["interesting_slices"], f"{scn_id}.interesting_slices"
    )
    _node_match(actual["root"], golden["root"], f"{scn_id}.root")


# --- test -----------------------------------------------------------------------


@pytest.mark.parametrize("scn", SCENARIOS, ids=[s[0] for s in SCENARIOS])
def test_golden_equivalence(real_session: Session, duck: Any, scn: tuple) -> None:
    actual = _run_scenario(real_session, duck, scn)

    if os.environ.get("DAT580_REGEN"):
        GOLDEN.parent.mkdir(exist_ok=True)
        store = json.loads(GOLDEN.read_text()) if GOLDEN.exists() else {}
        store[scn[0]] = actual
        GOLDEN.write_text(json.dumps(store, indent=2, sort_keys=True) + "\n")
        pytest.skip(f"regenerated golden for {scn[0]}")

    assert GOLDEN.exists(), "golden missing — run with DAT580_REGEN=1 on the pandas baseline"
    golden = json.loads(GOLDEN.read_text())
    assert scn[0] in golden, f"golden missing scenario {scn[0]} — regenerate"
    _assert_matches(actual, golden[scn[0]], scn[0])
