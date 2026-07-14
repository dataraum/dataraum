"""The named statistics behind stack v4 (DAT-761) — pure-function checks.

Each measure's defining property is asserted directly: row-g3 exactness, λ's
exact-FD-invariance vs vacuous-skew kill, BH's family control, the permutation
p-value's calibration, and the gate-#2 verdict split (ROLE / DIRT / ABSTAIN).
"""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from dataraum.analysis.hierarchies.stats import (
    RoleVerdict,
    bh_reject,
    codes_of,
    gk_lambda,
    minority_mass,
    perm_pvalue,
    role_verdict,
    row_g3,
)


def test_codes_of_null_is_its_own_category() -> None:
    s = pl.Series(["a", None, "b", "a", None])
    codes = codes_of(s)
    assert len(np.unique(codes)) == 3  # a, b, NULL
    assert codes[1] == codes[4]


def test_row_g3_exact_fd_is_zero() -> None:
    a = np.arange(1000) % 40
    b = a // 5
    assert row_g3(a, b) == 0.0


def test_row_g3_counts_minimum_violating_rows() -> None:
    a = np.arange(1000) % 40
    b = a // 5
    b[::250] = (b[::250] + 1) % 8  # 4 dirty rows
    assert row_g3(a, b) == pytest.approx(4 / 1000)


def test_lambda_exact_fd_is_one_regardless_of_skew() -> None:
    # 98%-dominant dependent, but an exact FD of a: λ must be exactly 1.
    a = np.arange(10_000) % 50
    b = (a == 0).astype(np.int64)
    assert gk_lambda(a, b) == 1.0


def test_lambda_vacuous_skew_is_near_zero() -> None:
    # Minority spread evenly across a-groups: g3 ≈ minority mass, λ ≈ 0.
    n = 10_000
    a = np.arange(n) % 50
    b = ((np.arange(n) // 50) % 200 == 0).astype(np.int64)
    assert row_g3(a, b) <= 0.01  # the effect screen alone would pass it
    assert gk_lambda(a, b) < 0.1


def test_minority_mass() -> None:
    assert minority_mass(np.array([0, 0, 0, 1])) == 0.25
    assert minority_mass(np.zeros(5, dtype=np.int64)) == 0.0


def test_bh_reject_family_control() -> None:
    # One real signal among untested-implicit-1.0 family members survives;
    # a flat family yields nothing.
    assert bh_reject({"x": 1e-4, "y": 0.9}, m_family=10) == {"x"}
    assert bh_reject({"x": 0.5, "y": 0.9}, m_family=10) == set()


def test_perm_pvalue_separates_dependence_from_noise() -> None:
    rng_data = np.random.default_rng(7)
    a = rng_data.integers(0, 30, 5_000)
    dependent = a // 3
    independent = rng_data.integers(0, 10, 5_000)
    assert perm_pvalue(a, dependent, np.random.default_rng(0)) < 0.001
    assert perm_pvalue(a, independent, np.random.default_rng(0)) > 0.05


def test_role_verdict_systematic_disagreement_is_role() -> None:
    # Disagreements happen exactly on the dropship rows: T1 fires.
    n = 8_000
    channel = (np.arange(n) % 100 < 2).astype(np.int64)
    dis = channel.copy()
    b = np.arange(n) % 500
    res = role_verdict(dis, {"channel": channel}, b, np.random.default_rng(0))
    assert res.verdict is RoleVerdict.ROLE
    assert res.t1_context == "channel"


def test_role_verdict_random_disagreement_is_dirt() -> None:
    # Disagreements land on rows no context predicts: both tests stay null,
    # and with the full permutation budget the p-floor is below α → DIRT.
    n = 8_000
    rng_data = np.random.default_rng(11)
    dis = np.zeros(n, dtype=np.int64)
    dis[rng_data.choice(n, 30, replace=False)] = 1
    channel = (np.arange(n) % 100 < 2).astype(np.int64)
    b = np.arange(n) % 500
    res = role_verdict(dis, {"channel": channel}, b, np.random.default_rng(0))
    assert res.verdict is RoleVerdict.DIRT


def test_role_verdict_abstains_below_k_floor() -> None:
    # With 2 disagreement rows the achievable p-floor is α-sensitive (ties
    # dominate the permutation null): the honest verdict is ABSTAIN, never a
    # coin-flip DIRT — the DAT-757 f6-role-dup cell.
    n = 2_000
    dis = np.zeros(n, dtype=np.int64)
    dis[:2] = 1
    contexts = {f"c{i}": np.arange(n) % (i + 2) for i in range(4)}
    res = role_verdict(dis, contexts, np.arange(n) % 7, np.random.default_rng(0))
    assert res.verdict is RoleVerdict.ABSTAIN
    assert res.k_disagree == 2


def test_role_verdict_abstains_when_floor_exceeds_alpha() -> None:
    # Above the k-floor but with a tiny permutation budget the p-floor cannot
    # resolve the Bonferroni α: still ABSTAIN.
    n = 2_000
    rng_data = np.random.default_rng(3)
    dis = np.zeros(n, dtype=np.int64)
    dis[rng_data.choice(n, 15, replace=False)] = 1
    contexts = {f"c{i}": np.arange(n) % (i + 2) for i in range(4)}
    res = role_verdict(dis, contexts, np.arange(n) % 7, np.random.default_rng(0), reps=30)
    assert res.verdict is RoleVerdict.ABSTAIN
