"""DAT-544 — driver-discovery feasibility kill-gate (throwaway spike, v4).

Question: does within-dataset variance-reduction RANKING + a within-dataset
PERMUTATION NULL separate real drivers from null dims on financial-shaped data,
WITHOUT a global threshold? (slice_variance/temporal_drift were cut: they needed a
global cutoff on an absolute statistic that natural financial variation defeats.
Hypothesis: ranking is ordinal/self-calibrating; the permutation null adapts to
each dataset's noise floor.)

Standalone: numpy + pandas.

History:
  v1 (12 min) gave the first read; v2 vectorized + added the (B) missingness gate;
  v3 fixed a review finding (v2 vacuously "gated" high-card — excised by min-support,
  the null never faced inflation) by adding a PARTICIPATING N_midcard, and added an
  effect-size ladder. v3 verdict: GREEN ON SEPARATION (independent-dims, single node),
  FDR controlled at alpha, power floor ~±20-25%.
  v4 (this) eats the two synthetic-testable residuals a review flagged, on-branch,
  before committing the build:
    - CONFOUNDING: N_proxy (80% copy of the strongest driver). Expected to SURFACE
      (it's a legit proxy; acceptable for aggregation). The real test: do the
      genuinely-independent nulls stay gated when a strong proxy competes in the
      max-over-candidates null?
    - RECURSION: a 2-level split. Does FDR COMPOUND across depth (more nodes -> more
      chances for a pure null to surface)? Per-node permutation null rebuilt on the
      node's rows; measure depth-2 null surfacing rate vs alpha.
  Still deferred to P1 (needs the real substrate): real-ERP-fixture transfer.

Run:  uv run --with numpy --with pandas python spikes/dat-544/driver_spike.py
"""

from __future__ import annotations

import numpy as np
import pandas as pd

ALPHA = 0.05
N_PERM = 500
N_ROWS = 20_000
MIN_SUPPORT = 200
MISSINGNESS_GATE = 0.5
N_SEEDS = 40
RECURSION_SEEDS = 12
RECALL_BAR = int(0.9 * N_SEEDS)
FDR_BAR = 2 * ALPHA

EFFECTS = {"D_e60": 0.60, "D_e25": 0.25, "D_e15": 0.15, "D_e08": 0.08, "D_e04": 0.04}
DRIVERS = list(EFFECTS)
REQUIRED = ["D_e60", "D_e25"]
NULLS_INDEP = ["N_lowcard", "N_midcard", "N_highcard", "N_mnar", "N_measure_missing"]
PROXY = "N_proxy"                                    # confounded with D_e60 (expected to surface)
DIMS = DRIVERS + NULLS_INDEP + [PROXY]


def make_corpus(rng: np.random.Generator) -> pd.DataFrame:
    n = N_ROWS
    base = rng.lognormal(mean=6.0, sigma=1.1, size=n)
    measure = base.copy()
    df = pd.DataFrame(index=np.arange(n))

    v_e60 = None
    for name, eps in EFFECTS.items():
        v = rng.integers(0, 4, n)
        if name == "D_e60":
            v_e60 = v
        measure *= 1.0 + eps * (v - 1.5) / 1.5
        df[name] = [f"{name}:{x}" for x in v]

    # CONFOUNDED proxy: 80% a copy of D_e60's value, else random -> correlated with the
    # strongest driver (it explains the measure THROUGH D_e60). Should surface; that is
    # acceptable for aggregation (a real proxy), only wrong for causal "root cause".
    proxy_v = np.where(rng.random(n) < 0.8, v_e60, rng.integers(0, 4, n))
    df[PROXY] = [f"px{x}" for x in proxy_v]

    df["N_lowcard"] = [f"l{v}" for v in rng.integers(0, 6, n)]
    df["N_midcard"] = [f"d{v}" for v in rng.integers(0, 90, n)]    # participates -> inflation test
    df["N_highcard"] = [f"h{v}" for v in rng.integers(0, 400, n)]  # excised by min-support

    present = rng.random(n) < 0.5
    df["N_mnar"] = np.where(present, [f"p{v}" for v in rng.integers(0, 5, n)], None)
    measure[~present] *= 1.5

    df["measure"] = measure

    n_mm = rng.integers(0, 5, n)
    df["N_measure_missing"] = [f"x{v}" for v in n_mm]
    bias = n_mm == 0
    drop = bias & (rng.random(n) < 0.85)
    df.loc[drop, "measure"] = np.nan
    df.loc[bias & ~drop, "measure"] *= 3.0
    return df


def build_codes(s_obj: np.ndarray, y_real: np.ndarray, *, handle: bool) -> tuple[np.ndarray, int]:
    dimnull = pd.isna(s_obj)
    yobs = ~np.isnan(y_real)
    codes = np.full(len(s_obj), -1, dtype=int)
    if handle:
        present = ~dimnull
        baseline = yobs[present].mean() if present.any() else 0.0
        nxt = 0
        for lab in pd.unique(s_obj[present]):
            sl = present & (s_obj == lab)
            rate = yobs[sl].mean() if sl.any() else 0.0
            if rate < MISSINGNESS_GATE * baseline:
                continue
            codes[sl] = nxt
            nxt += 1
        return codes, nxt
    s2 = np.where(dimnull, "__NULL__", s_obj)
    uniq = pd.unique(s2)
    for i, lab in enumerate(uniq):
        codes[s2 == lab] = i
    return codes, len(uniq)


def gain(codes: np.ndarray, ncodes: int, y: np.ndarray) -> float:
    obs = ~np.isnan(y)
    m = (codes >= 0) & obs
    if m.sum() < MIN_SUPPORT:
        return 0.0
    c, yy = codes[m], y[m]
    counts = np.bincount(c, minlength=ncodes)
    sums = np.bincount(c, weights=yy, minlength=ncodes)
    sqs = np.bincount(c, weights=yy * yy, minlength=ncodes)
    big = counts >= MIN_SUPPORT
    if big.sum() < 2:
        return 0.0
    nn, sm, sq = counts[big], sums[big], sqs[big]
    N = nn.sum()
    grand = sm.sum() / N
    total_var = sq.sum() / N - grand**2
    if total_var <= 0:
        return 0.0
    within = (sq.sum() - np.sum(sm * sm / nn)) / N
    return max(0.0, (total_var - within) / total_var)


def gate(df: pd.DataFrame, y: np.ndarray, rng, *, handle: bool, dims: list[str] = DIMS) -> dict:
    cm = {d: build_codes(df[d].astype(object).to_numpy(), y, handle=handle) for d in dims}
    real = {d: gain(*cm[d], y) for d in dims}
    perm_max = np.empty(N_PERM)
    for i in range(N_PERM):
        yp = rng.permutation(y)
        perm_max[i] = max(gain(*cm[d], yp) for d in dims)
    return {d: {"gain": real[d], "p": (1 + np.sum(perm_max >= real[d])) / (1 + N_PERM)} for d in dims}


def recursion_fdr(seed: int) -> tuple[int, int, int, int]:
    """2-level split: root on the empirically top driver, re-gate within each child.
    Returns (depth2_null_pass, depth2_null_tests, e25_pass, e25_tests)."""
    rng = np.random.default_rng(seed)
    df = make_corpus(rng)
    y = df["measure"].to_numpy(float)
    root_res = gate(df, y, rng, handle=True)
    root = max(DIMS, key=lambda d: root_res[d]["gain"])
    remaining = [d for d in DIMS if d != root]
    s = df[root].astype(object).to_numpy()
    npass = ntest = e25p = e25t = 0
    for val in pd.unique(s):
        if pd.isna(val):
            continue
        m = s == val
        if m.sum() < 4 * MIN_SUPPORT:
            continue
        sub = df[m].reset_index(drop=True)
        suby = y[m]
        res = gate(sub, suby, rng, handle=True, dims=remaining)
        for nd in NULLS_INDEP:
            ntest += 1
            npass += res[nd]["p"] < ALPHA
        if "D_e25" in remaining:
            e25t += 1
            e25p += res["D_e25"]["p"] < ALPHA
    return npass, ntest, e25p, e25t


def main() -> None:
    print("=" * 82)
    print("DAT-544 driver-discovery kill-gate (v4) — confounding + recursion residuals")
    print(f"  alpha={ALPHA} n_perm={N_PERM} n_rows={N_ROWS} min_support={MIN_SUPPORT} seeds={N_SEEDS}")
    print("=" * 82)

    # ── single node (with the confounded proxy now competing in the candidate set) ──
    print("\n### SINGLE NODE (handling on; N_proxy = 80% copy of D_e60, competing)")
    recall_ok = 0
    nullc = {d: 0 for d in NULLS_INDEP}
    proxy_pass = proxy_below_e60 = 0
    g_acc = {d: [] for d in DIMS}
    drvc = {d: 0 for d in DRIVERS}
    for seed in range(N_SEEDS):
        rng = np.random.default_rng(seed)
        df = make_corpus(rng)
        res = gate(df, df["measure"].to_numpy(float), rng, handle=True)
        for d in DIMS:
            g_acc[d].append(res[d]["gain"])
        passed = {d: res[d]["p"] < ALPHA for d in DIMS}
        for d in DRIVERS:
            drvc[d] += passed[d]
        for d in NULLS_INDEP:
            nullc[d] += passed[d]
        proxy_pass += passed[PROXY]
        proxy_below_e60 += res[PROXY]["gain"] <= res["D_e60"]["gain"]
        outrank = all(res[r]["gain"] > max(res[nl]["gain"] for nl in NULLS_INDEP) for r in REQUIRED)
        if all(passed[r] for r in REQUIRED) and outrank:
            recall_ok += 1
    print(f"  recall (required pass & outrank independent nulls): {recall_ok}/{N_SEEDS}")
    print("  drivers:")
    for d in DRIVERS:
        print(f"      {d} (±{EFFECTS[d]:.0%})  pass {drvc[d]:2d}/{N_SEEDS}  gain={np.mean(g_acc[d]):.4f}")
    print("  INDEPENDENT nulls (the FDR metric — must stay gated):")
    for d in NULLS_INDEP:
        print(f"      {d:20s} pass {nullc[d]:2d}/{N_SEEDS}  gain={np.mean(g_acc[d]):.4f}")
    print(f"  CONFOUNDED proxy N_proxy: pass {proxy_pass}/{N_SEEDS} (expected to surface), "
          f"gain={np.mean(g_acc[PROXY]):.4f}, ranked<=D_e60 in {proxy_below_e60}/{N_SEEDS}")
    indep_ok = all(c <= FDR_BAR * N_SEEDS for c in nullc.values())
    print(f"  >>> independent-null FDR intact under confounding: {indep_ok}")

    # ── recursion: does FDR compound at depth 2? ──
    print(f"\n### RECURSION (2-level split, {RECURSION_SEEDS} seeds)")
    npass = ntest = e25p = e25t = 0
    for seed in range(RECURSION_SEEDS):
        a, b, c, d = recursion_fdr(seed)
        npass += a; ntest += b; e25p += c; e25t += d
    rate = npass / ntest if ntest else 0.0
    print(f"  depth-2 independent-null surfacing: {npass}/{ntest} = {rate:.3f}  (alpha={ALPHA})")
    print(f"  depth-2 D_e25 detection within children: {e25p}/{e25t}")
    print(f"  >>> FDR compounds across depth? {'YES (>2*alpha)' if rate > FDR_BAR else 'NO (<=2*alpha)'}")


if __name__ == "__main__":
    main()
