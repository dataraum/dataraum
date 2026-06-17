"""DAT-544 — driver-discovery feasibility kill-gate (throwaway spike, v3).

Question: does within-dataset variance-reduction RANKING + a within-dataset
PERMUTATION NULL separate real drivers from null dims on financial-shaped data,
WITHOUT a global threshold? (slice_variance/temporal_drift were cut because they
needed a global cutoff on an absolute statistic that natural financial variation
defeats. Hypothesis: ranking is ordinal/self-calibrating; the permutation null
adapts to each dataset's noise floor.)

Standalone: numpy + pandas. Single split (root) — the tree is recursion.

v3 (after an adversarial review of v2 found the GREEN OVERSTATED):
  - v2 vacuously "gated" N_highcard: 400 cats / 20k rows ≈ 50 rows each, all below
    MIN_SUPPORT, so gain() hard-returned 0 — the SUPPORT FILTER excised it, the
    permutation null never actually faced finite-sample inflation (the headline claim).
    v3 adds N_midcard (~90 cats, ~222 rows) that PARTICIPATES past min-support, so the
    null is genuinely shown to tame an inflated dim (asserted: midcard real gain > 0).
    N_highcard kept to show min-support as the complementary defense for extreme card.
  - effect-size LADDER (±60/25/15/8/4 %) to locate the detection threshold, not just a
    300 %-spread softball.
  Deferred to P1 (documented): correlated/confounded dims; harder-MNAR (signal in
  surviving labels); recursive 2-level splits. The single-node inflation-control is
  what the kill-gate must settle, and N_midcard settles it.

Run:  uv run --with numpy --with pandas python spikes/dat-544/driver_spike.py
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# ── PRE-REGISTERED bar ───────────────────────────────────────────────────────
ALPHA = 0.05
N_PERM = 500
N_ROWS = 20_000
MIN_SUPPORT = 200
MISSINGNESS_GATE = 0.5
N_SEEDS = 40
RECALL_BAR = int(0.9 * N_SEEDS)   # strong+moderate pass & outrank all nulls
FDR_BAR = 2 * ALPHA               # a correct gate fires at ~alpha; finite-seed tolerance

# Effect-size ladder: half-spread of the group-mean multiplier around 1.0.
EFFECTS = {"D_e60": 0.60, "D_e25": 0.25, "D_e15": 0.15, "D_e08": 0.08, "D_e04": 0.04}
DRIVERS = list(EFFECTS)
REQUIRED = ["D_e60", "D_e25"]                       # the recall bar (clear drivers)
NULLS = ["N_lowcard", "N_midcard", "N_highcard", "N_mnar", "N_measure_missing"]
DIMS = DRIVERS + NULLS


def make_corpus(rng: np.random.Generator) -> pd.DataFrame:
    n = N_ROWS
    base = rng.lognormal(mean=6.0, sigma=1.1, size=n)   # heavy-tailed, CV≈2
    measure = base.copy()
    df = pd.DataFrame(index=np.arange(n))

    # drivers: 4 values each; multiplier spread = ±eps around 1.0
    for name, eps in EFFECTS.items():
        v = rng.integers(0, 4, n)
        mult = 1.0 + eps * (v - 1.5) / 1.5          # values map to [1-eps, 1+eps]
        measure *= mult
        df[name] = [f"{name}:{x}" for x in v]

    # nulls (independent of measure)
    df["N_lowcard"] = [f"l{v}" for v in rng.integers(0, 6, n)]
    df["N_midcard"] = [f"d{v}" for v in rng.integers(0, 90, n)]    # ~222/grp: PARTICIPATES → inflation test
    df["N_highcard"] = [f"h{v}" for v in rng.integers(0, 400, n)]  # ~50/grp: excised by min-support

    present = rng.random(n) < 0.5                                  # MNAR-dim
    df["N_mnar"] = np.where(present, [f"p{v}" for v in rng.integers(0, 5, n)], None)
    measure[~present] *= 1.5

    df["measure"] = measure

    n_mm = rng.integers(0, 5, n)                                   # measure-missing slice
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
        present = ~dimnull                                        # (A) drop dim-null rows
        baseline = yobs[present].mean() if present.any() else 0.0
        nxt = 0
        for lab in pd.unique(s_obj[present]):
            sl = present & (s_obj == lab)
            rate = yobs[sl].mean() if sl.any() else 0.0
            if rate < MISSINGNESS_GATE * baseline:                # (B) drop high-missingness slice
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


def gate(df: pd.DataFrame, y: np.ndarray, rng, *, handle: bool) -> dict:
    cm = {d: build_codes(df[d].astype(object).to_numpy(), y, handle=handle) for d in DIMS}
    real = {d: gain(*cm[d], y) for d in DIMS}
    perm_max = np.empty(N_PERM)
    for i in range(N_PERM):
        yp = rng.permutation(y)
        perm_max[i] = max(gain(*cm[d], yp) for d in DIMS)
    return {d: {"gain": real[d], "p": (1 + np.sum(perm_max >= real[d])) / (1 + N_PERM)} for d in DIMS}


def ratio_probe(seed: int) -> dict:
    rng = np.random.default_rng(1000 + seed)
    n = N_ROWS
    den = rng.lognormal(5.0, 1.0, n)
    d = rng.integers(0, 4, n)
    num = den * (np.array([0.10, 0.20, 0.30, 0.40])[d] + rng.normal(0, 0.04, n))
    cols = {"R_driver": [f"r{v}" for v in d],
            "N_low": [f"l{v}" for v in rng.integers(0, 6, n)],
            "N_high": [f"h{v}" for v in rng.integers(0, 400, n)]}
    rdims = list(cols)

    def codes_of(vals):
        uniq = pd.unique(np.array(vals, dtype=object))
        idx = {l: i for i, l in enumerate(uniq)}
        return np.array([idx[v] for v in vals]), len(uniq)

    cm = {k: codes_of(v) for k, v in cols.items()}

    def rgain(codes, ncodes, num_, den_):
        counts = np.bincount(codes, minlength=ncodes)
        W = np.bincount(codes, weights=den_, minlength=ncodes)
        WN = np.bincount(codes, weights=num_, minlength=ncodes)
        WNR = np.bincount(codes, weights=num_ * num_ / den_, minlength=ncodes)
        big = counts >= MIN_SUPPORT
        if big.sum() < 2:
            return 0.0
        Wb, WNb, WNRb = W[big], WN[big], WNR[big]
        Wtot = Wb.sum()
        grand = WNb.sum() / Wtot
        total = WNRb.sum() / Wtot - grand**2
        if total <= 0:
            return 0.0
        within = (WNRb.sum() - np.sum(WNb * WNb / Wb)) / Wtot
        return max(0.0, (total - within) / total)

    real = {k: rgain(*cm[k], num, den) for k in rdims}
    perm_max = np.empty(N_PERM)
    for i in range(N_PERM):
        p = rng.permutation(n)
        perm_max[i] = max(rgain(*cm[k], num[p], den[p]) for k in rdims)
    return {k: {"gain": real[k], "p": (1 + np.sum(perm_max >= real[k])) / (1 + N_PERM)} for k in rdims}


def main() -> None:
    print("=" * 80)
    print("DAT-544 driver-discovery kill-gate (v3) — PRE-REGISTERED bar")
    print(f"  alpha={ALPHA} n_perm={N_PERM} n_rows={N_ROWS} min_support={MIN_SUPPORT} "
          f"missingness_gate={MISSINGNESS_GATE} seeds={N_SEEDS}")
    print(f"  GREEN iff: required drivers {REQUIRED} pass & outrank all nulls in "
          f">={RECALL_BAR}/{N_SEEDS}; every null pass-rate <= {FDR_BAR:.0%}; "
          "N_midcard genuinely participates (real gain>0) yet is gated; ablation surfaces confounds.")
    print("=" * 80)

    for mode, handle in [("HANDLING ON", True), ("ABLATION: HANDLING OFF", False)]:
        print(f"\n### {mode}")
        recall_ok = 0
        nullc = {d: 0 for d in NULLS}
        g_acc = {d: [] for d in DIMS}
        p_acc = {d: [] for d in DIMS}
        drvc = {d: 0 for d in DRIVERS}
        for seed in range(N_SEEDS):
            rng = np.random.default_rng(seed)
            df = make_corpus(rng)
            res = gate(df, df["measure"].to_numpy(float), rng, handle=handle)
            for d in DIMS:
                g_acc[d].append(res[d]["gain"])
                p_acc[d].append(res[d]["p"])
            passed = {d: res[d]["p"] < ALPHA for d in DIMS}
            for d in DRIVERS:
                drvc[d] += passed[d]
            for d in NULLS:
                nullc[d] += passed[d]
            outrank = all(res[r]["gain"] > max(res[nl]["gain"] for nl in NULLS) for r in REQUIRED)
            if all(passed[r] for r in REQUIRED) and outrank:
                recall_ok += 1
        print(f"  recall (required pass & outrank all nulls): {recall_ok}/{N_SEEDS}")
        print("  effect-size ladder (driver pass-rate | mean gain):")
        for d in DRIVERS:
            print(f"      {d} (±{EFFECTS[d]:.0%})  pass {drvc[d]:2d}/{N_SEEDS}  gain={np.mean(g_acc[d]):.4f}")
        print("  nulls (pass-rate | mean gain | mean p):")
        for d in NULLS:
            print(f"      {d:20s} pass {nullc[d]:2d}/{N_SEEDS}  gain={np.mean(g_acc[d]):.4f}  p={np.mean(p_acc[d]):.4f}")
        if handle:
            midcard_participates = np.mean(g_acc["N_midcard"]) > 0.0
            green = (
                recall_ok >= RECALL_BAR
                and all(c <= FDR_BAR * N_SEEDS for c in nullc.values())
                and midcard_participates
            )
            print(f"  midcard participates (real gain>0, not excised): {midcard_participates} "
                  f"(mean gain {np.mean(g_acc['N_midcard']):.4f})")
            print(f"  >>> handling-on verdict: {'GREEN' if green else 'RED'}")
        else:
            print(f"  >>> ablation: confounds surface w/o handling? "
                  f"{'YES' if (nullc['N_mnar'] or nullc['N_measure_missing']) else 'NO'}")

    print("\n### RATIO / NON-ADDITIVE PROBE")
    rdims = ["R_driver", "N_low", "N_high"]
    rc = {k: 0 for k in rdims}
    rg = {k: [] for k in rdims}
    for seed in range(N_SEEDS):
        res = ratio_probe(seed)
        for k in rdims:
            rg[k].append(res[k]["gain"])
            rc[k] += res[k]["p"] < ALPHA
    for k in rdims:
        tag = "DRIVER" if k == "R_driver" else "null  "
        print(f"      [{tag}] {k:10s} pass {rc[k]:2d}/{N_SEEDS}  gain={np.mean(rg[k]):.4f}")
    print(f"  >>> ratio probe: {'GREEN' if rc['R_driver'] >= RECALL_BAR and rc['N_low'] <= FDR_BAR * N_SEEDS and rc['N_high'] <= FDR_BAR * N_SEEDS else 'RED'}")


if __name__ == "__main__":
    main()
