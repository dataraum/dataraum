"""DAT-544 — driver-discovery feasibility kill-gate (throwaway spike, v2).

Question: does a within-dataset variance-reduction RANKING + a within-dataset
PERMUTATION NULL separate real drivers from null dims on financial-shaped data,
WITHOUT any global threshold? (slice_variance / temporal_drift / outlier_rate were
cut because they needed a global cutoff on an absolute statistic. Hypothesis:
ranking is ordinal/self-calibrating; the permutation null adapts to each dataset's
noise floor; no global constant needed.)

Standalone: numpy + pandas only. Single split (root) — the tree is recursion.

v2 changes (from the v1 run, DAT-544 checkpoint):
  - vectorized gain via np.bincount on precomputed group codes (12 min -> seconds).
  - (B) handling upgraded: effective-n min-support ALONE leaked the measure-missing
    slice (kept ~600 > 200 rows); added a MISSINGNESS-CONCENTRATION gate
    (slice_conditional_null-style): drop a slice whose non-null-measure rate is far
    below the dim's baseline.
  - added a ratio/non-additive measure probe.

Run:  uv run --with numpy --with pandas python spikes/dat-544/driver_spike.py
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# ── PRE-REGISTERED bar (declared before results; not tuned to pass) ──────────
ALPHA = 0.05
N_PERM = 500
N_ROWS = 20_000
MIN_SUPPORT = 200            # effective-n (non-null measure rows) per contributing slice
MISSINGNESS_GATE = 0.5       # drop a slice whose non-null rate < this * dim baseline (B)
N_SEEDS = 40                 # 40 (not 10): a correct gate FIRES at the nominal alpha rate, so
                             # 10 seeds can't resolve a 5% rate (granularity 0.1 > alpha 0.05).
RECALL_BAR = int(0.9 * N_SEEDS)   # strong+moderate pass & outrank all nulls
# A null may fire at ~alpha by construction (that's a correct significance test, not a leak).
# Bar = pass-rate consistent with the nominal alpha, with finite-seed tolerance (2*alpha).
FDR_BAR = 2 * ALPHA


# ── Corpus: synthetic, KNOWN truth, financial-shaped (heavy-tailed, noisy) ───
def make_corpus(rng: np.random.Generator) -> pd.DataFrame:
    n = N_ROWS
    base = rng.lognormal(mean=6.0, sigma=1.1, size=n)
    df = pd.DataFrame(index=np.arange(n))

    d_strong = rng.integers(0, 4, n)
    d_moderate = rng.integers(0, 5, n)
    d_weak = rng.integers(0, 4, n)
    measure = (
        base
        * np.array([0.4, 0.8, 1.2, 1.6])[d_strong]
        * np.array([0.75, 0.9, 1.0, 1.1, 1.25])[d_moderate]
        * np.array([0.92, 0.97, 1.03, 1.08])[d_weak]
    )
    df["D_strong"] = [f"s{v}" for v in d_strong]
    df["D_moderate"] = [f"m{v}" for v in d_moderate]
    df["D_weak"] = [f"w{v}" for v in d_weak]

    df["N_lowcard"] = [f"l{v}" for v in rng.integers(0, 6, n)]
    df["N_highcard"] = [f"h{v}" for v in rng.integers(0, 400, n)]  # inflation adversary

    present = rng.random(n) < 0.5                                   # MNAR-dim adversary
    df["N_mnar"] = np.where(present, [f"p{v}" for v in rng.integers(0, 5, n)], None)
    measure = measure.copy()
    measure[~present] *= 1.5                                        # null-vs-present shift

    df["measure"] = measure

    n_mm = rng.integers(0, 5, n)                                    # measure-missing adversary
    df["N_measure_missing"] = [f"x{v}" for v in n_mm]
    bias = n_mm == 0
    drop = bias & (rng.random(n) < 0.85)
    df.loc[drop, "measure"] = np.nan
    surv = bias & ~drop
    df.loc[surv, "measure"] *= 3.0
    return df


DIMS = ["D_strong", "D_moderate", "D_weak", "N_lowcard", "N_highcard", "N_mnar", "N_measure_missing"]
PLANTED_REQ = ["D_strong", "D_moderate"]
NULLS = ["N_lowcard", "N_highcard", "N_mnar", "N_measure_missing"]


# ── Group codes: bake (A) dim-present + (B) missingness gate into -1 codes ───
def build_codes(s_obj: np.ndarray, y_real: np.ndarray, *, handle: bool) -> tuple[np.ndarray, int]:
    dimnull = pd.isna(s_obj)
    yobs = ~np.isnan(y_real)
    codes = np.full(len(s_obj), -1, dtype=int)
    if handle:
        present = ~dimnull                                          # (A) drop dim-null rows
        baseline = yobs[present].mean() if present.any() else 0.0
        nxt = 0
        for lab in pd.unique(s_obj[present]):
            sl = present & (s_obj == lab)
            rate = yobs[sl].mean() if sl.any() else 0.0
            if rate < MISSINGNESS_GATE * baseline:                  # (B) drop high-missingness slice
                continue
            codes[sl] = nxt
            nxt += 1
        return codes, nxt
    s2 = np.where(dimnull, "__NULL__", s_obj)                       # ablation: NULL is a value
    uniq = pd.unique(s2)
    for i, lab in enumerate(uniq):
        codes[s2 == lab] = i
    return codes, len(uniq)


def gain(codes: np.ndarray, ncodes: int, y: np.ndarray) -> float:
    """Variance-reduction fraction; small groups (effective-n < MIN_SUPPORT) dropped."""
    obs = ~np.isnan(y)
    m = (codes >= 0) & obs
    if m.sum() < MIN_SUPPORT:
        return 0.0
    c = codes[m]
    yy = y[m]
    counts = np.bincount(c, minlength=ncodes)
    sums = np.bincount(c, weights=yy, minlength=ncodes)
    sqs = np.bincount(c, weights=yy * yy, minlength=ncodes)
    big = counts >= MIN_SUPPORT
    if big.sum() < 2:
        return 0.0
    n, sm, sq = counts[big], sums[big], sqs[big]
    N = n.sum()
    grand = sm.sum() / N
    total_var = sq.sum() / N - grand**2
    if total_var <= 0:
        return 0.0
    within = (sq.sum() - np.sum(sm * sm / n)) / N
    return max(0.0, (total_var - within) / total_var)


def gate(df: pd.DataFrame, y: np.ndarray, rng, *, handle: bool) -> dict:
    cm = {d: build_codes(df[d].astype(object).to_numpy(), y, handle=handle) for d in DIMS}
    real = {d: gain(*cm[d], y) for d in DIMS}
    perm_max = np.empty(N_PERM)
    for i in range(N_PERM):
        yp = rng.permutation(y)
        perm_max[i] = max(gain(*cm[d], yp) for d in DIMS)
    return {d: {"gain": real[d], "p": (1 + np.sum(perm_max >= real[d])) / (1 + N_PERM)} for d in DIMS}


# ── Ratio / non-additive probe: support(denominator)-weighted variance reduction ──
def ratio_probe(seed: int) -> dict:
    rng = np.random.default_rng(1000 + seed)
    n = N_ROWS
    den = rng.lognormal(5.0, 1.0, n)                                # e.g. revenue base
    d = rng.integers(0, 4, n)
    margin = np.array([0.10, 0.20, 0.30, 0.40])[d] + rng.normal(0, 0.04, n)  # group ratio differs
    num = den * margin
    cols = {
        "R_driver": [f"r{v}" for v in d],
        "N_low": [f"l{v}" for v in rng.integers(0, 6, n)],
        "N_high": [f"h{v}" for v in rng.integers(0, 400, n)],
    }
    rdims = list(cols)

    def codes_of(vals):
        uniq = pd.unique(np.array(vals, dtype=object))
        idx = {l: i for i, l in enumerate(uniq)}
        return np.array([idx[v] for v in vals]), len(uniq)

    cm = {k: codes_of(v) for k, v in cols.items()}

    def rgain(codes, ncodes, num_, den_):
        counts = np.bincount(codes, minlength=ncodes)
        W = np.bincount(codes, weights=den_, minlength=ncodes)       # denominator weight
        WN = np.bincount(codes, weights=num_, minlength=ncodes)      # numerator
        WNR = np.bincount(codes, weights=num_ * num_ / den_, minlength=ncodes)  # w*r^2
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
        np_, dp = num[p], den[p]                                     # permute (num,den) jointly
        perm_max[i] = max(rgain(*cm[k], np_, dp) for k in rdims)
    return {k: {"gain": real[k], "p": (1 + np.sum(perm_max >= real[k])) / (1 + N_PERM)} for k in rdims}


def main() -> None:
    print("=" * 78)
    print("DAT-544 driver-discovery kill-gate (v2) — PRE-REGISTERED bar")
    print(f"  alpha={ALPHA} n_perm={N_PERM} n_rows={N_ROWS} min_support={MIN_SUPPORT} "
          f"missingness_gate={MISSINGNESS_GATE} seeds={N_SEEDS}")
    print(f"  GREEN iff: strong+moderate pass & outrank all nulls in >={RECALL_BAR}/{N_SEEDS}; "
          f"no null passes > {FDR_BAR:.0%}; ablation surfaces the confounds.")
    print("=" * 78)

    for mode, handle in [("HANDLING ON", True), ("ABLATION: HANDLING OFF", False)]:
        print(f"\n### {mode}")
        recall_ok = weak_pass = 0
        nullc = {d: 0 for d in NULLS}
        g_acc = {d: [] for d in DIMS}
        p_acc = {d: [] for d in DIMS}
        for seed in range(N_SEEDS):
            rng = np.random.default_rng(seed)
            df = make_corpus(rng)
            res = gate(df, df["measure"].to_numpy(float), rng, handle=handle)
            for d in DIMS:
                g_acc[d].append(res[d]["gain"])
                p_acc[d].append(res[d]["p"])
            passed = {d: res[d]["p"] < ALPHA for d in DIMS}
            outrank = all(res[r]["gain"] > max(res[nl]["gain"] for nl in NULLS) for r in PLANTED_REQ)
            if all(passed[r] for r in PLANTED_REQ) and outrank:
                recall_ok += 1
            weak_pass += passed["D_weak"]
            for d in NULLS:
                nullc[d] += passed[d]
        print(f"  recall (strong+moderate pass & outrank all nulls): {recall_ok}/{N_SEEDS}")
        print(f"  weak pass (gray, not required): {weak_pass}/{N_SEEDS}")
        for d in NULLS:
            print(f"      null {d:22s} pass {nullc[d]}/{N_SEEDS}")
        for d in DIMS:
            tag = "DRIVER" if d.startswith("D_") else "null  "
            print(f"      [{tag}] {d:22s} gain={np.mean(g_acc[d]):.4f}  p={np.mean(p_acc[d]):.4f}")
        if handle:
            green = recall_ok >= RECALL_BAR and all(c <= FDR_BAR * N_SEEDS for c in nullc.values())
            print(f"  >>> handling-on verdict: {'GREEN' if green else 'RED'}")
        else:
            print(f"  >>> ablation: confounds surface w/o handling? "
                  f"{'YES' if (nullc['N_mnar'] or nullc['N_measure_missing']) else 'NO'}")

    print("\n### RATIO / NON-ADDITIVE PROBE")
    rdims = ["R_driver", "N_low", "N_high"]
    rc = {k: 0 for k in rdims}
    rg = {k: [] for k in rdims}
    rp = {k: [] for k in rdims}
    for seed in range(N_SEEDS):
        res = ratio_probe(seed)
        for k in rdims:
            rg[k].append(res[k]["gain"])
            rp[k].append(res[k]["p"])
            rc[k] += res[k]["p"] < ALPHA
    for k in rdims:
        tag = "DRIVER" if k == "R_driver" else "null  "
        print(f"      [{tag}] {k:10s} pass {rc[k]}/{N_SEEDS}  gain={np.mean(rg[k]):.4f}  p={np.mean(rp[k]):.4f}")
    rgreen = rc["R_driver"] >= RECALL_BAR and rc["N_low"] <= FDR_BAR * N_SEEDS and rc["N_high"] <= FDR_BAR * N_SEEDS
    print(f"  >>> ratio probe: {'GREEN' if rgreen else 'RED'}")


if __name__ == "__main__":
    main()
