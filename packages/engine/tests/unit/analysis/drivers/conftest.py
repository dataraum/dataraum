"""Synthetic driver-discovery corpus (DAT-545) — ported from the DAT-544 kill-gate.

Vertical-neutral by design: abstract dimension names (``D_e*`` = planted drivers at
a known effect size, ``N_*`` = nulls that must stay gated) and a generic
multiplicative ``measure`` — nothing finance-specific. This is the same generative
family the spike's GREEN verdict rests on, so the engine's recall/FDR tests inherit
that calibration. The real-fixture transfer check lives in dataraum-eval (handoff).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

N_ROWS = 20_000

# Planted drivers: name → multiplicative effect size on the measure.
EFFECTS = {"D_e60": 0.60, "D_e25": 0.25, "D_e15": 0.15, "D_e08": 0.08, "D_e04": 0.04}
DRIVERS = list(EFFECTS)
# The drivers a recall test requires to surface AND outrank every independent null.
REQUIRED_DRIVERS = ["D_e60", "D_e25"]
# Independent nulls — must stay gated (the FDR metric).
INDEPENDENT_NULLS = ["N_lowcard", "N_midcard", "N_highcard", "N_mnar", "N_measure_missing"]
# Confounded proxy: 80% a copy of the strongest driver → expected to surface (a real
# proxy, fine for aggregation); the test is that INDEPENDENT nulls stay gated when it competes.
PROXY = "N_proxy"
ALL_DIMS = DRIVERS + INDEPENDENT_NULLS + [PROXY]


def make_corpus(rng: np.random.Generator) -> pd.DataFrame:
    """One synthetic dataset: planted drivers + independent nulls + a confounded proxy.

    ``N_mnar`` is missing-dimension on half the rows (exercises the (A) gate);
    ``N_measure_missing`` concentrates MEASURE missingness in one slice value
    (exercises the (B) gate — the leak min-support alone can't close).
    """
    n = N_ROWS
    measure = rng.lognormal(mean=6.0, sigma=1.1, size=n)
    df = pd.DataFrame(index=np.arange(n))

    v_e60 = None
    for name, eps in EFFECTS.items():
        v = rng.integers(0, 4, n)
        if name == "D_e60":
            v_e60 = v
        measure *= 1.0 + eps * (v - 1.5) / 1.5
        df[name] = [f"{name}:{x}" for x in v]

    proxy_v = np.where(rng.random(n) < 0.8, v_e60, rng.integers(0, 4, n))
    df[PROXY] = [f"px{x}" for x in proxy_v]

    df["N_lowcard"] = [f"l{v}" for v in rng.integers(0, 6, n)]
    df["N_midcard"] = [f"d{v}" for v in rng.integers(0, 90, n)]  # participates (inflation test)
    df["N_highcard"] = [f"h{v}" for v in rng.integers(0, 400, n)]  # excised by min-support

    present = rng.random(n) < 0.5
    df["N_mnar"] = np.where(present, [f"p{v}" for v in rng.integers(0, 5, n)], None)
    measure[~present] *= 1.5

    df["measure"] = measure

    # Measure-conditional missingness: in slice value 0 the measure is 85% dropped
    # (and 3× inflated where present) — a flat null-ratio hides it; the (B) gate closes it.
    n_mm = rng.integers(0, 5, n)
    df["N_measure_missing"] = [f"x{v}" for v in n_mm]
    bias = n_mm == 0
    drop = bias & (rng.random(n) < 0.85)
    df.loc[drop, "measure"] = np.nan
    df.loc[bias & ~drop, "measure"] *= 3.0
    return df


def columns(df: pd.DataFrame, dim: str) -> tuple[np.ndarray, np.ndarray]:
    """``(values, measure)`` numpy arrays for one dimension — the criterion's inputs."""
    return df[dim].astype(object).to_numpy(), df["measure"].to_numpy(dtype=float)
