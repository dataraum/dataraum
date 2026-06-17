"""Synthetic driver-discovery corpus (DAT-545) — ported from the DAT-544 kill-gate.

Vertical-neutral by design: abstract dimension names (``D_e*`` = planted drivers at
a known effect size, ``N_*`` = nulls that must stay gated) and a generic
multiplicative ``measure`` — nothing finance-specific. This is the same generative
family the spike's GREEN verdict rests on, so the engine's recall/FDR tests inherit
that calibration. The real-fixture transfer check lives in dataraum-eval (handoff).
"""

from __future__ import annotations

from collections.abc import Iterator

import duckdb
import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.storage import init_database

N_ROWS = 20_000


@pytest.fixture
def real_session() -> Iterator[Session]:
    """In-memory SQLite catalog (FKs off, the resolve-test pattern)."""
    engine = create_engine(
        "sqlite:///:memory:",
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _pragma(dbapi_conn, _record):  # noqa: ANN001, ANN202
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA foreign_keys=OFF")
        cur.close()

    init_database(engine)
    factory = sessionmaker(bind=engine)
    try:
        with factory() as s:
            yield s
    finally:
        engine.dispose()


@pytest.fixture
def duck() -> Iterator[duckdb.DuckDBPyConnection]:
    conn = duckdb.connect(":memory:")
    try:
        yield conn
    finally:
        conn.close()


# Planted drivers: name → multiplicative effect size on the measure.
EFFECTS = {"D_e60": 0.60, "D_e25": 0.25, "D_e15": 0.15, "D_e08": 0.08, "D_e04": 0.04}
DRIVERS = list(EFFECTS)
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


# Ratio corpus (DAT-545 P4): the RATIO numerator/denominator depends on the driver
# dims, with the denominator (volume) varying independently — so a naive mean of
# per-row ratios would misweight, but Σnum/Σden support-weighting recovers the driver.
RATIO_EFFECTS = {"R_e60": 0.60, "R_e25": 0.25}
RATIO_DRIVERS = list(RATIO_EFFECTS)
RATIO_NULLS = ["N_lowcard", "N_midcard", "N_highcard"]
RATIO_DIMS = RATIO_DRIVERS + RATIO_NULLS


def make_ratio_corpus(rng: np.random.Generator) -> pd.DataFrame:
    """A ratio (numerator/denominator) whose VALUE depends on the driver dims."""
    n = N_ROWS
    df = pd.DataFrame(index=np.arange(n))
    denominator = rng.lognormal(mean=5.0, sigma=0.8, size=n)  # volume, varies independently
    log_ratio = np.full(n, np.log(0.2))  # base ratio 0.2
    for name, eps in RATIO_EFFECTS.items():
        v = rng.integers(0, 4, n)
        df[name] = [f"{name}:{x}" for x in v]
        log_ratio += np.log1p(eps * (v - 1.5) / 1.5)
    ratio = np.exp(log_ratio + rng.normal(0.0, 0.1, n))  # + per-row noise
    df["N_lowcard"] = [f"l{v}" for v in rng.integers(0, 6, n)]
    df["N_midcard"] = [f"d{v}" for v in rng.integers(0, 90, n)]
    df["N_highcard"] = [f"h{v}" for v in rng.integers(0, 400, n)]
    df["numerator"] = denominator * ratio
    df["denominator"] = denominator
    return df


# Clustered corpus (DAT-552, ported from DAT-544 E1): repeated entities with a
# within-entity-correlated (high-ICC) measure. The driver + nulls are ENTITY-LEVEL
# (constant within entity); the measure has a per-entity random effect, so the
# exchangeable unit is the entity, not the row. ``CL_*`` names are abstract.
CL_N_ENTITIES = 200
CL_PER_ENTITY = 100  # → 20k rows, contiguous by entity
CL_ENT_SIGMA = 0.8  # entity random-effect sd (the within-entity correlation)
CL_ROW_SIGMA = 0.5
CL_DRIVER = "D_ent_real"  # entity-level attr that shifts the entity effect
CL_ENTITY_NULLS = ["N_ent_K6", "N_ent_K30"]  # entity-level, random wrt the effect
CL_ROW_NULL = "N_row_K6"  # row-level, random (a control — stays safe row-wise)
CL_DIMS = [CL_DRIVER, *CL_ENTITY_NULLS, CL_ROW_NULL]
CL_ENTITY = "entity"


def make_clustered_corpus(
    rng: np.random.Generator, *, row_sigma: float = CL_ROW_SIGMA
) -> pd.DataFrame:
    """200 entities × 100 rows; measure carries a per-entity random effect (high ICC).

    The row-wise permutation null is INVALID here (the entity is the exchangeable
    unit). Columns: ``entity``, ``measure``, an entity-level driver, two entity-level
    nulls, one row-level null. Rows are contiguous by entity. ``row_sigma`` knobs the
    within-entity noise: large values drown the between-entity signal → low ICC
    (used to test the ICC switch's row-wise side).
    """
    ent = np.repeat(np.arange(CL_N_ENTITIES), CL_PER_ENTITY)
    drv_grp = rng.integers(0, 4, CL_N_ENTITIES)
    drv_shift = np.array([-0.6, -0.2, 0.2, 0.6])[drv_grp]
    ent_effect = drv_shift + rng.normal(0, CL_ENT_SIGMA, CL_N_ENTITIES)
    row_noise = rng.normal(0, row_sigma, CL_N_ENTITIES * CL_PER_ENTITY)

    df = pd.DataFrame(index=np.arange(CL_N_ENTITIES * CL_PER_ENTITY))
    df[CL_ENTITY] = ent
    df["measure"] = np.exp(6.0 + ent_effect[ent] + row_noise)
    df[CL_DRIVER] = [f"d{g}" for g in drv_grp[ent]]
    df[CL_ENTITY_NULLS[0]] = [f"a{g}" for g in rng.integers(0, 6, CL_N_ENTITIES)[ent]]
    df[CL_ENTITY_NULLS[1]] = [f"b{g}" for g in rng.integers(0, 30, CL_N_ENTITIES)[ent]]
    df[CL_ROW_NULL] = [f"r{g}" for g in rng.integers(0, 6, CL_N_ENTITIES * CL_PER_ENTITY)]
    return df
