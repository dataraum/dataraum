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


# Two-driver clustered corpus (DAT-561): a HIGH-ICC measure carrying BOTH a
# between-entity (entity-level) driver AND a within-entity (row-level) driver, plus one
# null at each grain. Additive (not lognormal) so the within-entity de-mean cleanly
# isolates the row driver: residual = measure − entity_mean strips the entity level, so
# the row driver — diluted ~7× in the raw measure by the between-entity variance — is
# recovered, while the entity driver correctly drops out of the row-level family.
CL_ROW_DRIVER = "D_row_real"  # within-entity (row-level) driver
TWO_DRIVER_DIMS = [CL_DRIVER, CL_ROW_DRIVER, CL_ENTITY_NULLS[1], CL_ROW_NULL]


def make_clustered_two_driver_corpus(
    rng: np.random.Generator, *, ent_scale: float = 1.0
) -> pd.DataFrame:
    """200 entities × 100 rows, with an entity-level AND a within-entity row-level driver.

    ``ent_scale`` knobs the between-entity variance: ``1.0`` → high ICC (≈0.86, the
    de-mean power case); a small value (e.g. ``0.08``) → low ICC (≈0.04), where the
    row-level driver must surface in the row-wise PRIMARY on the raw measure (the
    low-ICC row-level recall case — no de-mean needed there).
    """
    ent = np.repeat(np.arange(CL_N_ENTITIES), CL_PER_ENTITY)
    n = CL_N_ENTITIES * CL_PER_ENTITY
    # Between-entity (ICC set by ent_scale): driver shift + an entity random effect.
    drv_grp = rng.integers(0, 4, CL_N_ENTITIES)
    ent_effect = ent_scale * (
        np.array([-3.0, -1.0, 1.0, 3.0])[drv_grp] + rng.normal(0, 3.0, CL_N_ENTITIES)
    )
    # Within-entity (row-level) driver shift (var ≈ 1.25) + row noise (var ≈ 1.0).
    row_grp = rng.integers(0, 4, n)
    row_shift = np.array([-1.5, -0.5, 0.5, 1.5])[row_grp]

    df = pd.DataFrame(index=np.arange(n))
    df[CL_ENTITY] = ent
    df["measure"] = 100.0 + ent_effect[ent] + row_shift + rng.normal(0, 1.0, n)
    df[CL_DRIVER] = [f"d{g}" for g in drv_grp[ent]]  # entity-level driver
    df[CL_ROW_DRIVER] = [f"w{g}" for g in row_grp]  # row-level driver
    df[CL_ENTITY_NULLS[1]] = [f"b{g}" for g in rng.integers(0, 30, CL_N_ENTITIES)[ent]]  # ent null
    df[CL_ROW_NULL] = [f"r{g}" for g in rng.integers(0, 6, n)]  # row-level null
    return df


# Two-ENTITY corpus (DAT-563): rows cross TWO recurring identities (customer, product),
# each with its own entity-level driver + null, plus a row-level null. The measure carries
# a LARGE customer random effect (higher ICC → customer is the primary entity grain) and a
# smaller product random effect (lower ICC → product is an entity-grain SECONDARY). Every
# candidate is constant within exactly one identity, so home-grain routing is unambiguous:
# customer attrs → customer grain, product attrs → product grain, the row null → row-wise.
TE_N_CUST = 120
TE_N_PROD = 40
TE_N_ROWS = 24_000
TE_CUST = "customer"
TE_PROD = "product"
TE_CUST_DRIVER = "D_cust"
TE_PROD_DRIVER = "D_prod"
TE_CUST_NULL = "N_cust"
TE_PROD_NULL = "N_prod"
TE_ROW_NULL = "N_row"
TE_DIMS = [TE_CUST_DRIVER, TE_PROD_DRIVER, TE_CUST_NULL, TE_PROD_NULL, TE_ROW_NULL]
TE_ENTITIES = [TE_CUST, TE_PROD]


def make_two_entity_corpus(rng: np.random.Generator) -> pd.DataFrame:
    """24k rows over 120 customers × 40 products; measure clusters within BOTH (customer > product)."""
    n = TE_N_ROWS
    cust = rng.integers(0, TE_N_CUST, n)
    prod = rng.integers(0, TE_N_PROD, n)
    # Customer-level driver + entity effect — the LARGER share (ICC ≈ 0.65 → customer is
    # primary AND clears the resolver's verify threshold).
    cust_drv = rng.integers(0, 4, TE_N_CUST)
    cust_effect = np.array([-3.0, -1.0, 1.0, 3.0])[cust_drv] + rng.normal(0, 2.5, TE_N_CUST)
    # Product-level driver + a smaller-but-real entity effect (ICC ≈ 0.27 → a verified
    # SECONDARY entity grain, below customer but above the 0.10 verify threshold). The
    # driver dominates the between-product variance (low entity noise) so it recalls
    # reliably at the 40-product entity grain.
    prod_drv = rng.integers(0, 4, TE_N_PROD)
    prod_effect = np.array([-2.5, -0.8, 0.8, 2.5])[prod_drv] + rng.normal(0, 1.0, TE_N_PROD)

    df = pd.DataFrame(index=np.arange(n))
    df[TE_CUST] = cust
    df[TE_PROD] = prod
    df["measure"] = 100.0 + cust_effect[cust] + prod_effect[prod] + rng.normal(0, 1.0, n)
    df[TE_CUST_DRIVER] = [f"cd{g}" for g in cust_drv[cust]]  # constant within customer
    df[TE_PROD_DRIVER] = [f"pd{g}" for g in prod_drv[prod]]  # constant within product
    df[TE_CUST_NULL] = [f"cn{g}" for g in rng.integers(0, 6, TE_N_CUST)[cust]]  # cust-level null
    df[TE_PROD_NULL] = [f"pn{g}" for g in rng.integers(0, 6, TE_N_PROD)[prod]]  # prod-level null
    df[TE_ROW_NULL] = [f"r{g}" for g in rng.integers(0, 6, n)]  # row-level null
    return df


# Clustered RATIO corpus (DAT-552 #321 fold): the per-row ratio (num/den) carries a
# per-entity level (high ICC on the ratio); an entity-level driver shifts that level;
# the denominator (volume) varies independently. Tests cluster-aware ratio.
CL_RATIO_DIMS = [CL_DRIVER, *CL_ENTITY_NULLS]  # all entity-level


def make_clustered_ratio_corpus(
    rng: np.random.Generator, *, ent_ratio_sigma: float = 0.05, row_sigma: float = 0.02
) -> pd.DataFrame:
    """200 entities × 100 rows; the RATIO num/den clusters within entity (high ICC)."""
    ent = np.repeat(np.arange(CL_N_ENTITIES), CL_PER_ENTITY)
    drv_grp = rng.integers(0, 4, CL_N_ENTITIES)
    drv_shift = np.array([-0.10, -0.03, 0.03, 0.10])[drv_grp]  # ratio shift (pp) by driver
    ent_ratio = 0.30 + drv_shift + rng.normal(0, ent_ratio_sigma, CL_N_ENTITIES)
    n = CL_N_ENTITIES * CL_PER_ENTITY
    den = rng.lognormal(mean=5.0, sigma=0.8, size=n)  # volume, varies independently
    row_ratio = ent_ratio[ent] + rng.normal(0, row_sigma, n)

    df = pd.DataFrame(index=np.arange(n))
    df[CL_ENTITY] = ent
    df["numerator"] = den * row_ratio
    df["denominator"] = den
    df[CL_DRIVER] = [f"d{g}" for g in drv_grp[ent]]
    df[CL_ENTITY_NULLS[0]] = [f"a{g}" for g in rng.integers(0, 6, CL_N_ENTITIES)[ent]]
    df[CL_ENTITY_NULLS[1]] = [f"b{g}" for g in rng.integers(0, 30, CL_N_ENTITIES)[ent]]
    return df


# Two-driver clustered RATIO corpus (DAT-561): a HIGH-ICC ratio carrying BOTH a
# between-entity (entity-level) ratio driver AND a within-entity (row-level) ratio
# driver, plus a null at each grain — the ratio analogue of make_clustered_two_driver_
# corpus. The within-entity de-mean (volume-weighted) must recover the row driver that
# the between-entity ratio level dilutes in the raw row-wise null.
CL_RATIO_ROW_DRIVER = "D_row_ratio"  # within-entity (row-level) ratio driver
RATIO_TWO_DRIVER_DIMS = [CL_DRIVER, CL_RATIO_ROW_DRIVER, CL_ENTITY_NULLS[1], CL_ROW_NULL]


def make_clustered_ratio_two_driver_corpus(rng: np.random.Generator) -> pd.DataFrame:
    """200 entities × 100 rows; high-ICC ratio with an entity-level AND a row-level driver."""
    ent = np.repeat(np.arange(CL_N_ENTITIES), CL_PER_ENTITY)
    n = CL_N_ENTITIES * CL_PER_ENTITY
    # Between-entity ratio level (high ICC): driver shift + a sizeable entity effect.
    drv_grp = rng.integers(0, 4, CL_N_ENTITIES)
    ent_ratio = (
        0.30 + np.array([-0.10, -0.03, 0.03, 0.10])[drv_grp] + rng.normal(0, 0.08, CL_N_ENTITIES)
    )
    # Within-entity (row-level) ratio shift + small row noise.
    row_grp = rng.integers(0, 4, n)
    row_shift = np.array([-0.06, -0.02, 0.02, 0.06])[row_grp]
    den = rng.lognormal(mean=5.0, sigma=0.8, size=n)  # volume, varies independently
    row_ratio = ent_ratio[ent] + row_shift + rng.normal(0, 0.01, n)

    df = pd.DataFrame(index=np.arange(n))
    df[CL_ENTITY] = ent
    df["numerator"] = den * row_ratio
    df["denominator"] = den
    df[CL_DRIVER] = [f"d{g}" for g in drv_grp[ent]]  # entity-level ratio driver
    df[CL_RATIO_ROW_DRIVER] = [f"w{g}" for g in row_grp]  # within-entity ratio driver
    df[CL_ENTITY_NULLS[1]] = [f"b{g}" for g in rng.integers(0, 30, CL_N_ENTITIES)[ent]]  # ent null
    df[CL_ROW_NULL] = [f"r{g}" for g in rng.integers(0, 6, n)]  # row-level null
    return df
