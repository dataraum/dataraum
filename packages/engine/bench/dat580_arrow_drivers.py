"""DAT-580 spike — memory + speed of the drivers load/handoff: pandas vs Arrow→Polars.

The driver engine (``analysis/drivers/processor.py``) reads the enriched view at ROW
grain into memory once, then runs a 500-shuffle permutation null over it. Two costs
dominate and both are paid in the LOAD + HANDOFF, never in the numpy core:

1. the materialized frame (DuckDB ``.df()`` → pandas), and
2. the per-dim arrays handed to the tree (``{d: frame[d].astype(object).to_numpy()}``)
   — string dims become Python ``str`` objects (~50-80 B/value): the long pole that
   stays resident through all 500 shuffles.

This spike measures both paths against a synthetic 1M-row × 15-string-dim fact:

  * **pandas-baseline** — ``conn.execute(sql).df()`` + the current object-array handoff,
    entity collapse via ``groupby``, ICC factorize via ``pd.factorize``.
  * **arrow-polars** — ``conn.execute(sql).arrow()`` → ``pl.from_arrow`` (zero-copy),
    dims factorized to physical ``int32`` codes BEFORE numpy (so the str-object blowup
    never happens), measure cast to ``DOUBLE`` in SQL so ``.to_numpy()`` is a clean
    float view (no int→float null-upcast copy), arrays forced C-contiguous. Entity
    collapse via ``group_by``, within-entity de-mean via ``.over()`` window.

Both then run the SAME representative permutation workload so the handoff is proven to
feed the stat core and stays resident during measurement. ``criterion.py`` is NOT
modified: the baseline imports the real ``build_codes``/``variance_reduction`` (they take
object arrays); the polars path uses an inline ``_codes_from_physical`` that is exactly
what the criterion.py port would become (int codes in, no ``pd.unique`` loop).

Run:
    uv run python bench/dat580_arrow_drivers.py            # build + run both, print table
    uv run python bench/dat580_arrow_drivers.py --mode pandas   # one path (subprocess)
    uv run python bench/dat580_arrow_drivers.py --mode polars
"""

from __future__ import annotations

import argparse
import resource
import subprocess
import sys
import time
from pathlib import Path

import duckdb
import numpy as np

DB_PATH = Path("/tmp/dat580_bench.duckdb")
N_ROWS = 1_000_000
N_ENTITIES = 5_000
N_PERM = 500
SEED = 7

# 15 string dims with a realistic spread of cardinalities (region/product/channel/...).
DIM_CARDS = [3, 5, 8, 12, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10, 30, 7]
DIMS = [f"d{i:02d}" for i in range(len(DIM_CARDS))]


def _peak_rss_bytes() -> int:
    """Process peak RSS. ru_maxrss is bytes on macOS, KiB on Linux."""
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss if sys.platform == "darwin" else rss * 1024


def _mb(n: float) -> str:
    return f"{n / 1e6:,.1f} MB"


def build_db() -> None:
    """Materialize the synthetic fact once into a DuckDB file (read by both paths)."""
    if DB_PATH.exists():
        DB_PATH.unlink()
    con = duckdb.connect(str(DB_PATH))
    # Pseudo-random but deterministic dims via hash(i*salt) % card; ~10% NULL measure.
    dim_cols = ",\n  ".join(
        f"('{d}_' || (hash(i * {salt + 1}) % {card}))::VARCHAR AS {d}"
        for d, card, salt in zip(DIMS, DIM_CARDS, range(len(DIMS)), strict=True)
    )
    con.execute(f"""
        CREATE TABLE fact AS
        SELECT
          i AS row_id,
          ('e' || (hash(i * 99) % {N_ENTITIES}))::VARCHAR AS entity,
          {dim_cols},
          CASE WHEN (hash(i * 7919) % 10) = 0 THEN NULL
               ELSE (hash(i * 104729) % 100000) / 100.0 END::DOUBLE AS m
        FROM range({N_ROWS}) t(i)
    """)  # noqa: S608 — generated identifiers, benchmark only
    (n,) = con.execute("SELECT COUNT(*) FROM fact").fetchone()
    con.close()
    print(f"built {DB_PATH} — {n:,} rows × {len(DIMS)} dims + entity + measure")


def _select_sql() -> str:
    cols = ", ".join([*DIMS, "entity", "m::DOUBLE AS m"])
    return f"SELECT {cols} FROM fact"  # noqa: S608 — fixed identifiers


def _handoff_bytes_object(values_by_dim: dict[str, np.ndarray]) -> int:
    """Resident bytes of an object-array handoff: pointer arrays + unique str objects."""
    total = 0
    seen: set[int] = set()
    for arr in values_by_dim.values():
        total += arr.nbytes  # the pointer array itself (8 B/row)
        for s in arr:
            if s is not None and id(s) not in seen:
                seen.add(id(s))
                total += sys.getsizeof(s)
    return total


def _handoff_bytes_codes(codes_by_dim: dict[str, np.ndarray]) -> int:
    return sum(a.nbytes for a in codes_by_dim.values())


def _codes_from_physical(phys: np.ndarray, measure: np.ndarray, gate: float = 0.5):
    """The criterion.py port: build group codes from pre-factorized physical codes.

    Same two null gates as ``criterion.build_codes`` (A: dim-present → -1; B: drop a
    slice whose measure is disproportionately missing), but vectorized over int codes —
    no ``pd.unique`` + per-label Python loop, and the input is int32, not str objects.
    """
    n_phys = int(phys.max()) + 1 if phys.size and phys.max() >= 0 else 0
    observed = ~np.isnan(measure)
    present = phys >= 0
    baseline = observed[present].mean() if present.any() else 0.0
    # per-physical-code observed rate (vectorized B-gate)
    tot = np.bincount(phys[present], minlength=n_phys)
    obs = np.bincount(phys[present & observed], minlength=n_phys)
    with np.errstate(invalid="ignore", divide="ignore"):
        rate = np.where(tot > 0, obs / np.maximum(tot, 1), 0.0)
    kept = rate >= gate * baseline
    remap = np.full(n_phys, -1, dtype=np.int64)
    remap[kept] = np.arange(int(kept.sum()))
    codes = np.where(present, remap[np.where(present, phys, 0)], -1)
    return codes.astype(np.int64), int(kept.sum())


def run_pandas() -> dict[str, float]:
    """Baseline: DuckDB .df() + the current object-array handoff + pandas groupby/factorize."""
    import pandas as pd

    from dataraum.analysis.drivers.criterion import build_codes, variance_reduction

    con = duckdb.connect(str(DB_PATH), read_only=True)
    t0 = time.perf_counter()
    frame: pd.DataFrame = con.execute(_select_sql()).df()
    t_load = time.perf_counter() - t0

    # --- handoff exactly as processor.py:675 ---
    measure = frame["m"].to_numpy(dtype=float)
    values_by_dim = {d: frame[d].astype(object).to_numpy() for d in DIMS}
    handoff = _handoff_bytes_object(values_by_dim)

    # --- representative group_by / window / factorize the port must cover ---
    t1 = time.perf_counter()
    g = frame.groupby("entity")["m"].agg(["mean", "count"])  # _collapse_to_entity
    _ = g["mean"].to_numpy(), g["count"].to_numpy()
    ent_codes, ent_uniques = pd.factorize(frame["entity"])  # _entity_icc
    _ = frame.groupby("entity")["m"].transform("mean").to_numpy()  # _within_entity_residual
    t_groupby = time.perf_counter() - t1

    # --- the long pole: build codes once per dim, 500 shuffles of max-gain ---
    t2 = time.perf_counter()
    rng = np.random.default_rng(SEED)
    coded = [build_codes(values_by_dim[d], measure, handle_nulls=True) for d in DIMS]
    perm_max = np.empty(N_PERM)
    for i in range(N_PERM):
        sh = rng.permutation(measure)
        perm_max[i] = max(variance_reduction(c, n, sh) for c, n in coded)
    t_perm = time.perf_counter() - t2

    return {
        "load_s": t_load,
        "groupby_s": t_groupby,
        "perm_s": t_perm,
        "handoff_bytes": handoff,
        "peak_rss": _peak_rss_bytes(),
        "_keep": (frame, values_by_dim, measure, ent_codes, perm_max),  # keep resident
    }


def run_polars() -> dict[str, float]:
    """Arrow→Polars: zero-copy load, physical-int dim codes, DOUBLE measure view."""
    import polars as pl

    con = duckdb.connect(str(DB_PATH), read_only=True)
    t0 = time.perf_counter()
    tbl = con.execute(_select_sql()).arrow()  # pyarrow.Table
    df = pl.from_arrow(tbl)  # zero-copy
    t_load = time.perf_counter() - t0

    # --- handoff: dims → physical int32 codes (tip #2/#3); measure → clean DOUBLE view ---
    # measure is already DOUBLE (cast in SQL, tip #1): no int→float null-upcast copy.
    measure = np.ascontiguousarray(df["m"].to_numpy(), dtype=np.float64)  # tip: order="C"
    phys_by_dim: dict[str, np.ndarray] = {}
    labels_by_dim: dict[str, list[str]] = {}
    for d in DIMS:
        cat = df[d].cast(pl.Categorical)
        # physical code (UInt32) → Int32, null → -1 sentinel (matches build_codes' -1)
        phys = cat.to_physical().cast(pl.Int32).fill_null(-1).to_numpy()
        phys_by_dim[d] = np.ascontiguousarray(phys)  # tip: order="C"
        labels_by_dim[d] = cat.cat.get_categories().to_list()
    handoff = _handoff_bytes_codes(phys_by_dim)

    # --- representative group_by / window / factorize, all in polars ---
    t1 = time.perf_counter()
    g = df.group_by("entity").agg(  # _collapse_to_entity
        pl.col("m").mean().alias("mean"), pl.col("m").count().alias("count")
    )
    _ = g["mean"].to_numpy(), g["count"].to_numpy()
    _ = df["entity"].cast(pl.Categorical).to_physical().to_numpy()  # _entity_icc factorize
    _ = df.select(pl.col("m").mean().over("entity"))  # _within_entity_residual window
    t_groupby = time.perf_counter() - t1

    # --- the long pole, on int codes via the port shim ---
    t2 = time.perf_counter()
    rng = np.random.default_rng(SEED)
    coded = [_codes_from_physical(phys_by_dim[d], measure) for d in DIMS]
    perm_max = np.empty(N_PERM)
    for i in range(N_PERM):
        sh = rng.permutation(measure)
        perm_max[i] = max(_variance_reduction_codes(c, n, sh) for c, n in coded)
    t_perm = time.perf_counter() - t2

    return {
        "load_s": t_load,
        "groupby_s": t_groupby,
        "perm_s": t_perm,
        "handoff_bytes": handoff,
        "peak_rss": _peak_rss_bytes(),
        "_keep": (df, phys_by_dim, measure, perm_max),  # keep resident
    }


def _variance_reduction_codes(codes: np.ndarray, n_codes: int, measure: np.ndarray) -> float:
    """variance_reduction on int codes (identical math to criterion.variance_reduction)."""
    observed = ~np.isnan(measure)
    keep = (codes >= 0) & observed
    if int(keep.sum()) < 200 or n_codes == 0:
        return 0.0
    c, y = codes[keep], measure[keep]
    counts = np.bincount(c, minlength=n_codes)
    sums = np.bincount(c, weights=y, minlength=n_codes)
    sq = np.bincount(c, weights=y * y, minlength=n_codes)
    big = counts >= 200
    if int(big.sum()) < 2:
        return 0.0
    n_big, s_big, sq_big = counts[big], sums[big], sq[big]
    total_n = n_big.sum()
    gmean = s_big.sum() / total_n
    total_var = sq_big.sum() / total_n - gmean**2
    if total_var <= 0:
        return 0.0
    within = (sq_big.sum() - np.sum(s_big * s_big / n_big)) / total_n
    return max(0.0, float((total_var - within) / total_var))


def _print_table(p: dict[str, float], q: dict[str, float]) -> None:
    rows = [
        ("load (DuckDB→frame)", f"{p['load_s']:.2f} s", f"{q['load_s']:.2f} s"),
        ("group_by/window/factorize", f"{p['groupby_s']:.2f} s", f"{q['groupby_s']:.2f} s"),
        ("permutation null (500×)", f"{p['perm_s']:.2f} s", f"{q['perm_s']:.2f} s"),
        ("handoff resident", _mb(p["handoff_bytes"]), _mb(q["handoff_bytes"])),
        ("PEAK RSS (process)", _mb(p["peak_rss"]), _mb(q["peak_rss"])),
    ]
    w = max(len(r[0]) for r in rows)
    print(f"\n{'metric':<{w}}  {'pandas-baseline':>18}  {'arrow-polars':>18}  {'delta':>10}")
    print("-" * (w + 52))
    for name, a, b in rows:
        try:
            av = float(a.split()[0].replace(",", ""))
            bv = float(b.split()[0].replace(",", ""))
            delta = f"{(bv - av) / av * 100:+.0f}%" if av else "—"
        except ValueError:
            delta = "—"
        print(f"{name:<{w}}  {a:>18}  {b:>18}  {delta:>10}")


def main() -> None:
    """Build the fact, then run each path in its own subprocess and print the table."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["pandas", "polars"])
    ap.add_argument("--build", action="store_true")
    args = ap.parse_args()

    if args.build:
        build_db()
        return

    if args.mode:
        res = run_pandas() if args.mode == "pandas" else run_polars()
        print(
            f"MODE={args.mode} "
            f"load={res['load_s']:.2f}s groupby={res['groupby_s']:.2f}s "
            f"perm={res['perm_s']:.2f}s handoff={res['handoff_bytes']} "
            f"peak_rss={res['peak_rss']}"
        )
        return

    # Orchestrator: build once, run each path in its own subprocess (isolated peak RSS).
    build_db()
    out: dict[str, dict[str, float]] = {}
    for mode in ("pandas", "polars"):
        cp = subprocess.run(
            [sys.executable, __file__, "--mode", mode],
            capture_output=True,
            text=True,
            check=True,
        )
        line = [ln for ln in cp.stdout.splitlines() if ln.startswith("MODE=")][0]
        kv = dict(tok.split("=") for tok in line.split()[1:])
        out[mode] = {
            "load_s": float(kv["load"][:-1]),
            "groupby_s": float(kv["groupby"][:-1]),
            "perm_s": float(kv["perm"][:-1]),
            "handoff_bytes": float(kv["handoff"]),
            "peak_rss": float(kv["peak_rss"]),
        }
        print(f"  {mode}: {line}")
    _print_table(out["pandas"], out["polars"])


if __name__ == "__main__":
    main()
