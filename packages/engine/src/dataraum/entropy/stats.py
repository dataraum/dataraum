"""Pure measurement statistics — the math behind the measurement table (ADR-0009).

Each function maps raw column data → a value in ``[0, 1]``. No config, no I/O, no
boost curves, no magic constants: a measurement is a *pure function*, testable in
milliseconds on synthetic or recorded data (``entropy_eval_architecture.md``). The
engine detectors and the eval Tier-1/2 tests import the SAME function, so there is
one implementation of each statistic, proven once.

Only *tunable entropy* a teach can close lives here (completeness, type fidelity,
referential integrity, derived-formula match, …). Informative signals no teach
resolves (benford, outliers, drift, variance) are column context, not measurements.
"""

from __future__ import annotations

from collections.abc import Sequence

_MISSING: frozenset[object] = frozenset({None, ""})


def null_ratio(cells: Sequence[object]) -> float:
    """Missing fraction of a column → completeness entropy.

    ``0.0`` = fully populated, ``1.0`` = entirely missing. The raw rate, no boost:
    a 40% missing column scores 0.40 (teach: fill, or document the nulls as expected).

    Args:
        cells: one raw cell per row, missing cells as ``None`` or ``""``.

    Returns:
        ``missing / total`` in ``[0, 1]`` (``0.0`` for an empty column).
    """
    if not cells:
        return 0.0
    missing = sum(1 for c in cells if c in _MISSING)
    return missing / len(cells)
