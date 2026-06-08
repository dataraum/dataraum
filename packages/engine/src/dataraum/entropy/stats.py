"""Pure measurement statistics — the math behind the measurement table (ADR-0009).

Each function maps raw column data → a value in ``[0, 1]``. No config, no I/O, no
boost curves, no magic constants: a measurement is a *pure function*, testable in
microseconds on synthetic or recorded data (``entropy_eval_architecture.md``). The
engine detectors and the eval Tier-1/2 tests import the SAME function, so there is
one implementation of each statistic, proven once.

Only *tunable entropy* a teach can close lives here (completeness, type fidelity,
referential integrity, derived-formula match, time role, naming/unit confidence,
cross-column dependency). Informative signals no teach resolves (benford, outliers,
drift, variance) are column context, not measurements — they belong elsewhere.

Convention: a HIGHER value always means MORE entropy / more concern (a teach is
warranted). 0.0 = clean.
"""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Hashable, Iterable, Sequence

_MISSING: frozenset[object] = frozenset({None, ""})


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


def rate(count: int, total: int) -> float:
    """A bounded incidence rate ``count / total`` in ``[0, 1]``.

    The shape of every raw-rate measurement (FK violations, formula mismatches, …).
    ``total <= 0`` → 0.0.
    """
    if total <= 0:
        return 0.0
    return _clamp01(count / total)


def null_ratio(cells: Sequence[object]) -> float:
    """Missing fraction of a column → completeness entropy.

    ``0.0`` = fully populated, ``1.0`` = entirely missing. The raw rate, no boost:
    a 40% missing column scores 0.40 (teach: fill, or document the nulls as expected).

    Args:
        cells: one raw cell per row, missing cells as ``None`` or ``""``.
    """
    if not cells:
        return 0.0
    missing = sum(1 for c in cells if c in _MISSING)
    return missing / len(cells)


def orphan_rate(child_keys: Sequence[object], parent_keys: Iterable[object]) -> float:
    """Referential-integrity entropy: fraction of non-null child FK values that orphan.

    ``0.0`` = every child resolves, ``1.0`` = every child is an orphan. Null child keys
    are not orphans (absent, not dangling). (teach: define / fix the relationship.)
    """
    parents = set(parent_keys)
    present = [k for k in child_keys if k not in _MISSING]
    if not present:
        return 0.0
    orphans = sum(1 for k in present if k not in parents)
    return orphans / len(present)


def type_fidelity(parse_success_rate: float, quarantine_rate: float = 0.0) -> float:
    """Type-cast entropy: the worse of the parse-failure and quarantine fractions.

    ``0.0`` = every value typed cleanly. No VARCHAR-fallback mid-score — the absence of
    a typing verdict is *ignorance*, carried separately, not a fabricated 0.5.
    (teach: re-type / teach the pattern.)
    """
    return _clamp01(max(1.0 - parse_success_rate, quarantine_rate))


def time_role_mismatch(*, is_temporal_type: bool, is_timestamp_role: bool) -> float:
    """Structural time-role entropy: a timestamp role on a non-temporal type.

    ``1.0`` when a column is used as a timestamp but is not a temporal type (unparseable
    dates fell back to VARCHAR), else ``0.0``. (teach: re-type / mark the time role.)
    """
    return 1.0 if (is_timestamp_role and not is_temporal_type) else 0.0


def confidence_entropy(confidence: float) -> float:
    """``1 − confidence`` — the model's uncertainty about a declaration.

    A column's meaning or a unit: garbage names / undeclared units read low confidence →
    high entropy. Pure model confidence, no deterministic override (ADR-0009 hard rule
    against deterministic semantic patches). (teach: name the column / declare the unit.)
    """
    return _clamp01(1.0 - confidence)


def nmi(x: Sequence[Hashable], y: Sequence[Hashable]) -> float:
    """Normalized mutual information ``NMI(X;Y) = MI / sqrt(H(X)·H(Y))`` in ``[0, 1]``.

    The cross-column dependency statistic. ``1.0`` = one column determines the other (a
    strong, possibly UNDOCUMENTED dependency
    worth surfacing); ``0.0`` = independent. Pure contingency-table estimate over aligned
    label sequences (no discretization for indicators / categoricals). A constant column
    shares no information → ``0.0``. (teach: document the business rule.)
    """
    n = len(x)
    if n == 0:
        return 0.0
    px, py, pxy = Counter(x), Counter(y), Counter(zip(x, y, strict=True))
    hx = -sum((c / n) * math.log2(c / n) for c in px.values())
    hy = -sum((c / n) * math.log2(c / n) for c in py.values())
    if hx == 0.0 or hy == 0.0:
        return 0.0
    mi = sum(
        (c / n) * math.log2((c / n) / ((px[a] / n) * (py[b] / n))) for (a, b), c in pxy.items()
    )
    return max(0.0, mi / math.sqrt(hx * hy))
