"""Result types for the driver-discovery engine (DAT-545).

In-memory only — DAT-545 is the engine, not the persisted artifact (that is
DAT-546). ``Measure`` is the engine's input value object; the rest is its output.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Measure:
    """The numeric target a driver search explains.

    ``target_type`` drives the criterion (DAT-545): ``flow`` (additive) and
    ``stock`` (point_in_time) variance-reduce the row-grain value directly; ``ratio``
    aggregates Σnumerator / Σdenominator per group, support-weighted (averaging
    ratios is invalid). The type is read from the catalog's ``temporal_behavior``;
    a ratio is a computed measure carrying both columns.
    """

    target_type: str  # 'flow' | 'stock' | 'ratio'
    column: str | None = None  # flow / stock
    numerator: str | None = None  # ratio
    denominator: str | None = None  # ratio

    def __post_init__(self) -> None:
        if self.target_type in ("flow", "stock"):
            if not self.column:
                raise ValueError(f"{self.target_type} measure needs `column`")
        elif self.target_type == "ratio":
            if not (self.numerator and self.denominator):
                raise ValueError("ratio measure needs `numerator` and `denominator`")
        else:
            raise ValueError(f"unknown target_type {self.target_type!r}")

    @property
    def label(self) -> str:
        """A human label for logs/diagnostics."""
        if self.target_type == "ratio":
            return f"{self.numerator}/{self.denominator}"
        return self.column or "?"


@dataclass(frozen=True)
class DriverSlice:
    """A filter slice where the measure deviates sharply from the node baseline.

    ``effect`` is the signed relative deviation of the slice's value from the node
    baseline (``group / baseline − 1``); the baseline is the node's dim-present mean
    (flow/stock) or pooled ratio (ratio). ``support`` is the slice's row count (it
    cleared ``min_support``).
    """

    dimension: str
    value: str
    effect: float
    support: int


@dataclass(frozen=True)
class DriverNode:
    """One surviving split: the dimension that best explains the node's measure.

    ``children`` are ``(slice_value, subtree)`` pairs — the deeper split found WITHIN
    that slice value's rows (the drill path). Empty at a leaf / at ``max_depth``.
    """

    dimension: str
    gain: float
    p_value: float
    # Dim-present rows at this node (codes ≥ 0) — INCLUDES rows in sub-min_support
    # groups that the gain itself excluded, so ``support`` ≥ Σ(slice.support).
    support: int
    slices: tuple[DriverSlice, ...]
    children: tuple[tuple[str, DriverNode], ...] = ()


@dataclass(frozen=True)
class DriverRanking:
    """The engine's output for one measure.

    ``ranked_dimensions`` = the significant dims at the root by gain (the best
    aggregation vectors); ``root`` = the greedy driver tree; ``driver_paths`` = the
    surviving dimension drill vectors; ``interesting_slices`` = the sharp-deviation
    slices across the tree, strongest first.
    """

    measure: str
    target_type: str
    n_rows: int
    ranked_dimensions: list[tuple[str, float]] = field(default_factory=list)
    root: DriverNode | None = None
    driver_paths: list[list[str]] = field(default_factory=list)
    interesting_slices: list[DriverSlice] = field(default_factory=list)
