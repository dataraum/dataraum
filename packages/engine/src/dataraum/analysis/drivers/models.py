"""Result types for the driver-discovery engine (DAT-545).

In-memory only — DAT-545 is the engine, not the persisted artifact (that is
DAT-546). ``Measure`` is the engine's input value object; the rest is its output.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class RankingStatus(Enum):
    """Whether a :class:`DriverRanking` was actually ranked, or abstained (DAT-859).

    A measure whose ``temporal_behavior`` is NULL/undetermined (the upstream fails
    closed post-DAT-847) must never be silently ranked as a flow — that was the
    landed-contract breach this closes. MEASURED/ABSTAINED is a loud, typed pair
    carried on every :class:`DriverRanking` and its persisted
    ``DriverRankingArtifact``, honored at every read site. It is NOT inferred from
    an empty ``ranked_dimensions``: a MEASURED ranking legitimately has an empty
    ``ranked_dimensions`` too (the tree ran and found no significant driver — a
    real answer, not an abstention).
    """

    MEASURED = "measured"
    ABSTAINED = "abstained"


class AbstainReason(Enum):
    """Closed vocabulary for why a :class:`DriverRanking` abstained (DAT-859).

    Names align with ``entropy.models.ABSTAIN_REASONS`` where the shape matches
    (``missing_inputs``) — a naming convention only; this module imports none of
    that module's machinery (a driver ranking is not an entropy object).
    """

    # A required upstream signal is absent: the measure's temporal_behavior is
    # NULL/unmapped (resolve_target_type), the fact has no grain-verified
    # enriched view, or the enriched view is missing a column discovery needs
    # (a catalog/view skew).
    MISSING_INPUTS = "missing_inputs"
    # Fewer than two usable candidate dimensions survived — too few catalog
    # slice dims, or every candidate was dropped as an entity-key alias during
    # cluster-aware routing — so there is nothing to rank against.
    INSUFFICIENT_CANDIDATES = "insufficient_candidates"
    # Candidates and a target type exist, but no ranking unit (row or entity)
    # carried a usable measure value.
    INSUFFICIENT_DATA = "insufficient_data"


@dataclass(frozen=True)
class TargetTypeResolution:
    """The outcome of mapping a measure's catalog ``temporal_behavior`` (DAT-859).

    MEASURED carries the resolved ``target_type`` (``flow``/``stock``); ABSTAINED
    carries none — the caller must never invent one (the bug this closes: NULL/
    unmapped ``temporal_behavior`` silently defaulting to ``"flow"``). Produced by
    :func:`dataraum.analysis.drivers.processor.resolve_target_type_for_behavior`,
    one level up from a :class:`DriverRanking` — before a :class:`Measure` can
    even be constructed.
    """

    status: RankingStatus
    target_type: str | None = None
    abstain_reason: AbstainReason | None = None

    def __post_init__(self) -> None:
        if self.status == RankingStatus.MEASURED:
            if self.target_type is None:
                raise ValueError("measured TargetTypeResolution requires target_type")
            if self.abstain_reason is not None:
                raise ValueError("measured TargetTypeResolution must not carry abstain_reason")
        else:
            if self.target_type is not None:
                raise ValueError("abstained TargetTypeResolution must not carry target_type")
            if self.abstain_reason is None:
                raise ValueError("abstained TargetTypeResolution requires abstain_reason")


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
class SecondaryDriver:
    """A significant dim from a NON-primary grain family (DAT-561/563).

    The cluster-aware search runs one family per resolved entity (entity-constant
    candidates at that entity's grain) plus a row-level family, and reports ONE as the
    primary tree (the highest-ICC entity, or row-wise when nothing clusters). Every
    other family's significant dims land here instead of in ``ranked_dimensions``: their
    gains were computed at a different exchangeable grain (``grain``), so they are NOT
    comparable to the primary tree's gains and must not be folded into the same ranking.
    A flat labeled list — strongest-first WITHIN each family's block, the family blocks in
    primary-precedence order (gains are not comparable across grains, so there is no global
    sort).

    ``entity`` names the identity column whose grain this dim was ranked at (DAT-563
    N-entity routing) — ``None`` for the row-level family. It disambiguates the now-many
    ``grain == "entity"`` families (one per resolved identity: customer, product, …).
    """

    dimension: str
    gain: float
    grain: str  # the exchangeable unit this dim's null used: "entity" or "row"
    entity: str | None = None  # the identity column this dim's entity grain belongs to


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

    ``grain`` is the exchangeable unit the PRIMARY family's null used: ``"row"`` (the
    default) or ``"entity"`` when the cluster-aware path made an entity-grain family
    primary (DAT-552/561). ``entity`` names WHICH identity column that entity grain
    belongs to (DAT-563) — ``None`` at row grain; with N resolved identities the primary
    is the highest-ICC one, so the headline must say which. ``n_rows`` is the count of
    those units (rows, or entities at entity grain) — i.e. the effective sample size the
    power scales with, so a "no significant driver" result on few entities is honestly
    attributable.

    ``secondary_dimensions`` carries every NON-primary grain family's significant dims
    when cluster keys are present (DAT-561/563 home-grain routing): each candidate is
    ranked at exactly one home grain — the entity it is constant within (entity grain) or
    row-wise — and one family is chosen primary (the highest-ICC entity, or row-wise when
    nothing clusters). All other families surface here as a flat list, each
    ``SecondaryDriver`` labeled with its ``grain`` and ``entity`` — never mixed into
    ``ranked_dimensions``/``root`` (the grains are not cross-comparable). With N=1 this is
    exactly DAT-561's primary/secondary split.

    ``status``/``abstain_reason`` (DAT-859) are the typed abstention pair: MEASURED
    is the default (every ranking the engine actually computed, including a
    legitimate "no significant driver" empty ``ranked_dimensions``); ABSTAINED marks
    the honest-empty construction sites (no enriched view, too few candidates, no
    usable measure value) AND an unresolved ``temporal_behavior`` upstream of
    :class:`Measure` — the caller must persist an abstention rather than guess a
    target type. An abstained ranking's OWN primary story never carries content —
    ``root``/``ranked_dimensions``/``driver_paths``/``interesting_slices`` are left
    at their empty defaults by every construction site (a discipline, not a
    ``__post_init__``-enforced invariant — see below for why) — but
    ``secondary_dimensions`` is deliberately not even part of that discipline:
    ``processor._routed_ranking`` can
    attach a NON-primary grain's real findings to an abstained primary (the
    fallback-primary case — every bucket-0/1 family is empty, an abstained
    entity-grain family is picked, and a demoted low-ICC family still found
    something), which is a pre-existing, correct shape (unchanged by DAT-859). Every
    read site gates on ``status`` alone (never on emptiness), so an abstained
    ranking's ``secondary_dimensions`` is inert at every consumer regardless —
    "abstained rankings never render as content" already holds without needing to
    forbid this field too.
    """

    measure: str
    target_type: str | None  # None only when status is ABSTAINED and the type itself is unknown
    n_rows: int
    status: RankingStatus = RankingStatus.MEASURED
    abstain_reason: AbstainReason | None = None
    grain: str = "row"
    entity: str | None = None  # which identity the primary entity grain belongs to (DAT-563)
    ranked_dimensions: list[tuple[str, float]] = field(default_factory=list)
    root: DriverNode | None = None
    driver_paths: list[list[str]] = field(default_factory=list)
    interesting_slices: list[DriverSlice] = field(default_factory=list)
    secondary_dimensions: list[SecondaryDriver] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Enforce the status/abstain_reason pairing at construction (fail loud).

        Mirrors ``entropy.models.EntropyObject.__post_init__``: this dataclass is
        the single creation chokepoint (every construction site in ``processor.py``
        + ``persistence.py`` goes through it), so an invalid combination — a
        measured ranking carrying a reason, or an abstention with none — never
        reaches persistence or a read site. Does NOT forbid ranked content on an
        abstained ranking's ``secondary_dimensions`` — see the class docstring.
        """
        if self.status == RankingStatus.MEASURED:
            if self.abstain_reason is not None:
                raise ValueError(
                    f"measured DriverRanking must not carry abstain_reason ({self.measure})"
                )
        elif self.abstain_reason is None:  # ABSTAINED
            raise ValueError(f"abstained DriverRanking requires abstain_reason ({self.measure})")
