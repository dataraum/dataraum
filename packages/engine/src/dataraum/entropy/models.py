"""Entropy layer data models.

This module defines the core data structures for the entropy layer.

Key models:
- EntropyObject: Core measurement with evidence

Default thresholds loaded from config/entropy/thresholds.yaml.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

# ---------------------------------------------------------------------------
# Measurement status vocabulary (DAT-853 abstention primitive).
#
# A detector either MEASURED its question (score carries the answer) or
# ABSTAINED ("I did not measure this") — a first-class outcome, never a skipped
# write, so "not measured" stays distinguishable from "measured clean" all the
# way to the readiness rollup. Closed vocabulary: enforced here at construction
# (the single writer chokepoint) and by CHECK constraints on
# ``entropy_objects`` (db_models.py).
# ---------------------------------------------------------------------------

STATUS_MEASURED = "measured"
STATUS_ABSTAINED = "abstained"
ENTROPY_STATUSES: tuple[str, ...] = (STATUS_MEASURED, STATUS_ABSTAINED)

# Why a detector abstained:
# - missing_inputs: required upstream analyses absent (harness, can_run False)
# - detector_error: load_data()/detect() raised (harness)
# - not_applicable: the statistic is undefined for this target (detector)
# - insufficient_data: below the detector's evidence floor (detector)
ABSTAIN_MISSING_INPUTS = "missing_inputs"
ABSTAIN_DETECTOR_ERROR = "detector_error"
ABSTAIN_NOT_APPLICABLE = "not_applicable"
ABSTAIN_INSUFFICIENT_DATA = "insufficient_data"
ABSTAIN_REASONS: tuple[str, ...] = (
    ABSTAIN_MISSING_INPUTS,
    ABSTAIN_DETECTOR_ERROR,
    ABSTAIN_NOT_APPLICABLE,
    ABSTAIN_INSUFFICIENT_DATA,
)

# Readiness-rollup coverage (DAT-853): the loss rollup's third outcome. The
# product band vocabulary (ready/investigate/blocked) stays frozen — band
# answers "how risky is what we measured", coverage answers "did the loss-path
# detectors actually measure": measured = every contributing loss-path detector
# measured; partial = some measured, some abstained; unmeasured = zero measured
# loss-path objects (the band is vacuous — previously this target silently got
# no readiness row at all and read as green).
COVERAGE_MEASURED = "measured"
COVERAGE_PARTIAL = "partial"
COVERAGE_UNMEASURED = "unmeasured"
COVERAGE_STATES: tuple[str, ...] = (
    COVERAGE_MEASURED,
    COVERAGE_PARTIAL,
    COVERAGE_UNMEASURED,
)


@dataclass
class WitnessClaim:
    """One witness's opinion on one claim slot (ADR-0009, DAT-457).

    Adjudication (pooling) detectors attach these to an :class:`EntropyObject`;
    the engine persists them as run-versioned ``ClaimWitnessRecord`` rows. They
    are the provenance behind a pooled ``(conflict, ignorance)``. ``claim_field``
    is the claim-slot identity (e.g. ``"null_token:TBD"``); ``distribution`` is
    label → probability over the canonical claim space.
    """

    claim_field: str
    witness_id: str
    distribution: dict[str, float]
    reliability: float


@dataclass
class EntropyObject:
    """Core entropy measurement object.

    Represents a single entropy measurement for a specific dimension/sub-dimension
    applied to a specific target (column, table, or relationship).
    """

    # Identity
    object_id: str = field(default_factory=lambda: str(uuid4()))
    layer: str = ""  # structural, semantic, value, computational
    dimension: str = ""  # schema, types, relations, business_meaning, units, etc.
    sub_dimension: str = ""  # naming_clarity, type_fidelity, etc.
    # Scope key: column:{t}.{c} / table:{t} / relationship:{from_col}::{to_col}.
    target: str = ""

    # Measurement. ``score`` is None exactly when the detector abstained — an
    # abstention carries no number, so no consumer can mistake it for
    # "measured clean" (DAT-853). ``abstain_reason`` is set exactly when
    # abstained (one of ABSTAIN_REASONS).
    score: float | None = 0.0  # 0.0 = deterministic, 1.0 = maximum uncertainty
    status: str = STATUS_MEASURED
    abstain_reason: str | None = None

    # Evidence (dimension-specific)
    evidence: list[dict[str, Any]] = field(default_factory=list)

    # Witness provenance for adjudication (pooled) measurements (ADR-0009).
    # The engine persists these to claim_witnesses, run-versioned, with the same
    # scoping as this object's record. Empty for non-pooled detectors.
    witnesses: list[WitnessClaim] = field(default_factory=list)

    # Metadata
    computed_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    source_analysis_ids: list[str] = field(default_factory=list)  # Links to source analyses
    detector_id: str = ""  # Which detector produced this

    def __post_init__(self) -> None:
        """Enforce the status/score/reason pairing at construction (fail loud).

        The dataclass is the single creation chokepoint for every detector and
        the record→object conversion, so an invalid combination (an abstention
        with a score, a measurement without one, an unknown vocabulary value)
        never reaches persistence or the rollup.
        """
        if self.status not in ENTROPY_STATUSES:
            raise ValueError(
                f"EntropyObject.status must be one of {ENTROPY_STATUSES}: {self.status!r}"
            )
        if self.status == STATUS_MEASURED:
            if self.score is None:
                raise ValueError(
                    f"measured EntropyObject requires a score ({self.detector_id} on {self.target})"
                )
            if self.abstain_reason is not None:
                raise ValueError(
                    f"measured EntropyObject must not carry abstain_reason ({self.detector_id} on {self.target})"
                )
        else:  # abstained
            if self.score is not None:
                raise ValueError(
                    f"abstained EntropyObject must not carry a score ({self.detector_id} on {self.target})"
                )
            if self.abstain_reason not in ABSTAIN_REASONS:
                raise ValueError(
                    f"abstained EntropyObject requires abstain_reason in {ABSTAIN_REASONS}: {self.abstain_reason!r}"
                )

    @property
    def measured_score(self) -> float:
        """The score of a MEASURED object; raises on an abstention (fail loud).

        The single narrowing point for score-consuming paths (loss, readiness,
        direct signals): an abstention reaching one of them is a caller bug —
        partition on ``status`` first.
        """
        if self.score is None:
            raise ValueError(
                f"abstained EntropyObject has no score: {self.detector_id} on {self.target}"
            )
        return self.score

    @property
    def dimension_path(self) -> str:
        """Return full dimension path (layer.dimension.sub_dimension)."""
        return f"{self.layer}.{self.dimension}.{self.sub_dimension}"


# Relationship target identity (DAT-408). The readiness/head key for a relationship
# is the directional column pair — column ids are fixed per session, so the key is
# replay-stable; ``::`` separates the two UUIDs (a ``-`` would be ambiguous inside
# them). One function so the detector (emit), the persist, and the reader (gate)
# agree on the exact string.
_REL_PREFIX = "relationship:"
_REL_SEP = "::"


def relationship_target_key(from_column_id: str, to_column_id: str) -> str:
    """The stable ``relationship:{from_col}::{to_col}`` target key (DAT-408)."""
    return f"{_REL_PREFIX}{from_column_id}{_REL_SEP}{to_column_id}"


def parse_relationship_target(target: str) -> tuple[str, str] | None:
    """Inverse of :func:`relationship_target_key` → ``(from_column_id, to_column_id)``.

    Returns ``None`` for any non-relationship or malformed target.
    """
    if not target.startswith(_REL_PREFIX):
        return None
    parts = target[len(_REL_PREFIX) :].split(_REL_SEP)
    if len(parts) != 2 or not all(parts):
        return None
    return parts[0], parts[1]
