"""The family-duplicate resolution (DAT-856, corrected by DAT-891).

The DAT-891 defect: with BOTH directions of a settlement family decided
(accounts_receivable incoming 0.90, accounts_payable outgoing 0.92 on the eval
corpus), neither tie-break branch of the old one-per-family rule applied and
the survivor was dict insertion order — silently dropping a correct,
higher-confidence member. These tests pin the corrected contract directly on
the extracted function; no test existed on this logic before, which is how an
insertion-order outcome shipped.
"""

from dataraum.analysis.cycles.config import UNDETERMINED_DIRECTION
from dataraum.analysis.cycles.models import DetectedCycle
from dataraum.pipeline.phases.business_cycles_phase import _resolve_family_duplicates


def _cycle(
    canonical_type: str,
    family: str | None,
    direction: str | None,
) -> DetectedCycle:
    return DetectedCycle(
        cycle_id=f"id_{canonical_type}",
        cycle_name=canonical_type.replace("_", " ").title(),
        cycle_type=canonical_type,
        canonical_type=canonical_type,
        is_known_type=True,
        family=family,
        direction=direction,
        description=f"{canonical_type} detection",
    )


def test_both_decided_directions_of_a_family_coexist() -> None:
    """The DAT-891 repro: incoming AND outgoing decided → both persist."""
    detected = {
        "accounts_receivable": _cycle("accounts_receivable", "settlement", "incoming"),
        "accounts_payable": _cycle("accounts_payable", "settlement", "outgoing"),
    }
    _resolve_family_duplicates(detected)
    assert set(detected) == {"accounts_receivable", "accounts_payable"}


def test_decided_member_beats_the_undetermined_family_claim() -> None:
    """DAT-856's rule survives: undetermined claims the whole family and is
    redundant beside any decided sibling."""
    detected = {
        "settlement": _cycle("settlement", "settlement", UNDETERMINED_DIRECTION),
        "accounts_receivable": _cycle("accounts_receivable", "settlement", "incoming"),
    }
    _resolve_family_duplicates(detected)
    assert set(detected) == {"accounts_receivable"}


def test_outcome_is_insertion_order_independent() -> None:
    """The defect was insertion-order survivorship — assert both orders give
    the identical outcome for both conflict shapes."""
    for order in (
        ["settlement", "accounts_payable"],
        ["accounts_payable", "settlement"],
    ):
        cycles = {
            "settlement": _cycle("settlement", "settlement", UNDETERMINED_DIRECTION),
            "accounts_payable": _cycle("accounts_payable", "settlement", "outgoing"),
        }
        detected = {k: cycles[k] for k in order}
        _resolve_family_duplicates(detected)
        assert set(detected) == {"accounts_payable"}, order


def test_lone_undetermined_family_claim_survives() -> None:
    """No decided sibling → the undetermined cycle IS the family's detection
    (the distinguishable undirected state DAT-856 specified)."""
    detected = {"settlement": _cycle("settlement", "settlement", UNDETERMINED_DIRECTION)}
    _resolve_family_duplicates(detected)
    assert set(detected) == {"settlement"}


def test_familyless_cycles_are_untouched() -> None:
    detected = {
        "period_close": _cycle("period_close", None, None),
        "accounts_receivable": _cycle("accounts_receivable", "settlement", "incoming"),
    }
    _resolve_family_duplicates(detected)
    assert set(detected) == {"period_close", "accounts_receivable"}
