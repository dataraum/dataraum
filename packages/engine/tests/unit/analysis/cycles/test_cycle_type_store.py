"""The typed cycle-type vocabulary — seed only (DAT-881, config→DB).

Pins the config→DB seam for the shipped cycle-type baseline: the shipped
vertical's ``cycles.yaml`` ``cycle_types`` block seeds typed ``CycleType`` rows
once (idempotently, ON CONFLICT DO NOTHING on the active-row index), reading the
SHIPPED base only — never the overlay-layered view a taught cycle would also
appear in. No reader is built here (nothing on the engine side consumes this
table); the cockpit's shipped-only readers query the mirrored view directly.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.cycles.cycle_type_store import ensure_cycle_types_seeded
from dataraum.analysis.cycles.db_models import CycleType


def _active(session: Session, vertical: str) -> dict[str, CycleType]:
    return {
        r.name: r
        for r in session.execute(
            select(CycleType).where(
                CycleType.vertical == vertical, CycleType.superseded_at.is_(None)
            )
        ).scalars()
    }


def test_seed_finance_creates_every_shipped_cycle_type(session: Session) -> None:
    n = ensure_cycle_types_seeded(session, "finance")
    rows = _active(session, "finance")
    assert n == len(rows)
    # The finance cycles.yaml ships these — a representative subset, not the whole
    # count (which would make this test brittle to future additions).
    assert {
        "order_to_cash",
        "accounts_receivable",
        "procure_to_pay",
        "accounts_payable",
        "period_close",
    } <= set(rows)
    for row in rows.values():
        assert row.source == "seed"


def test_seed_is_idempotent(session: Session) -> None:
    first = ensure_cycle_types_seeded(session, "finance")
    assert first > 0
    # A re-run is a no-op — ON CONFLICT DO NOTHING on the active-row index.
    assert ensure_cycle_types_seeded(session, "finance") == 0
    assert len(_active(session, "finance")) == first


def test_unknown_vertical_seeds_nothing_not_an_error(session: Session) -> None:
    assert ensure_cycle_types_seeded(session, "nonexistent") == 0
    assert _active(session, "nonexistent") == {}


def test_order_to_cash_round_trips_every_declared_field(session: Session) -> None:
    ensure_cycle_types_seeded(session, "finance")
    row = _active(session, "finance")["order_to_cash"]
    assert row.description == (
        "Complete revenue cycle from customer order through payment collection"
    )
    assert row.business_value == "high"
    assert set(row.aliases or []) == {"revenue_cycle", "sales_cycle", "o2c"}
    assert row.typical_stages is not None
    assert len(row.typical_stages) == 5
    first_stage = row.typical_stages[0]
    assert first_stage["name"] == "Order Placed"
    assert first_stage["order"] == 1
    assert first_stage["indicators"] == ["ordered", "new", "created", "pending"]
    assert row.completion_indicators == [
        "paid",
        "collected",
        "closed",
        "settled",
        "complete",
    ]
    assert row.feeds_into == ["accounts_receivable", "journal_entry_cycle"]


def test_a_cycle_type_with_no_feeds_into_round_trips_empty_list(
    session: Session,
) -> None:
    # intercompany_cycle ships with no `feeds_into:` key at all.
    ensure_cycle_types_seeded(session, "finance")
    row = _active(session, "finance")["intercompany_cycle"]
    assert row.feeds_into is None
