"""Validation spec loading — the typed DB home ⊕ teach overlay (DAT-735).

The loader reads the ``validations`` typed home (seed ⊕ generated rows) and layers
the ``validation`` teach overlay over it — replacing the pre-DAT-735 raw YAML
directory walk. The seed is the shipped vertical YAML normalized into typed rows
(``ensure_validations_seeded``); the ``verticals_dir`` escape hatch still reads raw
YAML and bypasses both the DB home and the overlay.
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from dataraum.analysis.validation.config import (
    get_validation_specs_for_cycles,
    load_all_validation_specs,
)
from dataraum.analysis.validation.validation_store import ensure_validations_seeded
from dataraum.core.overlay import (
    OverlayRow,
    reset_overlay_resolver_for_tests,
    set_overlay_resolver,
)

VERTICAL = "finance"


class TestLoadAllValidationSpecs:
    """Loading the declared set from the typed DB home."""

    def test_loads_seeded_specs_from_db_home(self, session: Session):
        """The shipped finance validations, once seeded, load from the typed home."""
        ensure_validations_seeded(session, VERTICAL)
        specs = load_all_validation_specs(VERTICAL, session)

        assert len(specs) >= 4
        assert "double_entry_balance" in specs
        assert "trial_balance" in specs
        assert "sign_conventions" in specs
        # The check LOGIC is typed (DAT-735): tolerance is a float, not a dict entry.
        assert specs["double_entry_balance"].tolerance == 0.01
        # sql_hints is gone; the binding prose lives in guidance.
        assert specs["double_entry_balance"].guidance

    def test_each_load_returns_fresh_dict(self, session: Session):
        """Each call returns a new dict (no caching)."""
        ensure_validations_seeded(session, VERTICAL)
        specs1 = load_all_validation_specs(VERTICAL, session)
        specs2 = load_all_validation_specs(VERTICAL, session)

        assert specs1 is not specs2
        assert specs1.keys() == specs2.keys()

    def test_unseeded_vertical_resolves_empty(self, session: Session):
        """An unknown / framed vertical with no rows + no overlay resolves empty."""
        assert load_all_validation_specs("nonexistent_vertical_xyz", session) == {}

    def test_no_session_no_dir_resolves_empty(self):
        """No DB session and no fixture dir → EMPTY (nothing to read), never raises."""
        assert load_all_validation_specs(VERTICAL) == {}


def _spec_payload(validation_id: str, **overrides) -> dict:
    """A minimal valid ``validation`` overlay payload for VERTICAL."""
    payload = {
        "vertical": VERTICAL,
        "validation_id": validation_id,
        "name": validation_id.replace("_", " ").title(),
        "description": "taught via overlay",
        "category": "financial",
        "check_type": "balance",
    }
    payload.update(overrides)
    return payload


class TestOverlayAwareLoading:
    """``validation`` teach overlay rows merge over the typed DB home (DAT-735)."""

    def teardown_method(self) -> None:
        reset_overlay_resolver_for_tests()

    def test_overlay_row_adds_a_spec(self, session: Session):
        ensure_validations_seeded(session, VERTICAL)
        set_overlay_resolver(
            lambda: [OverlayRow(type="validation", payload=_spec_payload("taught_check"))]
        )
        specs = load_all_validation_specs(VERTICAL, session)

        assert "taught_check" in specs
        assert specs["taught_check"].description == "taught via overlay"
        # Seeded specs survive alongside the teach.
        assert "double_entry_balance" in specs

    def test_overlay_row_replaces_seeded_spec_by_id(self, session: Session):
        """A teach row's legacy ``parameters.tolerance`` normalizes onto the typed field."""
        ensure_validations_seeded(session, VERTICAL)
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="validation",
                    payload=_spec_payload("double_entry_balance", parameters={"tolerance": 5.0}),
                )
            ]
        )
        specs = load_all_validation_specs(VERTICAL, session)

        assert specs["double_entry_balance"].tolerance == 5.0
        assert specs["double_entry_balance"].description == "taught via overlay"

    def test_framed_vertical_resolves_overlay_only(self, session: Session):
        """A framed vertical (no seed rows) is served by its overlay rows alone."""
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="validation",
                    payload=_spec_payload("framed_check", vertical="framed_v"),
                )
            ]
        )
        specs = load_all_validation_specs("framed_v", session)

        assert list(specs) == ["framed_check"]

    def test_rows_for_other_verticals_ignored(self, session: Session):
        ensure_validations_seeded(session, VERTICAL)
        set_overlay_resolver(
            lambda: [
                OverlayRow(
                    type="validation",
                    payload=_spec_payload("other_check", vertical="some_other_vertical"),
                )
            ]
        )
        specs = load_all_validation_specs(VERTICAL, session)

        assert "other_check" not in specs

    def test_test_path_bypasses_db_and_overlay(self, tmp_path):
        """``verticals_dir`` reads raw YAML — no DB session, no overlay."""
        set_overlay_resolver(
            lambda: [OverlayRow(type="validation", payload=_spec_payload("taught_check"))]
        )
        spec_dir = tmp_path / VERTICAL / "validations"
        spec_dir.mkdir(parents=True)
        (spec_dir / "on_disk.yaml").write_text(
            "validation_id: on_disk_check\n"
            "name: On Disk\n"
            "description: from the fixture dir\n"
            "category: financial\n"
            "check_type: balance\n"
        )
        specs = load_all_validation_specs(VERTICAL, verticals_dir=tmp_path)

        assert list(specs) == ["on_disk_check"]
        reset_overlay_resolver_for_tests()


# IDs of universal specs (relevant_cycles = [])
UNIVERSAL_IDS = {
    "stage_date_ordering",
    # tb_gl_reconciliation is universal by design: no GL cycle exists in
    # the finance cycles vocabulary, and the TB-GL identity holds regardless of
    # which business cycles ground.
    "tb_gl_reconciliation",
    "orphan_transactions",
}


class TestGetValidationSpecsForCycles:
    """Filtering the seeded specs by detected cycle types."""

    def test_returns_gl_specs_for_journal_entry_cycle(self, session: Session):
        """journal_entry_cycle → double_entry, trial_balance, sign_conventions + universals."""
        ensure_validations_seeded(session, VERTICAL)
        specs = get_validation_specs_for_cycles(["journal_entry_cycle"], VERTICAL, session)
        ids = {s.validation_id for s in specs}

        assert "double_entry_balance" in ids
        assert "trial_balance" in ids
        assert "sign_conventions" in ids
        assert UNIVERSAL_IDS <= ids

    def test_returns_p2p_specs_for_procure_to_pay(self, session: Session):
        """procure_to_pay → three_way_match + universals, no GL-specific specs."""
        ensure_validations_seeded(session, VERTICAL)
        specs = get_validation_specs_for_cycles(["procure_to_pay"], VERTICAL, session)
        ids = {s.validation_id for s in specs}

        assert "three_way_match" in ids
        assert UNIVERSAL_IDS <= ids
        assert "double_entry_balance" not in ids
        assert "sign_conventions" not in ids

    def test_universal_specs_always_included(self, session: Session):
        """Universal specs appear regardless of cycle type."""
        ensure_validations_seeded(session, VERTICAL)
        specs = get_validation_specs_for_cycles(["some_unknown_cycle"], VERTICAL, session)
        ids = {s.validation_id for s in specs}

        assert UNIVERSAL_IDS <= ids

    def test_empty_cycle_list_returns_only_universal(self, session: Session):
        """No cycle types → only universal specs (empty relevant_cycles)."""
        ensure_validations_seeded(session, VERTICAL)
        specs = get_validation_specs_for_cycles([], VERTICAL, session)
        ids = {s.validation_id for s in specs}

        assert ids == UNIVERSAL_IDS
