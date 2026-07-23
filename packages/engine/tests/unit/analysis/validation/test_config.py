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
from dataraum.analysis.validation.db_models import Validation
from dataraum.core.overlay import (
    OverlayRow,
    reset_overlay_resolver_for_tests,
    set_overlay_resolver,
)

VERTICAL = "finance"


def _seed_row(validation_id: str, **overrides) -> Validation:
    """A synthetic ``source='seed'`` row — DAT-725 band 3 retired finance's
    shipped ``validations/`` directory (no vertical ships one today), so these
    tests seed the typed home directly rather than via
    ``ensure_validations_seeded`` against the (now-deleted) real config tree."""
    fields: dict = {
        "vertical": VERTICAL,
        "validation_id": validation_id,
        "name": validation_id.replace("_", " ").title(),
        "description": "synthetic seed check",
        "category": "financial",
        "severity": "critical",
        "check_type": "balance",
        "tolerance": 0.01,
        "guidance": "Sum debit - credit per account_type.",
        "source": "seed",
    }
    fields.update(overrides)
    return Validation(**fields)


class TestLoadAllValidationSpecs:
    """Loading the declared set from the typed DB home."""

    def test_loads_seeded_specs_from_db_home(self, session: Session):
        """A source='seed' row (synthetic — no vertical ships a validations/ dir
        since DAT-725 band 3) loads from the typed home with its check
        definition intact."""
        session.add(_seed_row("double_entry_balance"))
        session.flush()
        specs = load_all_validation_specs(VERTICAL, session)

        assert "double_entry_balance" in specs
        # The check LOGIC is typed (DAT-735): tolerance is a float, not a dict entry.
        assert specs["double_entry_balance"].tolerance == 0.01
        # sql_hints is gone; the binding prose lives in guidance.
        assert specs["double_entry_balance"].guidance

    def test_each_load_returns_fresh_dict(self, session: Session):
        """Each call returns a new dict (no caching)."""
        session.add(_seed_row("double_entry_balance"))
        session.flush()
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
        session.add(_seed_row("double_entry_balance"))
        session.flush()
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
        session.add(_seed_row("double_entry_balance", tolerance=0.01))
        session.flush()
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


def _seed_cycle_scoped_rows(session: Session) -> None:
    """Four synthetic seed rows spanning the cycle-filter shapes
    ``get_validation_specs_for_cycles`` distinguishes — DAT-725 band 3 retired
    finance's shipped validations/ directory (the original nine-spec fixture),
    but the cycle-scoping MECHANISM is generic over any ``relevant_cycles``
    shape, so a small synthetic set exercises it identically."""
    rows = [
        ("gl_check", ["journal_entry_cycle"]),
        ("p2p_check", ["procure_to_pay"]),
        ("universal_a", []),
        ("universal_b", []),
    ]
    for validation_id, relevant_cycles in rows:
        session.add(_seed_row(validation_id, relevant_cycles=relevant_cycles or None))
    session.flush()


# IDs of the universal synthetic rows (relevant_cycles = []).
UNIVERSAL_IDS = {"universal_a", "universal_b"}


class TestGetValidationSpecsForCycles:
    """Filtering the seeded specs by detected cycle types."""

    def test_returns_gl_specs_for_journal_entry_cycle(self, session: Session):
        """journal_entry_cycle → the GL-scoped spec + universals, not procure_to_pay's."""
        _seed_cycle_scoped_rows(session)
        specs = get_validation_specs_for_cycles(["journal_entry_cycle"], VERTICAL, session)
        ids = {s.validation_id for s in specs}

        assert "gl_check" in ids
        assert "p2p_check" not in ids
        assert UNIVERSAL_IDS <= ids

    def test_returns_p2p_specs_for_procure_to_pay(self, session: Session):
        """procure_to_pay → the P2P-scoped spec + universals, no GL-specific specs."""
        _seed_cycle_scoped_rows(session)
        specs = get_validation_specs_for_cycles(["procure_to_pay"], VERTICAL, session)
        ids = {s.validation_id for s in specs}

        assert "p2p_check" in ids
        assert UNIVERSAL_IDS <= ids
        assert "gl_check" not in ids

    def test_universal_specs_always_included(self, session: Session):
        """Universal specs appear regardless of cycle type."""
        _seed_cycle_scoped_rows(session)
        specs = get_validation_specs_for_cycles(["some_unknown_cycle"], VERTICAL, session)
        ids = {s.validation_id for s in specs}

        assert UNIVERSAL_IDS <= ids

    def test_empty_cycle_list_returns_only_universal(self, session: Session):
        """No cycle types → only universal specs (empty relevant_cycles)."""
        _seed_cycle_scoped_rows(session)
        specs = get_validation_specs_for_cycles([], VERTICAL, session)
        ids = {s.validation_id for s in specs}

        assert ids == UNIVERSAL_IDS
