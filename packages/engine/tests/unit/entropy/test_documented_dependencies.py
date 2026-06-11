"""Documented-dependency teach loader — the substrate that closes dimensional_entropy.

A ``document_business_rule`` teach writes ``ConfigOverlay(type='expected_dependency')``;
``load_documented_dependencies`` returns the undirected column pairs so the NMI
dimensional_entropy detector can exclude EXPECTED structure (e.g. debit/credit) from
its score — i.e. a teach closes the measurement. Mirrors the relationship-confirmation
teach (DAT-409).
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy.orm import Session

from dataraum.entropy.detectors.loaders import load_documented_dependencies
from dataraum.storage import ConfigOverlay


def test_documented_dependency_returns_undirected_pair(session: Session) -> None:
    session.add(
        ConfigOverlay(
            type="expected_dependency",
            payload={"column_ids": ["c_debit", "c_credit"], "rule": "double-entry mutex"},
        )
    )
    session.flush()
    # Undirected: the detector tests `frozenset({a, b}) in documented` either way.
    assert load_documented_dependencies(session) == {frozenset({"c_debit", "c_credit"})}


def test_superseded_documented_dependency_is_ignored(session: Session) -> None:
    """An undone teach (superseded_at set) no longer documents the pair."""
    session.add(
        ConfigOverlay(
            type="expected_dependency",
            payload={"column_ids": ["c_debit", "c_credit"]},
            superseded_at=datetime(2020, 1, 1, tzinfo=UTC),
        )
    )
    session.flush()
    assert load_documented_dependencies(session) == set()


def test_other_overlay_types_are_ignored(session: Session) -> None:
    session.add(
        ConfigOverlay(
            type="relationship",
            payload={"action": "confirm", "from_column_id": "ca", "to_column_id": "cb"},
        )
    )
    session.flush()
    assert load_documented_dependencies(session) == set()


def test_malformed_payload_is_skipped(session: Session) -> None:
    """A payload without exactly two column_ids documents nothing."""
    session.add(ConfigOverlay(type="expected_dependency", payload={"column_ids": ["only_one"]}))
    session.flush()
    assert load_documented_dependencies(session) == set()
