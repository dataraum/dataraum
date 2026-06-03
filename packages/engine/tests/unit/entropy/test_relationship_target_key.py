"""Relationship target-key helpers (DAT-408) — one stable identity string the
detector (emit), persist, and reader (gate) all agree on."""

from dataraum.entropy.models import parse_relationship_target, relationship_target_key


def test_round_trip() -> None:
    key = relationship_target_key("col-from-uuid", "col-to-uuid")
    assert key == "relationship:col-from-uuid::col-to-uuid"
    assert parse_relationship_target(key) == ("col-from-uuid", "col-to-uuid")


def test_directional() -> None:
    """The key is directional — (a,b) != (b,a)."""
    assert relationship_target_key("a", "b") != relationship_target_key("b", "a")


def test_parse_rejects_non_relationship() -> None:
    assert parse_relationship_target("column:t.c") is None
    assert parse_relationship_target("table:t") is None
    assert parse_relationship_target("relationship:onlyone") is None
    assert parse_relationship_target("relationship:a::") is None
