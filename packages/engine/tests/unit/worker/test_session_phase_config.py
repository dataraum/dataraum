"""Unit test for the source-free begin_session phase config builder (DAT-401).

``run_session_phase`` builds a begin_session phase's ``ctx.config`` from the
phase's static pipeline.yaml config plus the session's frame ``vertical`` (read
off the ``InvestigationSession`` row) — NOT from a ``Source``. The vertical is
the one runtime value ``semantic_per_table`` reads (``ctx.config.get("vertical")``
for its ontology), so pin that threading here; no LLM, no DB.
"""

from __future__ import annotations

from dataraum.worker.activity import _build_session_phase_config


def test_injects_the_sessions_vertical() -> None:
    cfg = _build_session_phase_config("semantic_per_table", "financial_reporting")
    assert cfg["vertical"] == "financial_reporting"


def test_defaults_vertical_to_adhoc_on_a_cold_start_session() -> None:
    # A session with no frame vertical falls back to "_adhoc" — mirrors add_source's
    # ``identity.vertical or "_adhoc"`` so a cold-start begin_session still resolves.
    cfg = _build_session_phase_config("semantic_per_table", None)
    assert cfg["vertical"] == "_adhoc"


def test_returns_a_dict_carrying_vertical_for_any_phase() -> None:
    # relationships doesn't read vertical, but the builder is uniform — it always
    # returns the merged static config + vertical, never None.
    cfg = _build_session_phase_config("relationships", "marketing")
    assert isinstance(cfg, dict)
    assert cfg["vertical"] == "marketing"
