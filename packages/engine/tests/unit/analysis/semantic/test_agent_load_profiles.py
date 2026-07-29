"""``SemanticAgent._load_profiles`` excludes mint-owned surrogate columns (DAT-878).

Minted ``_sk__*`` surrogate join keys (``pipeline/phases/surrogate_mint_phase.py``,
``analysis/relationships/surrogate.py``) are not business columns — they must
never feed the ``column_annotation``/``semantic_per_table`` LLM prompts this
agent builds from. The pipeline's normal phase order runs semantic before
surrogate_mint on a FIRST pass, but a begin_session teach re-run over an
already-minted table set hits this loader with surrogates already carrying
real ``StatisticalProfile`` rows — the exact leak this test pins.
"""

from __future__ import annotations

from unittest.mock import MagicMock
from uuid import uuid4

from sqlalchemy.orm import Session

from dataraum.analysis.semantic.agent import SemanticAgent
from dataraum.analysis.statistics.db_models import StatisticalProfile
from dataraum.storage import Column, Source, Table


def _agent() -> SemanticAgent:
    # _load_profiles is a pure DB read — config/provider/renderer unused.
    return SemanticAgent(MagicMock(), MagicMock(), MagicMock())


def _profile(session: Session, column_id: str) -> None:
    session.add(
        StatisticalProfile(
            column_id=column_id, layer="typed", total_count=10, null_count=0, profile_data={}
        )
    )
    session.flush()


def _seed_table_cols(session: Session) -> tuple[str, str, str]:
    """A table with one business column (``amount``) + one surrogate. Returns
    ``(table_id, amount_column_id, surrogate_column_id)`` — no profiles yet."""
    source = Source(name=f"src_{uuid4().hex[:8]}", source_type="csv")
    session.add(source)
    session.flush()
    table = Table(source_id=source.source_id, table_name="orders", layer="typed", row_count=10)
    session.add(table)
    session.flush()

    amount = Column(
        column_id=str(uuid4()),
        table_id=table.table_id,
        column_name="amount",
        column_position=0,
        raw_type="VARCHAR",
    )
    surrogate = Column(
        column_id=str(uuid4()),
        table_id=table.table_id,
        column_name="_sk__id__vendor",
        column_position=1,
        raw_type="VARCHAR",
    )
    session.add_all([amount, surrogate])
    session.flush()

    return table.table_id, amount.column_id, surrogate.column_id


def _seed_table(session: Session, *, with_profiles: bool) -> str:
    table_id, amount_id, surrogate_id = _seed_table_cols(session)
    if with_profiles:
        _profile(session, amount_id)
        _profile(session, surrogate_id)
    return table_id


class TestLoadProfilesExcludesSurrogates:
    def test_primary_query_drops_surrogate_columns(self, session: Session) -> None:
        """The profile-joined primary query path (DAT-878)."""
        table_id = _seed_table(session, with_profiles=True)

        result = _agent()._load_profiles(session, [table_id])

        assert result.success
        names = {p.column_ref.column_name for p in result.unwrap()}
        assert names == {"amount"}

    def test_placeholder_fallback_drops_surrogate_columns(self, session: Session) -> None:
        """No profiles anywhere → the placeholder fallback path (DAT-878)."""
        table_id = _seed_table(session, with_profiles=False)

        result = _agent()._load_profiles(session, [table_id])

        assert result.success
        names = {p.column_ref.column_name for p in result.unwrap()}
        assert names == {"amount"}


class TestPlaceholderTriggerTestsThePostFilterList:
    """DAT-878: the ``if not profiles:`` trigger tests the SURROGATE-FILTERED list.

    A table whose ONLY profiled column is the mint-owned surrogate (the real
    ``amount`` column was never profiled) has a non-empty profile ROW set in
    the DB — but after the surrogate exclusion, the returned ``profiles`` list
    is empty, so the placeholder path fires. That is correct: ``amount`` still
    needs SOME representation for semantic analysis to run over it.
    """

    def test_surrogate_only_profiled_table_takes_the_placeholder_path(
        self, session: Session
    ) -> None:
        table_id, amount_id, surrogate_id = _seed_table_cols(session)
        _profile(session, surrogate_id)  # ONLY the surrogate gets a real profile.

        result = _agent()._load_profiles(session, [table_id])

        assert result.success
        profiles = result.unwrap()
        names = {p.column_ref.column_name for p in profiles}
        # The surrogate is excluded; `amount` still gets a PLACEHOLDER (not
        # silently dropped) because the post-filter empty list triggered the
        # fallback path rather than returning the (surrogate-only) real result.
        assert names == {"amount"}
        (amount_profile,) = [p for p in profiles if p.column_id == amount_id]
        assert amount_profile.total_count == 10  # table.row_count — the placeholder shape
