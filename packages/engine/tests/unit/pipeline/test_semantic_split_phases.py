"""Unit tests for the split semantic phases' should_skip logic (DAT-362).

The _run paths drive a live LLM and are exercised by integration/calibration;
here we pin the skip gates that decide whether each phase re-runs.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch
from uuid import uuid4

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.db_models import SemanticAnnotation, TableEntity
from dataraum.investigation.db_models import InvestigationSession
from dataraum.investigation.queries import link_session_tables
from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases.semantic_per_column_phase import SemanticPerColumnPhase
from dataraum.pipeline.phases.semantic_per_table_phase import SemanticPerTablePhase
from dataraum.storage import Column, Source, Table
from tests.conftest import baseline_session_id


def _source(session: Session) -> Source:
    src = Source(name="s", source_type="csv")
    session.add(src)
    session.flush()
    return src


def _typed_table(session: Session, source_id: str, name: str, cols: list[str]) -> Table:
    t = Table(source_id=source_id, table_name=name, layer="typed", row_count=10)
    session.add(t)
    session.flush()
    for pos, c in enumerate(cols):
        session.add(
            Column(table_id=t.table_id, column_name=c, column_position=pos, resolved_type="VARCHAR")
        )
    session.flush()
    # Mirror the typing phase: a typed table is linked to the run's session
    # (``session_tables``, DAT-407) in the same transaction. The add_source reduce
    # now scopes by that session anchor, not by ``source_id`` (DAT-421), so the
    # fixtures must link too or the phase sees no tables.
    link_session_tables(session, baseline_session_id(), [t.table_id])
    session.flush()
    return t


def _annotate(session: Session, table: Table, run_id: str | None = None) -> None:
    for col in table.columns:
        session.add(
            SemanticAnnotation(
                session_id=baseline_session_id(),
                column_id=col.column_id,
                run_id=run_id,
                semantic_role="attribute",
                annotation_source="llm",
                confidence=0.9,
            )
        )
    session.flush()


def _ctx(session: Session, duckdb_conn: duckdb.DuckDBPyConnection, source_id: str) -> PhaseContext:
    # semantic_per_column is session-scoped now (DAT-421): it derives its tables
    # from the run's ``session_tables``, so the ctx must carry the session id the
    # typed tables above are linked under (``baseline_session_id()``).
    return PhaseContext(
        session=session,
        duckdb_conn=duckdb_conn,
        source_id=source_id,
        session_id=baseline_session_id(),
    )


def _session_ctx(
    session: Session, duckdb_conn: duckdb.DuckDBPyConnection, table_ids: list[str]
) -> PhaseContext:
    """Source-free ctx for the begin_session phases — scoped by ``table_ids`` (DAT-401).

    Carries the baseline session id: the begin_session phases are session-scoped
    (``should_skip`` filters by ``session_id``), so the ctx must supply one,
    matching the rows seeded under ``baseline_session_id()``.
    """
    return PhaseContext(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=table_ids,
        session_id=baseline_session_id(),
    )


class TestPerColumnShouldSkip:
    def test_no_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        src = _source(session)
        assert SemanticPerColumnPhase().should_skip(_ctx(session, duckdb_conn, src.source_id)) == (
            "No typed tables found"
        )

    def test_runs_when_columns_unannotated(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        src = _source(session)
        _typed_table(session, src.source_id, "t1", ["a", "b"])
        assert (
            SemanticPerColumnPhase().should_skip(_ctx(session, duckdb_conn, src.source_id)) is None
        )

    def test_re_runs_when_only_a_prior_runs_annotations_exist(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """A prior run's annotations no longer gate a skip (DAT-413).

        The output-existence bail ("all columns already annotated → skip") is
        gone: a re-run mints a fresh ``run_id`` and re-grounds under it, its rows
        coexisting with the prior run's via the ``(column_id, run_id)``
        constraint. With the columns fully annotated under run-A, ``should_skip``
        still returns ``None`` — only the structural early-outs ("no typed
        tables" / "no columns") can skip.
        """
        src = _source(session)
        t1 = _typed_table(session, src.source_id, "t1", ["a", "b"])
        _annotate(session, t1, run_id="run-A")
        assert (
            SemanticPerColumnPhase().should_skip(_ctx(session, duckdb_conn, src.source_id)) is None
        )

    def test_scopes_by_session_anchor_not_source(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """DAT-421: the reduce scopes by the run's SESSION tables, not ``source_id``.

        A table linked to the run's session counts even when it belongs to a
        different (per-object) source; a same-source typed table NOT linked to the
        session does not. This source-agnostic scope is what lets one run span
        multiple per-object sources — it mirrors the ``detect`` key (DAT-410).
        """
        src_a = _source(session)
        src_b = Source(name="s2", source_type="csv")
        session.add(src_b)
        session.flush()
        # Two tables under DIFFERENT sources, both linked to the run's session.
        a = _typed_table(session, src_a.source_id, "a", ["x"])
        b = _typed_table(session, src_b.source_id, "b", ["y"])
        # A typed table under src_a NOT linked to the run's session.
        orphan = Table(source_id=src_a.source_id, table_name="orphan", layer="typed", row_count=1)
        session.add(orphan)
        session.flush()

        scoped = set(
            SemanticPerColumnPhase()._typed_table_ids(_ctx(session, duckdb_conn, src_a.source_id))
        )
        assert scoped == {a.table_id, b.table_id}  # across sources, session-anchored
        assert orphan.table_id not in scoped  # same source, but not session-linked

    def test_single_source_equivalence_is_behavior_preserving(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """DAT-421: for a single-source run the session-scoped set is IDENTICAL to
        the old ``Table.source_id``-filtered set — a direct proof of the
        behavior-preservation claim.
        """
        src = _source(session)
        t1 = _typed_table(session, src.source_id, "t1", ["a"])
        t2 = _typed_table(session, src.source_id, "t2", ["b"])

        scoped = set(
            SemanticPerColumnPhase()._typed_table_ids(_ctx(session, duckdb_conn, src.source_id))
        )
        # The pre-DAT-421 key: every typed table under this source.
        old_key = {
            tid
            for (tid,) in session.execute(
                select(Table.table_id).where(
                    Table.layer == "typed", Table.source_id == src.source_id
                )
            )
        }
        assert scoped == old_key == {t1.table_id, t2.table_id}


class TestPerColumnAdhocFailLoud:
    """Grounding-only ``_run`` fails loud on a cold-start ``_adhoc`` workspace
    with no frame-declared concepts (DAT-382). Induction has left the engine;
    the cockpit ``frame`` stage must write ``concept`` overlay rows first.
    """

    def _adhoc_ctx(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection, source_id: str
    ) -> PhaseContext:
        return PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=source_id,
            config={"vertical": "_adhoc"},
            session_id=baseline_session_id(),
        )

    @patch("dataraum.pipeline.phases.semantic_per_column_phase.OntologyLoader")
    @patch("dataraum.pipeline.phases.semantic_per_column_phase.PromptRenderer")
    @patch("dataraum.pipeline.phases.semantic_per_column_phase.create_provider")
    @patch("dataraum.pipeline.phases.semantic_per_column_phase.load_llm_config")
    def test_fails_loud_when_adhoc_has_no_concepts(
        self,
        mock_load_config: MagicMock,
        mock_create_provider: MagicMock,
        mock_renderer_cls: MagicMock,
        mock_loader_cls: MagicMock,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> None:
        config = MagicMock()
        config.active_provider = "anthropic"
        config.providers = {"anthropic": MagicMock()}
        mock_load_config.return_value = config
        # _adhoc ontology resolves to no concepts → fail loud, never ground.
        mock_loader_cls.return_value.load.return_value = None

        src = _source(session)
        _typed_table(session, src.source_id, "t1", ["a"])

        result = SemanticPerColumnPhase()._run(self._adhoc_ctx(session, duckdb_conn, src.source_id))

        assert result.status == PhaseStatus.FAILED
        assert "No concepts found for vertical '_adhoc'" in (result.error or "")
        # Grounded nothing — no annotations written.
        assert session.execute(select(SemanticAnnotation)).scalars().all() == []


class TestPerTableShouldSkip:
    """The per-table phase scopes by the session's ``table_ids`` (DAT-401, source-free)."""

    def test_no_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        assert SemanticPerTablePhase().should_skip(_session_ctx(session, duckdb_conn, [])) == (
            "No typed tables found"
        )

    def test_runs_when_a_table_lacks_an_entity(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        src = _source(session)
        t1 = _typed_table(session, src.source_id, "t1", ["a"])
        assert (
            SemanticPerTablePhase().should_skip(_session_ctx(session, duckdb_conn, [t1.table_id]))
            is None
        )

    def test_re_runs_when_already_classified(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """DAT-408: a versioned begin_session re-run re-classifies — no idempotency skip.

        The old "all tables already classified → skip" branch is gone; it would make
        a replay a silent no-op. Even fully classified, the phase runs.
        """
        src = _source(session)
        t1 = _typed_table(session, src.source_id, "t1", ["a"])
        session.add(
            TableEntity(
                session_id=baseline_session_id(),
                table_id=t1.table_id,
                detected_entity_type="thing",
                confidence=0.9,
                detection_source="llm",
            )
        )
        session.flush()
        assert (
            SemanticPerTablePhase().should_skip(_session_ctx(session, duckdb_conn, [t1.table_id]))
            is None
        )

    def test_scopes_across_sources_and_ignores_excluded(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """A selection spanning two sources is seen whole; an unselected table is not."""
        src_a = _source(session)
        src_b = Source(name="s2", source_type="csv")
        session.add(src_b)
        session.flush()
        a = _typed_table(session, src_a.source_id, "a", ["x"])
        b = _typed_table(session, src_b.source_id, "b", ["y"])
        _typed_table(session, src_a.source_id, "a_excluded", ["z"])  # not selected

        phase = SemanticPerTablePhase()
        selected = phase._typed_tables(_session_ctx(session, duckdb_conn, [a.table_id, b.table_id]))
        assert {t.table_id for t in selected} == {a.table_id, b.table_id}
        # Neither selected table is classified yet → the phase runs (no skip).
        assert (
            phase.should_skip(_session_ctx(session, duckdb_conn, [a.table_id, b.table_id])) is None
        )

    def test_another_sessions_classification_does_not_skip(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """A different session's entity over a shared table must not skip THIS session."""
        src = _source(session)
        t1 = _typed_table(session, src.source_id, "t1", ["a"])
        other_session = str(uuid4())
        session.add(InvestigationSession(session_id=other_session, intent="other", status="active"))
        session.flush()
        session.add(
            TableEntity(
                session_id=other_session,  # classified by a DIFFERENT session
                table_id=t1.table_id,
                detected_entity_type="thing",
                confidence=0.9,
                detection_source="llm",
            )
        )
        session.flush()
        # This (baseline) session has not classified t1 → it must still run.
        assert (
            SemanticPerTablePhase().should_skip(_session_ctx(session, duckdb_conn, [t1.table_id]))
            is None
        )
