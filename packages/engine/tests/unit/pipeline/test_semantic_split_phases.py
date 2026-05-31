"""Unit tests for the split semantic phases' should_skip logic (DAT-362).

The _run paths drive a live LLM and are exercised by integration/calibration;
here we pin the skip gates that decide whether each phase re-runs.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.db_models import SemanticAnnotation, TableEntity
from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases.semantic_per_column_phase import SemanticPerColumnPhase
from dataraum.pipeline.phases.semantic_per_table_phase import SemanticPerTablePhase
from dataraum.storage import Column, ConfigOverlay, Source, Table
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
    return t


def _annotate(session: Session, table: Table) -> None:
    for col in table.columns:
        session.add(
            SemanticAnnotation(
                session_id=baseline_session_id(),
                column_id=col.column_id,
                semantic_role="attribute",
                annotation_source="llm",
                confidence=0.9,
            )
        )
    session.flush()


def _ctx(session: Session, duckdb_conn: duckdb.DuckDBPyConnection, source_id: str) -> PhaseContext:
    return PhaseContext(session=session, duckdb_conn=duckdb_conn, source_id=source_id)


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

    def test_skips_when_all_annotated(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        src = _source(session)
        t1 = _typed_table(session, src.source_id, "t1", ["a", "b"])
        _annotate(session, t1)
        assert SemanticPerColumnPhase().should_skip(_ctx(session, duckdb_conn, src.source_id)) == (
            "All columns already have semantic annotations"
        )


class TestPerColumnReplayCleanup:
    def test_drops_annotation_but_leaves_concept_overlay(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """DAT-376: replay_cleanup wipes SemanticAnnotation, never concept rows.

        Induced ``_adhoc`` concepts live as workspace-scoped ``concept``
        ConfigOverlay rows (DAT-371). A reduce replay must re-annotate columns
        against the *existing* ontology — it must not delete the induced
        concepts that ontology is built from.
        """
        src = _source(session)
        t1 = _typed_table(session, src.source_id, "t1", ["a"])
        _annotate(session, t1)
        overlay = ConfigOverlay(
            session_id=None,
            type="concept",
            payload={"vertical": "_adhoc", "name": "revenue"},
        )
        session.add(overlay)
        session.flush()

        SemanticPerColumnPhase().replay_cleanup(_ctx(session, duckdb_conn, src.source_id), [])
        session.flush()

        assert session.execute(select(SemanticAnnotation)).scalars().all() == []
        overlays = session.execute(select(ConfigOverlay)).scalars().all()
        assert len(overlays) == 1
        assert overlays[0].type == "concept"
        assert overlays[0].payload["name"] == "revenue"


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
        assert "_adhoc concepts" in (result.error or "")
        # Grounded nothing — no annotations written.
        assert session.execute(select(SemanticAnnotation)).scalars().all() == []


class TestPerTableShouldSkip:
    def test_no_typed_tables(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        src = _source(session)
        assert SemanticPerTablePhase().should_skip(_ctx(session, duckdb_conn, src.source_id)) == (
            "No typed tables found"
        )

    def test_runs_when_a_table_lacks_an_entity(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        src = _source(session)
        _typed_table(session, src.source_id, "t1", ["a"])
        assert (
            SemanticPerTablePhase().should_skip(_ctx(session, duckdb_conn, src.source_id)) is None
        )

    def test_skips_when_all_tables_classified(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
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
        assert SemanticPerTablePhase().should_skip(_ctx(session, duckdb_conn, src.source_id)) == (
            "All tables already classified"
        )
