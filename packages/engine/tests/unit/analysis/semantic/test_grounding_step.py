"""Unit tests for the extracted ``ground_columns`` step (DAT-376).

Pins the structure-only extraction of the grounding tail of
``SemanticPerColumnPhase._run``: a mocked ``ColumnAnnotationAgent`` whose output
is persisted as ``SemanticAnnotation`` rows (returning the count), and the
disabled-config gate. ``persist_column_annotations`` itself is exercised by
``test_phase_split.py`` and reused untouched here.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from sqlalchemy import select

from dataraum.analysis.semantic.db_models import SemanticAnnotation as AnnotationDB
from dataraum.analysis.semantic.models import (
    ColumnAnnotationOutput,
    ColumnSemanticOutput,
    TableColumnAnnotation,
)
from dataraum.analysis.semantic.processor import ground_columns
from dataraum.core.models.base import Result
from dataraum.storage import Column, Source, Table
from tests.conftest import baseline_session_id


def _table_with_columns(session, name: str, columns: list[str]) -> Table:
    src = Source(name=f"src_{name}", source_type="csv")
    session.add(src)
    session.flush()
    table = Table(source_id=src.source_id, table_name=name, layer="typed", row_count=10)
    session.add(table)
    session.flush()
    for pos, col in enumerate(columns):
        session.add(
            Column(
                table_id=table.table_id, column_name=col, column_position=pos, raw_type="VARCHAR"
            )
        )
    session.flush()
    return table


def _col(name: str, role: str) -> ColumnSemanticOutput:
    return ColumnSemanticOutput(
        column_name=name,
        semantic_role=role,
        entity_type=f"{name}_entity",
        business_term=name.title(),
        description=f"{name} column",
        confidence=0.9,
    )


def _enabled_config() -> MagicMock:
    config = MagicMock()
    config.features.column_annotation = SimpleNamespace(enabled=True, model_tier="balanced")
    return config


class TestGroundColumns:
    @patch("dataraum.graphs.config.get_metric_definitions")
    @patch("dataraum.analysis.semantic.ontology.OntologyLoader")
    @patch("dataraum.graphs.loader.GraphLoader")
    @patch("dataraum.analysis.semantic.column_agent.ColumnAnnotationAgent")
    def test_persists_annotations_and_returns_count(
        self,
        mock_agent_cls: MagicMock,
        mock_graph_cls: MagicMock,
        mock_ontology_cls: MagicMock,
        mock_get_defs: MagicMock,
        session,
    ) -> None:
        table = _table_with_columns(session, "customers", ["customer_id", "revenue"])

        mock_get_defs.return_value = {}
        mock_graph_cls.return_value.get_all_abstract_fields.return_value = set()
        mock_ontology_cls.return_value.load.return_value = None
        mock_agent_cls.return_value.annotate.return_value = Result.ok(
            ColumnAnnotationOutput(
                tables=[
                    TableColumnAnnotation(
                        table_name="customers",
                        columns=[_col("customer_id", "key"), _col("revenue", "measure")],
                    )
                ]
            )
        )

        provider = MagicMock()
        provider.get_model_for_tier.return_value = "test-model"

        result = ground_columns(
            session=session,
            config=_enabled_config(),
            provider=provider,
            renderer=MagicMock(),
            table_ids=[table.table_id],
            ontology="finance",
            session_id=baseline_session_id(),
        )
        session.flush()

        assert result.success
        assert result.value == 2
        rows = session.execute(select(AnnotationDB)).scalars().all()
        assert len(rows) == 2
        assert {r.semantic_role for r in rows} == {"key", "measure"}
        assert all(r.annotated_by == "test-model" for r in rows)
        # Standard-field concepts come from the active metric graphs for `finance`.
        mock_graph_cls.assert_called_once_with(vertical="finance")

    def test_disabled_config_gate_fails(self, session) -> None:
        config = MagicMock()
        config.features.column_annotation = SimpleNamespace(enabled=False, model_tier="balanced")

        result = ground_columns(
            session=session,
            config=config,
            provider=MagicMock(),
            renderer=MagicMock(),
            table_ids=["t1"],
            ontology="finance",
            session_id=baseline_session_id(),
        )

        assert not result.success
        assert "disabled" in (result.error or "").lower()
        assert session.execute(select(AnnotationDB)).scalars().all() == []

    @patch("dataraum.graphs.config.get_metric_definitions")
    @patch("dataraum.analysis.semantic.ontology.OntologyLoader")
    @patch("dataraum.graphs.loader.GraphLoader")
    @patch("dataraum.analysis.semantic.column_agent.ColumnAnnotationAgent")
    def test_propagates_agent_failure(
        self,
        mock_agent_cls: MagicMock,
        mock_graph_cls: MagicMock,
        mock_ontology_cls: MagicMock,
        mock_get_defs: MagicMock,
        session,
    ) -> None:
        mock_get_defs.return_value = {}
        mock_graph_cls.return_value.get_all_abstract_fields.return_value = set()
        mock_agent_cls.return_value.annotate.return_value = Result.fail("annotation LLM down")

        result = ground_columns(
            session=session,
            config=_enabled_config(),
            provider=MagicMock(),
            renderer=MagicMock(),
            table_ids=["t1"],
            ontology="finance",
            session_id=baseline_session_id(),
        )

        assert not result.success
        assert "annotation LLM down" in (result.error or "")

    @patch("dataraum.graphs.config.get_metric_definitions")
    @patch("dataraum.analysis.semantic.ontology.OntologyLoader")
    @patch("dataraum.analysis.semantic.column_agent.ColumnAnnotationAgent")
    def test_grounding_prioritizes_overlay_metric_fields(
        self,
        mock_agent_cls: MagicMock,
        mock_ontology_cls: MagicMock,
        mock_get_defs: MagicMock,
        session,
    ) -> None:
        # A FRAMED vertical declares its metrics as `metric` overlay rows (no
        # on-disk dir); get_metric_definitions resolves them, and their extract-leaf
        # standard_fields must steer column grounding. load_all() (file-only) would
        # return nothing for a framed vertical — this is the DAT-471 AC3 fix. Real
        # GraphLoader (NOT mocked) so the overlay def actually parses and
        # get_all_abstract_fields walks its steps. A malformed sibling def (missing
        # metadata.name) must be SKIPPED for this grounding hint — logged, not
        # raised — without dropping the parseable one (covers the GraphLoadError
        # branch: if the except didn't fire, ground_columns would error here).
        table = _table_with_columns(session, "ledger", ["amount"])
        mock_get_defs.return_value = {
            "framed_margin": {
                "graph_id": "framed_margin",
                "version": "1.0",
                "metadata": {"name": "Framed Margin", "category": "test"},
                "output": {"type": "scalar", "metric_id": "framed_margin"},
                "dependencies": {
                    "extract_revenue": {
                        "level": 1,
                        "type": "extract",
                        "source": {"standard_field": "revenue"},
                        "output_step": True,
                    },
                },
            },
            # Unparseable (no metadata.name) → GraphLoadError, skipped for the hint.
            "broken_metric": {"graph_id": "broken_metric", "dependencies": {}},
        }
        mock_ontology_cls.return_value.load.return_value = None

        captured: dict[str, object] = {}

        def _annotate(**kwargs: object) -> Result[ColumnAnnotationOutput]:
            captured["required_standard_fields"] = kwargs["required_standard_fields"]
            return Result.ok(
                ColumnAnnotationOutput(
                    tables=[
                        TableColumnAnnotation(
                            table_name="ledger", columns=[_col("amount", "measure")]
                        )
                    ]
                )
            )

        mock_agent_cls.return_value.annotate.side_effect = _annotate
        provider = MagicMock()
        provider.get_model_for_tier.return_value = "test-model"

        result = ground_columns(
            session=session,
            config=_enabled_config(),
            provider=provider,
            renderer=MagicMock(),
            table_ids=[table.table_id],
            ontology="framed_co",
            session_id=baseline_session_id(),
        )
        session.flush()

        assert result.success
        # The framed metric's extract-leaf standard_field steers grounding —
        # sourced from the overlay-aware declared set, not file-only load_all().
        assert captured["required_standard_fields"] == ["revenue"]
        mock_get_defs.assert_called_once_with("framed_co")
