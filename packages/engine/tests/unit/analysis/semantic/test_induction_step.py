"""Unit tests for the extracted ``induce_adhoc_concepts`` step (DAT-376).

Pins the structure-only extraction of the cold-start ``_adhoc`` induction from
``SemanticPerColumnPhase._ensure_adhoc_ontology``: the short-circuit when
concepts already exist, the one-overlay-row-per-induced-concept insert, and
failure propagation. The induction agent / LLM call itself is mocked — its
behaviour is covered by ``test_induction.py``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from sqlalchemy import select

from dataraum.analysis.semantic.induction import induce_adhoc_concepts
from dataraum.analysis.semantic.ontology import OntologyConcept, OntologyDefinition
from dataraum.core.models.base import Result
from dataraum.storage import ConfigOverlay


def _concept(name: str) -> OntologyConcept:
    return OntologyConcept(name=name, description=f"{name} concept", typical_role="measure")


class TestInduceAdhocConcepts:
    @patch("dataraum.analysis.semantic.induction.OntologyInductionAgent")
    @patch("dataraum.analysis.semantic.ontology.OntologyLoader")
    def test_short_circuits_when_concepts_already_exist(
        self, mock_loader_cls: MagicMock, mock_agent_cls: MagicMock, session
    ) -> None:
        # Existing _adhoc ontology already has concepts -> induce must not run.
        mock_loader_cls.return_value.load.return_value = OntologyDefinition(
            name="_adhoc", concepts=[_concept("revenue")]
        )

        result = induce_adhoc_concepts(
            session=session,
            config=MagicMock(),
            provider=MagicMock(),
            renderer=MagicMock(),
            table_ids=["t1"],
        )

        assert result.success
        assert result.value == 0
        mock_agent_cls.assert_not_called()
        assert session.execute(select(ConfigOverlay)).scalars().all() == []

    @patch("dataraum.analysis.semantic.induction.OntologyInductionAgent")
    @patch("dataraum.analysis.semantic.ontology.OntologyLoader")
    def test_inserts_one_concept_overlay_row_per_induced_concept(
        self, mock_loader_cls: MagicMock, mock_agent_cls: MagicMock, session
    ) -> None:
        # Cold start: no existing ontology -> induce, then one overlay row each.
        mock_loader_cls.return_value.load.return_value = None
        mock_agent_cls.return_value.induce.return_value = Result.ok(
            OntologyDefinition(name="induced", concepts=[_concept("revenue"), _concept("region")])
        )

        result = induce_adhoc_concepts(
            session=session,
            config=MagicMock(),
            provider=MagicMock(),
            renderer=MagicMock(),
            table_ids=["t1"],
        )

        assert result.success
        assert result.value == 2
        rows = session.execute(select(ConfigOverlay)).scalars().all()
        assert len(rows) == 2
        assert all(r.type == "concept" for r in rows)
        # NB: production passes session_id=None (workspace-scoped); the conftest
        # before_flush hook autofills it for direct-constructed rows, so it isn't
        # asserted here. The load-bearing surface is type + payload.
        assert {r.payload["name"] for r in rows} == {"revenue", "region"}
        assert all(r.payload["vertical"] == "_adhoc" for r in rows)

    @patch("dataraum.analysis.semantic.induction.OntologyInductionAgent")
    @patch("dataraum.analysis.semantic.ontology.OntologyLoader")
    def test_propagates_induction_failure(
        self, mock_loader_cls: MagicMock, mock_agent_cls: MagicMock, session
    ) -> None:
        mock_loader_cls.return_value.load.return_value = None
        mock_agent_cls.return_value.induce.return_value = Result.fail("LLM down")

        result = induce_adhoc_concepts(
            session=session,
            config=MagicMock(),
            provider=MagicMock(),
            renderer=MagicMock(),
            table_ids=["t1"],
        )

        assert not result.success
        assert "LLM down" in (result.error or "")
        assert session.execute(select(ConfigOverlay)).scalars().all() == []

    @patch("dataraum.analysis.semantic.induction.OntologyInductionAgent")
    @patch("dataraum.analysis.semantic.ontology.OntologyLoader")
    def test_fails_when_induction_returns_no_concepts(
        self, mock_loader_cls: MagicMock, mock_agent_cls: MagicMock, session
    ) -> None:
        mock_loader_cls.return_value.load.return_value = None
        mock_agent_cls.return_value.induce.return_value = Result.ok(
            OntologyDefinition(name="induced", concepts=[])
        )

        result = induce_adhoc_concepts(
            session=session,
            config=MagicMock(),
            provider=MagicMock(),
            renderer=MagicMock(),
            table_ids=["t1"],
        )

        assert not result.success
        assert "no concepts" in (result.error or "").lower()
