"""``ColumnAnnotationAgent.annotate`` runaway-emission retry guard (DAT-889).

The per-column phase annotates ALL tables in one free-text-JSON call. Emission
is SAMPLED — Sonnet 5 has no temperature, so an identical request can legitimately
runaway into digit emission on one run and finish cleanly on the next. A run that
hits ``stop_reason=max_tokens`` used to surface a canned hint ("raise max_tokens
or reduce the batch") as a non-retryable ``PhaseFailed`` — killing roughly 1 in 2
calibration passes. These tests pin the fix: on a max_tokens cut-off, ``annotate``
retries on a REDUCED table batch (halved, then per-table) instead of failing
outright, discloses every retry, and still fails loud (never silently thinner)
once its bounded call budget is exhausted.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from dataraum.analysis.semantic.column_agent import ColumnAnnotationAgent
from dataraum.analysis.semantic.ontology import OntologyConcept, OntologyDefinition
from dataraum.analysis.statistics.models import ColumnProfile
from dataraum.core.models.base import ColumnRef, Result
from dataraum.llm.config import LLMPrivacy
from dataraum.llm.providers.base import ConversationResponse


def _profile(table_name: str, column_name: str) -> ColumnProfile:
    return ColumnProfile(
        column_id=f"{table_name}.{column_name}",
        column_ref=ColumnRef(table_name=table_name, column_name=column_name),
        profiled_at=datetime.now(UTC),
        total_count=10,
        null_count=0,
        distinct_count=5,
        null_ratio=0.0,
        cardinality_ratio=0.5,
    )


def _valid_content(*table_names: str) -> str:
    """A syntactically-complete ``ColumnAnnotationOutput`` for the given tables."""
    return json.dumps(
        {
            "tables": [
                {
                    "table_name": name,
                    "columns": [
                        {
                            "column_name": "amount",
                            "semantic_role": "measure",
                            "entity_type": "transaction_amount",
                            "business_term": "Amount",
                            "description": "amount column",
                            "confidence": 0.9,
                            "temporal_behavior_claim": "unsure",
                            "temporal_behavior_claim_confidence": 0.0,
                            "derived_formula_confidence": 0.0,
                        }
                    ],
                }
                for name in table_names
            ]
        }
    )


def _response(content: str, *, stop_reason: str = "end_turn") -> Result[ConversationResponse]:
    return Result.ok(
        ConversationResponse(
            content=content,
            stop_reason=stop_reason,
            model="claude-test",
            input_tokens=100,
            output_tokens=8000,
        )
    )


def _config() -> MagicMock:
    config = MagicMock()
    config.features.column_annotation = MagicMock(enabled=True, model_tier="balanced", effort=None)
    config.limits.max_output_tokens_per_request = 8192
    config.privacy = LLMPrivacy()
    return config


def _agent(provider: MagicMock, renderer: MagicMock | None = None) -> ColumnAnnotationAgent:
    renderer = renderer or MagicMock()
    renderer.render_split.return_value = ("system prompt", "user prompt", 0.0)
    return ColumnAnnotationAgent(config=_config(), provider=provider, prompt_renderer=renderer)


_ONTOLOGY = OntologyDefinition(
    name="test", concepts=[OntologyConcept(name="revenue", kind="measure")]
)


class TestHealthyPathUnchanged:
    @patch("dataraum.analysis.semantic.column_agent.load_workspace_concepts")
    def test_single_call_covers_every_table_no_retry(self, mock_concepts: MagicMock) -> None:
        mock_concepts.return_value = _ONTOLOGY
        provider = MagicMock()
        provider.get_model_for_tier.return_value = "test-model"
        provider.converse.side_effect = [_response(_valid_content("orders", "refunds"))]

        agent = _agent(provider)
        profiles = [_profile("orders", "amount"), _profile("refunds", "amount")]

        result = agent.annotate(
            session=MagicMock(), table_ids=["t1", "t2"], ontology="finance", profiles=profiles
        )

        assert result.success
        assert provider.converse.call_count == 1
        names = {t.table_name for t in result.unwrap().tables}
        assert names == {"orders", "refunds"}
        assert result.warnings == []


class TestRunawaySplitRetry:
    @patch("dataraum.analysis.semantic.column_agent.load_workspace_concepts")
    def test_runaway_on_full_batch_splits_and_merges_full_coverage(
        self, mock_concepts: MagicMock
    ) -> None:
        """COVERAGE INVARIANT: a runaway changes call granularity, never coverage —
        both tables are still annotated once the reduced batches succeed."""
        mock_concepts.return_value = _ONTOLOGY
        provider = MagicMock()
        provider.get_model_for_tier.return_value = "test-model"
        provider.converse.side_effect = [
            _response('{"tables": [{"table_name": "orders", "col', stop_reason="max_tokens"),
            _response(_valid_content("orders")),
            _response(_valid_content("refunds")),
        ]

        agent = _agent(provider)
        profiles = [_profile("orders", "amount"), _profile("refunds", "amount")]

        result = agent.annotate(
            session=MagicMock(), table_ids=["t1", "t2"], ontology="finance", profiles=profiles
        )

        assert result.success
        assert provider.converse.call_count == 3
        names = {t.table_name for t in result.unwrap().tables}
        assert names == {"orders", "refunds"}
        # Disclosed, never silent.
        assert len(result.warnings) == 1
        assert "runaway" in result.warnings[0]
        assert "splitting" in result.warnings[0]

    @patch("dataraum.analysis.semantic.column_agent.load_workspace_concepts")
    def test_runaway_on_single_table_batch_retries_same_table(
        self, mock_concepts: MagicMock
    ) -> None:
        mock_concepts.return_value = _ONTOLOGY
        provider = MagicMock()
        provider.get_model_for_tier.return_value = "test-model"
        provider.converse.side_effect = [
            _response('{"tables": [{"table_name": "orders"...', stop_reason="max_tokens"),
            _response(_valid_content("orders")),
        ]

        agent = _agent(provider)
        profiles = [_profile("orders", "amount")]

        result = agent.annotate(
            session=MagicMock(), table_ids=["t1"], ontology="finance", profiles=profiles
        )

        assert result.success
        assert provider.converse.call_count == 2
        names = {t.table_name for t in result.unwrap().tables}
        assert names == {"orders"}
        assert len(result.warnings) == 1
        assert "retrying the same table" in result.warnings[0]


class TestNonTruncationFailuresAreNotRetried:
    @patch("dataraum.analysis.semantic.column_agent.load_workspace_concepts")
    def test_end_turn_parse_failure_fails_immediately(self, mock_concepts: MagicMock) -> None:
        """A genuine contract break (finished turn, still-bad JSON) is not the
        runaway this guard targets — reducing the batch would not help it."""
        mock_concepts.return_value = _ONTOLOGY
        provider = MagicMock()
        provider.get_model_for_tier.return_value = "test-model"
        provider.converse.side_effect = [_response("not json at all", stop_reason="end_turn")]

        agent = _agent(provider)
        profiles = [_profile("orders", "amount")]

        result = agent.annotate(
            session=MagicMock(), table_ids=["t1"], ontology="finance", profiles=profiles
        )

        assert not result.success
        assert provider.converse.call_count == 1
        assert "stop_reason=end_turn" in (result.error or "")


class TestBoundedAttemptsPartialSuccessNeverMasquerades:
    @patch("dataraum.analysis.semantic.column_agent.load_workspace_concepts")
    def test_one_batch_succeeds_other_exhausts_retries_whole_call_fails(
        self, mock_concepts: MagicMock, monkeypatch
    ) -> None:
        """PARTIAL-SUCCESS SEMANTICS: batch A ("orders") succeeds; batch B
        ("refunds") keeps hitting max_tokens until the call budget is
        exhausted. The whole ``annotate`` call must FAIL — orders' successful
        annotation must never surface as if the phase fully covered the
        request, and the failure must name the gap.
        """
        mock_concepts.return_value = _ONTOLOGY
        monkeypatch.setattr(ColumnAnnotationAgent, "_MAX_ANNOTATION_CALLS", 3)

        provider = MagicMock()
        provider.get_model_for_tier.return_value = "test-model"
        provider.converse.side_effect = [
            # 1: full batch runs away -> splits into ["orders"], ["refunds"].
            _response('{"tables": [{"table_na', stop_reason="max_tokens"),
            # 2: "orders" (popped first, LIFO) succeeds.
            _response(_valid_content("orders")),
            # 3: "refunds" runs away again; budget (3) is now exhausted.
            _response('{"tables": [{"table_na', stop_reason="max_tokens"),
        ]

        agent = _agent(provider)
        profiles = [_profile("orders", "amount"), _profile("refunds", "amount")]

        result = agent.annotate(
            session=MagicMock(),
            table_ids=["t1", "t2"],
            ontology="finance",
            profiles=profiles,
        )

        assert not result.success
        assert result.value is None
        assert provider.converse.call_count == 3
        assert "exhausted" in (result.error or "")
        # The gap names exactly the unresolved table — "orders" already
        # succeeded and must not appear as still-unannotated.
        assert "tables ['refunds'] still unannotated" in (result.error or "")
