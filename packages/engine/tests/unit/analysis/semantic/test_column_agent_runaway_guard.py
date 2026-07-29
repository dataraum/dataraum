"""``ColumnAnnotationAgent.annotate`` runaway/omission retry guard (DAT-889).

The per-column phase annotates ALL tables in one structured-output call
(DAT-807: the API constrains decoding to the schema). Constrained decoding
fixes the JSON's GRAMMAR (shape), but not the LENGTH of a single string/number
literal, and thinking tokens (when used) are unconstrained prose — so emission
is still SAMPLED: Sonnet 5 has no temperature, and an identical request can
legitimately runaway into a digit literal that never terminates on one run and
finish cleanly on the next. A run that hits ``stop_reason=max_tokens`` used to
surface a canned hint ("raise max_tokens or reduce the batch") as a
non-retryable ``PhaseFailed`` — killing roughly 1 in 2 calibration passes.

These tests pin the fix on BOTH coverage axes:
1. Call-shape axis: a max_tokens cut-off retries on a REDUCED table batch
   (halved, then per-table) instead of failing outright.
2. Content axis: a parse-clean response that silently omits a requested
   table is retried on the gap, not accepted as a thinner success.

Every retry is disclosed (never silent), and the call budget is bounded so a
persistently-runaway workspace still fails loud rather than looping forever —
naming every table still unannotated, even when several batches remain
unattempted at the moment the budget runs out.
"""

from __future__ import annotations

import json
import re
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
    renderer.render_split.return_value = ("system prompt", "user prompt")
    return ColumnAnnotationAgent(config=_config(), provider=provider, prompt_renderer=renderer)


def _force_budget(monkeypatch, n: int) -> None:
    """Force the per-call budget to exactly ``n`` regardless of table count."""
    monkeypatch.setattr(ColumnAnnotationAgent, "_annotation_call_budget", lambda self, count: n)


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

    @patch("dataraum.analysis.semantic.column_agent.load_workspace_concepts")
    def test_no_tables_short_circuits_without_a_call(self, mock_concepts: MagicMock) -> None:
        mock_concepts.return_value = _ONTOLOGY
        provider = MagicMock()
        provider.get_model_for_tier.return_value = "test-model"

        agent = _agent(provider)

        result = agent.annotate(session=MagicMock(), table_ids=[], ontology="finance", profiles=[])

        assert result.success
        assert result.unwrap().tables == []
        assert provider.converse.call_count == 0


class TestRunawaySplitRetry:
    """Call-shape axis: a max_tokens cut-off retries on a reduced batch."""

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

    @patch("dataraum.analysis.semantic.column_agent.load_workspace_concepts")
    def test_each_attempt_dumps_under_its_own_key(self, mock_concepts: MagicMock) -> None:
        """A same-batch retry must not overwrite the first attempt's dump (DAT-890).

        The offline dump path is (label, dump_key, prompt_hash). Retrying the
        SAME batch renders the SAME prompt, so label and hash both repeat — only
        ``dump_key`` separates the attempts. Without it the runaway payload the
        guard exists to expose is overwritten by the clean retry that follows,
        which is why no DAT-889 response survives in the eval artifacts.
        """
        mock_concepts.return_value = _ONTOLOGY
        provider = MagicMock()
        provider.get_model_for_tier.return_value = "test-model"
        provider.converse.side_effect = [
            _response('{"tables": [{"table_name": "orders"...', stop_reason="max_tokens"),
            _response(_valid_content("orders")),
        ]

        agent = _agent(provider)
        agent.annotate(
            session=MagicMock(),
            table_ids=["t1"],
            ontology="finance",
            profiles=[_profile("orders", "amount")],
        )

        keys = [c.args[0].dump_key for c in provider.converse.call_args_list]
        assert keys == ["column_annotation.a01", "column_annotation.a02"]
        # The telemetry label stays stable across attempts — only the dump splits.
        labels = {c.args[0].label for c in provider.converse.call_args_list}
        assert labels == {"column_annotation"}


class TestContentOmissionRetry:
    """Content axis: a parse-clean response that omits a requested table."""

    @patch("dataraum.analysis.semantic.column_agent.load_workspace_concepts")
    def test_omitted_table_is_retried_and_recovers(self, mock_concepts: MagicMock) -> None:
        mock_concepts.return_value = _ONTOLOGY
        provider = MagicMock()
        provider.get_model_for_tier.return_value = "test-model"
        provider.converse.side_effect = [
            # Parses fine, stop_reason=end_turn (a genuinely finished turn) —
            # but silently drops "refunds" from the batch it was asked for.
            _response(_valid_content("orders")),
            _response(_valid_content("refunds")),
        ]

        agent = _agent(provider)
        profiles = [_profile("orders", "amount"), _profile("refunds", "amount")]

        result = agent.annotate(
            session=MagicMock(), table_ids=["t1", "t2"], ontology="finance", profiles=profiles
        )

        assert result.success
        assert provider.converse.call_count == 2
        names = {t.table_name for t in result.unwrap().tables}
        assert names == {"orders", "refunds"}
        assert len(result.warnings) == 1
        assert "omitted" in result.warnings[0]
        assert "retrying the gap" in result.warnings[0]

    @patch("dataraum.analysis.semantic.column_agent.load_workspace_concepts")
    def test_persistent_omission_exhausts_budget_and_fails(
        self, mock_concepts: MagicMock, monkeypatch
    ) -> None:
        """A table that keeps coming back empty (never a parse failure, never
        max_tokens) must still fail loud once the budget runs out — accepting
        an ever-empty response as success would silently report zero
        annotations as if the phase were complete."""
        mock_concepts.return_value = _ONTOLOGY
        _force_budget(monkeypatch, 2)

        provider = MagicMock()
        provider.get_model_for_tier.return_value = "test-model"
        # Parses fine every time, but never actually contains "refunds".
        provider.converse.side_effect = [_response(_valid_content()), _response(_valid_content())]

        agent = _agent(provider)
        profiles = [_profile("refunds", "amount")]

        result = agent.annotate(
            session=MagicMock(), table_ids=["t1"], ontology="finance", profiles=profiles
        )

        assert not result.success
        assert result.value is None
        assert provider.converse.call_count == 2
        assert "refunds" in (result.error or "")
        assert "exhausted" in (result.error or "")


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
        _force_budget(monkeypatch, 3)

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

    @patch("dataraum.analysis.semantic.column_agent.load_workspace_concepts")
    def test_exhaustion_names_every_table_including_still_pending_batches(
        self, mock_concepts: MagicMock, monkeypatch
    ) -> None:
        """The gap message must name EVERY unannotated table, not just the one
        batch that happened to be popped when the budget ran out — several
        OTHER batches can still be sitting unattempted in ``pending`` at that
        exact moment, and dropping them from the message would under-report
        the damage a killed pass actually did.

        Deliberately asserts against the GAP PHRASE specifically (not "the
        name appears anywhere in the error") — the retry-notes trail legitimately
        re-mentions the original 4-table batch too (it logs what was split),
        so a weaker "somewhere in the error" check would pass even if the gap
        computation dropped every still-pending batch (verified: removing the
        `for later_batch in pending` extension keeps a name-anywhere assertion
        green while this one correctly fails).
        """
        mock_concepts.return_value = _ONTOLOGY
        _force_budget(monkeypatch, 3)

        provider = MagicMock()
        provider.get_model_for_tier.return_value = "test-model"
        # Every call runs away — with a 4-table batch and a budget of 3, the
        # guard never gets far enough to succeed on any table.
        provider.converse.side_effect = [
            _response('{"tables": [{"table_na', stop_reason="max_tokens"),
            _response('{"tables": [{"table_na', stop_reason="max_tokens"),
            _response('{"tables": [{"table_na', stop_reason="max_tokens"),
        ]

        agent = _agent(provider)
        table_names = ["orders", "refunds", "invoices", "credits"]
        profiles = [_profile(name, "amount") for name in table_names]

        result = agent.annotate(
            session=MagicMock(),
            table_ids=["t1", "t2", "t3", "t4"],
            ontology="finance",
            profiles=profiles,
        )

        assert not result.success
        assert provider.converse.call_count == 3
        error = result.error or ""
        match = re.search(r"with tables (\[.*?\]) still unannotated", error)
        assert match, f"gap phrase not found in: {error!r}"
        gap_repr = match.group(1)
        for name in table_names:
            assert name in gap_repr, f"{name} missing from gap {gap_repr!r} (full: {error!r})"
