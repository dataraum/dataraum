"""LLM proposal agent for aggregation lineage (DAT-491).

The LLM is the HYPOTHESIS GENERATOR only: it reads the multi-table schema
(annotations, relationships, fact/grain entities) and proposes candidate
events→measure rollups with exact SQL alignment expressions. Every candidate is
then disposed by the deterministic reconciliation statistic — the LLM never
decides stock vs flow, so a wrong proposal costs a wasted check, never a wrong
verdict. This is the witness-factory pattern: LLM proposes, data grounds.
"""

from __future__ import annotations

from dataraum.analysis.lineage.models import AggregationLineageProposals, LineageCandidate
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.llm.features._base import LLMFeature
from dataraum.llm.providers.base import ConversationRequest, Message, ToolDefinition

logger = get_logger(__name__)

PROPOSAL_TEMPLATE_NAME = "aggregation_lineage"
_TOOL_NAME = "propose_aggregation_lineage"


class AggregationLineageAgent(LLMFeature):
    """Propose candidate events→measure rollups for deterministic disposal."""

    MAX_TOKENS = 4000

    def propose(self, schema_text: str, entities_text: str) -> Result[list[LineageCandidate]]:
        """Return candidate rollups for the session's tables (empty = none plausible).

        Args:
            schema_text: the formatted multi-table schema (tables, columns,
                annotations, relationships) with exact duckdb paths.
            entities_text: the fact/dimension + grain classification block from
                ``semantic_per_table``.
        """
        feature_config = self.config.features.aggregation_lineage
        if not feature_config or not feature_config.enabled:
            return Result.fail("aggregation_lineage feature is disabled in config")

        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                PROPOSAL_TEMPLATE_NAME,
                {"schema": schema_text, "entities": entities_text},
            )
        except Exception as e:
            return Result.fail(f"Failed to render aggregation_lineage prompt: {e}")

        tool = ToolDefinition(
            name=_TOOL_NAME,
            description=(
                "Propose events→measure aggregation-lineage candidates. Return an "
                "empty candidates list when no measure column plausibly aggregates "
                "an event table."
            ),
            input_schema=AggregationLineageProposals.model_json_schema(),
        )
        request = ConversationRequest(
            messages=[Message(role="user", content=user_prompt)],
            system=system_prompt,
            tools=[tool],
            tool_choice={"type": "tool", "name": _TOOL_NAME},
            max_tokens=self.MAX_TOKENS,
            temperature=temperature,
            model=self.provider.get_model_for_tier(feature_config.model_tier),
        )

        result = self.provider.converse(request)
        if not result.success or not result.value:
            return Result.fail(result.error or "LLM call failed")
        response = result.value
        if not response.tool_calls:
            # No tool call = degraded generation; no text-parse rescue (DAT-439).
            return Result.fail(f"LLM did not use the {_TOOL_NAME} tool — no structured output")
        tool_call = response.tool_calls[0]
        if tool_call.name != _TOOL_NAME:
            return Result.fail(f"Unexpected tool call: {tool_call.name}")
        try:
            output = AggregationLineageProposals.model_validate(tool_call.input)
        except Exception as e:
            return Result.fail(f"Failed to validate tool response: {e}")
        logger.info("aggregation_lineage_proposed", candidates=len(output.candidates))
        return Result.ok(output.candidates)
