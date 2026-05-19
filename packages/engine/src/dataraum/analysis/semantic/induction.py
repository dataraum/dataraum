"""Ontology Induction Agent — cold-start ontology generation.

Generates a domain ontology from table schemas and column statistics
when no vertical is selected (cold start). The induced ontology becomes
the session's naming convention for business concepts.

The output is written to _adhoc/ontology.yaml and used by the semantic
phase for concept mapping on the same pipeline run.
"""

from __future__ import annotations

import json

from sqlalchemy.orm import Session

from dataraum.analysis.semantic.ontology import OntologyDefinition
from dataraum.analysis.statistics.models import ColumnProfile
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.llm.features._base import LLMFeature
from dataraum.llm.privacy import DataSampler
from dataraum.llm.providers.base import (
    ConversationRequest,
    Message,
    ToolDefinition,
)

logger = get_logger(__name__)


class OntologyInductionAgent(LLMFeature):
    """Generate a domain ontology from table schemas.

    Used on cold start (no vertical selected) to bootstrap the session
    with business concepts. The induced ontology anchors naming
    conventions for all downstream phases.
    """

    def induce(
        self,
        session: Session,
        table_ids: list[str],
    ) -> Result[OntologyDefinition]:
        """Induce an ontology from table schemas and column statistics.

        Args:
            session: SQLAlchemy session with profiled data.
            table_ids: Tables to analyze.

        Returns:
            Result containing an OntologyDefinition with proposed concepts.
        """
        # Reuse SemanticAgent's profile loading (handles hybrid storage, placeholders).
        # __new__ bypasses __init__ — safe because _load_profiles and _build_tables_json
        # only use their parameters, not self state (config/provider/ontology_loader).
        # TODO: extract these as module-level functions in agent.py to remove this pattern.
        from dataraum.analysis.semantic.agent import SemanticAgent

        loader = SemanticAgent.__new__(SemanticAgent)
        profiles_result = SemanticAgent._load_profiles(loader, session, table_ids)
        if not profiles_result.success or not profiles_result.value:
            return Result.fail(
                profiles_result.error or "No column profiles found — run statistics first"
            )
        profiles: list[ColumnProfile] = profiles_result.value

        # Prepare privacy-safe samples
        sampler = DataSampler(self.config.privacy)
        samples = sampler.prepare_samples(profiles)

        # Build context (reuse SemanticAgent's tables JSON builder)
        tables_json = SemanticAgent._build_tables_json(loader, profiles, samples)
        context = {"tables_json": json.dumps(tables_json, indent=2)}

        # Render prompt
        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                "ontology_induction", context
            )
        except Exception as e:
            return Result.fail(f"Failed to render ontology_induction prompt: {e}")

        # Tool definition from OntologyDefinition schema
        schema = OntologyDefinition.model_json_schema()
        tool = ToolDefinition(
            name="induce_ontology",
            description=(
                "Propose a domain ontology with business concepts for the given data schema."
            ),
            input_schema=schema,
        )

        # Call LLM (use balanced tier — ontology induction is a reasoning task)
        model = self.provider.get_model_for_tier("balanced")
        request = ConversationRequest(
            messages=[Message(role="user", content=user_prompt)],
            system=system_prompt,
            tools=[tool],
            tool_choice={"type": "tool", "name": "induce_ontology"},
            max_tokens=self.config.limits.max_output_tokens_per_request,
            temperature=temperature,
            model=model,
        )

        response_result = self.provider.converse(request)
        if not response_result.success or not response_result.value:
            return Result.fail(f"Ontology induction LLM call failed: {response_result.error}")

        response = response_result.value
        if not response.tool_calls:
            return Result.fail("LLM did not use the induce_ontology tool")

        tool_call = response.tool_calls[0]
        try:
            definition = OntologyDefinition.model_validate(tool_call.input)
        except Exception as e:
            return Result.fail(f"Failed to parse ontology induction output: {e}")

        logger.info(
            "ontology_induction_complete",
            concepts=len(definition.concepts),
            name=definition.name,
        )
        return Result.ok(definition)
