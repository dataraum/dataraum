"""Batch plan agent — LLM-powered fix triage for CLI interactive flow.

Generates a batch action plan: one recommended fix per violating target,
with parameters pre-filled from entropy evidence. The CLI user reviews
the plan and confirms with y/n.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.llm.prompts import PromptRenderer
from dataraum.llm.providers.base import (
    ConversationRequest,
    LLMProvider,
    Message,
    ToolDefinition,
)

logger = get_logger(__name__)


# --- Pydantic tool schemas ---


class ClarifyingQuestion(BaseModel):
    """A follow-up question the agent wants to ask the user."""

    question: str
    question_type: Literal["free_text", "multiple_choice"]
    choices: list[str] = Field(default_factory=list)


class BatchPlanItem(BaseModel):
    """Proposed action for a single target in a batch plan."""

    target: str
    recommended_action: str
    reason: str
    parameters: dict[str, Any] = Field(default_factory=dict)


class BatchActionPlan(BaseModel):
    """Batch action plan for all violating targets in a dimension.

    Generated for multi-target dimensions so the user can review and
    confirm actions for ALL targets in one round instead of N rounds.
    """

    summary: str
    items: list[BatchPlanItem]
    follow_up_questions: list[ClarifyingQuestion] = Field(default_factory=list)


# --- Agent ---


class BatchPlanAgent:
    """LLM-powered agent for CLI fix triage.

    Uses the config_fix prompt in batch_plan mode to propose one action
    per violating target. The user confirms or edits the plan in one
    round, producing multiple teach calls.
    """

    def __init__(
        self,
        provider: LLMProvider,
        renderer: PromptRenderer,
        model: str,
    ) -> None:
        self.provider = provider
        self.renderer = renderer
        self.model = model

    def generate_batch_plan(
        self,
        context: str,
    ) -> Result[BatchActionPlan]:
        """Generate a batch action plan for all violating targets.

        For multi-target dimensions, proposes the best action for each
        target based on component evidence. The user confirms or edits
        the plan in one round, producing multiple teach calls.

        Args:
            context: Structured context with per-column evidence and actions.

        Returns:
            Result containing BatchActionPlan.
        """
        template_context = {
            "mode": "batch_plan",
            "column_context": context,
            "user_answers": "",
        }

        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                "config_fix", template_context
            )
        except Exception as e:
            return Result.fail(f"Failed to render prompt: {e}")

        tool = ToolDefinition(
            name="batch_action_plan",
            description="Propose an action for each violating target in a multi-target dimension.",
            input_schema=BatchActionPlan.model_json_schema(),
        )

        request = ConversationRequest(
            messages=[Message(role="user", content=user_prompt)],
            system=system_prompt,
            tools=[tool],
            tool_choice={"type": "tool", "name": "batch_action_plan"},
            max_tokens=4096,
            temperature=temperature,
            model=self.model,
        )

        response_result = self.provider.converse(request)
        if not response_result.success or not response_result.value:
            return Result.fail(response_result.error or "LLM call failed")

        response = response_result.value
        if not response.tool_calls:
            return Result.fail("LLM did not use the tool")

        try:
            output = BatchActionPlan.model_validate(response.tool_calls[0].input)
            return Result.ok(output)
        except Exception as e:
            return Result.fail(f"Failed to parse batch plan: {e}")
