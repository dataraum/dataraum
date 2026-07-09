"""One-turn tool-output schema repair (DAT-699, DAT-710).

When a model's forced tool call returns arguments that fail Pydantic validation,
re-prompt the model with its own serialized output plus the exact validation
error under a forced tool choice, and validate again. A finished, correct call
is never discarded on a serialization slip, and a single malformed element never
fails the whole phase. Enforcement, not coercion: the model fixes its own output;
the schema is not ``json.loads``-ed behind its back.

This is the recall-safe alternative to ``ToolDefinition.strict`` for **large
batched extractions** — a strict grammar makes the model legally under-produce on
those (see ``ToolDefinition.strict``), so the repair turn, not strict, is the
right lever for tools like ``generate_sql`` and ``analyze_tables`` that emit a
variable-length set. Generic over the output model so every structured-output
agent shares one implementation.
"""

from __future__ import annotations

import json
from typing import Any

import structlog
from pydantic import BaseModel, ValidationError

from dataraum.core.models.base import Result
from dataraum.llm.providers.base import (
    ConversationRequest,
    LLMProvider,
    Message,
    ToolDefinition,
)

logger = structlog.get_logger(__name__)


def repair_tool_output[T: BaseModel](
    provider: LLMProvider,
    tool: ToolDefinition,
    invalid_input: dict[str, Any],
    error: ValidationError,
    output_cls: type[T],
    *,
    model: str,
    label: str,
    max_tokens: int,
) -> Result[T]:
    """One schema-repair turn: the model fixes its own tool output.

    The repair request is a fresh single-turn conversation (no dataset context,
    so it is cheap, and no assistant-turn continuation, so it cannot trip the
    thinking-block continuation constraint) under a forced tool choice. One
    attempt: a model that cannot satisfy the schema twice fails loud with both
    errors, so the caller degrades gracefully instead of crashing the phase.

    Args:
        provider: The LLM provider to re-prompt.
        tool: The tool whose arguments failed validation.
        invalid_input: The model's rejected tool input.
        error: The validation error to feed back to the model.
        output_cls: The Pydantic model the repaired input must validate against.
        model: The model id to run the repair on.
        label: Base label for the call (``"_repair"`` is appended).
        max_tokens: Output token ceiling for the repair call.

    Returns:
        The validated output, or a loud failure carrying both errors.
    """
    repair_prompt = (
        "Your previous call to the tool below failed schema validation.\n\n"
        f"Validation error:\n{error}\n\n"
        f"Your tool input was:\n{json.dumps(invalid_input, indent=2)}\n\n"
        f"Call {tool.name} again with the SAME content, corrected to satisfy "
        "the schema exactly — e.g. a field you emitted as a JSON-encoded "
        "string must be a structured object. Fix only the schema "
        "violations; do not change the substance of any field."
    )
    request = ConversationRequest(
        messages=[Message(role="user", content=repair_prompt)],
        system="You repair tool-call arguments to satisfy their JSON schema exactly.",
        tools=[tool],
        tool_choice={"type": "tool", "name": tool.name},
        label=f"{label}_repair",
        max_tokens=max_tokens,
        model=model,
    )
    logger.warning("tool_output_schema_repair", tool=tool.name, error=str(error)[:200])
    response = provider.converse(request).unwrap()
    if not response.tool_calls:
        return Result.fail(
            f"Failed to validate tool response ({error}); the schema-repair "
            "turn produced no tool call"
        )
    try:
        return Result.ok(output_cls.model_validate(response.tool_calls[0].input))
    except ValidationError as second:
        return Result.fail(
            f"Failed to validate tool response after a repair turn: {second} "
            f"(original error: {error})"
        )
