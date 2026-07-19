"""One-turn tool-output repair — schema (DAT-699/710) and content contract (DAT-727).

When a model's forced tool call returns arguments that fail Pydantic validation,
re-prompt the model with its own serialized output plus the exact validation
error under a forced tool choice, and validate again. A finished, correct call
is never discarded on a serialization slip, and a single malformed element never
fails the whole phase. Enforcement, not coercion: the model fixes its own output;
the schema is not ``json.loads``-ed behind its back. The contract twin
(:func:`repair_tool_contract`) applies the same mechanics to outputs that are
schema-valid but violate a caller-enforced semantic contract (the caller
re-checks the repaired output itself).

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
    logger.warning("tool_output_schema_repair", tool=tool.name, error=str(error)[:200])
    return _repair_turn(
        provider,
        tool,
        repair_prompt,
        output_cls,
        original_error=str(error),
        model=model,
        label=label,
        max_tokens=max_tokens,
    )


def repair_tool_contract[T: BaseModel](
    provider: LLMProvider,
    tool: ToolDefinition,
    invalid_input: dict[str, Any],
    violations: list[str],
    output_cls: type[T],
    *,
    model: str,
    label: str,
    max_tokens: int,
) -> Result[T]:
    """One CONTRACT-repair turn: the model fixes a semantically-invalid output.

    The schema twin above catches shape errors; this catches outputs that are
    schema-valid but violate an enforced semantic contract the caller checked
    (e.g. the grounding provenance contract v2, DAT-727 — enumerated columns
    must be members of the served relation schema and complete against the
    emitted SQL parts). Same DAT-710 mechanics: a fresh single-turn
    conversation under a forced tool choice, one attempt, loud double-failure.
    The returned output is only schema-validated here — the caller re-runs its
    OWN semantic check on the repaired output (this module does not know it).
    """
    numbered = "\n".join(f"- {v}" for v in violations)
    repair_prompt = (
        "Your previous call to the tool below was schema-valid but violated "
        "the tool's enforced content contract.\n\n"
        f"Contract violations:\n{numbered}\n\n"
        f"Your tool input was:\n{json.dumps(invalid_input, indent=2)}\n\n"
        f"Call {tool.name} again with the SAME grounding substance, corrected "
        "to resolve every named violation. Fix only what the violations name; "
        "do not change anything else."
    )
    logger.warning("tool_output_contract_repair", tool=tool.name, violations=len(violations))
    return _repair_turn(
        provider,
        tool,
        repair_prompt,
        output_cls,
        original_error="; ".join(violations),
        model=model,
        label=label,
        max_tokens=max_tokens,
    )


def _repair_turn[T: BaseModel](
    provider: LLMProvider,
    tool: ToolDefinition,
    repair_prompt: str,
    output_cls: type[T],
    *,
    original_error: str,
    model: str,
    label: str,
    max_tokens: int,
) -> Result[T]:
    """The shared repair mechanics: forced tool choice, one attempt, loud failure."""
    request = ConversationRequest(
        messages=[Message(role="user", content=repair_prompt)],
        system="You repair tool-call arguments to satisfy their JSON schema exactly.",
        tools=[tool],
        tool_choice={"type": "tool", "name": tool.name},
        label=f"{label}_repair",
        max_tokens=max_tokens,
        model=model,
    )
    response = provider.converse(request).unwrap()
    if not response.tool_calls:
        return Result.fail(
            f"Failed to validate tool response ({original_error}); the repair "
            "turn produced no tool call"
        )
    try:
        return Result.ok(output_cls.model_validate(response.tool_calls[0].input))
    except ValidationError as second:
        return Result.fail(
            f"Failed to validate tool response after a repair turn: {second} "
            f"(original error: {original_error})"
        )
