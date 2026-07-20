"""One-turn CONTENT-contract repair (DAT-727).

When a model's structured output is schema-valid but violates a semantic
contract the caller enforces, re-prompt the model with its own serialized
output plus the exact violations, and validate again. Enforcement, not
coercion: the model fixes its own output; the schema is not ``json.loads``-ed
behind its back.

The schema twin this module used to carry (``repair_tool_output``) is gone with
DAT-807. It existed to survive a MALFORMED payload — a stringified field, a
missing key — which was an artifact of forcing a tool as a structured-output
stand-in. Constrained decoding (``output_config.format``) makes that class
structurally unreachable, so the repair turn for it had nothing left to repair.

What constrained decoding does NOT guarantee is semantic correctness, so this
half stays: the shape is right, the CONTENT violates the contract (the
grounding provenance enumeration, DAT-727). Generic over the output model so
every structured-output agent shares one implementation.
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
)

logger = structlog.get_logger(__name__)


def repair_tool_contract[T: BaseModel](
    provider: LLMProvider,
    invalid_output: dict[str, Any],
    violations: list[str],
    output_cls: type[T],
    *,
    model: str,
    label: str,
    max_tokens: int,
) -> Result[T]:
    """One CONTRACT-repair turn: the model fixes a semantically-invalid output.

    Constrained decoding catches shape errors before they exist; this catches
    outputs that are schema-valid but violate an enforced semantic contract the
    caller checked (e.g. the grounding provenance contract v2, DAT-727 —
    enumerated columns must be members of the served relation schema and
    complete against the emitted SQL parts). The repair request is a fresh
    single-turn conversation (no dataset context, so it is cheap, and no
    assistant-turn continuation, so it cannot trip the thinking-block
    continuation constraint) under the same output schema. One attempt: a model
    that cannot satisfy the contract twice fails loud with both errors, so the
    caller degrades gracefully instead of crashing the phase. The returned
    output is only schema-validated here — the caller re-runs its OWN semantic
    check on the repaired output (this module does not know it).

    Args:
        provider: The LLM provider to re-prompt.
        invalid_output: The model's contract-violating output.
        violations: The caller's violation lines, fed back verbatim.
        output_cls: The Pydantic model the repaired output must validate against.
        model: The model id to run the repair on.
        label: Base label for the call (``"_repair"`` is appended).
        max_tokens: Output token ceiling for the repair call.

    Returns:
        The validated output, or a loud failure carrying both errors.
    """
    numbered = "\n".join(f"- {v}" for v in violations)
    repair_prompt = (
        "Your previous answer was schema-valid but violated the enforced content "
        "contract.\n\n"
        f"Contract violations:\n{numbered}\n\n"
        f"Your output was:\n{json.dumps(invalid_output, indent=2)}\n\n"
        "Answer again with the SAME grounding substance, corrected to resolve "
        "every named violation. Fix only what the violations name; do not change "
        "anything else."
    )
    logger.warning("tool_output_contract_repair", label=label, violations=len(violations))
    request = ConversationRequest(
        messages=[Message(role="user", content=repair_prompt)],
        system="You repair structured answers to satisfy their enforced content contract.",
        output_schema=output_cls.model_json_schema(),
        label=f"{label}_repair",
        max_tokens=max_tokens,
        model=model,
    )
    response = provider.converse(request).unwrap()
    original_error = "; ".join(violations)
    try:
        return Result.ok(output_cls.model_validate_json(response.content))
    except ValidationError as second:
        return Result.fail(
            f"Failed to validate the repaired output: {second} "
            f"(original violations: {original_error})"
        )
