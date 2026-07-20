"""The ONE way an agent turns a structured-output response into a typed model.

Every engine agent asks for its result via ``ConversationRequest.output_schema``
(Anthropic structured outputs, DAT-807) and receives it as message content. This
module is the single parse boundary for that content, so the diagnosis of a
failure is written once instead of nine times.

Why a diagnosis matters here: constrained decoding guarantees the payload
matches the schema — but only for a payload the model was allowed to FINISH.
A turn cut off at ``max_tokens`` returns a valid-prefix JSON document that does
not parse; a refused turn returns no payload at all. Both land in the same
``ValidationError`` branch as a genuine contract break, and they are the far
more likely causes. So the failure leads with ``stop_reason``: telling the next
debugger "the API contract broke" when the real cause was truncation sends them
to the wrong place entirely.
"""

from __future__ import annotations

from pydantic import BaseModel, ValidationError

from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.llm.providers.base import ConversationResponse

logger = get_logger(__name__)

# ``stop_reason`` values that explain an unparseable payload by themselves — the
# model never got to finish, so the content is a prefix or absent. Anything else
# with a parse failure is a genuine surprise worth escalating.
_TRUNCATING_STOP_REASONS = frozenset({"max_tokens", "refusal", "pause_turn"})


def parse_structured_output[T: BaseModel](
    response: ConversationResponse,
    output_cls: type[T],
    *,
    label: str,
) -> Result[T]:
    """Validate a structured-output response into ``output_cls``.

    Args:
        response: The provider response whose ``content`` carries the JSON the
            API constrained to ``output_cls``'s schema.
        output_cls: The Pydantic model to validate against.
        label: The agent/phase tag, for the log line and the error text.

    Returns:
        The validated output, or a loud failure naming ``stop_reason`` first.
        A cross-field validator on ``output_cls`` (a contract the grammar cannot
        express) also fails here — that one IS the model's fault, and the
        ``stop_reason`` in the message says so by being ``end_turn``.
    """
    try:
        return Result.ok(output_cls.model_validate_json(response.content))
    except ValidationError as e:
        truncated = response.stop_reason in _TRUNCATING_STOP_REASONS
        logger.warning(
            "structured_output_unparseable",
            label=label,
            stop_reason=response.stop_reason,
            truncated=truncated,
            output_tokens=response.output_tokens,
            content_chars=len(response.content),
        )
        cause = (
            f"the turn did not finish (stop_reason={response.stop_reason}) — raise "
            "max_tokens or reduce the batch"
            if truncated
            else f"stop_reason={response.stop_reason}"
        )
        return Result.fail(f"Failed to parse the {label} output: {cause}. Detail: {e}")
