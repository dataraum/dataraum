"""Abstract base class for LLM providers.

This module defines the interface that all LLM providers must implement.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel, Field

from dataraum.core.models.base import Result

# === API-failure classification (DAT-503) ===
#
# A provider RAISES one of these typed exceptions on an API failure instead of
# folding the error into ``Result.fail``. Retryability is then carried by the
# exception *type*, not by a substring of the error string, so it survives the
# ``converse`` -> phase-agent -> ``BasePhase.run`` -> ``run_phase`` chain
# without any fragile re-parse. The durable boundary (the worker's
# ``_outcome_or_raise``) classifies by ``isinstance`` and chooses the Temporal
# retry policy accordingly.
#
# This deliberately crosses the "Result, not exceptions, for expected failures"
# convention for the provider transient/permanent path specifically: a
# transient API failure is exactly the case where retryability must live at the
# durable/exception boundary, not as data threaded through a Result chain every
# intermediate layer would have to forward faithfully (the substring tag this
# replaces silently read as permanent the moment a layer reworded the message).


class ProviderError(Exception):
    """Base for provider API failures raised out of :meth:`LLMProvider.converse`.

    Carries the human-readable provider message; the subclass *type* carries the
    retryability the durable boundary reads.
    """


class TransientProviderError(ProviderError):
    """A retryable provider failure — rate limit, 5xx, timeout, connection error.

    Retrying may succeed, so the worker raises a *retryable* Temporal error and
    lets the workflow's LLM retry policy re-run the whole activity with backoff.
    """


class PermanentProviderError(ProviderError):
    """A non-retryable provider failure — auth, bad request, schema, payload.

    Retrying the identical request cannot help; the user must fix credentials,
    the input, or configuration. The worker surfaces it as a non-retryable
    Temporal error.
    """


# === Tool Use Models ===


class ToolDefinition(BaseModel):
    """Definition of a tool the LLM can use.

    ``strict=True`` asks the API to guarantee the tool's arguments validate
    against ``input_schema`` — killing the malformed-args class (Sonnet 5
    stringifying a whole payload into one field, 2026-07-02 smoke). It is
    OPT-IN per tool, not the default: on the large batched extractions the
    strict grammar made Sonnet 5 legally under-produce (column_annotation
    emitted 1 of 8 tables, 642 output tokens vs 6060 — same smoke), so only
    small fixed-shape outputs (validation_sql) enable it. The stringified-
    payload hazard for non-strict tools is handled at the parse boundary
    instead (the provider coerces a stringified array/object argument by
    parsing it against the declared schema).
    """

    name: str
    description: str
    input_schema: dict[str, Any]  # JSON Schema for tool parameters
    strict: bool = False


class ToolCall(BaseModel):
    """A tool call made by the LLM."""

    id: str  # Unique ID for this tool call
    name: str  # Tool name
    input: dict[str, Any]  # Tool input parameters


class ToolResult(BaseModel):
    """Result of executing a tool."""

    tool_use_id: str  # ID of the tool call this is responding to
    content: str  # JSON string of the result
    is_error: bool = False


class Message(BaseModel):
    """A message in a conversation."""

    role: str  # "user", "assistant", "tool_result"
    content: str | list[ToolResult] = ""
    tool_calls: list[ToolCall] | None = None  # For assistant messages with tool use


class ConversationRequest(BaseModel):
    """Request for a multi-turn conversation with tool use."""

    messages: list[Message]
    system: str | None = None
    tools: list[ToolDefinition] = Field(default_factory=list)
    tool_choice: dict[str, str] | None = None  # e.g. {"type": "tool", "name": "..."}
    max_tokens: int = 4096
    temperature: float = 0.0
    model: str | None = None  # Override default model
    # Per-feature output effort (DAT-603): "low" | "medium" | "high" | "xhigh"
    # | "max". None = the API default. The provider only sends it to models
    # that support the parameter.
    effort: str | None = None
    # Greppable agent/phase tag for per-call telemetry (DAT-600). The provider
    # has no phase context of its own, so each call site stamps the prompt
    # template / feature name it is invoking (e.g. "graph_sql_generation").
    label: str | None = None


class ConversationResponse(BaseModel):
    """Response from a conversation request."""

    content: str  # Text content (may be empty if only tool calls)
    tool_calls: list[ToolCall] = Field(default_factory=list)
    stop_reason: str  # "end_turn", "tool_use", "max_tokens"
    model: str
    input_tokens: int
    output_tokens: int
    # Prompt-cache usage (DAT-600). Captured for telemetry today; the verifier
    # for DAT-601 (engine prompt caching) reads cache_read to confirm hits.
    cache_read_input_tokens: int = 0
    cache_creation_input_tokens: int = 0


class LLMProvider(ABC):
    """Abstract base for LLM providers."""

    @abstractmethod
    def converse(self, request: ConversationRequest) -> Result[ConversationResponse]:
        """Send a conversation request with optional tool use.

        Args:
            request: Conversation request with messages, tools, etc.

        Returns:
            Result containing the ConversationResponse on success.

        Raises:
            TransientProviderError: A retryable API failure (rate limit, 5xx,
                timeout, connection error).
            PermanentProviderError: A non-retryable API failure (auth, bad
                request, schema, payload, or any unexpected error).
        """
        pass

    @abstractmethod
    def get_model_for_tier(self, tier: str) -> str:
        """Get model name for a given tier.

        Args:
            tier: Model tier ('fast', 'balanced')

        Returns:
            Model name/identifier for the provider
        """
        pass
