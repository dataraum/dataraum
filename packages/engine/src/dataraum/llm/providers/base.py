"""Abstract base class for LLM providers.

This module defines the interface that all LLM providers must implement.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel, Field

from dataraum.core.models.base import Result

# === API-failure classification ===
#
# A provider tags its ``Result.error`` as transient or permanent so the durable
# layer (the worker's ``_outcome_or_raise``) can decide retryability without a
# provider-specific import or a brittle bare-string match. ``format_api_error``
# (producer) and ``is_transient_error`` (consumer) are the one shared definition
# of that tag — a round-trip test keeps them in lockstep.
TRANSIENT_ERROR_KIND = "transient"
PERMANENT_ERROR_KIND = "permanent"

_TRANSIENT_TAG = f"API error ({TRANSIENT_ERROR_KIND})"


def format_api_error(provider: str, kind: str, message: str) -> str:
    """Render a provider API failure, embedding its transient/permanent ``kind``.

    The ``({kind})`` tag is the single classification carrier read back by
    :func:`is_transient_error` at the retry choke point.
    """
    return f"{provider} API error ({kind}): {message}"


def is_transient_error(error: str | None) -> bool:
    """True if ``error`` was tagged transient by :func:`format_api_error`.

    Transient failures (rate limits, 5xx, timeouts, connection errors) should be
    retried by Temporal; permanent ones (auth, bad request) should not.
    """
    return error is not None and _TRANSIENT_TAG in error


# === Tool Use Models ===


class ToolDefinition(BaseModel):
    """Definition of a tool the LLM can use."""

    name: str
    description: str
    input_schema: dict[str, Any]  # JSON Schema for tool parameters


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


class ConversationResponse(BaseModel):
    """Response from a conversation request."""

    content: str  # Text content (may be empty if only tool calls)
    tool_calls: list[ToolCall] = Field(default_factory=list)
    stop_reason: str  # "end_turn", "tool_use", "max_tokens"
    model: str
    input_tokens: int
    output_tokens: int


class LLMProvider(ABC):
    """Abstract base for LLM providers."""

    @abstractmethod
    def converse(self, request: ConversationRequest) -> Result[ConversationResponse]:
        """Send a conversation request with optional tool use.

        Args:
            request: Conversation request with messages, tools, etc.

        Returns:
            Result containing ConversationResponse or error message
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
