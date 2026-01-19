"""Abstract base class for LLM providers.

This module defines the interface that all LLM providers must implement.
"""

from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel, Field

from dataraum_context.core.models.base import Result


class LLMRequest(BaseModel):
    """Request to LLM provider."""

    prompt: str
    max_tokens: int = 4000
    temperature: float = 0.0
    response_format: str = "json"  # "json" or "text"


class LLMResponse(BaseModel):
    """Response from LLM provider."""

    content: str
    model: str
    input_tokens: int
    output_tokens: int
    cached: bool = False  # True if from our cache
    provider_cached: bool = False  # True if provider cache hit


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
    """Abstract base for LLM providers.

    All LLM providers (Anthropic, OpenAI, Local) must implement this interface.
    """

    @abstractmethod
    def complete(self, request: LLMRequest) -> Result[LLMResponse]:
        """Send completion request to provider.

        Args:
            request: The LLM request with prompt and parameters

        Returns:
            Result containing LLMResponse or error message
        """
        pass

    def converse(self, request: ConversationRequest) -> Result[ConversationResponse]:
        """Send a conversation request with optional tool use.

        This is the preferred method for agentic interactions.
        Default implementation raises NotImplementedError.

        Args:
            request: Conversation request with messages, tools, etc.

        Returns:
            Result containing ConversationResponse or error message
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support conversation/tool use"
        )

    @abstractmethod
    def get_model_for_tier(self, tier: str) -> str:
        """Get model name for a given tier.

        Args:
            tier: Model tier ('fast', 'balanced')

        Returns:
            Model name/identifier for the provider
        """
        pass
