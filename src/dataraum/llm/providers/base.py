"""Abstract base class for LLM providers.

This module defines the interface that all LLM providers must implement.
"""

from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel, Field

from dataraum.core.models.base import Result

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
