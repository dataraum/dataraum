"""Prompt template loading and rendering.

Loads prompt templates from config/prompts/*.yaml and renders them
with context variables using string formatting.
"""

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class PromptTemplate(BaseModel):
    """A prompt template from YAML."""

    name: str
    version: str
    description: str
    temperature: float
    # Legacy: single prompt (for backward compatibility)
    prompt: str | None = None  # Template with {variable} placeholders
    # New: system/user split for Anthropic API best practices
    system_prompt: str | None = None  # System message (instructions, role)
    user_prompt: str | None = None  # User message (context, data)
    inputs: dict[str, Any] = Field(default_factory=dict)
    output_schema: dict[str, Any] = Field(default_factory=dict)
    validation: dict[str, list[str]] = Field(default_factory=dict)


class PromptRenderer:
    """Render prompt templates with context variables.

    Loads templates from YAML files and caches them in memory.
    Supports variable substitution and default values.
    """

    def __init__(self, prompts_dir: Path | None = None):
        """Initialize prompt renderer.

        Args:
            prompts_dir: Directory containing prompt YAML files.
                        If None, uses config/prompts/
        """
        if prompts_dir is None:
            prompts_dir = Path("config/prompts")

        self.prompts_dir = prompts_dir
        self._cache: dict[str, PromptTemplate] = {}

    def load_template(self, name: str) -> PromptTemplate:
        """Load and cache a prompt template.

        Args:
            name: Template name (without .yaml extension)

        Returns:
            Loaded prompt template

        Raises:
            FileNotFoundError: If template file doesn't exist
            yaml.YAMLError: If YAML is invalid
            pydantic.ValidationError: If template doesn't match schema
        """
        if name in self._cache:
            return self._cache[name]

        template_path = self.prompts_dir / f"{name}.yaml"
        if not template_path.exists():
            raise FileNotFoundError(
                f"Prompt template not found: {template_path}. "
                f"Available templates: {self._list_templates()}"
            )

        with open(template_path) as f:
            data = yaml.safe_load(f)

        template = PromptTemplate(**data)
        self._cache[name] = template
        return template

    def render(self, template_name: str, context: dict[str, Any]) -> tuple[str, float]:
        """Render a prompt with context variables (legacy single-prompt format).

        Args:
            template_name: Name of template to render
            context: Context variables for substitution

        Returns:
            Tuple of (rendered_prompt, temperature)

        Raises:
            ValueError: If required inputs are missing
            KeyError: If template has undefined variables
        """
        template = self.load_template(template_name)
        full_context = self._prepare_context(template, context)

        # For legacy templates with single prompt
        if template.prompt:
            try:
                rendered = template.prompt.format(**full_context)
            except KeyError as e:
                raise KeyError(
                    f"Template '{template_name}' has undefined variable: {e}. "
                    f"Available context: {list(full_context.keys())}"
                ) from e
            return rendered, template.temperature

        # For new templates with system/user split - combine for legacy use
        system = self._render_text(template.system_prompt or "", full_context)
        user = self._render_text(template.user_prompt or "", full_context)
        return f"{system}\n\n{user}", template.temperature

    def render_split(
        self, template_name: str, context: dict[str, Any]
    ) -> tuple[str | None, str, float]:
        """Render a prompt with system/user split.

        Args:
            template_name: Name of template to render
            context: Context variables for substitution

        Returns:
            Tuple of (system_prompt, user_prompt, temperature)
            system_prompt is None for legacy single-prompt templates

        Raises:
            ValueError: If required inputs are missing
            KeyError: If template has undefined variables
        """
        template = self.load_template(template_name)
        full_context = self._prepare_context(template, context)

        # For new templates with system/user split
        if template.system_prompt is not None or template.user_prompt is not None:
            system = self._render_text(template.system_prompt or "", full_context)
            user = self._render_text(template.user_prompt or "", full_context)
            return system, user, template.temperature

        # For legacy templates - return as user message only
        rendered = self._render_text(template.prompt or "", full_context)
        return None, rendered, template.temperature

    def _prepare_context(self, template: PromptTemplate, context: dict[str, Any]) -> dict[str, Any]:
        """Prepare context with defaults and validation."""
        # Validate required inputs
        for input_name, input_spec in template.inputs.items():
            if input_spec.get("required", False) and input_name not in context:
                raise ValueError(
                    f"Missing required input '{input_name}' for template '{template.name}'"
                )

        # Fill in defaults for missing optional inputs
        full_context = {}
        for input_name, input_spec in template.inputs.items():
            if input_name in context:
                full_context[input_name] = context[input_name]
            elif "default" in input_spec:
                full_context[input_name] = input_spec["default"]

        return full_context

    def _render_text(self, text: str, context: dict[str, Any]) -> str:
        """Render text with context variables."""
        try:
            return text.format(**context)
        except KeyError as e:
            raise KeyError(
                f"Template has undefined variable: {e}. Available context: {list(context.keys())}"
            ) from e

    def _list_templates(self) -> list[str]:
        """List available template names."""
        if not self.prompts_dir.exists():
            return []

        return [p.stem for p in self.prompts_dir.glob("*.yaml")]

    def clear_cache(self) -> None:
        """Clear the template cache.

        Useful for development when templates are being modified.
        """
        self._cache.clear()
