"""CLI for dataraum pipeline.

Provides commands for running, inspecting, and monitoring the pipeline.

Usage:
    dataraum run /path/to/data
    dataraum status ./pipeline_output
    dataraum entropy ./pipeline_output --tui

Environment:
    Loads .env file from current directory if present.
    Set ANTHROPIC_API_KEY for LLM phases.
"""

from dataraum.cli.main import app, main

__all__ = ["app", "main"]
