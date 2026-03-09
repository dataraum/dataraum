"""CLI for dataraum pipeline.

Provides commands for running, exploring, and managing data sources.

Usage:
    dataraum run /path/to/data
    dataraum tui ./pipeline_output
    dataraum query "What was total revenue?"
    dataraum sources discover /path/to/data
    dataraum dev phases

Environment:
    Loads .env file from current directory if present.
    Set ANTHROPIC_API_KEY for LLM phases.
"""

from dataraum.cli.main import app, main

__all__ = ["app", "main"]
