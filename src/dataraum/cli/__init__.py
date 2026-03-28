"""CLI for dataraum pipeline.

Provides commands for running the pipeline and developer utilities.

Usage:
    dataraum run /path/to/data
    dataraum dev phases
    dataraum dev context ./pipeline_output

Environment:
    Loads .env file from current directory if present.
    Set ANTHROPIC_API_KEY for LLM phases.
"""

from dataraum.cli.main import app, main

__all__ = ["app", "main"]
