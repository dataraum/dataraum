"""DataRaum Context Engine.

A rich metadata context engine for AI-driven data analytics. The engine is
driven through the 3-verb Starlette kernel in `dataraum.server`
(`/measure`, `/query`, `/probe`, plus `/health`); the legacy MCP tool
implementations in `dataraum.mcp` are reference-only for the cockpit
takeover. There is no public in-process Python API.
"""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

from dataraum.core.models.base import Result

try:
    __version__ = _pkg_version("dataraum")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"

__all__ = [
    "Result",
    "__version__",
]
