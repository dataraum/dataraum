"""DataRaum Context Engine.

A rich metadata context engine for AI-driven data analytics. The engine runs
as a Temporal activity worker (`dataraum.worker`); there is no HTTP surface and
no public in-process Python API. The legacy MCP tool implementations have moved
out of the package to `reference/mcp/` (dead, kept only as reference for the
cockpit takeover; slated for deletion in slice 2).
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
