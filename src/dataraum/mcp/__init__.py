"""MCP Server for DataRaum.

The MCP transport is hosted by the FastAPI control plane app
(``dataraum.server.app``); this package exposes the tool registry only.
"""

from dataraum.mcp.server import create_server

__all__ = ["create_server"]
