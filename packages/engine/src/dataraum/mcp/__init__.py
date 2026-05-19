"""MCP Server for DataRaum (legacy — no transport mounted in v1).

This package still exposes ``create_server`` and the 12-tool engine logic
behind it. The v0.2.x HTTP MCP transport is gone (v1 plan pivot — see
Cockpit + Engine REST plan on Confluence). Engine logic migrates into
``src/dataraum/api/`` FastAPI handlers route-by-route; this package
retires once nothing imports it.
"""

from dataraum.mcp.server import create_server

__all__ = ["create_server"]
