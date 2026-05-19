"""Dump the FastAPI app's OpenAPI 3.1 spec as YAML on stdout.

Usage:
    uv run python scripts/export_openapi.py > openapi.yaml

Importing ``dataraum.server.app`` does not fire the lifespan — only route
registration runs at import time — so this script does not need a live
DuckLake / Postgres substrate. The output is the canonical contract
artifact published to ``dataraum-api``; PR 3c automates the diff-publish
in CI.
"""

from __future__ import annotations

import sys

import yaml

from dataraum.server.app import app


def main() -> int:
    spec = app.openapi()
    yaml.safe_dump(spec, sys.stdout, sort_keys=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())
