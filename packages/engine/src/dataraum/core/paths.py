"""Container filesystem path conventions.

Two roots, fixed by the DAT-294 Minimum-Port Plan container shell:

- ``SOURCES_DIR`` — user-supplied source yaml (DAT-286 recipes). Volume-mounted
  in ``docker-compose.yml`` from ``${HOST_SOURCES_DIR:-./sources}``.
- ``CONFIG_DIR`` — repo's ``config/`` directory (verticals, ontologies,
  prompts, llm configs). Baked into the image at build time via Dockerfile
  ``COPY config/ /opt/dataraum/config/``.

These are container-absolute paths. On the host (non-container runs) callers
still pass explicit paths or rely on the existing
``DATARAUM_CONFIG_PATH`` env-var override in :mod:`dataraum.core.config`.
"""

from __future__ import annotations

from pathlib import Path

SOURCES_DIR: Path = Path("/var/lib/dataraum/sources")
"""User-supplied source yaml (DAT-286 recipes). Volume-mounted."""

CONFIG_DIR: Path = Path("/opt/dataraum/config")
"""Baked-in repo config — verticals, ontologies, prompts, llm configs."""
