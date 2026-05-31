"""Container filesystem path conventions.

- ``CONFIG_DIR`` — the ``dataraum-config`` package (verticals, ontologies,
  prompts, llm configs). Bind-mounted into the container at runtime via
  ``docker-compose.yml`` from ``${HOST_CONFIG_DIR:-../dataraum-config}``
  (DAT-361 — config is mounted, not baked into the image).

This is a container-absolute path. On the host (non-container runs) callers
rely on the existing ``DATARAUM_CONFIG_PATH`` env-var override in
:mod:`dataraum.core.config`.

The former ``SOURCES_DIR`` folder-scan is gone (DAT-389): file sources are
addressed by ``s3://<lake-bucket>/<key>`` URIs read over httpfs (validated by
``dataraum.core.uri.validate_source_uri``), not by a bind-mounted sources
directory.
"""

from __future__ import annotations

from pathlib import Path

CONFIG_DIR: Path = Path("/opt/dataraum/config")
"""Bind-mounted dataraum-config package — verticals, ontologies, prompts, llm configs."""
