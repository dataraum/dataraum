"""Overlay-aware loader for the metric (transformation-graph) declared set.

Loads a vertical's metric graph definitions from
``config/verticals/<vertical>/metrics/`` (a directory of per-``graph_id`` YAML
files, possibly nested by category), layered with workspace ``metric`` overlay
teach rows (DAT-456) — the same dual-path pattern validation and cycles use:
the production path is overlay-aware (teach rows upsert over the shipped
vertical's graphs by ``graph_id``; a *framed* vertical with no on-disk
directory resolves overlay-only), while an explicit ``verticals_dir`` (tests /
fixtures) reads raw YAML and bypasses the overlay.

The merged collection IS the declared set for the metric lifecycle family: each
``graph_id`` is declared as one ``metric`` lifecycle artifact, then composed
(grounded) + executed against the workspace. The engine induces nothing —
declares come from the vertical now; user declares arrive via frame-2 teach
rows. ``GraphLoader`` parses the raw dicts this returns into
:class:`~dataraum.graphs.models.TransformationGraph` objects, so a taught metric
is groundable + executable exactly like a shipped one.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from dataraum.core.logging import get_logger

logger = get_logger(__name__)


def _read_metric_dir(metrics_dir: Path) -> list[dict[str, Any]]:
    """Read every metric graph YAML under ``metrics_dir`` into raw dicts.

    Recurses (graphs are nested by category, e.g. ``working_capital/dso.yaml``)
    and supports multi-document files (``---`` separators), mirroring
    :meth:`GraphLoader._load_directory`. Returns the raw definition dicts (each
    carrying a top-level ``graph_id``); parsing into ``TransformationGraph`` is
    the loader's job.
    """
    entries: list[dict[str, Any]] = []
    for yaml_file in sorted(metrics_dir.rglob("*.yaml")):
        with open(yaml_file) as f:
            for doc in yaml.safe_load_all(f):
                if doc:
                    entries.append(doc)
    return entries


def get_metrics_config(vertical: str, verticals_dir: Path | None = None) -> dict[str, Any]:
    """Load a vertical's metric graphs, layered with ``metric`` overlay rows.

    Production path (``verticals_dir`` is ``None``): read the shipped vertical's
    ``metrics/`` directory (empty base when the vertical is framed — declared
    via the cockpit, no on-disk directory), then merge active ``metric`` overlay
    rows via :func:`dataraum.core.overlay.apply_overlay` (upsert by
    ``graph_id``). An unknown vertical resolves to an EMPTY collection, never
    raises — "no declared metrics" is a loud, explicit outcome at the phase
    tier, not a loader crash.

    Test path (explicit ``verticals_dir``): read
    ``<verticals_dir>/<vertical>/metrics`` raw, bypassing the overlay —
    deterministic for unit tests (mirrors ``OntologyLoader`` /
    ``load_all_validation_specs`` / ``get_cycles_config``).

    Args:
        vertical: Vertical name (e.g. ``'finance'``).
        verticals_dir: Root verticals directory override (tests only).

    Returns:
        ``{"metrics": [graph_def_dict, ...]}`` — the merged collection, or
        ``{"metrics": []}`` when neither directory nor overlay declares anything.
    """
    if verticals_dir is not None:
        metrics_dir = verticals_dir / vertical / "metrics"
        entries = _read_metric_dir(metrics_dir) if metrics_dir.is_dir() else []
        return {"metrics": entries}

    from dataraum.core.config import get_config_dir
    from dataraum.core.overlay import apply_overlay

    config_dir: Path | None
    try:
        config_dir = get_config_dir(f"verticals/{vertical}/metrics")
    except FileNotFoundError:
        # Framed vertical (no on-disk directory) or a vertical without shipped
        # metrics — the overlay rows ARE the declared set.
        config_dir = None
    base_entries = _read_metric_dir(config_dir) if config_dir is not None else []
    merged = apply_overlay(f"verticals/{vertical}/metrics", {"metrics": base_entries})
    return {"metrics": merged.get("metrics") or []}


def get_metric_definitions(
    vertical: str, verticals_dir: Path | None = None
) -> dict[str, dict[str, Any]]:
    """Get the declared metric graph definitions keyed by ``graph_id``.

    The declared set for the metric lifecycle family (overlay-aware). A
    later-declared graph with the same ``graph_id`` has already replaced the
    earlier one in :func:`get_metrics_config`; here a duplicate ``graph_id`` in
    the merged list keeps the last occurrence. Definitions without a
    ``graph_id`` are dropped (a malformed graph cannot be a declared artifact).

    Args:
        vertical: Vertical name (e.g. ``'finance'``).
        verticals_dir: Root verticals directory override (tests only).

    Returns:
        Mapping of ``graph_id`` → raw graph definition dict.
    """
    config = get_metrics_config(vertical, verticals_dir)
    definitions: dict[str, dict[str, Any]] = {}
    for entry in config.get("metrics") or []:
        graph_id = entry.get("graph_id")
        if not graph_id:
            continue
        definitions[graph_id] = entry
    logger.debug("metric_definitions_loaded", vertical=vertical, count=len(definitions))
    return definitions
