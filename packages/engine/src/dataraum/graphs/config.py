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

from dataraum.core.logging import get_logger
from dataraum.core.vertical_loader import Family, VerticalLoader

logger = get_logger(__name__)


def get_metrics_config(vertical: str, verticals_dir: Path | None = None) -> dict[str, Any]:
    """Load a vertical's metric graphs, layered with ``metric`` overlay rows.

    Thin wrapper over :class:`~dataraum.core.vertical_loader.VerticalLoader`
    (DAT-481): the shipped ``metrics/`` directory (empty base when the vertical
    is framed — declared via the cockpit, no on-disk directory) ⊕ active
    ``metric`` overlay rows (upsert by ``graph_id``). An unknown vertical
    resolves to an EMPTY collection, never raises — "no declared metrics" is a
    loud, explicit outcome at the phase tier. An explicit ``verticals_dir`` reads
    raw YAML and bypasses the overlay (tests).

    Returns:
        ``{"metrics": [graph_def_dict, ...]}`` — the merged collection.
    """
    return VerticalLoader(vertical, verticals_dir).collection(Family.METRICS)


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
