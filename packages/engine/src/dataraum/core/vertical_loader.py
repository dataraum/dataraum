"""One loader for a vertical's config collections (DAT-481).

Every vertical "family" — concepts, validations, cycles, metrics — loads the
same way: read the shipped vertical's on-disk config (a *directory* of per-id
YAML files for validations/metrics, a *single file* for concepts/cycles), fall
back to an empty base when the vertical is *framed* (declared via the cockpit
``frame`` stage — no on-disk path), then layer the workspace's overlay teach
rows on top (:func:`dataraum.core.overlay.apply_overlay`). An explicit
``verticals_dir`` (tests / fixtures) reads raw YAML and bypasses the overlay.

This collapses the four copy-pasted loaders (``graphs/config``,
``validation/config``, ``cycles/config``, ``semantic/ontology``) and their
duplicate directory walks into one place. The facade owns LOAD + LAYER; each
family still parses the raw dicts into its own model (``TransformationGraph`` /
``ValidationSpec`` / ``OntologyDefinition`` / cycle dicts) — ``collection()``
returns merged raw dicts, never typed objects.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Final

import yaml

from dataraum.core.config import get_config_dir, get_config_file
from dataraum.core.overlay import apply_overlay


class Family(StrEnum):
    """A vertical config collection."""

    CONCEPTS = "concepts"
    VALIDATIONS = "validations"
    CYCLES = "cycles"
    METRICS = "metrics"


@dataclass(frozen=True)
class _FamilySpec:
    """How one family resolves: where it lives, file-vs-dir, and its empty base."""

    subpath: str  # path under ``verticals/<vertical>/``
    is_dir: bool  # directory of per-id YAML files vs a single file
    list_key: str | None  # dir families wrap entries as ``{list_key: [...]}``
    empty_base: Callable[[str], dict[str, Any]]  # framed / missing-source base


# Adding a vertical family is one row here (+ its overlay applier in
# ``core.overlay``'s ``_VERTICAL_REGISTRY``) — not a new loader.
_SPECS: Final[dict[Family, _FamilySpec]] = {
    Family.CONCEPTS: _FamilySpec(
        "ontology.yaml", False, None, lambda v: {"name": v, "concepts": []}
    ),
    Family.VALIDATIONS: _FamilySpec(
        "validations", True, "validations", lambda v: {"validations": []}
    ),
    Family.CYCLES: _FamilySpec("cycles.yaml", False, None, lambda v: {}),
    Family.METRICS: _FamilySpec("metrics", True, "metrics", lambda v: {"metrics": []}),
}


def _walk_dir(directory: Path) -> list[dict[str, Any]]:
    """Recurse a vertical collection directory, one raw dict per YAML document.

    The single directory walk replacing ``graphs/config._read_metric_dir``,
    ``validation/config._read_spec_dir``, and ``GraphLoader._load_directory``.
    Recurses (collections nest by category), supports multi-document files
    (``---``), and skips empty / non-mapping documents.
    """
    entries: list[dict[str, Any]] = []
    for yaml_file in sorted(directory.rglob("*.yaml")):
        with open(yaml_file) as f:
            for doc in yaml.safe_load_all(f):
                if doc and isinstance(doc, dict):
                    entries.append(doc)
    return entries


class VerticalLoader:
    """Load a vertical's config collections, overlay-aware (DAT-481)."""

    def __init__(self, vertical: str, verticals_dir: Path | None = None) -> None:
        """Initialize.

        Args:
            vertical: Vertical name (e.g. ``'finance'``).
            verticals_dir: Root verticals dir override. ``None`` (production)
                resolves the config tree and layers overlay rows; an explicit
                path (tests / fixtures) reads raw YAML and bypasses the overlay.
        """
        self.vertical = vertical
        self.verticals_dir = verticals_dir

    def collection(self, family: Family) -> dict[str, Any]:
        """Return a family's merged (library ⊕ overlay) raw config dict.

        Dir families (validations/metrics) return ``{list_key: [entry, ...]}``;
        single-file families (concepts/cycles) return the file's merged dict. A
        framed vertical with no on-disk source resolves to the family's empty
        base (⊕ any overlay rows) — never raises; "nothing declared" is a loud
        outcome at the phase tier, not a loader crash.
        """
        spec = _SPECS[family]
        if self.verticals_dir is not None:
            return self._read_raw(spec)
        relative = f"verticals/{self.vertical}/{spec.subpath}"
        return apply_overlay(relative, self._read_base(spec, relative))

    def shipped_base(self, family: Family) -> dict[str, Any]:
        """A family's SHIPPED on-disk config, WITHOUT the overlay ``⊕`` layer.

        The seed source for a typed home (DAT-735): a config→DB seed writes the
        shipped vocabulary only, so the teach overlay stays a SEPARATE read-time
        ``⊕`` layer and is never absorbed into the seed rows (contrast
        :meth:`collection`, which applies the overlay). A framed vertical with no
        on-disk source resolves to the family's empty base. An explicit
        ``verticals_dir`` (tests) reads raw YAML the same way.
        """
        spec = _SPECS[family]
        if self.verticals_dir is not None:
            return self._read_raw(spec)
        relative = f"verticals/{self.vertical}/{spec.subpath}"
        return self._read_base(spec, relative)

    def _read_raw(self, spec: _FamilySpec) -> dict[str, Any]:
        """Test path: raw on-disk read under ``verticals_dir``, no overlay."""
        assert self.verticals_dir is not None
        path = self.verticals_dir / self.vertical / spec.subpath
        if spec.is_dir:
            assert spec.list_key is not None
            return {spec.list_key: _walk_dir(path) if path.is_dir() else []}
        if not path.is_file():
            return spec.empty_base(self.vertical)
        with open(path) as f:
            return yaml.safe_load(f) or {}

    def _read_base(self, spec: _FamilySpec, relative: str) -> dict[str, Any]:
        """Production base: the on-disk config, or the empty (framed) base."""
        if spec.is_dir:
            assert spec.list_key is not None
            try:
                directory = get_config_dir(relative)
            except FileNotFoundError:
                return spec.empty_base(self.vertical)  # framed / no shipped dir
            return {spec.list_key: _walk_dir(directory)}
        try:
            path = get_config_file(relative)
        except FileNotFoundError:
            return spec.empty_base(self.vertical)
        with open(path) as f:
            return yaml.safe_load(f) or {}


__all__ = ["Family", "VerticalLoader"]
