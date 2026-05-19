"""Vertical configuration resolver.

Resolves paths for a specific vertical (e.g. 'finance') within the
config/verticals/ directory. Follows the same pattern as OntologyLoader:
accept optional verticals_dir, default from get_config_dir("verticals").

Fails loudly if the vertical directory does not exist.
"""

from __future__ import annotations

from pathlib import Path


class VerticalConfig:
    """Resolved paths for a specific vertical (e.g. 'finance').

    Raises FileNotFoundError on construction if the vertical directory
    does not exist. This ensures misconfiguration is caught early.

    Usage:
        vc = VerticalConfig("finance")
        vc.cycles_path       # -> config/verticals/finance/cycles.yaml
        vc.validations_dir   # -> config/verticals/finance/validations/
        vc.metrics_dir       # -> config/verticals/finance/metrics/
        vc.ontology_path     # -> config/verticals/finance/ontology.yaml
    """

    def __init__(self, name: str, verticals_dir: Path | None = None):
        """Initialize vertical config.

        Args:
            name: Vertical name (e.g. 'finance')
            verticals_dir: Root verticals directory.
                          If None, uses config/verticals/

        Raises:
            FileNotFoundError: If the vertical directory does not exist.
        """
        if verticals_dir is None:
            from dataraum.core.config import get_config_dir

            verticals_dir = get_config_dir("verticals")

        self.name = name
        self.base_dir = verticals_dir / name

        if not self.base_dir.is_dir():
            raise FileNotFoundError(
                f"Vertical '{name}' not found: {self.base_dir} does not exist. "
                f"Available verticals: {_list_verticals(verticals_dir)}"
            )

    @property
    def ontology_path(self) -> Path:
        """Path to the ontology YAML file."""
        return self.base_dir / "ontology.yaml"

    @property
    def cycles_path(self) -> Path:
        """Path to the cycles YAML file."""
        return self.base_dir / "cycles.yaml"

    @property
    def validations_dir(self) -> Path:
        """Path to the validations directory."""
        return self.base_dir / "validations"

    @property
    def metrics_dir(self) -> Path:
        """Path to the metrics directory."""
        return self.base_dir / "metrics"


def _list_verticals(verticals_dir: Path) -> str:
    """List available vertical names for error messages."""
    if not verticals_dir.is_dir():
        return "(verticals directory not found)"
    names = sorted(d.name for d in verticals_dir.iterdir() if d.is_dir())
    return ", ".join(names) if names else "(none)"
