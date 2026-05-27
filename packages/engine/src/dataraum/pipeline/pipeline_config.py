"""YAML-driven pipeline phase metadata.

Reads per-phase detector declarations from config/pipeline.yaml. The Temporal
activity worker reads each phase's ``detectors`` to run them as post-steps
after the phase completes (``worker/activity.py``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dataraum.core.config import load_pipeline_config


@dataclass(frozen=True)
class PhaseDeclaration:
    """Per-phase metadata read from pipeline.yaml."""

    name: str
    description: str
    detectors: list[str] = field(default_factory=list)


def load_phase_declarations(
    pipeline_config: dict[str, Any] | None = None,
) -> dict[str, PhaseDeclaration]:
    """Load phase declarations from pipeline.yaml.

    Args:
        pipeline_config: Pre-loaded pipeline config dict. If None, loads
            from the active config root.

    Returns:
        Dict of phase name -> PhaseDeclaration, insertion-ordered.
    """
    if pipeline_config is None:
        pipeline_config = load_pipeline_config()

    phases_raw = pipeline_config.get("phases", {})
    if isinstance(phases_raw, list):
        raise ValueError("pipeline.yaml 'phases' must be a dict (structured format), not a list.")

    declarations: dict[str, PhaseDeclaration] = {}
    for name, spec in phases_raw.items():
        if spec is None:
            spec = {}
        declarations[name] = PhaseDeclaration(
            name=name,
            description=spec.get("description", ""),
            detectors=spec.get("detectors", []),
        )

    return declarations
