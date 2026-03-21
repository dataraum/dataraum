"""YAML-driven pipeline declarations.

Reads structural phase metadata (dependencies, produces, gate, detectors)
from config/pipeline.yaml. The scheduler reads this instead of Python
class properties.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dataraum.core.config import load_pipeline_config
from dataraum.entropy.dimensions import AnalysisKey


@dataclass(frozen=True)
class PhaseDeclaration:
    """Structural metadata for a pipeline phase, read from YAML."""

    name: str
    description: str
    dependencies: list[str]
    produces: set[AnalysisKey] = field(default_factory=set)
    gate: bool = False
    detectors: list[str] = field(default_factory=list)


def load_phase_declarations(
    pipeline_config: dict[str, Any] | None = None,
) -> dict[str, PhaseDeclaration]:
    """Load and validate phase declarations from pipeline.yaml.

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
        raise ValueError(
            "pipeline.yaml 'phases' must be a dict (structured format), "
            "not a list. See spec/05-entropy-measurement-design.md."
        )

    analysis_key_lookup = {k.value: k for k in AnalysisKey}
    declarations: dict[str, PhaseDeclaration] = {}

    for name, spec in phases_raw.items():
        if spec is None:
            spec = {}

        # Parse produces → set[AnalysisKey]
        produces_raw = spec.get("produces", [])
        produces: set[AnalysisKey] = set()
        for key_str in produces_raw:
            ak = analysis_key_lookup.get(key_str)
            if ak is None:
                raise ValueError(
                    f"Phase {name!r}: unknown produces key {key_str!r}. "
                    f"Valid keys: {sorted(analysis_key_lookup)}"
                )
            produces.add(ak)

        declarations[name] = PhaseDeclaration(
            name=name,
            description=spec.get("description", ""),
            dependencies=spec.get("dependencies", []),
            produces=produces,
            gate=spec.get("gate", False),
            detectors=spec.get("detectors", []),
        )

    _validate_dependencies(declarations)
    _validate_no_cycles(declarations)

    return declarations


def _validate_dependencies(declarations: dict[str, PhaseDeclaration]) -> None:
    """Validate that all declared dependencies reference existing phases."""
    all_names = set(declarations)
    for name, decl in declarations.items():
        unknown = [d for d in decl.dependencies if d not in all_names]
        if unknown:
            raise ValueError(f"Phase {name!r} declares unknown dependencies: {unknown}")


def _validate_no_cycles(declarations: dict[str, PhaseDeclaration]) -> None:
    """Detect cycles in the dependency graph via topological sort."""
    in_degree: dict[str, int] = dict.fromkeys(declarations, 0)
    adj: dict[str, list[str]] = {name: [] for name in declarations}
    for name, decl in declarations.items():
        for dep in decl.dependencies:
            adj[dep].append(name)
            in_degree[name] += 1

    queue = [n for n, d in in_degree.items() if d == 0]
    visited = 0
    while queue:
        node = queue.pop(0)
        visited += 1
        for successor in adj[node]:
            in_degree[successor] -= 1
            if in_degree[successor] == 0:
                queue.append(successor)

    if visited != len(declarations):
        # Find the cycle participants for a useful error message
        cycle_nodes = [n for n, d in in_degree.items() if d > 0]
        raise ValueError(f"Dependency cycle detected among phases: {cycle_nodes}")


def validate_detectors(declarations: dict[str, PhaseDeclaration]) -> None:
    """Validate that all declared detector IDs exist in the registry.

    Separated from load_phase_declarations() because it imports the
    detector registry, which triggers heavier module loading.
    """
    from dataraum.entropy.detectors.base import get_default_registry

    registry = get_default_registry()
    known_ids = {d.detector_id for d in registry.get_all_detectors()}

    for name, decl in declarations.items():
        unknown = [d for d in decl.detectors if d not in known_ids]
        if unknown:
            raise ValueError(
                f"Phase {name!r} declares unknown detectors: {unknown}. Known: {sorted(known_ids)}"
            )


def get_all_dependencies_from_declarations(
    phase_name: str,
    declarations: dict[str, PhaseDeclaration],
) -> set[str]:
    """Get all transitive dependencies for a phase from YAML declarations."""
    decl = declarations.get(phase_name)
    if not decl:
        return set()

    deps: set[str] = set(decl.dependencies)
    for dep in decl.dependencies:
        deps |= get_all_dependencies_from_declarations(dep, declarations)
    return deps


def get_downstream_phases_from_declarations(
    phase_name: str,
    declarations: dict[str, PhaseDeclaration],
) -> set[str]:
    """Get all phases that transitively depend on the given phase."""
    downstream: set[str] = set()
    for name in declarations:
        if name == phase_name:
            continue
        if phase_name in get_all_dependencies_from_declarations(name, declarations):
            downstream.add(name)
    return downstream
