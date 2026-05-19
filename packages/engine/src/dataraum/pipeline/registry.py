"""Phase registry with auto-discovery and YAML-driven declarations.

Provides a decorator-based registry for pipeline phases, lazy auto-discovery,
and a YAML-aware wrapper that sources structural metadata from pipeline.yaml.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from types import ModuleType

    from sqlalchemy.orm import Session

    from dataraum.entropy.dimensions import AnalysisKey
    from dataraum.pipeline.base import PhaseContext, PhaseResult
    from dataraum.pipeline.phases.base import BasePhase
    from dataraum.pipeline.pipeline_config import PhaseDeclaration

_REGISTRY: dict[str, type[BasePhase]] = {}
_discovered = False


def analysis_phase(cls: type[BasePhase]) -> type[BasePhase]:
    """Class decorator that registers an analysis phase.

    The class is instantiated once at decoration time to read its name property.
    Phase __init__ must have no side effects.
    """
    instance = cls()
    _REGISTRY[instance.name] = cls
    return cls


def get_registry() -> dict[str, type[BasePhase]]:
    """Get all registered phase classes. Triggers discovery on first call."""
    global _discovered
    if not _discovered:
        discover_phases()
        _discovered = True
    return _REGISTRY


def discover_phases() -> None:
    """Import all phase modules to trigger @analysis_phase decorators."""
    import importlib
    import pkgutil

    from dataraum.pipeline import phases as phases_pkg

    for _importer, modname, _ispkg in pkgutil.iter_modules(phases_pkg.__path__):
        if modname != "base" and modname != "__init__":
            importlib.import_module(f"dataraum.pipeline.phases.{modname}")


def get_phase_class(name: str) -> type[BasePhase] | None:
    """Get a phase class by name."""
    return get_registry().get(name)


def import_all_phase_models() -> None:
    """Import all db_model modules declared by registered phases.

    Accessing instance.db_models triggers lazy imports inside each phase's
    property, which registers the models with Base.metadata. No importlib needed.
    """
    registry = get_registry()
    seen: set[str] = set()
    for cls in registry.values():
        instance = cls()
        for module in instance.db_models:
            mod_name = module.__name__
            if mod_name not in seen:
                seen.add(mod_name)


def get_downstream_phases(phase_name: str) -> set[str]:
    """Get all phases that transitively depend on the given phase.

    Uses YAML declarations for the dependency graph.
    """
    from dataraum.pipeline.pipeline_config import (
        get_downstream_phases_from_declarations,
        load_phase_declarations,
    )

    declarations = load_phase_declarations()
    return get_downstream_phases_from_declarations(phase_name, declarations)


def get_all_dependencies(phase_name: str) -> set[str]:
    """Get all transitive dependencies for a phase.

    Uses YAML declarations for the dependency graph.
    """
    from dataraum.pipeline.pipeline_config import (
        get_all_dependencies_from_declarations,
        load_phase_declarations,
    )

    declarations = load_phase_declarations()
    return get_all_dependencies_from_declarations(phase_name, declarations)


class YAMLAwarePhase:
    """Wraps a BasePhase with YAML-sourced structural declarations.

    Delegates runtime behavior (_run, cleanup, should_skip, db_models,
    duckdb_layers) to the inner phase. Returns YAML values for
    dependencies, description, produces_analyses.
    """

    def __init__(self, inner: BasePhase, declaration: PhaseDeclaration) -> None:
        self._inner = inner
        self._decl = declaration

    @property
    def name(self) -> str:
        return self._decl.name

    @property
    def description(self) -> str:
        return self._decl.description

    @property
    def dependencies(self) -> list[str]:
        return self._decl.dependencies

    @property
    def produces_analyses(self) -> set[AnalysisKey]:
        return self._decl.produces

    @property
    def detectors(self) -> list[str]:
        return self._decl.detectors

    # --- Delegated to inner phase ---

    @property
    def duckdb_layers(self) -> list[str]:
        return self._inner.duckdb_layers

    @property
    def db_models(self) -> list[ModuleType]:
        return self._inner.db_models

    def run(self, ctx: PhaseContext) -> PhaseResult:
        return self._inner.run(ctx)

    def cleanup(
        self,
        session: Session,
        source_id: str,
        table_ids: list[str],
        column_ids: list[str],
    ) -> int:
        return self._inner.cleanup(session, source_id, table_ids, column_ids)

    def should_skip(self, ctx: PhaseContext) -> str | None:
        return self._inner.should_skip(ctx)


def build_yaml_aware_phases(
    pipeline_config: dict[str, Any] | None = None,
) -> dict[str, YAMLAwarePhase]:
    """Build phase instances wrapped with YAML declarations.

    Args:
        pipeline_config: Pre-loaded pipeline config dict. If None, loads
            from the active config root.

    Returns:
        Dict of phase name -> YAMLAwarePhase, for phases that have both
        a YAML declaration and a registered Python class.
    """
    from dataraum.pipeline.pipeline_config import load_phase_declarations

    declarations = load_phase_declarations(pipeline_config)
    registry = get_registry()

    phases: dict[str, YAMLAwarePhase] = {}
    for name, decl in declarations.items():
        cls = registry.get(name)
        if cls is None:
            from dataraum.core.logging import get_logger

            get_logger(__name__).warning(
                "phase_declared_but_not_registered",
                phase=name,
                message=f"Phase {name!r} is declared in pipeline.yaml but has no registered class.",
            )
            continue
        phases[name] = YAMLAwarePhase(cls(), decl)

    return phases
