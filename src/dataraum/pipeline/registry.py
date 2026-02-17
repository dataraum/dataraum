"""Phase registry with auto-discovery.

Provides a decorator-based registry for pipeline phases and lazy auto-discovery.
Phase classes are the single source of truth for name, description, dependencies,
and outputs. The registry eliminates manual imports and registration.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dataraum.pipeline.phases.base import BasePhase

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


def get_all_dependencies(phase_name: str) -> set[str]:
    """Get all transitive dependencies for a phase.

    Walks the dependency graph recursively using phase class properties.

    Args:
        phase_name: Name of the phase to resolve dependencies for.

    Returns:
        Set of all transitive dependency phase names.
    """
    registry = get_registry()
    cls = registry.get(phase_name)
    if not cls:
        return set()

    instance = cls()
    deps = set(instance.dependencies)
    for dep in instance.dependencies:
        deps |= get_all_dependencies(dep)
    return deps
