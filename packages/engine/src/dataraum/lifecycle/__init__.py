"""Typed artifact lifecycle — the operating_model substrate (DAT-438).

General across teach types (validation in slice 1; cycles + metrics iterate
it in later slices): a run-versioned artifact row per
``(session, type, key, run)`` plus a typed transition API with stage
authorization. See :mod:`dataraum.lifecycle.db_models` for the versioning
contract and :mod:`dataraum.lifecycle.transitions` for the state machine.
"""

from dataraum.lifecycle.base_runs import BaseRunMap, resolve_base_runs
from dataraum.lifecycle.db_models import LifecycleArtifact
from dataraum.lifecycle.transitions import (
    ArtifactState,
    IllegalTransitionError,
    LifecycleError,
    StageNotAuthorizedError,
    authorize,
    declare_artifact,
    transition,
)

__all__ = [
    "ArtifactState",
    "BaseRunMap",
    "IllegalTransitionError",
    "LifecycleArtifact",
    "LifecycleError",
    "StageNotAuthorizedError",
    "authorize",
    "declare_artifact",
    "resolve_base_runs",
    "transition",
]
