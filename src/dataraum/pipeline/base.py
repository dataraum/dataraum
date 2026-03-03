"""Pipeline base types and protocols.

Defines the Phase protocol and related data structures used by the orchestrator.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

    from dataraum.core.connections import ConnectionManager


class PhaseStatus(str, Enum):
    """Status of a pipeline phase."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    GATE_BLOCKED = "gate_blocked"


@dataclass
class PhaseContext:
    """Context passed to each phase.

    Contains database connections and source information.
    """

    session: Session
    duckdb_conn: duckdb.DuckDBPyConnection
    source_id: str
    table_ids: list[str] = field(default_factory=list)

    # Deprecated: kept for orchestrator backward compatibility until Phase 6.
    # No phase code should read this field — use DB queries instead.
    previous_outputs: dict[str, dict[str, Any]] = field(default_factory=dict)

    # Configuration overrides
    config: dict[str, Any] = field(default_factory=dict)

    # Session factory for parallel execution within phases
    # Returns a context manager that yields a Session
    session_factory: Callable[[], Any] | None = None

    # Connection manager for vector DB access (optional)
    manager: ConnectionManager | None = None


@dataclass
class PhaseResult:
    """Result from a phase execution."""

    status: PhaseStatus
    outputs: dict[str, Any] = field(default_factory=dict)
    duration_seconds: float = 0.0
    error: str | None = None
    warnings: list[str] = field(default_factory=list)

    # Metrics for observability
    records_processed: int = 0
    records_created: int = 0

    @classmethod
    def success(
        cls,
        outputs: dict[str, Any] | None = None,
        duration: float = 0.0,
        records_processed: int = 0,
        records_created: int = 0,
        warnings: list[str] | None = None,
    ) -> PhaseResult:
        """Create a successful result."""
        return cls(
            status=PhaseStatus.COMPLETED,
            outputs=outputs or {},
            duration_seconds=duration,
            records_processed=records_processed,
            records_created=records_created,
            warnings=warnings or [],
        )

    @classmethod
    def failed(cls, error: str, duration: float = 0.0) -> PhaseResult:
        """Create a failed result."""
        return cls(
            status=PhaseStatus.FAILED,
            error=error,
            duration_seconds=duration,
        )

    @classmethod
    def skipped(cls, reason: str) -> PhaseResult:
        """Create a skipped result."""
        return cls(
            status=PhaseStatus.SKIPPED,
            error=reason,
        )


class Phase(Protocol):
    """Protocol for pipeline phases.

    Each phase is a callable that takes a PhaseContext and returns a PhaseResult.
    Phases declare their dependencies and can be skipped based on DB state.
    """

    @property
    def name(self) -> str:
        """Unique identifier for this phase."""
        ...

    @property
    def description(self) -> str:
        """Human-readable description."""
        ...

    @property
    def dependencies(self) -> list[str]:
        """List of phase names that must complete before this phase."""
        ...

    def run(self, ctx: PhaseContext) -> PhaseResult:
        """Execute the phase.

        Args:
            ctx: Phase context with connections and source information

        Returns:
            PhaseResult with status and outputs
        """
        ...

    @property
    def entropy_preconditions(self) -> dict[str, float]:
        """Hard entropy dimensions that must be below thresholds before this phase runs."""
        ...

    @property
    def post_verification(self) -> list[str]:
        """Hard detector sub_dimensions to re-measure after this phase completes."""
        ...

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Check if this phase should be skipped.

        Returns:
            None if phase should run, or a reason string if it should be skipped.
        """
        ...
