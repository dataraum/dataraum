"""Pipeline base types and protocols.

Defines the Phase protocol and related data structures used by the pipeline scheduler.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

from dataraum.entropy.dimensions import _StrValueMixin

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

    from dataraum.core.connections import ConnectionManager
    from dataraum.entropy.dimensions import AnalysisKey


class PhaseStatus(_StrValueMixin):
    """Status of a pipeline phase."""

    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class PhaseContext:
    """Context passed to each phase.

    Contains database connections and source information.
    """

    session: Session
    duckdb_conn: duckdb.DuckDBPyConnection
    source_id: str
    table_ids: list[str] = field(default_factory=list)

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
    summary: str = ""

    # Metrics for observability
    records_processed: int = 0
    records_created: int = 0

    @classmethod
    def success(
        cls,
        outputs: dict[str, Any] | None = None,
        records_processed: int = 0,
        records_created: int = 0,
        warnings: list[str] | None = None,
        summary: str = "",
    ) -> PhaseResult:
        """Create a successful result.

        Duration is set by BasePhase.run() — phases should not set it.
        """
        return cls(
            status=PhaseStatus.COMPLETED,
            outputs=outputs or {},
            records_processed=records_processed,
            records_created=records_created,
            warnings=warnings or [],
            summary=summary,
        )

    @classmethod
    def failed(cls, error: str, duration: float = 0.0) -> PhaseResult:
        """Create a failed result.

        Duration is normally set by BasePhase.run(). The parameter exists
        only for BasePhase.run() itself to pass elapsed time on exceptions.
        """
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
    def produces_analyses(self) -> set[AnalysisKey]:
        """Analysis keys this phase produces (used for auto-derive)."""
        ...

    @property
    def is_quality_gate(self) -> bool:
        """Whether this phase is a quality gate for entropy assessment."""
        ...

    @property
    def duckdb_layers(self) -> list[str]:
        """DuckDB layers this phase creates (for cleanup)."""
        ...

    def cleanup(
        self,
        session: Session,
        source_id: str,
        table_ids: list[str],
        column_ids: list[str],
    ) -> int:
        """Delete this phase's output records for the given source."""
        ...

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Check if this phase should be skipped.

        Returns:
            None if phase should run, or a reason string if it should be skipped.
        """
        ...
