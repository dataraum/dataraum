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


@dataclass
class PhaseContext:
    """Context passed to each phase.

    Contains database connections, source information, and any
    outputs from previous phases.
    """

    session: Session
    duckdb_conn: duckdb.DuckDBPyConnection
    source_id: str
    table_ids: list[str] = field(default_factory=list)

    # Outputs from previous phases (keyed by phase name)
    previous_outputs: dict[str, dict[str, Any]] = field(default_factory=dict)

    # Configuration overrides
    config: dict[str, Any] = field(default_factory=dict)

    # Session factory for parallel execution within phases
    # Returns a context manager that yields a Session
    session_factory: Callable[[], Any] | None = None

    # Connection manager for vector DB access (optional)
    manager: ConnectionManager | None = None

    def get_output(self, phase_name: str, key: str, default: Any = None) -> Any:
        """Get an output from a previous phase."""
        return self.previous_outputs.get(phase_name, {}).get(key, default)


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
    Phases declare their dependencies and what they produce.
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

    @property
    def outputs(self) -> list[str]:
        """List of output keys this phase produces."""
        ...

    def run(self, ctx: PhaseContext) -> PhaseResult:
        """Execute the phase.

        Args:
            ctx: Phase context with connections and previous outputs

        Returns:
            PhaseResult with status and outputs
        """
        ...

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Check if this phase should be skipped.

        Returns:
            None if phase should run, or a reason string if it should be skipped.
        """
        ...


@dataclass
class PhaseDefinition:
    """Static definition of a phase for the DAG."""

    name: str
    description: str
    dependencies: list[str]
    outputs: list[str]
    requires_llm: bool = False
    parallel_group: str | None = None  # Phases in same group can run in parallel


# Pipeline DAG definition
# This defines the structure; actual Phase implementations are in phases/
PIPELINE_DAG: list[PhaseDefinition] = [
    # ============================================================
    # FOUNDATION PHASES
    # ============================================================
    PhaseDefinition(
        name="import",
        description="Load CSV files into raw tables",
        dependencies=[],
        outputs=["raw_tables"],
    ),
    PhaseDefinition(
        name="typing",
        description="Type inference and resolution",
        dependencies=["import"],
        outputs=["typed_tables", "type_decisions"],
    ),
    # ============================================================
    # ANALYSIS PHASES (can run in parallel post-typing)
    # ============================================================
    PhaseDefinition(
        name="statistics",
        description="Statistical profiling",
        dependencies=["typing"],
        outputs=["statistical_profiles"],
        parallel_group="post_typing",
    ),
    PhaseDefinition(
        name="temporal",
        description="Temporal column analysis",
        dependencies=["typing"],
        outputs=["temporal_profiles"],
        parallel_group="post_typing",
    ),
    # ============================================================
    # STATISTICAL ANALYSIS PHASES
    # ============================================================
    PhaseDefinition(
        name="statistical_quality",
        description="Benford's Law and outlier detection",
        dependencies=["statistics"],
        outputs=["quality_metrics"],
    ),
    PhaseDefinition(
        name="relationships",
        description="Cross-table relationship detection",
        dependencies=["statistics"],
        outputs=["relationship_candidates"],
    ),
    PhaseDefinition(
        name="correlations",
        description="Within-table correlation analysis",
        dependencies=["statistics"],
        outputs=["correlations", "derived_columns"],
    ),
    # ============================================================
    # LLM SEMANTIC PHASES
    # ============================================================
    PhaseDefinition(
        name="semantic",
        description="LLM semantic enrichment",
        dependencies=["relationships", "correlations"],
        outputs=["semantic_annotations", "confirmed_relationships"],
        requires_llm=True,
    ),
    PhaseDefinition(
        name="validation",
        description="LLM-powered validation checks",
        dependencies=["semantic"],  # Needs semantic annotations for better SQL generation
        outputs=["validation_results"],
        requires_llm=True,
    ),
    # ============================================================
    # SLICING PHASES
    # ============================================================
    PhaseDefinition(
        name="slicing",
        description="LLM-powered data slicing",
        dependencies=["semantic"],  # Needs semantic annotations for context
        outputs=["slice_definitions"],
        requires_llm=True,
    ),
    PhaseDefinition(
        name="slice_analysis",
        description="Analysis on slice tables (includes TDA topology)",
        dependencies=["slicing"],
        outputs=["slice_profiles", "slice_topology"],
        requires_llm=True,
    ),
    PhaseDefinition(
        name="temporal_slice_analysis",
        description="Temporal + topology analysis on slices",
        dependencies=["slice_analysis", "temporal"],
        outputs=["temporal_slice_profiles", "slice_topology", "topology_drift"],
    ),
    # ============================================================
    # ENTROPY PHASES
    # ============================================================
    PhaseDefinition(
        name="entropy",
        description="Entropy detection across all dimensions",
        dependencies=[
            "typing",
            "statistics",
            "semantic",
            "relationships",
            "correlations",
            "quality_summary",
        ],
        outputs=["entropy_profiles", "compound_risks"],
    ),
    PhaseDefinition(
        name="entropy_interpretation",
        description="LLM interpretation of entropy metrics",
        dependencies=["entropy"],
        outputs=["entropy_interpretations", "assumptions", "resolution_actions"],
        requires_llm=True,
    ),
    # ============================================================
    # BUSINESS ANALYSIS PHASES (LLM)
    # ============================================================
    PhaseDefinition(
        name="business_cycles",
        description="Expert LLM cycle detection",
        dependencies=["semantic", "relationships"],
        outputs=["cycle_analysis"],
        requires_llm=True,
    ),
    PhaseDefinition(
        name="cross_table_quality",
        description="Cross-table correlation analysis",
        dependencies=["semantic"],
        outputs=["cross_table_correlations", "multicollinearity_groups"],
        # Note: Despite being in the "quality" family, this is statistical analysis
        requires_llm=False,
    ),
    PhaseDefinition(
        name="quality_summary",
        description="LLM quality report generation",
        dependencies=["slice_analysis"],
        outputs=["quality_reports", "quality_grades"],
        requires_llm=True,
    ),
    # ============================================================
    # METRIC CALCULATION
    # ============================================================
    PhaseDefinition(
        name="graph_execution",
        description="Execute metric graphs via LLM SQL generation",
        dependencies=[
            "semantic",
            "statistics",
            "statistical_quality",
            "temporal",
            "relationships",
            "correlations",
            "slicing",
            "quality_summary",
            "business_cycles",
            "entropy_interpretation",
        ],
        outputs=["metrics_calculated", "metrics_skipped"],
        requires_llm=True,
    ),
]


def get_phase_definition(name: str) -> PhaseDefinition | None:
    """Get a phase definition by name."""
    for phase in PIPELINE_DAG:
        if phase.name == name:
            return phase
    return None


def get_all_dependencies(phase_name: str) -> set[str]:
    """Get all transitive dependencies for a phase."""
    phase = get_phase_definition(phase_name)
    if not phase:
        return set()

    deps = set(phase.dependencies)
    for dep in phase.dependencies:
        deps |= get_all_dependencies(dep)
    return deps
