"""Entropy context for query agent.

Provides EntropyForQuery view for query/agent.py consumption.
This view includes contract evaluation and confidence levels.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.entropy.analysis.aggregator import (
    ColumnSummary,
    EntropyAggregator,
)
from dataraum.entropy.contracts import (
    ConfidenceLevel,
    ContractEvaluation,
    evaluate_contract,
    find_best_contract,
    get_contract,
)
from dataraum.entropy.core.storage import EntropyRepository

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)


@dataclass
class EntropyForQuery:
    """Entropy context optimized for query agent.

    Provides the query agent with:
    - Overall readiness assessment
    - Contract compliance information
    - Confidence levels (traffic light)
    - Column entropy for assumption generation
    """

    # Overall readiness
    overall_readiness: str = "investigate"

    # Counts
    high_entropy_count: int = 0
    critical_entropy_count: int = 0

    # Contract evaluation (if evaluated)
    contract_name: str | None = None
    contract_evaluation: ContractEvaluation | None = None
    confidence_level: ConfidenceLevel = ConfidenceLevel.YELLOW

    # Column summaries keyed by "table.column"
    columns: dict[str, ColumnSummary] = field(default_factory=dict)

    # Computed entropy score (average)
    overall_entropy_score: float | None = None

    # Metadata
    computed_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def is_blocked(self, contract: str | None = None) -> bool:
        """Check if query should be blocked based on entropy.

        Args:
            contract: Optional contract name to check against

        Returns:
            True if query should be blocked
        """
        if self.confidence_level == ConfidenceLevel.RED:
            return True
        if self.overall_readiness == "blocked":
            return True
        return False

    def get_confidence_level(self) -> ConfidenceLevel:
        """Get the confidence level for this query context."""
        return self.confidence_level

    def get_blocking_reason(self) -> str | None:
        """Get human-readable reason for blocking, if blocked.

        Returns:
            Reason string or None if not blocked
        """
        if not self.is_blocked():
            return None

        if self.contract_evaluation and not self.contract_evaluation.is_compliant:
            violations = self.contract_evaluation.get_blocking_violations()
            if violations:
                return f"Contract '{self.contract_name}' violated: {violations[0].details}"

        return f"Data readiness is {self.overall_readiness}"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "overall_readiness": self.overall_readiness,
            "high_entropy_count": self.high_entropy_count,
            "critical_entropy_count": self.critical_entropy_count,
            "contract_name": self.contract_name,
            "confidence_level": self.confidence_level.value,
            "confidence_emoji": self.confidence_level.emoji,
            "overall_entropy_score": self.overall_entropy_score,
            "is_blocked": self.is_blocked(),
            "computed_at": self.computed_at.isoformat(),
        }


def build_for_query(
    session: Session,
    table_ids: list[str],
    *,
    contract: str | None = None,
    auto_contract: bool = False,
) -> EntropyForQuery:
    """Build entropy context for query agent.

    Loads entropy data, evaluates contracts, and returns a view
    optimized for query execution decisions.

    Args:
        session: SQLAlchemy session
        table_ids: List of table IDs to include
        contract: Explicit contract name to evaluate
        auto_contract: If True, find strictest passing contract

    Returns:
        EntropyForQuery with computed summaries and contract evaluation
    """
    if not table_ids:
        return EntropyForQuery(
            overall_readiness="ready",
            confidence_level=ConfidenceLevel.YELLOW,
        )

    repo = EntropyRepository(session)
    aggregator = EntropyAggregator()

    # Get typed table IDs only
    typed_table_ids = repo.get_typed_table_ids(table_ids)
    if not typed_table_ids:
        logger.warning("No typed tables found for query entropy context")
        return EntropyForQuery(
            overall_readiness="ready",
            confidence_level=ConfidenceLevel.YELLOW,  # Unknown quality
        )

    table_map, column_map = repo.get_table_column_mapping(typed_table_ids)

    # Load entropy objects
    entropy_objects = repo.load_for_tables(typed_table_ids, enforce_typed=True)

    if not entropy_objects:
        return EntropyForQuery(
            overall_readiness="ready",
            confidence_level=ConfidenceLevel.GREEN,  # No entropy data = assume good
        )

    # Aggregate into column summaries
    column_summaries, _ = aggregator.summarize_columns_by_table(
        entropy_objects=entropy_objects,
        table_map=table_map,
        column_map=column_map,
    )

    # Calculate statistics
    from dataraum.entropy.config import get_entropy_config

    config = get_entropy_config()
    high_threshold = config.high_entropy_threshold
    critical_threshold = config.critical_entropy_threshold

    high_entropy_count = sum(
        1 for c in column_summaries.values() if c.composite_score >= high_threshold
    )
    critical_entropy_count = sum(
        1 for c in column_summaries.values() if c.composite_score >= critical_threshold
    )

    # Determine overall readiness
    if critical_entropy_count > 0:
        overall_readiness = "blocked"
    elif high_entropy_count > 0:
        overall_readiness = "investigate"
    else:
        overall_readiness = "ready"

    # Calculate overall entropy score
    if column_summaries:
        overall_entropy_score = sum(c.composite_score for c in column_summaries.values()) / len(
            column_summaries
        )
    else:
        overall_entropy_score = None

    # Build a minimal EntropyContext-like object for contract evaluation
    # We need to wrap our summaries in a format contracts.py can use
    entropy_context_wrapper = _create_contract_wrapper(column_summaries)

    # Evaluate contracts
    contract_name: str | None = None
    contract_evaluation: ContractEvaluation | None = None
    confidence_level = ConfidenceLevel.YELLOW  # Default when unknown

    if auto_contract:
        # Find strictest passing contract
        best_name, best_eval = find_best_contract(entropy_context_wrapper)
        if best_name and best_eval:
            contract_name = best_name
            contract_evaluation = best_eval
            confidence_level = best_eval.confidence_level
        else:
            # No contracts pass
            contract_name = "exploratory_analysis"
            confidence_level = ConfidenceLevel.RED
    elif contract:
        # Evaluate explicit contract
        if get_contract(contract) is None:
            logger.warning(f"Contract not found: {contract}")
        else:
            contract_evaluation = evaluate_contract(entropy_context_wrapper, contract)
            contract_name = contract
            confidence_level = contract_evaluation.confidence_level
    else:
        # Default contract
        contract_name = "exploratory_analysis"
        try:
            contract_evaluation = evaluate_contract(entropy_context_wrapper, "exploratory_analysis")
            confidence_level = contract_evaluation.confidence_level
        except ValueError:
            # Contract not configured - use heuristic
            if overall_readiness == "blocked":
                confidence_level = ConfidenceLevel.RED
            elif overall_readiness == "investigate":
                confidence_level = ConfidenceLevel.YELLOW
            else:
                confidence_level = ConfidenceLevel.GREEN

    return EntropyForQuery(
        overall_readiness=overall_readiness,
        high_entropy_count=high_entropy_count,
        critical_entropy_count=critical_entropy_count,
        contract_name=contract_name,
        contract_evaluation=contract_evaluation,
        confidence_level=confidence_level,
        columns=column_summaries,
        overall_entropy_score=overall_entropy_score,
    )


def _create_contract_wrapper(
    column_summaries: dict[str, ColumnSummary],
) -> Any:
    """Create a wrapper object that mimics EntropyContext for contract evaluation.

    The contracts module expects an EntropyContext-like object with:
    - column_profiles: dict[str, ColumnEntropyProfile]
    - compound_risks: list[CompoundRisk]

    We wrap our ColumnSummary objects to provide this interface.
    """
    from dataraum.entropy.models import ColumnEntropyProfile, CompoundRisk, EntropyContext

    # Convert ColumnSummary to ColumnEntropyProfile for contract evaluation
    column_profiles: dict[str, ColumnEntropyProfile] = {}

    for key, summary in column_summaries.items():
        profile = ColumnEntropyProfile(
            column_id=summary.column_id,
            column_name=summary.column_name,
            table_name=summary.table_name,
            structural_entropy=summary.layer_scores.get("structural", 0.0),
            semantic_entropy=summary.layer_scores.get("semantic", 0.0),
            value_entropy=summary.layer_scores.get("value", 0.0),
            computational_entropy=summary.layer_scores.get("computational", 0.0),
            composite_score=summary.composite_score,
            dimension_scores=summary.dimension_scores,
            high_entropy_dimensions=summary.high_entropy_dimensions,
            readiness=summary.readiness,
        )
        column_profiles[key] = profile

    # Collect all compound risks
    all_risks: list[CompoundRisk] = []
    for summary in column_summaries.values():
        all_risks.extend(summary.compound_risks)

    # Create EntropyContext wrapper
    context = EntropyContext(
        column_profiles=column_profiles,
        compound_risks=all_risks,
    )
    context.update_summary_stats()

    return context
