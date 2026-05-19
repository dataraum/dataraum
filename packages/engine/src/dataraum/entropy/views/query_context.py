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
)
from dataraum.entropy.contracts import (
    ConfidenceLevel,
    ContractEvaluation,
    evaluate_contract,
    find_best_contract,
    get_contract,
)
from dataraum.entropy.views.network_context import (
    EntropyForNetwork,
    build_for_network,
)

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

    # Build network context (handles typed table enforcement internally)
    network_ctx = build_for_network(session, table_ids)

    if not network_ctx.columns:
        return EntropyForQuery(
            overall_readiness=network_ctx.overall_readiness or "ready",
            confidence_level=ConfidenceLevel.GREEN,  # No entropy data = assume good
        )

    # Derive readiness counts from network
    overall_readiness = network_ctx.overall_readiness
    high_entropy_count = network_ctx.columns_blocked + network_ctx.columns_investigate
    critical_entropy_count = network_ctx.columns_blocked

    overall_entropy_score: float | None = network_ctx.avg_entropy_score

    # Convert network results to ColumnSummary for contract evaluation
    column_summaries = network_to_column_summaries(network_ctx)

    # Evaluate contracts
    contract_name: str | None = None
    contract_evaluation: ContractEvaluation | None = None
    confidence_level = ConfidenceLevel.YELLOW  # Default when unknown

    if auto_contract:
        # Find strictest passing contract
        best_name, best_eval = find_best_contract(column_summaries)
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
            contract_evaluation = evaluate_contract(column_summaries, contract)
            contract_name = contract
            confidence_level = contract_evaluation.confidence_level
    else:
        # Default contract
        contract_name = "exploratory_analysis"
        try:
            contract_evaluation = evaluate_contract(column_summaries, "exploratory_analysis")
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


def network_to_column_summaries(
    network_ctx: EntropyForNetwork,
) -> dict[str, ColumnSummary]:
    """Convert network results to ColumnSummary for contract evaluation.

    Contracts evaluate raw dimension scores, which we extract from
    the network's per-node evidence AND direct signals (unmapped detectors).

    Args:
        network_ctx: EntropyForNetwork with per-column results

    Returns:
        Dict mapping "table.column" to ColumnSummary with dimension_scores populated
    """
    summaries: dict[str, ColumnSummary] = {}
    for target, col_result in network_ctx.columns.items():
        col_key = target.removeprefix("column:")
        parts = col_key.split(".", 1)
        table_name = parts[0] if len(parts) > 1 else ""
        column_name = parts[1] if len(parts) > 1 else col_key

        # Build dimension_scores from node evidence
        dimension_scores: dict[str, float] = {}
        for ne in col_result.node_evidence:
            if ne.dimension_path:
                dimension_scores[ne.dimension_path] = ne.score

        summary = ColumnSummary(
            column_name=column_name,
            table_name=table_name,
            readiness=col_result.readiness,
            dimension_scores=dimension_scores,
        )
        summaries[col_key] = summary

    # Fold in direct signals (unmapped detector results) so that
    # dimensions like semantic.dimensional are visible to contracts.
    for ds in network_ctx.direct_signals:
        if not ds.dimension_path or not ds.target:
            continue

        col_key = ds.target.removeprefix("column:")
        if col_key in summaries:
            # Add direct signal score to existing summary's dimension_scores.
            # If the dimension path already exists (from node evidence), keep
            # the higher score to be conservative.
            existing = summaries[col_key].dimension_scores.get(ds.dimension_path)
            if existing is None or ds.score > existing:
                summaries[col_key].dimension_scores[ds.dimension_path] = ds.score
        else:
            # Column only has direct signals (no network-mapped detectors).
            # Create a new summary for it.
            parts = col_key.split(".", 1)
            table_name = parts[0] if len(parts) > 1 else ""
            column_name = parts[1] if len(parts) > 1 else col_key

            summaries[col_key] = ColumnSummary(
                column_name=column_name,
                table_name=table_name,
                readiness="ready",  # No network inference available
                dimension_scores={ds.dimension_path: ds.score},
            )

    return summaries
