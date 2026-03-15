"""Unified Query Document model for both Graph and Query Agents.

A QueryDocument represents the complete semantic representation of a query,
containing all the information needed for:
- Semantic search (summary, steps, assumptions)
- Context injection (full document as JSON)
- Library storage (all fields persisted)

This model is used by both Graph Agent (graphs/) and Query Agent (query/)
to ensure consistent storage and retrieval.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class SQLStep:
    """A single step in SQL generation."""

    step_id: str
    sql: str
    description: str

    def to_dict(self) -> dict[str, str]:
        """Convert to dictionary for JSON serialization."""
        return {"step_id": self.step_id, "sql": self.sql, "description": self.description}


@dataclass
class QueryAssumptionData:
    """An assumption made during query generation.

    This is a simplified data class for storage/retrieval purposes.
    For the full QueryAssumption model with methods, see graphs/models.py.
    """

    dimension: str
    target: str
    assumption: str
    basis: str  # "system_default", "inferred", "user_specified"
    confidence: float

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "dimension": self.dimension,
            "target": self.target,
            "assumption": self.assumption,
            "basis": self.basis,
            "confidence": self.confidence,
        }


@dataclass
class QueryDocument:
    """Complete semantic document for a query.

    Used by both Graph Agent and Query Agent for consistent storage.
    Contains all the semantic information needed for:
    - Embedding generation (summary + steps + assumptions)
    - Library retrieval (full context as JSON)
    - Semantic search matching
    """

    summary: str
    steps: list[SQLStep]
    final_sql: str
    column_mappings: dict[str, str] = field(default_factory=dict)
    assumptions: list[QueryAssumptionData] = field(default_factory=list)

    @staticmethod
    def _build_assumptions(
        assumptions: list[dict[str, Any]] | None,
        pydantic_assumptions: list[Any] | None = None,
    ) -> list[QueryAssumptionData]:
        """Convert assumption dicts or Pydantic objects to QueryAssumptionData.

        Args:
            assumptions: List of assumption dicts (takes priority if provided)
            pydantic_assumptions: List of Pydantic assumption objects (fallback)

        Returns:
            List of QueryAssumptionData
        """
        if assumptions:
            return [
                QueryAssumptionData(
                    dimension=a.get("dimension", ""),
                    target=a.get("target", ""),
                    assumption=a.get("assumption", ""),
                    basis=a.get("basis", "inferred"),
                    confidence=a.get("confidence", 0.5),
                )
                for a in assumptions
            ]
        if pydantic_assumptions:
            return [
                QueryAssumptionData(
                    dimension=a.dimension,
                    target=a.target,
                    assumption=a.assumption,
                    basis=a.basis,
                    confidence=a.confidence,
                )
                for a in pydantic_assumptions
            ]
        return []

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "summary": self.summary,
            "steps": [s.to_dict() for s in self.steps],
            "final_sql": self.final_sql,
            "column_mappings": self.column_mappings,
            "assumptions": [a.to_dict() for a in self.assumptions],
        }


__all__ = ["QueryDocument", "SQLStep", "QueryAssumptionData"]
