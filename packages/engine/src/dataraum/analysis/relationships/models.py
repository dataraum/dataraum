"""Relationship detection and evaluation models.

Models for:
- JoinCandidate: potential join between columns (with evaluation metrics)
- RelationshipCandidate: candidate relationship between tables (with evaluation metrics)
- RelationshipDetectionResult: detection results

Evaluation metrics are populated by analysis/relationships/evaluator.py.
"""

from datetime import datetime

from pydantic import BaseModel, Field


class JoinCandidate(BaseModel):
    """A potential join between two columns.

    Core metrics:
    - join_confidence: value overlap score (Jaccard/containment), min 0.3
    - cardinality: detected relationship cardinality
    - left/right_uniqueness: distinct/total ratio for each column
    - statistical_confidence: confidence in the Jaccard estimate (0-1)
    - algorithm: which algorithm was used (exact, sampled, minhash)

    Evaluation metrics (populated by evaluator.py):
    - left_referential_integrity: % of FK values with matching PK
    - right_referential_integrity: % of PK values that are referenced
    - orphan_count: FK values with no matching PK
    - cardinality_verified: whether detected cardinality matches actual
    """

    column1: str
    column2: str
    join_confidence: float  # Value overlap (Jaccard/containment)
    cardinality: str  # one-to-one, one-to-many, many-to-one, many-to-many

    # Column characteristics (from statistics, not name matching)
    left_uniqueness: float = 0.0  # distinct/total ratio
    right_uniqueness: float = 0.0

    # Statistical confidence in the Jaccard estimate (0-1)
    # Higher = more certain the score is accurate
    # 1.0 = exact computation, <1.0 = sampling/minhash estimate
    statistical_confidence: float = 1.0

    # Algorithm used for computation
    algorithm: str = "exact"  # exact, sampled, or minhash

    # Evaluation metrics (populated by evaluator.py)
    left_referential_integrity: float | None = None  # 0-100%
    right_referential_integrity: float | None = None  # 0-100%
    orphan_count: int | None = None
    cardinality_verified: bool | None = None


class RelationshipCandidate(BaseModel):
    """A candidate relationship between two tables.

    Evaluation metrics (populated by evaluator.py):
    - join_success_rate: % of rows from table1 that match in table2
    - introduces_duplicates: whether join multiplies rows (fan trap)
    """

    table1: str
    table2: str
    join_candidates: list[JoinCandidate] = Field(default_factory=list)

    # Evaluation metrics (populated by evaluator.py)
    join_success_rate: float | None = None  # 0-100%
    introduces_duplicates: bool | None = None


class CompositeKey(BaseModel):
    """A multi-column key that rescues a single-column many-to-many fan-out (DAT-277).

    The structural detector finds every value-overlapping column pair between two
    tables separately; when the best pair is many-to-many (the silent over-count),
    its true key is often composite — a real FK plus one or more shared scoping
    columns. Binding them as ONE key collapses the fan-out.

    Attributes:
        column_pairs: ordered ``(table1_column, table2_column)`` components; the
            first is the anchor (highest-confidence) join, the rest are the scoping
            columns added to collapse the fan-out.
        cardinality: the composite join's cardinality — never ``"many-to-many"``
            (a non-m2m result IS the rescue-success condition).
        coverage: share of the REFERENCING (many) side's non-NULL-key rows the
            composite actually matches — the multiplicity proof says nothing
            about it (DAT-695). Oriented by the measured cardinality, never by
            the arbitrary table1/table2 pairing order. Evidence for the LLM
            judge, never a gate.
        coverage_table: the table whose rows ``coverage`` describes.
    """

    column_pairs: list[tuple[str, str]]
    cardinality: str
    coverage: float | None = None
    coverage_table: str | None = None


class RelationshipDetectionResult(BaseModel):
    """Result of relationship detection."""

    candidates: list[RelationshipCandidate] = Field(default_factory=list)

    total_tables: int = 0
    total_candidates: int = 0
    high_confidence_count: int = 0

    computed_at: datetime | None = None
    duration_seconds: float = 0.0
