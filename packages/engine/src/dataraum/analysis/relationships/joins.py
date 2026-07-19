"""Join column detection using value overlap with adaptive algorithms.

Uses parallel processing and adaptive algorithm selection for efficient detection.

Performance optimizations:
1. Pre-compute column stats (distinct count, total count) once per column
2. Filter column pairs by cardinality compatibility before expensive intersection
3. Adaptive algorithm selection based on cardinality:
   - Below 1M distinct: Exact computation
   - At or above 1M distinct: MinHash signatures
4. Use parallel processing for intersection queries

Both algorithms are fully deterministic. A reservoir-sampled middle band
(10K–1M distinct) was deleted in DAT-794: it ran on top of a DISTINCT
subquery (so the expensive materialization was already paid, making it
SLOWER than exact — 59ms vs 17ms at 1M distinct), and its unseeded sampling
made candidate detection nondeterministic — a subset FK whose true Jaccard
sits below min_score survives only through the containment>=0.95 rescue,
and the sampled containment estimate dropped such pairs in ~30% of runs.

References:
- MinHash: Broder, A. (1997) "On the resemblance and containment of documents"
"""

import math
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
from typing import Any

import duckdb

from dataraum.core.logging import get_logger

logger = get_logger(__name__)


# Thresholds for algorithm selection
LARGE_CARDINALITY_THRESHOLD = 1_000_000  # Exact below, MinHash above
DEFAULT_NUM_HASHES = 128  # MinHash signature size
MIN_CONFIDENCE_THRESHOLD = 0.5  # Minimum statistical confidence to accept

# The containment rescue only counts toward a REFERENCED side that is a key in
# its own table (DAT-725). An FK target must identify its row, so a true key's
# uniqueness ratio (distinct / non-NULL rows) is 1.0 by definition; the floor
# grants the referenced side the same near-inclusion tolerance the containment
# threshold itself grants the contained side — a few duplicate-loaded key rows
# are dirt, not structure — while any column whose values repeat structurally
# (status/category codes) sits far below it.
REF_UNIQUENESS_MIN = 0.95

# Type compatibility groups for join detection
# Types within a group can be compared for Jaccard similarity
TYPE_GROUPS: dict[str, set[str]] = {
    # Numeric: DuckDB handles implicit casting between all numeric types
    "numeric": {
        "TINYINT",
        "INT1",
        "SMALLINT",
        "INT2",
        "SHORT",
        "INTEGER",
        "INT4",
        "INT",
        "SIGNED",
        "BIGINT",
        "INT8",
        "LONG",
        "HUGEINT",
        "UTINYINT",
        "USMALLINT",
        "UINTEGER",
        "UBIGINT",
        "UHUGEINT",
        "FLOAT",
        "FLOAT4",
        "REAL",
        "DOUBLE",
        "FLOAT8",
        "DECIMAL",
        "NUMERIC",
    },
    # String types
    "string": {"VARCHAR", "CHAR", "TEXT", "STRING", "BPCHAR"},
    # Temporal: cast all to TIMESTAMP for comparison
    "temporal": {
        "DATE",
        "TIME",
        "TIMESTAMP",
        "DATETIME",
        "TIMESTAMP WITH TIME ZONE",
        "TIMESTAMPTZ",
        "TIMESTAMP_S",
        "TIMESTAMP_MS",
        "TIMESTAMP_NS",
    },
    # Boolean
    "boolean": {"BOOLEAN", "BOOL", "LOGICAL"},
    # UUID
    "uuid": {"UUID"},
}


def _get_type_group(resolved_type: str | None) -> str | None:
    """Get the compatibility group for a resolved type.

    Args:
        resolved_type: The column's resolved type (e.g., "VARCHAR", "BIGINT", "DECIMAL(18,2)")

    Returns:
        Group name ("numeric", "string", "temporal", "boolean", "uuid") or None if unknown
    """
    if not resolved_type:
        return None

    # Normalize: uppercase, strip precision like DECIMAL(18,2) -> DECIMAL
    normalized = resolved_type.upper().split("(")[0].strip()

    for group, types in TYPE_GROUPS.items():
        if normalized in types:
            return group
    return None


def _are_types_compatible(type1: str | None, type2: str | None) -> bool:
    """Check if two types can be compared for join detection.

    Types are compatible if they belong to the same type group.
    Unknown types are not compared (conservative approach).

    Args:
        type1: First column's resolved type
        type2: Second column's resolved type

    Returns:
        True if types can be meaningfully compared for Jaccard similarity
    """
    group1 = _get_type_group(type1)
    group2 = _get_type_group(type2)

    # Unknown types are not compared
    if group1 is None or group2 is None:
        return False

    return group1 == group2


def _is_temporal_type(resolved_type: str | None) -> bool:
    """Check if a type is temporal (needs TIMESTAMP casting for comparison)."""
    return _get_type_group(resolved_type) == "temporal"


def _get_cast_expression(column: str, resolved_type: str | None) -> str:
    """Get the SQL expression for a column, with TIMESTAMP cast for temporal types.

    For temporal types (DATE, TIME, TIMESTAMP variants), cast to TIMESTAMP
    so that DATE and TIMESTAMP values can be compared.

    Args:
        column: Column name (will be quoted)
        resolved_type: The column's resolved type

    Returns:
        SQL expression like '"{column}"' or '"{column}"::TIMESTAMP'
    """
    if _is_temporal_type(resolved_type):
        return f'"{column}"::TIMESTAMP'
    return f'"{column}"'


class JoinAlgorithm(Enum):
    """Algorithm used for Jaccard computation."""

    EXACT = "exact"
    MINHASH = "minhash"


@dataclass
class ColumnStats:
    """Pre-computed statistics for a column."""

    column_name: str
    distinct_count: int
    total_count: int
    is_unique: bool  # distinct_count == total_count
    resolved_type: str | None = None  # Column's resolved type (e.g., "VARCHAR", "BIGINT")


def _uniqueness(stats: ColumnStats) -> float:
    """Distinct / non-NULL rows — the key-likeness of a column in its own table."""
    return stats.distinct_count / stats.total_count if stats.total_count else 0.0


@dataclass
class JoinScoreResult:
    """Result of a join score computation."""

    column1: str
    column2: str
    score: float
    cardinality: str
    confidence: float  # Statistical confidence (0-1)
    algorithm: JoinAlgorithm


def _precompute_column_stats(
    conn: duckdb.DuckDBPyConnection,
    table_path: str,
    columns: list[str],
    column_types: dict[str, str | None] | None = None,
) -> dict[str, ColumnStats]:
    """Pre-compute statistics for all columns in a table.

    Uses DuckDB's exact count distinct (no sampling here - stats are fast).

    Args:
        conn: DuckDB connection
        table_path: Path to the table in DuckDB
        columns: List of column names to analyze
        column_types: Optional dict mapping column name -> resolved type
    """
    stats: dict[str, ColumnStats] = {}
    column_types = column_types or {}

    for col in columns:
        try:
            result = conn.execute(f"""
                SELECT
                    COUNT(DISTINCT "{col}") AS distinct_count,
                    COUNT(*) FILTER (WHERE "{col}" IS NOT NULL) AS total_count
                FROM {table_path}
            """).fetchone()

            if result:
                distinct_count = result[0]
                total_count = result[1]
                stats[col] = ColumnStats(
                    column_name=col,
                    distinct_count=distinct_count,
                    total_count=total_count,
                    is_unique=(distinct_count == total_count and distinct_count > 0),
                    resolved_type=column_types.get(col),
                )
        except Exception:
            # Skip columns that fail (e.g., unsupported types)
            pass

    return stats


def _should_compare_columns(
    stats1: ColumnStats,
    stats2: ColumnStats,
    cardinality_ratio_threshold: float = 100.0,
) -> bool:
    """Filter column pairs that are unlikely to be related.

    Skip pairs where:
    - Types are incompatible (e.g., VARCHAR vs BIGINT)
    - Either column is boolean (only 2 possible values — always gives false containment matches)
    - Either column has 0 or 1 distinct values (constant)
    - Cardinality ratio is extreme (e.g., 1000:1)
    """
    # Skip if types are incompatible
    if not _are_types_compatible(stats1.resolved_type, stats2.resolved_type):
        return False

    # Skip boolean columns — {True, False} will always fully contain each other,
    # giving containment=1.0 for every boolean pair regardless of actual relationship
    if _get_type_group(stats1.resolved_type) == "boolean":
        return False

    # Skip constant or near-constant columns
    if stats1.distinct_count <= 1 or stats2.distinct_count <= 1:
        return False

    # Skip if the cardinality ratio is extreme — UNLESS the larger side is a
    # key. The cap prunes value-pool comparisons where a huge distinct-count
    # mismatch makes a join implausible; but an FK target is a key, and a
    # referencing column may legitimately use only a handful of a large key
    # column's values — a real FK's ratio is unbounded — so a (near-)unique
    # larger side keeps the pair alive for the containment rescue
    # (REF_UNIQUENESS_MIN, DAT-725). The rescued comparison stays cheap: the
    # contained side's distinct set is tiny. A non-key larger side at extreme
    # ratio is a value pool — pruned as before.
    ratio = max(stats1.distinct_count, stats2.distinct_count) / max(
        min(stats1.distinct_count, stats2.distinct_count), 1
    )
    if ratio > cardinality_ratio_threshold:
        larger = stats1 if stats1.distinct_count >= stats2.distinct_count else stats2
        if _uniqueness(larger) < REF_UNIQUENESS_MIN:
            return False

    return True


def _determine_cardinality(stats1: ColumnStats, stats2: ColumnStats) -> str:
    """Determine cardinality based on uniqueness."""
    if stats1.is_unique and stats2.is_unique:
        return "one-to-one"
    elif stats1.is_unique and not stats2.is_unique:
        return "one-to-many"
    elif not stats1.is_unique and stats2.is_unique:
        return "many-to-one"
    else:
        return "many-to-many"


def _select_algorithm(stats1: ColumnStats, stats2: ColumnStats) -> JoinAlgorithm:
    """Select the best algorithm based on cardinality.

    Exact below 1M distinct (faster than sampling at every scale it would
    cover, and deterministic — DAT-794); MinHash at or above.
    """
    max_distinct = max(stats1.distinct_count, stats2.distinct_count)

    if max_distinct < LARGE_CARDINALITY_THRESHOLD:
        return JoinAlgorithm.EXACT
    return JoinAlgorithm.MINHASH


def _compute_exact_jaccard(
    conn: duckdb.DuckDBPyConnection,
    table1_path: str,
    table2_path: str,
    col1: str,
    col2: str,
    stats1: ColumnStats,
    stats2: ColumnStats,
) -> JoinScoreResult:
    """Compute exact Jaccard using full distinct values.

    Used below 1M distinct values (DAT-794).
    Returns confidence=1.0 since this is exact.
    """
    with conn.cursor() as cursor:
        try:
            # For temporal types, cast to TIMESTAMP for cross-type comparison
            col1_expr = _get_cast_expression(col1, stats1.resolved_type)
            col2_expr = _get_cast_expression(col2, stats2.resolved_type)

            result = cursor.execute(f"""
                WITH
                vals1 AS (SELECT DISTINCT {col1_expr} AS v FROM {table1_path} WHERE "{col1}" IS NOT NULL),
                vals2 AS (SELECT DISTINCT {col2_expr} AS v FROM {table2_path} WHERE "{col2}" IS NOT NULL)
                SELECT COUNT(*) FROM vals1 WHERE v IN (SELECT v FROM vals2)
            """).fetchone()

            if result is None:
                return JoinScoreResult(col1, col2, 0.0, "unknown", 0.0, JoinAlgorithm.EXACT)

            intersection = result[0]
            count1 = stats1.distinct_count
            count2 = stats2.distinct_count

            if count1 == 0 or count2 == 0:
                return JoinScoreResult(col1, col2, 0.0, "unknown", 0.0, JoinAlgorithm.EXACT)

            union = count1 + count2 - intersection
            jaccard = intersection / union if union > 0 else 0.0

            # Containment (near-inclusion of one distinct set in another): a dirty
            # subset FK (a few orphan values) is still an FK — the candidate must
            # exist for the referential-integrity evaluator to quantify the orphans —
            # so rescue at >=0.95 fractional containment, computed exactly (DAT-794).
            # Containment is FK-shaped evidence only toward a REFERENCED side that
            # is a key — (near-)unique in its own table (REF_UNIQUENESS_MIN) — so
            # each direction is gated on the uniqueness of the side being contained
            # INTO, at ANY cardinality of the contained side (DAT-725: the old
            # `min_distinct > 10` floor made every low-cardinality FK column
            # structurally unproposable — a 2-distinct code column 100%-contained
            # in a unique reference key is a real FK). Trivial mutual containment
            # stays dead under the same gate: two categorical columns drawing from
            # a shared value pool both repeat across rows, so neither side is a
            # key. The fractional value (not a snapped 1.0) is the score: honest
            # evidence for the LLM.
            containment = 0.0
            if _uniqueness(stats2) >= REF_UNIQUENESS_MIN:
                containment = intersection / count1  # col1's values ⊆ col2 (a key)
            if _uniqueness(stats1) >= REF_UNIQUENESS_MIN:
                containment = max(containment, intersection / count2)
            if containment < 0.95:
                containment = 0.0
            score = max(jaccard, containment)

            cardinality = _determine_cardinality(stats1, stats2)

            return JoinScoreResult(
                column1=col1,
                column2=col2,
                score=score,
                cardinality=cardinality,
                confidence=1.0,  # Exact computation
                algorithm=JoinAlgorithm.EXACT,
            )

        except Exception:
            return JoinScoreResult(col1, col2, 0.0, "unknown", 0.0, JoinAlgorithm.EXACT)


def _compute_minhash_jaccard(
    conn: duckdb.DuckDBPyConnection,
    table1_path: str,
    table2_path: str,
    col1: str,
    col2: str,
    stats1: ColumnStats,
    stats2: ColumnStats,
    num_hashes: int = DEFAULT_NUM_HASHES,
) -> JoinScoreResult:
    """Compute Jaccard using MinHash signatures.

    MinHash provides O(n) complexity instead of O(n^2) for intersection.
    Error is O(1/sqrt(k)) where k is the number of hash functions.

    For k=128: ~8.8% standard error
    For k=256: ~6.3% standard error

    Args:
        num_hashes: Number of hash functions (signature size)

    Returns:
        JoinScoreResult with MinHash-estimated Jaccard and confidence
    """
    with conn.cursor() as cursor:
        try:
            # For temporal types, cast to TIMESTAMP first so DATE and TIMESTAMP
            # have consistent string representations for hashing
            col1_expr = _get_cast_expression(col1, stats1.resolved_type)
            col2_expr = _get_cast_expression(col2, stats2.resolved_type)

            # Generate MinHash signatures using DuckDB's hash function
            # We use different seeds by appending different suffixes
            hash_selects1 = []
            hash_selects2 = []

            for i in range(num_hashes):
                seed = f"_mh_seed_{i}"
                hash_selects1.append(f"MIN(hash(CAST({col1_expr} AS VARCHAR) || '{seed}')) AS h{i}")
                hash_selects2.append(f"MIN(hash(CAST({col2_expr} AS VARCHAR) || '{seed}')) AS h{i}")

            # Execute queries for both tables
            sig1_query = f"""
                SELECT {", ".join(hash_selects1)}
                FROM {table1_path}
                WHERE "{col1}" IS NOT NULL
            """
            sig2_query = f"""
                SELECT {", ".join(hash_selects2)}
                FROM {table2_path}
                WHERE "{col2}" IS NOT NULL
            """

            sig1 = cursor.execute(sig1_query).fetchone()
            sig2 = cursor.execute(sig2_query).fetchone()

            if sig1 is None or sig2 is None:
                return JoinScoreResult(col1, col2, 0.0, "unknown", 0.0, JoinAlgorithm.MINHASH)

            # Count matching signature positions (estimate of Jaccard)
            matches = sum(1 for h1, h2 in zip(sig1, sig2, strict=True) if h1 == h2)
            jaccard_estimate = matches / num_hashes

            # MinHash only estimates Jaccard, not containment
            # For containment, we'd need a different approach
            score = jaccard_estimate

            # Statistical confidence for MinHash
            # SE = sqrt(J * (1-J) / k) for true Jaccard J
            # Use our estimate as proxy
            if 0 < jaccard_estimate < 1:
                se = math.sqrt(jaccard_estimate * (1 - jaccard_estimate) / num_hashes)
            else:
                se = 1.0 / math.sqrt(num_hashes)

            confidence = max(0.0, min(1.0, 1.0 - se))

            cardinality = _determine_cardinality(stats1, stats2)

            return JoinScoreResult(
                column1=col1,
                column2=col2,
                score=score,
                cardinality=cardinality,
                confidence=confidence,
                algorithm=JoinAlgorithm.MINHASH,
            )

        except Exception as e:
            logger.debug(f"MinHash failed for {col1}-{col2}: {e}")
            return JoinScoreResult(col1, col2, 0.0, "unknown", 0.0, JoinAlgorithm.MINHASH)


def _compute_join_score_adaptive(
    conn: duckdb.DuckDBPyConnection,
    table1_path: str,
    table2_path: str,
    col1: str,
    col2: str,
    stats1: ColumnStats,
    stats2: ColumnStats,
) -> JoinScoreResult:
    """Compute join score using adaptive algorithm selection.

    Selects the best algorithm based on cardinality:
    - Below 1M distinct: Exact computation (confidence=1.0)
    - At or above 1M distinct: MinHash signatures
    """
    algorithm = _select_algorithm(stats1, stats2)

    if algorithm == JoinAlgorithm.EXACT:
        return _compute_exact_jaccard(conn, table1_path, table2_path, col1, col2, stats1, stats2)
    return _compute_minhash_jaccard(conn, table1_path, table2_path, col1, col2, stats1, stats2)


def find_join_columns(
    conn: duckdb.DuckDBPyConnection,
    table1_path: str,
    table2_path: str,
    columns1: list[str],
    columns2: list[str],
    min_score: float = 0.3,
    min_confidence: float = MIN_CONFIDENCE_THRESHOLD,
    max_workers: int = 8,
    column_types1: dict[str, str | None] | None = None,
    column_types2: dict[str, str | None] | None = None,
    same_table: bool = False,
) -> list[dict[str, Any]]:
    """Find join columns using adaptive algorithm selection.

    Uses a three-phase approach for efficiency:
    1. Pre-compute column statistics (distinct count, total count) once per column
    2. Filter column pairs by type compatibility and cardinality
    3. Compute Jaccard using the best algorithm for each pair's cardinality:
       - Below 1M distinct: Exact computation
       - At or above 1M distinct: MinHash signatures

    Args:
        conn: DuckDB connection
        table1_path: Path to first table
        table2_path: Path to second table
        columns1: Columns from first table
        columns2: Columns from second table
        min_score: Minimum Jaccard/containment score to include
        min_confidence: Minimum statistical confidence to include
        max_workers: Number of parallel workers
        column_types1: Optional dict mapping column name -> resolved type for table1
        column_types2: Optional dict mapping column name -> resolved type for table2
        same_table: True when table1 and table2 are the SAME table (a self-
            referential FK probe, e.g. ``chart_of_accounts.parent_id -> account_id``).
            Restricts the pairs to the upper triangle (index i < j): a column never
            references itself (the diagonal is trivial identity), and each unordered
            pair is tried once — direction is normalized downstream at persist
            (DAT-758). Cross-table detection is unaffected. Assumes ``columns1 ==
            columns2`` (the caller passes one table's columns as both sides); the
            upper-triangle index guard and the Phase-1 stats reuse both rely on it.

    Returns:
        List of dicts with:
        - column1, column2: column names
        - join_confidence: value overlap score (Jaccard/containment)
        - cardinality: one-to-one, one-to-many, etc.
        - statistical_confidence: confidence in the score (0-1)
        - algorithm: which algorithm was used (exact, minhash)
    """
    # Phase 1: Pre-compute column statistics (with type info for filtering)
    stats1 = _precompute_column_stats(conn, table1_path, columns1, column_types1)
    # A self-probe (same_table) passes the identical table/columns as both sides —
    # reuse the stats instead of recomputing the same distinct-count queries.
    stats2 = (
        stats1
        if same_table
        else _precompute_column_stats(conn, table2_path, columns2, column_types2)
    )

    # Phase 2: Filter column pairs
    pairs_to_check = []
    for i, col1 in enumerate(columns1):
        if col1 not in stats1:
            continue
        for j, col2 in enumerate(columns2):
            if col2 not in stats2:
                continue
            # Self-referential probe: only the upper triangle. i == j is a column
            # matched to itself (trivial identity); i > j is the reverse of a pair
            # already queued (direction is normalized at persist, DAT-758).
            if same_table and i >= j:
                continue
            if _should_compare_columns(stats1[col1], stats2[col2]):
                pairs_to_check.append((col1, col2))

    logger.debug(
        f"Filtered {len(columns1) * len(columns2)} pairs to {len(pairs_to_check)} "
        f"for {table1_path} <-> {table2_path}"
    )

    if not pairs_to_check:
        return []

    # Phase 3: Compute Jaccard scores in parallel using adaptive algorithms
    candidates = []

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [
            pool.submit(
                _compute_join_score_adaptive,
                conn,
                table1_path,
                table2_path,
                col1,
                col2,
                stats1[col1],
                stats2[col2],
            )
            for col1, col2 in pairs_to_check
        ]

        for future in futures:
            result = future.result()
            # Filter by both score and confidence
            if result.score >= min_score and result.confidence >= min_confidence:
                candidates.append(
                    {
                        "column1": result.column1,
                        "column2": result.column2,
                        "join_confidence": result.score,
                        "cardinality": result.cardinality,
                        "statistical_confidence": result.confidence,
                        "algorithm": result.algorithm.value,
                    }
                )

    # Sort by score descending
    def _sort_key(x: dict[str, Any]) -> float:
        return float(x["join_confidence"])

    candidates.sort(key=_sort_key, reverse=True)
    return candidates
