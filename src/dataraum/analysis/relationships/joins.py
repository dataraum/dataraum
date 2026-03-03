"""Join column detection using value overlap with adaptive algorithms.

Uses parallel processing and adaptive algorithm selection for efficient detection.

Performance optimizations:
1. Pre-compute column stats (distinct count, total count) once per column
2. Filter column pairs by cardinality compatibility before expensive intersection
3. Adaptive algorithm selection based on cardinality:
   - Small (<10K distinct): Exact computation
   - Medium (10K-1M distinct): Sampling with error bounds
   - Large (>1M distinct): MinHash signatures
4. Use parallel processing for intersection queries

References:
- Sampling-based Jaccard estimation: arXiv:2507.10019v3
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
SMALL_CARDINALITY_THRESHOLD = 10_000  # Use exact computation
LARGE_CARDINALITY_THRESHOLD = 1_000_000  # Use MinHash
DEFAULT_NUM_HASHES = 128  # MinHash signature size
MIN_SAMPLE_SIZE = 1000  # Minimum sample for sampling algorithm
DEFAULT_SAMPLE_RATE = 0.1  # 10% sample rate
MIN_CONFIDENCE_THRESHOLD = 0.5  # Minimum statistical confidence to accept

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
    SAMPLED = "sampled"
    MINHASH = "minhash"


@dataclass
class ColumnStats:
    """Pre-computed statistics for a column."""

    column_name: str
    distinct_count: int
    total_count: int
    is_unique: bool  # distinct_count == total_count
    resolved_type: str | None = None  # Column's resolved type (e.g., "VARCHAR", "BIGINT")


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

    # Skip if cardinality ratio is extreme
    ratio = max(stats1.distinct_count, stats2.distinct_count) / max(
        min(stats1.distinct_count, stats2.distinct_count), 1
    )
    if ratio > cardinality_ratio_threshold:
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
    """Select the best algorithm based on cardinality."""
    max_distinct = max(stats1.distinct_count, stats2.distinct_count)

    if max_distinct < SMALL_CARDINALITY_THRESHOLD:
        return JoinAlgorithm.EXACT
    elif max_distinct < LARGE_CARDINALITY_THRESHOLD:
        return JoinAlgorithm.SAMPLED
    else:
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

    Used for small datasets (<10K distinct values).
    Returns confidence=1.0 since this is exact.
    """
    cursor = conn.cursor()
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

        # Check containment (full inclusion of one set in another).
        # Only meaningful when the smaller set has enough distinct values —
        # low-cardinality columns (e.g., 2-3 values) trivially contain each other.
        min_distinct = min(count1, count2)
        if min_distinct > 10 and (intersection == count1 or intersection == count2):
            containment = 1.0
        else:
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
    finally:
        cursor.close()


def _compute_sampled_jaccard(
    conn: duckdb.DuckDBPyConnection,
    table1_path: str,
    table2_path: str,
    col1: str,
    col2: str,
    stats1: ColumnStats,
    stats2: ColumnStats,
    sample_rate: float = DEFAULT_SAMPLE_RATE,
    min_samples: int = MIN_SAMPLE_SIZE,
) -> JoinScoreResult:
    """Compute Jaccard using sampling with statistical error bounds.

    Uses uniform random sampling from distinct values.
    Based on arXiv:2507.10019v3 - unbiased estimator with O(1/sqrt(x)) error.

    Args:
        sample_rate: Fraction of distinct values to sample
        min_samples: Minimum number of samples to take

    Returns:
        JoinScoreResult with estimated Jaccard and statistical confidence
    """
    cursor = conn.cursor()
    try:
        n1, n2 = stats1.distinct_count, stats2.distinct_count

        # Calculate sample sizes - ensure minimum samples for statistical validity
        m1 = max(min_samples, int(n1 * sample_rate))
        m2 = max(min_samples, int(n2 * sample_rate))

        # Cap at actual distinct counts
        m1 = min(m1, n1)
        m2 = min(m2, n2)

        # Use RESERVOIR sampling for uniform random sampling
        # RESERVOIR is the only method that supports fixed row counts and
        # provides truly uniform random samples (DuckDB docs)
        # Note: We sample distinct values directly using a subquery
        # For temporal types, cast to TIMESTAMP for cross-type comparison
        col1_expr = _get_cast_expression(col1, stats1.resolved_type)
        col2_expr = _get_cast_expression(col2, stats2.resolved_type)

        result = cursor.execute(f"""
            WITH
            sampled1 AS (
                SELECT v FROM (
                    SELECT DISTINCT {col1_expr} AS v
                    FROM {table1_path}
                    WHERE "{col1}" IS NOT NULL
                ) USING SAMPLE reservoir({m1} ROWS)
            ),
            sampled2 AS (
                SELECT v FROM (
                    SELECT DISTINCT {col2_expr} AS v
                    FROM {table2_path}
                    WHERE "{col2}" IS NOT NULL
                ) USING SAMPLE reservoir({m2} ROWS)
            ),
            actual_counts AS (
                SELECT
                    (SELECT COUNT(*) FROM sampled1) AS m1_actual,
                    (SELECT COUNT(*) FROM sampled2) AS m2_actual,
                    (SELECT COUNT(*) FROM sampled1 WHERE v IN (SELECT v FROM sampled2)) AS x
            )
            SELECT m1_actual, m2_actual, x FROM actual_counts
        """).fetchone()

        if result is None:
            return JoinScoreResult(col1, col2, 0.0, "unknown", 0.0, JoinAlgorithm.SAMPLED)

        m1_actual, m2_actual, x = result

        if m1_actual == 0 or m2_actual == 0:
            return JoinScoreResult(col1, col2, 0.0, "unknown", 0.0, JoinAlgorithm.SAMPLED)

        # Unbiased intersection estimate: I_hat = x * N1 * N2 / (M1 * M2)
        # From arXiv:2507.10019v3
        intersection_estimate = x * n1 * n2 / (m1_actual * m2_actual)

        # Jaccard estimate
        union_estimate = n1 + n2 - intersection_estimate
        jaccard_estimate = intersection_estimate / union_estimate if union_estimate > 0 else 0.0

        # Check for full containment (one set fully contained in another).
        # Only meaningful when the smaller set has enough distinct values —
        # low-cardinality columns trivially contain each other.
        min_distinct = min(n1, n2)
        if min_distinct > 10:
            containment1 = intersection_estimate / n1 if n1 > 0 else 0.0
            containment2 = intersection_estimate / n2 if n2 > 0 else 0.0
            containment = 1.0 if (containment1 >= 0.95 or containment2 >= 0.95) else 0.0
        else:
            containment = 0.0

        score = max(jaccard_estimate, containment)

        # Clamp to valid range (sampling can produce values slightly outside [0,1])
        score = max(0.0, min(1.0, score))

        # Calculate statistical confidence
        # Fractional standard error is O(1/sqrt(x))
        # Confidence = 1 - SE (rough approximation)
        if x > 0:
            fractional_se = 1.0 / math.sqrt(x)
            confidence = max(0.0, min(1.0, 1.0 - fractional_se))
        else:
            # No observed overlap - low confidence
            confidence = 0.1

        cardinality = _determine_cardinality(stats1, stats2)

        return JoinScoreResult(
            column1=col1,
            column2=col2,
            score=score,
            cardinality=cardinality,
            confidence=confidence,
            algorithm=JoinAlgorithm.SAMPLED,
        )

    except Exception as e:
        logger.debug(f"Sampled Jaccard failed for {col1}-{col2}: {e}")
        return JoinScoreResult(col1, col2, 0.0, "unknown", 0.0, JoinAlgorithm.SAMPLED)
    finally:
        cursor.close()


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
    cursor = conn.cursor()
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
    finally:
        cursor.close()


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
    - Small (<10K): Exact computation (confidence=1.0)
    - Medium (10K-1M): Sampling with error bounds
    - Large (>1M): MinHash signatures
    """
    algorithm = _select_algorithm(stats1, stats2)

    if algorithm == JoinAlgorithm.EXACT:
        return _compute_exact_jaccard(conn, table1_path, table2_path, col1, col2, stats1, stats2)
    elif algorithm == JoinAlgorithm.SAMPLED:
        return _compute_sampled_jaccard(conn, table1_path, table2_path, col1, col2, stats1, stats2)
    else:  # MINHASH
        return _compute_minhash_jaccard(conn, table1_path, table2_path, col1, col2, stats1, stats2)


def find_join_columns(
    conn: duckdb.DuckDBPyConnection,
    table1_path: str,
    table2_path: str,
    columns1: list[str],
    columns2: list[str],
    min_score: float = 0.3,
    min_confidence: float = MIN_CONFIDENCE_THRESHOLD,
    max_workers: int = 4,
    column_types1: dict[str, str | None] | None = None,
    column_types2: dict[str, str | None] | None = None,
) -> list[dict[str, Any]]:
    """Find join columns using adaptive algorithm selection.

    Uses a three-phase approach for efficiency:
    1. Pre-compute column statistics (distinct count, total count) once per column
    2. Filter column pairs by type compatibility and cardinality
    3. Compute Jaccard using the best algorithm for each pair's cardinality:
       - Small (<10K distinct): Exact computation
       - Medium (10K-1M): Sampling with error bounds
       - Large (>1M): MinHash signatures

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

    Returns:
        List of dicts with:
        - column1, column2: column names
        - join_confidence: value overlap score (Jaccard/containment)
        - cardinality: one-to-one, one-to-many, etc.
        - statistical_confidence: confidence in the score (0-1)
        - algorithm: which algorithm was used (exact, sampled, minhash)
    """
    # Phase 1: Pre-compute column statistics (with type info for filtering)
    stats1 = _precompute_column_stats(conn, table1_path, columns1, column_types1)
    stats2 = _precompute_column_stats(conn, table2_path, columns2, column_types2)

    # Phase 2: Filter column pairs
    pairs_to_check = []
    for col1 in columns1:
        if col1 not in stats1:
            continue
        for col2 in columns2:
            if col2 not in stats2:
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
