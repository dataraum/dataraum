"""Type inference through pattern matching."""

from collections import defaultdict
from datetime import UTC, datetime
from uuid import uuid4

import duckdb
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.config import Settings, get_settings
from dataraum_context.core.models.base import (
    ColumnRef,
    DataType,
    Result,
)
from dataraum_context.profiling.models import TypeCandidate as TypeCandidateModel
from dataraum_context.profiling.patterns import PatternConfig, load_pattern_config
from dataraum_context.profiling.units import detect_unit
from dataraum_context.storage.models_v2 import Column, Table
from dataraum_context.storage.models_v2 import TypeCandidate as DBTypeCandidate


async def infer_type_candidates(
    table: Table,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
) -> Result[list[TypeCandidateModel]]:
    """Infer type candidates for all VARCHAR columns in a table.

    This function:
    1. Samples values from each VARCHAR column
    2. Applies pattern matching to detect types
    3. Attempts to cast values to candidate types
    4. Scores candidates by parse success rate
    5. Detects units for numeric types

    Args:
        table: Table to analyze
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session

    Returns:
        Result containing list of TypeCandidate objects
    """
    settings = get_settings()
    pattern_config = load_pattern_config()

    try:
        # Get all VARCHAR columns
        columns = await session.run_sync(
            lambda sync_session: sync_session.query(Column)
            .filter(Column.table_id == table.table_id)
            .filter(Column.raw_type == "VARCHAR")
            .all()
        )

        if not columns:
            return Result.ok([])  # No VARCHAR columns to infer

        all_candidates = []

        for column in columns:
            # Infer types for this column
            candidates_result = await _infer_column_types(
                table=table,
                column=column,
                duckdb_conn=duckdb_conn,
                pattern_config=pattern_config,
                settings=settings,
            )

            if not candidates_result.success:
                continue

            candidates = candidates_result.value
            if not candidates:
                continue

            # Store in database
            for candidate in candidates:
                db_candidate = DBTypeCandidate(
                    candidate_id=str(uuid4()),
                    column_id=column.column_id,
                    detected_at=datetime.now(UTC),
                    data_type=candidate.data_type.value,
                    confidence=candidate.confidence,
                    parse_success_rate=candidate.parse_success_rate,
                    failed_examples=candidate.failed_examples,
                    detected_pattern=candidate.detected_pattern,
                    pattern_match_rate=candidate.pattern_match_rate,
                    detected_unit=candidate.detected_unit,
                    unit_confidence=candidate.unit_confidence,
                )
                session.add(db_candidate)

            all_candidates.extend(candidates)

        await session.commit()
        return Result.ok(all_candidates)

    except Exception as e:
        return Result.fail(f"Type inference failed: {e}")


async def _infer_column_types(
    table: Table,
    column: Column,
    duckdb_conn: duckdb.DuckDBPyConnection,
    pattern_config: PatternConfig,
    settings: Settings,
) -> Result[list[TypeCandidateModel]]:
    """Infer type candidates for a single column.

    Args:
        table: Parent table
        column: Column to analyze
        duckdb_conn: DuckDB connection
        pattern_config: Pattern configuration
        settings: Application settings

    Returns:
        Result containing list of TypeCandidate objects
    """
    try:
        table_name = table.duckdb_path
        col_name = column.column_name

        if not table_name or not col_name:
            return Result.fail("No table or column name found")

        # Sample values (exclude nulls)
        sample_size = settings.profile_sample_size or 100_000
        sample_query = f"""
            SELECT DISTINCT "{col_name}"
            FROM {table_name}
            WHERE "{col_name}" IS NOT NULL
            LIMIT {min(sample_size, 10000)}
        """

        sample_values = [row[0] for row in duckdb_conn.execute(sample_query).fetchall()]

        if not sample_values:
            return Result.fail("No values to analyze")

        # Pattern matching
        pattern_matches: dict[str, int] = defaultdict(int)
        pattern_by_name = {}

        for value in sample_values:
            str_value = str(value)
            matches = pattern_config.match_value(str_value)
            for pattern in matches:
                pattern_matches[pattern.name] += 1
                pattern_by_name[pattern.name] = pattern

        # Check column name hints
        column_name_hints = pattern_config.match_column_name(col_name)

        # Boost pattern matches that align with column name hints
        for hint in column_name_hints:
            if hint.likely_type:
                # Find patterns that infer the same type as the column name suggests
                for pattern_name, pattern in pattern_by_name.items():
                    if pattern.inferred_type == hint.likely_type:
                        # Boost match count by 20% when column name confirms the pattern
                        pattern_matches[pattern_name] = int(pattern_matches[pattern_name] * 1.2)

        # Try Pint unit detection on the VARCHAR values
        # This detects units like "100 kg", "5.5 m", "$25.30" that patterns might miss
        pint_unit_result = detect_unit(sample_values)

        # Build type candidates
        candidates = []

        # Strategy 1: From pattern matches
        for pattern_name, match_count in pattern_matches.items():
            pattern = pattern_by_name[pattern_name]
            match_rate = match_count / len(sample_values)

            # Only consider patterns with reasonable match rate
            if match_rate < 0.5:
                continue

            # Test parsing with DuckDB
            parse_result = await _test_type_cast(
                table_name=table_name,
                col_name=col_name,
                target_type=pattern.inferred_type,
                duckdb_conn=duckdb_conn,
                standardization_expr=pattern.standardization_expr,
            )

            if parse_result.success_rate < 0.8:
                continue  # Too many parse failures

            # Detect units on VARCHAR values
            # Note: Units are detected BEFORE type conversion, on the original string values
            detected_unit = None
            unit_confidence = None

            if pattern.detected_unit:
                # Pattern already specifies unit (e.g., currency pattern like "$123.45")
                # The pattern matched the VARCHAR string, so we know it has this unit
                detected_unit = pattern.detected_unit
                unit_confidence = match_rate
            elif pint_unit_result and pattern.inferred_type in [
                DataType.DOUBLE,
                DataType.DECIMAL,
                DataType.INTEGER,
                DataType.BIGINT,
            ]:
                # Pattern indicates numeric type, and Pint detected a unit
                # This catches cases like "100 kg" that match the numeric pattern
                # but also have units that Pint can extract
                detected_unit = pint_unit_result.unit
                unit_confidence = pint_unit_result.confidence

            # Calculate overall confidence
            confidence = (match_rate + parse_result.success_rate) / 2.0

            candidate = TypeCandidateModel(
                column_id=column.column_id,
                column_ref=ColumnRef(
                    table_name=table.table_name,
                    column_name=column.column_name,
                ),
                data_type=pattern.inferred_type,
                confidence=confidence,
                parse_success_rate=parse_result.success_rate,
                failed_examples=parse_result.failed_examples,
                detected_pattern=pattern_name,
                pattern_match_rate=match_rate,
                detected_unit=detected_unit,
                unit_confidence=unit_confidence,
            )
            candidates.append(candidate)

        # Strategy 2: Column name hints when no value patterns matched
        # This handles cases where the column name strongly suggests a type
        # but our value patterns didn't detect it (e.g., unconventional formatting)
        if not candidates and column_name_hints:
            for hint in column_name_hints:
                if hint.likely_type:
                    # Test if values can actually be cast to the suggested type
                    parse_result = await _test_type_cast(
                        table_name=table_name,
                        col_name=col_name,
                        target_type=hint.likely_type,
                        duckdb_conn=duckdb_conn,
                    )

                    # Only add as candidate if parse success rate is reasonable
                    if parse_result.success_rate >= 0.8:
                        candidate = TypeCandidateModel(
                            column_id=column.column_id,
                            column_ref=ColumnRef(
                                table_name=table.table_name,
                                column_name=column.column_name,
                            ),
                            data_type=hint.likely_type,
                            confidence=parse_result.success_rate
                            * 0.9,  # Slightly lower than pattern-based
                            parse_success_rate=parse_result.success_rate,
                            failed_examples=parse_result.failed_examples,
                            detected_pattern=hint.pattern,
                            pattern_match_rate=1.0,  # Column name matched 100%
                        )
                        candidates.append(candidate)

        # Strategy 3: If Pint detected units but no patterns matched
        # This handles cases like "100 kg" where the unit detection worked
        # but our regex patterns didn't catch it
        if not candidates and pint_unit_result:
            # Infer numeric type since Pint successfully parsed it
            candidate = TypeCandidateModel(
                column_id=column.column_id,
                column_ref=ColumnRef(
                    table_name=table.table_name,
                    column_name=column.column_name,
                ),
                data_type=DataType.DOUBLE,  # Default to DOUBLE for unit values
                confidence=pint_unit_result.confidence,
                parse_success_rate=pint_unit_result.confidence,
                failed_examples=[],
                detected_pattern="pint_detected",
                pattern_match_rate=pint_unit_result.confidence,
                detected_unit=pint_unit_result.unit,
                unit_confidence=pint_unit_result.confidence,
            )
            candidates.append(candidate)

        # Strategy 4: Fallback to VARCHAR if nothing else worked
        if not candidates:
            candidate = TypeCandidateModel(
                column_id=column.column_id,
                column_ref=ColumnRef(
                    table_name=table.table_name,
                    column_name=column.column_name,
                ),
                data_type=DataType.VARCHAR,
                confidence=1.0,
                parse_success_rate=1.0,
                failed_examples=[],
            )
            candidates.append(candidate)

        # Sort by confidence descending
        candidates.sort(key=lambda c: c.confidence, reverse=True)

        return Result.ok(candidates)

    except Exception as e:
        return Result.fail(f"Failed to infer types for column {column.column_name}: {e}")


class ParseResult:
    """Result of type cast testing."""

    def __init__(self, success_rate: float, failed_examples: list[str]):
        self.success_rate = success_rate
        self.failed_examples = failed_examples


async def _test_type_cast(
    table_name: str,
    col_name: str,
    target_type: DataType,
    duckdb_conn: duckdb.DuckDBPyConnection,
    standardization_expr: str | None = None,
) -> ParseResult:
    """Test casting a column to a target type.

    Args:
        table_name: DuckDB table name
        col_name: Column name
        target_type: Target data type
        duckdb_conn: DuckDB connection
        standardization_expr: Optional DuckDB SQL to normalize value before casting
                             (e.g., STRPTIME for date formats)
    Returns:
        ParseResult with success rate and failed examples
    """
    try:
        # Count total non-null values
        total_query = f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE "{col_name}" IS NOT NULL
        """
        total_count_rows = duckdb_conn.execute(total_query).fetchone()
        total_count = total_count_rows[0] if total_count_rows else 0

        if total_count == 0:
            return ParseResult(success_rate=0.0, failed_examples=[])

        # Build cast expression - use standardization_expr if provided
        if standardization_expr:
            cast_expression = standardization_expr.format(col=col_name)
            cast_expr = f"TRY_CAST({cast_expression} AS {target_type.value})"
        else:
            cast_expr = f'TRY_CAST("{col_name}" AS {target_type.value})'

        # Count successful casts
        success_query = f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE {cast_expr} IS NOT NULL
            AND "{col_name}" IS NOT NULL
        """
        success_count_rows = duckdb_conn.execute(success_query).fetchone()
        success_count = success_count_rows[0] if success_count_rows else 0

        success_rate = success_count / total_count if total_count > 0 else 0

        # Get examples of failed casts
        failed_examples = []
        if success_rate < 1.0:
            failed_query = f"""
                SELECT "{col_name}"
                FROM {table_name}
                WHERE "{col_name}" IS NOT NULL
                AND {cast_expr} IS NULL
                LIMIT 5
            """
            failed_examples = [row[0] for row in duckdb_conn.execute(failed_query).fetchall()]

        return ParseResult(
            success_rate=success_rate,
            failed_examples=failed_examples,
        )

    except Exception:
        return ParseResult(success_rate=0.0, failed_examples=[])
