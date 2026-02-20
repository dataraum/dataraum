"""Parquet file loader - strongly typed source.

Parquet files have enforced types from their schema. DuckDB reads them natively,
so loading is a simple CREATE TABLE AS SELECT. Type inference can be simplified
since the source already provides reliable type information.
"""

import time
from pathlib import Path
from uuid import uuid4

import duckdb
import pyarrow.parquet as pq
from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.core.models import Result, SourceConfig
from dataraum.sources.base import ColumnInfo, LoaderBase, TypeSystemStrength, normalize_column_name
from dataraum.sources.csv.models import StagedTable, StagingResult
from dataraum.storage import Column, Source, Table

logger = get_logger(__name__)

# Map PyArrow types to DuckDB SQL type names
_ARROW_TO_DUCKDB: dict[str, str] = {
    "int8": "TINYINT",
    "int16": "SMALLINT",
    "int32": "INTEGER",
    "int64": "BIGINT",
    "uint8": "UTINYINT",
    "uint16": "USMALLINT",
    "uint32": "UINTEGER",
    "uint64": "UBIGINT",
    "float16": "FLOAT",
    "float": "FLOAT",
    "float32": "FLOAT",
    "double": "DOUBLE",
    "float64": "DOUBLE",
    "bool": "BOOLEAN",
    "string": "VARCHAR",
    "large_string": "VARCHAR",
    "utf8": "VARCHAR",
    "large_utf8": "VARCHAR",
    "binary": "BLOB",
    "large_binary": "BLOB",
    "date32": "DATE",
    "date32[day]": "DATE",
    "date64": "DATE",
    "timestamp[ns]": "TIMESTAMP",
    "timestamp[us]": "TIMESTAMP",
    "timestamp[ms]": "TIMESTAMP",
    "timestamp[s]": "TIMESTAMP",
    "time32[ms]": "TIME",
    "time64[us]": "TIME",
    "time64[ns]": "TIME",
}


def _arrow_type_to_duckdb(arrow_type_str: str) -> str:
    """Map a PyArrow type string to a DuckDB type name.

    Falls back to VARCHAR for unknown types.
    """
    # Direct match
    if arrow_type_str in _ARROW_TO_DUCKDB:
        return _ARROW_TO_DUCKDB[arrow_type_str]

    # Prefix matches for parameterized types (e.g., "timestamp[ns, tz=UTC]")
    lower = arrow_type_str.lower()
    if lower.startswith("timestamp"):
        return "TIMESTAMP"
    if lower.startswith("date"):
        return "DATE"
    if lower.startswith("time"):
        return "TIME"
    if lower.startswith("decimal"):
        return "DECIMAL"
    if lower.startswith("list") or lower.startswith("large_list"):
        return "VARCHAR"
    if lower.startswith("struct"):
        return "VARCHAR"
    if lower.startswith("dictionary"):
        return "VARCHAR"

    return "VARCHAR"


class ParquetLoader(LoaderBase):
    """Loader for Parquet files.

    Parquet files are strongly typed - column types are enforced by the format.
    DuckDB reads Parquet natively, making loading very efficient.
    """

    @property
    def type_system_strength(self) -> TypeSystemStrength:
        """Parquet files are strongly typed."""
        return TypeSystemStrength.STRONG

    def get_schema(
        self,
        source_config: SourceConfig,
    ) -> Result[list[ColumnInfo]]:
        """Get Parquet column names and types from file metadata.

        Args:
            source_config: Source configuration with path to Parquet file

        Returns:
            Result containing list of ColumnInfo with source types
        """
        if not source_config.path:
            return Result.fail("Parquet source requires 'path' in configuration")

        path = Path(source_config.path)
        if not path.exists():
            return Result.fail(f"Parquet file not found: {path}")

        try:
            schema = pq.read_schema(path)
            columns = []

            for idx, field in enumerate(schema):
                arrow_type_str = str(field.type)
                duckdb_type = _arrow_type_to_duckdb(arrow_type_str)

                columns.append(
                    ColumnInfo(
                        name=field.name,
                        position=idx,
                        source_type=duckdb_type,
                        nullable=field.nullable,
                    )
                )

            return Result.ok(columns)

        except Exception as e:
            return Result.fail(f"Failed to read Parquet schema: {e}")

    def load(
        self,
        source_config: SourceConfig,
        duckdb_conn: duckdb.DuckDBPyConnection,
        session: Session,
    ) -> Result[StagingResult]:
        """Load Parquet file into DuckDB preserving native types.

        Args:
            source_config: Source configuration
            duckdb_conn: DuckDB connection
            session: SQLAlchemy session for metadata

        Returns:
            Result containing StagingResult
        """
        if not source_config.path:
            return Result.fail("Parquet source requires 'path' in configuration")

        path = Path(source_config.path)
        if not path.exists():
            return Result.fail(f"Parquet file not found: {path}")

        start_time = time.time()

        try:
            # Create Source record
            source_id = str(uuid4())
            source = Source(
                source_id=source_id,
                name=source_config.name,
                source_type="parquet",
                connection_config={"path": str(path)},
            )
            session.add(source)

            # Load the file
            file_result = self._load_single_file(
                file_path=path,
                source_id=source_id,
                duckdb_conn=duckdb_conn,
                session=session,
            )

            if not file_result.success:
                logger.warning("parquet_load_failed", file=str(path), error=file_result.error)
                return Result.fail(file_result.error or "Failed to load Parquet")

            staged_table = file_result.unwrap()
            session.commit()

            duration = time.time() - start_time
            logger.info(
                "parquet_loaded",
                file=str(path),
                table=staged_table.table_name,
                rows=staged_table.row_count,
                columns=staged_table.column_count,
                duration_s=round(duration, 2),
            )

            return Result.ok(
                StagingResult(
                    source_id=source_id,
                    tables=[staged_table],
                    total_rows=staged_table.row_count,
                    duration_seconds=duration,
                )
            )

        except Exception as e:
            logger.error("parquet_load_error", file=str(path), error=str(e))
            return Result.fail(f"Failed to load Parquet: {e}")

    def load_directory(
        self,
        directory_path: str,
        source_name: str,
        duckdb_conn: duckdb.DuckDBPyConnection,
        session: Session,
        file_pattern: str = "*.parquet",
    ) -> Result[StagingResult]:
        """Load all Parquet files from a directory into DuckDB.

        Args:
            directory_path: Path to the directory containing Parquet files
            source_name: Name for the source
            duckdb_conn: DuckDB connection
            session: SQLAlchemy session for metadata
            file_pattern: Glob pattern for Parquet files

        Returns:
            Result containing StagingResult with multiple tables
        """
        directory = Path(directory_path)
        if not directory.exists():
            return Result.fail(f"Directory not found: {directory}")
        if not directory.is_dir():
            return Result.fail(f"Path is not a directory: {directory}")

        # Find Parquet files (support both .parquet and .pq)
        parquet_files = sorted(directory.glob(file_pattern))
        if not parquet_files:
            return Result.fail(f"No Parquet files found matching '{file_pattern}' in {directory}")

        logger.info(
            "parquet_directory_loading",
            directory=str(directory),
            file_count=len(parquet_files),
            pattern=file_pattern,
        )

        start_time = time.time()
        warnings: list[str] = []

        try:
            # Create single Source for the directory
            source_id = str(uuid4())
            source = Source(
                source_id=source_id,
                name=source_name,
                source_type="parquet_directory",
                connection_config={
                    "directory": str(directory),
                    "file_pattern": file_pattern,
                    "file_count": len(parquet_files),
                },
            )
            session.add(source)

            staged_tables: list[StagedTable] = []
            total_rows = 0

            for pq_file in parquet_files:
                file_result = self._load_single_file(
                    file_path=pq_file,
                    source_id=source_id,
                    duckdb_conn=duckdb_conn,
                    session=session,
                )

                if not file_result.success:
                    logger.warning(
                        "parquet_file_skipped", file=pq_file.name, error=file_result.error
                    )
                    warnings.append(f"Failed to load {pq_file.name}: {file_result.error}")
                    continue

                staged_table = file_result.unwrap()
                staged_tables.append(staged_table)
                total_rows += staged_table.row_count

            if not staged_tables:
                return Result.fail("No Parquet files were successfully loaded")

            session.commit()

            duration = time.time() - start_time
            logger.info(
                "parquet_directory_loaded",
                directory=str(directory),
                tables=len(staged_tables),
                total_rows=total_rows,
                skipped=len(warnings),
                duration_s=round(duration, 2),
            )

            result = StagingResult(
                source_id=source_id,
                tables=staged_tables,
                total_rows=total_rows,
                duration_seconds=duration,
            )

            return Result.ok(result, warnings=warnings)

        except Exception as e:
            logger.error("parquet_directory_load_error", directory=str(directory), error=str(e))
            return Result.fail(f"Failed to load Parquet directory: {e}")

    def _load_single_file(
        self,
        file_path: Path,
        source_id: str,
        duckdb_conn: duckdb.DuckDBPyConnection,
        session: Session,
    ) -> Result[StagedTable]:
        """Load a single Parquet file into DuckDB.

        DuckDB reads Parquet natively, preserving column types.
        Column names are normalized for SQL safety.

        Args:
            file_path: Path to the Parquet file
            source_id: ID of the parent source
            duckdb_conn: DuckDB connection
            session: SQLAlchemy session

        Returns:
            Result containing StagedTable
        """
        try:
            # Read schema from Parquet metadata
            schema = pq.read_schema(file_path)

            # Normalize column names and detect collisions
            col_mapping: list[tuple[str, str, str]] = []  # (original, normalized, duckdb_type)
            seen: dict[str, int] = {}

            for idx, field in enumerate(schema):
                original = field.name
                normalized = normalize_column_name(original, idx)
                if normalized in seen:
                    seen[normalized] += 1
                    normalized = f"{normalized}_{seen[normalized]}"
                else:
                    seen[normalized] = 1

                duckdb_type = _arrow_type_to_duckdb(str(field.type))
                col_mapping.append((original, normalized, duckdb_type))

            # Sanitize table name
            table_name = self._sanitize_table_name(file_path.stem)
            raw_table_name = f"raw_{table_name}"

            # Build SELECT with aliasing for normalized names
            select_exprs = [
                f'"{original}" AS "{normalized}"' for original, normalized, _ in col_mapping
            ]

            # DuckDB reads Parquet natively — preserves types
            sql = f"""
                CREATE TABLE "{raw_table_name}" AS
                SELECT {", ".join(select_exprs)}
                FROM read_parquet('{file_path}')
            """
            duckdb_conn.execute(sql)

            # Get row count
            row_count_result = duckdb_conn.execute(
                f'SELECT COUNT(*) FROM "{raw_table_name}"'
            ).fetchone()
            row_count = row_count_result[0] if row_count_result else 0

            # Create Table record
            table_id = str(uuid4())
            table = Table(
                table_id=table_id,
                source_id=source_id,
                table_name=table_name,
                layer="raw",
                duckdb_path=raw_table_name,
                row_count=row_count,
            )
            session.add(table)

            # Create Column records with Parquet-native types
            for position, (original, normalized, duckdb_type) in enumerate(col_mapping):
                column_id = str(uuid4())
                column = Column(
                    column_id=column_id,
                    table_id=table_id,
                    column_name=normalized,
                    original_name=original,
                    column_position=position,
                    raw_type=duckdb_type,
                    resolved_type=None,
                )
                session.add(column)

            return Result.ok(
                StagedTable(
                    table_id=table_id,
                    table_name=table_name,
                    raw_table_name=raw_table_name,
                    row_count=row_count,
                    column_count=len(col_mapping),
                )
            )

        except Exception as e:
            return Result.fail(f"Failed to load {file_path.name}: {e}")
