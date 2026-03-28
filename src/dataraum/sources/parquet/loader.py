"""Parquet file loader - strongly typed source.

Parquet files have enforced types from their schema. DuckDB reads them natively,
so loading is a simple CREATE TABLE AS SELECT. Type inference can be simplified
since the source already provides reliable type information.
"""

import time
from pathlib import Path
from uuid import uuid4

import duckdb
from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.core.models import Result, SourceConfig
from dataraum.sources.base import ColumnInfo, LoaderBase, normalize_column_name
from dataraum.sources.csv.models import StagedTable, StagingResult
from dataraum.storage import Column, Source, Table

logger = get_logger(__name__)


def _describe_parquet(
    file_path: Path,
    conn: duckdb.DuckDBPyConnection,
) -> list[tuple[str, str, bool]]:
    """Read Parquet schema using DuckDB DESCRIBE.

    Returns list of (column_name, duckdb_type, nullable).
    """
    safe_path = str(file_path).replace("'", "''")
    rows = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{safe_path}')").fetchall()
    return [(row[0], row[1], row[2] == "YES") for row in rows]


class ParquetLoader(LoaderBase):
    """Loader for Parquet files.

    Parquet files are strongly typed - column types are enforced by the format.
    DuckDB reads Parquet natively, making loading very efficient.
    """

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
            conn = duckdb.connect()
            try:
                schema = _describe_parquet(path, conn)
            finally:
                conn.close()

            columns = [
                ColumnInfo(
                    name=name,
                    position=idx,
                    source_type=dtype,
                    nullable=nullable,
                )
                for idx, (name, dtype, nullable) in enumerate(schema)
            ]

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
            logger.debug(
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
            # Read schema using DuckDB DESCRIBE
            schema = _describe_parquet(file_path, duckdb_conn)

            # Normalize column names and detect collisions
            col_mapping: list[tuple[str, str, str]] = []  # (original, normalized, duckdb_type)
            seen: dict[str, int] = {}

            for idx, (original, duckdb_type, _nullable) in enumerate(schema):
                normalized = normalize_column_name(original, idx)
                if normalized in seen:
                    seen[normalized] += 1
                    normalized = f"{normalized}_{seen[normalized]}"
                else:
                    seen[normalized] = 1

                col_mapping.append((original, normalized, duckdb_type))

            # Sanitize table name
            table_name = self._sanitize_table_name(file_path.stem)
            raw_table_name = f"raw_{table_name}"

            # Build SELECT with aliasing for normalized names
            select_exprs = [
                f'"{original}" AS "{normalized}"' for original, normalized, _ in col_mapping
            ]

            # DuckDB reads Parquet natively — preserves types
            safe_path = str(file_path).replace("'", "''")
            sql = f"""
                CREATE TABLE "{raw_table_name}" AS
                SELECT {", ".join(select_exprs)}
                FROM read_parquet('{safe_path}')
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
