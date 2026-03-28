"""CSV file loader - untyped source with VARCHAR-first approach."""

from __future__ import annotations

import time
from pathlib import Path
from uuid import uuid4

import duckdb
from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.core.models import Result, SourceConfig
from dataraum.sources.base import ColumnInfo, LoaderBase, normalize_column_name
from dataraum.sources.csv.models import StagedTable, StagingResult
from dataraum.sources.csv.null_values import NullValueConfig, load_null_value_config
from dataraum.storage import Column, Source, Table

logger = get_logger(__name__)

_ENCODING_ERROR_MSG = (
    "File is not UTF-8 encoded (likely Excel export with Latin-1/CP1252). "
    "Re-save as UTF-8: in Excel use 'Save As → CSV UTF-8 (Comma delimited)'."
)


def _check_encoding_error(error: str) -> str:
    """Return a clear message if the error is a DuckDB encoding failure."""
    if "not utf-8 encoded" in error.lower() or "byte sequence mismatch" in error.lower():
        return _ENCODING_ERROR_MSG
    return error


class CSVLoader(LoaderBase):
    """Loader for CSV files.

    CSV files are untyped sources - all data is text. We use a VARCHAR-first
    approach to preserve raw values and prevent data loss during loading.
    """

    def get_schema(
        self,
        source_config: SourceConfig,
    ) -> Result[list[ColumnInfo]]:
        """Get CSV column names and sample values.

        Args:
            source_config: Source configuration with path to CSV

        Returns:
            Result containing list of ColumnInfo
        """
        if not source_config.path:
            return Result.fail("CSV source requires 'path' in configuration")

        path = Path(source_config.path)
        if not path.exists():
            return Result.fail(f"CSV file not found: {path}")

        try:
            # Use DuckDB to read CSV header and sample
            safe_path = str(path).replace("'", "''")
            conn = duckdb.connect(":memory:")

            # Read first few rows to get schema
            sample_df = conn.execute(f"""
                SELECT * FROM read_csv_auto('{safe_path}')
                LIMIT 10
            """).df()

            columns = []
            for idx, col_name in enumerate(sample_df.columns):
                # Get sample values (as strings)
                sample_values = sample_df[col_name].astype(str).head(5).tolist()

                columns.append(
                    ColumnInfo(
                        name=col_name,
                        position=idx,
                        source_type="VARCHAR",  # CSV is always text
                        nullable=True,
                        sample_values=sample_values,
                    )
                )

            conn.close()
            return Result.ok(columns)

        except Exception as e:
            return Result.fail(f"Failed to read CSV schema: {_check_encoding_error(str(e))}")

    def load(
        self,
        source_config: SourceConfig,
        duckdb_conn: duckdb.DuckDBPyConnection,
        session: Session,
    ) -> Result[StagingResult]:
        """Load CSV file into DuckDB as all VARCHAR columns.

        Args:
            source_config: Source configuration
            duckdb_conn: DuckDB connection
            session: SQLAlchemy session for metadata

        Returns:
            Result containing StagingResult
        """
        if not source_config.path:
            return Result.fail("CSV source requires 'path' in configuration")

        path = Path(source_config.path)
        if not path.exists():
            return Result.fail(f"CSV file not found: {path}")

        start_time = time.time()

        try:
            # Create Source record
            source_id = str(uuid4())
            source = Source(
                source_id=source_id,
                name=source_config.name,
                source_type="csv",
                connection_config={"path": str(path)},
            )
            session.add(source)

            # Load the file
            null_config = load_null_value_config()
            file_result = self._load_single_file(
                file_path=path,
                source_id=source_id,
                duckdb_conn=duckdb_conn,
                session=session,
                null_config=null_config,
            )

            if not file_result.success:
                logger.warning("csv_load_failed", file=str(path), error=file_result.error)
                return Result.fail(file_result.error or "Failed to load CSV")

            staged_table = file_result.unwrap()
            session.commit()

            duration = time.time() - start_time
            logger.debug(
                "csv_loaded",
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
            logger.error("csv_load_error", file=str(path), error=str(e))
            return Result.fail(f"Failed to load CSV: {e}")

    def _load_single_file(
        self,
        file_path: Path,
        source_id: str,
        duckdb_conn: duckdb.DuckDBPyConnection,
        session: Session,
        null_config: NullValueConfig,
        junk_columns: list[str] | None = None,
    ) -> Result[StagedTable]:
        """Load a single CSV file into an existing source.

        Internal helper used by both load() and load_directory().

        Args:
            file_path: Path to the CSV file
            source_id: ID of the parent source
            duckdb_conn: DuckDB connection
            session: SQLAlchemy session
            null_config: Null value configuration
            junk_columns: Column names to drop after loading (e.g., pandas index columns)

        Returns:
            Result containing StagedTable
        """
        try:
            # Get schema
            temp_config = SourceConfig(
                name=file_path.stem,
                source_type="csv",
                path=str(file_path),
            )
            schema_result = self.get_schema(temp_config)
            if not schema_result.success:
                return Result.fail(schema_result.error or "Failed to get schema")

            columns = schema_result.value
            if not columns:
                return Result.fail("No columns found in CSV")

            # Sanitize table name
            table_name = self._sanitize_table_name(file_path.stem)
            raw_table_name = f"raw_{table_name}"

            # Track which columns are junk for later filtering (match on original name)
            junk_set = set(junk_columns) if junk_columns else set()

            # Normalize column names and detect collisions
            seen: dict[str, int] = {}
            for col in columns:
                col.original_name = col.name
                normalized = normalize_column_name(col.name, col.position)
                if normalized in seen:
                    seen[normalized] += 1
                    normalized = f"{normalized}_{seen[normalized]}"
                else:
                    seen[normalized] = 1
                col.name = normalized

            # Filter out junk columns before SQL generation (match on original name)
            kept_columns = [col for col in columns if col.original_name not in junk_set]

            # Build column type specification for read_csv (uses original headers)
            column_spec = {col.original_name: "VARCHAR" for col in columns}

            # Format null strings for DuckDB
            null_strings = null_config.get_null_strings(include_placeholders=True)
            null_str_param = ", ".join(f"'{s}'" for s in null_strings)

            # Build SELECT with aliasing: "OriginalName" AS "normalized_name"
            select_exprs = [f'"{col.original_name}" AS "{col.name}"' for col in kept_columns]
            safe_path = str(file_path).replace("'", "''")

            # Create the raw table with normalized column names
            sql = f"""
                CREATE TABLE "{raw_table_name}" AS
                SELECT {", ".join(select_exprs)}
                FROM read_csv(
                    '{safe_path}',
                    columns = {column_spec},
                    header = true,
                    nullstr = [{null_str_param}],
                    ignore_errors = false,
                    auto_detect = false
                )
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

            # Create Column records for kept columns
            for position, col_info in enumerate(kept_columns):
                column_id = str(uuid4())
                column = Column(
                    column_id=column_id,
                    table_id=table_id,
                    column_name=col_info.name,
                    original_name=col_info.original_name,
                    column_position=position,
                    raw_type="VARCHAR",
                    resolved_type=None,
                )
                session.add(column)

            # Calculate actual column count after filtering
            actual_column_count = len(kept_columns)

            return Result.ok(
                StagedTable(
                    table_id=table_id,
                    table_name=table_name,
                    raw_table_name=raw_table_name,
                    row_count=row_count,
                    column_count=actual_column_count,
                )
            )

        except Exception as e:
            return Result.fail(f"Failed to load {file_path.name}: {_check_encoding_error(str(e))}")
