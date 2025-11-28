"""CSV file loader - untyped source with VARCHAR-first approach."""

import time
from pathlib import Path
from uuid import uuid4

import duckdb
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models import (
    Result,
    SourceConfig,
    StagedColumn,
    StagedTable,
    StagingResult,
)
from dataraum_context.staging.base import ColumnInfo, LoaderBase, TypeSystemStrength
from dataraum_context.staging.null_values import load_null_value_config
from dataraum_context.storage.models_v2 import Column, Source, Table


class CSVLoader(LoaderBase):
    """Loader for CSV files.

    CSV files are untyped sources - all data is text. We use a VARCHAR-first
    approach to preserve raw values and prevent data loss during loading.
    """

    @property
    def type_system_strength(self) -> TypeSystemStrength:
        """CSV files are untyped."""
        return TypeSystemStrength.UNTYPED

    async def get_schema(
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
            conn = duckdb.connect(":memory:")

            # Read first few rows to get schema
            sample_df = conn.execute(f"""
                SELECT * FROM read_csv_auto('{path}')
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
            return Result.fail(f"Failed to read CSV schema: {e}")

    async def load(
        self,
        source_config: SourceConfig,
        duckdb_conn: duckdb.DuckDBPyConnection,
        session: AsyncSession,
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
        warnings = []

        try:
            # Load null value configuration
            null_config = load_null_value_config()
            null_strings = null_config.get_null_strings(include_placeholders=True)

            # Get schema first
            schema_result = await self.get_schema(source_config)
            if not schema_result.success and schema_result.error:
                return Result.fail(schema_result.error)

            columns = schema_result.value

            if not columns:
                return Result.fail("Columns empty or zero length")

            # Sanitize table name
            table_name = self._sanitize_table_name(path.stem)
            raw_table_name = f"raw_{table_name}"

            # Build column type specification (all VARCHAR)
            column_spec = {col.name: "VARCHAR" for col in columns}

            # Build DuckDB read_csv parameters
            # Format null strings for DuckDB
            null_str_param = ", ".join(f"'{s}'" for s in null_strings)

            # Create the raw table with all VARCHAR columns
            sql = f"""
                CREATE TABLE {raw_table_name} AS
                SELECT * FROM read_csv(
                    '{path}',
                    columns = {column_spec},
                    header = true,
                    nullstr = [{null_str_param}],
                    ignore_errors = false,
                    auto_detect = false
                )
            """

            duckdb_conn.execute(sql)

            # Get row count
            row_count_rows = duckdb_conn.execute(
                f"SELECT COUNT(*) FROM {raw_table_name}"
            ).fetchone()
            row_count = row_count_rows[0] if row_count_rows else 0

            # Create Source record in metadata DB
            source_id = str(uuid4())
            source = Source(
                source_id=source_id,
                name=source_config.name,
                source_type="csv",
                connection_config={"path": str(path)},
            )
            session.add(source)

            # Create Table record
            table_id = str(uuid4())
            table = Table(
                table_id=table_id,
                source_id=source_id,
                table_name=table_name,
                layer="raw",  # Untyped sources start at raw layer
                duckdb_path=raw_table_name,
                row_count=row_count,
            )
            session.add(table)

            # Create Column records
            staged_columns = []
            for col_info in columns:
                column_id = str(uuid4())
                column = Column(
                    column_id=column_id,
                    table_id=table_id,
                    column_name=col_info.name,
                    column_position=col_info.position,
                    raw_type="VARCHAR",
                    resolved_type=None,  # Will be determined by profiling
                )
                session.add(column)

                staged_columns.append(
                    StagedColumn(
                        column_id=column_id,
                        name=col_info.name,
                        position=col_info.position,
                        sample_values=col_info.sample_values,
                    )
                )

            await session.commit()

            # Build result
            staged_table = StagedTable(
                table_id=table_id,
                table_name=table_name,
                raw_table_name=raw_table_name,
                row_count=row_count,
                columns=staged_columns,
            )

            duration = time.time() - start_time

            result = StagingResult(
                source_id=source_id,
                tables=[staged_table],
                total_rows=row_count,
                duration_seconds=duration,
            )

            return Result.ok(result, warnings=warnings)

        except Exception as e:
            return Result.fail(f"Failed to load CSV: {e}")
