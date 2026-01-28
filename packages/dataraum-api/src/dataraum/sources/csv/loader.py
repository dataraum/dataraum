"""CSV file loader - untyped source with VARCHAR-first approach."""

import time
from pathlib import Path
from uuid import uuid4

import duckdb
from sqlalchemy.orm import Session

from dataraum.core.models import Result, SourceConfig
from dataraum.sources.base import ColumnInfo, LoaderBase, TypeSystemStrength
from dataraum.sources.csv.models import StagedTable, StagingResult
from dataraum.sources.csv.null_values import NullValueConfig, load_null_value_config
from dataraum.storage import Column, Source, Table


class CSVLoader(LoaderBase):
    """Loader for CSV files.

    CSV files are untyped sources - all data is text. We use a VARCHAR-first
    approach to preserve raw values and prevent data loss during loading.
    """

    @property
    def type_system_strength(self) -> TypeSystemStrength:
        """CSV files are untyped."""
        return TypeSystemStrength.UNTYPED

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
                return Result.fail(file_result.error or "Failed to load CSV")

            staged_table = file_result.unwrap()
            session.commit()

            return Result.ok(
                StagingResult(
                    source_id=source_id,
                    tables=[staged_table],
                    total_rows=staged_table.row_count,
                    duration_seconds=time.time() - start_time,
                )
            )

        except Exception as e:
            return Result.fail(f"Failed to load CSV: {e}")

    def load_directory(
        self,
        directory_path: str,
        source_name: str,
        duckdb_conn: duckdb.DuckDBPyConnection,
        session: Session,
        file_pattern: str = "*.csv",
    ) -> Result[StagingResult]:
        """Load all CSV files from a directory into DuckDB.

        Creates a single Source with multiple Table records (one per CSV file).

        Args:
            directory_path: Path to the directory containing CSV files
            source_name: Name for the source (dataset name)
            duckdb_conn: DuckDB connection
            session: SQLAlchemy session for metadata
            file_pattern: Glob pattern for CSV files (default: "*.csv")

        Returns:
            Result containing StagingResult with multiple tables
        """
        directory = Path(directory_path)
        if not directory.exists():
            return Result.fail(f"Directory not found: {directory}")
        if not directory.is_dir():
            return Result.fail(f"Path is not a directory: {directory}")

        # Find all CSV files
        csv_files = sorted(directory.glob(file_pattern))
        if not csv_files:
            return Result.fail(f"No CSV files found matching '{file_pattern}' in {directory}")

        start_time = time.time()
        warnings: list[str] = []

        try:
            # Load null value configuration once
            null_config = load_null_value_config()

            # Create single Source for the directory
            source_id = str(uuid4())
            source = Source(
                source_id=source_id,
                name=source_name,
                source_type="csv_directory",
                connection_config={
                    "directory": str(directory),
                    "file_pattern": file_pattern,
                    "file_count": len(csv_files),
                },
            )
            session.add(source)

            # Load each CSV file
            staged_tables: list[StagedTable] = []
            total_rows = 0

            for csv_file in csv_files:
                file_result = self._load_single_file(
                    file_path=csv_file,
                    source_id=source_id,
                    duckdb_conn=duckdb_conn,
                    session=session,
                    null_config=null_config,
                )

                if not file_result.success:
                    warnings.append(f"Failed to load {csv_file.name}: {file_result.error}")
                    continue

                staged_table = file_result.unwrap()
                staged_tables.append(staged_table)
                total_rows += staged_table.row_count

            if not staged_tables:
                return Result.fail("No CSV files were successfully loaded")

            session.commit()

            duration = time.time() - start_time

            result = StagingResult(
                source_id=source_id,
                tables=staged_tables,
                total_rows=total_rows,
                duration_seconds=duration,
            )

            return Result.ok(result, warnings=warnings)

        except Exception as e:
            return Result.fail(f"Failed to load CSV directory: {e}")

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

            # Build column type specification (all VARCHAR)
            column_spec = {col.name: "VARCHAR" for col in columns}

            # Track which columns are junk for later filtering
            junk_set = set(junk_columns) if junk_columns else set()

            # Format null strings for DuckDB
            null_strings = null_config.get_null_strings(include_placeholders=True)
            null_str_param = ", ".join(f"'{s}'" for s in null_strings)

            # Create the raw table with all VARCHAR columns
            sql = f"""
                CREATE TABLE {raw_table_name} AS
                SELECT * FROM read_csv(
                    '{file_path}',
                    columns = {column_spec},
                    header = true,
                    nullstr = [{null_str_param}],
                    ignore_errors = false,
                    auto_detect = false
                )
            """
            duckdb_conn.execute(sql)

            # Drop junk columns from DuckDB table
            dropped_columns: list[str] = []
            for junk in junk_set:
                try:
                    duckdb_conn.execute(f'ALTER TABLE {raw_table_name} DROP COLUMN "{junk}"')
                    dropped_columns.append(junk)
                except Exception:
                    # Column doesn't exist - that's fine
                    pass

            # Get row count
            row_count_result = duckdb_conn.execute(
                f"SELECT COUNT(*) FROM {raw_table_name}"
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

            # Create Column records (excluding junk columns)
            # Adjust positions after filtering
            position = 0
            for col_info in columns:
                if col_info.name in junk_set:
                    continue  # Skip junk columns
                column_id = str(uuid4())
                column = Column(
                    column_id=column_id,
                    table_id=table_id,
                    column_name=col_info.name,
                    column_position=position,
                    raw_type="VARCHAR",
                    resolved_type=None,
                )
                session.add(column)
                position += 1

            # Calculate actual column count after filtering
            actual_column_count = len(columns) - len(dropped_columns)

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
            return Result.fail(f"Failed to load {file_path.name}: {e}")
