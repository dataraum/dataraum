"""JSON/JSONL file loader - untyped source with VARCHAR-first approach.

JSON files have no enforced types. Like CSV, we use a VARCHAR-first approach:
DuckDB's read_json_auto() infers structure, then we cast all columns to VARCHAR
to preserve raw values and let the typing phase handle inference.
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


class JsonLoader(LoaderBase):
    """Loader for JSON and JSONL files.

    JSON files are untyped sources — all data is loaded as VARCHAR to preserve
    raw values. DuckDB's read_json_auto() handles both JSON arrays and
    newline-delimited JSONL.
    """

    def get_schema(
        self,
        source_config: SourceConfig,
    ) -> Result[list[ColumnInfo]]:
        """Get JSON column names and sample values.

        Args:
            source_config: Source configuration with path to JSON file.

        Returns:
            Result containing list of ColumnInfo.
        """
        if not source_config.path:
            return Result.fail("JSON source requires 'path' in configuration")

        path = Path(source_config.path)
        if not path.exists():
            return Result.fail(f"JSON file not found: {path}")

        try:
            safe_path = str(path).replace("'", "''")
            conn = duckdb.connect(":memory:")
            try:
                sample_df = conn.execute(f"""
                    SELECT * FROM read_json_auto('{safe_path}')
                    LIMIT 10
                """).df()
            finally:
                conn.close()

            columns = []
            for idx, col_name in enumerate(sample_df.columns):
                sample_values = sample_df[col_name].astype(str).head(5).tolist()
                columns.append(
                    ColumnInfo(
                        name=col_name,
                        position=idx,
                        source_type="VARCHAR",
                        nullable=True,
                        sample_values=sample_values,
                    )
                )

            return Result.ok(columns)

        except Exception as e:
            return Result.fail(f"Failed to read JSON schema: {e}")

    def load(
        self,
        source_config: SourceConfig,
        duckdb_conn: duckdb.DuckDBPyConnection,
        session: Session,
    ) -> Result[StagingResult]:
        """Load JSON file into DuckDB as all VARCHAR columns.

        Args:
            source_config: Source configuration.
            duckdb_conn: DuckDB connection.
            session: SQLAlchemy session for metadata.

        Returns:
            Result containing StagingResult.
        """
        if not source_config.path:
            return Result.fail("JSON source requires 'path' in configuration")

        path = Path(source_config.path)
        if not path.exists():
            return Result.fail(f"JSON file not found: {path}")

        start_time = time.time()

        try:
            source_id = str(uuid4())
            source = Source(
                source_id=source_id,
                name=source_config.name,
                source_type="json",
                connection_config={"path": str(path)},
            )
            session.add(source)

            file_result = self._load_single_file(
                file_path=path,
                source_id=source_id,
                duckdb_conn=duckdb_conn,
                session=session,
            )

            if not file_result.success:
                logger.warning("json_load_failed", file=str(path), error=file_result.error)
                return Result.fail(file_result.error or "Failed to load JSON")

            staged_table = file_result.unwrap()
            session.commit()

            duration = time.time() - start_time
            logger.debug(
                "json_loaded",
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
            logger.error("json_load_error", file=str(path), error=str(e))
            return Result.fail(f"Failed to load JSON: {e}")

    def _load_single_file(
        self,
        file_path: Path,
        source_id: str,
        duckdb_conn: duckdb.DuckDBPyConnection,
        session: Session,
    ) -> Result[StagedTable]:
        """Load a single JSON/JSONL file into DuckDB as all VARCHAR.

        Args:
            file_path: Path to the JSON file.
            source_id: ID of the parent source.
            duckdb_conn: DuckDB connection.
            session: SQLAlchemy session.

        Returns:
            Result containing StagedTable.
        """
        try:
            # Escape single quotes in path for SQL safety
            safe_path = str(file_path).replace("'", "''")

            # Discover columns via read_json_auto
            schema = duckdb_conn.execute(
                f"DESCRIBE SELECT * FROM read_json_auto('{safe_path}')"
            ).fetchall()

            if not schema:
                return Result.fail("No columns found in JSON file")

            # Normalize column names and detect collisions
            col_mapping: list[tuple[str, str]] = []  # (original, normalized)
            seen: dict[str, int] = {}

            for idx, row in enumerate(schema):
                original = row[0]
                normalized = normalize_column_name(original, idx)
                if normalized in seen:
                    seen[normalized] += 1
                    normalized = f"{normalized}_{seen[normalized]}"
                else:
                    seen[normalized] = 1
                col_mapping.append((original, normalized))

            # Sanitize table name
            table_name = self._sanitize_table_name(file_path.stem)
            raw_table_name = f"raw_{table_name}"

            # Build SELECT: serialize every column to VARCHAR via to_json().
            # Plain CAST(col AS VARCHAR) fails on STRUCT/LIST types that
            # read_json_auto infers for nested objects/arrays.
            select_exprs = [
                f'CAST(to_json("{original}") AS VARCHAR) AS "{normalized}"'
                for original, normalized in col_mapping
            ]

            sql = f"""
                CREATE TABLE "{raw_table_name}" AS
                SELECT {", ".join(select_exprs)}
                FROM read_json_auto('{safe_path}')
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

            # Create Column records — all VARCHAR
            for position, (original, normalized) in enumerate(col_mapping):
                column_id = str(uuid4())
                column = Column(
                    column_id=column_id,
                    table_id=table_id,
                    column_name=normalized,
                    original_name=original,
                    column_position=position,
                    raw_type="VARCHAR",
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
