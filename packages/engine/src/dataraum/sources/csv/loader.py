"""CSV file loader - untyped source with VARCHAR-first approach."""

from __future__ import annotations

from uuid import uuid4

import duckdb
from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.core.models import Result, SourceConfig
from dataraum.core.uri import uri_basename, uri_stem
from dataraum.sources.base import ColumnInfo, LoaderBase, normalize_column_name
from dataraum.sources.csv.models import StagedTable
from dataraum.sources.csv.null_values import NullValueConfig
from dataraum.storage import Column, Table

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

        # ``path`` is an ``s3://<lake-bucket>/<key>`` URI; handed verbatim to
        # ``read_csv_auto`` over httpfs (DAT-389), never to pathlib.
        safe_path = source_config.path.replace("'", "''")

        try:
            # Throwaway in-memory connection: schema sniffing must NOT touch
            # the workspace lake. The shared session manager's connection is
            # ``USE``d on ``lake.typed`` (post-DAT-341); a sniff CREATE TABLE
            # there would pollute the workspace-stable typed schema with stub
            # tables. Keep this ephemeral and unrelated to the lake — but
            # register the object-store secret on it so an ``s3://`` source URI
            # resolves (DAT-389; reuses the DAT-388 helper).
            from dataraum.server.storage import apply_s3_secret

            conn = duckdb.connect(":memory:")
            try:
                # Defense in depth (DAT-389): disable the local filesystem on the
                # sniff connection (after httpfs loads) so a URI that slipped past
                # validation cannot read a local file.
                apply_s3_secret(conn, disable_local_fs=True)
                # Read first few rows to get schema
                sample_df = conn.execute(f"""
                    SELECT * FROM read_csv_auto('{safe_path}')
                    LIMIT 10
                """).df()
            finally:
                conn.close()

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

            return Result.ok(columns)

        except Exception as e:
            return Result.fail(f"Failed to read CSV schema: {_check_encoding_error(str(e))}")

    def _load_single_file(
        self,
        source_uri: str,
        source_id: str,
        source_name: str,
        duckdb_conn: duckdb.DuckDBPyConnection,
        session: Session,
        null_config: NullValueConfig,
        junk_columns: list[str] | None = None,
    ) -> Result[StagedTable]:
        """Load a single CSV file into an existing source.

        ``source_uri`` is an ``s3://<lake-bucket>/<key>`` URI handed verbatim to
        DuckDB (DAT-389). The session ``duckdb_conn`` already carries the
        object-store secret (DAT-388); the schema sniff in ``get_schema`` applies
        the secret to its own throwaway connection.

        Args:
            source_uri: URI of the CSV file (passed straight to ``read_csv``).
            source_id: ID of the parent source
            source_name: Logical name of the parent source (used to compose
                the source-prefixed table identifier in ``lake.raw``).
            duckdb_conn: DuckDB connection
            session: SQLAlchemy session
            null_config: Null value configuration
            junk_columns: Column names to drop after loading (e.g., pandas index columns)

        Returns:
            Result containing StagedTable
        """
        from dataraum.core.duckdb_naming import schema_for_layer, table_name_for_source
        from dataraum.server.storage import LAKE_CATALOG_ALIAS

        file_stem = uri_stem(source_uri)
        try:
            # Get schema
            temp_config = SourceConfig(
                name=file_stem,
                source_type="csv",
                path=source_uri,
            )
            schema_result = self.get_schema(temp_config)
            if not schema_result.success:
                return Result.fail(schema_result.error or "Failed to get schema")

            columns = schema_result.value
            if not columns:
                return Result.fail("No columns found in CSV")

            # Compose the source-prefixed name. The catalog alias is resolved
            # here so the loader can write directly into ``lake.raw.*``.
            file_table_name = self._sanitize_table_name(file_stem)
            bare = table_name_for_source(source_name, file_table_name)
            raw_target = f'{LAKE_CATALOG_ALIAS}.{schema_for_layer("raw")}."{bare}"'

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
            safe_path = source_uri.replace("'", "''")

            # Create the raw table with normalized column names
            sql = f"""
                CREATE OR REPLACE TABLE {raw_target} AS
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
            row_count_result = duckdb_conn.execute(f"SELECT COUNT(*) FROM {raw_target}").fetchone()
            row_count = row_count_result[0] if row_count_result else 0

            # Create Table record
            table_id = str(uuid4())
            table = Table(
                table_id=table_id,
                source_id=source_id,
                table_name=bare,
                layer="raw",
                duckdb_path=bare,
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
                    table_name=bare,
                    raw_table_name=bare,
                    row_count=row_count,
                    column_count=actual_column_count,
                )
            )

        except Exception as e:
            return Result.fail(
                f"Failed to load {uri_basename(source_uri)}: {_check_encoding_error(str(e))}"
            )
