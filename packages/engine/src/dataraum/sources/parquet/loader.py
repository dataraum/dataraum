"""Parquet file loader - strongly typed source.

Parquet files have enforced types from their schema. DuckDB reads them natively,
so loading is a simple CREATE TABLE AS SELECT. Type inference can be simplified
since the source already provides reliable type information.
"""

from __future__ import annotations

from uuid import uuid4

import duckdb
from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.core.models import Result, SourceConfig
from dataraum.core.uri import uri_basename
from dataraum.sources.base import ColumnInfo, LoaderBase, normalize_column_name
from dataraum.sources.csv.models import StagedTable
from dataraum.storage import Column, Table

logger = get_logger(__name__)


def _describe_parquet(
    source_uri: str,
    conn: duckdb.DuckDBPyConnection,
) -> list[tuple[str, str, bool]]:
    """Read Parquet schema using DuckDB DESCRIBE.

    ``source_uri`` is an ``s3://<lake-bucket>/<key>`` URI passed verbatim to
    ``read_parquet`` over httpfs (DAT-389).

    Returns list of (column_name, duckdb_type, nullable).
    """
    safe_path = source_uri.replace("'", "''")
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

        # ``s3://<lake-bucket>/<key>`` source URI — DAT-389.
        source_uri = source_config.path

        try:
            # Throwaway connection for the schema sniff; register the
            # object-store secret on it so an ``s3://`` URI resolves over
            # httpfs (DAT-389; reuses the DAT-388 helper).
            from dataraum.server.storage import apply_s3_secret

            conn = duckdb.connect()
            try:
                # Defense in depth (DAT-389): disable the local filesystem on the
                # sniff connection (after httpfs loads) so a URI that slipped past
                # validation cannot read a local file.
                apply_s3_secret(conn, disable_local_fs=True)
                schema = _describe_parquet(source_uri, conn)
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

    def _load_single_file(
        self,
        source_uri: str,
        source_id: str,
        duckdb_conn: duckdb.DuckDBPyConnection,
        session: Session,
    ) -> Result[StagedTable]:
        """Load a single Parquet file into DuckDB.

        DuckDB reads Parquet natively, preserving column types.
        Column names are normalized for SQL safety.

        ``source_uri`` is an ``s3://<lake-bucket>/<key>`` URI handed verbatim to
        DuckDB (DAT-389). The schema DESCRIBE runs on the session ``duckdb_conn``,
        which already carries the object-store secret (DAT-388).

        Args:
            source_uri: URI of the Parquet file (passed straight to ``read_parquet``).
            source_id: ID of the parent source
            duckdb_conn: DuckDB connection
            session: SQLAlchemy session

        Returns:
            Result containing StagedTable
        """
        from dataraum.core.duckdb_naming import schema_for_layer
        from dataraum.server.storage import LAKE_CATALOG_ALIAS
        from dataraum.sources.base import raw_table_name_for_uri

        try:
            # Read schema using DuckDB DESCRIBE
            schema = _describe_parquet(source_uri, duckdb_conn)

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

            # Compose the narrow, workspace-unique name (DAT-639 — no source
            # prefix) via the single canonical derivation the import phase's
            # collision guard also uses. The catalog alias is resolved here so
            # the loader can write directly into ``lake.raw.*`` — avoids a
            # cross-schema move in import_phase.
            bare = raw_table_name_for_uri(source_uri)
            raw_target = f'{LAKE_CATALOG_ALIAS}.{schema_for_layer("raw")}."{bare}"'

            # Build SELECT with aliasing for normalized names
            select_exprs = [
                f'"{original}" AS "{normalized}"' for original, normalized, _ in col_mapping
            ]

            # DuckDB reads Parquet natively — preserves types
            safe_path = source_uri.replace("'", "''")
            sql = f"""
                CREATE OR REPLACE TABLE {raw_target} AS
                SELECT {", ".join(select_exprs)}
                FROM read_parquet('{safe_path}')
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
                    table_name=bare,
                    raw_table_name=bare,
                    row_count=row_count,
                    column_count=len(col_mapping),
                )
            )

        except Exception as e:
            return Result.fail(f"Failed to load {uri_basename(source_uri)}: {e}")
