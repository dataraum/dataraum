"""JSON/JSONL file loader - untyped source with VARCHAR-first approach.

JSON files have no enforced types. Like CSV, we use a VARCHAR-first approach:
DuckDB's read_json_auto() infers structure, then we cast all columns to VARCHAR
to preserve raw values and let the typing phase handle inference.
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

        # ``s3://<lake-bucket>/<key>`` source URI — DAT-389.
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
                sample_df = conn.execute(f"""
                    SELECT * FROM read_json_auto('{safe_path}')
                    LIMIT 10
                """).pl()  # polars — no pandas (DAT-580)
            finally:
                conn.close()

            columns = []
            for idx, col_name in enumerate(sample_df.columns):
                sample_values = [str(v) for v in sample_df[col_name].head(5).to_list()]
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

    def _load_single_file(
        self,
        source_uri: str,
        source_id: str,
        duckdb_conn: duckdb.DuckDBPyConnection,
        session: Session,
    ) -> Result[StagedTable]:
        """Load a single JSON/JSONL file into DuckDB as all VARCHAR.

        ``source_uri`` is an ``s3://<lake-bucket>/<key>`` URI handed verbatim to
        DuckDB (DAT-389). The schema DESCRIBE runs on the session ``duckdb_conn``,
        which already carries the object-store secret (DAT-388).

        Args:
            source_uri: URI of the JSON file (passed straight to ``read_json_auto``).
            source_id: ID of the parent source.
            duckdb_conn: DuckDB connection.
            session: SQLAlchemy session.

        Returns:
            Result containing StagedTable.
        """
        from dataraum.core.duckdb_naming import schema_for_layer
        from dataraum.server.storage import LAKE_CATALOG_ALIAS
        from dataraum.sources.base import raw_table_name_for_uri

        try:
            # Escape single quotes in the URI for SQL safety
            safe_path = source_uri.replace("'", "''")

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

            # Compose the narrow, workspace-unique name (DAT-639 — no source
            # prefix) via the single canonical derivation the import phase's
            # collision guard also uses. The catalog alias is resolved here so
            # the loader can write directly into ``lake.raw.*``.
            bare = raw_table_name_for_uri(source_uri)
            raw_target = f'{LAKE_CATALOG_ALIAS}.{schema_for_layer("raw")}."{bare}"'

            # Build SELECT: serialize every column to VARCHAR via to_json().
            # Plain CAST(col AS VARCHAR) fails on STRUCT/LIST types that
            # read_json_auto infers for nested objects/arrays.
            select_exprs = [
                f'CAST(to_json("{original}") AS VARCHAR) AS "{normalized}"'
                for original, normalized in col_mapping
            ]

            sql = f"""
                CREATE OR REPLACE TABLE {raw_target} AS
                SELECT {", ".join(select_exprs)}
                FROM read_json_auto('{safe_path}')
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
                    table_name=bare,
                    raw_table_name=bare,
                    row_count=row_count,
                    column_count=len(col_mapping),
                )
            )

        except Exception as e:
            return Result.fail(f"Failed to load {uri_basename(source_uri)}: {e}")
