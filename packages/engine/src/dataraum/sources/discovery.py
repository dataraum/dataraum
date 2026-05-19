"""Workspace file scanner for data source discovery.

Scans a directory for CSV, Parquet, JSON, and XLSX files with column previews.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

_log = logging.getLogger(__name__)

# File extensions recognized as data sources.
DATA_EXTENSIONS: set[str] = {".csv", ".parquet", ".json", ".jsonl", ".xlsx", ".xls", ".tsv"}

# Max files to report per format (avoid overwhelming output).
_MAX_FILES_PER_FORMAT = 50


@dataclass
class FilePreview:
    """Preview of a discovered data file."""

    path: str
    format: str
    size_bytes: int
    columns: list[str] = field(default_factory=list)
    row_count_estimate: int | None = None


@dataclass
class DiscoveryResult:
    """Result of workspace file discovery."""

    files: list[FilePreview] = field(default_factory=list)
    existing_sources: list[str] = field(default_factory=list)
    scan_root: str = ""


def discover_sources(
    root: Path,
    recursive: bool = True,
    existing_sources: list[str] | None = None,
) -> DiscoveryResult:
    """Scan workspace for data files and return previews.

    Args:
        root: Directory to scan.
        recursive: Whether to scan subdirectories.
        existing_sources: Names of already-registered sources to include in result.

    Returns:
        DiscoveryResult with file previews and existing source names.
    """
    if not root.is_dir():
        return DiscoveryResult(scan_root=str(root), existing_sources=existing_sources or [])

    pattern = "**/*" if recursive else "*"
    files: list[FilePreview] = []
    count = 0

    for path in sorted(root.glob(pattern)):
        if not path.is_file():
            continue
        if path.suffix.lower() not in DATA_EXTENSIONS:
            continue
        if count >= _MAX_FILES_PER_FORMAT * len(DATA_EXTENSIONS):
            break

        try:
            preview = _preview_file(path, root)
            if preview:
                files.append(preview)
                count += 1
        except Exception:
            _log.debug("Could not preview %s", path, exc_info=True)

    return DiscoveryResult(
        files=files,
        existing_sources=existing_sources or [],
        scan_root=str(root),
    )


def _preview_file(path: Path, root: Path) -> FilePreview | None:
    """Build a file preview with column names via DuckDB."""
    size = path.stat().st_size
    if size == 0:
        return None

    suffix = path.suffix.lower()
    fmt = _suffix_to_format(suffix)
    rel_path = str(path.relative_to(root)) if path.is_relative_to(root) else str(path)

    preview = FilePreview(
        path=rel_path,
        format=fmt,
        size_bytes=size,
    )

    # Try to get column names and row count via DuckDB
    try:
        conn = duckdb.connect()
        try:
            columns, row_count = _read_schema(conn, path, suffix)
            preview.columns = columns
            preview.row_count_estimate = row_count
        finally:
            conn.close()
    except Exception:
        _log.debug("Schema read failed for %s", path, exc_info=True)

    return preview


def _suffix_to_format(suffix: str) -> str:
    mapping = {
        ".csv": "csv",
        ".tsv": "csv",
        ".parquet": "parquet",
        ".json": "json",
        ".jsonl": "json",
        ".xlsx": "xlsx",
        ".xls": "xlsx",
    }
    return mapping.get(suffix, suffix.lstrip("."))


def _read_schema(
    conn: duckdb.DuckDBPyConnection,
    path: Path,
    suffix: str,
) -> tuple[list[str], int | None]:
    """Read column names and approximate row count for a file."""
    safe = str(path).replace("'", "''")

    if suffix in (".csv", ".tsv"):
        # Read just the header
        result = conn.execute(f"SELECT * FROM read_csv_auto('{safe}') LIMIT 0")
        columns = [desc[0] for desc in result.description]
        # Estimate row count from file size (rough)
        row_count = None
        try:
            count_result = conn.execute(f"SELECT count(*) FROM read_csv_auto('{safe}')").fetchone()
            row_count = count_result[0] if count_result else None
        except Exception:
            pass
        return columns, row_count

    elif suffix == ".parquet":
        result = conn.execute(f"SELECT * FROM read_parquet('{safe}') LIMIT 0")
        columns = [desc[0] for desc in result.description]
        # Parquet metadata has exact count
        try:
            count_result = conn.execute(
                f"SELECT count(*) FROM parquet_metadata('{safe}')"
            ).fetchone()
            row_count = count_result[0] if count_result else None
        except Exception:
            row_count = None
        return columns, row_count

    elif suffix in (".json", ".jsonl"):
        result = conn.execute(f"SELECT * FROM read_json_auto('{safe}') LIMIT 0")
        columns = [desc[0] for desc in result.description]
        return columns, None

    return [], None
