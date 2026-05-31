"""Source-URI helpers (DAT-389).

A file source's ``connection_config['path']`` is an ``s3://<bucket>/<key>`` URI
on the object store (the lake + uploaded files live there; SeaweedFS runs in
dev compose too). DuckDB's ``read_*_auto`` resolves it over ``httpfs``, so the
engine must never hand a source path to ``pathlib`` — a ``Path("s3://b/k.csv")``
mangles the scheme. These helpers extract the two facts the loaders need (the
format suffix to dispatch on, the basename stem to name the raw table) by
treating the URI as a string.

Security (DAT-389 hardening): a source URI is **not** opaque. It is handed to
DuckDB's ``read_*_auto`` verbatim, so any path DuckDB can resolve is a read
primitive — a local path (``/etc/passwd``), a ``file://`` URI, or another
bucket would be an arbitrary-file / arbitrary-bucket read on the worker. SQL
literal escaping does not help: a valid literal ``'/etc/passwd'`` is still read.
:func:`validate_source_uri` is the single ingress gate — every source URI must
pass it before reaching a loader.
"""

from __future__ import annotations

from urllib.parse import urlsplit

from dataraum.core.settings import get_settings

__all__ = ["uri_basename", "uri_stem", "uri_suffix", "validate_source_uri"]


def validate_source_uri(uri: str) -> str:
    """Return ``uri`` if it is an ``s3://<lake-bucket>/<key>`` URI; else raise.

    The lake bucket is the one configured via ``settings.s3_bucket`` (the same
    bucket DuckLake writes parquet to). A source URI is passed verbatim to
    DuckDB's ``read_*_auto``, so this is the one gate that keeps the worker from
    reading an arbitrary local file or a foreign bucket. Rejected shapes:

    * non-``s3`` schemes — bare local paths (``/etc/passwd``), relative paths
      (``../foo.csv``, ``foo.csv``), ``file:///...``;
    * any bucket other than the configured lake bucket (``s3://other/x.csv``);
    * credential-in-URL forms (``s3://key:secret@bucket/x.csv``) — the secret is
      registered out of band, never carried in the source URI;
    * a missing object key (``s3://bucket`` with no ``/key``);
    * ``..`` path-traversal segments in the key.

    Args:
        uri: The source URI from ``connection_config['path']``.

    Returns:
        The validated URI unchanged (so callers can inline the check).

    Raises:
        ValueError: If the URI is not exactly ``s3://<lake-bucket>/<key>``.
    """
    bucket = get_settings().s3_bucket
    parts = urlsplit(uri)

    if parts.scheme != "s3":
        raise ValueError(
            f"Invalid source URI {uri!r}: only 's3://{bucket}/<key>' is allowed "
            f"(scheme was {parts.scheme or 'none'!r}). Local paths, 'file://', "
            "and relative paths are rejected — upload the file to the lake bucket "
            "and register its s3:// URI."
        )

    # ``netloc`` (not ``hostname``) is compared on purpose: it preserves any
    # ``user:pass@`` userinfo and ``:port`` suffix, so a cred-in-URL form
    # (``s3://k:s@dataraum-lake/x.csv``) fails this exact match even though its
    # parsed hostname would equal the bucket. Case-sensitive: bucket names are.
    if parts.netloc != bucket:
        raise ValueError(
            f"Invalid source URI {uri!r}: bucket must be exactly {bucket!r}, "
            f"got {parts.netloc!r}. Foreign buckets and credential-in-URL forms "
            "(s3://key:secret@bucket/...) are rejected."
        )

    key = parts.path.lstrip("/")
    if not key:
        raise ValueError(
            f"Invalid source URI {uri!r}: missing an object key after the bucket "
            f"(expected 's3://{bucket}/<key>')."
        )

    if ".." in key.split("/"):
        raise ValueError(
            f"Invalid source URI {uri!r}: '..' path traversal is not allowed in the key."
        )

    return uri


def uri_basename(uri: str) -> str:
    """Return the last ``/``-separated segment of an ``s3://`` source URI.

    ``s3://bucket/uploads/<uuid>/orders.csv`` → ``orders.csv``. The scheme,
    host, and query string are stripped.
    """
    # urlsplit puts ``s3://host/path`` segments in ``path``. Strip any trailing
    # slash, then take the final segment.
    path = urlsplit(uri).path or uri
    return path.rstrip("/").rsplit("/", 1)[-1]


def uri_suffix(uri: str) -> str:
    """Return the lowercased file extension of a source URI (incl. the dot).

    ``s3://bucket/orders.csv`` → ``.csv``; a segment with no dot → ``""``. Used
    to dispatch the loader by format.
    """
    basename = uri_basename(uri)
    dot = basename.rfind(".")
    if dot <= 0:  # no dot, or leading-dot dotfile with no extension
        return ""
    return basename[dot:].lower()


def uri_stem(uri: str) -> str:
    """Return the basename of a source URI without its extension.

    ``s3://bucket/uploads/<uuid>/orders.csv`` → ``orders``. Used to compose the
    raw table name; the loaders sanitize it further for SQL safety.
    """
    basename = uri_basename(uri)
    dot = basename.rfind(".")
    if dot <= 0:
        return basename
    return basename[:dot]
