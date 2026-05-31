"""Opaque source-URI helpers (DAT-389).

A file source's ``connection_config['path']`` is an opaque URI: an ``s3://``
URI in the container (the lake + uploaded files live on the object store) or a
bare local path for direct dev runs. DuckDB's ``read_*_auto`` reads either
transparently over ``httpfs``, so the engine must never hand a source path to
``pathlib`` — a ``Path("s3://bucket/key.csv")`` mangles the scheme. These
helpers extract the two facts the loaders need (the format suffix to dispatch
on, the basename stem to name the raw table) by treating the URI as a string,
without statting the filesystem or assuming an ``s3://`` prefix.
"""

from __future__ import annotations

from urllib.parse import urlsplit

__all__ = ["uri_basename", "uri_stem", "uri_suffix"]


def uri_basename(uri: str) -> str:
    """Return the last ``/``-separated segment of a source URI.

    Works for ``s3://bucket/uploads/<uuid>/orders.csv`` (→ ``orders.csv``) and
    a bare local path ``/data/orders.csv`` (→ ``orders.csv``). The scheme,
    host, and query string are stripped; no prefix is assumed.
    """
    # urlsplit handles ``s3://host/path`` (path component) and a bare path
    # (everything lands in ``path``). Strip any trailing slash, then take the
    # final segment.
    path = urlsplit(uri).path or uri
    return path.rstrip("/").rsplit("/", 1)[-1]


def uri_suffix(uri: str) -> str:
    """Return the lowercased file extension of a source URI (incl. the dot).

    ``s3://bucket/orders.csv`` → ``.csv``; ``/data/archive.tar.gz`` → ``.gz``;
    a segment with no dot → ``""``. Used to dispatch the loader by format.
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
