"""Tests for the source-URI helpers and the security validator (DAT-389).

``validate_source_uri`` is the single ingress gate that keeps a source URI from
becoming an arbitrary-file / arbitrary-bucket read on the worker: the URI is
handed verbatim to DuckDB's ``read_*_auto``, so anything but
``s3://<lake-bucket>/<key>`` must be rejected. SQL-literal escaping does not
help — a valid literal ``'/etc/passwd'`` is still read — so these adversarial
cases assert the validator rejects every non-lake-bucket shape outright.

The lake bucket is ``dataraum-lake`` here (conftest sets ``S3_BUCKET``; the
autouse ``_reset_settings_cache`` fixture makes ``get_settings`` re-read it).
"""

from __future__ import annotations

import pytest

from dataraum.core.uri import (
    uri_basename,
    uri_stem,
    uri_suffix,
    validate_source_uri,
)

_BUCKET = "dataraum-lake"


class TestValidateSourceUri:
    """The security gate: only ``s3://<lake-bucket>/<key>`` is accepted."""

    @pytest.mark.parametrize(
        "uri",
        [
            "/etc/passwd",  # absolute local path — arbitrary file read
            "/app/.env",  # secrets file on the worker
            "../foo.csv",  # relative path traversal
            "file:///etc/passwd",  # file:// scheme
            "foo.csv",  # bare name (relative)
            "s3://other-bucket/x.csv",  # a different bucket on the endpoint
            "s3://k:s@dataraum-lake/x.csv",  # credential-in-URL form
        ],
    )
    def test_rejects_dangerous_uris(self, uri: str) -> None:
        with pytest.raises(ValueError, match="Invalid source URI"):
            validate_source_uri(uri)

    @pytest.mark.parametrize(
        "uri",
        [
            f"s3://{_BUCKET}/orders.csv",
            f"s3://{_BUCKET}/uploads/0c5b1f7e-1c2d-4a9e-9f3a-1234567890ab/x.csv",
            f"s3://{_BUCKET}/events.parquet",
            f"s3://{_BUCKET}/nested/dir/data.jsonl",
        ],
    )
    def test_accepts_lake_bucket_uris(self, uri: str) -> None:
        # Returns the URI unchanged so callers can inline the check.
        assert validate_source_uri(uri) == uri

    def test_rejects_bucket_without_key(self) -> None:
        # An object key is required: s3://bucket alone is not a readable file.
        with pytest.raises(ValueError, match="missing an object key"):
            validate_source_uri(f"s3://{_BUCKET}")

    def test_rejects_traversal_in_key(self) -> None:
        with pytest.raises(ValueError, match="traversal"):
            validate_source_uri(f"s3://{_BUCKET}/../other-bucket/x.csv")

    def test_rejects_userinfo_even_when_host_matches(self) -> None:
        # The cred-in-URL form parses to a hostname equal to the bucket, but the
        # userinfo must still make it fail (we match the full netloc, not the
        # parsed host).
        with pytest.raises(ValueError, match="Invalid source URI"):
            validate_source_uri(f"s3://accesskey:secret@{_BUCKET}/x.csv")

    def test_rejects_port_suffix(self) -> None:
        # A ``:port`` after the bucket changes the netloc and must be rejected.
        with pytest.raises(ValueError, match="Invalid source URI"):
            validate_source_uri(f"s3://{_BUCKET}:9000/x.csv")

    def test_error_names_the_allowed_bucket(self) -> None:
        with pytest.raises(ValueError) as exc:
            validate_source_uri("s3://other-bucket/x.csv")
        assert _BUCKET in str(exc.value)


class TestUriBasename:
    def test_s3_uri(self) -> None:
        assert uri_basename("s3://dataraum-lake/uploads/abc/orders.csv") == "orders.csv"

    def test_strips_query(self) -> None:
        assert uri_basename("s3://dataraum-lake/x.csv?v=1") == "x.csv"


class TestUriSuffix:
    def test_csv(self) -> None:
        assert uri_suffix("s3://dataraum-lake/orders.csv") == ".csv"

    def test_lowercased(self) -> None:
        assert uri_suffix("s3://dataraum-lake/DATA.PARQUET") == ".parquet"

    def test_no_extension(self) -> None:
        assert uri_suffix("s3://dataraum-lake/folder/noext") == ""


class TestUriStem:
    def test_strips_extension(self) -> None:
        assert uri_stem("s3://dataraum-lake/uploads/abc/orders.csv") == "orders"

    def test_no_extension(self) -> None:
        assert uri_stem("s3://dataraum-lake/folder/noext") == "noext"
