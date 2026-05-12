"""Tests for the db_recipe yaml parser and validator."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from dataraum.sources.db_recipe import Recipe, parse_recipe

VALID_RECIPE = """\
backend: mssql
tables:
  invoices:
    sql: |
      SELECT invoice_id, total_amount
      FROM dbo.Invoices
      WHERE invoice_date >= '2024-01-01'
  customers:
    sql: |
      SELECT customer_id, name, region
      FROM dbo.Customers
"""


@pytest.fixture
def write_recipe(tmp_path: Path):
    def _write(content: str, suffix: str = ".yaml") -> Path:
        p = tmp_path / f"recipe{suffix}"
        p.write_text(content)
        return p

    return _write


class TestValidRecipes:
    def test_parses_minimal_valid_recipe(self, write_recipe):
        path = write_recipe(VALID_RECIPE)
        result = parse_recipe(path)
        assert result.success, result.error
        recipe = result.unwrap()
        assert isinstance(recipe, Recipe)
        assert recipe.backend == "mssql"
        assert [t.name for t in recipe.tables] == ["invoices", "customers"]
        assert "dbo.Invoices" in recipe.tables[0].sql
        assert recipe.source_path == path

    def test_recipe_hash_is_sha256_of_raw_bytes(self, write_recipe):
        path = write_recipe(VALID_RECIPE)
        recipe = parse_recipe(path).unwrap()
        expected = hashlib.sha256(path.read_bytes()).hexdigest()
        assert recipe.recipe_hash == expected

    def test_recipe_hash_changes_with_whitespace(self, tmp_path):
        a = tmp_path / "a.yaml"
        a.write_text(VALID_RECIPE)
        b = tmp_path / "b.yaml"
        b.write_text(VALID_RECIPE + "\n")  # trailing newline
        ha = parse_recipe(a).unwrap().recipe_hash
        hb = parse_recipe(b).unwrap().recipe_hash
        assert ha != hb, "Hash must reflect raw bytes — trailing whitespace counts"

    def test_accepts_yml_extension(self, write_recipe):
        path = write_recipe(VALID_RECIPE, suffix=".yml")
        result = parse_recipe(path)
        assert result.success, result.error

    def test_backend_is_normalized_lowercase(self, write_recipe):
        path = write_recipe("backend: MSSQL\ntables:\n  t:\n    sql: SELECT 1\n")
        result = parse_recipe(path)
        assert result.success
        assert result.unwrap().backend == "mssql"

    def test_each_supported_backend_accepted(self, write_recipe):
        for backend in ("mssql", "postgres", "mysql", "sqlite"):
            path = write_recipe(f"backend: {backend}\ntables:\n  t:\n    sql: SELECT 1\n")
            result = parse_recipe(path)
            assert result.success, f"{backend}: {result.error}"

    def test_sql_is_stripped_of_surrounding_whitespace(self, write_recipe):
        path = write_recipe("backend: mssql\ntables:\n  t:\n    sql: '   SELECT 1   '\n")
        recipe = parse_recipe(path).unwrap()
        assert recipe.tables[0].sql == "SELECT 1"

    def test_table_name_is_stripped(self, write_recipe):
        # Note: YAML mapping keys can have surrounding spaces if quoted
        path = write_recipe('backend: mssql\ntables:\n  "  spaced  ":\n    sql: SELECT 1\n')
        recipe = parse_recipe(path).unwrap()
        assert recipe.tables[0].name == "spaced"


class TestFileLevelRejections:
    def test_missing_file(self, tmp_path):
        result = parse_recipe(tmp_path / "nonexistent.yaml")
        assert not result.success
        assert "not found" in result.error.lower()

    def test_wrong_extension(self, tmp_path):
        path = tmp_path / "recipe.txt"
        path.write_text(VALID_RECIPE)
        result = parse_recipe(path)
        assert not result.success
        assert ".yaml" in result.error or ".yml" in result.error

    def test_malformed_yaml(self, write_recipe):
        path = write_recipe("backend: mssql\ntables:\n  - this is a list not a map\n")
        result = parse_recipe(path)
        # YAML parses fine but the structure is wrong — that's a different error.
        # We want to make sure outright YAML syntax errors also fail loud.
        truly_bad = write_recipe("backend: mssql\n  bad_indent:\n: : :\n")
        result_bad = parse_recipe(truly_bad)
        assert not result_bad.success
        assert "invalid" in result_bad.error.lower() or "yaml" in result_bad.error.lower()

    def test_top_level_not_a_mapping(self, write_recipe):
        path = write_recipe("- just a list\n- of items\n")
        result = parse_recipe(path)
        assert not result.success
        assert "mapping" in result.error.lower()


class TestSecretFreeEnforcement:
    @pytest.mark.parametrize(
        "forbidden_key",
        ["connection", "credentials", "auth", "password", "secret", "secrets"],
    )
    def test_credential_like_top_level_keys_rejected(self, write_recipe, forbidden_key):
        path = write_recipe(
            f"backend: mssql\n"
            f"{forbidden_key}: anything\n"
            f"tables:\n  t:\n    sql: SELECT 1\n"
        )
        result = parse_recipe(path)
        assert not result.success
        assert "secret-free" in result.error.lower()
        assert forbidden_key in result.error
        assert "DATARAUM_" in result.error

    def test_multiple_forbidden_keys_listed(self, write_recipe):
        path = write_recipe(
            "backend: mssql\n"
            "connection: x\n"
            "password: y\n"
            "tables:\n  t:\n    sql: SELECT 1\n"
        )
        result = parse_recipe(path)
        assert not result.success
        assert "connection" in result.error
        assert "password" in result.error


class TestBackendRejections:
    def test_missing_backend(self, write_recipe):
        path = write_recipe("tables:\n  t:\n    sql: SELECT 1\n")
        result = parse_recipe(path)
        assert not result.success
        assert "backend" in result.error.lower()

    def test_empty_backend(self, write_recipe):
        path = write_recipe("backend: ''\ntables:\n  t:\n    sql: SELECT 1\n")
        result = parse_recipe(path)
        assert not result.success
        assert "backend" in result.error.lower()

    def test_unsupported_backend(self, write_recipe):
        path = write_recipe("backend: oracle\ntables:\n  t:\n    sql: SELECT 1\n")
        result = parse_recipe(path)
        assert not result.success
        assert "oracle" in result.error.lower()
        assert "supported" in result.error.lower()

    def test_non_string_backend(self, write_recipe):
        path = write_recipe("backend: 42\ntables:\n  t:\n    sql: SELECT 1\n")
        result = parse_recipe(path)
        assert not result.success


class TestTablesRejections:
    def test_missing_tables(self, write_recipe):
        path = write_recipe("backend: mssql\n")
        result = parse_recipe(path)
        assert not result.success
        assert "tables" in result.error.lower()

    def test_empty_tables(self, write_recipe):
        path = write_recipe("backend: mssql\ntables: {}\n")
        result = parse_recipe(path)
        assert not result.success
        assert "at least one" in result.error.lower()

    def test_tables_as_list_rejected(self, write_recipe):
        path = write_recipe("backend: mssql\ntables:\n  - invoices\n  - customers\n")
        result = parse_recipe(path)
        assert not result.success

    def test_table_without_sql_key(self, write_recipe):
        path = write_recipe("backend: mssql\ntables:\n  t: {}\n")
        result = parse_recipe(path)
        assert not result.success
        assert "sql" in result.error.lower()

    def test_table_with_empty_sql(self, write_recipe):
        path = write_recipe("backend: mssql\ntables:\n  t:\n    sql: ''\n")
        result = parse_recipe(path)
        assert not result.success
        assert "empty" in result.error.lower() or "missing" in result.error.lower()

    def test_table_with_whitespace_only_sql(self, write_recipe):
        path = write_recipe("backend: mssql\ntables:\n  t:\n    sql: '   '\n")
        result = parse_recipe(path)
        assert not result.success

    def test_table_body_not_a_mapping(self, write_recipe):
        path = write_recipe("backend: mssql\ntables:\n  t: SELECT 1\n")
        result = parse_recipe(path)
        assert not result.success
        assert "mapping" in result.error.lower()

    def test_duplicate_table_name_case_insensitive(self, write_recipe):
        path = write_recipe(
            "backend: mssql\n"
            "tables:\n"
            "  invoices:\n"
            "    sql: SELECT 1\n"
            "  Invoices:\n"
            "    sql: SELECT 2\n"
        )
        result = parse_recipe(path)
        assert not result.success
        assert "duplicate" in result.error.lower()
