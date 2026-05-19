"""Tests for resolve_source_path — recipe-aware path resolution.

Convention: recipes live directly under `root` (the container
``SOURCES_DIR``). The resolver tries the path as-given first, then
falls back to bare-name lookup in ``root`` for recipe-shaped inputs
(``.yaml``/``.yml`` or no extension).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dataraum.sources.manager import resolve_source_path

VALID_RECIPE = "backend: mssql\ntables:\n  t:\n    sql: SELECT 1\n"


@pytest.fixture
def root(tmp_path: Path) -> Path:
    """A fake SOURCES_DIR — recipes live directly under it."""
    return tmp_path


class TestDirectPaths:
    def test_absolute_existing_path(self, root, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("a\n1\n")
        result = resolve_source_path(str(f), root)
        assert result is not None
        assert result.path == f.resolve()
        assert result.fell_back_to_recipes is False

    def test_relative_existing_path(self, root, tmp_path, monkeypatch):
        f = tmp_path / "data.csv"
        f.write_text("a\n1\n")
        monkeypatch.chdir(tmp_path)
        result = resolve_source_path("data.csv", root)
        assert result is not None
        assert result.path == f.resolve()
        assert result.fell_back_to_recipes is False

    def test_user_tilde_expansion(self, root, tmp_path, monkeypatch):
        f = tmp_path / "home_csv.csv"
        f.write_text("a\n1\n")
        monkeypatch.setenv("HOME", str(tmp_path))
        result = resolve_source_path("~/home_csv.csv", root)
        assert result is not None
        assert result.path == f.resolve()
        assert result.fell_back_to_recipes is False


class TestRecipesFallback:
    def test_bare_name_resolves_to_yaml(self, root):
        recipe = root / "erp.yaml"
        recipe.write_text(VALID_RECIPE)
        result = resolve_source_path("erp", root)
        assert result is not None
        assert result.path == recipe.resolve()
        assert result.fell_back_to_recipes is True

    def test_bare_name_resolves_to_yml(self, root):
        # .yml is also a valid recipe extension
        recipe = root / "erp.yml"
        recipe.write_text(VALID_RECIPE)
        result = resolve_source_path("erp", root)
        assert result is not None
        assert result.path == recipe.resolve()

    def test_yaml_filename_resolves_under_root(self, root):
        recipe = root / "erp.yaml"
        recipe.write_text(VALID_RECIPE)
        result = resolve_source_path("erp.yaml", root)
        assert result is not None
        assert result.path == recipe.resolve()
        # Direct lookup hits via the as-given branch when CWD is root;
        # bare-name lookup uses fell_back_to_recipes=True when CWD is elsewhere.

    def test_yml_filename_resolves_under_root(self, root):
        recipe = root / "erp.yml"
        recipe.write_text(VALID_RECIPE)
        result = resolve_source_path("erp.yml", root)
        assert result is not None
        assert result.path == recipe.resolve()

    def test_prefers_yaml_over_yml_for_bare_name(self, root):
        """If both erp.yaml and erp.yml exist, .yaml wins."""
        (root / "erp.yaml").write_text(VALID_RECIPE)
        (root / "erp.yml").write_text(VALID_RECIPE)
        result = resolve_source_path("erp", root)
        assert result is not None
        assert result.path.suffix == ".yaml"


class TestNoFallbackForNonRecipeFiles:
    def test_csv_missing_does_not_fall_back(self, root):
        # Even if a same-named .csv existed under root, a .csv extension
        # wouldn't trigger the fallback — only recipe-shaped names do.
        (root / "data.csv").write_text("a\n1\n")
        # CWD-relative lookup may still hit; force the test from elsewhere.
        result = resolve_source_path("/nonexistent/data.csv", root)
        assert result is None, "CSV path must not fall back to root/"

    def test_parquet_missing_does_not_fall_back(self, root):
        result = resolve_source_path("/nonexistent/data.parquet", root)
        assert result is None


class TestMissingPaths:
    def test_returns_none_when_nothing_found(self, root):
        result = resolve_source_path("nonexistent.yaml", root)
        assert result is None

    def test_returns_none_for_bare_name_with_no_match(self, root):
        result = resolve_source_path("nonexistent", root)
        assert result is None


class TestDirectPathWinsOverFallback:
    def test_existing_local_yaml_beats_root(self, root, tmp_path, monkeypatch):
        # If a local erp.yaml exists in cwd AND SOURCES_DIR/erp.yaml
        # exists, the local one wins.
        local_dir = tmp_path / "local"
        local_dir.mkdir()
        local_recipe = local_dir / "erp.yaml"
        local_recipe.write_text(VALID_RECIPE)
        (root / "erp.yaml").write_text("backend: sqlite\ntables:\n  x:\n    sql: SELECT 1\n")
        monkeypatch.chdir(local_dir)

        result = resolve_source_path("erp.yaml", root)
        assert result is not None
        assert result.path == local_recipe.resolve()
        assert result.fell_back_to_recipes is False
