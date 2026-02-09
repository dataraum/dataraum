"""Tests for central config resolution."""

import pytest

from dataraum.core.config import get_config_dir, get_config_file, load_yaml_config


class TestGetConfigFile:
    """Tests for get_config_file()."""

    def test_resolves_system_config(self):
        path = get_config_file("system/llm.yaml")
        assert path.exists()
        assert path.name == "llm.yaml"

    def test_resolves_nested_system_config(self):
        path = get_config_file("system/entropy/thresholds.yaml")
        assert path.exists()
        assert path.name == "thresholds.yaml"

    def test_resolves_vertical_config(self):
        path = get_config_file("verticals/finance/ontology.yaml")
        assert path.exists()
        assert path.name == "ontology.yaml"

    def test_raises_on_missing_file(self):
        with pytest.raises(FileNotFoundError, match="Config file not found"):
            get_config_file("nonexistent/file.yaml")

    def test_error_message_includes_paths(self):
        with pytest.raises(FileNotFoundError, match="relative: bogus.yaml"):
            get_config_file("bogus.yaml")


class TestGetConfigDir:
    """Tests for get_config_dir()."""

    def test_resolves_system_prompts_dir(self):
        path = get_config_dir("system/prompts")
        assert path.is_dir()
        assert path.name == "prompts"

    def test_resolves_verticals_dir(self):
        path = get_config_dir("verticals/finance")
        assert path.is_dir()

    def test_raises_on_missing_dir(self):
        with pytest.raises(FileNotFoundError, match="Config directory not found"):
            get_config_dir("nonexistent/dir")

    def test_raises_on_file_not_dir(self):
        with pytest.raises(FileNotFoundError, match="Config directory not found"):
            get_config_dir("system/llm.yaml")


class TestLoadYamlConfig:
    """Tests for load_yaml_config()."""

    def test_loads_valid_yaml(self):
        data = load_yaml_config("system/llm.yaml")
        assert isinstance(data, dict)
        assert "providers" in data or "version" in data or len(data) > 0

    def test_loads_nested_yaml(self):
        data = load_yaml_config("system/entropy/thresholds.yaml")
        assert isinstance(data, dict)

    def test_raises_on_missing_file(self):
        with pytest.raises(FileNotFoundError):
            load_yaml_config("nonexistent.yaml")

    def test_loads_vertical_config(self):
        data = load_yaml_config("verticals/finance/ontology.yaml")
        assert isinstance(data, dict)
