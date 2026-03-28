"""Tests for central config resolution."""

from pathlib import Path

import pytest

from dataraum.core.config import (
    _get_config_root,
    get_config_dir,
    get_config_file,
    load_phase_config,
    load_pipeline_config,
    load_yaml_config,
    reset_config_root,
    set_config_root,
)


class TestGetConfigFile:
    """Tests for get_config_file()."""

    def test_resolves_llm_config(self):
        path = get_config_file("llm/config.yaml")
        assert path.exists()
        assert path.name == "config.yaml"

    def test_resolves_entropy_thresholds_config(self):
        path = get_config_file("entropy/thresholds.yaml")
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

    def test_resolves_llm_prompts_dir(self):
        path = get_config_dir("llm/prompts")
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
            get_config_dir("llm/config.yaml")


class TestLoadYamlConfig:
    """Tests for load_yaml_config()."""

    def test_loads_valid_yaml(self):
        data = load_yaml_config("llm/config.yaml")
        assert isinstance(data, dict)
        assert "providers" in data or "version" in data or len(data) > 0

    def test_loads_entropy_thresholds(self):
        data = load_yaml_config("entropy/thresholds.yaml")
        assert isinstance(data, dict)

    def test_raises_on_missing_file(self):
        with pytest.raises(FileNotFoundError):
            load_yaml_config("nonexistent.yaml")

    def test_loads_vertical_config(self):
        data = load_yaml_config("verticals/finance/ontology.yaml")
        assert isinstance(data, dict)


class TestLoadPhaseConfig:
    """Tests for load_phase_config()."""

    def test_loads_existing_phase_config(self):
        data = load_phase_config("import")
        assert isinstance(data, dict)
        assert "junk_columns" in data

    def test_returns_empty_for_nonexistent_phase(self):
        data = load_phase_config("nonexistent_phase_xyz")
        assert data == {}

    def test_loads_semantic_phase_config(self):
        data = load_phase_config("semantic")
        assert isinstance(data, dict)
        assert "vertical" in data

    def test_loads_missing_phase_config_returns_empty(self):
        data = load_phase_config("nonexistent_phase")
        assert isinstance(data, dict)
        assert data == {}


class TestLoadPipelineConfig:
    """Tests for load_pipeline_config()."""

    def test_loads_pipeline_config(self):
        data = load_pipeline_config()
        assert isinstance(data, dict)
        assert "phases" in data
        assert "pipeline" in data
        assert isinstance(data["phases"], dict)
        assert "import" in data["phases"]

    def test_pipeline_config_has_pipeline_settings(self):
        data = load_pipeline_config()
        pipeline_cfg = data["pipeline"]
        assert "max_parallel" in pipeline_cfg
        assert "fail_fast" in pipeline_cfg
        assert "retry" in pipeline_cfg

    def test_pipeline_config_has_no_phase_sections(self):
        """Pipeline.yaml should only have pipeline config, not phase config."""
        data = load_pipeline_config()
        # These inline sections were moved to config/phases/
        for phase_section in [
            "import",
            "semantic",
            "temporal",
            "temporal_slice_analysis",
            "quality_summary",
            "slicing",
        ]:
            assert phase_section not in data, (
                f"Phase section '{phase_section}' should not be in pipeline.yaml"
            )


class TestSetConfigRoot:
    """Tests for set_config_root() and reset_config_root()."""

    def teardown_method(self) -> None:
        """Reset config root after each test."""
        reset_config_root()

    def test_set_config_root_overrides(self, tmp_path: Path) -> None:
        config_dir = tmp_path / "custom_config"
        config_dir.mkdir()
        (config_dir / "test.yaml").write_text("key: custom\n")

        set_config_root(config_dir)
        assert _get_config_root() == config_dir

    def test_set_config_root_affects_load_yaml(self, tmp_path: Path) -> None:
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "test.yaml").write_text("custom_key: custom_value\n")

        set_config_root(config_dir)
        data = load_yaml_config("test.yaml")
        assert data["custom_key"] == "custom_value"

    def test_set_config_root_affects_load_phase_config(self, tmp_path: Path) -> None:
        config_dir = tmp_path / "config"
        phases_dir = config_dir / "phases"
        phases_dir.mkdir(parents=True)
        (phases_dir / "typing.yaml").write_text("min_confidence: 0.5\n")

        set_config_root(config_dir)
        data = load_phase_config("typing")
        assert data["min_confidence"] == 0.5

    def test_reset_config_root_reverts(self, tmp_path: Path) -> None:
        original = _get_config_root()

        set_config_root(tmp_path)
        assert _get_config_root() == tmp_path

        reset_config_root()
        assert _get_config_root() == original

    def test_set_config_root_can_be_called_twice(self, tmp_path: Path) -> None:
        dir_a = tmp_path / "a"
        dir_b = tmp_path / "b"
        dir_a.mkdir()
        dir_b.mkdir()

        set_config_root(dir_a)
        assert _get_config_root() == dir_a

        set_config_root(dir_b)
        assert _get_config_root() == dir_b
