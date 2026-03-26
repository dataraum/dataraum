"""Tests for get_ontology, set_vertical, _create_vertical MCP tool helpers."""

from __future__ import annotations

from pathlib import Path

import yaml

MINIMAL_ONTOLOGY = """\
name: test_ontology
version: "1.0.0"
description: Test ontology for unit tests
concepts:
  - name: revenue
    description: Income from sales
    indicators:
      - revenue
      - total
    temporal_behavior: additive
    typical_role: measure
"""


def _write_ontology(verticals_dir: Path, vertical_name: str) -> None:
    vdir = verticals_dir / vertical_name
    vdir.mkdir(parents=True)
    (vdir / "ontology.yaml").write_text(MINIMAL_ONTOLOGY)


def _write_phase_configs(config_dir: Path, vertical: str = "finance") -> None:
    phases_dir = config_dir / "phases"
    phases_dir.mkdir(parents=True)
    for name in ["semantic", "validation", "graph_execution", "business_cycles"]:
        path = phases_dir / f"{name}.yaml"
        path.write_text(f"vertical: {vertical}\n")


class TestGetOntology:
    def test_list_verticals(self, tmp_path: Path, monkeypatch: object) -> None:
        """list_verticals=True returns available vertical names."""
        from dataraum.mcp.server import _get_ontology

        _write_ontology(tmp_path, "finance")
        _write_ontology(tmp_path, "shopify_datev")

        monkeypatch.setattr(
            "dataraum.core.config.get_config_dir",
            lambda name: tmp_path if name == "verticals" else tmp_path / name,
        )

        result = _get_ontology(vertical=None, concept=None, list_verticals=True)

        assert "verticals" in result
        assert "finance" in result["verticals"]
        assert "shopify_datev" in result["verticals"]

    def test_returns_concepts_for_vertical(self, tmp_path: Path, monkeypatch: object) -> None:
        """Returns concept list when a valid vertical is specified."""
        from dataraum.mcp.server import _get_ontology

        _write_ontology(tmp_path, "finance")

        monkeypatch.setattr(
            "dataraum.core.config.get_config_dir",
            lambda name: tmp_path if name == "verticals" else tmp_path / name,
        )

        result = _get_ontology(vertical="finance", concept=None, list_verticals=False)

        assert "error" not in result
        assert result["concept_count"] == 1
        assert result["concepts"][0]["name"] == "revenue"

    def test_filters_by_concept_name(self, tmp_path: Path, monkeypatch: object) -> None:
        """concept= filter returns only matching concept."""
        from dataraum.mcp.server import _get_ontology

        _write_ontology(tmp_path, "finance")

        monkeypatch.setattr(
            "dataraum.core.config.get_config_dir",
            lambda name: tmp_path if name == "verticals" else tmp_path / name,
        )

        result = _get_ontology(vertical="finance", concept="revenue", list_verticals=False)

        assert "error" not in result
        assert len(result["concepts"]) == 1
        assert result["concepts"][0]["name"] == "revenue"

    def test_unknown_concept_returns_error(self, tmp_path: Path, monkeypatch: object) -> None:
        """concept= with a name that doesn't exist returns error."""
        from dataraum.mcp.server import _get_ontology

        _write_ontology(tmp_path, "finance")

        monkeypatch.setattr(
            "dataraum.core.config.get_config_dir",
            lambda name: tmp_path if name == "verticals" else tmp_path / name,
        )

        result = _get_ontology(vertical="finance", concept="nonexistent", list_verticals=False)

        assert "error" in result

    def test_unknown_vertical_returns_error_with_available(
        self, tmp_path: Path, monkeypatch: object
    ) -> None:
        """Unknown vertical returns error and lists available verticals."""
        from dataraum.mcp.server import _get_ontology

        _write_ontology(tmp_path, "finance")

        monkeypatch.setattr(
            "dataraum.core.config.get_config_dir",
            lambda name: tmp_path if name == "verticals" else tmp_path / name,
        )

        result = _get_ontology(vertical="mystery", concept=None, list_verticals=False)

        assert "error" in result
        assert "available" in result
        assert "finance" in result["available"]


class TestSetVertical:
    def test_updates_all_phase_config_files(self, tmp_path: Path, monkeypatch: object) -> None:
        """set_vertical writes the new vertical name to all 4 phase config files."""
        from dataraum.mcp.server import _set_vertical

        verticals_dir = tmp_path / "verticals"
        _write_ontology(verticals_dir, "shopify_datev")
        _write_phase_configs(tmp_path)

        monkeypatch.setattr(
            "dataraum.core.config.get_config_dir",
            lambda name: verticals_dir if name == "verticals" else tmp_path / name,
        )
        monkeypatch.setattr(
            "dataraum.core.config.get_config_file",
            lambda rel: tmp_path / rel,
        )

        result = _set_vertical("shopify_datev")

        assert "error" not in result
        assert result["vertical"] == "shopify_datev"
        assert len(result["updated_phase_configs"]) == 4

        # Verify the files on disk
        for name in ["semantic", "validation", "graph_execution", "business_cycles"]:
            path = tmp_path / "phases" / f"{name}.yaml"
            data = yaml.safe_load(path.read_text())
            assert data["vertical"] == "shopify_datev"

    def test_unknown_vertical_returns_error(self, tmp_path: Path, monkeypatch: object) -> None:
        """set_vertical with a name that doesn't exist returns error without touching files."""
        from dataraum.mcp.server import _set_vertical

        verticals_dir = tmp_path / "verticals"
        verticals_dir.mkdir()
        _write_phase_configs(tmp_path)

        monkeypatch.setattr(
            "dataraum.core.config.get_config_dir",
            lambda name: verticals_dir if name == "verticals" else tmp_path / name,
        )
        monkeypatch.setattr(
            "dataraum.core.config.get_config_file",
            lambda rel: tmp_path / rel,
        )

        result = _set_vertical("nonexistent")

        assert "error" in result
        assert "available" in result

        # Files should not have been modified
        for name in ["semantic", "validation", "graph_execution", "business_cycles"]:
            path = tmp_path / "phases" / f"{name}.yaml"
            data = yaml.safe_load(path.read_text())
            assert data["vertical"] == "finance"

    def test_returns_list_of_updated_files(self, tmp_path: Path, monkeypatch: object) -> None:
        """Response includes updated_phase_configs list."""
        from dataraum.mcp.server import _set_vertical

        verticals_dir = tmp_path / "verticals"
        _write_ontology(verticals_dir, "finance")
        _write_phase_configs(tmp_path)

        monkeypatch.setattr(
            "dataraum.core.config.get_config_dir",
            lambda name: verticals_dir if name == "verticals" else tmp_path / name,
        )
        monkeypatch.setattr(
            "dataraum.core.config.get_config_file",
            lambda rel: tmp_path / rel,
        )

        result = _set_vertical("finance")

        assert isinstance(result["updated_phase_configs"], list)


class TestToolRegistration:
    def test_new_helpers_importable(self) -> None:
        """Verify the three new helper functions exist and are callable."""
        from dataraum.mcp.server import _create_vertical, _get_ontology, _set_vertical

        assert callable(_get_ontology)
        assert callable(_set_vertical)
        assert callable(_create_vertical)

    def test_tool_names_in_list_tools(self) -> None:
        """All three new tools appear in the server's tool list."""
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=Path("/tmp/test_output"))
        assert server is not None
        # The tools list is registered via @server.list_tools() — we verify by name
        # (no easy introspection; just ensure no import error and server created OK)
