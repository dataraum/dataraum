"""Tests for inline fix data models and config patch utilities."""

from pathlib import Path

import pytest
import yaml

from dataraum.pipeline.fixes import (
    ConfigPatch,
    FixInput,
    FixResult,
    apply_config_patch,
)


class TestConfigPatch:
    """Tests for ConfigPatch dataclass."""

    def test_create_with_all_fields(self) -> None:
        patch = ConfigPatch(
            config_path="phases/typing.yaml",
            operation="set",
            key_path=["min_confidence"],
            value=0.8,
            reason="Lower confidence threshold for date detection",
        )
        assert patch.config_path == "phases/typing.yaml"
        assert patch.operation == "set"
        assert patch.key_path == ["min_confidence"]
        assert patch.value == 0.8
        assert patch.reason == "Lower confidence threshold for date detection"

    def test_defaults(self) -> None:
        patch = ConfigPatch(
            config_path="phases/typing.yaml",
            operation="remove",
            key_path=["obsolete_key"],
        )
        assert patch.value is None
        assert patch.reason == ""


class TestFixInput:
    """Tests for FixInput dataclass."""

    def test_create_with_all_fields(self) -> None:
        fix_input = FixInput(
            action_name="transform_exclude_outliers",
            parameters={"exclude_columns": ["amount", "quantity"]},
            interpretation="User wants to exclude financial columns from outlier analysis",
            affected_columns=["orders.amount", "orders.quantity"],
            entropy_evidence={"outlier_rate": 0.15, "method": "iqr"},
        )
        assert fix_input.action_name == "transform_exclude_outliers"
        assert fix_input.parameters["exclude_columns"] == ["amount", "quantity"]
        assert len(fix_input.affected_columns) == 2

    def test_defaults(self) -> None:
        fix_input = FixInput(action_name="document_unit")
        assert fix_input.parameters == {}
        assert fix_input.interpretation == ""
        assert fix_input.affected_columns == []
        assert fix_input.entropy_evidence == {}


class TestFixResult:
    """Tests for FixResult dataclass."""

    def test_create_with_patches(self) -> None:
        patch = ConfigPatch(
            config_path="phases/statistics.yaml",
            operation="set",
            key_path=["outlier_exclusions"],
            value=["amount"],
            reason="Exclude amount from outlier analysis",
        )
        result = FixResult(
            config_patches=[patch],
            requires_rerun="statistics",
            summary="Excluded 1 column from outlier analysis",
        )
        assert len(result.config_patches) == 1
        assert result.requires_rerun == "statistics"

    def test_defaults(self) -> None:
        result = FixResult()
        assert result.config_patches == []
        assert result.requires_rerun == ""
        assert result.summary == ""


class TestApplyConfigPatchSet:
    """Tests for apply_config_patch with 'set' operation."""

    def test_set_top_level_key(self, tmp_path: Path) -> None:
        config_root = tmp_path / "config"
        config_root.mkdir()
        phases_dir = config_root / "phases"
        phases_dir.mkdir()
        (phases_dir / "typing.yaml").write_text("min_confidence: 0.9\n")

        patch = ConfigPatch(
            config_path="phases/typing.yaml",
            operation="set",
            key_path=["min_confidence"],
            value=0.7,
        )
        apply_config_patch(config_root, patch)

        result = yaml.safe_load((phases_dir / "typing.yaml").read_text())
        assert result["min_confidence"] == 0.7

    def test_set_nested_key(self, tmp_path: Path) -> None:
        config_root = tmp_path / "config"
        config_root.mkdir()
        (config_root / "test.yaml").write_text("overrides:\n  amount: USD\n")

        patch = ConfigPatch(
            config_path="test.yaml",
            operation="set",
            key_path=["overrides", "quantity"],
            value="units",
        )
        apply_config_patch(config_root, patch)

        result = yaml.safe_load((config_root / "test.yaml").read_text())
        assert result["overrides"]["quantity"] == "units"
        assert result["overrides"]["amount"] == "USD"  # preserved

    def test_set_creates_intermediate_dicts(self, tmp_path: Path) -> None:
        config_root = tmp_path / "config"
        config_root.mkdir()
        (config_root / "test.yaml").write_text("{}\n")

        patch = ConfigPatch(
            config_path="test.yaml",
            operation="set",
            key_path=["a", "b", "c"],
            value=42,
        )
        apply_config_patch(config_root, patch)

        result = yaml.safe_load((config_root / "test.yaml").read_text())
        assert result["a"]["b"]["c"] == 42

    def test_set_creates_file_if_missing(self, tmp_path: Path) -> None:
        config_root = tmp_path / "config"
        config_root.mkdir()

        patch = ConfigPatch(
            config_path="phases/new_phase.yaml",
            operation="set",
            key_path=["enabled"],
            value=True,
        )
        apply_config_patch(config_root, patch)

        result = yaml.safe_load((config_root / "phases" / "new_phase.yaml").read_text())
        assert result["enabled"] is True

    def test_set_overwrites_existing_value(self, tmp_path: Path) -> None:
        config_root = tmp_path / "config"
        config_root.mkdir()
        (config_root / "test.yaml").write_text("key: old_value\n")

        patch = ConfigPatch(
            config_path="test.yaml",
            operation="set",
            key_path=["key"],
            value="new_value",
        )
        apply_config_patch(config_root, patch)

        result = yaml.safe_load((config_root / "test.yaml").read_text())
        assert result["key"] == "new_value"


class TestApplyConfigPatchAppend:
    """Tests for apply_config_patch with 'append' operation."""

    def test_append_to_existing_list(self, tmp_path: Path) -> None:
        config_root = tmp_path / "config"
        config_root.mkdir()
        (config_root / "test.yaml").write_text("date_patterns:\n  - '%Y-%m-%d'\n")

        patch = ConfigPatch(
            config_path="test.yaml",
            operation="append",
            key_path=["date_patterns"],
            value="%d/%m/%Y",
        )
        apply_config_patch(config_root, patch)

        result = yaml.safe_load((config_root / "test.yaml").read_text())
        assert result["date_patterns"] == ["%Y-%m-%d", "%d/%m/%Y"]

    def test_append_creates_list_if_missing(self, tmp_path: Path) -> None:
        config_root = tmp_path / "config"
        config_root.mkdir()
        (config_root / "test.yaml").write_text("{}\n")

        patch = ConfigPatch(
            config_path="test.yaml",
            operation="append",
            key_path=["exclusions"],
            value="amount",
        )
        apply_config_patch(config_root, patch)

        result = yaml.safe_load((config_root / "test.yaml").read_text())
        assert result["exclusions"] == ["amount"]

    def test_append_to_non_list_raises(self, tmp_path: Path) -> None:
        config_root = tmp_path / "config"
        config_root.mkdir()
        (config_root / "test.yaml").write_text("key: a_string\n")

        patch = ConfigPatch(
            config_path="test.yaml",
            operation="append",
            key_path=["key"],
            value="item",
        )
        with pytest.raises(ValueError, match="Cannot append to non-list"):
            apply_config_patch(config_root, patch)


class TestApplyConfigPatchRemove:
    """Tests for apply_config_patch with 'remove' operation."""

    def test_remove_existing_key(self, tmp_path: Path) -> None:
        config_root = tmp_path / "config"
        config_root.mkdir()
        (config_root / "test.yaml").write_text("keep: 1\ndelete_me: 2\n")

        patch = ConfigPatch(
            config_path="test.yaml",
            operation="remove",
            key_path=["delete_me"],
        )
        apply_config_patch(config_root, patch)

        result = yaml.safe_load((config_root / "test.yaml").read_text())
        assert "delete_me" not in result
        assert result["keep"] == 1

    def test_remove_missing_key_is_idempotent(self, tmp_path: Path) -> None:
        config_root = tmp_path / "config"
        config_root.mkdir()
        (config_root / "test.yaml").write_text("key: value\n")

        patch = ConfigPatch(
            config_path="test.yaml",
            operation="remove",
            key_path=["nonexistent"],
        )
        # Should not raise
        apply_config_patch(config_root, patch)

        result = yaml.safe_load((config_root / "test.yaml").read_text())
        assert result["key"] == "value"

    def test_remove_nested_key(self, tmp_path: Path) -> None:
        config_root = tmp_path / "config"
        config_root.mkdir()
        (config_root / "test.yaml").write_text("parent:\n  keep: 1\n  remove: 2\n")

        patch = ConfigPatch(
            config_path="test.yaml",
            operation="remove",
            key_path=["parent", "remove"],
        )
        apply_config_patch(config_root, patch)

        result = yaml.safe_load((config_root / "test.yaml").read_text())
        assert result["parent"] == {"keep": 1}


class TestApplyConfigPatchMerge:
    """Tests for apply_config_patch with 'merge' operation."""

    def test_merge_into_existing_dict(self, tmp_path: Path) -> None:
        config_root = tmp_path / "config"
        config_root.mkdir()
        (config_root / "test.yaml").write_text("overrides:\n  col_a: VARCHAR\n")

        patch = ConfigPatch(
            config_path="test.yaml",
            operation="merge",
            key_path=["overrides"],
            value={"col_b": "INTEGER", "col_c": "DATE"},
        )
        apply_config_patch(config_root, patch)

        result = yaml.safe_load((config_root / "test.yaml").read_text())
        assert result["overrides"] == {"col_a": "VARCHAR", "col_b": "INTEGER", "col_c": "DATE"}

    def test_merge_creates_dict_if_missing(self, tmp_path: Path) -> None:
        config_root = tmp_path / "config"
        config_root.mkdir()
        (config_root / "test.yaml").write_text("{}\n")

        patch = ConfigPatch(
            config_path="test.yaml",
            operation="merge",
            key_path=["new_section"],
            value={"key1": "val1", "key2": "val2"},
        )
        apply_config_patch(config_root, patch)

        result = yaml.safe_load((config_root / "test.yaml").read_text())
        assert result["new_section"] == {"key1": "val1", "key2": "val2"}

    def test_merge_non_dict_value_raises(self, tmp_path: Path) -> None:
        config_root = tmp_path / "config"
        config_root.mkdir()
        (config_root / "test.yaml").write_text("{}\n")

        patch = ConfigPatch(
            config_path="test.yaml",
            operation="merge",
            key_path=["key"],
            value="not_a_dict",
        )
        with pytest.raises(ValueError, match="Cannot merge non-dict value"):
            apply_config_patch(config_root, patch)

    def test_merge_into_non_dict_raises(self, tmp_path: Path) -> None:
        config_root = tmp_path / "config"
        config_root.mkdir()
        (config_root / "test.yaml").write_text("key: a_string\n")

        patch = ConfigPatch(
            config_path="test.yaml",
            operation="merge",
            key_path=["key"],
            value={"a": 1},
        )
        with pytest.raises(ValueError, match="Cannot merge into non-dict"):
            apply_config_patch(config_root, patch)


class TestApplyConfigPatchEdgeCases:
    """Edge case tests for apply_config_patch."""

    def test_empty_key_path_raises(self, tmp_path: Path) -> None:
        config_root = tmp_path / "config"
        config_root.mkdir()
        (config_root / "test.yaml").write_text("{}\n")

        patch = ConfigPatch(
            config_path="test.yaml",
            operation="set",
            key_path=[],
            value="whatever",
        )
        with pytest.raises(ValueError, match="key_path must not be empty"):
            apply_config_patch(config_root, patch)

    def test_unknown_operation_raises(self, tmp_path: Path) -> None:
        config_root = tmp_path / "config"
        config_root.mkdir()
        (config_root / "test.yaml").write_text("{}\n")

        patch = ConfigPatch(
            config_path="test.yaml",
            operation="delete_all",
            key_path=["key"],
        )
        with pytest.raises(ValueError, match="Unknown operation"):
            apply_config_patch(config_root, patch)

    def test_missing_config_root_raises(self, tmp_path: Path) -> None:
        config_root = tmp_path / "nonexistent"

        patch = ConfigPatch(
            config_path="test.yaml",
            operation="set",
            key_path=["key"],
            value=1,
        )
        with pytest.raises(FileNotFoundError, match="Config root not found"):
            apply_config_patch(config_root, patch)

    def test_intermediate_non_dict_is_replaced(self, tmp_path: Path) -> None:
        """When navigating key_path, a non-dict intermediate is replaced with {}."""
        config_root = tmp_path / "config"
        config_root.mkdir()
        (config_root / "test.yaml").write_text("parent: a_string\n")

        patch = ConfigPatch(
            config_path="test.yaml",
            operation="set",
            key_path=["parent", "child"],
            value="value",
        )
        apply_config_patch(config_root, patch)

        result = yaml.safe_load((config_root / "test.yaml").read_text())
        assert result["parent"]["child"] == "value"

    def test_multiple_patches_accumulate(self, tmp_path: Path) -> None:
        """Multiple patches to the same file accumulate correctly."""
        config_root = tmp_path / "config"
        config_root.mkdir()
        (config_root / "test.yaml").write_text("{}\n")

        patches = [
            ConfigPatch(config_path="test.yaml", operation="set", key_path=["a"], value=1),
            ConfigPatch(config_path="test.yaml", operation="set", key_path=["b"], value=2),
            ConfigPatch(
                config_path="test.yaml", operation="append", key_path=["items"], value="first"
            ),
            ConfigPatch(
                config_path="test.yaml", operation="append", key_path=["items"], value="second"
            ),
        ]
        for p in patches:
            apply_config_patch(config_root, p)

        result = yaml.safe_load((config_root / "test.yaml").read_text())
        assert result == {"a": 1, "b": 2, "items": ["first", "second"]}
