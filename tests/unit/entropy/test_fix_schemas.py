"""Tests for YAML fix schema loader."""

from __future__ import annotations

from pathlib import Path

import pytest

from dataraum.entropy.fix_schemas import (
    clear_fix_schema_cache,
    get_all_schemas,
    get_fix_schema,
    get_schemas_for_detector,
    get_triage_guidance,
)
from dataraum.pipeline.fixes.models import FixSchema

# Resolve the config file path relative to the project root
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_FIXES_YAML = _PROJECT_ROOT / "config" / "entropy" / "fixes.yaml"


@pytest.fixture(autouse=True)
def _clear_cache() -> None:
    """Clear schema cache before each test."""
    clear_fix_schema_cache()


class TestLoaderBasics:
    """Test the YAML loader functions."""

    def test_get_all_schemas_returns_all_detectors(self) -> None:
        all_schemas = get_all_schemas(config_path=_FIXES_YAML)
        # 14 detectors with fix schemas
        assert len(all_schemas) == 14

    def test_total_schema_count(self) -> None:
        all_schemas = get_all_schemas(config_path=_FIXES_YAML)
        total = sum(len(schemas) for schemas in all_schemas.values())
        assert total == 20

    def test_get_schemas_for_known_detector(self) -> None:
        schemas = get_schemas_for_detector("type_fidelity", config_path=_FIXES_YAML)
        assert len(schemas) == 3
        actions = {s.action for s in schemas}
        assert actions == {"accept_finding", "set_column_type", "add_type_pattern"}

    def test_get_schemas_for_unknown_detector(self) -> None:
        schemas = get_schemas_for_detector("nonexistent", config_path=_FIXES_YAML)
        assert schemas == []

    def test_get_fix_schema_by_action_and_dimension(self) -> None:
        schema = get_fix_schema(
            "accept_finding",
            dimension_path="value.nulls.null_ratio",
            config_path=_FIXES_YAML,
        )
        assert schema is not None
        assert schema.action == "accept_finding"
        assert schema.config_path == "entropy/thresholds.yaml"
        assert schema.key_path == ["detectors", "null_ratio", "accepted_columns"]

    def test_get_fix_schema_without_dimension_returns_first_match(self) -> None:
        schema = get_fix_schema("accept_finding", config_path=_FIXES_YAML)
        assert schema is not None
        assert schema.action == "accept_finding"

    def test_get_fix_schema_not_found(self) -> None:
        schema = get_fix_schema("nonexistent_action", config_path=_FIXES_YAML)
        assert schema is None

    def test_get_triage_guidance(self) -> None:
        guidance = get_triage_guidance("type_fidelity", config_path=_FIXES_YAML)
        assert "add_type_pattern" in guidance
        assert "set_column_type" in guidance

    def test_get_triage_guidance_empty(self) -> None:
        guidance = get_triage_guidance("null_ratio", config_path=_FIXES_YAML)
        assert guidance == ""

    def test_get_triage_guidance_unknown_detector(self) -> None:
        guidance = get_triage_guidance("nonexistent", config_path=_FIXES_YAML)
        assert guidance == ""

    def test_caching_returns_same_objects(self) -> None:
        s1 = get_schemas_for_detector("type_fidelity", config_path=_FIXES_YAML)
        s2 = get_schemas_for_detector("type_fidelity", config_path=_FIXES_YAML)
        assert s1 is s2  # Same list object from cache

    def test_clear_cache_forces_reload(self) -> None:
        s1 = get_schemas_for_detector("type_fidelity", config_path=_FIXES_YAML)
        clear_fix_schema_cache()
        s2 = get_schemas_for_detector("type_fidelity", config_path=_FIXES_YAML)
        assert s1 is not s2  # Different objects after cache clear
        assert len(s1) == len(s2)


class TestSchemaFields:
    """Test that YAML schemas produce correct FixSchema instances."""

    def test_config_target_schema(self) -> None:
        schema = get_fix_schema(
            "set_column_type",
            dimension_path="structural.types.type_fidelity",
            config_path=_FIXES_YAML,
        )
        assert schema is not None
        assert schema.target == "config"
        assert schema.config_path == "phases/typing.yaml"
        assert schema.key_path == ["overrides", "forced_types"]
        assert schema.operation == "merge"
        assert schema.requires_rerun == "typing"
        assert schema.routing == "preprocess"
        assert schema.gate is None
        assert "target_type" in schema.fields
        field = schema.fields["target_type"]
        assert field.type == "enum"
        assert field.required is True
        assert field.default == "VARCHAR"
        assert field.enum_values == [
            "VARCHAR", "BIGINT", "DOUBLE", "DATE", "TIMESTAMP", "BOOLEAN"
        ]

    def test_data_target_schema(self) -> None:
        schema = get_fix_schema(
            "recalculate_derived_column",
            config_path=_FIXES_YAML,
        )
        assert schema is not None
        assert schema.target == "data"
        assert schema.templates is not None
        assert "recalculate" in schema.templates
        assert schema.requires_rerun == "correlations"
        assert schema.routing == "preprocess"

    def test_postprocess_routing(self) -> None:
        schema = get_fix_schema(
            "accept_finding",
            dimension_path="value.nulls.null_ratio",
            config_path=_FIXES_YAML,
        )
        assert schema is not None
        assert schema.routing == "postprocess"
        assert schema.gate == "quality_review"
        assert schema.requires_rerun is None

    def test_key_template(self) -> None:
        schema = get_fix_schema(
            "confirm_relationship",
            config_path=_FIXES_YAML,
        )
        assert schema is not None
        assert schema.key_template == "{from_table}->{to_table}"

    def test_all_schemas_are_fixschema_instances(self) -> None:
        all_schemas = get_all_schemas(config_path=_FIXES_YAML)
        for detector_id, schemas in all_schemas.items():
            for schema in schemas:
                assert isinstance(schema, FixSchema), (
                    f"{detector_id}/{schema.action} is not a FixSchema"
                )


class TestRoutingConsistency:
    """Verify routing and gate fields are set correctly."""

    def test_all_schemas_have_routing(self) -> None:
        all_schemas = get_all_schemas(config_path=_FIXES_YAML)
        for detector_id, schemas in all_schemas.items():
            for schema in schemas:
                assert schema.routing in ("preprocess", "postprocess"), (
                    f"{detector_id}/{schema.action} has routing={schema.routing!r}"
                )

    def test_preprocess_schemas_have_requires_rerun(self) -> None:
        all_schemas = get_all_schemas(config_path=_FIXES_YAML)
        for detector_id, schemas in all_schemas.items():
            for schema in schemas:
                if schema.routing == "preprocess":
                    assert schema.requires_rerun is not None, (
                        f"{detector_id}/{schema.action} is preprocess but has no requires_rerun"
                    )

    def test_postprocess_schemas_have_gate(self) -> None:
        all_schemas = get_all_schemas(config_path=_FIXES_YAML)
        for detector_id, schemas in all_schemas.items():
            for schema in schemas:
                if schema.routing == "postprocess":
                    assert schema.gate is not None, (
                        f"{detector_id}/{schema.action} is postprocess but has no gate"
                    )

    def test_routing_counts(self) -> None:
        """5 preprocess, 15 postprocess per the plan."""
        all_schemas = get_all_schemas(config_path=_FIXES_YAML)
        pre = post = 0
        for schemas in all_schemas.values():
            for schema in schemas:
                if schema.routing == "preprocess":
                    pre += 1
                elif schema.routing == "postprocess":
                    post += 1
        assert pre == 5
        assert post == 15


class TestDetectorSchemaInventory:
    """Verify YAML has schemas for all detectors that should have them."""

    def test_all_expected_detectors_have_schemas(self) -> None:
        """Every detector that previously had Python fix_schemas has a YAML entry."""
        expected = {
            "type_fidelity", "join_path_determinism", "relationship_entropy",
            "null_ratio", "outlier_rate", "benford", "temporal_drift",
            "unit_entropy", "temporal_entropy", "business_meaning",
            "dimensional_entropy", "business_cycle_health",
            "derived_value", "cross_table_consistency",
        }
        all_schemas = get_all_schemas(config_path=_FIXES_YAML)
        assert set(all_schemas.keys()) == expected

    def test_detectors_without_schemas_are_excluded(self) -> None:
        """column_quality and dimension_coverage have no fix schemas."""
        all_schemas = get_all_schemas(config_path=_FIXES_YAML)
        assert "column_quality" not in all_schemas
        assert "dimension_coverage" not in all_schemas
