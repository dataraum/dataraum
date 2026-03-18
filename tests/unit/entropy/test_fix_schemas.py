"""Tests for YAML fix schema loader and parity with Python detector schemas."""

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


class TestParityWithDetectors:
    """Verify YAML schemas match Python detector schemas field-by-field.

    This test catches transcription errors. Once Phase 2 removes Python
    schemas, this class should be deleted.
    """

    def _get_python_schemas(self) -> dict[str, list[FixSchema]]:
        """Get fix schemas from Python detector classes."""
        from dataraum.entropy.detectors.base import DetectorRegistry, _register_builtin_detectors

        registry = DetectorRegistry()
        _register_builtin_detectors(registry)
        result: dict[str, list[FixSchema]] = {}
        for detector in registry.get_all_detectors():
            if detector.fix_schemas:
                result[detector.detector_id] = detector.fix_schemas
        return result

    def _get_python_triage(self) -> dict[str, str]:
        """Get triage guidance from Python detector classes."""
        from dataraum.entropy.detectors.base import DetectorRegistry, _register_builtin_detectors

        registry = DetectorRegistry()
        _register_builtin_detectors(registry)
        result: dict[str, str] = {}
        for detector in registry.get_all_detectors():
            if detector.triage_guidance:
                result[detector.detector_id] = detector.triage_guidance
        return result

    def test_same_detectors_have_schemas(self) -> None:
        python = self._get_python_schemas()
        yaml_schemas = get_all_schemas(config_path=_FIXES_YAML)
        assert set(python.keys()) == set(yaml_schemas.keys())

    def test_same_action_names_per_detector(self) -> None:
        python = self._get_python_schemas()
        yaml_schemas = get_all_schemas(config_path=_FIXES_YAML)
        for detector_id in python:
            py_actions = {s.action for s in python[detector_id]}
            yaml_actions = {s.action for s in yaml_schemas[detector_id]}
            assert py_actions == yaml_actions, (
                f"{detector_id}: Python actions {py_actions} != YAML actions {yaml_actions}"
            )

    def test_field_by_field_equivalence(self) -> None:
        """Compare every schema field between Python and YAML.

        Note: In Python, all schemas use ``requires_rerun`` for both
        preprocess (actual phase re-run) and postprocess (gate name).
        The YAML splits this: preprocess schemas keep ``requires_rerun``,
        postprocess schemas move the value to ``gate`` and leave
        ``requires_rerun`` as None. This test accounts for that mapping.
        """
        python = self._get_python_schemas()
        yaml_schemas = get_all_schemas(config_path=_FIXES_YAML)

        # Gate names that indicate a postprocess action in Python code.
        # When Python requires_rerun is one of these, YAML stores it in
        # gate instead and sets requires_rerun=None.
        gate_names = {"quality_review", "analysis_review", "computation_review"}

        # Actions that Python marked as requires_rerun="semantic" but the
        # YAML intentionally reclassifies as postprocess (the whole point
        # of the config-driven-fixes feature: skip expensive re-runs for
        # fixes that just patch config/metadata).
        reclassified_to_postprocess = {
            "confirm_relationship",
            "set_unit_source",
            "document_business_meaning",
            "set_timestamp_role",
        }

        # Fields to compare (routing/gate are new in YAML, not in Python)
        compare_fields = [
            "action", "target", "config_path", "key_path", "operation",
            "model", "templates", "key_template",
        ]

        for detector_id in python:
            py_by_action = {s.action: s for s in python[detector_id]}
            yaml_by_action = {s.action: s for s in yaml_schemas[detector_id]}

            for action_name in py_by_action:
                py_s = py_by_action[action_name]
                yaml_s = yaml_by_action[action_name]

                for field_name in compare_fields:
                    py_val = getattr(py_s, field_name)
                    yaml_val = getattr(yaml_s, field_name)
                    assert py_val == yaml_val, (
                        f"{detector_id}/{action_name}.{field_name}: "
                        f"Python={py_val!r} != YAML={yaml_val!r}"
                    )

                # Check requires_rerun / gate mapping
                py_rerun = py_s.requires_rerun
                if action_name in reclassified_to_postprocess:
                    # These actions were Python requires_rerun="semantic"
                    # but YAML intentionally reclassifies as postprocess
                    assert yaml_s.routing == "postprocess", (
                        f"{detector_id}/{action_name}: expected postprocess routing"
                    )
                    assert yaml_s.gate is not None, (
                        f"{detector_id}/{action_name}: reclassified action needs a gate"
                    )
                    assert yaml_s.requires_rerun is None, (
                        f"{detector_id}/{action_name}: postprocess should have requires_rerun=None"
                    )
                elif py_rerun in gate_names:
                    # Python stored gate name in requires_rerun;
                    # YAML moved it to gate field
                    assert yaml_s.routing == "postprocess", (
                        f"{detector_id}/{action_name}: expected postprocess routing"
                    )
                    assert yaml_s.gate == py_rerun, (
                        f"{detector_id}/{action_name}.gate: "
                        f"Python requires_rerun={py_rerun!r} != YAML gate={yaml_s.gate!r}"
                    )
                    assert yaml_s.requires_rerun is None, (
                        f"{detector_id}/{action_name}: postprocess should have requires_rerun=None"
                    )
                else:
                    # Preprocess: requires_rerun should match
                    assert yaml_s.routing == "preprocess", (
                        f"{detector_id}/{action_name}: expected preprocess routing"
                    )
                    assert yaml_s.requires_rerun == py_rerun, (
                        f"{detector_id}/{action_name}.requires_rerun: "
                        f"Python={py_rerun!r} != YAML={yaml_s.requires_rerun!r}"
                    )

                # Compare field definitions
                assert set(py_s.fields.keys()) == set(yaml_s.fields.keys()), (
                    f"{detector_id}/{action_name} fields mismatch: "
                    f"Python={set(py_s.fields.keys())} YAML={set(yaml_s.fields.keys())}"
                )

                for fname in py_s.fields:
                    py_f = py_s.fields[fname]
                    yaml_f = yaml_s.fields[fname]
                    assert py_f.type == yaml_f.type, (
                        f"{detector_id}/{action_name}.fields.{fname}.type: "
                        f"{py_f.type!r} != {yaml_f.type!r}"
                    )
                    assert py_f.required == yaml_f.required, (
                        f"{detector_id}/{action_name}.fields.{fname}.required: "
                        f"{py_f.required!r} != {yaml_f.required!r}"
                    )
                    assert py_f.default == yaml_f.default, (
                        f"{detector_id}/{action_name}.fields.{fname}.default: "
                        f"{py_f.default!r} != {yaml_f.default!r}"
                    )
                    assert py_f.enum_values == yaml_f.enum_values, (
                        f"{detector_id}/{action_name}.fields.{fname}.enum_values: "
                        f"{py_f.enum_values!r} != {yaml_f.enum_values!r}"
                    )

    def test_triage_guidance_parity(self) -> None:
        """Verify triage guidance content matches between Python and YAML.

        YAML block scalars (``|``) preserve internal newlines, while Python
        string concatenation produces a single long line per paragraph.
        We normalize both by collapsing runs of whitespace to single spaces
        before comparing.
        """
        import re

        def _normalize(text: str) -> str:
            # Collapse all whitespace runs (including newlines) to single space
            return re.sub(r"\s+", " ", text.strip())

        python_triage = self._get_python_triage()
        for detector_id, py_guidance in python_triage.items():
            yaml_guidance = get_triage_guidance(detector_id, config_path=_FIXES_YAML)
            assert _normalize(py_guidance) == _normalize(yaml_guidance), (
                f"{detector_id} triage guidance mismatch:\n"
                f"--- Python (normalized) ---\n{_normalize(py_guidance)}\n"
                f"--- YAML (normalized) ---\n{_normalize(yaml_guidance)}"
            )
