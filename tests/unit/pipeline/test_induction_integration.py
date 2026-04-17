"""Tests for induction agent integration in pipeline phases.

Verifies that induction is triggered when _adhoc config is empty
and skipped when config already has content.
"""

from unittest.mock import MagicMock, patch

import pytest

from dataraum.core.models.base import Result


def _make_ctx(vertical: str = "_adhoc") -> MagicMock:
    """Create a minimal mock PhaseContext."""
    ctx = MagicMock()
    ctx.config = {"vertical": vertical}
    ctx.source_id = "src-1"
    # Mock typed tables query
    mock_table = MagicMock()
    mock_table.table_id = "t1"
    ctx.session.execute.return_value.scalars.return_value.all.return_value = [mock_table]
    return ctx


@pytest.fixture()
def _mock_llm():
    """Patch LLM infrastructure so phases can initialize.

    Patch paths target the consumer modules (business_cycles_phase,
    validation_phase) because the phases do `from dataraum.llm import ...`
    at module load, which captures bindings at import time.
    """
    mock_config = MagicMock()
    mock_config.active_provider = "anthropic"
    mock_config.providers = {"anthropic": MagicMock()}
    mock_config.features = MagicMock()
    mock_config.limits.max_output_tokens_per_request = 8000

    with (
        patch(
            "dataraum.pipeline.phases.business_cycles_phase.load_llm_config",
            return_value=mock_config,
        ),
        patch(
            "dataraum.pipeline.phases.business_cycles_phase.create_provider",
            return_value=MagicMock(),
        ),
        patch(
            "dataraum.pipeline.phases.business_cycles_phase.PromptRenderer",
            return_value=MagicMock(),
        ),
        patch(
            "dataraum.pipeline.phases.validation_phase.load_llm_config",
            return_value=mock_config,
        ),
        patch(
            "dataraum.pipeline.phases.validation_phase.create_provider",
            return_value=MagicMock(),
        ),
        patch(
            "dataraum.pipeline.phases.validation_phase.PromptRenderer",
            return_value=MagicMock(),
        ),
    ):
        yield


class TestBusinessCyclesInduction:
    @patch("dataraum.analysis.cycles.agent.BusinessCycleAgent.analyze")
    @patch("dataraum.analysis.cycles.induction.save_cycles_config")
    @patch("dataraum.analysis.cycles.induction.CycleInductionAgent.induce")
    @patch("dataraum.analysis.cycles.config.get_cycle_types")
    def test_induction_triggered_when_empty(
        self,
        mock_get_types: MagicMock,
        mock_induce: MagicMock,
        mock_save: MagicMock,
        mock_analyze: MagicMock,
        _mock_llm: None,
    ) -> None:
        mock_get_types.return_value = {}
        mock_induce.return_value = Result.ok({"cycle_types": {"test": {"description": "t"}}})
        mock_analyze.return_value = Result.ok(
            MagicMock(
                cycles=[], detected_processes=[], data_quality_observations=[], recommendations=[]
            )
        )

        from dataraum.pipeline.phases.business_cycles_phase import BusinessCyclesPhase

        result = BusinessCyclesPhase()._run(_make_ctx("_adhoc"))

        mock_induce.assert_called_once()
        mock_save.assert_called_once()
        assert result.success

    @patch("dataraum.analysis.cycles.agent.BusinessCycleAgent.analyze")
    @patch("dataraum.analysis.cycles.config.get_cycle_types")
    def test_induction_skipped_when_config_exists(
        self,
        mock_get_types: MagicMock,
        mock_analyze: MagicMock,
        _mock_llm: None,
    ) -> None:
        mock_get_types.return_value = {"existing": {"description": "already here"}}
        mock_analyze.return_value = Result.ok(
            MagicMock(
                cycles=[], detected_processes=[], data_quality_observations=[], recommendations=[]
            )
        )

        from dataraum.pipeline.phases.business_cycles_phase import BusinessCyclesPhase

        with patch("dataraum.analysis.cycles.induction.CycleInductionAgent.induce") as mock_induce:
            result = BusinessCyclesPhase()._run(_make_ctx("_adhoc"))

        mock_induce.assert_not_called()
        assert result.success

    @patch("dataraum.analysis.cycles.agent.BusinessCycleAgent.analyze")
    def test_induction_skipped_for_named_vertical(
        self,
        mock_analyze: MagicMock,
        _mock_llm: None,
    ) -> None:
        mock_analyze.return_value = Result.ok(
            MagicMock(
                cycles=[], detected_processes=[], data_quality_observations=[], recommendations=[]
            )
        )

        from dataraum.pipeline.phases.business_cycles_phase import BusinessCyclesPhase

        with patch("dataraum.analysis.cycles.induction.CycleInductionAgent.induce") as mock_induce:
            result = BusinessCyclesPhase()._run(_make_ctx("finance"))

        mock_induce.assert_not_called()
        assert result.success


class TestValidationInduction:
    @patch("dataraum.analysis.validation.agent.ValidationAgent.run_validations")
    @patch("dataraum.analysis.validation.induction.save_validation_specs")
    @patch("dataraum.analysis.validation.induction.ValidationInductionAgent.induce")
    @patch("dataraum.analysis.validation.config.load_all_validation_specs")
    def test_induction_triggered_when_empty(
        self,
        mock_load_specs: MagicMock,
        mock_induce: MagicMock,
        mock_save: MagicMock,
        mock_run: MagicMock,
        _mock_llm: None,
    ) -> None:
        mock_load_specs.return_value = {}
        mock_induce.return_value = Result.ok([{"validation_id": "test", "name": "Test"}])
        mock_run.return_value = Result.ok(
            MagicMock(total_checks=0, passed=0, failed=0, skipped=0, errors=0, results=[])
        )

        from dataraum.pipeline.phases.validation_phase import ValidationPhase

        result = ValidationPhase()._run(_make_ctx("_adhoc"))

        mock_induce.assert_called_once()
        mock_save.assert_called_once()
        assert result.success

    @patch("dataraum.analysis.validation.agent.ValidationAgent.run_validations")
    @patch("dataraum.analysis.validation.config.load_all_validation_specs")
    def test_induction_skipped_when_specs_exist(
        self,
        mock_load_specs: MagicMock,
        mock_run: MagicMock,
        _mock_llm: None,
    ) -> None:
        mock_load_specs.return_value = {"existing": MagicMock()}
        mock_run.return_value = Result.ok(
            MagicMock(total_checks=1, passed=1, failed=0, skipped=0, errors=0, results=[])
        )

        from dataraum.pipeline.phases.validation_phase import ValidationPhase

        with patch(
            "dataraum.analysis.validation.induction.ValidationInductionAgent.induce"
        ) as mock_induce:
            result = ValidationPhase()._run(_make_ctx("_adhoc"))

        mock_induce.assert_not_called()
        assert result.success
