"""Tests for entropy contracts module."""

import pytest

from dataraum.entropy.analysis.aggregator import ColumnSummary
from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.contracts import (
    ConfidenceLevel,
    ContractProfile,
    Violation,
    _calculate_confidence_level,
    clear_contracts_cache,
    evaluate_contract,
    find_best_contract,
    get_contract,
    get_contracts,
    list_contracts,
)
from dataraum.entropy.models import CompoundRisk


def _make_column_summary(
    column_name: str = "test_column",
    table_name: str = "test_table",
    layer_scores: dict[str, float] | None = None,
    dimension_scores: dict[str, float] | None = None,
    compound_risks: list[CompoundRisk] | None = None,
) -> ColumnSummary:
    """Helper to create ColumnSummary for tests."""
    config = get_entropy_config()
    weights = config.composite_weights

    layer_scores = layer_scores or {
        "structural": 0.1,
        "semantic": 0.1,
        "value": 0.1,
        "computational": 0.1,
    }

    composite_score = (
        layer_scores.get("structural", 0.0) * weights["structural"]
        + layer_scores.get("semantic", 0.0) * weights["semantic"]
        + layer_scores.get("value", 0.0) * weights["value"]
        + layer_scores.get("computational", 0.0) * weights["computational"]
    )

    dimension_scores = dimension_scores or {}
    high_threshold = config.high_entropy_threshold
    high_entropy_dims = [d for d, s in dimension_scores.items() if s >= high_threshold]

    return ColumnSummary(
        column_id=f"col_{column_name}",
        column_name=column_name,
        table_id=f"tbl_{table_name}",
        table_name=table_name,
        composite_score=composite_score,
        readiness=config.get_readiness(composite_score),
        layer_scores=layer_scores,
        dimension_scores=dimension_scores,
        high_entropy_dimensions=high_entropy_dims,
        compound_risks=compound_risks or [],
    )


@pytest.fixture(autouse=True)
def clear_cache():
    """Clear contracts cache before each test."""
    clear_contracts_cache()
    yield
    clear_contracts_cache()


class TestConfidenceLevel:
    """Tests for ConfidenceLevel enum."""

    def test_emoji(self):
        """Each level has correct emoji."""
        assert ConfidenceLevel.GREEN.emoji == "🟢"
        assert ConfidenceLevel.YELLOW.emoji == "🟡"
        assert ConfidenceLevel.ORANGE.emoji == "🟠"
        assert ConfidenceLevel.RED.emoji == "🔴"

    def test_label(self):
        """Each level has correct label."""
        assert ConfidenceLevel.GREEN.label == "GOOD"
        assert ConfidenceLevel.YELLOW.label == "MARGINAL"
        assert ConfidenceLevel.ORANGE.label == "ISSUES"
        assert ConfidenceLevel.RED.label == "BLOCKED"

    def test_value(self):
        """Each level has correct string value."""
        assert ConfidenceLevel.GREEN.value == "green"
        assert ConfidenceLevel.YELLOW.value == "yellow"
        assert ConfidenceLevel.ORANGE.value == "orange"
        assert ConfidenceLevel.RED.value == "red"


class TestContractsFromYaml:
    """Tests for contract definitions loaded from YAML."""

    def test_contracts_exist(self):
        """Contracts are available from YAML config."""
        contracts = get_contracts()
        assert len(contracts) >= 5

        expected = [
            "exploratory_analysis",
            "operational_analytics",
            "executive_dashboard",
            "regulatory_reporting",
            "data_science",
        ]
        for name in expected:
            assert name in contracts

    def test_contracts_ordered_by_strictness(self):
        """Contracts have expected strictness ordering."""
        contracts = get_contracts()

        # Strictness is measured by overall_threshold (lower = stricter)
        assert (
            contracts["regulatory_reporting"].overall_threshold
            < contracts["executive_dashboard"].overall_threshold
        )
        assert (
            contracts["executive_dashboard"].overall_threshold
            < contracts["operational_analytics"].overall_threshold
        )
        assert (
            contracts["operational_analytics"].overall_threshold
            < contracts["exploratory_analysis"].overall_threshold
        )

    def test_contract_has_dimension_thresholds(self):
        """Each contract has dimension thresholds defined."""
        contracts = get_contracts()

        for name, contract in contracts.items():
            assert len(contract.dimension_thresholds) > 0, f"{name} has no dimension thresholds"
            # Should have at least structural and semantic dimensions
            assert any("structural" in d for d in contract.dimension_thresholds), (
                f"{name} missing structural dimensions"
            )
            assert any("semantic" in d for d in contract.dimension_thresholds), (
                f"{name} missing semantic dimensions"
            )


class TestContractAccess:
    """Tests for contract access functions."""

    def test_list_contracts(self):
        """list_contracts returns summary of all contracts."""
        contracts = list_contracts()
        assert len(contracts) >= 5

        # Each entry has required fields
        for c in contracts:
            assert "name" in c
            assert "display_name" in c
            assert "description" in c
            assert "overall_threshold" in c

    def test_get_contract(self):
        """get_contract returns specific contract."""
        contract = get_contract("executive_dashboard")
        assert contract is not None
        assert contract.name == "executive_dashboard"
        assert contract.overall_threshold == 0.25

    def test_get_contract_not_found(self):
        """get_contract returns None for unknown contract."""
        contract = get_contract("nonexistent_contract")
        assert contract is None

    def test_get_contracts_cached(self):
        """get_contracts caches results."""
        contracts1 = get_contracts()
        contracts2 = get_contracts()
        assert contracts1 is contracts2


class TestContractEvaluation:
    """Tests for contract evaluation."""

    @pytest.fixture
    def low_entropy_summaries(self) -> tuple[dict[str, ColumnSummary], list[CompoundRisk]]:
        """Column summaries with low entropy across all dimensions."""
        summary = _make_column_summary(
            layer_scores={
                "structural": 0.1,
                "semantic": 0.1,
                "value": 0.1,
                "computational": 0.1,
            },
            dimension_scores={
                "structural.types": 0.1,
                "structural.relations": 0.1,
                "semantic.business_meaning": 0.1,
                "semantic.units": 0.1,
                "semantic.temporal": 0.1,
                "value.nulls": 0.1,
                "value.outliers": 0.1,
                "computational.derived_values": 0.1,
                "computational.aggregations": 0.1,
            },
        )
        return {"test_table.test_column": summary}, []

    @pytest.fixture
    def high_entropy_summaries(self) -> tuple[dict[str, ColumnSummary], list[CompoundRisk]]:
        """Column summaries with high entropy across dimensions."""
        summary = _make_column_summary(
            layer_scores={
                "structural": 0.7,
                "semantic": 0.8,
                "value": 0.6,
                "computational": 0.5,
            },
            dimension_scores={
                "structural.types": 0.7,
                "structural.relations": 0.6,
                "semantic.business_meaning": 0.8,
                "semantic.units": 0.7,
                "semantic.temporal": 0.6,
                "value.nulls": 0.5,
                "value.outliers": 0.6,
                "computational.derived_values": 0.4,
                "computational.aggregations": 0.5,
            },
        )
        return {"test_table.test_column": summary}, []

    @pytest.fixture
    def summaries_with_critical_risk(self) -> tuple[dict[str, ColumnSummary], list[CompoundRisk]]:
        """Column summaries with critical compound risk."""
        critical_risk = CompoundRisk(
            target="test_table.test_column",
            dimensions=["semantic.units", "computational.aggregations"],
            risk_level="critical",
            impact="Unknown currencies being summed",
            combined_score=0.9,
        )
        summary = _make_column_summary(
            layer_scores={
                "structural": 0.3,
                "semantic": 0.3,
                "value": 0.3,
                "computational": 0.3,
            },
            dimension_scores={
                "structural.types": 0.2,
                "semantic.units": 0.3,
            },
            compound_risks=[critical_risk],
        )
        return {"test_table.test_column": summary}, [critical_risk]

    def test_evaluate_exploratory_low_entropy(self, low_entropy_summaries):
        """Low entropy passes exploratory contract."""
        summaries, risks = low_entropy_summaries
        evaluation = evaluate_contract(summaries, "exploratory_analysis", risks)

        assert evaluation.is_compliant is True
        assert evaluation.confidence_level == ConfidenceLevel.GREEN
        assert len(evaluation.violations) == 0

    def test_evaluate_regulatory_low_entropy(self, low_entropy_summaries):
        """Low entropy passes regulatory contract."""
        summaries, risks = low_entropy_summaries
        evaluation = evaluate_contract(summaries, "regulatory_reporting", risks)

        assert evaluation.is_compliant is True
        # Score of 0.1 equals the threshold, so may be YELLOW (approaching threshold)
        assert evaluation.confidence_level in (ConfidenceLevel.GREEN, ConfidenceLevel.YELLOW)

    def test_evaluate_exploratory_high_entropy(self, high_entropy_summaries):
        """High entropy may fail exploratory contract."""
        summaries, risks = high_entropy_summaries
        evaluation = evaluate_contract(summaries, "exploratory_analysis", risks)

        # Exploratory is lenient but 0.8+ scores will trigger blocking
        # Our fixture has 0.8 for semantic.business_meaning
        # Should have violations but may still pass depending on exact scores
        assert len(evaluation.violations) > 0 or len(evaluation.warnings) > 0

    def test_evaluate_regulatory_high_entropy(self, high_entropy_summaries):
        """High entropy fails regulatory contract."""
        summaries, risks = high_entropy_summaries
        evaluation = evaluate_contract(summaries, "regulatory_reporting", risks)

        assert evaluation.is_compliant is False
        assert evaluation.confidence_level in (ConfidenceLevel.ORANGE, ConfidenceLevel.RED)
        assert len(evaluation.violations) > 0

    def test_critical_risk_blocks_strict_contracts(self, summaries_with_critical_risk):
        """Critical compound risk blocks strict contracts."""
        summaries, risks = summaries_with_critical_risk
        # Regulatory should be blocked by critical risk
        reg_eval = evaluate_contract(summaries, "regulatory_reporting", risks)
        assert reg_eval.is_compliant is False

        # Executive should also be blocked
        exec_eval = evaluate_contract(summaries, "executive_dashboard", risks)
        assert exec_eval.is_compliant is False

    def test_evaluation_has_dimension_scores(self, low_entropy_summaries):
        """Evaluation includes dimension scores."""
        summaries, risks = low_entropy_summaries
        evaluation = evaluate_contract(summaries, "exploratory_analysis", risks)

        assert len(evaluation.dimension_scores) > 0
        # Should have scores for dimensions in contract
        assert "structural.types" in evaluation.dimension_scores or any(
            "structural" in k for k in evaluation.dimension_scores
        )

    def test_evaluation_to_dict(self, low_entropy_summaries):
        """Evaluation can be serialized to dict."""
        summaries, risks = low_entropy_summaries
        evaluation = evaluate_contract(summaries, "exploratory_analysis", risks)
        d = evaluation.to_dict()

        assert d["contract_name"] == "exploratory_analysis"
        assert d["is_compliant"] is True
        assert "confidence_level" in d
        assert "dimension_scores" in d
        assert "evaluated_at" in d

    def test_unknown_contract_raises(self, low_entropy_summaries):
        """Evaluating unknown contract raises ValueError."""
        summaries, risks = low_entropy_summaries
        with pytest.raises(ValueError, match="Contract not found"):
            evaluate_contract(summaries, "nonexistent_contract", risks)


class TestFindBestContract:
    """Tests for find_best_contract function."""

    def test_find_best_with_low_entropy(self):
        """Low entropy data should pass strict contracts."""
        summary = _make_column_summary(
            layer_scores={
                "structural": 0.05,
                "semantic": 0.05,
                "value": 0.05,
                "computational": 0.05,
            },
            dimension_scores={
                "structural.types": 0.05,
                "structural.relations": 0.05,
                "semantic.business_meaning": 0.05,
                "semantic.units": 0.05,
                "semantic.temporal": 0.05,
                "value.nulls": 0.05,
                "value.outliers": 0.05,
                "computational.derived_values": 0.05,
                "computational.aggregations": 0.05,
            },
        )
        summaries = {"test_table.test_column": summary}

        name, evaluation = find_best_contract(summaries, [])

        # Should find regulatory_reporting as strictest passing
        assert name == "regulatory_reporting"
        assert evaluation.is_compliant is True

    def test_find_best_with_medium_entropy(self):
        """Medium entropy should find appropriate contract."""
        summary = _make_column_summary(
            layer_scores={
                "structural": 0.3,
                "semantic": 0.35,
                "value": 0.3,
                "computational": 0.25,
            },
            dimension_scores={
                "structural.types": 0.25,
                "structural.relations": 0.3,
                "semantic.business_meaning": 0.35,
                "semantic.units": 0.3,
                "semantic.temporal": 0.35,
                "value.nulls": 0.3,
                "value.outliers": 0.25,
                "computational.derived_values": 0.25,
                "computational.aggregations": 0.25,
            },
        )
        summaries = {"test_table.test_column": summary}

        name, evaluation = find_best_contract(summaries, [])

        # Should find something less strict than regulatory
        assert name in ("operational_analytics", "data_science", "exploratory_analysis")
        assert evaluation.is_compliant is True

    def test_find_best_with_very_high_entropy(self):
        """Very high entropy data should return None when no contracts pass."""
        summary = _make_column_summary(
            layer_scores={
                "structural": 0.9,
                "semantic": 0.9,
                "value": 0.9,
                "computational": 0.9,
            },
            dimension_scores={
                "structural.types": 0.9,
                "structural.relations": 0.9,
                "semantic.business_meaning": 0.9,
                "semantic.units": 0.9,
                "semantic.temporal": 0.9,
                "value.nulls": 0.9,
                "value.outliers": 0.9,
                "computational.derived_values": 0.9,
                "computational.aggregations": 0.9,
            },
        )
        summaries = {"test_table.test_column": summary}

        name, evaluation = find_best_contract(summaries, [])

        # Should return None when no contracts pass
        assert name is None
        assert evaluation is None


class TestConfidenceLevelCalculation:
    """Tests for confidence level calculation."""

    def test_green_when_compliant_no_warnings(self):
        """GREEN when compliant with no warnings."""
        contract = ContractProfile(
            name="test",
            display_name="Test",
            description="Test contract",
            overall_threshold=0.5,
        )

        level = _calculate_confidence_level(
            is_compliant=True,
            violations=[],
            warnings=[],
            dimension_scores={},
            contract=contract,
        )

        assert level == ConfidenceLevel.GREEN

    def test_yellow_when_compliant_with_warnings(self):
        """YELLOW when compliant but has warnings."""
        contract = ContractProfile(
            name="test",
            display_name="Test",
            description="Test contract",
            overall_threshold=0.5,
        )

        warning = Violation(
            violation_type="dimension",
            severity="warning",
            dimension="test.dim",
            max_allowed=0.3,
            actual=0.28,
            details="Approaching threshold",
        )

        level = _calculate_confidence_level(
            is_compliant=True,
            violations=[],
            warnings=[warning],
            dimension_scores={},
            contract=contract,
        )

        assert level == ConfidenceLevel.YELLOW

    def test_orange_when_non_compliant_few_blocking(self):
        """ORANGE when non-compliant with few blocking violations."""
        contract = ContractProfile(
            name="test",
            display_name="Test",
            description="Test contract",
            overall_threshold=0.5,
        )

        violation = Violation(
            violation_type="dimension",
            severity="blocking",
            dimension="test.dim",
            max_allowed=0.3,
            actual=0.5,
            details="Exceeds threshold",
        )

        level = _calculate_confidence_level(
            is_compliant=False,
            violations=[violation],
            warnings=[],
            dimension_scores={},
            contract=contract,
        )

        assert level == ConfidenceLevel.ORANGE

    def test_red_when_many_blocking_violations(self):
        """RED when many blocking violations."""
        contract = ContractProfile(
            name="test",
            display_name="Test",
            description="Test contract",
            overall_threshold=0.5,
        )

        violations = [
            Violation(
                violation_type="dimension",
                severity="blocking",
                dimension=f"test.dim{i}",
                max_allowed=0.3,
                actual=0.5,
                details="Exceeds threshold",
            )
            for i in range(4)
        ]

        level = _calculate_confidence_level(
            is_compliant=False,
            violations=violations,
            warnings=[],
            dimension_scores={},
            contract=contract,
        )

        assert level == ConfidenceLevel.RED

    def test_red_when_blocking_condition_triggered(self):
        """RED when blocking condition is triggered."""
        contract = ContractProfile(
            name="test",
            display_name="Test",
            description="Test contract",
            overall_threshold=0.5,
        )

        violation = Violation(
            violation_type="blocking_condition",
            severity="blocking",
            condition="has_critical_compound_risk",
            details="Critical compound risk exists",
        )

        level = _calculate_confidence_level(
            is_compliant=False,
            violations=[violation],
            warnings=[],
            dimension_scores={},
            contract=contract,
        )

        assert level == ConfidenceLevel.RED
