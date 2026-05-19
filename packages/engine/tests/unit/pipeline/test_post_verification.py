"""Tests for auto-derive post-verification: YAML declares produces and gates."""

from dataraum.entropy.dimensions import AnalysisKey
from dataraum.pipeline.pipeline_config import load_phase_declarations


class TestPhaseProducesAnalyses:
    """Verify that YAML declarations correctly declare what analyses phases produce."""

    def test_typing_phase_produces_typing(self):
        decl = load_phase_declarations()["typing"]
        assert decl.produces == {AnalysisKey.TYPING}

    def test_statistics_phase_produces_statistics(self):
        decl = load_phase_declarations()["statistics"]
        assert decl.produces == {AnalysisKey.STATISTICS}

    def test_semantic_phase_produces_semantic(self):
        decl = load_phase_declarations()["semantic"]
        assert decl.produces == {AnalysisKey.SEMANTIC}

    def test_relationships_phase_produces_relationships(self):
        decl = load_phase_declarations()["relationships"]
        assert decl.produces == {AnalysisKey.RELATIONSHIPS}

    def test_correlations_phase_produces_correlation(self):
        decl = load_phase_declarations()["correlations"]
        assert decl.produces == {AnalysisKey.CORRELATION}

    def test_temporal_slice_analysis_produces_drift_summaries(self):
        decl = load_phase_declarations()["temporal_slice_analysis"]
        assert decl.produces == {AnalysisKey.DRIFT_SUMMARIES}

    def test_slice_analysis_produces_slice_variance(self):
        decl = load_phase_declarations()["slice_analysis"]
        assert decl.produces == {AnalysisKey.SLICE_VARIANCE}

    def test_enriched_views_produces_enriched_view(self):
        decl = load_phase_declarations()["enriched_views"]
        assert decl.produces == {AnalysisKey.ENRICHED_VIEW}

    def test_import_phase_produces_nothing(self):
        decl = load_phase_declarations()["import"]
        assert decl.produces == set()


class TestAutoDeriveMappingCompleteness:
    """Every AnalysisKey is produced by exactly one phase in YAML."""

    def test_all_analysis_keys_have_producers(self):
        declarations = load_phase_declarations()
        produced: set[AnalysisKey] = set()
        for decl in declarations.values():
            produced.update(decl.produces)

        assert produced == set(AnalysisKey), f"Missing producers for: {set(AnalysisKey) - produced}"
