"""Tests for auto-derive post-verification: phases declare produces_analyses."""

from dataraum.entropy.dimensions import AnalysisKey


class TestPhaseProducesAnalyses:
    """Verify that phases correctly declare what analyses they produce."""

    def test_typing_phase_produces_typing(self):
        from dataraum.pipeline.registry import get_phase_class

        cls = get_phase_class("typing")
        assert cls is not None
        phase = cls()
        assert phase.produces_analyses == {AnalysisKey.TYPING}

    def test_statistics_phase_produces_statistics(self):
        from dataraum.pipeline.registry import get_phase_class

        cls = get_phase_class("statistics")
        assert cls is not None
        phase = cls()
        assert phase.produces_analyses == {AnalysisKey.STATISTICS}

    def test_semantic_phase_produces_semantic(self):
        from dataraum.pipeline.registry import get_phase_class

        cls = get_phase_class("semantic")
        assert cls is not None
        phase = cls()
        assert phase.produces_analyses == {AnalysisKey.SEMANTIC}

    def test_relationships_phase_produces_relationships(self):
        from dataraum.pipeline.registry import get_phase_class

        cls = get_phase_class("relationships")
        assert cls is not None
        phase = cls()
        assert phase.produces_analyses == {AnalysisKey.RELATIONSHIPS}

    def test_correlations_phase_produces_correlation(self):
        from dataraum.pipeline.registry import get_phase_class

        cls = get_phase_class("correlations")
        assert cls is not None
        phase = cls()
        assert phase.produces_analyses == {AnalysisKey.CORRELATION}

    def test_temporal_slice_analysis_produces_drift_summaries(self):
        from dataraum.pipeline.registry import get_phase_class

        cls = get_phase_class("temporal_slice_analysis")
        assert cls is not None
        phase = cls()
        assert phase.produces_analyses == {AnalysisKey.DRIFT_SUMMARIES}

    def test_slice_analysis_produces_slice_variance(self):
        from dataraum.pipeline.registry import get_phase_class

        cls = get_phase_class("slice_analysis")
        assert cls is not None
        phase = cls()
        assert phase.produces_analyses == {AnalysisKey.SLICE_VARIANCE}

    def test_quality_summary_produces_column_quality_reports(self):
        from dataraum.pipeline.registry import get_phase_class

        cls = get_phase_class("quality_summary")
        assert cls is not None
        phase = cls()
        assert phase.produces_analyses == {AnalysisKey.COLUMN_QUALITY_REPORTS}

    def test_enriched_views_produces_enriched_view(self):
        from dataraum.pipeline.registry import get_phase_class

        cls = get_phase_class("enriched_views")
        assert cls is not None
        phase = cls()
        assert phase.produces_analyses == {AnalysisKey.ENRICHED_VIEW}

    def test_import_phase_produces_nothing(self):
        from dataraum.pipeline.registry import get_phase_class

        cls = get_phase_class("import")
        assert cls is not None
        phase = cls()
        assert phase.produces_analyses == set()

    def test_quality_review_is_quality_gate(self):
        from dataraum.pipeline.registry import get_phase_class

        cls = get_phase_class("quality_review")
        assert cls is not None
        phase = cls()
        assert phase.is_quality_gate is True
        assert phase.produces_analyses == set()


class TestAutoDeriveMappingCompleteness:
    """Every AnalysisKey is produced by exactly one phase."""

    def test_all_analysis_keys_have_producers(self):
        from dataraum.pipeline.registry import get_registry

        registry = get_registry()
        produced: set[AnalysisKey] = set()
        for cls in registry.values():
            phase = cls()
            produced.update(phase.produces_analyses)

        assert produced == set(AnalysisKey), f"Missing producers for: {set(AnalysisKey) - produced}"
