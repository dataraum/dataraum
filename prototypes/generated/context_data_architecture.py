class ContextDataArchitecture:
    """
    Complete architecture for context generation and quality metrics
    """

    def context_sources(self):
        return {
            "statistical_context": {
                "source": "StatisticalProfiler",
                "data_from": ["raw_tables", "staged_views"],
                "outputs": {
                    "basic_stats": "min, max, mean, median, std, quartiles",
                    "distributions": "histogram, kde, normality tests",
                    "correlations": "pearson, spearman, mutual_information",
                    "time_patterns": "seasonality, trends, autocorrelation",
                },
                "tools": ["pandas.describe", "scipy.stats", "statsmodels"],
            },
            "topological_context": {
                "source": "TopologicalAnalyzer",
                "data_from": ["transaction_flows", "account_relationships"],
                "outputs": {
                    "persistence_diagrams": "birth/death of features",
                    "betti_numbers": "connected components, loops, voids",
                    "mapper_graphs": "simplified network structure",
                    "cycle_detection": "money flow patterns",
                },
                "tools": ["ripser", "gudhi", "networkx"],
            },
            "semantic_context": {
                "source": "SemanticEnricher",
                "data_from": ["column_names", "table_schemas", "data_samples"],
                "outputs": {
                    "entity_classification": "account types, transaction types",
                    "relationship_graph": "foreign keys, business relationships",
                    "business_rules": "extracted constraints and validations",
                    "domain_mapping": "to standard CoA, IFRS concepts",
                },
                "tools": ["sentence-transformers", "spacy", "custom_rules_engine"],
            },
            "temporal_context": {
                "source": "TemporalAnalyzer",
                "data_from": ["datetime_columns", "transaction_timestamps"],
                "outputs": {
                    "periodicity": "daily, weekly, monthly patterns",
                    "gaps_analysis": "missing periods, irregular updates",
                    "velocity": "transaction frequency, volume over time",
                    "fiscal_alignment": "period ends, year boundaries",
                },
                "tools": ["pandas.dt", "statsmodels.tsa", "prophet"],
            },
            "quality_context": {
                "source": "QualityMonitor",
                "data_from": ["all_pipeline_stages"],
                "outputs": {
                    "completeness_scores": "null rates, missing required fields",
                    "consistency_scores": "cross-table validation results",
                    "accuracy_indicators": "type mismatches, range violations",
                    "timeliness_metrics": "data freshness, update delays",
                },
                "tools": ["great_expectations", "deequ", "custom_validators"],
            },
        }
