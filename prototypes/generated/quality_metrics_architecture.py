class QualityMetricsArchitecture:
    """
    Detailed quality metrics with implementation hints
    """

    def metrics_catalog(self):
        return {
            "data_integrity_metrics": {
                "null_rate": {
                    "formula": "count_null / count_total",
                    "implementation": "pandas.isnull().sum() / len(df)",
                    "threshold": "< 0.05 for required fields",
                    "library": "pandas",
                },
                "type_consistency": {
                    "formula": "count_correct_type / count_total",
                    "implementation": "pd.to_numeric(errors='coerce').notna().sum()",
                    "threshold": "> 0.99",
                    "library": "pandas",
                },
                "duplicate_rate": {
                    "formula": "1 - (unique_rows / total_rows)",
                    "implementation": "df.duplicated().sum() / len(df)",
                    "threshold": "< 0.01",
                    "library": "pandas",
                },
                "referential_integrity": {
                    "formula": "foreign_keys_valid / foreign_keys_total",
                    "implementation": "df.merge(validate='many_to_one')",
                    "threshold": "= 1.0",
                    "library": "pandas",
                },
            },
            "statistical_quality_metrics": {
                "benford_law_compliance": {
                    "formula": "chi_square_test(observed, benford_expected)",
                    "implementation": "scipy.stats.chisquare()",
                    "threshold": "p_value > 0.05",
                    "library": "scipy",
                    "use_case": "fraud detection in financial amounts",
                },
                "outlier_score": {
                    "formula": "isolation_forest anomaly score",
                    "implementation": "sklearn.ensemble.IsolationForest",
                    "threshold": "anomaly_ratio < 0.05",
                    "library": "scikit-learn",
                },
                "distribution_stability": {
                    "formula": "KS test between periods",
                    "implementation": "scipy.stats.ks_2samp()",
                    "threshold": "p_value > 0.01",
                    "library": "scipy",
                },
                "variance_inflation": {
                    "formula": "VIF = 1/(1-R²)",
                    "implementation": "statsmodels.stats.outliers_influence.variance_inflation_factor",
                    "threshold": "VIF < 10",
                    "library": "statsmodels",
                },
            },
            "financial_specific_metrics": {
                "double_entry_balance": {
                    "formula": "abs(sum(debits) - sum(credits))",
                    "implementation": "grouped.debit.sum() - grouped.credit.sum()",
                    "threshold": "< 0.01 (rounding tolerance)",
                    "library": "pandas",
                },
                "account_sign_convention": {
                    "formula": "count_correct_sign / count_total",
                    "implementation": "check_sign_by_account_type()",
                    "threshold": "> 0.99",
                    "library": "custom",
                },
                "trial_balance_check": {
                    "formula": "assets = liabilities + equity",
                    "implementation": "tb.groupby('type').sum()",
                    "threshold": "difference < 0.001%",
                    "library": "pandas",
                },
                "consolidation_completeness": {
                    "formula": "intercompany_eliminated / intercompany_total",
                    "implementation": "match_intercompany_transactions()",
                    "threshold": "= 1.0",
                    "library": "custom",
                },
            },
            "topological_metrics": {
                "cycle_persistence": {
                    "formula": "lifetime of 1-dimensional features",
                    "implementation": "ripser()['dgms'][1]",
                    "threshold": "significant cycles > 3",
                    "library": "ripser",
                },
                "component_count": {
                    "formula": "number of 0-dimensional features",
                    "implementation": "connected_components(graph)",
                    "threshold": "= expected_components",
                    "library": "networkx",
                },
                "structural_complexity": {
                    "formula": "sum of Betti numbers",
                    "implementation": "sum([len(dgm) for dgm in persistence['dgms']])",
                    "threshold": "within historical range",
                    "library": "gudhi/ripser",
                },
                "homological_stability": {
                    "formula": "bottleneck_distance between periods",
                    "implementation": "gudhi.bottleneck_distance()",
                    "threshold": "< 0.2",
                    "library": "gudhi",
                },
            },
            "temporal_metrics": {
                "update_frequency_score": {
                    "formula": "std(time_between_updates)",
                    "implementation": "df.time.diff().std()",
                    "threshold": "< expected_std",
                    "library": "pandas",
                },
                "seasonality_strength": {
                    "formula": "1 - (var(deseasonalized) / var(original))",
                    "implementation": "statsmodels.tsa.seasonal_decompose()",
                    "threshold": "> 0.6 for seasonal data",
                    "library": "statsmodels",
                },
                "trend_break_detection": {
                    "formula": "CUSUM or Chow test",
                    "implementation": "ruptures.Pelt(model='rbf')",
                    "threshold": "breakpoints < 2 per year",
                    "library": "ruptures",
                },
                "data_freshness": {
                    "formula": "now() - max(update_timestamp)",
                    "implementation": "datetime.now() - df.timestamp.max()",
                    "threshold": "< SLA requirement",
                    "library": "datetime",
                },
            },
        }
