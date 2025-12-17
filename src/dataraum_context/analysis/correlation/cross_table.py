"""Cross-table quality analysis for confirmed relationships.

Analyzes data quality issues that can only be detected AFTER relationships
are confirmed by the semantic agent. Runs VDP and correlation analysis on
joined data to detect:

1. Cross-table correlations (unexpected relationships between columns in different tables)
2. Redundant columns (r ≈ 1.0 within same table)
3. Derived columns (r ≈ 1.0 suggesting one column is computed from another)
4. Multicollinearity groups (VDP-based, with cross-table flag)

This complements the pre-confirmation evaluation in analysis/relationships/evaluator.py
which focuses on referential integrity and join quality.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal

import numpy as np

from dataraum_context.analysis.correlation.algorithms import (
    CorrelationResult,
    compute_multicollinearity,
    compute_pairwise_correlations,
)
from dataraum_context.analysis.correlation.models import (
    CrossTableCorrelation,
    CrossTableQualityResult,
    DependencyGroup,
    DerivedColumnCandidate,
    QualityIssue,
    RedundantColumnPair,
)

if TYPE_CHECKING:
    import duckdb

    from dataraum_context.analysis.correlation.models import EnrichedRelationship


def analyze_relationship_quality(
    relationship: EnrichedRelationship,
    duckdb_conn: duckdb.DuckDBPyConnection,
    from_table_path: str,
    to_table_path: str,
    min_correlation: float = 0.3,
    redundancy_threshold: float = 0.99,
    max_sample_size: int = 50000,
) -> CrossTableQualityResult | None:
    """Analyze quality of a single confirmed relationship.

    Joins the tables and runs correlation + VDP analysis to detect
    quality issues in the joined data.

    Args:
        relationship: The confirmed relationship to analyze
        duckdb_conn: DuckDB connection
        from_table_path: DuckDB path to from_table
        to_table_path: DuckDB path to to_table
        min_correlation: Minimum |r| to report
        redundancy_threshold: Correlation threshold for redundancy detection
        max_sample_size: Maximum rows to sample for analysis (default: 50000)

    Returns:
        CrossTableQualityResult or None if analysis fails
    """
    try:
        # Get numeric columns from each table
        from_cols = _get_numeric_columns(duckdb_conn, from_table_path)
        to_cols = _get_numeric_columns(duckdb_conn, to_table_path)

        if not from_cols or not to_cols:
            return None

        # Build and execute join query
        join_col_from = relationship.from_column
        join_col_to = relationship.to_column

        select_parts = []
        col_labels: list[tuple[str, str]] = []  # [(table, column), ...]

        for c in from_cols:
            select_parts.append(f'a."{c}"')
            col_labels.append((relationship.from_table, c))
        for c in to_cols:
            select_parts.append(f'b."{c}"')
            col_labels.append((relationship.to_table, c))

        # First get total row count to decide if sampling is needed
        count_query = f"""
            SELECT COUNT(*) FROM {from_table_path} a
            JOIN {to_table_path} b ON a."{join_col_from}" = b."{join_col_to}"
        """
        count_result = duckdb_conn.execute(count_query).fetchone()
        if count_result is None:
            return None
        total_rows: int = count_result[0]

        # Use sampling for large joins to avoid memory/performance issues
        if total_rows > max_sample_size:
            sample_pct = (max_sample_size / total_rows) * 100
            join_query = f"""
                SELECT {", ".join(select_parts)}
                FROM {from_table_path} a
                JOIN {to_table_path} b ON a."{join_col_from}" = b."{join_col_to}"
                USING SAMPLE {sample_pct:.4f}%
            """
        else:
            join_query = f"""
                SELECT {", ".join(select_parts)}
                FROM {from_table_path} a
                JOIN {to_table_path} b ON a."{join_col_from}" = b."{join_col_to}"
            """

        result = duckdb_conn.execute(join_query).fetchnumpy()
        data = np.column_stack([result[k] for k in result.keys()])

        # Remove NaN rows
        data = data[~np.isnan(data).any(axis=1)]

        if len(data) < 10:
            return None

        n_from = len(from_cols)

        # Compute correlations
        correlations = compute_pairwise_correlations(data, min_correlation=min_correlation)

        # Separate cross-table vs within-table correlations
        cross_table_corrs: list[CrossTableCorrelation] = []
        redundant_pairs: list[RedundantColumnPair] = []
        derived_candidates: list[DerivedColumnCandidate] = []

        corr: CorrelationResult
        for corr in correlations:
            label1 = col_labels[corr.col1_idx]
            label2 = col_labels[corr.col2_idx]
            is_cross = (corr.col1_idx < n_from) != (corr.col2_idx < n_from)

            if is_cross:
                # Cross-table correlation
                from_label = label1 if corr.col1_idx < n_from else label2
                to_label = label2 if corr.col1_idx < n_from else label1
                is_join = (from_label[1] == join_col_from and to_label[1] == join_col_to) or (
                    from_label[1] == join_col_to and to_label[1] == join_col_from
                )

                cross_table_corrs.append(
                    CrossTableCorrelation(
                        from_table=from_label[0],
                        from_column=from_label[1],
                        to_table=to_label[0],
                        to_column=to_label[1],
                        pearson_r=corr.pearson_r,
                        spearman_rho=corr.spearman_rho,
                        strength=corr.strength,
                        is_join_column=is_join,
                    )
                )
            else:
                # Within-table correlation
                table = label1[0]
                col1, col2 = label1[1], label2[1]

                if abs(corr.pearson_r) >= redundancy_threshold:
                    # Check if likely redundant vs derived
                    # Redundant: same semantic meaning (e.g., credit_limit vs credit_limit_copy)
                    # Derived: different semantic meaning (e.g., amount vs tax)
                    redundant_pairs.append(
                        RedundantColumnPair(
                            table=table,
                            column1=col1,
                            column2=col2,
                            correlation=corr.pearson_r,
                            recommendation="Consider if one column is redundant or derived",
                        )
                    )

                    # Also add as derived candidate
                    derived_candidates.append(
                        DerivedColumnCandidate(
                            table=table,
                            derived_column=col2,
                            source_column=col1,
                            correlation=corr.pearson_r,
                            likely_formula=None,  # Could be computed with regression
                        )
                    )

        # Compute multicollinearity
        col_stds = np.std(data, axis=0)
        valid_mask = col_stds > 1e-10
        valid_indices = np.where(valid_mask)[0]
        data_valid = data[:, valid_mask]
        valid_labels = [col_labels[i] for i in valid_indices]

        dependency_groups: list[DependencyGroup] = []
        cross_table_groups: list[DependencyGroup] = []
        overall_ci = 1.0
        overall_severity: Literal["none", "moderate", "severe"] = "none"

        if data_valid.shape[1] >= 2:
            corr_matrix = np.corrcoef(data_valid, rowvar=False)
            corr_matrix = np.nan_to_num(corr_matrix, nan=0.0)

            mc_result = compute_multicollinearity(corr_matrix, vdp_threshold=0.5)
            overall_ci = mc_result.overall_condition_index
            overall_severity = mc_result.overall_severity

            for group in mc_result.dependency_groups:
                involved = [valid_labels[idx] for idx in group.involved_col_indices]
                tables_involved = {lbl[0] for lbl in involved}
                is_cross = len(tables_involved) > 1

                vdp_dict = {
                    valid_labels[idx]: vdp
                    for idx, vdp in zip(
                        group.involved_col_indices, group.variance_proportions, strict=True
                    )
                }

                dep_group = DependencyGroup(
                    columns=involved,
                    condition_index=group.condition_index,
                    severity=group.severity,
                    variance_proportions=vdp_dict,
                    is_cross_table=is_cross,
                )
                dependency_groups.append(dep_group)
                if is_cross:
                    cross_table_groups.append(dep_group)

        # Build issues list
        issues: list[QualityIssue] = []

        for rp in redundant_pairs:
            issues.append(
                QualityIssue(
                    issue_type="redundant_column",
                    severity="warning",
                    message=f"Columns {rp.column1} and {rp.column2} in {rp.table} are perfectly correlated (r={rp.correlation:.3f})",
                    affected_columns=[(rp.table, rp.column1), (rp.table, rp.column2)],
                )
            )

        # Unexpected cross-table correlations (not join columns, strong correlation)
        for ctc in cross_table_corrs:
            if not ctc.is_join_column and ctc.strength in ("strong", "very_strong"):
                issues.append(
                    QualityIssue(
                        issue_type="unexpected_correlation",
                        severity="info",
                        message=f"Unexpected correlation between {ctc.from_table}.{ctc.from_column} and {ctc.to_table}.{ctc.to_column} (r={ctc.pearson_r:.3f})",
                        affected_columns=[
                            (ctc.from_table, ctc.from_column),
                            (ctc.to_table, ctc.to_column),
                        ],
                    )
                )

        if overall_severity == "severe":
            issues.append(
                QualityIssue(
                    issue_type="multicollinearity",
                    severity="warning",
                    message=f"Severe multicollinearity detected (condition index={overall_ci:.1f})",
                    affected_columns=[],
                )
            )

        return CrossTableQualityResult(
            relationship_id=relationship.relationship_id,
            from_table=relationship.from_table,
            to_table=relationship.to_table,
            join_column_from=join_col_from,
            join_column_to=join_col_to,
            joined_row_count=total_rows,  # Report actual join size, not sample
            numeric_columns_analyzed=len(col_labels),
            cross_table_correlations=cross_table_corrs,
            redundant_columns=redundant_pairs,
            derived_columns=derived_candidates,
            overall_condition_index=overall_ci,
            overall_severity=overall_severity,
            dependency_groups=dependency_groups,
            cross_table_dependency_groups=cross_table_groups,
            issues=issues,
            analyzed_at=datetime.now(UTC),
        )

    except Exception:
        return None


def _get_numeric_columns(conn: duckdb.DuckDBPyConnection, table_path: str) -> list[str]:
    """Get numeric column names from a table."""
    result = conn.execute(f"DESCRIBE {table_path}").fetchall()
    numeric_types = {"INTEGER", "BIGINT", "DOUBLE", "FLOAT", "DECIMAL", "REAL", "HUGEINT"}
    return [row[0] for row in result if row[1].upper() in numeric_types]
