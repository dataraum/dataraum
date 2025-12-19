"""Financial anomaly detection.

Detects financial-specific topological anomalies.
"""

from typing import Any

from dataraum_context.analysis.topology.models import TopologicalAnomaly


def detect_financial_anomalies(
    topological_result: Any,  # TopologicalQualityResult
    classified_cycles: list[dict[str, Any]],
) -> list[TopologicalAnomaly]:
    """Detect financial-specific topological anomalies.

    Anomaly types:
    - excessive_financial_cycles: Too many cycles for financial data
    - unclassified_financial_cycles: Cycles couldn't be classified
    - financial_data_fragmentation: Disconnected components
    - missing_financial_cycles: Expected cycles not found
    - cost_center_isolation: Orphaned components

    Args:
        topological_result: TopologicalQualityResult from analysis
        classified_cycles: List of LLM-classified cycle dicts

    Returns:
        List of TopologicalAnomaly objects with financial context
    """
    anomalies: list[TopologicalAnomaly] = []

    if topological_result is None:
        return anomalies

    # Get table name for affected_tables
    table_name = getattr(topological_result, "table_name", "unknown")

    # Anomaly 1: Unusual cycle complexity
    cycle_count = len(classified_cycles)
    unclassified = [c for c in classified_cycles if c.get("cycle_type") in [None, "UNKNOWN"]]

    if cycle_count > 15:
        anomalies.append(
            TopologicalAnomaly(
                anomaly_type="excessive_financial_cycles",
                severity="high",
                description=f"Unusually high number of cycles ({cycle_count}) for financial data",
                evidence={"cycle_count": cycle_count, "expected_max": 10},
                affected_tables=[table_name],
                affected_columns=[],
            )
        )

    if len(unclassified) > 5:
        anomalies.append(
            TopologicalAnomaly(
                anomaly_type="unclassified_financial_cycles",
                severity="medium",
                description=f"{len(unclassified)} cycles could not be classified",
                evidence={"unclassified_count": len(unclassified), "total_count": cycle_count},
                affected_tables=[table_name],
                affected_columns=[],
            )
        )

    # Anomaly 2: Disconnected components
    if hasattr(topological_result, "betti_numbers"):
        betti_0 = topological_result.betti_numbers.betti_0
        if betti_0 > 3:
            anomalies.append(
                TopologicalAnomaly(
                    anomaly_type="financial_data_fragmentation",
                    severity="high",
                    description=f"Financial data has {betti_0} disconnected components. Expected: 1-2",
                    evidence={
                        "component_count": betti_0,
                        "expected_max": 2,
                        "interpretation": "Accounts or entities are not properly linked",
                    },
                    affected_tables=[table_name],
                    affected_columns=[],
                )
            )

    # Anomaly 3: Missing expected financial cycles
    cycle_types = {c.get("cycle_type") for c in classified_cycles if c.get("cycle_type")}
    expected_cycles = {"accounts_receivable_cycle", "expense_cycle", "revenue_cycle"}
    missing_cycles = expected_cycles - cycle_types

    if missing_cycles and cycle_count > 0:
        anomalies.append(
            TopologicalAnomaly(
                anomaly_type="missing_financial_cycles",
                severity="medium",
                description=f"Expected financial cycles not detected: {', '.join(missing_cycles)}",
                evidence={
                    "missing_cycles": list(missing_cycles),
                    "detected_cycles": list(cycle_types),
                },
                affected_tables=[table_name],
                affected_columns=[],
            )
        )

    # Anomaly 4: Cost center isolation
    orphaned = getattr(topological_result, "orphaned_components", 0)
    if orphaned > 0:
        anomalies.append(
            TopologicalAnomaly(
                anomaly_type="cost_center_isolation",
                severity="medium",
                description=f"{orphaned} isolated components detected",
                evidence={"orphaned_count": orphaned},
                affected_tables=[table_name],
                affected_columns=[],
            )
        )

    return anomalies
