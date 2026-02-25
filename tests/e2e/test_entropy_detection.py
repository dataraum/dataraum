"""E2E tests: verify entropy detection against ground truth.

Runs the pipeline against medium-strategy testdata (12 entropy injections)
and verifies the pipeline detects what was injected. Also tests the Bayesian
network produces elevated scores for injected entropy.

GROUND TRUTH: Do not modify assertions to fix failures — fix the production code instead.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from dataraum.core.connections import ConnectionManager
from dataraum.entropy.db_models import EntropyObjectRecord, EntropySnapshotRecord
from dataraum.pipeline.runner import RunResult

pytestmark = pytest.mark.e2e

# Mapping from testdata injection detector_id to pipeline detector_id.
# These map 1:1 (testdata uses the same detector_id values as the pipeline).
DETECTOR_ID_MAP = {
    "corrupt_types": "type_fidelity",
    "introduce_nulls": "null_ratio",
    "inject_outliers": "outlier_rate",
    "break_benford": "benford",
    "obscure_column_names": "business_meaning",
    "mix_units": "unit_entropy",
    "corrupt_dates": "temporal_entropy",
    "break_referential_integrity": "relationship_entropy",
    "drift_formula": "derived_value",
    "inject_temporal_drift": "temporal_drift",
}

# Injection types with high severity that should definitely be caught
HIGH_SEVERITY_DETECTORS = {"type_fidelity", "relationship_entropy"}


@pytest.fixture
def medium_metadata_session(
    medium_output_manager: ConnectionManager,
) -> Session:  # type: ignore[misc]
    """Fresh SQLAlchemy session for querying medium pipeline metadata."""
    with medium_output_manager.session_scope() as session:
        yield session


# =============================================================================
# Entropy detection vs ground truth
# =============================================================================


class TestEntropyDetection:
    """Verify the pipeline detects injected entropy from medium strategy."""

    def test_medium_pipeline_succeeded(self, medium_pipeline_run: RunResult) -> None:
        """Medium pipeline should complete (possibly with warnings)."""
        assert medium_pipeline_run.success, (
            f"Medium pipeline failed: "
            f"{[(p.phase_name, p.error) for p in medium_pipeline_run.get_failed_phases()]}"
        )

    def test_entropy_objects_exist(self, medium_metadata_session: Session) -> None:
        """EntropyObjectRecord entries should exist for the medium pipeline."""
        count = medium_metadata_session.execute(
            select(func.count()).select_from(EntropyObjectRecord)
        ).scalar()
        assert count is not None and count > 0, "No entropy objects found"

    def test_injected_detectors_have_entropy_objects(
        self,
        entropy_injections: list[Any],
        medium_metadata_session: Session,
    ) -> None:
        """For each unique detector_id in injections, matching EntropyObjectRecords should exist."""
        # Get unique pipeline detector_ids from injections
        injected_detector_ids = set()
        for inj in entropy_injections:
            pipeline_detector = DETECTOR_ID_MAP.get(inj.injection_type, inj.detector_id)
            injected_detector_ids.add(pipeline_detector)

        # Query detected detector_ids
        detected_ids = set(
            medium_metadata_session.execute(
                select(EntropyObjectRecord.detector_id).distinct()
            ).scalars().all()
        )

        # Check coverage — most injected detectors should be found
        found = injected_detector_ids & detected_ids
        missing = injected_detector_ids - detected_ids
        coverage = len(found) / len(injected_detector_ids) if injected_detector_ids else 0

        assert coverage >= 0.5, (
            f"Only {len(found)}/{len(injected_detector_ids)} injected detectors found. "
            f"Missing: {missing}"
        )

    def test_injected_columns_have_elevated_scores(
        self,
        entropy_injections: list[Any],
        medium_metadata_session: Session,
    ) -> None:
        """Injected (table, column) pairs should have elevated entropy scores."""
        # Build set of (table, column) from injections
        injected_pairs = set()
        for inj in entropy_injections:
            # target_file is like "journal_lines.csv" — strip extension
            table = inj.target_file.replace(".csv", "")
            injected_pairs.add((table, inj.target_column))

        # Query entropy objects with scores
        objects = medium_metadata_session.execute(
            select(EntropyObjectRecord)
        ).scalars().all()

        # Find objects matching injected pairs
        elevated = []
        for obj in objects:
            # target format: "column:{table}.{column}"
            if obj.target and obj.target.startswith("column:"):
                parts = obj.target.removeprefix("column:").split(".", 1)
                if len(parts) == 2:
                    table, col = parts
                    if (table, col) in injected_pairs and obj.score > 0.3:
                        elevated.append((table, col, obj.score))

        assert len(elevated) > 0, (
            f"No injected columns have elevated entropy scores (>0.3). "
            f"Injected pairs: {injected_pairs}"
        )

    def test_no_false_negatives_for_high_severity(
        self,
        entropy_injections: list[Any],
        medium_metadata_session: Session,
    ) -> None:
        """High-severity injections should definitely be detected."""
        # Get high-severity injection detector_ids
        high_sev_detectors = set()
        for inj in entropy_injections:
            pipeline_detector = DETECTOR_ID_MAP.get(inj.injection_type, inj.detector_id)
            if pipeline_detector in HIGH_SEVERITY_DETECTORS:
                high_sev_detectors.add(pipeline_detector)

        if not high_sev_detectors:
            pytest.skip("No high-severity injections in this run")

        detected_ids = set(
            medium_metadata_session.execute(
                select(EntropyObjectRecord.detector_id).distinct()
            ).scalars().all()
        )

        missing = high_sev_detectors - detected_ids
        assert not missing, (
            f"High-severity detectors not found in entropy objects: {missing}"
        )


# =============================================================================
# Bayesian network
# =============================================================================


class TestBayesianNetwork:
    """Verify the Bayesian network produces meaningful results with injected entropy."""

    def test_entropy_network_computed(
        self,
        medium_pipeline_run: RunResult,
        medium_metadata_session: Session,
    ) -> None:
        """EntropySnapshotRecord should exist for the medium pipeline source."""
        count = medium_metadata_session.execute(
            select(func.count())
            .select_from(EntropySnapshotRecord)
            .where(EntropySnapshotRecord.source_id == medium_pipeline_run.source_id)
        ).scalar()
        assert count is not None and count > 0, "No entropy snapshot found"

    def test_injected_entropy_elevates_scores(
        self,
        pipeline_run: RunResult,
        medium_pipeline_run: RunResult,
        output_manager: ConnectionManager,
        medium_output_manager: ConnectionManager,
    ) -> None:
        """Medium pipeline avg_entropy_score should be higher than clean pipeline."""
        # Get clean snapshot score
        with output_manager.session_scope() as session:
            clean_snapshot = session.execute(
                select(EntropySnapshotRecord).where(
                    EntropySnapshotRecord.source_id == pipeline_run.source_id
                )
            ).scalars().first()

        # Get medium snapshot score
        with medium_output_manager.session_scope() as session:
            medium_snapshot = session.execute(
                select(EntropySnapshotRecord).where(
                    EntropySnapshotRecord.source_id == medium_pipeline_run.source_id
                )
            ).scalars().first()

        assert clean_snapshot is not None, "No clean entropy snapshot"
        assert medium_snapshot is not None, "No medium entropy snapshot"

        assert medium_snapshot.avg_entropy_score > clean_snapshot.avg_entropy_score, (
            f"Medium entropy ({medium_snapshot.avg_entropy_score:.3f}) should be "
            f"higher than clean ({clean_snapshot.avg_entropy_score:.3f})"
        )

    def test_intent_nodes_reflect_problems(
        self,
        medium_pipeline_run: RunResult,
        medium_metadata_session: Session,
    ) -> None:
        """Intent leaf nodes should reflect upstream injected entropy.

        The 3 intent leaves (query_intent, aggregation_intent, reporting_intent)
        should show non-"low" states when upstream nodes have injected entropy.
        """
        snapshot = medium_metadata_session.execute(
            select(EntropySnapshotRecord).where(
                EntropySnapshotRecord.source_id == medium_pipeline_run.source_id
            )
        ).scalars().first()

        if snapshot is None or snapshot.snapshot_data is None:
            pytest.skip("No snapshot_data available to check intent nodes")

        # snapshot_data should contain node states from the Bayesian network
        node_states = snapshot.snapshot_data.get("node_states", {})
        if not node_states:
            pytest.skip("No node_states in snapshot_data")

        intent_nodes = ["query_intent", "aggregation_intent", "reporting_intent"]
        non_ready_intents = []
        for node in intent_nodes:
            state = node_states.get(node, {})
            if isinstance(state, dict):
                # Structure: {worst_p_high, mean_p_high, overall_readiness, ...}
                readiness = state.get("overall_readiness", "ready")
                if readiness != "ready":
                    non_ready_intents.append(node)

        assert len(non_ready_intents) > 0, (
            f"All intent nodes show 'ready' state despite injected entropy. "
            f"States: {[(n, node_states.get(n)) for n in intent_nodes]}"
        )
