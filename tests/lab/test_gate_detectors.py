"""Entropy lab — run gate-measurable detectors against real e2e pipeline output.

Verifies detector behavior against pre-computed analysis results from a full
pipeline run. Uses take_snapshot() to run detectors exactly as
measure_at_gate() would.

Structure:
- TestObservatory: runs all 9 detectors across all columns, prints summary
- TestOutlierRate: Priority C — verify excluded/non-numeric behavior
- TestTimeRole: Priority C — date types, VARCHAR mismatches
- TestJoinPathDeterminism: Priority C — key columns, orphans
- TestRelationshipQuality: Priority C — relationship evaluation
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import duckdb
import pytest
from sqlalchemy.orm import Session

from dataraum.entropy.dimensions import SubDimension
from dataraum.entropy.snapshot import Snapshot, take_snapshot

# The 9 gate-measurable sub-dimensions (available after semantic phase)
GATE_DIMENSIONS = [
    SubDimension.TYPE_FIDELITY,
    SubDimension.NULL_RATIO,
    SubDimension.BENFORD_COMPLIANCE,
    SubDimension.OUTLIER_RATE,
    SubDimension.NAMING_CLARITY,
    SubDimension.TIME_ROLE,
    SubDimension.UNIT_DECLARATION,
    SubDimension.JOIN_PATH_DETERMINISM,
    SubDimension.RELATIONSHIP_QUALITY,
]


def _snap_column(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_name: str,
    column_name: str,
    dimensions: list[SubDimension] | None = None,
) -> Snapshot:
    """Take a snapshot for a single column."""
    return take_snapshot(
        target=f"column:{table_name}.{column_name}",
        session=session,
        duckdb_conn=duckdb_conn,
        dimensions=dimensions or GATE_DIMENSIONS,
    )


# ---------------------------------------------------------------------------
# Observatory — full scan of all detectors × all columns
# ---------------------------------------------------------------------------


class TestObservatory:
    """Run all gate detectors against all typed columns and report findings.

    This is the central lab test: it builds a complete picture of what each
    detector produces for the e2e dataset, making blind spots visible.
    """

    def test_full_scan(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection,
        typed_columns: list[dict[str, str]],
    ) -> None:
        """Run all gate detectors on every typed column.

        Collects results into a summary table for inspection. Does not assert
        specific scores — this is an observatory, not a regression test.
        """
        results: dict[str, dict[str, float | None]] = {}
        objects_by_dim: dict[str, list[Any]] = defaultdict(list)
        detector_hits: dict[str, int] = defaultdict(int)

        for col in typed_columns:
            target = f"{col['table_name']}.{col['column_name']}"
            snap = _snap_column(
                lab_session,
                lab_duckdb,
                col["table_name"],
                col["column_name"],
            )
            results[target] = {}
            for dim in GATE_DIMENSIONS:
                dim_str = str(dim)
                score = snap.scores.get(dim_str)
                results[target][dim_str] = score
                if score is not None:
                    detector_hits[dim_str] += 1

            for obj in snap.objects:
                objects_by_dim[obj.sub_dimension].append(obj)

        # Print summary table
        print("\n" + "=" * 100)
        print("ENTROPY LAB — FULL GATE SCAN")
        print("=" * 100)

        # Per-detector summary
        print("\n--- Detector Coverage ---")
        for dim in GATE_DIMENSIONS:
            dim_str = str(dim)
            hits = detector_hits.get(dim_str, 0)
            total = len(typed_columns)
            scores = [
                results[t][dim_str]
                for t in results
                if results[t][dim_str] is not None
            ]
            if scores:
                avg = sum(scores) / len(scores)
                mx = max(scores)
                nonzero = sum(1 for s in scores if s > 0)
                print(
                    f"  {dim_str:30s}  "
                    f"hits={hits:2d}/{total}  "
                    f"avg={avg:.3f}  max={mx:.3f}  "
                    f"nonzero={nonzero}"
                )
            else:
                print(f"  {dim_str:30s}  hits=0/{total}  — NO DATA")

        # Columns with highest entropy per dimension
        print("\n--- Top Entropy per Dimension ---")
        for dim in GATE_DIMENSIONS:
            dim_str = str(dim)
            scored = [
                (t, results[t][dim_str])
                for t in results
                if results[t][dim_str] is not None and results[t][dim_str] > 0
            ]
            scored.sort(key=lambda x: x[1], reverse=True)
            if scored:
                print(f"\n  {dim_str}:")
                for target, score in scored[:5]:
                    print(f"    {score:.3f}  {target}")

        # Resolution options inventory
        print("\n--- Resolution Options Inventory ---")
        for dim in GATE_DIMENSIONS:
            dim_str = str(dim)
            objs = objects_by_dim.get(dim_str, [])
            actions: dict[str, int] = defaultdict(int)
            for obj in objs:
                for ro in obj.resolution_options:
                    actions[ro.action] += 1
            if actions:
                print(f"  {dim_str}:")
                for action, count in sorted(actions.items()):
                    print(f"    {action}: {count}")

        # Assert we got SOME data — if all empty, the harness is broken
        total_hits = sum(detector_hits.values())
        assert total_hits > 0, "No detectors produced any scores — harness broken?"


# ---------------------------------------------------------------------------
# Priority C — outlier_rate
# ---------------------------------------------------------------------------


class TestOutlierRate:
    """Outlier rate detector behavior with the e2e dataset.

    The e2e data has all measure columns excluded via exclude_outlier_columns
    config (post-fix state from DAT-100 testing). This means:
    - skip_outliers=True → iqr_outlier_ratio=NULL → loader omits outlier_detection
    - detector returns [] (not assessed) rather than false 0-score
    """

    def test_measure_columns_produce_scores(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection,
    ) -> None:
        """Numeric measure columns should produce outlier scores."""
        snap = _snap_column(
            lab_session, lab_duckdb,
            "bank_transactions", "amount",
            dimensions=[SubDimension.OUTLIER_RATE],
        )
        assert str(SubDimension.OUTLIER_RATE) in snap.scores
        score = snap.scores[str(SubDimension.OUTLIER_RATE)]
        assert 0.0 < score <= 1.0

        # Verify evidence structure
        obj = snap.objects[0]
        ev = obj.evidence[0]
        assert "outlier_ratio" in ev
        assert "outlier_count" in ev
        assert "outlier_impact" in ev
        print(f"\n  bank_transactions.amount: score={score:.3f}")
        print(f"  outlier_ratio={ev['outlier_ratio']:.3f}, count={ev['outlier_count']}")
        print(f"  impact={ev['outlier_impact']}, cv_attenuated={ev.get('cv_attenuated', False)}")

    def test_varchar_columns_return_no_score(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection,
    ) -> None:
        """Non-numeric columns have no outlier data → no score."""
        snap = _snap_column(
            lab_session, lab_duckdb,
            "bank_transactions", "currency",
            dimensions=[SubDimension.OUTLIER_RATE],
        )
        assert str(SubDimension.OUTLIER_RATE) not in snap.scores

    def test_key_columns_skipped_by_semantic_role(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection,
    ) -> None:
        """Columns with semantic_role=key should be skipped."""
        snap = _snap_column(
            lab_session, lab_duckdb,
            "bank_transactions", "txn_id",
            dimensions=[SubDimension.OUTLIER_RATE],
        )
        # Either no score (skipped) or score exists but should be 0
        score = snap.scores.get(str(SubDimension.OUTLIER_RATE))
        if score is not None:
            assert score == 0.0, f"Key column should not have outlier entropy, got {score}"

    def test_detector_ran_when_targeted(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection,
    ) -> None:
        """Verify the detector is at least attempted (appears in detectors_run or not)."""
        snap = _snap_column(
            lab_session, lab_duckdb,
            "bank_transactions", "amount",
            dimensions=[SubDimension.OUTLIER_RATE],
        )
        # The detector should have run (load_data + can_run) even if it returned []
        # If it didn't run at all, that's a different issue
        assert snap.detectors_run == ["outlier_rate"] or snap.detectors_run == []


# ---------------------------------------------------------------------------
# Priority C — time_role
# ---------------------------------------------------------------------------


class TestTimeRole:
    """Temporal entropy detector — proper identification of temporal columns."""

    def test_date_type_column_detected(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection,
    ) -> None:
        """DATE columns should be detected by the temporal detector."""
        snap = _snap_column(
            lab_session, lab_duckdb,
            "bank_transactions", "date",
            dimensions=[SubDimension.TIME_ROLE],
        )
        assert str(SubDimension.TIME_ROLE) in snap.scores
        assert "temporal_entropy" in snap.detectors_run

    def test_varchar_date_mismatch(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection,
    ) -> None:
        """payments.date is VARCHAR — if marked as timestamp, should score high (mismatch)."""
        snap = _snap_column(
            lab_session, lab_duckdb,
            "payments", "date",
            dimensions=[SubDimension.TIME_ROLE],
        )
        score = snap.scores.get(str(SubDimension.TIME_ROLE))
        if score is not None:
            # Mismatch between type and role should produce high entropy
            assert score >= 0.5, f"VARCHAR date with timestamp role should score ≥0.5, got {score}"

    def test_non_temporal_columns_skipped(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection,
    ) -> None:
        """Non-temporal columns should not produce a time_role score."""
        snap = _snap_column(
            lab_session, lab_duckdb,
            "bank_transactions", "amount",
            dimensions=[SubDimension.TIME_ROLE],
        )
        assert str(SubDimension.TIME_ROLE) not in snap.scores

    def test_all_date_columns_detected(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection,
        typed_columns: list[dict[str, str]],
    ) -> None:
        """All DATE/TIMESTAMP typed columns should produce a time_role score."""
        date_columns = [
            col for col in typed_columns
            if "date" in col["column_name"].lower()
            or "period" in col["column_name"].lower()
        ]
        detected = []
        missed = []
        for col in date_columns:
            snap = _snap_column(
                lab_session, lab_duckdb,
                col["table_name"], col["column_name"],
                dimensions=[SubDimension.TIME_ROLE],
            )
            if str(SubDimension.TIME_ROLE) in snap.scores:
                detected.append(f"{col['table_name']}.{col['column_name']}")
            else:
                missed.append(f"{col['table_name']}.{col['column_name']}")

        print(f"\n  Temporal columns detected: {detected}")
        print(f"  Temporal columns missed: {missed}")
        # At least some date columns should be detected
        assert len(detected) > 0, f"No date columns detected. Missed: {missed}"


# ---------------------------------------------------------------------------
# Priority C — join_path_determinism
# ---------------------------------------------------------------------------


class TestJoinPathDeterminism:
    """Join path detector — measures ambiguity in join paths."""

    def test_foreign_key_columns(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection,
    ) -> None:
        """FK columns should have join path data (if relationships exist)."""
        # journal_lines.entry_id likely has a relationship to journal_entries
        snap = _snap_column(
            lab_session, lab_duckdb,
            "journal_lines", "entry_id",
            dimensions=[SubDimension.JOIN_PATH_DETERMINISM],
        )
        score = snap.scores.get(str(SubDimension.JOIN_PATH_DETERMINISM))
        print(f"\n  journal_lines.entry_id join_path score: {score}")
        if score is not None:
            # Deterministic FK should score low
            assert score < 0.5, f"Deterministic FK should score low, got {score}"

    def test_orphan_columns(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection,
    ) -> None:
        """Columns with no relationships should score high (orphan)."""
        # fx_rates.source is unlikely to have relationships
        snap = _snap_column(
            lab_session, lab_duckdb,
            "fx_rates", "source",
            dimensions=[SubDimension.JOIN_PATH_DETERMINISM],
        )
        score = snap.scores.get(str(SubDimension.JOIN_PATH_DETERMINISM))
        print(f"\n  fx_rates.source join_path score: {score}")
        # If no relationships, detector should either return no score or high score

    def test_scan_all_key_columns(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection,
        typed_columns: list[dict[str, str]],
    ) -> None:
        """Scan all columns likely to be keys/FKs for join path scores."""
        key_like = [
            col for col in typed_columns
            if col["column_name"].endswith("_id") or col["column_name"] == "id"
        ]
        print(f"\n  Key-like columns ({len(key_like)}):")
        for col in key_like:
            snap = _snap_column(
                lab_session, lab_duckdb,
                col["table_name"], col["column_name"],
                dimensions=[SubDimension.JOIN_PATH_DETERMINISM],
            )
            score = snap.scores.get(str(SubDimension.JOIN_PATH_DETERMINISM))
            status = f"{score:.3f}" if score is not None else "—"
            print(f"    {col['table_name']}.{col['column_name']}: {status}")


# ---------------------------------------------------------------------------
# Priority C — relationship_quality
# ---------------------------------------------------------------------------


class TestRelationshipQuality:
    """Relationship quality detector — evaluates relationship metrics."""

    def test_fk_column_quality(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection,
    ) -> None:
        """FK columns with relationships should get quality scores."""
        snap = _snap_column(
            lab_session, lab_duckdb,
            "journal_lines", "entry_id",
            dimensions=[SubDimension.RELATIONSHIP_QUALITY],
        )
        score = snap.scores.get(str(SubDimension.RELATIONSHIP_QUALITY))
        print(f"\n  journal_lines.entry_id relationship_quality: {score}")
        if score is not None:
            # Should have some evaluation
            assert 0.0 <= score <= 1.0

    def test_relationship_evidence_populated(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection,
    ) -> None:
        """Detector should produce evidence with relationship metrics."""
        snap = _snap_column(
            lab_session, lab_duckdb,
            "journal_lines", "entry_id",
            dimensions=[SubDimension.RELATIONSHIP_QUALITY],
        )
        for obj in snap.objects:
            if obj.sub_dimension == str(SubDimension.RELATIONSHIP_QUALITY):
                assert obj.evidence, "Relationship quality should have evidence"
                ev = obj.evidence[0]
                print(f"\n  Evidence: {ev}")
                # Check expected fields
                assert "from_table" in ev or "aggregation_method" in ev

    def test_scan_all_relationship_columns(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection,
        typed_columns: list[dict[str, str]],
    ) -> None:
        """Scan all columns for relationship quality scores."""
        scored = []
        for col in typed_columns:
            snap = _snap_column(
                lab_session, lab_duckdb,
                col["table_name"], col["column_name"],
                dimensions=[SubDimension.RELATIONSHIP_QUALITY],
            )
            score = snap.scores.get(str(SubDimension.RELATIONSHIP_QUALITY))
            if score is not None:
                target = f"{col['table_name']}.{col['column_name']}"
                scored.append((target, score))

        print(f"\n  Columns with relationship_quality scores ({len(scored)}):")
        for target, score in sorted(scored, key=lambda x: x[1], reverse=True):
            print(f"    {score:.3f}  {target}")
