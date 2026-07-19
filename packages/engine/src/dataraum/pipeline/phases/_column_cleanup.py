"""Shared cleanup of run-stamped metadata that FK-references ``columns`` (DAT-506).

The metadata children of ``columns`` no longer ``ON DELETE CASCADE`` (the
torn-window cut removed every cascade). So whenever a phase deletes/replaces a
set of ``columns`` rows — column-eligibility drops, or an enriched/slicing view
replacing its prior dimension set on a re-run — it must remove the dependent
child rows itself, or the ``columns`` delete FK-violates against the prior run's
``statistical_profiles`` (and the other 13 children).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dataraum.pipeline.base import PhaseContext


def delete_column_dependents(ctx: PhaseContext, column_ids: list[str]) -> None:
    """Delete every run-stamped row that FK-references the given columns (DAT-506).

    Covers all 15 FK children of ``columns`` (verified against ``schema.sql``):
    ``type_candidates``, ``type_decisions``, ``statistical_profiles``,
    ``statistical_quality_metrics``, ``temporal_column_profiles``,
    ``semantic_annotations``, ``column_concepts`` (DAT-637 — a typed-table column
    the table agent conceptualized can later be dropped, e.g. a reconciled
    surrogate, DAT-277), ``slice_definitions``, ``entropy_objects``,
    ``entropy_readiness``, ``claim_witnesses`` (all ``column_id``-keyed), plus the
    differently-named ``derived_columns.derived_column_id``,
    ``driver_rankings.measure_column_id`` (was MISSING before DAT-778 — a
    prior run's ranking FK-blocked the column delete on the eligibility /
    surrogate-mint / enriched-views paths, which only reach this function,
    never the table-level teardown),
    ``measure_aggregation_lineage`` (reachable through ``measure_column_id`` —
    the sole column FK on the row now that the witness axis/slice ids are gone),
    and ``relationships`` (reachable
    through either ``from_column_id`` / ``to_column_id`` endpoint).

    Run BEFORE deleting the ``columns`` rows so the FK constraints are satisfied
    when the column rows go.
    """
    if not column_ids:
        return
    from sqlalchemy import delete, or_

    from dataraum.analysis.correlation.db_models import DerivedColumn
    from dataraum.analysis.drivers.db_models import DriverRankingArtifact
    from dataraum.analysis.lineage.db_models import MeasureAggregationLineage
    from dataraum.analysis.relationships.db_models import Relationship
    from dataraum.analysis.semantic.db_models import ColumnConcept, SemanticAnnotation
    from dataraum.analysis.slicing.db_models import SliceDefinition
    from dataraum.analysis.statistics.db_models import StatisticalProfile as _StatProfile
    from dataraum.analysis.statistics.quality_db_models import StatisticalQualityMetrics
    from dataraum.analysis.temporal.db_models import TemporalColumnProfile
    from dataraum.analysis.typing.db_models import TypeCandidate, TypeDecision
    from dataraum.entropy.db_models import (
        ClaimWitnessRecord,
        EntropyObjectRecord,
        EntropyReadinessRecord,
    )

    column_keyed = (
        TypeCandidate,
        TypeDecision,
        _StatProfile,
        StatisticalQualityMetrics,
        TemporalColumnProfile,
        SemanticAnnotation,
        ColumnConcept,
        SliceDefinition,
        EntropyObjectRecord,
        EntropyReadinessRecord,
        ClaimWitnessRecord,
    )
    for model in column_keyed:
        ctx.session.execute(delete(model).where(model.column_id.in_(column_ids)))
    # Differently-named column FKs.
    ctx.session.execute(
        delete(DerivedColumn).where(DerivedColumn.derived_column_id.in_(column_ids))
    )
    ctx.session.execute(
        delete(DriverRankingArtifact).where(DriverRankingArtifact.measure_column_id.in_(column_ids))
    )
    ctx.session.execute(
        delete(MeasureAggregationLineage).where(
            MeasureAggregationLineage.measure_column_id.in_(column_ids)
        )
    )
    # Relationships reach a column through either endpoint.
    ctx.session.execute(
        delete(Relationship).where(
            or_(
                Relationship.from_column_id.in_(column_ids),
                Relationship.to_column_id.in_(column_ids),
            )
        )
    )
    ctx.session.flush()
