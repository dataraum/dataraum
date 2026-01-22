"""Entropy interpretation phase implementation.

LLM-powered interpretation of entropy metrics to generate:
- Contextual assumptions about uncertain data
- Resolution actions to reduce entropy
- Human-readable explanations
"""

from __future__ import annotations

from sqlalchemy import select

from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.analysis.typing.db_models import TypeDecision
from dataraum.entropy import (
    EntropyInterpreter,
    InterpretationInput,
)
from dataraum.entropy.db_models import EntropyInterpretationRecord, EntropyObjectRecord
from dataraum.entropy.models import ColumnEntropyProfile
from dataraum.llm import LLMCache, PromptRenderer, create_provider, load_llm_config
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.storage import Column, Table


class EntropyInterpretationPhase(BasePhase):
    """LLM-powered entropy interpretation phase.

    Interprets entropy metrics to generate assumptions, resolution
    actions, and explanations for uncertain data.

    Requires: entropy phase.
    """

    @property
    def name(self) -> str:
        return "entropy_interpretation"

    @property
    def description(self) -> str:
        return "LLM interpretation of entropy"

    @property
    def dependencies(self) -> list[str]:
        return ["entropy"]

    @property
    def outputs(self) -> list[str]:
        return ["interpretations", "assumptions", "resolution_actions"]

    @property
    def is_llm_phase(self) -> bool:
        return True

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if no entropy records exist."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return "No typed tables found"

        table_ids = [t.table_id for t in typed_tables]

        # Check for entropy records
        entropy_stmt = (
            select(EntropyObjectRecord).where(EntropyObjectRecord.table_id.in_(table_ids)).limit(1)
        )
        entropy_record = (ctx.session.execute(entropy_stmt)).scalar_one_or_none()

        if not entropy_record:
            return "No entropy records found"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Run entropy interpretation using LLM."""
        # Get typed tables for this source
        stmt = select(Table).where(Table.layer == "typed", Table.source_id == ctx.source_id)
        result = ctx.session.execute(stmt)
        typed_tables = result.scalars().all()

        if not typed_tables:
            return PhaseResult.failed("No typed tables found. Run typing phase first.")

        table_ids = [t.table_id for t in typed_tables]
        table_map = {t.table_id: t for t in typed_tables}

        # Get columns
        cols_stmt = select(Column).where(Column.table_id.in_(table_ids))
        all_columns = (ctx.session.execute(cols_stmt)).scalars().all()
        column_ids = [c.column_id for c in all_columns]
        column_map = {c.column_id: c for c in all_columns}

        # Load entropy records grouped by column
        entropy_stmt = select(EntropyObjectRecord).where(
            EntropyObjectRecord.column_id.in_(column_ids)
        )
        entropy_records = (ctx.session.execute(entropy_stmt)).scalars().all()

        # Group entropy records by column
        entropy_by_column: dict[str, list[EntropyObjectRecord]] = {}
        for record in entropy_records:
            if record.column_id is None:
                continue
            if record.column_id not in entropy_by_column:
                entropy_by_column[record.column_id] = []
            entropy_by_column[record.column_id].append(record)

        if not entropy_by_column:
            return PhaseResult.success(
                outputs={
                    "interpretations": 0,
                    "message": "No entropy records to interpret",
                },
                records_processed=0,
                records_created=0,
            )

        # Load type decisions
        type_decisions: dict[str, TypeDecision] = {}
        type_stmt = select(TypeDecision).where(TypeDecision.column_id.in_(column_ids))
        for td in (ctx.session.execute(type_stmt)).scalars().all():
            type_decisions[td.column_id] = td

        # Load semantic annotations
        semantic_annotations: dict[str, SemanticAnnotation] = {}
        sem_stmt = select(SemanticAnnotation).where(SemanticAnnotation.column_id.in_(column_ids))
        for ann in (ctx.session.execute(sem_stmt)).scalars().all():
            semantic_annotations[ann.column_id] = ann

        # Initialize LLM infrastructure
        try:
            config = load_llm_config()
        except FileNotFoundError as e:
            return PhaseResult.failed(f"LLM config not found: {e}")

        # Create provider
        provider_config = config.providers.get(config.active_provider)
        if not provider_config:
            return PhaseResult.failed(f"Provider '{config.active_provider}' not configured")

        try:
            provider = create_provider(config.active_provider, provider_config.model_dump())
        except Exception as e:
            return PhaseResult.failed(f"Failed to create LLM provider: {e}")

        # Create other components
        cache = LLMCache()
        renderer = PromptRenderer()

        # Create interpreter
        interpreter = EntropyInterpreter(
            config=config,
            provider=provider,
            prompt_renderer=renderer,
            cache=cache,
        )

        # Build interpretation inputs
        inputs: list[InterpretationInput] = []
        # Track column metadata for persistence
        column_metadata: dict[str, dict[str, str]] = {}

        for column_id, entropy_records in entropy_by_column.items():
            col = column_map.get(column_id)
            if not col:
                continue

            table = table_map.get(col.table_id)
            if not table:
                continue

            # Build ColumnEntropyProfile from records
            profile = ColumnEntropyProfile(
                column_id=column_id,
                column_name=col.column_name,
                table_name=table.table_name,
            )

            # Aggregate scores by layer
            layer_scores: dict[str, list[float]] = {
                "structural": [],
                "semantic": [],
                "value": [],
                "computational": [],
            }

            for record in entropy_records:
                if record.layer in layer_scores:
                    layer_scores[record.layer].append(record.score)
                # Construct dimension path from layer.dimension.sub_dimension
                dim_path = f"{record.layer}.{record.dimension}.{record.sub_dimension}"
                profile.dimension_scores[dim_path] = record.score

            # Calculate layer averages
            profile.structural_entropy = (
                sum(layer_scores["structural"]) / len(layer_scores["structural"])
                if layer_scores["structural"]
                else 0.0
            )
            profile.semantic_entropy = (
                sum(layer_scores["semantic"]) / len(layer_scores["semantic"])
                if layer_scores["semantic"]
                else 0.0
            )
            profile.value_entropy = (
                sum(layer_scores["value"]) / len(layer_scores["value"])
                if layer_scores["value"]
                else 0.0
            )
            profile.computational_entropy = (
                sum(layer_scores["computational"]) / len(layer_scores["computational"])
                if layer_scores["computational"]
                else 0.0
            )

            profile.calculate_composite()
            profile.update_readiness()

            # Get additional context
            detected_type = "unknown"
            if column_id in type_decisions:
                detected_type = type_decisions[column_id].decided_type or "unknown"

            business_description = None
            if column_id in semantic_annotations:
                business_description = semantic_annotations[column_id].business_description

            # Create interpretation input
            input_item = InterpretationInput.from_profile(
                profile=profile,
                detected_type=detected_type,
                business_description=business_description,
            )
            inputs.append(input_item)

            # Track metadata for persistence
            key = f"{table.table_name}.{col.column_name}"
            column_metadata[key] = {
                "column_id": column_id,
                "table_id": col.table_id,
                "source_id": ctx.source_id,
            }

        # Get batch size from config
        batch_size = ctx.config.get("interpretation_batch_size", 10)

        # Process in batches
        all_interpretations = {}
        total_assumptions = 0
        total_actions = 0
        errors = []

        for i in range(0, len(inputs), batch_size):
            batch = inputs[i : i + batch_size]
            batch_num = i // batch_size + 1

            interpret_result = interpreter.interpret_batch(
                session=ctx.session,
                inputs=batch,
            )

            if interpret_result.success:
                batch_interpretations = interpret_result.unwrap()
                all_interpretations.update(batch_interpretations)

                for interp in batch_interpretations.values():
                    total_assumptions += len(interp.assumptions)
                    total_actions += len(interp.resolution_actions)
            else:
                # Fail explicitly on LLM errors - no silent fallbacks
                errors.append(f"Batch {batch_num}: {interpret_result.error}")

        if errors and not all_interpretations:
            # All batches failed
            return PhaseResult.failed(f"All interpretation batches failed: {'; '.join(errors)}")

        if errors:
            # Some batches failed - report partial success with warnings
            return PhaseResult.success(
                outputs={
                    "interpretations": len(all_interpretations),
                    "assumptions": total_assumptions,
                    "resolution_actions": total_actions,
                    "columns_interpreted": len(inputs),
                    "batch_errors": errors,
                },
                records_processed=len(inputs),
                records_created=len(all_interpretations),
                warnings=errors,
            )

        # Persist interpretations to database
        records_created = 0
        for key, interp in all_interpretations.items():
            meta = column_metadata.get(key)
            if not meta:
                continue

            # Convert assumptions to JSON-serializable format
            assumptions_json = [
                {
                    "dimension": a.dimension,
                    "assumption_text": a.assumption_text,
                    "confidence": a.confidence,
                    "impact": a.impact,
                    "basis": a.basis,
                }
                for a in interp.assumptions
            ]

            # Convert resolution actions to JSON-serializable format
            resolution_actions_json = [
                {
                    "action": r.action,
                    "description": r.description,
                    "priority": r.priority,
                    "effort": r.effort,
                    "expected_impact": r.expected_impact,
                    "parameters": r.parameters,
                }
                for r in interp.resolution_actions
            ]

            interp_record = EntropyInterpretationRecord(
                source_id=meta["source_id"],
                table_id=meta["table_id"],
                column_id=meta["column_id"],
                table_name=interp.table_name,
                column_name=interp.column_name,
                composite_score=interp.composite_score,
                readiness=interp.readiness,
                explanation=interp.explanation,
                assumptions_json=assumptions_json,
                resolution_actions_json=resolution_actions_json,
                model_used=interp.model_used,
                from_cache=interp.from_cache,
            )
            ctx.session.add(interp_record)
            records_created += 1

        return PhaseResult.success(
            outputs={
                "interpretations": len(all_interpretations),
                "assumptions": total_assumptions,
                "resolution_actions": total_actions,
                "columns_interpreted": len(inputs),
            },
            records_processed=len(inputs),
            records_created=records_created,
        )
