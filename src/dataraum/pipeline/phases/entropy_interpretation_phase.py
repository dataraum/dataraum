"""Entropy interpretation phase implementation.

LLM-powered interpretation of entropy metrics to generate:
- Contextual assumptions about uncertain data
- Resolution actions to reduce entropy
- Human-readable explanations
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import or_, select

from dataraum.analysis.quality_summary.db_models import ColumnQualityReport
from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.analysis.typing.db_models import TypeDecision
from dataraum.core.logging import get_logger
from dataraum.entropy import (
    EntropyInterpreter,
    InterpretationInput,
)
from dataraum.entropy.analysis.aggregator import ColumnSummary, TableSummary
from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.db_models import EntropyInterpretationRecord, EntropyObjectRecord
from dataraum.entropy.interpretation import TableInterpretationInput
from dataraum.llm import PromptRenderer, create_provider, load_llm_config
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Column, Table

logger = get_logger(__name__)


@analysis_phase
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

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if no entropy records exist or all columns already have interpretations."""
        from sqlalchemy import func

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

        # Count columns with entropy records (distinct)
        columns_with_entropy_stmt = select(
            func.count(func.distinct(EntropyObjectRecord.column_id))
        ).where(EntropyObjectRecord.table_id.in_(table_ids))
        columns_with_entropy = (ctx.session.execute(columns_with_entropy_stmt)).scalar() or 0

        if columns_with_entropy == 0:
            return "No columns with entropy records"

        # Count column-level interpretations (column_id IS NOT NULL, model_used IS NOT NULL)
        # This excludes stale table-level records from removed DimensionalSummaryAgent
        interp_stmt = select(func.count(EntropyInterpretationRecord.interpretation_id)).where(
            EntropyInterpretationRecord.table_id.in_(table_ids),
            EntropyInterpretationRecord.column_id.isnot(None),
            EntropyInterpretationRecord.model_used.isnot(None),
        )
        interp_count = (ctx.session.execute(interp_stmt)).scalar() or 0

        if interp_count >= columns_with_entropy:
            return "All columns already have entropy interpretations"

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
        # Include both column-level records AND table-level records (column_id is NULL)
        entropy_stmt = select(EntropyObjectRecord).where(
            or_(
                EntropyObjectRecord.column_id.in_(column_ids),
                # Include table-level entropy records (e.g., dimensional entropy)
                EntropyObjectRecord.table_id.in_(table_ids)
                & EntropyObjectRecord.column_id.is_(None),
            )
        )
        entropy_records = (ctx.session.execute(entropy_stmt)).scalars().all()

        # Separate table-level entropy records (for later distribution to columns)
        table_level_records: list[EntropyObjectRecord] = []

        # Group entropy records by column
        entropy_by_column: dict[str, list[EntropyObjectRecord]] = {}
        for record in entropy_records:
            if record.column_id is None:
                # Table-level record - store for later distribution
                table_level_records.append(record)
                continue
            if record.column_id not in entropy_by_column:
                entropy_by_column[record.column_id] = []
            entropy_by_column[record.column_id].append(record)

        # Distribute table-level entropy records to all columns of that table
        # This allows dimensional entropy (table-level) to be included in column interpretations
        for record in table_level_records:
            for col in all_columns:
                if col.table_id == record.table_id:
                    if col.column_id not in entropy_by_column:
                        entropy_by_column[col.column_id] = []
                    entropy_by_column[col.column_id].append(record)

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

        # Load quality reports for enriching column-level interpretation
        # Aggregate per source_column_id: best grade + top findings
        quality_by_column: dict[str, dict[str, Any]] = {}
        qr_stmt = select(ColumnQualityReport).where(
            ColumnQualityReport.source_column_id.in_(column_ids)
        )
        for qr in ctx.session.execute(qr_stmt).scalars().all():
            col_id = qr.source_column_id
            if col_id not in quality_by_column:
                quality_by_column[col_id] = {
                    "grades": [],
                    "findings": [],
                }
            quality_by_column[col_id]["grades"].append(qr.quality_grade)
            data = qr.report_data or {}
            quality_by_column[col_id]["findings"].extend(data.get("key_findings", []))

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
        renderer = PromptRenderer()

        # Create interpreter
        interpreter = EntropyInterpreter(
            config=config,
            provider=provider,
            prompt_renderer=renderer,
        )

        # Build interpretation inputs
        inputs: list[InterpretationInput] = []
        # Track column metadata for persistence
        column_metadata: dict[str, dict[str, str]] = {}

        for column_id, entropy_records in entropy_by_column.items():
            col_or_none = column_map.get(column_id)
            if not col_or_none:
                continue
            col = col_or_none

            table_or_none = table_map.get(col.table_id)
            if not table_or_none:
                continue
            table = table_or_none

            # Build ColumnSummary from records
            # Aggregate scores by layer
            layer_scores_raw: dict[str, list[float]] = {
                "structural": [],
                "semantic": [],
                "value": [],
                "computational": [],
            }
            dimension_scores: dict[str, float] = {}

            for record in entropy_records:
                if record.layer in layer_scores_raw:
                    layer_scores_raw[record.layer].append(record.score)
                # Construct dimension path from layer.dimension.sub_dimension
                dim_path = f"{record.layer}.{record.dimension}.{record.sub_dimension}"
                dimension_scores[dim_path] = record.score

            # Calculate layer averages
            layer_scores: dict[str, float] = {}
            for layer in ["structural", "semantic", "value", "computational"]:
                if layer_scores_raw[layer]:
                    layer_scores[layer] = sum(layer_scores_raw[layer]) / len(
                        layer_scores_raw[layer]
                    )
                else:
                    layer_scores[layer] = 0.0

            # Calculate composite score using config weights
            entropy_config = get_entropy_config()
            weights = entropy_config.composite_weights
            composite_score = (
                layer_scores.get("structural", 0.0) * weights["structural"]
                + layer_scores.get("semantic", 0.0) * weights["semantic"]
                + layer_scores.get("value", 0.0) * weights["value"]
                + layer_scores.get("computational", 0.0) * weights["computational"]
            )

            # Determine readiness
            readiness = entropy_config.get_readiness(composite_score)

            # Identify high-entropy dimensions
            high_threshold = entropy_config.high_entropy_threshold
            high_entropy_dimensions = [
                dim for dim, score in dimension_scores.items() if score >= high_threshold
            ]

            summary = ColumnSummary(
                column_id=column_id,
                column_name=col.column_name,
                table_id=col.table_id,
                table_name=table.table_name,
                composite_score=composite_score,
                readiness=readiness,
                layer_scores=layer_scores,
                dimension_scores=dimension_scores,
                high_entropy_dimensions=high_entropy_dimensions,
            )

            # Get additional context
            detected_type = "unknown"
            if column_id in type_decisions:
                detected_type = type_decisions[column_id].decided_type or "unknown"

            business_description = None
            if column_id in semantic_annotations:
                business_description = semantic_annotations[column_id].business_description

            # Extract quality context if available
            quality_grade = None
            quality_findings = None
            if column_id in quality_by_column:
                qc = quality_by_column[column_id]
                # Use worst grade as representative
                grade_order = {"F": 0, "D": 1, "C": 2, "B": 3, "A": 4}
                grades = qc["grades"]
                if grades:
                    quality_grade = min(grades, key=lambda g: grade_order.get(g, 5))
                # Take top 3 unique findings
                seen: set[str] = set()
                unique_findings: list[str] = []
                for f in qc["findings"]:
                    if f not in seen:
                        seen.add(f)
                        unique_findings.append(f)
                        if len(unique_findings) >= 3:
                            break
                if unique_findings:
                    quality_findings = unique_findings

            # Create interpretation input
            input_item = InterpretationInput.from_summary(
                summary=summary,
                detected_type=detected_type,
                business_description=business_description,
                quality_grade=quality_grade,
                quality_findings=quality_findings,
            )
            inputs.append(input_item)

            # Track metadata for persistence
            key = f"{table.table_name}.{col.column_name}"
            column_metadata[key] = {
                "column_id": column_id,
                "table_id": col.table_id,
                "source_id": ctx.source_id,
            }

        # Get batch size from LLM feature config or pipeline config
        feature_batch_size = getattr(config.features.entropy_interpretation, "batch_size", None)
        batch_size = feature_batch_size or ctx.config.get("interpretation_batch_size", 10)

        # Get max parallel batches (default 4 for rate limiting)
        max_parallel = getattr(config.features.entropy_interpretation, "max_parallel", None)
        max_parallel = max_parallel or ctx.config.get("interpretation_max_parallel", 4)

        # Split inputs into batches
        batches = []
        for i in range(0, len(inputs), batch_size):
            batch = inputs[i : i + batch_size]
            batch_num = i // batch_size + 1
            batches.append((batch_num, batch))

        # Process batches in parallel
        from concurrent.futures import ThreadPoolExecutor, as_completed

        all_interpretations = {}
        total_assumptions = 0
        total_actions = 0
        errors = []

        def process_batch(
            batch_info: tuple[int, list[InterpretationInput]],
        ) -> tuple[int, dict[str, Any] | None, str | None, int]:
            """Process a single batch and return results."""
            batch_num, batch = batch_info
            result = interpreter.interpret_batch(
                session=ctx.session,
                inputs=batch,
            )
            if result.success:
                return (batch_num, result.unwrap(), None, len(batch))
            else:
                return (batch_num, None, result.error, len(batch))

        with ThreadPoolExecutor(max_workers=max_parallel) as executor:
            futures = {executor.submit(process_batch, b): b[0] for b in batches}

            for future in as_completed(futures):
                batch_num = futures[future]
                try:
                    _, batch_interpretations, error, batch_size = future.result()
                    if batch_interpretations:
                        if len(batch_interpretations) < batch_size:
                            logger.warning(
                                "interpretation_batch_partial",
                                batch=batch_num,
                                returned=len(batch_interpretations),
                                expected=batch_size,
                                matched_keys=sorted(batch_interpretations.keys()),
                            )
                        all_interpretations.update(batch_interpretations)
                        for interp in batch_interpretations.values():
                            total_assumptions += len(interp.assumptions)
                            total_actions += len(interp.resolution_actions)
                    elif error:
                        errors.append(f"Batch {batch_num}: {error}")
                    else:
                        # LLM responded successfully but no keys matched
                        logger.warning(
                            "interpretation_batch_empty",
                            batch=batch_num,
                            batch_size=batch_size,
                            message="LLM returned OK but zero column keys matched input keys",
                        )
                        errors.append(f"Batch {batch_num}: 0/{batch_size} column keys matched")
                except Exception as e:
                    errors.append(f"Batch {batch_num}: {e}")

        if errors and not all_interpretations:
            # All batches failed
            return PhaseResult.failed(f"All interpretation batches failed: {'; '.join(errors)}")

        # Persist interpretations to database (even if some batches failed)
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
            )
            ctx.session.add(interp_record)
            records_created += 1

        # =====================================================================
        # Table-level interpretation
        # =====================================================================
        table_interp_count = 0
        table_interp_errors: list[str] = []

        # Build compact column interpretation summaries from column-level results
        col_interp_by_table: dict[str, list[dict[str, Any]]] = {}
        for _key, interp in all_interpretations.items():
            tbl = interp.table_name
            col_interp_by_table.setdefault(tbl, []).append(
                {
                    "column": interp.column_name,
                    "readiness": interp.readiness,
                    "top_assumption": interp.assumptions[0].assumption_text
                    if interp.assumptions
                    else None,
                    "top_action": interp.resolution_actions[0].action
                    if interp.resolution_actions
                    else None,
                }
            )

        # Load dimensional entropy objects (table-level patterns) per table
        # We load cross_column_patterns (individual pattern details) and the
        # summary record. The individual patterns contain large raw_evidence
        # blobs that can overflow token limits, so we compact them to keep
        # only the semantically useful fields.
        dim_patterns_by_table: dict[str, list[dict[str, Any]]] = {}
        dim_stmt = select(EntropyObjectRecord).where(
            EntropyObjectRecord.table_id.in_(table_ids),
            EntropyObjectRecord.column_id.is_(None),
            EntropyObjectRecord.detector_id.like("dimensional_entropy%"),
        )
        # Fields to keep from cross_column_patterns evidence (drop raw_evidence)
        _COMPACT_EVIDENCE_KEYS = {
            "pattern_type",
            "columns",
            "confidence",
            "description",
            "business_rule_hypothesis",
            "uncertainty_bits",
        }
        for rec in ctx.session.execute(dim_stmt).scalars().all():
            tbl_id = rec.table_id
            if not tbl_id:
                continue
            tbl = table_map.get(tbl_id)
            if not tbl:
                continue
            tbl_name = tbl.table_name
            # Extract first evidence entry: evidence is stored as a list of dicts in JSON
            evidence_summary = rec.evidence if rec.evidence else {}
            if isinstance(evidence_summary, list) and evidence_summary:
                evidence_summary = evidence_summary[0]
            # Compact cross_column_patterns evidence: strip raw_evidence to reduce tokens
            if isinstance(evidence_summary, dict) and rec.sub_dimension == "cross_column_patterns":
                evidence_summary = {
                    k: v for k, v in evidence_summary.items() if k in _COMPACT_EVIDENCE_KEYS
                }
            dim_patterns_by_table.setdefault(tbl_name, []).append(
                {
                    "detector_id": rec.detector_id,
                    "sub_dimension": rec.sub_dimension,
                    "score": round(rec.score, 3),
                    "evidence": evidence_summary,
                }
            )

        # Build quality overview per table from quality_by_column
        quality_overview_by_table: dict[str, dict[str, Any]] = {}
        for col_id, qc in quality_by_column.items():
            qc_col = column_map.get(col_id)
            if not qc_col:
                continue
            qc_tbl = table_map.get(qc_col.table_id)
            if not qc_tbl:
                continue
            tbl_name = qc_tbl.table_name
            if tbl_name not in quality_overview_by_table:
                quality_overview_by_table[tbl_name] = {"grade_counts": {}, "total": 0}
            overview = quality_overview_by_table[tbl_name]
            for g in qc["grades"]:
                overview["grade_counts"][g] = overview["grade_counts"].get(g, 0) + 1
                overview["total"] += 1

        # Build TableSummary objects from column summaries we already computed
        # Group column summaries by table
        summaries_by_table: dict[str, list[ColumnSummary]] = {}
        for inp in inputs:
            summaries_by_table.setdefault(inp.table_name, []).append(
                ColumnSummary(
                    column_id="",  # Not needed for table summary
                    column_name=inp.column_name,
                    table_id="",
                    table_name=inp.table_name,
                    composite_score=inp.composite_score,
                    readiness=inp.readiness,
                    layer_scores={
                        "structural": inp.structural_entropy,
                        "semantic": inp.semantic_entropy,
                        "value": inp.value_entropy,
                        "computational": inp.computational_entropy,
                    },
                    dimension_scores=inp.dimension_scores,
                    high_entropy_dimensions=inp.high_entropy_dimensions,
                    compound_risks=inp.compound_risks,
                )
            )

        table_inputs: list[TableInterpretationInput] = []
        table_id_map: dict[str, str] = {}  # table_name -> table_id

        for table in typed_tables:
            col_summaries = summaries_by_table.get(table.table_name, [])
            if not col_summaries:
                continue

            # Build a TableSummary for this table
            avg_composite = sum(c.composite_score for c in col_summaries) / len(col_summaries)
            max_composite = max(c.composite_score for c in col_summaries)

            avg_layers: dict[str, float] = {}
            for layer in ["structural", "semantic", "value", "computational"]:
                vals = [c.layer_scores.get(layer, 0.0) for c in col_summaries]
                avg_layers[layer] = sum(vals) / len(vals) if vals else 0.0

            entropy_config = get_entropy_config()
            table_readiness = entropy_config.get_readiness(avg_composite)

            high_entropy_cols = [
                c.column_name
                for c in col_summaries
                if entropy_config.is_high_entropy(c.composite_score)
            ]
            blocked_cols = [
                c.column_name
                for c in col_summaries
                if entropy_config.is_critical_entropy(c.composite_score)
            ]

            # Collect compound risks across columns
            all_risks = []
            for c in col_summaries:
                all_risks.extend(c.compound_risks)

            table_summary = TableSummary(
                table_id=table.table_id,
                table_name=table.table_name,
                columns=col_summaries,
                avg_composite_score=avg_composite,
                max_composite_score=max_composite,
                avg_layer_scores=avg_layers,
                readiness=table_readiness,
                high_entropy_columns=high_entropy_cols,
                blocked_columns=blocked_cols,
                compound_risks=all_risks,
            )

            table_input = TableInterpretationInput.from_summary(table_summary)

            # Enrich with column interpretation summaries, dimensional patterns, quality
            table_input.column_interpretations_summary = col_interp_by_table.get(table.table_name)
            table_input.dimensional_patterns = dim_patterns_by_table.get(table.table_name)
            table_input.quality_overview = quality_overview_by_table.get(table.table_name)

            table_inputs.append(table_input)
            table_id_map[table.table_name] = table.table_id

        if table_inputs:
            table_result = interpreter.interpret_tables(session=ctx.session, inputs=table_inputs)
            if table_result.success:
                for tbl_name, interp in table_result.unwrap().items():
                    tbl_id = table_id_map.get(tbl_name)
                    if not tbl_id:
                        continue

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
                        source_id=ctx.source_id,
                        table_id=tbl_id,
                        column_id=None,
                        table_name=interp.table_name,
                        column_name=None,
                        composite_score=interp.composite_score,
                        readiness=interp.readiness,
                        explanation=interp.explanation,
                        assumptions_json=assumptions_json,
                        resolution_actions_json=resolution_actions_json,
                        model_used=interp.model_used,
                    )
                    ctx.session.add(interp_record)
                    records_created += 1
                    table_interp_count += 1
            else:
                table_interp_errors.append(f"Table interpretation: {table_result.error}")

        all_errors = errors + table_interp_errors
        outputs: dict[str, int | list[str]] = {
            "interpretations": len(all_interpretations) + table_interp_count,
            "assumptions": total_assumptions,
            "resolution_actions": total_actions,
            "columns_interpreted": len(inputs),
            "tables_interpreted": table_interp_count,
        }
        if all_errors:
            outputs["batch_errors"] = all_errors

        return PhaseResult.success(
            outputs=outputs,
            records_processed=len(inputs),
            records_created=records_created,
            warnings=all_errors if all_errors else None,
        )
