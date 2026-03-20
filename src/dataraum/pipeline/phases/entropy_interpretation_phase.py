"""Entropy interpretation phase implementation.

LLM-powered interpretation of entropy metrics to generate:
- Contextual assumptions about uncertain data
- Resolution actions to reduce entropy
- Human-readable explanations
"""

from __future__ import annotations

from collections.abc import Sequence
from types import ModuleType
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, or_, select

from dataraum.analysis.quality_summary.db_models import ColumnQualityReport
from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.analysis.typing.db_models import TypeDecision
from dataraum.core.logging import get_logger
from dataraum.entropy import (
    EntropyInterpreter,
    InterpretationInput,
)
from dataraum.entropy.db_models import EntropyObjectRecord
from dataraum.entropy.interpretation import TableInterpretationInput
from dataraum.entropy.interpretation_db_models import EntropyInterpretationRecord
from dataraum.llm import PromptRenderer, create_provider, load_llm_config
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.cleanup import exec_delete
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Column, Table

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = get_logger(__name__)

# Defaults used when baseline_filter config is absent or incomplete
_DEFAULT_P_HIGH_THRESHOLD = 0.35
_DEFAULT_POOR_QUALITY_GRADES = {"C", "D", "F"}


def _is_interesting(
    inp: InterpretationInput,
    p_high_threshold: float = _DEFAULT_P_HIGH_THRESHOLD,
    poor_quality_grades: set[str] = _DEFAULT_POOR_QUALITY_GRADES,
) -> bool:
    """Return True if a column has signals worth sending to the LLM.

    A column is interesting if ANY of these hold:
    1. Has high-impact nodes (nodes with state != "low")
    2. Worst intent P(high) exceeds p_high_threshold
    3. Quality grade is in poor_quality_grades
    4. Has quality findings
    """
    na = inp.network_analysis
    if na:
        if na.get("high_impact_nodes"):
            return True
        intents = na.get("intents", {})
        if intents and max(i.get("p_high", 0) for i in intents.values()) > p_high_threshold:
            return True
    if inp.quality_grade and inp.quality_grade in poor_quality_grades:
        return True
    if inp.quality_findings:
        return True
    return False


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

    def cleanup(
        self,
        session: Session,
        source_id: str,
        table_ids: list[str],
        column_ids: list[str],
    ) -> int:
        from dataraum.entropy.interpretation_db_models import EntropyInterpretationRecord

        return exec_delete(
            session,
            delete(EntropyInterpretationRecord).where(
                EntropyInterpretationRecord.source_id == source_id
            ),
        )

    @property
    def db_models(self) -> list[ModuleType]:
        from dataraum.entropy import interpretation_db_models

        return [interpretation_db_models]

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
        # Delete stale interpretations to ensure idempotency on re-runs.
        self.cleanup(ctx.session, ctx.source_id, [], [])

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

        # --- Run quality-dependent detectors ---
        # ColumnQualityDetector and DimensionalEntropyDetector need data from
        # quality_summary (ColumnQualityReport, ColumnSliceProfile). They were
        # skipped during the entropy phase because that data didn't exist yet.
        # Run them now and persist the new EntropyObjectRecords.
        self._run_quality_dependent_detectors(ctx, typed_tables, table_map)

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

        # Load quality reports for enriching column-level interpretation.
        # Reports are keyed by slicing_view column IDs (source_column_id),
        # but interpretations work on typed table columns.  Resolve via the
        # report's denormalized (table_name, column_name) instead of IDs.
        # Key: "typed_table.column_name" → {grades, findings}
        quality_by_key: dict[str, dict[str, Any]] = {}
        qr_all_stmt = select(ColumnQualityReport).where(
            ColumnQualityReport.source_column_id.in_(
                select(Column.column_id).where(
                    Column.table_id.in_(
                        select(Table.table_id).where(
                            Table.source_id == ctx.source_id,
                        )
                    )
                )
            )
        )
        for qr in ctx.session.execute(qr_all_stmt).scalars().all():
            # Derive typed table name: "slicing_journal_lines" → "journal_lines"
            typed_name = qr.source_table_name
            if typed_name.startswith("slicing_"):
                typed_name = typed_name[len("slicing_") :]
            key = f"{typed_name}.{qr.column_name}"
            if key not in quality_by_key:
                quality_by_key[key] = {
                    "grades": [],
                    "findings": [],
                }
            quality_by_key[key]["grades"].append(qr.quality_grade)
            data = qr.report_data or {}
            quality_by_key[key]["findings"].extend(data.get("key_findings", []))

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

        for column_id, _entropy_records in entropy_by_column.items():
            col_or_none = column_map.get(column_id)
            if not col_or_none:
                continue
            col = col_or_none

            table_or_none = table_map.get(col.table_id)
            if not table_or_none:
                continue
            table = table_or_none

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
            qc_key = f"{table.table_name}.{col.column_name}"
            if qc_key in quality_by_key:
                qc = quality_by_key[qc_key]
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

            # Create interpretation input (minimal — network_analysis added later)
            input_item = InterpretationInput(
                table_name=table.table_name,
                column_name=col.column_name,
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

        # Build Bayesian network context and inject per-column analysis.
        # network_ctx is also used for table-level interpretation below.
        from dataraum.entropy.engine import compute_network
        from dataraum.entropy.views.network_context import EntropyForNetwork

        network_ctx: EntropyForNetwork | None = compute_network(ctx.session, ctx.source_id)

        if network_ctx is not None:
            # Inject per-column network_analysis into each InterpretationInput
            for inp in inputs:
                col_target = f"column:{inp.table_name}.{inp.column_name}"
                col_result = network_ctx.columns.get(col_target)
                if col_result is None:
                    logger.warning(
                        "network_analysis_missing_for_column",
                        column=f"{inp.table_name}.{inp.column_name}",
                        target=col_target,
                    )
                    continue

                # Build compact network analysis dict for prompt
                intents_dict: dict[str, dict[str, Any]] = {}
                for intent in col_result.intents:
                    intents_dict[intent.intent_name] = {
                        "p_high": round(intent.p_high, 2),
                        "readiness": intent.readiness,
                    }

                high_impact_nodes = [
                    {
                        "node": ne.node_name,
                        "state": ne.state,
                        "impact_delta": round(ne.impact_delta, 2),
                    }
                    for ne in sorted(
                        col_result.node_evidence,
                        key=lambda x: x.impact_delta,
                        reverse=True,
                    )
                    if ne.state != "low"
                ]

                top_fix_dict: dict[str, Any] | None = None
                if col_result.top_priority_node:
                    top_fix_dict = {
                        "node": col_result.top_priority_node,
                        "impact_delta": round(col_result.top_priority_impact, 2),
                    }

                inp.network_analysis = {
                    "readiness": col_result.readiness,
                    "intents": intents_dict,
                    "high_impact_nodes": high_impact_nodes,
                    "top_fix": top_fix_dict,
                }

        # Initialize result accumulators (before filtering, so baseline entries are preserved)
        all_interpretations: dict[str, Any] = {}
        total_assumptions = 0
        total_actions = 0
        errors: list[str] = []

        # Partition inputs: only send "interesting" columns to LLM
        from dataraum.entropy.interpretation import EntropyInterpretation

        # Read baseline filter config from LLM feature config
        feature_cfg = config.features.entropy_interpretation
        baseline_cfg: dict[str, Any] = getattr(feature_cfg, "baseline_filter", None) or {}
        baseline_enabled: bool = baseline_cfg.get("enabled", True)
        p_high_threshold: float = baseline_cfg.get("p_high_threshold", _DEFAULT_P_HIGH_THRESHOLD)
        poor_quality_grades: set[str] = set(
            baseline_cfg.get("poor_quality_grades", _DEFAULT_POOR_QUALITY_GRADES)
        )

        llm_inputs: list[InterpretationInput] = []
        baseline_inputs: list[InterpretationInput] = []
        if baseline_enabled:
            for inp in inputs:
                if _is_interesting(inp, p_high_threshold, poor_quality_grades):
                    llm_inputs.append(inp)
                else:
                    baseline_inputs.append(inp)
        else:
            llm_inputs = inputs

        logger.debug(
            "baseline_filter",
            enabled=baseline_enabled,
            total=len(inputs),
            llm=len(llm_inputs),
            baseline=len(baseline_inputs),
            p_high_threshold=p_high_threshold,
        )

        # Create static interpretations for baseline columns
        for inp in baseline_inputs:
            key = f"{inp.table_name}.{inp.column_name}"
            all_interpretations[key] = EntropyInterpretation(
                table_name=inp.table_name,
                column_name=inp.column_name,
                assumptions=[],
                resolution_actions=[],
                explanation="No specific concerns beyond baseline uncertainty.",
                model_used="static",
            )

        # Only send interesting columns to LLM
        inputs = llm_inputs

        # Get batch size from LLM feature config or phase config
        feature_batch_size = getattr(config.features.entropy_interpretation, "batch_size", None)
        batch_size = feature_batch_size or ctx.config.get("batch_size", 10)

        # Get max parallel batches (default 4 for rate limiting)
        max_parallel = getattr(config.features.entropy_interpretation, "max_parallel", None)
        max_parallel = max_parallel or ctx.config.get("workers", 4)

        # Split inputs into batches
        batches = []
        for i in range(0, len(inputs), batch_size):
            batch = inputs[i : i + batch_size]
            batch_num = i // batch_size + 1
            batches.append((batch_num, batch))

        # Process batches in parallel
        from concurrent.futures import ThreadPoolExecutor, as_completed

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

        # Build compact column interpretation summaries from column-level results.
        # Skip baseline columns (model_used="static") — they have no assumptions
        # or actions and just add noise to the table-level prompt.
        col_interp_by_table: dict[str, list[dict[str, Any]]] = {}
        for _key, interp in all_interpretations.items():
            if not interp.assumptions and not interp.resolution_actions:
                continue
            tbl = interp.table_name
            col_interp_by_table.setdefault(tbl, []).append(
                {
                    "column": interp.column_name,
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

        # Build quality overview per table from quality_by_key
        quality_overview_by_table: dict[str, dict[str, Any]] = {}
        for qc_key, qc in quality_by_key.items():
            tbl_name = qc_key.split(".", 1)[0]
            if tbl_name not in quality_overview_by_table:
                quality_overview_by_table[tbl_name] = {"grade_counts": {}, "total": 0}
            overview = quality_overview_by_table[tbl_name]
            for g in qc["grades"]:
                overview["grade_counts"][g] = overview["grade_counts"].get(g, 0) + 1
                overview["total"] += 1

        # Build table-level interpretation inputs from network context
        table_inputs: list[TableInterpretationInput] = []
        table_id_map: dict[str, str] = {}  # table_name -> table_id

        from dataraum.entropy.network.model import EntropyNetwork
        from dataraum.entropy.views.network_context import (
            _aggregate_intents,
            _compute_cross_column_fix,
            _readiness_from_p_high,
        )

        network = EntropyNetwork()

        if network_ctx is not None:
            for table in typed_tables:
                # Filter columns for this table from network_ctx
                table_prefix = f"column:{table.table_name}."
                table_col_results = {
                    target: col
                    for target, col in network_ctx.columns.items()
                    if target.startswith(table_prefix)
                }
                if not table_col_results:
                    continue

                # Per-column compact summaries
                column_summaries_list: list[dict[str, Any]] = []
                for target, col_result in table_col_results.items():
                    col_name = target[len(table_prefix) :]
                    col_summary: dict[str, Any] = {
                        "column": col_name,
                        "readiness": col_result.readiness,
                        "worst_p_high": round(col_result.worst_intent_p_high, 2),
                    }
                    if col_result.top_priority_node:
                        col_summary["top_fix"] = col_result.top_priority_node
                    column_summaries_list.append(col_summary)

                # Per-table intent aggregation
                table_intents = _aggregate_intents(table_col_results)
                tbl_intents_dict: dict[str, dict[str, Any]] = {}
                for ai in table_intents:
                    tbl_intents_dict[ai.intent_name] = {
                        "worst_p_high": round(ai.worst_p_high, 2),
                        "mean_p_high": round(ai.mean_p_high, 2),
                        "columns_blocked": ai.columns_blocked,
                        "columns_investigate": ai.columns_investigate,
                        "columns_ready": ai.columns_ready,
                        "readiness": ai.overall_readiness,
                    }

                # Per-table top fix
                table_top_fix = _compute_cross_column_fix(table_col_results, network)
                tbl_top_fix_dict: dict[str, Any] | None = None
                if table_top_fix:
                    tbl_top_fix_dict = {
                        "node": table_top_fix.node_name,
                        "columns_affected": table_top_fix.columns_affected,
                        "total_delta": round(table_top_fix.total_intent_delta, 2),
                    }

                # Overall table readiness from worst intent
                table_readiness = "ready"
                if table_intents:
                    worst_p = max(ai.worst_p_high for ai in table_intents)
                    table_readiness = _readiness_from_p_high(worst_p)

                table_network_analysis: dict[str, Any] = {
                    "readiness": table_readiness,
                    "intents": tbl_intents_dict,
                    "columns": column_summaries_list,
                    "top_fix": tbl_top_fix_dict,
                }

                table_input = TableInterpretationInput(
                    table_name=table.table_name,
                    column_count=len(table_col_results),
                    network_analysis=table_network_analysis,
                    column_interpretations_summary=col_interp_by_table.get(table.table_name),
                    dimensional_patterns=dim_patterns_by_table.get(table.table_name),
                    quality_overview=quality_overview_by_table.get(table.table_name),
                )
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
        total_columns = len(llm_inputs) + len(baseline_inputs)
        outputs: dict[str, int | list[str]] = {
            "interpretations": len(all_interpretations) + table_interp_count,
            "assumptions": total_assumptions,
            "resolution_actions": total_actions,
            "columns_interpreted": total_columns,
            "columns_llm": len(llm_inputs),
            "columns_baseline": len(baseline_inputs),
            "tables_interpreted": table_interp_count,
        }
        if all_errors:
            outputs["batch_errors"] = all_errors

        interp_count = len(all_interpretations) + table_interp_count
        return PhaseResult.success(
            outputs=outputs,
            records_processed=total_columns,
            records_created=records_created,
            warnings=all_errors if all_errors else None,
            summary=f"{interp_count} interpretations, {total_actions} resolution actions",
        )

    def _run_quality_dependent_detectors(
        self,
        ctx: PhaseContext,
        typed_tables: Sequence[Any],
        table_map: dict[str, Any],
    ) -> int:
        """Run table-scoped detectors that were skipped during the entropy phase.

        Detectors like ColumnQualityDetector and DimensionalEntropyDetector
        require data from quality_summary. They fail can_run() during the
        entropy phase and are silently skipped. Now that quality_summary
        has run, re-run all table-scoped detectors — can_run() filters
        naturally — and persist only records from detectors that didn't
        produce records during the entropy phase.

        Returns:
            Number of new EntropyObjectRecords created.
        """
        from sqlalchemy import func

        from dataraum.entropy.engine import _extract_column_id, _make_record, persist_records
        from dataraum.entropy.snapshot import take_snapshot

        # Detector IDs that already have records from the entropy phase
        existing_ids = set(
            ctx.session.execute(
                select(func.distinct(EntropyObjectRecord.detector_id)).where(
                    EntropyObjectRecord.source_id == ctx.source_id
                )
            )
            .scalars()
            .all()
        )

        new_records: list[Any] = []
        for table in typed_tables:
            snapshot = take_snapshot(
                target=f"table:{table.table_name}",
                session=ctx.session,
            )

            for obj in snapshot.objects:
                if obj.detector_id in existing_ids:
                    continue
                record = _make_record(
                    source_id=ctx.source_id,
                    entropy_obj=obj,
                    table_id=table.table_id,
                    column_id=_extract_column_id(obj),
                )
                new_records.append(record)

        if new_records:
            persist_records(ctx.session, new_records)
            new_detector_ids = {r.detector_id for r in new_records}
            logger.info(
                "quality_dependent_detectors",
                source_id=ctx.source_id,
                records=len(new_records),
                detectors=sorted(new_detector_ids),
            )

        return len(new_records)
