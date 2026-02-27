"""Entropy interpretation phase implementation.

LLM-powered interpretation of entropy metrics to generate:
- Contextual assumptions about uncertain data
- Resolution actions to reduce entropy
- Human-readable explanations
"""

from __future__ import annotations

from types import ModuleType
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
from dataraum.entropy.db_models import EntropyObjectRecord
from dataraum.entropy.interpretation import TableInterpretationInput
from dataraum.entropy.interpretation_db_models import EntropyInterpretationRecord
from dataraum.llm import PromptRenderer, create_provider, load_llm_config
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.storage import Column, Table

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
        from dataraum.entropy.core.storage import EntropyRepository
        from dataraum.entropy.network.model import EntropyNetwork
        from dataraum.entropy.views.network_context import (
            EntropyForNetwork,
            _assemble_network_context,
        )

        network_ctx: EntropyForNetwork | None = None
        network = EntropyNetwork()
        repo = EntropyRepository(ctx.session)
        typed_table_ids = repo.get_typed_table_ids(table_ids)
        if typed_table_ids:
            entropy_domain_objects = repo.load_for_tables(typed_table_ids, enforce_typed=True)
            if entropy_domain_objects:
                network_ctx = _assemble_network_context(entropy_domain_objects, network)

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

        # =================================================================
        # Require Tier 1 config
        # =================================================================
        structured_config = config.features.entropy_interpretation_structured
        if not structured_config or not structured_config.enabled:
            return PhaseResult.failed(
                "entropy_interpretation_structured feature not configured"
            )

        # Initialize result accumulators
        errors: list[str] = []

        # Partition inputs: only send "interesting" columns to LLM
        from dataraum.entropy.interpretation import (
            Assumption,
            EntropyInterpretation,
            ResolutionAction,
            Tier1ColumnOutput,
            Tier1TableOutput,
        )

        # Read baseline filter config from structured feature config
        baseline_cfg: dict[str, Any] = getattr(structured_config, "baseline_filter", None) or {}
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

        logger.info(
            "baseline_filter",
            enabled=baseline_enabled,
            total=len(inputs),
            llm=len(llm_inputs),
            baseline=len(baseline_inputs),
            p_high_threshold=p_high_threshold,
        )

        # =================================================================
        # Tier 1: Column batches (Haiku, parallel)
        # =================================================================
        feature_batch_size = getattr(structured_config, "batch_size", None)
        batch_size = feature_batch_size or ctx.config.get("batch_size", 10)
        max_parallel = getattr(structured_config, "max_parallel", None)
        max_parallel = max_parallel or ctx.config.get("workers", 4)

        # Split into batches
        batches: list[tuple[int, list[InterpretationInput]]] = []
        for i in range(0, len(llm_inputs), batch_size):
            batch = llm_inputs[i : i + batch_size]
            batch_num = i // batch_size + 1
            batches.append((batch_num, batch))

        tier1_col_results: dict[str, Tier1ColumnOutput] = {}

        from concurrent.futures import ThreadPoolExecutor, as_completed

        def process_tier1_batch(
            batch_info: tuple[int, list[InterpretationInput]],
        ) -> tuple[int, dict[str, Tier1ColumnOutput] | None, str | None, int]:
            batch_num, batch = batch_info
            result = interpreter.interpret_batch_tier1(
                session=ctx.session,
                inputs=batch,
            )
            if result.success:
                return (batch_num, result.unwrap(), None, len(batch))
            else:
                return (batch_num, None, result.error, len(batch))

        with ThreadPoolExecutor(max_workers=max_parallel) as executor:
            futures = {executor.submit(process_tier1_batch, b): b[0] for b in batches}

            for future in as_completed(futures):
                batch_num = futures[future]
                try:
                    _, batch_results, error, bsize = future.result()
                    if batch_results:
                        if len(batch_results) < bsize:
                            logger.warning(
                                "tier1_batch_partial",
                                batch=batch_num,
                                returned=len(batch_results),
                                expected=bsize,
                            )
                        tier1_col_results.update(batch_results)
                    elif error:
                        errors.append(f"T1 Batch {batch_num}: {error}")
                    else:
                        logger.warning(
                            "tier1_batch_empty",
                            batch=batch_num,
                            batch_size=bsize,
                        )
                        errors.append(f"T1 Batch {batch_num}: 0/{bsize} column keys matched")
                except Exception as e:
                    errors.append(f"T1 Batch {batch_num}: {e}")

        if errors and not tier1_col_results:
            return PhaseResult.failed(
                f"All T1 column batches failed: {'; '.join(errors)}"
            )

        # =================================================================
        # Build table inputs (needed before T1 table call)
        # =================================================================

        # Build compact column interpretation summaries from T1 results
        col_interp_by_table: dict[str, list[dict[str, Any]]] = {}
        for key, col_out in tier1_col_results.items():
            if not col_out.assumptions and not col_out.resolution_actions:
                continue
            parts = key.split(".", 1)
            if len(parts) != 2:
                continue
            tbl_name = parts[0]
            col_name = parts[1]
            col_interp_by_table.setdefault(tbl_name, []).append(
                {
                    "column": col_name,
                    "top_assumption": col_out.assumptions[0].assumption_text
                    if col_out.assumptions
                    else None,
                    "top_action": col_out.resolution_actions[0].action
                    if col_out.resolution_actions
                    else None,
                }
            )

        # Load dimensional entropy objects (table-level patterns) per table
        dim_patterns_by_table: dict[str, list[dict[str, Any]]] = {}
        dim_stmt = select(EntropyObjectRecord).where(
            EntropyObjectRecord.table_id.in_(table_ids),
            EntropyObjectRecord.column_id.is_(None),
            EntropyObjectRecord.detector_id.like("dimensional_entropy%"),
        )
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
            evidence_summary = rec.evidence if rec.evidence else {}
            if isinstance(evidence_summary, list) and evidence_summary:
                evidence_summary = evidence_summary[0]
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

        # Build quality overview per table
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

        # Build table-level interpretation inputs from network context
        table_inputs: list[TableInterpretationInput] = []
        table_id_map: dict[str, str] = {}

        from dataraum.entropy.views.network_context import (
            _aggregate_intents,
            _compute_cross_column_fix,
            _readiness_from_p_high,
        )

        if network_ctx is not None:
            for table in typed_tables:
                table_prefix = f"column:{table.table_name}."
                table_col_results = {
                    target: col
                    for target, col in network_ctx.columns.items()
                    if target.startswith(table_prefix)
                }
                if not table_col_results:
                    continue

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

                table_top_fix = _compute_cross_column_fix(table_col_results, network)
                tbl_top_fix_dict: dict[str, Any] | None = None
                if table_top_fix:
                    tbl_top_fix_dict = {
                        "node": table_top_fix.node_name,
                        "columns_affected": table_top_fix.columns_affected,
                        "total_delta": round(table_top_fix.total_intent_delta, 2),
                    }

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

        # =================================================================
        # Tier 1: Table batch (Haiku, sequential after columns)
        # =================================================================
        tier1_table_results: dict[str, Tier1TableOutput] = {}
        table_interp_errors: list[str] = []

        if table_inputs:
            table_result = interpreter.interpret_tables_tier1(
                session=ctx.session, inputs=table_inputs
            )
            if table_result.success:
                tier1_table_results = table_result.unwrap()
            else:
                table_interp_errors.append(f"T1 table: {table_result.error}")

        # =================================================================
        # Tier 2: Explanation synthesis (Sonnet, single call)
        # =================================================================
        explanations: dict[str, str] = {}

        if tier1_col_results or tier1_table_results:
            t2_result = interpreter.synthesize_explanations(
                session=ctx.session,
                tier1_columns=tier1_col_results,
                tier1_tables=tier1_table_results,
                column_inputs=llm_inputs,
                table_inputs=table_inputs,
            )
            if t2_result.success:
                explanations = t2_result.unwrap()
            else:
                # T2 failure is non-fatal — structured data is preserved
                logger.warning(
                    "tier2_explanation_failed",
                    error=t2_result.error,
                    message="Using fallback explanations; structured data preserved",
                )

        # Determine model_used for auditability
        t1_model = "unknown"
        if structured_config:
            t1_model = structured_config.model_tier
        t2_model = "unknown"
        interp_cfg = config.features.entropy_interpretation
        if interp_cfg:
            t2_model = interp_cfg.model_tier
        model_used = f"tier1:{t1_model},tier2:{t2_model}"

        # =================================================================
        # Merge T1 + T2 into EntropyInterpretation objects and persist
        # =================================================================
        all_interpretations: dict[str, EntropyInterpretation] = {}
        total_assumptions = 0
        total_actions = 0
        records_created = 0
        fallback_explanation = "See assumptions and actions for details."

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

        # Merge T1 column results with T2 explanations
        inputs_by_key = {f"{i.table_name}.{i.column_name}": i for i in llm_inputs}
        for key, col_out in tier1_col_results.items():
            col_inp = inputs_by_key.get(key)
            if not col_inp:
                continue

            assumptions = [
                Assumption(
                    dimension=a.dimension,
                    assumption_text=a.assumption_text,
                    confidence=a.confidence,
                    impact=a.impact,
                    basis="inferred",
                )
                for a in col_out.assumptions
            ]
            resolution_actions = [
                ResolutionAction(
                    action=r.action,
                    description=r.description,
                    effort=r.effort,
                    expected_impact=r.expected_impact,
                    parameters=r.parameters,
                )
                for r in col_out.resolution_actions
            ]
            explanation = explanations.get(key, fallback_explanation)

            interp = EntropyInterpretation(
                column_name=col_inp.column_name,
                table_name=col_inp.table_name,
                assumptions=assumptions,
                resolution_actions=resolution_actions,
                explanation=explanation,
                model_used=model_used,
            )
            all_interpretations[key] = interp
            total_assumptions += len(assumptions)
            total_actions += len(resolution_actions)

        # Persist column-level interpretations
        for key, interp in all_interpretations.items():
            meta = column_metadata.get(key)
            if not meta:
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

        # Persist table-level interpretations
        table_interp_count = 0
        for tbl_name, tbl_out in tier1_table_results.items():
            tbl_id = table_id_map.get(tbl_name)
            if not tbl_id:
                continue

            assumptions = [
                Assumption(
                    dimension=a.dimension,
                    assumption_text=a.assumption_text,
                    confidence=a.confidence,
                    impact=a.impact,
                    basis="inferred",
                )
                for a in tbl_out.assumptions
            ]
            resolution_actions = [
                ResolutionAction(
                    action=r.action,
                    description=r.description,
                    effort=r.effort,
                    expected_impact=r.expected_impact,
                    parameters=r.parameters,
                )
                for r in tbl_out.resolution_actions
            ]
            explanation = explanations.get(tbl_name, fallback_explanation)

            assumptions_json = [
                {
                    "dimension": a.dimension,
                    "assumption_text": a.assumption_text,
                    "confidence": a.confidence,
                    "impact": a.impact,
                    "basis": a.basis,
                }
                for a in assumptions
            ]
            resolution_actions_json = [
                {
                    "action": r.action,
                    "description": r.description,
                    "effort": r.effort,
                    "expected_impact": r.expected_impact,
                    "parameters": r.parameters,
                }
                for r in resolution_actions
            ]

            interp_record = EntropyInterpretationRecord(
                source_id=ctx.source_id,
                table_id=tbl_id,
                column_id=None,
                table_name=tbl_name,
                column_name=None,
                explanation=explanation,
                assumptions_json=assumptions_json,
                resolution_actions_json=resolution_actions_json,
                model_used=model_used,
            )
            ctx.session.add(interp_record)
            records_created += 1
            table_interp_count += 1
            total_assumptions += len(assumptions)
            total_actions += len(resolution_actions)

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

        return PhaseResult.success(
            outputs=outputs,
            records_processed=total_columns,
            records_created=records_created,
            warnings=all_errors if all_errors else None,
        )
