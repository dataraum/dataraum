"""Column Annotation Agent — authoritative per-column LLM annotation.

Annotates columns with semantic roles, entity types, business terms, ontology
concept mappings, and unit sources. Post-DAT-362 this is the per-column phase's
authoritative agent, run on the capable (balanced) model — not a throwaway fast
pre-pass. Its output is persisted as ``SemanticAnnotation`` rows and later read
by ``semantic_per_table`` as read-only context.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

from sqlalchemy.orm import Session

from dataraum.analysis.semantic.concept_store import load_workspace_concepts
from dataraum.analysis.semantic.models import (
    ColumnAnnotationOutput,
    TableColumnAnnotation,
)
from dataraum.analysis.semantic.ontology import OntologyLoader
from dataraum.analysis.semantic.utils import prompt_samples
from dataraum.analysis.statistics.models import ColumnProfile
from dataraum.core.logging import get_logger
from dataraum.core.models.base import (
    Result,
)
from dataraum.llm.features._base import LLMFeature
from dataraum.llm.providers.base import (
    ConversationRequest,
    Message,
)
from dataraum.llm.structured_output import parse_structured_output

if TYPE_CHECKING:
    from dataraum.llm.config import LLMConfig
    from dataraum.llm.prompts import PromptRenderer
    from dataraum.llm.providers.base import LLMProvider

logger = get_logger(__name__)


class ColumnAnnotationAgent(LLMFeature):
    """Authoritative per-column annotation agent (DAT-362 semantic_per_column).

    Annotates columns with semantic metadata on the configured model tier
    (balanced post-split). Does NOT handle relationships or table-level entity
    classification — that is ``semantic_per_table``'s job. Output is persisted
    as ``SemanticAnnotation`` rows.
    """

    # Runaway-emission guard (DAT-889). The phase has been structured output
    # since DAT-807 — constrained decoding fixes the JSON's GRAMMAR (shape),
    # but not the LENGTH of one string/number literal, and thinking tokens
    # (when the model uses them) are unconstrained prose. Emission is
    # therefore still SAMPLED: Sonnet 5 exposes no sampling knobs at all (the
    # engine's request model carries no temperature field — DAT-889 ask #2),
    # so an identical request can legitimately runaway into a digit literal
    # that never terminates on one run and finish cleanly (~15k chars,
    # end_turn) on the next (~70k chars, stop_reason=max_tokens, tail
    # "...9999"). ``annotate`` retries a max_tokens cut-off on a REDUCED table
    # batch (halved, then per-table once a batch can no longer be split)
    # instead of raising max_tokens (a runaway just runs longer) or failing
    # the whole phase on the first bad roll.
    #
    # The call budget SCALES with the table count (``_annotation_call_budget``)
    # rather than a flat ceiling: a full binary-split retry tree over N tables
    # can cost up to ~2N-1 calls in the worst case, so a fixed small ceiling
    # would itself start failing large requests loud for reasons unrelated to
    # any persistent runaway — exactly the failure this guard exists to
    # prevent, just rarer. ``_MIN_ANNOTATION_CALLS`` is the floor for a small
    # request.
    _MIN_ANNOTATION_CALLS: ClassVar[int] = 6

    def _annotation_call_budget(self, table_count: int) -> int:
        """Bounded LLM-call budget for one ``annotate`` call (DAT-889).

        Scales with the table count so a large request isn't bounded by the
        same fixed ceiling as a small one; floored at
        ``_MIN_ANNOTATION_CALLS`` so a small request still gets a few
        retries. A real method (not an inline expression in ``annotate``) so
        a test can force a specific budget regardless of table count.
        """
        return max(self._MIN_ANNOTATION_CALLS, 2 * table_count)

    def __init__(
        self,
        config: LLMConfig,
        provider: LLMProvider,
        prompt_renderer: PromptRenderer,
        verticals_dir: Path | None = None,
    ) -> None:
        super().__init__(config, provider, prompt_renderer)
        self._ontology_loader = OntologyLoader(verticals_dir)

    def annotate(
        self,
        session: Session,
        table_ids: list[str],
        ontology: str = "general",
        profiles: list[ColumnProfile] | None = None,
        required_standard_fields: list[str] | None = None,
    ) -> Result[ColumnAnnotationOutput]:
        """Annotate columns with semantic metadata.

        Batches the request by TABLE (never by column — a table's columns are
        never split across calls) and retries on a reduced batch when a
        runaway is detected (DAT-889, see ``_annotation_call_budget``). The
        common case (no runaway) is exactly one call over every requested
        table, unchanged from before this guard.

        COVERAGE INVARIANT, enforced on TWO axes:

        1. Call-shape axis: the guard changes call granularity, never
           coverage. A max_tokens cut-off retries on reduced batches that
           partition the same requested table set, and their outputs are
           merged — nothing is sampled down because a batch got smaller.
        2. Content axis: a batch can also parse CLEANLY yet silently OMIT a
           table it was asked for — the same sampled-incompleteness class as
           a runaway, just past the parse boundary. The omitted tables are
           re-queued as a retry batch (counted against the same budget)
           rather than accepted as a thinner success.

        If any batch exhausts the call budget, the WHOLE call fails (no
        partial ``ColumnAnnotationOutput`` is ever returned as if it were
        complete) — the caller's existing failure path (``ground_columns`` →
        ``PhaseResult.failed`` → non-retryable ``PhaseFailed``) surfaces it,
        naming the tables still unannotated. A post-loop check re-verifies
        full coverage as the final guarantee before reporting success.

        Args:
            session: Database session
            table_ids: List of table IDs to annotate
            ontology: Ontology name for concept mapping
            profiles: Pre-loaded column profiles (avoids re-loading)
            required_standard_fields: Standard-field concepts required by active
                metric graphs. When provided, the prompt prioritizes mapping
                these concepts to actual dataset columns (DAT-362: this used to
                live in the tier-2 SemanticAgent; concept mapping is now owned by
                the per-column phase).

        Returns:
            Result containing ColumnAnnotationOutput. On success, ``warnings``
            carries one entry per retry (runaway OR content-omission). It is
            read by ``ground_columns`` → ``PhaseResult.warnings``, which feeds
            both the phase summary's retry-count suffix and the
            ``activity.phase_done`` / ``activity.session_phase_done`` log
            line's ``warnings`` field (DAT-889) — disclosure is not just a
            debug log a human has to go looking for.
        """
        feature_config = self.config.features.column_annotation
        if not feature_config or not feature_config.enabled:
            return Result.fail("Column annotation is disabled in config")

        # Load profiles if not provided
        if profiles is None:
            from dataraum.analysis.semantic.agent import SemanticAgent

            temp_agent = SemanticAgent.__new__(SemanticAgent)
            profiles_result = SemanticAgent._load_profiles(temp_agent, session, table_ids)
            if not profiles_result.success or not profiles_result.value:
                return Result.fail(
                    profiles_result.error if profiles_result.error else "Failed to load profiles"
                )
            profiles = profiles_result.value

        # Prepare samples once — keyed by (table_name, column_name), so it is
        # reused unchanged across every batch attempt below. Capped at the
        # configured prompt budget (DAT-890): this prompt is the pipeline's
        # most sample-dense, and serving the profiler's full top-k made it 88%
        # raw corpus bytes.
        samples = prompt_samples(
            profiles,
            limit=self.config.privacy.max_sample_values,
            max_chars=self.config.privacy.max_sample_value_chars,
        )

        # Concepts from the typed vocabulary table (DAT-728, config→DB); the
        # loader below is retained only as the prompt formatter.
        ontology_def = load_workspace_concepts(session, ontology)
        if not ontology_def.concepts:
            return Result.fail(f"Vertical '{ontology}' has no concepts to ground against.")

        ontology_concepts = self._ontology_loader.format_concepts_for_prompt(ontology_def)
        required_fields_text = self._format_required_fields(required_standard_fields)
        model = self.provider.get_model_for_tier(feature_config.model_tier)

        # Group profiles by table, preserving first-seen order — the batching
        # unit the runaway guard splits and retries over. One BATCH containing
        # every table is the pre-existing, unguarded shape.
        profiles_by_table: dict[str, list[ColumnProfile]] = {}
        for profile in profiles:
            profiles_by_table.setdefault(profile.column_ref.table_name, []).append(profile)
        all_tables = list(profiles_by_table)
        if not all_tables:
            return Result.ok(ColumnAnnotationOutput(tables=[]))

        max_calls = self._annotation_call_budget(len(all_tables))
        tables_out: list[TableColumnAnnotation] = []
        retry_notes: list[str] = []
        pending: list[list[str]] = [all_tables]
        calls_made = 0

        def _fail_with_retries(message: str) -> Result[ColumnAnnotationOutput]:
            """Fail, carrying whatever retries already ran.

            A render exception, a genuine contract break, or budget
            exhaustion each end the call — but retries already spent on
            EARLIER batches in this same call must not vanish from the
            error just because a later batch is what finally gave up.
            """
            if retry_notes:
                message = f"{message} Retries so far: {'; '.join(retry_notes)}"
            return Result.fail(message)

        while pending:
            batch = pending.pop()

            if calls_made >= max_calls:
                gap_tables = list(batch)
                for later_batch in pending:
                    gap_tables.extend(later_batch)
                return _fail_with_retries(
                    "column_annotation runaway guard exhausted "
                    f"{max_calls} LLM calls with tables {gap_tables!r} still unannotated."
                )

            batch_profiles = [p for name in batch for p in profiles_by_table[name]]
            tables_json = self._build_tables_json(batch_profiles, samples)
            context = {
                "tables_json": json.dumps(tables_json),
                "ontology_name": ontology,
                "ontology_concepts": ontology_concepts,
                "required_standard_fields": required_fields_text,
            }

            # Render prompt
            try:
                system_prompt, user_prompt = self.renderer.render_split(
                    "column_annotation", context
                )
            except Exception as e:
                return _fail_with_retries(f"Failed to render column_annotation prompt: {e}")

            # Call LLM — structured output (DAT-807): the API constrains decoding
            # to the schema, so the answer is JSON message content, not tool
            # arguments.
            #
            # `label` stays the literal "column_annotation" on every attempt —
            # it is the telemetry tag, asserted verbatim by
            # ``tests/unit/llm/test_agent_request_shape.py::test_column_annotation``
            # through a helper ~9 other feature-agent tests share.
            #
            # `dump_key` carries the attempt instead (DAT-890). A runaway retry
            # re-sends the SAME batch, so the prompt hash is identical and the
            # offline dump (``llm/prompt_log.py``, truncate-written) used to
            # overwrite the earlier attempt — destroying the very runaway
            # payload the guard exists to expose. This is why no DAT-889
            # response survives in the eval artifacts.
            request = ConversationRequest(
                messages=[Message(role="user", content=user_prompt)],
                system=system_prompt,
                output_schema=ColumnAnnotationOutput.model_json_schema(),
                label="column_annotation",
                dump_key=f"column_annotation.a{calls_made + 1:02d}",
                effort=feature_config.effort,
                max_tokens=self.config.limits.max_output_tokens_per_request,
                model=model,
            )

            # converse raises a typed ProviderError on an API failure (DAT-503) —
            # transient/permanent retryability rides the exception to the worker's
            # durable boundary, so we don't re-wrap it as a Result here. A
            # returned Result is always a success (max_tokens is a normal
            # completed API call, not a raised failure).
            response = self.provider.converse(request).unwrap()
            calls_made += 1

            parsed = parse_structured_output(
                response, ColumnAnnotationOutput, label="column_annotation"
            )
            if parsed.success:
                output = parsed.unwrap()
                tables_out.extend(output.tables)

                # Content-axis coverage (DAT-889): a parse-clean response can
                # still silently OMIT a table this batch asked for. Re-queue
                # exactly the gap — it counts against the same call budget as
                # any other retry — rather than accept a thinner success.
                returned = {t.table_name for t in output.tables}
                missing = [name for name in batch if name not in returned]
                if missing:
                    note = (
                        f"parse-clean response for batch {batch!r} omitted "
                        f"{missing!r} (attempt {calls_made}/{max_calls}) — "
                        "retrying the gap"
                    )
                    logger.warning(
                        "column_annotation_incomplete_batch_retry",
                        batch=batch,
                        missing=missing,
                        attempt=calls_made,
                        max_calls=max_calls,
                    )
                    retry_notes.append(note)
                    pending.append(missing)
                continue

            if response.stop_reason != "max_tokens":
                # A genuine contract break (bad schema, refusal, …) — not the
                # runaway this guard targets. Reducing the batch would not fix
                # it, so fail exactly as before the guard existed.
                return _fail_with_retries(parsed.error or "column_annotation failed")

            # Runaway: retry on a reduced batch (DAT-889). Halve while more
            # than one table remains; once down to a single table, retry that
            # same table (sampling — not batch size — is now the only lever
            # left, and Sonnet 5's lack of temperature means a retry is a
            # genuinely different roll).
            if len(batch) > 1:
                mid = len(batch) // 2
                left, right = batch[:mid], batch[mid:]
                note = (
                    f"runaway (stop_reason=max_tokens) on {len(batch)}-table batch "
                    f"{batch!r} (attempt {calls_made}/{max_calls}) — "
                    f"splitting into {len(left)}+{len(right)} tables and retrying"
                )
                pending.append(right)
                pending.append(left)
            else:
                note = (
                    f"runaway (stop_reason=max_tokens) on single-table batch {batch!r} "
                    f"(attempt {calls_made}/{max_calls}) — retrying "
                    "the same table"
                )
                pending.append(batch)

            logger.warning(
                "column_annotation_runaway_retry",
                batch=batch,
                stop_reason=response.stop_reason,
                output_tokens=response.output_tokens,
                content_chars=len(response.content),
                # The one field that distinguishes a digit runaway from
                # legitimately long output at a glance (DAT-889 fold-in).
                content_tail=response.content[-80:],
                attempt=calls_made,
                max_calls=max_calls,
            )
            retry_notes.append(note)

        # Post-loop safety check (DAT-889 fold-in): the final guarantee, not
        # the primary mechanism. Every path above either re-queues a gap or
        # returns Result.fail, so `pending` emptying out should already imply
        # full coverage — verify it anyway rather than trust it.
        covered = {t.table_name for t in tables_out}
        still_missing = [name for name in all_tables if name not in covered]
        if still_missing:
            return _fail_with_retries(
                f"column_annotation's retry loop ended with tables {still_missing!r} "
                "still missing from the merged output — refusing to report a "
                "thinner success."
            )

        logger.debug(
            "column_annotation_complete",
            tables=len(tables_out),
            columns=sum(len(t.columns) for t in tables_out),
            model=model,
            calls=calls_made,
            retries=len(retry_notes),
        )
        return Result.ok(ColumnAnnotationOutput(tables=tables_out), warnings=retry_notes)

    @staticmethod
    def _format_required_fields(fields: list[str] | None) -> str:
        """Format required standard fields for the prompt."""
        if not fields:
            return "No specific standard fields required by metrics."
        lines = ["The following standard_field concepts are used by active metrics:"]
        lines.extend(f"  - {f}" for f in fields)
        lines.append("")
        # DAT-769: vocabulary context only — concept binding was retired with the
        # catalogue-grain meaning redesign; nothing asks the per-column agent to
        # map columns onto concept names.
        lines.append("Use these as vocabulary context when describing columns.")
        return "\n".join(lines)

    def _build_tables_json(
        self, profiles: list[ColumnProfile], samples: dict[tuple[str, str], list[Any]]
    ) -> list[dict[str, Any]]:
        """Build JSON representation of tables for prompt."""
        tables_data: dict[str, dict[str, Any]] = {}

        for profile in profiles:
            table_name = profile.column_ref.table_name
            column_name = profile.column_ref.column_name

            if table_name not in tables_data:
                tables_data[table_name] = {
                    "table_name": table_name,
                    "row_count": profile.total_count,
                    "columns": [],
                }

            col_data: dict[str, Any] = {
                "column_name": column_name,
                "distinct_count": profile.distinct_count,
                "cardinality_ratio": round(profile.cardinality_ratio, 4),
                "sample_values": samples.get((table_name, column_name), []),
            }

            # Include original column name when it differs from normalized name
            if profile.original_name and profile.original_name != column_name:
                col_data["original_name"] = profile.original_name

            null_ratio = round(profile.null_ratio, 4)
            if null_ratio > 0.0:
                col_data["null_ratio"] = null_ratio

            if profile.numeric_stats:
                col_data["min"] = profile.numeric_stats.min_value
                col_data["max"] = profile.numeric_stats.max_value
                col_data["mean"] = round(profile.numeric_stats.mean, 4)

            if profile.string_stats:
                col_data["avg_length"] = round(profile.string_stats.avg_length, 1)

            tables_data[table_name]["columns"].append(col_data)

        return list(tables_data.values())
