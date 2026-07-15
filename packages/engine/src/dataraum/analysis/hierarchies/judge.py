"""Dimension-identity judge (DAT-762) — the class-routed LLM lane.

Stats DECIDE, this judge vetoes or fills the gaps the statistics cannot see.
Two judgment classes ship here, one per prompt:

- ``hierarchy_veto`` — names-only UPHOLD/VETO on structures the stack-v4
  statistical pass ASSERTED, restricted by deterministic routing
  (``routing.py``) to the classes the DAT-757 channel-ablation scorecard
  measured as name-judgeable: quasi-identifier, free-text determinant,
  proxy bijection. Evidence is identifiers only — the scorecard measured
  that serving the judge statistics makes it worse on exactly these classes
  (it rationalizes near-FD numbers into upholding).
- ``dimension_conform`` — cross-fact CONFORM/ROLE/DISTINCT/ABSTAIN where no
  pairwise statistic exists (different facts share no rows). Evidence is
  names + attribute sets + authored column meanings (DAT-769); meanings
  corroborate, never auto-confirm.

The seam is producer-agnostic on purpose (the aggregation-lineage convention
veto rides it later): a producer submits evidence dicts and gets typed
verdicts back; routing tables live with the producer, never here.

Posture rules (research record, DAT-757/762): no deterministic overrides of
LLM judgments; a FAILED judgment call means the lane is skipped for that view
and stats verdicts stand — absence of judgment is not a judgment.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, ValidationError

from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.llm.config import LLMConfig
from dataraum.llm.features._base import LLMFeature
from dataraum.llm.prompts import PromptRenderer
from dataraum.llm.providers.base import (
    ConversationRequest,
    LLMProvider,
    Message,
    ToolDefinition,
)
from dataraum.llm.tool_repair import repair_tool_output

logger = get_logger(__name__)


class VetoVerdict(BaseModel):
    """One names-only verdict on an asserted structure."""

    structure_ref: str = Field(description="The structure's ref echoed back verbatim")
    verdict: Literal["uphold", "veto"]
    reason: str = Field(description="One sentence naming the class applied")


class VetoBatchOutput(BaseModel):
    """Tool output: one verdict per submitted structure."""

    verdicts: list[VetoVerdict]


class ConformVerdict(BaseModel):
    """One cross-fact dimension-identity verdict."""

    pair_ref: str = Field(description="The candidate pair's ref echoed back verbatim")
    verdict: Literal["conform", "role", "distinct", "abstain"]
    concept_label: str | None = Field(
        default=None,
        description="Canonical concept label (lowercase, singular) — required on conform",
    )
    reason: str = Field(description="One sentence of grounds")


class ConformBatchOutput(BaseModel):
    """Tool output: one verdict per submitted candidate pair."""

    verdicts: list[ConformVerdict]


class DimensionIdentityJudge(LLMFeature):
    """Forced-tool judge over the two DAT-762 judgment classes."""

    def __init__(
        self,
        config: LLMConfig,
        provider: LLMProvider,
        prompt_renderer: PromptRenderer | None = None,
    ) -> None:
        super().__init__(config, provider, prompt_renderer or PromptRenderer())

    def veto(
        self,
        *,
        table_name: str,
        all_columns: list[str],
        structures: list[dict[str, Any]],
    ) -> Result[list[VetoVerdict]]:
        """Names-only veto pass over routed structures of ONE table.

        Args:
            table_name: The fact/view the structures were asserted on.
            all_columns: The table's full column-name list (the C2 evidence).
            structures: Routed structures, each ``{ref, kind, members,
                routed_class}`` — ``members`` is the ordered column-name list.

        Returns:
            One verdict per structure, or a failed Result on an unusable
            response (the caller skips the lane, never fails the phase).
        """
        if not structures:
            return Result.ok([])
        context = {
            "table_name": table_name,
            "all_columns": ", ".join(all_columns),
            "structures": self._format_structures(structures),
        }
        result = self._judge(
            template="hierarchy_veto",
            context=context,
            tool_name="review_structures",
            tool_description=(
                "Return an UPHOLD or VETO verdict, with a one-sentence reason, "
                "for every asserted structure listed."
            ),
            output_model=VetoBatchOutput,
        )
        if not result.success:
            return Result.fail(result.error or "hierarchy_veto judgment failed")
        return Result.ok(result.unwrap().verdicts)

    def conform(self, *, candidates: list[dict[str, Any]]) -> Result[list[ConformVerdict]]:
        """Cross-fact conform/role judgment over exposure pairs.

        Args:
            candidates: Each ``{ref, left, right}`` where each side is
                ``{fact_table, key, attributes, meanings}`` — ``meanings`` maps
                column name to its authored meaning (may be empty; the prompt
                instructs the judge to abstain more readily then).

        Returns:
            One verdict per pair, or a failed Result on an unusable response.
        """
        if not candidates:
            return Result.ok([])
        context = {"candidates": self._format_candidates(candidates)}
        result = self._judge(
            template="dimension_conform",
            context=context,
            tool_name="judge_exposures",
            tool_description=(
                "Return a CONFORM, ROLE, DISTINCT, or ABSTAIN verdict, with a "
                "one-sentence reason (and a canonical concept label on conform), "
                "for every candidate exposure pair listed."
            ),
            output_model=ConformBatchOutput,
        )
        if not result.success:
            return Result.fail(result.error or "dimension_conform judgment failed")
        return Result.ok(result.unwrap().verdicts)

    def _judge[T: BaseModel](
        self,
        *,
        template: str,
        context: dict[str, Any],
        tool_name: str,
        tool_description: str,
        output_model: type[T],
    ) -> Result[T]:
        """One forced-tool judgment turn with the DAT-710 schema repair."""
        # Tier/effort from feature config (DAT-603) — an absent entry keeps the
        # defaults: balanced tier, API-default effort. `enabled` is deliberately
        # not consulted: the veto lane rides every dimension_hierarchies run
        # (the graph_sql_generation posture — not an optional feature).
        feature = self.config.features.dimension_identity_judgment
        tier = feature.model_tier if feature else "balanced"
        effort = feature.effort if feature else None
        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(template, context)
        except Exception as e:  # noqa: BLE001 — template errors are config errors
            return Result.fail(f"Failed to render {template} prompt: {e}")

        tool = ToolDefinition(
            name=tool_name,
            description=tool_description,
            input_schema=output_model.model_json_schema(),
        )
        model = self.provider.get_model_for_tier(tier)
        request = ConversationRequest(
            messages=[Message(role="user", content=user_prompt)],
            system=system_prompt,
            tools=[tool],
            tool_choice={"type": "tool", "name": tool_name},
            label=template,
            effort=effort,
            max_tokens=self.config.limits.max_output_tokens_per_request,
            temperature=temperature,
            model=model,
        )
        response = self.provider.converse(request).unwrap()
        if not response.tool_calls or response.tool_calls[0].name != tool_name:
            return Result.fail(f"LLM did not use the {tool_name} tool")
        tool_input = response.tool_calls[0].input
        try:
            return Result.ok(output_model.model_validate(tool_input))
        except ValidationError as e:
            repaired = repair_tool_output(
                self.provider,
                tool,
                tool_input,
                e,
                output_model,
                model=model,
                label=template,
                max_tokens=self.config.limits.max_output_tokens_per_request,
            )
            if not repaired.success:
                return Result.fail(f"Failed to parse {template} response: {repaired.error}")
            return Result.ok(repaired.unwrap())

    @staticmethod
    def _format_structures(structures: list[dict[str, Any]]) -> str:
        """Render routed structures as a deterministic text block."""
        lines: list[str] = []
        for s in structures:
            members = " -> ".join(str(m) for m in s["members"])
            lines.append(
                f"- ref={s['ref']} kind={s['kind']} routed_class={s['routed_class']}: {members}"
            )
        return "\n".join(lines)

    @staticmethod
    def _format_candidates(candidates: list[dict[str, Any]]) -> str:
        """Render exposure pairs as a deterministic text block."""
        blocks: list[str] = []
        for c in candidates:
            blocks.append(f"- ref={c['ref']}")
            for side_name in ("left", "right"):
                side = c[side_name]
                blocks.append(
                    f"  {side_name}: fact={side['fact_table']} key={side['key']} "
                    f"attributes=[{', '.join(side['attributes'])}]"
                )
                meanings: dict[str, str] = side.get("meanings") or {}
                for col in sorted(meanings):
                    blocks.append(f"    {col}: {meanings[col]}")
        return "\n".join(blocks)
