"""Dimension-identity judge (DAT-762) — the two gaps the statistics cannot reach.

Stats DECIDE; this judge fills the two identity questions no statistic can
settle. Both ship here:

- ``dimension_conform`` — cross-fact CONFORM/ROLE/DISTINCT/ABSTAIN where no
  pairwise statistic exists (different facts share no rows). Evidence is
  names + attribute sets + authored column meanings (DAT-769); meanings
  corroborate, never auto-confirm.
- ``alias_identity`` — within-view, is a bijection ONE dimension (a true
  relabeling, code↔name) or a COINCIDENTAL 1:1 (two different attributes that
  merely line up on these rows — an entity key and a per-row timestamp)? A
  perfect bijection is statistically indistinguishable from a coincidental one
  (g3 = 0, λ = 1, both survive the permutation null), so only meaning separates
  them. Returns a calibrated confidence; the finder merges the confident ones
  and surfaces the rest as ``needs_confirmation`` — a coincidental merge would
  collapse two drill axes into one and corrupt every aggregation over them.

The seam is producer-agnostic on purpose: a producer submits evidence dicts
and gets typed verdicts back; candidate selection lives with the producer,
never here.

Posture rules (research record, DAT-757/762): no deterministic overrides of
LLM judgments; a FAILED judgment call means the lane is skipped and the
stats' verdicts stand — absence of judgment is not a judgment.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, ValidationError, model_validator

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


class ConformVerdict(BaseModel):
    """One cross-fact dimension-identity verdict."""

    pair_ref: str = Field(description="The candidate pair's ref echoed back verbatim")
    verdict: Literal["conform", "role", "distinct", "abstain"]
    concept_label: str | None = Field(
        default=None,
        description="Canonical concept label (lowercase, singular) — required on conform",
    )
    reason: str = Field(description="One sentence of grounds")

    @model_validator(mode="after")
    def _conform_requires_label(self) -> ConformVerdict:
        """A conform without its label is a MALFORMED response, not a judgment.

        Enforced at the model so the DAT-710 repair loop re-asks the judge —
        the consumer must never complete an LLM judgment field
        deterministically (a column name is not a concept label).
        """
        if self.verdict == "conform" and not self.concept_label:
            raise ValueError("concept_label is required on a conform verdict")
        return self


class ConformBatchOutput(BaseModel):
    """Tool output: one verdict per submitted candidate pair.

    Emptiness is NOT constrained here — it is checked by the caller. A schema
    ``min_length`` would route an empty batch into the DAT-710 repair turn,
    which re-prompts with the validation error and the previous output but
    WITHOUT the candidate list, so the only way to satisfy the constraint is to
    INVENT verdicts. A fabricated verdict on a guessable ref is far worse than
    the silence it would replace (DAT-725 review).
    """

    verdicts: list[ConformVerdict]


class AliasIdentityVerdict(BaseModel):
    """One within-view bijection verdict: how clearly is the pair one dimension?

    The verdict is carried by ``confidence`` alone — a directional, evidence-
    anchored number (near 1.0 a clear alias, near 0.0 a clear coincidence). There
    is no separate accept/decline flag: the finder merges iff ``confidence`` clears
    the merge floor, the relationship-judge pattern (verdict-in-confidence).
    """

    pair_ref: str = Field(description="The candidate pair's ref echoed back verbatim")
    confidence: float = Field(
        ge=0.0,
        le=1.0,
        description=(
            "Calibrated [0,1] identity confidence: how clearly the names and values "
            "show the pair to be ONE entity re-encoded — near 1.0 a clear alias, near "
            "0.0 a clear coincidence (house convention)"
        ),
    )
    reason: str = Field(description="One sentence of grounds")


class AliasIdentityBatchOutput(BaseModel):
    """Tool output: one identity verdict per submitted bijection pair.

    Emptiness is checked by the caller, never here — see
    ``ConformBatchOutput``. The stakes are higher on this batch: alias refs are
    ``str(i)`` ("0", "1", …), so a verdict invented by a context-free repair
    turn would land on a REAL pair, and one above the merge floor collapses two
    drill axes into one dimension — the exact corruption this module exists to
    prevent (DAT-725 review).
    """

    verdicts: list[AliasIdentityVerdict]


class DimensionIdentityJudge(LLMFeature):
    """Forced-tool judge over the DAT-762 cross-fact conform judgment."""

    def __init__(
        self,
        config: LLMConfig,
        provider: LLMProvider,
        prompt_renderer: PromptRenderer | None = None,
    ) -> None:
        super().__init__(config, provider, prompt_renderer or PromptRenderer())

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

    def alias_identity(
        self, *, candidates: list[dict[str, Any]]
    ) -> Result[list[AliasIdentityVerdict]]:
        """Within-view identity: is a bijection one dimension, or coincidental?

        Args:
            candidates: Each ``{ref, table, a, b, meanings}`` where ``a`` and
                ``b`` are ``{name, distinct, samples}`` (the two bijective
                columns) and ``meanings`` maps column name to its authored
                meaning (may be empty — the judge then leans on names + values).

        Returns:
            One verdict per pair, or a failed Result on an unusable response.
        """
        if not candidates:
            return Result.ok([])
        context = {"candidates": self._format_alias_candidates(candidates)}
        result = self._judge(
            template="dimension_alias",
            context=context,
            tool_name="judge_aliases",
            tool_description=(
                "Return a calibrated [0,1] identity confidence and a one-sentence "
                "reason for every candidate bijection pair — high when the names and "
                "values plainly show one entity re-encoded, low when they are "
                "different attributes that merely align 1:1."
            ),
            output_model=AliasIdentityBatchOutput,
        )
        if not result.success:
            return Result.fail(result.error or "dimension_alias judgment failed")
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
        # not consulted: the conform lane rides every dimension_hierarchies run
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

    @staticmethod
    def _format_alias_candidates(candidates: list[dict[str, Any]]) -> str:
        """Render within-view bijection pairs as a deterministic text block."""
        blocks: list[str] = []
        for c in candidates:
            blocks.append(f"- ref={c['ref']} table={c['table']}")
            for side_name in ("a", "b"):
                side = c[side_name]
                samples = ", ".join(str(v) for v in side.get("samples", []))
                blocks.append(
                    f"    {side_name}: {side['name']} — {side['distinct']:,} distinct"
                    + (f", e.g. {samples}" if samples else "")
                )
            meanings: dict[str, str] = c.get("meanings") or {}
            for col in sorted(meanings):
                blocks.append(f"      meaning[{col}]: {meanings[col]}")
        return "\n".join(blocks)
