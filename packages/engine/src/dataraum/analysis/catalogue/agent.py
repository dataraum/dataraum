"""Catalogue-semantics agent — the one authoring turn per catalogue (DAT-823).

Follows the SemanticAgent pattern: extends LLMFeature, renders the
``catalogue_semantics`` prompt over the composed-catalogue evidence
(:mod:`dataraum.analysis.catalogue.context`), and constrains the output to
:class:`CatalogueSemanticsOutput`.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy.orm import Session

from dataraum.analysis.catalogue.context import build_catalogue_inputs
from dataraum.analysis.catalogue.models import CatalogueSemanticsOutput
from dataraum.analysis.semantic.concept_store import load_workspace_concepts
from dataraum.analysis.semantic.ontology import OntologyLoader
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.llm.features._base import LLMFeature
from dataraum.llm.privacy import DataSampler
from dataraum.llm.providers.base import ConversationRequest, Message
from dataraum.llm.structured_output import parse_structured_output

if TYPE_CHECKING:
    import duckdb

    from dataraum.llm.config import LLMConfig
    from dataraum.llm.prompts import PromptRenderer
    from dataraum.llm.providers.base import LLMProvider

logger = get_logger(__name__)


def _required_standard_fields(ontology: str) -> list[str]:
    """Metric-graph required fields — the same steer ``ground_columns`` serves.

    Overlay-aware (``get_metric_definitions``: shipped graphs ⊕ metric teach
    rows, DAT-471), so a framed vertical's declared metrics steer the catalogue
    authoring too. A declared metric that won't parse is skipped for this HINT —
    its born-loud handling stays the metrics phase's job.
    """
    from dataraum.graphs.config import get_metric_definitions
    from dataraum.graphs.loader import GraphLoader, GraphLoadError

    metric_loader = GraphLoader()
    for graph_id, defn in get_metric_definitions(ontology).items():
        try:
            metric_loader.graphs.update(metric_loader.graphs_from_definitions({graph_id: defn}))
        except GraphLoadError as exc:
            logger.warning("metric_catalogue_hint_skip", graph_id=graph_id, error=str(exc))
    return sorted(metric_loader.get_all_abstract_fields())


class CatalogueSemanticsAgent(LLMFeature):
    """One authoring turn over the composed catalogue (DAT-823).

    Gated by the same ``semantic_analysis`` feature as the per-table tier — the
    catalogue phase is the relocated authoring half of that feature, not a new
    knob — and runs on the same model tier the authoring used before the split.
    """

    def __init__(
        self,
        config: LLMConfig,
        provider: LLMProvider,
        prompt_renderer: PromptRenderer,
        verticals_dir: Path | None = None,
    ) -> None:
        """Initialize the catalogue agent (mirrors SemanticAgent's construction)."""
        super().__init__(config, provider, prompt_renderer)
        self._ontology_loader = OntologyLoader(verticals_dir)

    def author(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        *,
        table_ids: list[str],
        session_table_ids: list[str],
        ontology: str,
        run_id: str,
    ) -> Result[CatalogueSemanticsOutput]:
        """Author table readings + column concepts for ``table_ids``.

        ``table_ids`` is the authoring scope (the coverage retry narrows it to
        the still-uncovered tables); ``session_table_ids`` is the full session
        selection the cross-table evidence loads over. ``duckdb_conn`` feeds
        the chain-conditioned label aggregates on the relationship lines
        (DAT-853). Structured output is constrained to
        :class:`CatalogueSemanticsOutput`; a provider failure raises typed
        (retryability rides the exception to the durable boundary, DAT-503).
        """
        feature_config = self.config.features.semantic_analysis
        if not feature_config.enabled:
            return Result.fail("Semantic analysis is disabled in config")

        ontology_def = load_workspace_concepts(session, ontology)
        if not ontology_def.concepts:
            return Result.fail(f"Vertical '{ontology}' has no concepts.")

        sampler = DataSampler(self.config.privacy)
        inputs = build_catalogue_inputs(
            session,
            duckdb_conn,
            table_ids=table_ids,
            session_table_ids=session_table_ids,
            run_id=run_id,
            sampler=sampler,
        )
        required_fields = _required_standard_fields(ontology)
        context = {
            **inputs,
            "ontology_name": ontology,
            "ontology_concepts": self._ontology_loader.format_concepts_for_prompt(ontology_def),
            "required_standard_fields": (
                "\n".join(f"- {field}" for field in required_fields)
                if required_fields
                else "(none declared)"
            ),
        }

        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                "catalogue_semantics", context
            )
        except Exception as e:
            return Result.fail(f"Failed to render catalogue_semantics prompt: {e}")

        model = self.provider.get_model_for_tier(feature_config.model_tier)
        request = ConversationRequest(
            messages=[Message(role="user", content=user_prompt)],
            system=system_prompt,
            output_schema=CatalogueSemanticsOutput.model_json_schema(),
            label="catalogue_semantics",
            effort=feature_config.effort,
            max_tokens=self.config.limits.max_output_tokens_per_request,
            temperature=temperature,
            model=model,
        )

        # ``converse`` raises a typed ProviderError on an API failure (DAT-503);
        # the structured output is API-constrained to the schema (DAT-807), so a
        # shape failure means the API contract broke — parse fails loud.
        response = self.provider.converse(request).unwrap()
        return parse_structured_output(
            response, CatalogueSemanticsOutput, label="catalogue_semantics"
        )


__all__ = ["CatalogueSemanticsAgent"]
