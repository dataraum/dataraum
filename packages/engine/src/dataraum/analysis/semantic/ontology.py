"""Ontology loading from configuration files.

Loads ontology definitions from config/verticals/<vertical>/ontology.yaml
through the shared :class:`~dataraum.core.vertical_loader.VerticalLoader`
(DAT-481). The concept vocabulary moved config→DB (DAT-728): concepts are no
longer overlay rows — the shipped YAML is the seed for the typed ``concepts``
table (``concept_store``), which the runtime reads. This loader resolves the
baked-in YAML ⊕ the surviving overlay families (validation/cycle/metric); a
framed vertical resolves its concepts as EMPTY here (they live in the typed
table). Custom ``verticals_dir`` (test fixtures) bypasses the overlay.
"""

from __future__ import annotations

from collections.abc import Collection
from pathlib import Path

from pydantic import BaseModel, Field, model_validator

from dataraum.core.config import get_config_dir
from dataraum.core.vertical import VerticalKind, resolve_vertical
from dataraum.core.vertical_loader import Family, VerticalLoader


class OntologyConcept(BaseModel):
    """A concept within an ontology."""

    name: str
    # The ontological kind (DAT-728): measure|entity|dimension|unit. The seed
    # normalizes this into the typed ``concepts`` table; a seeded concept without a
    # kind is a config error (validated born-loud at seed time, not here — an empty
    # framed vertical parses fine and declares its kinds through ``frame``).
    kind: str | None = None
    description: str | None = None
    indicators: list[str] = Field(default_factory=list)
    exclude_patterns: list[str] = Field(default_factory=list)
    unit_from_concept: str | None = None  # Which concept provides this measure's unit
    # Ordered vs nominal dimension axis (DAT-730): OPTIONAL seed declaration on a
    # ``kind=dimension`` concept — 'ordered' | 'nominal' | None. Passed through to
    # ``Concept.ordering`` at seed (never inferred); absent ⇒ NULL ⇒ nominal (windows
    # withheld). The cockpit ``frame`` authoring path is a later lane.
    ordering: str | None = None
    # Operating-model axis (DAT-855): OPTIONAL seed declaration — demand | offer |
    # supply | capacity | throughput | capital | cross_cutting | None. Passed through
    # to ``Concept.dimension_facet`` at seed (never inferred); absent ⇒ NULL ⇒ "no
    # writer classified yet" (a framed vertical mid-authoring — the shipped finance
    # vertical declares one for every concept, so this stays populated there).
    dimension_facet: str | None = None


class OntologyConvention(BaseModel):
    """A domain convention piped verbatim to SQL-authoring agents (DAT-645).

    Conventions carry vertical-specific guidance that an LLM applies when
    authoring SQL — e.g. the sign/normalization rule that makes a credit-normal
    measure read positive. The engine is deliberately agnostic to the *content*:
    it parses only this generic envelope, validates that ``concept_groups``
    members are declared concepts, and routes the block by ``targets`` — it never
    interprets the group labels or the ``statement`` prose (both finance-specific,
    LLM-facing). Mirrors how ``description`` / ``sql_hints`` are parsed fields
    whose values are opaque to the engine.
    """

    id: str
    targets: list[str] = Field(default_factory=list)  # consumer labels: extraction|validation|qa
    statement: str  # verbatim LLM-facing guidance — engine never interprets it
    concept_groups: dict[str, list[str]] = Field(default_factory=dict)  # label -> concept names


class OntologyComposition(BaseModel):
    """A concept-composition declaration → ``part_of`` edges (DAT-729).

    Mereological composition at the CONCEPT grain: a ``whole`` concept is composed of
    its ``parts`` (``current_assets`` = cash + receivables + inventory). The seed
    promotes each ``part → whole`` into a typed ``part_of`` :class:`ConceptEdge`; the
    transitive closure (all ancestors of a concept) is a bounded recursive CTE.

    This is the concept-grain composition ONLY — the account-instance chart-of-accounts
    tree is the physical ``references`` topology and the dimension-member roll-up
    is ``rolls_up_to``; neither is re-expressed here.
    """

    whole: str  # the composite concept
    parts: list[str] = Field(default_factory=list)  # concepts that compose the whole


class OntologyEntity(BaseModel):
    """A declared TABLE-level entity kind — the vertical's entity taxonomy (DAT-724).

    The table-grain sibling of :class:`OntologyConcept`. A vertical's concept
    vocabulary says what a *column* can mean; nothing said what a *table* can be, so
    ``table_entities.detected_entity_type`` was unconstrained LLM free text and the
    fact/dimension bit was inferred blind. An entity declares the kinds of table the
    domain actually has, so both become ontology-graded.

    The declaration **proposes, never suppresses**: a table matching no declared
    entity keeps free-text detection. That is the framed-vertical / novel-table case
    and it is the common one — the taxonomy is evidence served into the classifier's
    context, never a filter applied to its answer.

    ``role`` reuses the existing :class:`~dataraum.analysis.semantic.db_models.TableRole`
    vocabulary (fact | periodic_snapshot | dimension) rather than inventing a parallel
    one — it is a declaration ABOUT the same column the pipeline persists. It is a
    *label source and tiebreaker*, never an override: ``derive_table_role`` still owns
    the structural ``periodic_snapshot`` refinement, and this value never reaches it.
    """

    name: str
    # The table role this entity kind takes in the operating model — a TableRole value
    # (fact | periodic_snapshot | dimension). Validated born-loud at SEED, not here, so
    # an empty/mid-authoring framed vertical still parses (the OntologyConcept.kind
    # discipline).
    role: str | None = None
    description: str | None = None
    # Concepts an instance of this entity is expected to carry. Every name must resolve
    # to a DECLARED concept (linted below) — a typo'd or renamed concept is an authoring
    # error, not a runtime surprise. An empty list is legitimate and informative: it says
    # the vertical's concept vocabulary has nothing at this table's grain yet.
    concepts: list[str] = Field(default_factory=list)
    # Business cycles this entity participates in. Validated at seed against the typed
    # ``cycle_types`` vocabulary (DAT-881), which is seeded earlier in the same phase.
    cycles: list[str] = Field(default_factory=list)
    # Matching hints — the physical table names/synonyms this entity kind shows up as.
    # Served to the classifier as evidence; never used for an engine-side string match
    # (no algorithmic pre-pass — the LLM decides whether a table IS this entity).
    aliases: list[str] = Field(default_factory=list)


class OntologyDefinition(BaseModel):
    """A complete ontology definition from YAML."""

    name: str
    # No fabricated default (DAT-883): a shipped vertical's YAML always declares its
    # own version; a framed vertical (loaded via the empty ``{"name": v}`` base, no
    # on-disk file) genuinely has none, and must resolve to NULL here, never an
    # invented "1.0.0". The typed envelope home (`envelope_store`) carries the same
    # nullability forward into the DB.
    version: str | None = None
    description: str | None = None
    concepts: list[OntologyConcept] = Field(default_factory=list)
    conventions: list[OntologyConvention] = Field(default_factory=list)
    compositions: list[OntologyComposition] = Field(default_factory=list)
    entities: list[OntologyEntity] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_entities(self) -> OntologyDefinition:
        """Lint the entity taxonomy against the concept vocabulary (DAT-724), born-loud.

        The same authoring-time reference lint the conventions and compositions get,
        and for the same reason: an entity naming a concept that does not exist is a
        typo or a rename that silently degrades the evidence served to the classifier.

        Names must also be unique — two entities sharing a name would collide on the
        ``vertical_entities`` active-row partial-unique index at seed, which is a far
        worse place to discover it.

        ``role`` and ``cycles`` are deliberately NOT checked here: ``role`` is a closed
        vocabulary (validated born-loud at seed, the ``OntologyConcept.kind``
        discipline) and ``cycles`` resolves against a vocabulary that lives in another
        family's table entirely — neither is answerable from this document.
        """
        if not self.entities:
            return self
        concept_names = {c.name for c in self.concepts}
        seen: set[str] = set()
        for ent in self.entities:
            if ent.name in seen:
                raise ValueError(
                    f"entity '{ent.name}' is declared twice — entity names must be unique "
                    f"within a vertical"
                )
            seen.add(ent.name)
            for concept in ent.concepts:
                if concept not in concept_names:
                    raise ValueError(
                        f"entity '{ent.name}' references concept '{concept}', "
                        f"which is not a declared concept"
                    )
        return self

    @model_validator(mode="after")
    def _validate_conventions(self) -> OntologyDefinition:
        """Lint conventions against the concept vocabulary (DAT-645), born-loud.

        Generic, vertical-agnostic checks — the engine validates that references
        *resolve*, never what a group *means*:

        - **resolve:** every concept named in a ``concept_groups`` list must be a
          declared concept (catches typos / renamed concepts).
        - **disjoint:** a concept may not appear in two groups of the same
          convention (e.g. both credit- and debit-normal — a contradiction).

        Coverage ("every measure is grouped") is intentionally NOT enforced: raw
        ledger columns and period balances legitimately have no fixed group, so
        full coverage is ill-defined. The per-metric runtime verifier is the
        backstop for an ungrouped-but-should-be-grouped concept.
        """
        if not self.conventions:
            return self
        concept_names = {c.name for c in self.concepts}
        for conv in self.conventions:
            placed: dict[str, str] = {}  # concept name -> the group it first landed in
            for group, members in conv.concept_groups.items():
                for member in members:
                    if member not in concept_names:
                        raise ValueError(
                            f"convention '{conv.id}' group '{group}' references "
                            f"'{member}', which is not a declared concept"
                        )
                    if member in placed:
                        raise ValueError(
                            f"convention '{conv.id}' assigns '{member}' to both "
                            f"'{placed[member]}' and '{group}' — groups must be disjoint"
                        )
                    placed[member] = group
        return self

    @model_validator(mode="after")
    def _validate_compositions(self) -> OntologyDefinition:
        """Lint composition declarations against the vocabulary (DAT-729), born-loud.

        A ``part_of`` edge is only meaningful between declared concepts, so both the
        ``whole`` and every ``part`` must resolve; a concept cannot be part of itself
        (a trivial self-loop the graph must never carry). Deeper cycles are guarded by
        the closure traversal at read time, not here.
        """
        if not self.compositions:
            return self
        concept_names = {c.name for c in self.concepts}
        for comp in self.compositions:
            for name in (comp.whole, *comp.parts):
                if name not in concept_names:
                    raise ValueError(
                        f"composition of '{comp.whole}' references '{name}', "
                        f"which is not a declared concept"
                    )
            if comp.whole in comp.parts:
                raise ValueError(
                    f"composition of '{comp.whole}' lists itself as a part — "
                    f"a concept cannot be part_of itself"
                )
        return self


class OntologyLoader:
    """Load ontology definitions from YAML configuration files.

    Loads ontologies from config/verticals/<vertical>/ontology.yaml.
    """

    def __init__(self, verticals_dir: Path | None = None):
        """Initialize ontology loader.

        Args:
            verticals_dir: Root verticals directory. ``None`` (production)
                routes loads through :func:`load_yaml_config` so workspace
                overlay rows are merged. A custom path (tests / fixtures)
                reads YAML directly and bypasses the overlay —
                deterministic for unit tests.
        """
        self.verticals_dir = verticals_dir
        # No cache: the overlay-aware production path must reflect newly
        # inserted overlay rows on the next call (a validation/cycle/metric
        # teach lands then re-reads in the same phase). Per-load latency is one
        # filesystem read + dict copies; not a hotspot.

    def load(self, vertical: str) -> OntologyDefinition | None:
        """Load an ontology definition for a vertical.

        Resolution is the shared :class:`VerticalLoader.collection` (DAT-481):
        production (``verticals_dir`` is ``None``) reads the shipped baseline ⊕
        overlay rows; the test path (explicit ``verticals_dir``) reads raw YAML
        and bypasses the overlay.

        Args:
            vertical: Vertical name (e.g. ``'finance'`` or ``'_adhoc'``).

        Returns:
            The (overlay-merged) ontology definition. A builtin resolves its
            baked-in YAML ⊕ surviving overlay families (validation/cycle/metric).
            Concepts moved config→DB (DAT-728): a framed vertical has no on-disk
            YAML and concepts are no longer overlay rows, so this resolves its
            concepts as EMPTY — the framed concept vocabulary lives in the typed
            ``concepts`` table and is read via ``concept_store`` at runtime, not
            here. ``None`` only on the production path for an UNKNOWN vertical (a
            typo or one never framed) — the single ``None`` case, owned by
            ``resolve_vertical`` (DAT-480); every known/placeholder/framed name
            resolves to a (possibly empty) definition.
        """
        # DAT-480: an UNKNOWN production vertical is the only None case. The test
        # path is deterministic (no overlay), so the guard is production-only.
        if self.verticals_dir is None and resolve_vertical(vertical) is VerticalKind.UNKNOWN:
            return None
        data = VerticalLoader(vertical, self.verticals_dir).collection(Family.CONCEPTS)
        return OntologyDefinition(**data)

    def list_verticals(self) -> list[str]:
        """List the shipped (on-disk) verticals.

        File-globs ``verticals/*/ontology.yaml`` only — *framed* verticals
        (declared via the cockpit ``frame`` stage; they live entirely in overlay
        rows with no on-disk file, DAT-480) are NOT enumerated here. Returns
        ``[]`` if the verticals root doesn't exist.
        """
        root = self.verticals_dir if self.verticals_dir is not None else get_config_dir("verticals")
        if not root.exists():
            return []
        return [p.parent.name for p in root.glob("*/ontology.yaml")]

    def format_concepts_for_prompt(self, ontology: OntologyDefinition | None) -> str:
        """Format ontology concepts for inclusion in LLM prompts.

        Args:
            ontology: Ontology definition, or None

        Returns:
            Formatted string describing concepts, or default message
        """
        if ontology is None or not ontology.concepts:
            return "No specific ontology concepts defined"

        lines = []
        for concept in ontology.concepts:
            indicators_str = ", ".join(concept.indicators) if concept.indicators else ""
            if concept.description:
                lines.append(f"- {concept.name}: {concept.description}")
                if indicators_str:
                    lines.append(f"  Indicators: {indicators_str}")
            elif indicators_str:
                lines.append(f"- {concept.name}: {indicators_str}")
            else:
                lines.append(f"- {concept.name}")
            # Unit-from-concept (DAT-647): the concept declares that a sibling
            # concept (e.g. `currency`) supplies its measures' unit. Fed so the
            # agent grounds unit_source_column on it — the concept-level unit teach.
            if concept.unit_from_concept:
                lines.append(f"  Unit from concept: {concept.unit_from_concept}")

        return "\n".join(lines)

    def format_entities_for_prompt(self, ontology: OntologyDefinition | None) -> str:
        """Format the declared table-entity taxonomy for LLM prompts (DAT-724).

        Renders the vertical's declared entity kinds as EVIDENCE for the two agents
        that answer table-grain questions — the per-table structural agent (which
        answers ``is_fact_table``) and the catalogue agent (which authors
        ``entity_type``). Both already receive ``ontology_concepts``; this is its
        table-grain sibling.

        Returns the "nothing declared" sentence when a vertical declares no taxonomy
        (every framed vertical today, and any builtin that hasn't authored one). The
        sentence is deliberately explicit that free-text detection is the mode in that
        case — the prompt slot must not read as an empty list of *permitted* answers.
        """
        if ontology is None or not ontology.entities:
            return "No table entity taxonomy declared — describe each table in your own words."

        lines = []
        for entity in ontology.entities:
            role = f" [{entity.role}]" if entity.role else ""
            header = f"- {entity.name}{role}"
            if entity.description:
                header += f": {entity.description}"
            lines.append(header)
            if entity.concepts:
                lines.append(f"  Carries concepts: {', '.join(entity.concepts)}")
            if entity.cycles:
                lines.append(f"  Business cycles: {', '.join(entity.cycles)}")
            # "Commonly named" not "matches": these are hints for the model's judgment,
            # never an engine-side string match (no algorithmic pre-pass).
            if entity.aliases:
                lines.append(f"  Commonly named: {', '.join(entity.aliases)}")

        return "\n".join(lines)

    def format_conventions_for_prompt(
        self,
        ontology: OntologyDefinition | None,
        target: str,
        qualifier: str | None = None,
        *,
        include_ids: Collection[str] = (),
    ) -> str:
        """Render the vertical's conventions for one consumer (DAT-645).

        Returns the verbatim ``statement`` plus each ``concept_groups`` entry, for
        every convention whose ``targets`` include this consumer. A target may be
        BROAD (``"validation"`` — every validation) or SPECIFIC
        (``"validation:sign_conventions"`` — only that spec); ``qualifier`` carries
        the specific id so a convention reaches ONLY the consumers it names. This
        keeps an irrelevant convention (e.g. a sign rule) out of unrelated prompts
        (e.g. a raw debit=credit check) — relevance is the vertical's call, not the
        engine's. The engine routes by the generic label only; it never interprets
        the group labels or the statement. Empty string when nothing applies.

        ``include_ids`` is the pull side of the routing (DAT-865): a check's
        declared ``relevant_conventions`` select a convention by id regardless of
        its ``targets`` — the convention-side push can only name checks that exist
        at authoring time, so a GENERATED check declares its dependencies from the
        other side. Selection only; the rendering is identical either way.

        Args:
            ontology: Ontology definition, or None.
            target: Consumer label (e.g. ``"extraction"``, ``"validation"``).
            qualifier: Optional specific id (e.g. a validation's id) — a convention
                also matches if its targets include ``f"{target}:{qualifier}"``.
            include_ids: Convention ids to include regardless of ``targets`` (the
                validation-side declared dependencies).

        Returns:
            Formatted conventions text, or "" when none target this consumer.
        """
        if ontology is None or not ontology.conventions:
            return ""
        specific = f"{target}:{qualifier}" if qualifier else None
        blocks: list[str] = []
        for conv in ontology.conventions:
            if (
                target not in conv.targets
                and (specific is None or specific not in conv.targets)
                and conv.id not in include_ids
            ):
                continue
            lines = [conv.statement.strip()]
            for group, members in conv.concept_groups.items():
                lines.append(f"{group}: {', '.join(members)}")
            blocks.append("\n".join(lines))
        return "\n\n".join(blocks)


__all__ = [
    "OntologyComposition",
    "OntologyConcept",
    "OntologyConvention",
    "OntologyDefinition",
    "OntologyEntity",
    "OntologyLoader",
]
