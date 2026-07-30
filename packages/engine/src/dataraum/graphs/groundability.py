"""Deterministic ungroundable-dimension verdict (DAT-620).

An opaque/coded discriminator column with no resolving reference table gets
confidently mislabeled instead of abstaining: the grounding agent guesses a
predicate over code values nothing in the workspace resolves, and the absence
screen (``classify_no_support``'s CONCEPT_ABSENT) then concludes "the data
carries no value for this concept" — unsound when the served values are opaque
codes that may well ENCODE the concept. This module computes the honest verdict
from EXISTING persisted signals only — deterministic, no LLM, computed every
run — and the one cure its message names is generic: LINK a reference/lookup
table that resolves these values. (GL codes → chart of accounts is one
vertical's instance; no domain vocabulary lives here.)

**The trigger** — a column is ``ungroundable`` when all three legs hold:

1. *Opacity / no ontology match* — the column has NO resolved meaning at the
   pinned catalogue run: no ``ColumnConcept`` row, ``meaning`` NULL, or
   ``meaning_status='ambiguous'`` (DAT-823's persisted declared-ignorance — the
   catalogue agent looked and said the composed evidence does not settle it).
   The ticket words this as two legs ("business_meaning abstained or meaning
   absent" and "no ColumnMeaning/concept binding"), and the ``business_meaning``
   entropy arm is deliberately NOT read: under the three-leg conjunction it is
   absorbed — ``(naming_abstained OR no_meaning) AND no_meaning ≡ no_meaning``
   — so it can never flip the verdict, only re-word it. (It could not be read
   without a threshold anyway: ``SemanticAnnotation`` has no abstention
   encoding — the column-annotation prompt is anti-abstention, confidence is
   NEVER NULL from the writer, and the detector coerces a missing confidence to
   1.0. ADR-0009: we consume the agent's own persisted abstention
   (``meaning_status``), never re-judge its meaning.)
2. *Coded/id-like discriminator* — the head-pinned ``SemanticAnnotation``'s
   ``semantic_role == 'identifier'``: the annotator's own persisted judgment
   that the VALUES identify things (codes), the one existing role that says
   coded/id-like. Deliberately narrow — a plain-word categorical
   (role ``dimension``/``attribute``) never triggers, so a coded column the
   annotator mis-roled escapes in v1 (conservative by design; no cardinality
   threshold is invented here).
3. *No resolving join* — no defined reference edge (``og_references``
   membership read at the base table: ``relationship_type IN ('foreign_key',
   'hierarchy') AND detection_method != 'candidate'`` at the pinned catalogue
   run) resolves the column: an edge FROM the column (child side) counts when
   the referenced table carries at least one non-key text-like column; an edge
   TO the column (the column IS a reference table's key) counts when its OWN
   table carries one. **v1 approximation, documented:** "resolving" means the
   reachable table HAS name/description-shaped material (a non-key
   VARCHAR/TEXT column other than the join key) — whether that text actually
   describes these values is not verified, and a composite-key reference
   table's SECOND key column reads as text-like material too (the safe
   direction: it can only make a linked table count as resolving).

Leg 3 measures DETECTED, CONFIRMED edges only — a resolving table may exist
un-detected or un-confirmed. The trigger deliberately still fires then
(abstaining on candidates would neuter the class: candidate confidence is
DAT-839 overlap noise, and judge-DECLINED pairs stay ``candidate`` forever);
instead, candidate edges that reach text-bearing tables are DISCLOSED on the
evidence (``candidate_links``) so the user is routed to CONFIRM the
relationship rather than re-upload a table that is already there.

**No false abstention** (the ticket's third criterion): when the resolving
table IS linked, leg 3 fails, the verdict is ``groundable`` with the reason
``resolving_reference_linked``, and grounding proceeds untouched.

Consumers: :func:`dataraum.pipeline.phases.metrics_phase._persist_groundability_verdicts`
persists one run-versioned row per evaluated dependency column
(``dimension_groundability``, read as ``current_dimension_groundability``), and
the ``classify_no_support`` seam (agent.py) turns an ``ungroundable`` verdict on
a failing extract's filter column into the ``UNGROUNDABLE_DIMENSION`` no-support
class. Both consume THIS evaluator, so for FRESHLY-MEASURED findings the
persisted row and the failure message cannot disagree within a run. (A finding
CARRIED across a compliant fall-loud refresh is a prior run's measurement: the
sticky carry in ``_save_failed_snippet`` re-evaluates it where the pinned reads
are available and keeps it verbatim where they are not.)
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from sqlalchemy import or_, select

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.semantic.db_models import ColumnConcept, SemanticAnnotation
from dataraum.core.logging import get_logger
from dataraum.storage import Column, Table
from dataraum.storage.snapshot_head import GENERATION_STAGE, head_run_id

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    import duckdb
    from sqlalchemy.orm import Session

logger = get_logger(__name__)


class GroundabilityStatus(StrEnum):
    """CLASSIFIED carries a verdict + reason; ABSTAINED carries only its abstain reason."""

    CLASSIFIED = "classified"
    ABSTAINED = "abstained"


class GroundabilityVerdict(StrEnum):
    """Whether a metric filter over this discriminator column can be grounded honestly."""

    UNGROUNDABLE = "ungroundable"
    GROUNDABLE = "groundable"


class GroundabilityReason(StrEnum):
    """Why the verdict is what it is — leg-level, deterministic.

    ``ungroundable`` always pairs with ``NO_RESOLVING_REFERENCE`` (the trigger
    is the three-leg conjunction; the join leg names the cure). ``groundable``
    names the FIRST failing leg in a fixed order — discriminator shape, then
    meaning, then join — so the row says which existing signal already resolves
    the column.
    """

    NO_RESOLVING_REFERENCE = "no_resolving_reference"
    NOT_CODED_DISCRIMINATOR = "not_coded_discriminator"
    MEANING_RESOLVED = "meaning_resolved"
    RESOLVING_REFERENCE_LINKED = "resolving_reference_linked"


class GroundabilityAbstainReason(StrEnum):
    """Why no verdict could be reached — a row that says so, never a hole.

    ``NO_CATALOGUE_RUN``: no promoted begin_session head — legs 1 and 3 have no
    run to read at. ``NO_SEMANTIC_ANNOTATION``: the column's table has no
    promoted generation head or no annotation row, so the coded/id-like leg is
    undecidable.
    """

    NO_CATALOGUE_RUN = "no_catalogue_run"
    NO_SEMANTIC_ANNOTATION = "no_semantic_annotation"


#: Reasons a ``groundable`` verdict may carry (everything but the trigger reason).
GROUNDABLE_REASONS: frozenset[str] = frozenset(
    v.value for v in GroundabilityReason if v is not GroundabilityReason.NO_RESOLVING_REFERENCE
)


@dataclass(frozen=True)
class ColumnGroundability:
    """One typed discriminator column's verdict (or typed abstention), run-stampable.

    ``candidate_links`` counts UNCONFIRMED candidate edges from/to this column
    that reach a text-bearing table — leg 3 does not measure them (see the
    module note), but the evidence disclosure routes the user to confirm one
    instead of re-uploading a table that already exists.
    """

    column_id: str
    column_name: str
    table_id: str
    table_name: str
    status: GroundabilityStatus
    verdict: GroundabilityVerdict | None = None
    reason: GroundabilityReason | None = None
    abstain_reason: GroundabilityAbstainReason | None = None
    candidate_links: int = 0

    def evidence(self) -> str:
        """The UNGROUNDABLE_DIMENSION message: the column, its table, and the generic cure.

        Honest about what leg 3 measured: no CONFIRMED relationship — not
        "nothing in the workspace" — and when unconfirmed candidate links to
        text-bearing tables exist, they are disclosed so confirming one is the
        first move, not another upload.
        """
        base = (
            f"filter column '{self.column_name}' of table '{self.table_name}' holds "
            "coded values with no resolved meaning, no ontology match, and no "
            "confirmed relationship linking them to a reference table; link a "
            "reference/lookup table that resolves these values (or confirm the "
            "relationship if the table already exists)"
        )
        if self.candidate_links:
            base += (
                f"; {self.candidate_links} unconfirmed candidate link(s) exist — "
                "confirming one may resolve these values"
            )
        return base


def where_predicate_columns(
    where: Iterable[Any], duckdb_conn: duckdb.DuckDBPyConnection
) -> set[str]:
    """Every COLUMN_REF identifier in an extract's ``where`` parts.

    The typed home of filter columns is the healthy provenance's
    ``filter_members`` — but a RETAINED-FAILURE row carries no basis by design
    (``FailedSnippetProvenance``: no column enumeration), and its ``parts`` ARE
    retained (DAT-671, the parts are the artifact). So the attempted grounding's
    filter columns are recovered from its own predicate strings via DuckDB's
    catalog-free parse (``json_serialize_sql`` — the ``parse_aggregate_calls``
    machinery, additivity.py), stripping any table qualifier. A predicate that
    does not parse contributes nothing (best-effort recovery, warned). The walk
    descends into SUBQUERY nodes too — harmless over-collection: a name that
    belongs to an inner relation simply resolves to no served column and is
    skipped at resolution.
    """
    columns: set[str] = set()
    for predicate in where:
        text = str(predicate).strip()
        if not text:
            continue
        try:
            raw = duckdb_conn.execute("SELECT json_serialize_sql(?)", [f"SELECT {text}"]).fetchone()
            doc = json.loads(raw[0]) if raw else None
        except Exception as exc:  # noqa: BLE001 - best-effort recovery, never a failure
            logger.warning("where_predicate_parse_failed", predicate=text, error=str(exc))
            continue
        if not doc or doc.get("error"):
            logger.warning("where_predicate_parse_failed", predicate=text, error="parse error")
            continue
        _collect_column_refs(doc["statements"][0]["node"]["select_list"], columns)
    return columns


def _collect_column_refs(node: Any, out: set[str]) -> None:
    """Every ``COLUMN_REF`` base column beneath a node (additivity.py's walker shape)."""
    if isinstance(node, dict):
        if node.get("class") == "COLUMN_REF":
            names = node.get("column_names") or []
            if names:
                out.add(str(names[-1]))
        else:
            for value in node.values():
                _collect_column_refs(value, out)
    elif isinstance(node, list):
        for item in node:
            _collect_column_refs(item, out)


def resolve_served_filter_columns(
    session: Session, relation: str, names: set[str]
) -> dict[str, tuple[Column, Table]]:
    """Served filter-column name → the TYPED ``(Column, Table)`` it projects.

    A grounded extract reads an ENRICHED VIEW (DAT-811/812), whose served
    columns each carry a ``source_column_id`` to the typed column they project —
    the identity every leg signal (``ColumnConcept``, ``SemanticAnnotation``,
    ``Relationship``) is keyed on. Resolution mirrors the additivity resolver's
    (``served_relation`` + the served-column read). A name that does not resolve
    (relation not a current enriched view, or a served column without a typed
    source) is simply absent from the result — the caller logs it; no verdict
    row can exist without a column identity.
    """
    from dataraum.graphs.additivity_resolver import served_relation

    if not names:
        return {}
    served = served_relation(session, relation)
    if served is None:
        return {}
    rows = session.execute(
        select(Column.column_name, Column.source_column_id).where(
            Column.table_id == served.columns_table_id,
            Column.column_name.in_(sorted(names)),
            Column.source_column_id.isnot(None),
        )
    ).all()
    source_ids = {str(src): str(name) for name, src in rows}
    if not source_ids:
        return {}
    typed = session.execute(
        select(Column, Table)
        .join(Table, Column.table_id == Table.table_id)
        .where(Column.column_id.in_(sorted(source_ids)))
    ).all()
    return {source_ids[str(col.column_id)]: (col, table) for col, table in typed}


#: ``resolved_type`` families that can carry a name/description-like resolution.
_TEXT_TYPE_MARKERS = ("CHAR", "TEXT", "STRING")


def _is_text_like(resolved_type: str | None) -> bool:
    upper = (resolved_type or "").upper()
    return any(marker in upper for marker in _TEXT_TYPE_MARKERS)


def evaluate_groundability(
    session: Session,
    columns: Iterable[tuple[Column, Table]],
    *,
    catalogue_run_id: str | None,
    semantic_runs: Mapping[str, str] | None = None,
) -> list[ColumnGroundability]:
    """The three-leg verdict for each typed discriminator column, one result each.

    Every input column comes back classified or typed-abstained — never dropped
    (absence ≠ not-judged is the persisted table's contract, and it starts
    here). Reads are pinned: ``ColumnConcept`` and the reference catalogue at
    ``catalogue_run_id`` (the begin_session catalogue head, og_references'
    membership); ``SemanticAnnotation`` at each table's promoted generation
    head — passed in as ``semantic_runs`` (the phase's pinned
    ``BaseRunMap.semantic_runs``) or resolved here through the same
    ``head_run_id`` read that map is built from (the classification seam's
    path; generation heads do not move within an operating_model run, so the
    two paths read the same rows).
    """
    cols = list(columns)
    if not cols:
        return []
    if catalogue_run_id is None:
        return [
            _abstained(col, table, GroundabilityAbstainReason.NO_CATALOGUE_RUN)
            for col, table in cols
        ]

    column_ids = sorted({str(col.column_id) for col, _ in cols})
    table_ids = sorted({str(col.table_id) for col, _ in cols})

    heads: dict[str, str] = dict(semantic_runs) if semantic_runs is not None else {}
    if semantic_runs is None:
        for table_id in table_ids:
            run = head_run_id(session, f"table:{table_id}", GENERATION_STAGE)
            if run is not None:
                heads[table_id] = run

    meaning_rows = session.execute(
        select(ColumnConcept.column_id, ColumnConcept.meaning, ColumnConcept.meaning_status).where(
            ColumnConcept.column_id.in_(column_ids),
            ColumnConcept.run_id == catalogue_run_id,
        )
    ).all()
    meaning_by_column = {str(cid): (meaning, status) for cid, meaning, status in meaning_rows}

    # A column's annotation counts only at its OWN table's promoted generation
    # head (`(column_id, run_id)` is unique) — another table's head run must
    # never resolve it, and an unpinned row (no promoted head) reads as absent.
    table_by_column = {str(col.column_id): str(col.table_id) for col, _ in cols}
    role_rows = session.execute(
        select(
            SemanticAnnotation.column_id,
            SemanticAnnotation.run_id,
            SemanticAnnotation.semantic_role,
        ).where(SemanticAnnotation.column_id.in_(column_ids))
    ).all()
    role_by_column: dict[str, str | None] = {}
    for cid, run, role in role_rows:
        if str(run) == heads.get(table_by_column.get(str(cid), "")):
            role_by_column[str(cid)] = role

    # Edges at the pinned catalogue run, in two classes. CONFIRMED references are
    # og_references membership at the base table (reference kinds, defined
    # catalogue — the Python filter mirrors the view's SQL `!=`, which drops NULL
    # detection_method rows; no writer produces NULL). CANDIDATE edges
    # (detection_method='candidate', incl. judge-declined pairs, which stay
    # candidate — DAT-824) never satisfy leg 3; they feed the disclosure count
    # only (see the module note).
    edges = session.execute(
        select(
            Relationship.from_column_id,
            Relationship.to_table_id,
            Relationship.to_column_id,
            Relationship.relationship_type,
            Relationship.detection_method,
        ).where(
            Relationship.run_id == catalogue_run_id,
            or_(
                Relationship.from_column_id.in_(column_ids),
                Relationship.to_column_id.in_(column_ids),
            ),
        )
    ).all()

    # (resolving table, columns excluded from the non-key text check) per column,
    # split by edge class.
    evaluated = set(column_ids)
    probe_tables: dict[str, list[tuple[str, set[str]]]] = {}
    candidate_probes: dict[str, list[tuple[str, set[str]]]] = {}
    for from_col, to_table, to_col, rel_type, method in edges:
        if method == "candidate":
            target = candidate_probes
        elif rel_type in ("foreign_key", "hierarchy") and method is not None:
            target = probe_tables
        else:
            continue  # conformed_dimension et al. — neither a reference nor a candidate
        if str(from_col) in evaluated:
            # Child side: the referenced (parent) table would resolve the codes.
            target.setdefault(str(from_col), []).append((str(to_table), {str(to_col)}))
        if str(to_col) in evaluated:
            # Parent side: the column IS a reference table's key — its own
            # non-key siblings are the resolution.
            target.setdefault(str(to_col), []).append((str(to_table), {str(to_col)}))

    all_probes = [
        probe
        for candidates in (*probe_tables.values(), *candidate_probes.values())
        for probe in candidates
    ]
    text_tables = _tables_with_nonkey_text(session, all_probes)

    out: list[ColumnGroundability] = []
    for col, table in cols:
        cid = str(col.column_id)
        if cid not in role_by_column:
            out.append(_abstained(col, table, GroundabilityAbstainReason.NO_SEMANTIC_ANNOTATION))
            continue
        coded = role_by_column[cid] == "identifier"
        meaning, status = meaning_by_column.get(cid, (None, None))
        meaning_unresolved = meaning is None or status == "ambiguous"
        resolved_by_reference = any(
            (probe_table, tuple(sorted(excluded))) in text_tables
            for probe_table, excluded in probe_tables.get(cid, [])
        )
        candidate_links = sum(
            1
            for probe_table, excluded in candidate_probes.get(cid, [])
            if (probe_table, tuple(sorted(excluded))) in text_tables
        )
        if not coded:
            verdict, reason = (
                GroundabilityVerdict.GROUNDABLE,
                GroundabilityReason.NOT_CODED_DISCRIMINATOR,
            )
        elif not meaning_unresolved:
            verdict, reason = GroundabilityVerdict.GROUNDABLE, GroundabilityReason.MEANING_RESOLVED
        elif resolved_by_reference:
            verdict, reason = (
                GroundabilityVerdict.GROUNDABLE,
                GroundabilityReason.RESOLVING_REFERENCE_LINKED,
            )
        else:
            verdict, reason = (
                GroundabilityVerdict.UNGROUNDABLE,
                GroundabilityReason.NO_RESOLVING_REFERENCE,
            )
        out.append(
            ColumnGroundability(
                column_id=cid,
                column_name=col.column_name,
                table_id=str(col.table_id),
                table_name=table.table_name,
                status=GroundabilityStatus.CLASSIFIED,
                verdict=verdict,
                reason=reason,
                candidate_links=candidate_links,
            )
        )
    return out


def _tables_with_nonkey_text(
    session: Session, all_probes: Iterable[tuple[str, set[str]]]
) -> set[tuple[str, tuple[str, ...]]]:
    """Which ``(table, excluded key columns)`` probes carry a non-key text-like column."""
    probes = {(table_id, tuple(sorted(excluded))) for table_id, excluded in all_probes}
    if not probes:
        return set()
    table_ids = sorted({table_id for table_id, _ in probes})
    rows = session.execute(
        select(Column.table_id, Column.column_id, Column.resolved_type).where(
            Column.table_id.in_(table_ids)
        )
    ).all()
    by_table: dict[str, list[tuple[str, str | None]]] = {}
    for table_id, column_id, resolved_type in rows:
        by_table.setdefault(str(table_id), []).append((str(column_id), resolved_type))
    resolved: set[tuple[str, tuple[str, ...]]] = set()
    for table_id, excluded in probes:
        for column_id, resolved_type in by_table.get(table_id, []):
            if column_id not in excluded and _is_text_like(resolved_type):
                resolved.add((table_id, excluded))
                break
    return resolved


def _abstained(
    col: Column, table: Table, reason: GroundabilityAbstainReason
) -> ColumnGroundability:
    return ColumnGroundability(
        column_id=str(col.column_id),
        column_name=col.column_name,
        table_id=str(col.table_id),
        table_name=table.table_name,
        status=GroundabilityStatus.ABSTAINED,
        abstain_reason=reason,
    )


__all__ = [
    "GROUNDABLE_REASONS",
    "ColumnGroundability",
    "GroundabilityAbstainReason",
    "GroundabilityReason",
    "GroundabilityStatus",
    "GroundabilityVerdict",
    "evaluate_groundability",
    "resolve_served_filter_columns",
    "where_predicate_columns",
]
