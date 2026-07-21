"""Utility functions for relationship analysis.

Helper functions for loading and formatting relationship data.
"""

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from dataraum.analysis.relationships.db_models import Relationship, SurrogateKeyIntent
from dataraum.storage import Column, Table


def load_surrogate_key_intents(session: Session, run_id: str) -> list[SurrogateKeyIntent]:
    """This run's LLM-confirmed composite keys awaiting their mint (DAT-277).

    Written by ``semantic_per_table`` (one row per composite VERDICT), consumed
    ONLY by the ``surrogate_mint`` phase — hence the ``status='confirmed'``
    filter: declined verdicts (DAT-697) exist for the keeper machinery, never
    for the mint. Run-scoped by construction — an intent is an instruction to
    this run's mint, not durable catalog state.
    """
    return list(
        session.execute(
            select(SurrogateKeyIntent).where(
                SurrogateKeyIntent.run_id == run_id,
                SurrogateKeyIntent.status == "confirmed",
            )
        ).scalars()
    )


def load_defined_relationships(
    session: Session,
    table_ids: list[str],
    *,
    run_id: str | None = None,
    both_tables: bool = True,
    eager_columns: bool = False,
) -> list[Relationship]:
    """The session's **defined** relationships for ``table_ids`` (DAT-408).

    "Defined" = ``detection_method != 'candidate'`` — the durable catalog (llm +
    materialized manual/keeper), NOT the ephemeral per-run structural candidates.
    One definition so every downstream consumer (enriched_views, cycles,
    validation, graphs, …) agrees on what "the relationships" are and on
    run-scoping; they only vary the flags:

    - ``run_id``: scope to the current run's catalog (DAT-408). ``None`` reads all
      runs — only safe for callers that don't yet thread a run_id (dormant stages
      reactivated by their own slices must pass it).
    - ``both_tables``: require BOTH endpoints in ``table_ids`` (intra-selection
      joins) vs either endpoint.
    - ``eager_columns``: eager-load from/to columns + their tables.

    Deliberately NO confidence filter (DAT-699): defined rows are already
    judge-verified or user-asserted; a numeric floor here pre-empts downstream
    judges with an uncalibrated number. Confidence is evidence, not a gate.
    """
    stmt = select(Relationship).where(Relationship.detection_method != "candidate")
    if eager_columns:
        stmt = stmt.options(
            selectinload(Relationship.from_column).selectinload(Column.table),
            selectinload(Relationship.to_column).selectinload(Column.table),
        )
    if both_tables:
        stmt = stmt.where(
            Relationship.from_table_id.in_(table_ids),
            Relationship.to_table_id.in_(table_ids),
        )
    else:
        stmt = stmt.where(
            Relationship.from_table_id.in_(table_ids) | Relationship.to_table_id.in_(table_ids)
        )
    if run_id is not None:
        stmt = stmt.where(Relationship.run_id == run_id)
    return list(session.execute(stmt).scalars())


def relationship_overlay_rows(session: Session, action: str) -> list[Any]:
    """Active ``ConfigOverlay(type='relationship')`` ROWS for one ``action``.

    For callers that mutate overlays (e.g. the DAT-697 keep retraction, which
    needs ``superseded_at``); readers that only need the column pairs use
    :func:`relationship_overlay_pairs`. Malformed payloads (missing either
    column id) are excluded — every consumer of the rows may rely on both ids
    being present.
    """
    from dataraum.storage import ConfigOverlay

    rows = session.execute(
        select(ConfigOverlay).where(
            ConfigOverlay.type == "relationship",
            ConfigOverlay.superseded_at.is_(None),
        )
    ).scalars()
    return [
        row
        for row in rows
        if (row.payload or {}).get("action") == action
        and (row.payload or {}).get("from_column_id")
        and (row.payload or {}).get("to_column_id")
    ]


def relationship_overlay_pairs(session: Session, action: str) -> list[tuple[str, str]]:
    """Active ``ConfigOverlay(type='relationship')`` column pairs for one ``action``.

    The one relationship-overlay payload shape (DAT-409) is
    ``{action, from_column_id, to_column_id}`` — ``confirm`` / ``reject`` / ``add`` /
    ``keep`` are all states of the single type, keyed on the directional column pair
    (NOT table names — a relationship is between columns). Every engine reader of
    these overlays goes through this one parser so they agree on the shape.
    ``superseded_at IS NULL`` filters undone teaches out.
    """
    return [
        (row.payload["from_column_id"], row.payload["to_column_id"])
        for row in relationship_overlay_rows(session, action)
    ]


def load_suppressed_relationship_pairs(session: Session) -> set[frozenset[str]]:
    """Undirected column pairs the user has dropped (``action == "reject"``).

    A re-run must not re-create a suppressed relationship, and its readiness must not
    surface. Undirected (frozenset), like :func:`load_confirmed_relationship_pairs`:
    a reject identifies the EDGE between two columns, so it holds whichever way a
    row names the endpoints. This matters since DAT-777 canonicalizes orientation
    (all rows stored many→one) while a teach overlay names the pair as the user saw
    it — a directional reject would orphan silently when the two disagree. Callers
    test ``frozenset({from_col, to_col}) in suppressed``.
    """
    return {frozenset(pair) for pair in relationship_overlay_pairs(session, "reject")}


def load_confirmed_relationship_pairs(session: Session) -> set[frozenset[str]]:
    """Undirected column pairs the user has confirmed (``action == "confirm"``, DAT-409).

    Read by ``relationship_entropy`` (confirmation lifts semantic clarity, DAT-372)
    and ``join_path_determinism`` (a confirmed path resolves join ambiguity).
    Undirected — confirmation of ``{a, b}`` holds whichever way a detector names the
    endpoints — so callers test ``frozenset({from_col, to_col}) in confirmed``.
    """
    return {frozenset(pair) for pair in relationship_overlay_pairs(session, "confirm")}


def load_relationship_candidates_for_semantic(
    session: Session,
    table_ids: list[str] | None = None,
    detection_method: str = "candidate",
    run_id: str | None = None,
) -> list[dict[str, Any]]:
    """Load relationship candidates from DB formatted for semantic agent.

    Groups relationships by table pair and includes all evaluation metrics
    from the evidence JSON field.

    Args:
        session: Database session
        table_ids: Optional list of table IDs to filter by. If None, loads all.
        detection_method: Filter by detection method (default: 'candidate')

    Returns:
        List of relationship candidates in the format expected by SemanticAgent:
        [
            {
                "table1": "...",
                "table2": "...",
                "introduces_duplicates": False,
                "join_columns": [
                    {
                        "column1": "...",
                        "column2": "...",
                        "confidence": 0.9,
                        "cardinality": "one-to-many",
                        "left_uniqueness": 0.02,
                        "right_uniqueness": 1.0,
                        "left_referential_integrity": 100.0,
                        "right_referential_integrity": 85.0,
                        "left_orphan_count": 5,
                        "cardinality_verified": True,
                        "column1_role": "timestamp",
                        "column1_entity_type": "fiscal_period",
                        "column2_role": "key",
                    }
                ]
            }
        ]
    """
    # Local import, defensive: the relationships↔semantic coupling is already
    # order-dependent (relationships/__init__ imports this module;
    # semantic/__init__ imports agent.py, which imports
    # relationships.graph_topology) — today both orders happen to resolve, but
    # a module-level import here is one __init__ reordering away from a cycle.
    from dataraum.analysis.semantic.db_models import SemanticAnnotation

    # Build query — scoped to the current run's catalog (DAT-408) when ``run_id`` is
    # given, so coexisting prior-run rows aren't fed to the LLM twice.
    stmt = select(Relationship).where(Relationship.detection_method == detection_method)

    if run_id is not None:
        stmt = stmt.where(Relationship.run_id == run_id)

    if table_ids:
        stmt = stmt.where(
            (Relationship.from_table_id.in_(table_ids)) | (Relationship.to_table_id.in_(table_ids))
        )

    # ORDERED. Unordered, the ``###`` candidate blocks reached the prompt in
    # Postgres physical row order, so the same data could present the judge a
    # different candidate list between runs while the prompt asks it to emit a
    # stable orientation (DAT-725). Column pairs are re-sorted by overlap in the
    # formatter; this fixes the order of the pairs themselves.
    stmt = stmt.order_by(
        Relationship.from_table_id,
        Relationship.to_table_id,
        Relationship.from_column_id,
        Relationship.to_column_id,
    )
    relationships = session.execute(stmt).scalars().all()

    if not relationships:
        return []

    # Load table and column metadata for names
    table_cache: dict[str, str] = {}  # table_id -> table_name
    column_cache: dict[str, str] = {}  # column_id -> column_name

    for rel in relationships:
        if rel.from_table_id not in table_cache:
            table = session.get(Table, rel.from_table_id)
            if table:
                table_cache[rel.from_table_id] = table.table_name
        if rel.to_table_id not in table_cache:
            table = session.get(Table, rel.to_table_id)
            if table:
                table_cache[rel.to_table_id] = table.table_name
        if rel.from_column_id not in column_cache:
            col = session.get(Column, rel.from_column_id)
            if col:
                column_cache[rel.from_column_id] = col.column_name
        if rel.to_column_id not in column_cache:
            col = session.get(Column, rel.to_column_id)
            if col:
                column_cache[rel.to_column_id] = col.column_name

    # Established per-column annotations (DAT-723): each candidate side's
    # semantic_role / entity_type ride along as EVIDENCE, so the judge weighs
    # e.g. "period identifier × containment" itself — the candidates stay
    # unfiltered (evidence, never a pre-filter). Annotations are object-grain
    # (written under an add_source run), so the catalogue ``run_id`` scoping the
    # candidates above cannot scope them; mirroring load_persisted_annotations'
    # unit merge, take the MOST RECENT annotation per column (confidence as the
    # deterministic tiebreak).
    annotation_cache: dict[str, tuple[str | None, str | None]] = {}
    candidate_column_ids = {rel.from_column_id for rel in relationships} | {
        rel.to_column_id for rel in relationships
    }
    annotation_rows = session.execute(
        select(
            SemanticAnnotation.column_id,
            SemanticAnnotation.semantic_role,
            SemanticAnnotation.entity_type,
        )
        .where(SemanticAnnotation.column_id.in_(candidate_column_ids))
        .order_by(SemanticAnnotation.annotated_at.desc(), SemanticAnnotation.confidence.desc())
    ).all()
    for column_id, semantic_role, entity_type in annotation_rows:
        annotation_cache.setdefault(column_id, (semantic_role, entity_type))

    # Group by table pair
    # Key: (from_table_id, to_table_id) -> list of relationships
    grouped: dict[tuple[str, str], list[Relationship]] = {}
    for rel in relationships:
        key = (rel.from_table_id, rel.to_table_id)
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(rel)

    # Build output format
    result = []
    for (from_table_id, to_table_id), rels in grouped.items():
        table1 = table_cache.get(from_table_id, "?")
        table2 = table_cache.get(to_table_id, "?")

        # Get relationship-level metrics from first relationship's evidence
        # (these are the same for all relationships in the group)
        first_evidence = rels[0].evidence or {}
        introduces_duplicates = first_evidence.get("introduces_duplicates")

        # Build join columns list
        join_columns = []
        for rel in rels:
            col1 = column_cache.get(rel.from_column_id, "?")
            col2 = column_cache.get(rel.to_column_id, "?")
            evidence = rel.evidence or {}

            jc = {
                "column1": col1,
                "column2": col2,
                "confidence": rel.confidence,
                "cardinality": rel.cardinality or "unknown",
            }

            # Established annotations per side (DAT-723). Keyed off the STORED
            # from/to column ids, so they are direction-true through the DAT-777
            # orientation swap by construction. A column without an annotation
            # (or with a null field) serves NO key — absence, never a default.
            role1, entity1 = annotation_cache.get(rel.from_column_id, (None, None))
            role2, entity2 = annotation_cache.get(rel.to_column_id, (None, None))
            if role1 is not None:
                jc["column1_role"] = role1
            if entity1 is not None:
                jc["column1_entity_type"] = entity1
            if role2 is not None:
                jc["column2_role"] = role2
            if entity2 is not None:
                jc["column2_entity_type"] = entity2

            # Orientation evidence (DAT-725): the per-side uniqueness the detector
            # measured (finder.py) rides from the evidence JSON into the served
            # dict — the FK side of a real relationship is the non-unique side,
            # and the judge needs the asymmetry to orient. Dropped here before,
            # so the formatter's ``[uniq: L= R=]`` bracket never rendered on the
            # DB path (the only path the pipeline uses).
            if "left_uniqueness" in evidence:
                jc["left_uniqueness"] = evidence["left_uniqueness"]
            if "right_uniqueness" in evidence:
                jc["right_uniqueness"] = evidence["right_uniqueness"]

            # Add evaluation metrics if present in evidence. Both sides of each
            # pair are served: they are the same measurement on either endpoint
            # (DAT-725), and serving only the left one meant a row whose
            # endpoints had flipped rendered no orphan evidence at all.
            if "left_referential_integrity" in evidence:
                jc["left_referential_integrity"] = evidence["left_referential_integrity"]
            if "right_referential_integrity" in evidence:
                jc["right_referential_integrity"] = evidence["right_referential_integrity"]
            if "left_key_coverage" in evidence:
                jc["left_key_coverage"] = evidence["left_key_coverage"]
            if "right_key_coverage" in evidence:
                jc["right_key_coverage"] = evidence["right_key_coverage"]
            if "left_orphan_count" in evidence:
                jc["left_orphan_count"] = evidence["left_orphan_count"]
            if "right_orphan_count" in evidence:
                jc["right_orphan_count"] = evidence["right_orphan_count"]
            if "cardinality_verified" in evidence:
                jc["cardinality_verified"] = evidence["cardinality_verified"]

            join_columns.append(jc)

        candidate = {
            "table1": table1,
            "table2": table2,
            "join_columns": join_columns,
        }

        # Add optional relationship-level metrics
        if introduces_duplicates is not None:
            candidate["introduces_duplicates"] = introduces_duplicates

        result.append(candidate)

    return result
