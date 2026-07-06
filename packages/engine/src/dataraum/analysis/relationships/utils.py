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
    min_confidence: float | None = None,
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
    - ``min_confidence``: optional confidence floor.
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
    if min_confidence is not None:
        stmt = stmt.where(Relationship.confidence >= min_confidence)
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


def load_suppressed_relationship_pairs(session: Session) -> set[tuple[str, str]]:
    """Directional column pairs the user has dropped (``action == "reject"``).

    A re-run must not re-create a suppressed relationship, and its readiness must not
    surface. Directional: rejecting ``(a, b)`` does not reject ``(b, a)``.
    """
    return set(relationship_overlay_pairs(session, "reject"))


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
                "join_success_rate": 95.0,
                "introduces_duplicates": False,
                "join_columns": [
                    {
                        "column1": "...",
                        "column2": "...",
                        "confidence": 0.9,
                        "cardinality": "one-to-many",
                        "left_referential_integrity": 100.0,
                        "right_referential_integrity": 85.0,
                        "orphan_count": 5,
                        "cardinality_verified": True,
                    }
                ]
            }
        ]
    """
    # Build query — scoped to the current run's catalog (DAT-408) when ``run_id`` is
    # given, so coexisting prior-run rows aren't fed to the LLM twice.
    stmt = select(Relationship).where(Relationship.detection_method == detection_method)

    if run_id is not None:
        stmt = stmt.where(Relationship.run_id == run_id)

    if table_ids:
        stmt = stmt.where(
            (Relationship.from_table_id.in_(table_ids)) | (Relationship.to_table_id.in_(table_ids))
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
        join_success_rate = first_evidence.get("join_success_rate")
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

            # Add evaluation metrics if present in evidence
            if "left_referential_integrity" in evidence:
                jc["left_referential_integrity"] = evidence["left_referential_integrity"]
            if "right_referential_integrity" in evidence:
                jc["right_referential_integrity"] = evidence["right_referential_integrity"]
            if "orphan_count" in evidence:
                jc["orphan_count"] = evidence["orphan_count"]
            if "cardinality_verified" in evidence:
                jc["cardinality_verified"] = evidence["cardinality_verified"]

            join_columns.append(jc)

        candidate = {
            "table1": table1,
            "table2": table2,
            "join_columns": join_columns,
        }

        # Add optional relationship-level metrics
        if join_success_rate is not None:
            candidate["join_success_rate"] = join_success_rate
        if introduces_duplicates is not None:
            candidate["introduces_duplicates"] = introduces_duplicates

        result.append(candidate)

    return result
