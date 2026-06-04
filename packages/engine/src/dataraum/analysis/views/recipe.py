"""Session-scoped rebuild/reset of enriched views from their versioned recipes (DAT-415).

begin_session seals at one head (``session:{id}``/``detect``); there is no
per-view head, so the *current* enriched view definitions are the latest stored
:class:`~dataraum.analysis.typing.db_models.MaterializationRecipe` (``layer=
"enriched"``) per fact for the session — sqlglot-gated storage (the
``enriched_views`` phase) re-stamps a fact's recipe only when its DDL changes, so
the most recent row per fact IS that fact's live definition.

:func:`rebuild_enriched_views` re-materializes the lake to that set —
re-executing each recipe transactionally in dependency order (the lake is
latest-only, so a reset is a re-materialization from the versioned DDL, not a
re-derivation of the phase) — and drops any physical enriched view this session
produced in an earlier run that the current set no longer contains. Scoped to the
session's own recipes throughout, so a reset never touches another session's
views in the shared ``lake.typed`` schema. Forward-orphan GC of views with no
recipe at all is deferred to Slice C (DAT-416).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from dataraum.analysis.typing.db_models import MaterializationRecipe
from dataraum.analysis.typing.recipe import order_recipes_by_dependency
from dataraum.core.duckdb_naming import schema_for_layer
from dataraum.core.logging import get_logger
from dataraum.server.storage import LAKE_CATALOG_ALIAS

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

logger = get_logger(__name__)


def _bare_name(target_fqn: str) -> str:
    """``lake.typed."enriched_x"`` → ``enriched_x`` (the last quoted segment)."""
    return target_fqn.split('"')[-2]


def _enriched_fqn(bare: str) -> str:
    """Compose the fully-qualified enriched-view name from its bare name."""
    return f'{LAKE_CATALOG_ALIAS}.{schema_for_layer("enriched")}."{bare}"'


def rebuild_enriched_views(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    *,
    session_id: str,
) -> list[str]:
    """Re-materialize the session's enriched views from their recipes + drop strays.

    Re-executes the current (latest-per-fact) enriched recipes transactionally in
    dependency order, then drops any physical enriched view this session produced
    in an earlier run that the current set no longer contains. Atomic: a
    mid-rebuild failure rolls the whole reset back rather than leaving the lake
    half-materialized.

    Args:
        session: Active SQLAlchemy session.
        duckdb_conn: DuckDB connection (USE-scoped to ``lake.typed``) to execute against.
        session_id: The investigation session whose enriched views to rebuild.

    Returns:
        The ``target_fqn``s rebuilt, in execution order.
    """
    # All of the session's enriched recipes, newest first. The first row seen per
    # fact (``table_id``) is its live definition; the full set is the universe of
    # view names this session has ever produced (for safe, session-scoped strays).
    rows = list(
        session.execute(
            select(MaterializationRecipe)
            .where(
                MaterializationRecipe.session_id == session_id,
                MaterializationRecipe.layer == "enriched",
            )
            .order_by(MaterializationRecipe.created_at.desc())
        ).scalars()
    )
    latest: dict[str, MaterializationRecipe] = {}
    session_bares: set[str] = set()
    for recipe in rows:
        latest.setdefault(recipe.table_id, recipe)
        session_bares.add(_bare_name(recipe.target_fqn))

    target = list(latest.values())
    target_bares = {_bare_name(r.target_fqn) for r in target}
    stray_bares = session_bares - target_bares

    rebuilt: list[str] = []
    duckdb_conn.execute("BEGIN TRANSACTION")
    try:
        for recipe in order_recipes_by_dependency(target):
            duckdb_conn.execute(recipe.ddl)
            rebuilt.append(recipe.target_fqn)
        for bare in stray_bares:
            duckdb_conn.execute(f"DROP VIEW IF EXISTS {_enriched_fqn(bare)}")
    except Exception:
        duckdb_conn.execute("ROLLBACK")
        raise
    duckdb_conn.execute("COMMIT")

    logger.info(
        "enriched_views_rebuilt",
        session_id=session_id,
        rebuilt=len(rebuilt),
        dropped=len(stray_bares),
    )
    return rebuilt
