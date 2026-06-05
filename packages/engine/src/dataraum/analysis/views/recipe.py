"""Session-scoped rebuild/reset of the session's views from their versioned recipes (DAT-415).

begin_session seals at one head (``session:{id}``/``detect``); there is no
per-view head, so the *current* view definitions are the latest stored
:class:`~dataraum.analysis.typing.db_models.MaterializationRecipe` per
``(fact, layer)`` for the session — sqlglot-gated storage (the ``enriched_views``
and ``slicing_view`` phases) re-stamps a fact's recipe only when its DDL changes,
so the most recent row per ``(fact, layer)`` IS that view's live definition.

:func:`rebuild_session_views` re-materializes the lake to that set across BOTH
view layers — enriched and slicing — re-executing each recipe transactionally in
cross-layer dependency order (a slicing view's ``depends_on`` names the enriched
view it projects from, so ``order_recipes_by_dependency`` rebuilds enriched
first). The lake is latest-only, so a reset is a re-materialization from the
versioned DDL, not a re-derivation of the phases. It then drops any physical view
this session produced in an earlier run that the current set no longer contains.
Scoped to the session's own recipes throughout, so a reset never touches another
session's views in the shared ``lake.typed`` schema. Forward-orphan GC of views
with no recipe at all is deferred to Slice C (DAT-416).
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

# The view layers materialized on the recipe substrate, rebuilt together in
# cross-layer dependency order (slicing projects from enriched).
_VIEW_LAYERS = ("enriched", "slicing")


def _bare_name(target_fqn: str) -> str:
    """``lake.typed."enriched_x"`` → ``enriched_x`` (the last quoted segment).

    Every view recipe ``target_fqn`` is composed via ``_lake_fqn`` and so always
    quotes the bare segment; a target without quotes is a corrupt recipe row —
    fail loud rather than silently mis-derive a name during a reset.
    """
    parts = target_fqn.split('"')
    if len(parts) < 3:
        raise ValueError(f"view recipe target_fqn is not a quoted FQN: {target_fqn!r}")
    return parts[-2]


def _view_fqn(layer: str, bare: str) -> str:
    """Compose the fully-qualified view name from its layer + bare name.

    Enriched and slicing views both resolve to the ``typed`` schema
    (``schema_for_layer``), so this re-derives the exact ``DROP`` target for a
    stray of either layer.
    """
    return f'{LAKE_CATALOG_ALIAS}.{schema_for_layer(layer)}."{bare}"'


def rebuild_session_views(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    *,
    session_id: str,
) -> list[str]:
    """Re-materialize the session's enriched + slicing views from their recipes + drop strays.

    Re-executes the current (latest-per-``(fact, layer)``) view recipes
    transactionally in cross-layer dependency order, then drops any physical view
    this session produced in an earlier run that the current set no longer
    contains. Atomic: a mid-rebuild failure rolls the whole reset back rather than
    leaving the lake half-materialized.

    Args:
        session: Active SQLAlchemy session.
        duckdb_conn: DuckDB connection (USE-scoped to ``lake.typed``) to execute against.
        session_id: The investigation session whose views to rebuild.

    Returns:
        The ``target_fqn``s rebuilt, in execution order.
    """
    # All of the session's view recipes, newest first. The first row seen per
    # ``(fact, layer)`` is its live definition; the full set is the universe of
    # view names this session has ever produced (for safe, session-scoped strays).
    rows = list(
        session.execute(
            select(MaterializationRecipe)
            .where(
                MaterializationRecipe.session_id == session_id,
                MaterializationRecipe.layer.in_(_VIEW_LAYERS),
            )
            .order_by(MaterializationRecipe.created_at.desc())
        ).scalars()
    )
    latest: dict[tuple[str, str], MaterializationRecipe] = {}
    session_targets: set[tuple[str, str]] = set()
    for recipe in rows:
        latest.setdefault((recipe.table_id, recipe.layer), recipe)
        session_targets.add((recipe.layer, _bare_name(recipe.target_fqn)))

    target = list(latest.values())
    target_set = {(r.layer, _bare_name(r.target_fqn)) for r in target}
    strays = session_targets - target_set

    rebuilt: list[str] = []
    duckdb_conn.execute("BEGIN TRANSACTION")
    try:
        for recipe in order_recipes_by_dependency(target):
            duckdb_conn.execute(recipe.ddl)
            rebuilt.append(recipe.target_fqn)
        for layer, bare in strays:
            duckdb_conn.execute(f"DROP VIEW IF EXISTS {_view_fqn(layer, bare)}")
    except Exception:
        duckdb_conn.execute("ROLLBACK")
        raise
    duckdb_conn.execute("COMMIT")

    logger.info(
        "session_views_rebuilt",
        session_id=session_id,
        rebuilt=len(rebuilt),
        dropped=len(strays),
    )
    return rebuilt
