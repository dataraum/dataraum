"""Versioned materialization recipes for typed/quarantine artifacts (DAT-414).

Typing materializes its physical DuckDB tables by executing a
``CREATE OR REPLACE TABLE … AS SELECT`` string. This module captures that string
as versioned metadata: :func:`store_recipe` persists it stamped with the run's
``run_id``, captured so a stored run's DDL can be replayed to rebuild the
physical artifact — **without** re-deriving the typing phase.

Consumer: the typing phase, which after building each typed/quarantine table
records the exact DDL it just executed (emit → store → execute).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from dataraum.analysis.typing.db_models import MaterializationRecipe
from dataraum.core.logging import get_logger
from dataraum.storage import Table
from dataraum.storage.snapshot_head import head_run_id
from dataraum.storage.upsert import upsert

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

logger = get_logger(__name__)


def store_recipe(
    session: Session,
    *,
    table_id: str,
    layer: str,
    run_id: str,
    target_fqn: str,
    ddl: str,
    depends_on: list[str] | None = None,
) -> None:
    """Persist the ``CREATE TABLE`` DDL for one typed/quarantine artifact.

    Upserts on the ``(table_id, layer, run_id)`` grain so a Temporal
    at-least-once retry (same ``run_id``) is idempotent — re-running typing
    overwrites the run's recipe rather than duplicating it — while a NEW run's
    recipe coexists with prior runs'. The promoted snapshot head names which run
    is current (DAT-413).

    Args:
        session: Active SQLAlchemy session.
        table_id: The *typed* Table id whose physical artifact the DDL produces
            (stable across re-types, DAT-373).
        layer: Produced lake layer — ``"typed"`` / ``"quarantine"`` (typing) or the
            view layers ``"enriched"`` / ``"slicing"`` (DAT-415). An open VARCHAR,
            not an enum; the dependency-order rebuild is layer-aware via the FQNs.
        run_id: The run that emitted this DDL (DAT-413).
        target_fqn: Fully-qualified DuckDB target the DDL creates.
        ddl: The exact ``CREATE OR REPLACE TABLE … AS SELECT`` string.
        depends_on: Fully-qualified DuckDB names this DDL reads from
            (e.g. ``lake.raw."x"``), for dependency-order rebuild. Layer-qualified
            so a rebuild never confuses the raw input with the same-bare-named
            typed/quarantine artifacts. ``None`` is stored as no dependency.
    """
    upsert(
        session,
        MaterializationRecipe,
        [
            {
                "table_id": table_id,
                "layer": layer,
                "run_id": run_id,
                "target_fqn": target_fqn,
                "ddl": ddl,
                "depends_on": depends_on,
            }
        ],
        index_elements=["table_id", "layer", "run_id"],
    )


def order_recipes_by_dependency(
    recipes: list[MaterializationRecipe],
) -> list[MaterializationRecipe]:
    """Order recipes so a DDL runs after the artifacts it reads from.

    Each recipe's ``target_fqn`` is matched against the ``depends_on`` of the
    others. Both are fully-qualified DuckDB names (``lake.<layer>."<bare>"``), so
    the match is layer-aware — a typed recipe depending on ``lake.raw."x"`` does
    NOT spuriously match a quarantine recipe producing ``lake.quarantine."x"``
    (raw/typed/quarantine share the same bare name; only the schema differs).
    Typed/quarantine both read only the raw layer (not each other), so the set is
    independent today; the topological pass future-proofs the multi-level view
    chains in Slice B (DAT-415). A dependency that is not itself a recipe in the
    set (e.g. the raw table) is treated as already present.
    """
    # Map each recipe by the fully-qualified target it produces, so a dependent's
    # ``depends_on`` FQN can reference it precisely.
    produced: dict[str, MaterializationRecipe] = {r.target_fqn: r for r in recipes}

    ordered: list[MaterializationRecipe] = []
    # Key visited/placed sets on Python object identity, not ``recipe_id``: an
    # unpersisted recipe (constructed but not yet flushed) has a NULL PK, so all
    # would collapse to one key. ``id()`` is stable for the duration of this call.
    placed: set[int] = set()

    def _visit(recipe: MaterializationRecipe, seen: set[int]) -> None:
        key = id(recipe)
        if key in placed:
            return
        if key in seen:
            # A cycle is a bug in the recipe graph — break it rather than recurse
            # forever; the artifact still materializes (its deps just may lag).
            logger.warning("materialization_recipe_cycle", target_fqn=recipe.target_fqn)
            return
        seen.add(key)
        for dep in recipe.depends_on or []:
            dep_recipe = produced.get(dep)
            if dep_recipe is not None:
                _visit(dep_recipe, seen)
        placed.add(key)
        ordered.append(recipe)

    for r in recipes:
        _visit(r, set())
    return ordered


def _load_recipes(session: Session, table_id: str, run_id: str) -> list[MaterializationRecipe]:
    """The stored recipes for one ``(table_id, run_id)`` grain (empty if none)."""
    return list(
        session.execute(
            select(MaterializationRecipe).where(
                MaterializationRecipe.table_id == table_id,
                MaterializationRecipe.run_id == run_id,
            )
        ).scalars()
    )


def _replay(
    duckdb_conn: duckdb.DuckDBPyConnection, recipes: list[MaterializationRecipe]
) -> list[str]:
    """Re-execute recipes in dependency order, atomically.

    Wrapped in one DuckDB transaction so a mid-chain failure rolls the whole
    rebuild back rather than leaving the lake half-materialized. The set is
    independent today (typed/quarantine read only the raw layer), but the
    all-or-nothing guarantee is load-bearing once the multi-level view chains land
    on this substrate (Slice B, DAT-415).
    """
    rebuilt: list[str] = []
    duckdb_conn.execute("BEGIN TRANSACTION")
    try:
        for recipe in order_recipes_by_dependency(recipes):
            duckdb_conn.execute(recipe.ddl)
            rebuilt.append(recipe.target_fqn)
    except Exception:
        duckdb_conn.execute("ROLLBACK")
        raise
    duckdb_conn.execute("COMMIT")
    return rebuilt


def _quarantine_sibling_id(session: Session, typed_table_id: str) -> str | None:
    """The quarantine Table id sharing the typed table's ``(source, name)``, or ``None``.

    Typed and quarantine are separate Table rows for one logical table,
    discriminated by ``layer``; a reset rebuilds the pair together. A run with no
    cast failures — or the strongly-typed copy — produces no quarantine artifact.
    """
    typed = session.get(Table, typed_table_id)
    if typed is None:
        return None
    quarantine = session.execute(
        select(Table).where(
            Table.source_id == typed.source_id,
            Table.table_name == typed.table_name,
            Table.layer == "quarantine",
        )
    ).scalar_one_or_none()
    return quarantine.table_id if quarantine is not None else None


def current_typing_run(session: Session, table_id: str) -> str | None:
    """The promoted typing ``run_id`` for ``table_id``, or ``None`` (DAT-413).

    Convenience over ``head_run_id`` for the typing stage's head key.
    """
    return head_run_id(session, f"table:{table_id}", "typing")
