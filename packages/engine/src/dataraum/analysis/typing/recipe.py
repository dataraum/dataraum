"""Versioned materialization recipes for typed/quarantine artifacts (DAT-414).

Typing materializes its physical DuckDB tables by executing a
``CREATE OR REPLACE TABLE … AS SELECT`` string. This module captures that string
as versioned metadata: :func:`store_recipe` persists it stamped with the run's
``run_id``, and :func:`rebuild_from_recipe` re-executes a stored run's DDL to
rebuild the physical artifact — **without** re-deriving the typing phase.

Two consumers:
- The typing phase, which after building each typed/quarantine table records the
  exact DDL it just executed (emit → store → execute).
- A physical reset/rebuild, which flips the snapshot head to a prior run and
  replays that run's stored DDL in dependency order (the lake is latest-only, so
  a reset is a re-materialization from the versioned recipe, not a re-typing).
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
    session_id: str,
    table_id: str,
    layer: str,
    run_id: str | None,
    target_fqn: str,
    ddl: str,
    depends_on: list[str] | None = None,
) -> None:
    """Persist the ``CREATE TABLE`` DDL for one typed/quarantine artifact.

    Upserts on the ``(table_id, layer, run_id)`` grain so a Temporal
    at-least-once retry (same ``run_id``) is idempotent — re-running typing
    overwrites the run's recipe rather than duplicating it — while a NEW run's
    recipe coexists with prior runs'. The promoted snapshot head names which run
    is current (DAT-413). ``run_id=None`` (non-run callers) is NOT dedup-keyed —
    Postgres treats a NULL ``run_id`` as distinct in the unique constraint, so
    repeated NULL-run writes accrue rows; the Temporal path always stamps a
    ``run_id``, so production never hits this.

    Args:
        session: Active SQLAlchemy session.
        session_id: Owning investigation session.
        table_id: The *typed* Table id whose physical artifact the DDL produces
            (stable across re-types, DAT-373).
        layer: Produced lake layer — ``"typed"`` or ``"quarantine"``.
        run_id: The run that emitted this DDL (DAT-413). ``None`` for non-run
            callers, matching the TypeDecision/TypeCandidate convention.
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
                "session_id": session_id,
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


def rebuild_from_recipe(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    *,
    table_id: str,
    run_id: str,
) -> list[str]:
    """Rebuild one Table's physical artifact from a run's stored DDL.

    Re-executes the ``(table_id, run_id)`` recipes against DuckDB in dependency
    order, reproducing the table's DATA as the original run produced it — no
    typing re-derivation. The recipe versions the *transformation*, not the data:
    a faithful re-execution reproduces identical rows, while audit columns the DDL
    writes (the quarantine ``_quarantined_at`` ``CURRENT_TIMESTAMP``) re-stamp to
    the rebuild time.

    Args:
        session: Active SQLAlchemy session.
        duckdb_conn: DuckDB connection to execute the DDL against.
        table_id: The Table whose artifact to rebuild.
        run_id: The run whose stored recipes to replay.

    Returns:
        The ``target_fqn``s rebuilt, in execution order.

    Raises:
        RuntimeError: If no recipe is stored for ``(table_id, run_id)``.
    """
    recipes = _load_recipes(session, table_id, run_id)
    if not recipes:
        raise RuntimeError(
            f"No materialization recipe for table {table_id} at run {run_id} — "
            "nothing to rebuild (was the recipe stored during typing?)."
        )
    rebuilt = _replay(duckdb_conn, recipes)
    logger.info(
        "materialization_rebuilt",
        table_id=table_id,
        run_id=run_id,
        artifacts=len(rebuilt),
    )
    return rebuilt


def reset_to_run(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    *,
    table_id: str,
    run_id: str,
) -> list[str]:
    """Reset a typed Table's physical artifacts to a prior run (DAT-414 AC#3).

    Re-executes ``run_id``'s stored materialization DDL — for the typed artifact
    AND its quarantine sibling — then flips the typing snapshot head for
    ``table:{table_id}`` to ``run_id``. Typed and quarantine are separate Table
    rows (distinct ``table_id``s) for one logical table, so the reset rebuilds the
    pair together in one transaction; resetting only the typed half would leave the
    lake at typed@``run_id`` / quarantine@whatever-materialized-last. No phase
    re-derivation — the versioned recipe is the source of truth; the lake is
    latest-only, so "reset" re-materializes the target run's recipe over the
    current physical tables.

    ``table_id`` is the *typed* Table id (the head key). A run with no cast
    failures — or the strongly-typed copy — has no quarantine recipe, so only the
    typed artifact is rebuilt.

    Args:
        session: Active SQLAlchemy session.
        duckdb_conn: DuckDB connection to execute the DDL against.
        table_id: The typed Table to reset.
        run_id: The run to reset the physical artifacts to.

    Returns:
        The ``target_fqn``s rebuilt, in execution order.

    Raises:
        RuntimeError: If no recipe is stored for the typed ``(table_id, run_id)``.
    """
    recipes = _load_recipes(session, table_id, run_id)
    if not recipes:
        raise RuntimeError(
            f"No materialization recipe for table {table_id} at run {run_id} — "
            "nothing to reset (was the recipe stored during typing?)."
        )
    # The pair resets together: pull the quarantine sibling's recipes (if this run
    # produced one) so the lake can't end up typed@run / quarantine@another-run.
    quarantine_id = _quarantine_sibling_id(session, table_id)
    if quarantine_id is not None and quarantine_id != table_id:
        recipes += _load_recipes(session, quarantine_id, run_id)
    rebuilt = _replay(duckdb_conn, recipes)
    _point_head(session, table_id, run_id)
    logger.info(
        "materialization_reset",
        table_id=table_id,
        run_id=run_id,
        artifacts=len(rebuilt),
    )
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


def _point_head(session: Session, table_id: str, run_id: str) -> None:
    """Flip the ``(table:{id}, "typing")`` snapshot head to ``run_id``.

    Mirrors ``worker.activity._upsert_head`` but scoped to the single typing
    head a physical reset re-points — inserts at ``version=0`` if absent, else
    re-points + bumps the version. Kept local to the typing module so a reset
    does not depend on the worker package.
    """
    from datetime import UTC, datetime

    from dataraum.storage.snapshot_head import MetadataSnapshotHead

    target = f"table:{table_id}"
    stage = "typing"
    now = datetime.now(UTC)
    head = session.execute(
        select(MetadataSnapshotHead).where(
            MetadataSnapshotHead.target == target,
            MetadataSnapshotHead.stage == stage,
        )
    ).scalar_one_or_none()
    if head is None:
        session.add(
            MetadataSnapshotHead(
                target=target, stage=stage, run_id=run_id, promoted_at=now, version=0
            )
        )
    else:
        head.run_id = run_id
        head.promoted_at = now
        head.version = head.version + 1


def current_typing_run(session: Session, table_id: str) -> str | None:
    """The promoted typing ``run_id`` for ``table_id``, or ``None`` (DAT-413).

    Convenience over ``head_run_id`` for the typing stage's head key.
    """
    return head_run_id(session, f"table:{table_id}", "typing")
