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
    is current (DAT-413).

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


def _order_by_dependency(recipes: list[MaterializationRecipe]) -> list[MaterializationRecipe]:
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


def rebuild_from_recipe(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    *,
    table_id: str,
    run_id: str,
) -> list[str]:
    """Rebuild a typed Table's physical artifacts from a run's stored DDL.

    Re-executes the ``(table_id, run_id)`` recipes against DuckDB in dependency
    order, recreating the physical ``typed``/``quarantine`` tables exactly as the
    original run produced them. No typing re-derivation — the versioned DDL string
    is the source of truth.

    Args:
        session: Active SQLAlchemy session.
        duckdb_conn: DuckDB connection to execute the DDL against.
        table_id: The typed Table whose artifacts to rebuild.
        run_id: The run whose stored recipes to replay.

    Returns:
        The ``target_fqn``s rebuilt, in execution order.

    Raises:
        RuntimeError: If no recipe is stored for ``(table_id, run_id)``.
    """
    recipes = list(
        session.execute(
            select(MaterializationRecipe).where(
                MaterializationRecipe.table_id == table_id,
                MaterializationRecipe.run_id == run_id,
            )
        ).scalars()
    )
    if not recipes:
        raise RuntimeError(
            f"No materialization recipe for table {table_id} at run {run_id} — "
            "nothing to rebuild (was the recipe stored during typing?)."
        )

    rebuilt: list[str] = []
    for recipe in _order_by_dependency(recipes):
        duckdb_conn.execute(recipe.ddl)
        rebuilt.append(recipe.target_fqn)

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

    Flips the typing snapshot head for ``table:{table_id}`` to ``run_id`` and
    re-executes that run's stored materialization DDL, rebuilding the physical
    DuckDB tables from the versioned recipe **without** a full phase
    re-derivation. The lake is latest-only, so "reset" means re-materializing the
    target run's recipe over the current physical table.

    Args:
        session: Active SQLAlchemy session.
        duckdb_conn: DuckDB connection to execute the DDL against.
        table_id: The typed Table to reset.
        run_id: The run to reset the physical artifact to.

    Returns:
        The ``target_fqn``s rebuilt, in execution order.

    Raises:
        RuntimeError: If no recipe is stored for ``(table_id, run_id)``.
    """
    rebuilt = rebuild_from_recipe(session, duckdb_conn, table_id=table_id, run_id=run_id)
    _point_head(session, table_id, run_id)
    return rebuilt


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
