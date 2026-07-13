"""Dialect-aware insert-or-update helper.

Temporal activities are AT-LEAST-ONCE: a worker can commit rows then crash
before acking, so an activity re-runs with the SAME ``run_id`` and would write
duplicate rows. For one-row-per-column metadata models that carry a
``(column_id, run_id)`` unique key, an upsert makes the write idempotent under
those retries (and the head-resolved loaders' ``scalar_one_or_none()`` stays
single-valued).

Prod is Postgres; unit tests run SQLite (in-memory StaticPool). SQLAlchemy's
``on_conflict_do_update`` is dialect-specific and Core has no agnostic form, so
this helper picks the right dialect ``insert`` at call time.
"""

from __future__ import annotations

from typing import Any, cast

from sqlalchemy import CursorResult
from sqlalchemy.orm import Session


def insert_if_absent(
    session: Session,
    model: Any,
    rows: list[dict[str, Any]],
    *,
    index_elements: list[str],
    index_where: Any = None,
) -> int:
    """Dialect-aware ``INSERT … ON CONFLICT DO NOTHING`` — insert-or-skip.

    For SEED / idempotent writes where a conflicting row must be LEFT AS-IS (not
    updated): a concept seed must never clobber a ``frame`` edit, and must be
    race-safe against a concurrent seed/write on its unique index (the read-then-
    insert it replaces has a TOCTOU window — two callers both see the row absent
    and both insert, the second raising ``IntegrityError``). ``ON CONFLICT DO
    NOTHING`` closes that window at the DB. ``index_where`` is a WHERE clause (e.g.
    ``text("superseded_at IS NULL")``) targeting a PARTIAL unique index (Postgres +
    SQLite both accept a partial-index conflict target; the ``WHERE`` must match the
    index's). Returns the number of rows actually inserted (conflicts excluded).

    Omit uuid PKs / defaulted columns from ``rows`` so the model's Python-side
    ``default`` applies (same contract as :func:`upsert`). ``rows`` must be
    non-empty (the caller guards — an empty INSERT has nothing to skip).
    """
    name = session.get_bind().dialect.name
    stmt: Any
    if name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        stmt = pg_insert(model).values(rows)
    elif name == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        stmt = sqlite_insert(model).values(rows)
    else:
        raise RuntimeError(f"insert_if_absent: unsupported dialect {name!r}")
    kwargs: dict[str, Any] = {"index_elements": index_elements}
    if index_where is not None:
        kwargs["index_where"] = index_where
    # Session.execute is typed Result[Any]; a DML statement yields a CursorResult
    # (the .rowcount carrier) at runtime.
    result = cast("CursorResult[Any]", session.execute(stmt.on_conflict_do_nothing(**kwargs)))
    return result.rowcount


def upsert(
    session: Session, model: Any, rows: list[dict[str, Any]], *, index_elements: list[str]
) -> None:
    """Dialect-aware insert-or-update on conflict.

    Postgres (prod) + SQLite (tests) both expose ``on_conflict_do_update`` via
    their dialect ``insert``; Core has no agnostic form.

    Args:
        session: Active SQLAlchemy session bound to a Postgres or SQLite engine.
        model: The ORM model to write rows for.
        rows: Row dicts. Omit the uuid PK so the model's Python-side ``default``
            applies (verified on SQLite for ``insert().values([...])``).
        index_elements: The conflict-target columns (the unique key), e.g.
            ``["column_id", "run_id"]``.
    """
    if not rows:
        return
    name = session.get_bind().dialect.name
    # Postgres and SQLite return distinct dialect ``Insert`` types that share the
    # ``.values()`` / ``.excluded`` / ``.on_conflict_do_update()`` surface; ``Any``
    # bridges them so the two branches can assign one ``stmt``.
    stmt: Any
    if name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        stmt = pg_insert(model).values(rows)
    elif name == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        stmt = sqlite_insert(model).values(rows)
    else:
        raise RuntimeError(f"upsert: unsupported dialect {name!r}")
    update = {
        c.name: stmt.excluded[c.name]
        for c in model.__table__.columns
        if c.name not in index_elements and not c.primary_key
    }
    session.execute(stmt.on_conflict_do_update(index_elements=index_elements, set_=update))
