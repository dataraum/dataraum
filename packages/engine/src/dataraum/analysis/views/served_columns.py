"""Read helpers over an enriched view's served columns (DAT-811).

An enriched view registers EVERY served column under its ``view_table_id`` — the
fact's own ``f.*`` passthrough columns (``origin='fact'``) AND the joined dimension
columns (``origin='dimension'``). Consumers that want only the added dimensions filter
on ``origin`` in ONE place here, so the filter cannot be silently dropped in one
consumer and kept in another.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.storage import Column


def enriched_dimension_columns(session: Session, view_table_id: str) -> list[Column]:
    """The JOINED dimension columns of an enriched view (``origin='dimension'``).

    Excludes the fact's own ``f.*`` passthrough columns (``origin='fact'``), which are
    already carried by the fact table itself — a consumer counting the *added*
    dimensions must not double-count them.
    """
    return list(
        session.execute(
            select(Column).where(
                Column.table_id == view_table_id,
                Column.origin == "dimension",
            )
        ).scalars()
    )


__all__ = ["enriched_dimension_columns"]
