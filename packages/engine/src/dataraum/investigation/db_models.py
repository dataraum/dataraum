"""Run-table anchor model (DAT-506).

The engine no longer models investigation sessions — sessions live in
cockpit_db. What the engine keeps is the per-run table set: which typed tables a
run (``run_id``) operated over. ``add_source`` and ``begin_session`` write their
own anchor at the start of the run; the terminal sealing step re-reads it, and
``operating_model`` reads the catalog head's run anchor.
"""

from __future__ import annotations

from sqlalchemy import ForeignKey, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage.base import Base


class RunTable(Base):
    """Association linking a run to the typed tables it operated over (DAT-506).

    The relational scope key for the analysis/readiness layer: the detect step,
    readiness persist, and promote run over *these* tables. A run's source(s) are
    derived by joining through to ``Table.source_id`` rather than stored here.
    Keyed by ``(run_id, table_id)`` — one row per typed table in the run.
    """

    __tablename__ = "run_tables"

    run_id: Mapped[str] = mapped_column(String, primary_key=True)
    table_id: Mapped[str] = mapped_column(
        ForeignKey("tables.table_id"),
        primary_key=True,
    )


Index("idx_run_tables_table", RunTable.table_id)
