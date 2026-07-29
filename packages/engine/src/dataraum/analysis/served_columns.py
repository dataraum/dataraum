"""The one home of "which columns does a surface serve?" (DAT-811, DAT-878).

Two independent exclusions live here, because both answer that same question and
both were previously re-derived by hand at every call site:

- **Mint-owned surrogate join keys** (``_sk__*``, DAT-277). These are real
  physical columns on the typed tables — the mint amends the typing DDL with
  ``SELECT *, md5(…) AS "_sk__…"`` (``relationships/surrogate.py``) and registers
  a real ``Column`` row for each — so *every* consumer that reads columns from
  the catalog or DESCRIBEs a relation sees them unless it says otherwise. They
  are machinery, not business columns: nothing a user named, nothing an LLM
  should be offered as an analysable attribute, and (being a deterministic hash
  of its own components) a column whose sample values are raw md5 hex and whose
  functional dependencies are true by construction.
- **An enriched view's fact passthrough** (``origin='fact'``, DAT-811), for
  consumers that want only the *added* dimension columns.

The invariant this module exists to hold: a filter that has to be repeated is a
filter that gets dropped. Before DAT-878 the surrogate exclusion existed five
times across three unrelated context builders while six other builders had none,
so the same column was a business attribute in one prompt and machinery in the
next. Routing the reads through here makes a surface surrogate-free
*structurally*, not incidentally.

**Where the exclusion does NOT belong.** A surrogate pair IS the relationship
once a composite key is cured, so join evidence, join composition and the
graph's vertex maps must keep seeing them:

- ``graphs/context_reads.py::_read_references`` resolves FK endpoint names
  through its own ``og_columns`` vertex map — a separate read from the served
  column load, deliberately unfiltered.
- ``pipeline/phases/enriched_views_phase.py`` resolves each ``DimensionJoin``'s
  legs from the *unfiltered* column map; only the LLM-facing projection filters.
- ``analysis/catalogue/context.py::_format_enriched_views`` renders
  ``joins t.col -> t2.col`` lines with surrogate names intact.

So the rule is per-*surface*, not per-table: filter what is SERVED, never what
is JOINED.

**The predicate is a strict PREFIX test, and that is not an accident.**
``is_surrogate_column`` matches ``_sk__`` only at position 0 — a substring test
would be a guess about user data (a real column may contain that text), which
this codebase does not do. One consequence has to be designed around rather than
patched: the enriched-view builder RENAMES joined dimension columns to
``{fact_fk}__{col}``, so a dimension-side surrogate would land as
``account_id___sk__account__business_id`` and escape the predicate. Detection
after the fact therefore cannot close that case. It is closed at the source
instead — the enrichment agent is never offered a surrogate as a candidate
(``enriched_views_phase._build_context_data``), so the renamed form is never
created. If that filter is ever dropped, this becomes an undetectable leak.
"""

from __future__ import annotations

from collections.abc import Iterable

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.relationships.surrogate import is_surrogate_column
from dataraum.storage import Column


def served_columns(columns: Iterable[Column]) -> list[Column]:
    """``columns`` minus the mint-owned surrogate join keys.

    The ORM-side entry point for every surface that serves a column list to an
    LLM prompt, a user-facing catalog, or a persisted inventory. Takes an
    already-loaded iterable rather than issuing its own query, because the call
    sites legitimately differ in how they load (``selectinload(Table.columns)``,
    ``select(Column).where(table_id.in_(…))``, a per-table query) — the shared
    thing is the exclusion, not the query shape.

    Rows that are not ``Column`` ORM objects (e.g. an annotation-join ``Row``)
    call :func:`is_surrogate_column` directly; that predicate, not this helper,
    is the seam.

    Args:
        columns: the loaded column rows a surface is about to serve.

    Returns:
        The same rows, in order, without the surrogates.
    """
    return [c for c in columns if not is_surrogate_column(c.column_name)]


def describe_served(
    duckdb_conn: duckdb.DuckDBPyConnection,
    relation: str,
) -> list[tuple[str, str]]:
    """``(name, type)`` pairs for a physical relation's served columns.

    The physical-side twin of :func:`served_columns`, for the two paths that
    cannot filter through the ORM because they read the relation itself. Both
    kinds of relation carry surrogates — a typed table from the mint's DDL
    amendment, an enriched view through its ``f.*`` fact passthrough — so
    neither is safe to DESCRIBE raw.

    Args:
        duckdb_conn: connection to DESCRIBE through.
        relation: the table or view name.

    Returns:
        ``(column_name, column_type)`` in physical order, without surrogates.

    Raises:
        duckdb.Error: if the relation is not queryable. Callers that treat a
            missing relation as skippable catch it themselves — this helper does
            not swallow it, so absence falls loud by default.
    """
    rows = duckdb_conn.execute(f'DESCRIBE "{relation}"').fetchall()
    return [(str(r[0]), str(r[1])) for r in rows if not is_surrogate_column(str(r[0]))]


def enriched_dimension_columns(session: Session, view_table_id: str) -> list[Column]:
    """The JOINED dimension columns of an enriched view (``origin='dimension'``).

    Excludes the fact's own ``f.*`` passthrough columns (``origin='fact'``), which are
    already carried by the fact table itself — a consumer counting the *added*
    dimensions must not double-count them.

    No surrogate filter here, deliberately: ``origin='dimension'`` rows are always
    RENAMED to ``{fact_fk}__{col}`` by the view builder, so a dimension-side
    surrogate would not match the prefix predicate anyway (see this module's note
    on the renamed form). That class is closed by prevention at the enrichment
    agent's candidate list, not by detection here.
    """
    return list(
        session.execute(
            select(Column).where(
                Column.table_id == view_table_id,
                Column.origin == "dimension",
            )
        ).scalars()
    )


__all__ = ["describe_served", "enriched_dimension_columns", "served_columns"]
