"""Surrogate-key mint helpers (DAT-277).

An LLM-confirmed composite key (a fan-out anchor plus its scoping columns) is
cured at the SOURCE, dbt-style: both typed tables gain ONE deterministic hash
column over the composite's components, and the catalog persists an ordinary
single-column relationship on the surrogate pair. Every downstream consumer —
enriched views, the SQL agents, drivers, grounding — stays single-column; no
multi-column ON machinery exists anywhere.

Two deliberate deviations from dbt's ``generate_surrogate_key`` macro, both
because this surrogate is a JOIN key, not a primary key:

- **NULL propagates** (``||``, never ``concat_ws``/``coalesce``): any NULL
  component makes the surrogate NULL, so a LEFT JOIN simply doesn't match —
  standard FK semantics. dbt's placeholder would false-join NULL↔NULL rows.
- **Every component casts to VARCHAR with a ``|`` delimiter** so ``('ab','c')``
  never collides with ``('a','bc')``. A delimiter INSIDE a value can still
  collide two tuples — accepted, as dbt does.

The mint rides the DAT-414 materialization-recipe substrate: the typing run's
``CREATE OR REPLACE TABLE … AS SELECT`` DDL is wrapped (never edited in place)
with the surrogate projections, stored run-stamped, and executed. Rebuilding
from raw keeps hash values in lockstep with the data by construction.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

SURROGATE_PREFIX = "_sk__"

# The one boundary in the typing DDL shape (``CREATE OR REPLACE TABLE {typed}
# AS SELECT {selects} FROM {raw}``): column aliases are always quoted
# (``AS "name"``) and cast targets are type names (``AS BIGINT``), so the bare
# ``AS SELECT`` occurs exactly once, at the head.
_DDL_BOUNDARY = " AS SELECT "


@dataclass(frozen=True)
class SurrogateSpec:
    """One surrogate column to mint on one typed table."""

    table_id: str
    column_name: str
    component_names: tuple[str, ...]

    @property
    def hash_expr(self) -> str:
        """The NULL-propagating DuckDB hash expression over the components."""
        parts = " || '|' || ".join(f'"{c}"::VARCHAR' for c in self.component_names)
        return f"md5({parts})"


def surrogate_column_name(component_names: list[str]) -> str:
    """The deterministic surrogate name for a component set, e.g. ``_sk__a__b``.

    Component order is the intent's CANONICAL pair order (all pairs sorted by
    a direction-neutral name key — neither the anchor nor the from/to
    orientation holds positional privilege: the LLM's anchor choice AND its
    emission direction are not run-stable, and the name must be), so the
    ``(table_id, column_name)``-upserted ``column_id`` is stable across runs
    for the same confirmed key.
    """
    return SURROGATE_PREFIX + "__".join(component_names)


def composite_intent_digest(id_pairs: Iterable[Sequence[str]]) -> str:
    """The direction-neutral identity of a composite key (DAT-697).

    sha1 over the UNORDERED component column-id pairs, each rendered
    ``min:max`` and the pair set sorted — identity depends only on WHICH
    column pairs the key joins. Neither the judge's anchor choice nor its
    from/to orientation is run-stable (seen live on DAT-695), and the
    declined-composite record is digested from the HINT's orientation while
    a confirmation is digested from the LLM's — a direction-sensitive digest
    would split one composite into two identities and break the
    offered-vs-confirmed verdict arithmetic. Consumers matching stored rows
    recompute this from the row's natural column ids rather than comparing
    stored digest strings, so the digest format can evolve without stranding
    old rows.
    """
    keys = sorted(":".join(sorted((a, b))) for a, b in id_pairs)
    return hashlib.sha1("|".join(keys).encode(), usedforsecurity=False).hexdigest()


def is_surrogate_column(column_name: str) -> bool:
    """Whether a column is mint-owned (reconciled by the surrogate_mint phase)."""
    return column_name.startswith(SURROGATE_PREFIX)


def amend_typed_ddl(base_ddl: str, specs: list[SurrogateSpec]) -> str:
    """Wrap a typing-run DDL with surrogate projections (never edits the SELECT).

    ``CREATE OR REPLACE TABLE t AS SELECT {selects} FROM {raw}`` becomes
    ``CREATE OR REPLACE TABLE t AS SELECT *, {hashes} FROM (SELECT {selects}
    FROM {raw})`` — the hash expressions reference the TYPED column names from
    the subquery, so they hash post-cast values, and re-amending always starts
    from the base DDL (the amended form is never a mint input).

    The partition relies on the typing DDL never containing a second bare
    ``AS SELECT``: if a future ``standardization_expr`` (typing.yaml) ever
    embeds a subquery-shaped expression, revisit this boundary first.

    Args:
        base_ddl: the typing head's stored recipe DDL for the table.
        specs: the surrogate columns to project (empty returns the base DDL).

    Returns:
        The amended DDL, or ``base_ddl`` unchanged when ``specs`` is empty.

    Raises:
        ValueError: if the DDL doesn't carry the expected ``AS SELECT`` boundary.
    """
    if not specs:
        return base_ddl
    head, sep, body = base_ddl.partition(_DDL_BOUNDARY)
    if not sep:
        raise ValueError(f"typed DDL has no '{_DDL_BOUNDARY.strip()}' boundary: {base_ddl[:80]}…")
    extras = ", ".join(f'{spec.hash_expr} AS "{spec.column_name}"' for spec in specs)
    return f"{head} AS SELECT *, {extras} FROM (SELECT {body})"
