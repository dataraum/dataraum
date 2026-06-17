"""Source-qualified slice-table names (DAT-356).

The single source of truth for the physical names of the slice tables the
slicing agent's ``sql_template`` generates, so name GENERATION and name MATCHING
can never drift.

Names key off the fact table's **source-qualified** ``duckdb_path`` (``csv__orders``,
the same key enriched_views uses) rather than the bare ``table_name`` — so two
same-named facts in different sources (a multi-source begin_session selection) do
not collide on a shared ``lake.typed`` name and silently overwrite each other.
"""

from __future__ import annotations

import re

_NON_ALNUM = re.compile(r"[^a-zA-Z0-9]")
_RUN_UNDERSCORE = re.compile(r"_+")


def _sanitize(value: str) -> str:
    """Lowercase identifier: non-alnum → ``_``, runs collapsed, edges stripped.

    Identical to the agent's ``_sanitize_for_table_name`` and the matchers' old
    ``_sanitize_name`` (they were byte-for-byte the same) — kept here as the one
    definition all of them now share.
    """
    safe = _NON_ALNUM.sub("_", str(value))
    return _RUN_UNDERSCORE.sub("_", safe).strip("_").lower()


def slice_table_prefix(source_key: str, column_name: str) -> str:
    """The ``slice_{source}_{column}_`` prefix shared by every value's slice table.

    Matching code that scans by prefix (and slices out the value suffix) uses this
    so it stays in lockstep with :func:`slice_table_name`.
    """
    return f"slice_{_sanitize(source_key)}_{_sanitize(column_name)}_"


def slice_table_name(source_key: str, column_name: str, value: str) -> str:
    """Full slice-table name ``slice_{source}_{column}_{value}``.

    An empty/all-symbol value sanitizes to ``unknown`` (matching the agent's
    generation) so reconstruction never diverges from what was created.
    """
    return f"{slice_table_prefix(source_key, column_name)}{_sanitize(value) or 'unknown'}"


__all__ = ["slice_table_prefix", "slice_table_name"]
