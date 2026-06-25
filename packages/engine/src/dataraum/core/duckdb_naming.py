"""DuckDB identifier composition for the workspace-typed substrate.

Centralizes the mapping from logical identity ``(layer, source_name, table_name)``
to DuckDB schema + table names. Three layers exist in slice 1:

* ``raw`` — VARCHAR-first staging loaded from external sources
* ``typed`` — type-resolved tables produced by the typing phase
* ``quarantine`` — rows that failed type casts during typing

Slice 2 may add ``enriched`` / ``slicing_view`` / ``slice`` as first-class
layers; today they're stored with the same flat-namespace convention.

The catalog alias (``lake`` in slice 1, plausibly ``lake_<workspace_id>`` once
per-workspace catalogs land) is NOT encoded here — keeping stored values
catalog-agnostic means the future ATTACH-alias rename is a connection-layer
change, not a bulk data rewrite. Callers that need a fully-qualified name
including the catalog compose it at query time.

Convention for ``Table.duckdb_path``:

    Stores the **unqualified table name** (e.g., ``"csv_source__orders"``).
    The schema is derived from ``Table.layer`` via :func:`schema_for_layer`.
    Reads pre-DAT-341 produced bare names like ``"raw_orders"`` resolved
    via the connection's ``USE`` state; post-DAT-341 the connection USEs
    a workspace-stable schema (``lake.typed`` by default) and cross-layer
    queries take an explicit FQN via :func:`qualified_table`.

Reserved schema namespaces:

    * ``raw``, ``typed``, ``quarantine`` — workspace-stable, this module
    * ``session_*`` — reserved for slice 2 session-overlay schemas
    * ``archive_*`` — reserved for slice 2 archived-session schemas
"""

from __future__ import annotations

import re

# Layers with their canonical schema names. Slice 1 has the three substrate
# layers below; views/slices stay in the typed schema flat-namespace for now.
_LAYER_SCHEMA: dict[str, str] = {
    "raw": "raw",
    "typed": "typed",
    "quarantine": "quarantine",
}

# Sentinel default for view-like layers that don't own a dedicated schema.
# They live in the typed schema so DuckLake doesn't see N more schemas
# accumulate per-source.
_DEFAULT_SCHEMA = "typed"

# DuckDB identifier rules: an unquoted identifier matches [A-Za-z_][A-Za-z0-9_]*.
# We always lowercase + replace runs of non-id chars with underscores, then
# collapse consecutive underscores. Quoting at the call site would also work
# but the convention is "produce a bare identifier" so SQL stays readable.
_NON_IDENT = re.compile(r"[^a-zA-Z0-9_]+")
_RUN_UNDERSCORE = re.compile(r"_{2,}")


def sanitize_identifier(value: str) -> str:
    """Produce a DuckDB-safe lowercase identifier.

    Non-identifier characters collapse to a single underscore; runs of
    underscores collapse to one. A leading digit (which DuckDB rejects
    unquoted) gets prefixed with ``x_``.

    Args:
        value: Arbitrary user-supplied name (source name, table name, etc.).

    Returns:
        A bare DuckDB identifier — safe to embed in SQL without quoting.

    Examples:
        >>> sanitize_identifier("SalesLT.Customer")
        'saleslt_customer'
        >>> sanitize_identifier("2024_orders")
        'x_2024_orders'
        >>> sanitize_identifier("  weird--name  ")
        'weird_name'
    """
    s = _NON_IDENT.sub("_", value.strip().lower())
    s = _RUN_UNDERSCORE.sub("_", s).strip("_")
    if not s:
        raise ValueError(f"identifier {value!r} sanitizes to empty string")
    if s[0].isdigit():
        s = f"x_{s}"
    return s


def schema_for_layer(layer: str) -> str:
    """Return the DuckDB schema name that holds tables of this layer.

    Slice 1 mapping: ``raw`` -> ``raw``, ``typed`` -> ``typed``,
    ``quarantine`` -> ``quarantine``. View-like layers (``enriched``,
    ``slicing_view``, ``slice``) share the ``typed`` schema — they're
    derived artifacts of typed tables.

    Args:
        layer: The ``Table.layer`` value.

    Returns:
        Schema name (unqualified, sanitized).
    """
    return _LAYER_SCHEMA.get(layer, _DEFAULT_SCHEMA)


def workspace_table_name(table_name: str) -> str:
    """Compose the workspace-unique physical table name — NARROW (DAT-639).

    Tables are per-WORKSPACE, not per-source: a workspace holds one ``orders``,
    full stop. The source is an atomic content-hashed wrapper (how the table
    arrived), not a namespace — so the physical name carries NO source prefix.
    This completes DAT-506 (source dropped as a DB identifier) into the physical
    naming, which had kept the ``{source}__`` prefix. A second source trying to
    land the same name is a conflict (resolved at import: replay on the same
    content hash, else fail-loud), never a silently-disambiguated parallel table.

    Args:
        table_name: Logical name of the table.

    Returns:
        ``"<table>"`` sanitized — safe to embed unquoted in SQL.
    """
    return sanitize_identifier(table_name)


def qualified_table(layer: str, table_name: str) -> str:
    """Return the schema-qualified ``"schema.table"`` form (no catalog).

    Compose with a catalog alias at query time for full FQN, e.g.
    ``f"lake.{qualified_table(layer, tbl)}"``.

    Args:
        layer: ``Table.layer`` value.
        table_name: Logical (narrow) name of the table.

    Returns:
        ``"<schema>.<table>"``, safe to embed unquoted in SQL.
    """
    return f"{schema_for_layer(layer)}.{workspace_table_name(table_name)}"


# Schemas reserved for slice 2 substrate (session overlays + archive). Documented
# here so future per-session schema work doesn't collide with the workspace-stable
# names above.
RESERVED_SCHEMA_PREFIXES = frozenset({"session_", "archive_"})


def is_reserved_schema(schema: str) -> bool:
    """Return True if ``schema`` is reserved for slice 2 session-overlay use.

    Production code that constructs schema names dynamically should refuse
    to produce a name with any of the reserved prefixes.
    """
    return any(schema.startswith(prefix) for prefix in RESERVED_SCHEMA_PREFIXES)
