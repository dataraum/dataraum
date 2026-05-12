"""Recipe model + parser.

A recipe declares one database backend and a set of named SELECT
queries. Credentials never appear here — they are resolved at
extraction time via the existing `CredentialChain`.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from pathlib import Path

import yaml
from pydantic import BaseModel

from dataraum.core.models import Result

# Backends supported via DuckDB extensions. Mirrored in sources/backends.py
# but duplicated here so parse-time validation fails loud before any
# database interaction.
SUPPORTED_BACKENDS: frozenset[str] = frozenset({"mssql", "postgres", "mysql", "sqlite"})

# Top-level recipe keys that would suggest credentials in the yaml.
# Recipes are secret-free; we reject these loud so practitioners never
# accidentally commit secrets to git.
_FORBIDDEN_TOP_LEVEL_KEYS: frozenset[str] = frozenset(
    {"connection", "credentials", "auth", "password", "secret", "secrets"}
)


class RecipeTable(BaseModel):
    """One named SELECT inside a recipe.

    `name` becomes the DuckDB table name (`raw_{name}` in staging).
    `sql` is materialized verbatim against the attached database.
    """

    name: str
    sql: str


class Recipe(BaseModel):
    """A parsed and validated database source recipe.

    `recipe_hash` is the sha256 of the raw yaml bytes — used in Phase B
    as the durable identity for teachings across daily snapshots.
    """

    backend: str
    tables: list[RecipeTable]
    recipe_hash: str
    source_path: Path


def parse_recipe(path: str | Path) -> Result[Recipe]:
    """Parse and validate a recipe yaml file.

    Fails loud on every rule violation, naming the offending key.

    Args:
        path: Path to the recipe yaml file.

    Returns:
        Result[Recipe] — success if the recipe is well-formed and
        secret-free; otherwise a failure with a specific error message.
    """
    recipe_path = Path(path)
    if not recipe_path.exists():
        return Result.fail(f"Recipe file not found: {path}")
    if recipe_path.suffix.lower() not in (".yaml", ".yml"):
        return Result.fail(
            f"Recipe must have a .yaml or .yml extension; got '{recipe_path.suffix}'."
        )

    raw_bytes = recipe_path.read_bytes()
    try:
        data = yaml.safe_load(raw_bytes)
    except yaml.YAMLError as exc:
        return Result.fail(f"Recipe yaml is invalid: {exc}")
    if not isinstance(data, Mapping):
        return Result.fail(
            f"Recipe yaml must be a mapping at top level; got {type(data).__name__}."
        )

    forbidden = sorted(_FORBIDDEN_TOP_LEVEL_KEYS.intersection(data.keys()))
    if forbidden:
        return Result.fail(
            "Recipe must be secret-free; credential-like keys are forbidden at the top level. "
            f"Found: {', '.join(forbidden)}. "
            "Put credentials in .env as DATARAUM_{NAME}_URL where {NAME} is the source name."
        )

    backend_raw = data.get("backend")
    if not isinstance(backend_raw, str) or not backend_raw.strip():
        return Result.fail(
            "Recipe must declare `backend:` (one of: "
            f"{', '.join(sorted(SUPPORTED_BACKENDS))})."
        )
    backend = backend_raw.strip().lower()
    if backend not in SUPPORTED_BACKENDS:
        return Result.fail(
            f"Unsupported backend '{backend}'. "
            f"Supported: {', '.join(sorted(SUPPORTED_BACKENDS))}."
        )

    tables_raw = data.get("tables")
    if not isinstance(tables_raw, Mapping) or not tables_raw:
        return Result.fail("Recipe must declare at least one entry under `tables:`.")

    tables: list[RecipeTable] = []
    seen: set[str] = set()
    for name, body in tables_raw.items():
        if not isinstance(name, str) or not name.strip():
            return Result.fail(f"Table name must be a non-empty string; got {name!r}.")
        normalized = name.strip()
        lowered = normalized.lower()
        if lowered in seen:
            return Result.fail(
                f"Duplicate table name (case-insensitive): {normalized!r}."
            )
        seen.add(lowered)
        if not isinstance(body, Mapping):
            return Result.fail(
                f"Table '{normalized}' must be a mapping with a `sql:` key."
            )
        sql_raw = body.get("sql")
        if not isinstance(sql_raw, str) or not sql_raw.strip():
            return Result.fail(
                f"Table '{normalized}' has an empty or missing `sql:` field."
            )
        tables.append(RecipeTable(name=normalized, sql=sql_raw.strip()))

    recipe_hash = hashlib.sha256(raw_bytes).hexdigest()
    return Result.ok(
        Recipe(
            backend=backend,
            tables=tables,
            recipe_hash=recipe_hash,
            source_path=recipe_path,
        )
    )
