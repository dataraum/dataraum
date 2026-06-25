"""The recipe-table shape a database source materializes.

A db_recipe source carries its recipe as `connection_config['tables']` — a list
of named SELECT queries synthesized by the cockpit `select` tool (the only
producer; the legacy YAML recipe parser is gone, DAT-430). Credentials never
appear in a recipe — they are resolved at extraction time via the existing
`CredentialChain` (`DATARAUM_{NAME}_URL`).
"""

from __future__ import annotations

from pydantic import BaseModel


class RecipeTable(BaseModel):
    """One named SELECT inside a recipe.

    `name` becomes the DuckDB table name in `lake.raw` (narrow, no prefix — DAT-639).
    `sql` is materialized verbatim against the attached database.
    """

    name: str
    sql: str
