"""Database source recipes: yaml-declared backend + named SELECTs.

A recipe is a secret-free yaml file that declares which backend to use
and what SELECT queries to materialize as raw DuckDB tables. Credentials
are resolved at extraction time via the existing CredentialChain — the
``DATARAUM_{NAME}_URL`` env var.
"""

from __future__ import annotations

from dataraum.sources.db_recipe.recipe import Recipe, RecipeTable, parse_recipe

__all__ = ["Recipe", "RecipeTable", "parse_recipe"]
