"""Database source recipes: a backend + named SELECTs to materialize.

A recipe declares which backend to use and what SELECT queries to materialize
as raw DuckDB tables. It lives in the source row's ``connection_config['tables']``,
synthesized by the cockpit ``select`` tool (DAT-430 — the YAML recipe file format
and its parser are gone). Credentials are resolved at extraction time via the
existing CredentialChain — the ``DATARAUM_{NAME}_URL`` env var.
"""

from __future__ import annotations

from dataraum.sources.db_recipe.recipe import RecipeTable

__all__ = ["RecipeTable"]
