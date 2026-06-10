"""Query module — the SQL-snippet KNOWLEDGE BASE substrate.

The natural-language query CONSUMER (``QueryAgent`` / ``answer_question``)
migrated to the cockpit TS tier — the ``answer`` sub-agent (DAT-485/494) — and
was removed in DAT-487. What remains here is the engine-owned snippet substrate
the LIVE producer path depends on:

- ``snippet_library``: ``SnippetLibrary`` (save / find_by_key / record_usage) —
  written by the GraphAgent (``graphs/agent.py``) and ``metrics_phase``.
- ``snippet_models``: the ``SQLSnippetRecord`` / ``SnippetUsageRecord`` ORM models.
- ``snippet_utils``: ``normalize_sql`` / ``determine_usage_type`` /
  ``normalize_expression``.
- ``execution``: ``execute_sql_steps`` — the engine's SQL-step executor (GraphAgent).

Submodules are imported directly (e.g. ``from dataraum.query import
snippet_models``); this package exposes no top-level re-exports.
"""
