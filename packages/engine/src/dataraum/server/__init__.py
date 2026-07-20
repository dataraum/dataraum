"""Substrate bootstrap helpers (DuckLake anchor + workspace overlay).

Post-DAT-344 the engine has no HTTP server — it runs as a Temporal activity
worker (see :mod:`dataraum.worker`). What remains here is substrate-open code
the worker bootstrap reuses: :mod:`dataraum.server.storage` (the process-wide
DuckLake anchor, plus fresh DuckDB connections onto the same named in-memory
database) and :mod:`dataraum.server.workspace` (workspace config overlay +
``ws_<id>`` schema selector).
"""
