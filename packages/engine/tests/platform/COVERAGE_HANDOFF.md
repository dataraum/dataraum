# Test coverage handoff — MCP retirement

The MCP transport is being retired across the slice progression. Coverage migrates
**per stage**, not per route — there is no parallel "MCP tests next to REST tests"
era. Tests for the engine logic that the MCP tools wrap (still reachable via the
forthcoming REST routes) live in route-level tests landing alongside each slice.

Slice 1 (DAT-339) decomposes into staged engine tickets E0–E4: **E0** retires the
outgoing test scaffolding (this ticket, DAT-340), **E1** rebuilds the schema substrate
(DAT-341), E2 / E3 follow with session lifecycle + pipeline glue, and **E4** lands the
first REST route — `add_source` (DAT-344). Slice 2+ ports the remaining MCP tool
surfaces analogously.

## Slice 1 (DAT-339)

Ports the `add_source` surface to REST. Coverage for that surface lands in **E4
([DAT-344](https://real-dataraum.atlassian.net/browse/DAT-344))** — route tests
under `tests/unit/api/` exercise the REST handler; the underlying engine logic
keeps its existing unit/integration coverage in `tests/unit/analysis/`,
`tests/unit/pipeline/`, `tests/integration/storage/`.

## Slice 2+

The remaining MCP tool surfaces get REST equivalents:

- `begin_session`, `end_session`, `resume_session` — session lifecycle
- `measure` — pipeline trigger + progress
- `run_sql` — query passthrough
- `search_snippets` — snippet store query
- `query` — natural-language analytical query

Each tool's REST port carries its own route-level test ticket; until those land,
the underlying engine logic remains covered by the existing unit/integration
test pyramid.

## What was removed by DAT-340

- `tests/unit/mcp/` (16 files) — per-tool MCP unit tests
- `tests/integration/mcp/test_session_isolation.py` + `conftest.py` — per-session
  DuckLake invariants (the per-session-schema substrate is being retired in E1 /
  [DAT-341](https://real-dataraum.atlassian.net/browse/DAT-341))
- `tests/platform/smoke_dat321.py` — substrate-wide `session_id` FK assertions
  on the 26 per-session tables; coverage of a model E1 replaces
- `tests/platform/smoke_dat323.py` — per-session DuckDB / DuckLake substrate
  smoke; same retirement scope

`tests/platform/smoke_dat_324.py` is **kept** — it covers the
`SOURCES_DIR` / `CONFIG_DIR` / `DATARAUM_CONFIG_PATH` container-path wiring +
grep audit, none of which is per-session-schema.
