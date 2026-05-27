# reference/ — dead code kept for reference only

Code here is **retired**, not part of the engine package. It is outside
`src/dataraum/`, so it is not imported, built into the wheel, type-checked,
or tested. It exists only as a reading reference during the cockpit takeover
and is slated for deletion.

- `mcp/` — the legacy MCP tool surface (DAT-339: "stays as dead code through
  slice 1; whole-folder delete in slice 2"). Moved out of `src/dataraum/` in
  DAT-369 (E4c) so retiring the hand-rolled scheduler/monitoring tables no
  longer has to thread through it. Delete in slice 2 alongside the
  session-lifecycle reimplementation.
