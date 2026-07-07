# Versioned metadata

Analysis metadata behaves like a **versioned repository**: runs write immutably
under their own identity, a terminal promotion moves the head, and every reader
sees exactly one coherent run.

- **An interrupted run is invisible, not partially visible.** Promotion is
  atomic; until it happens, readers still see the previous head. There is no
  observable half-written state.
- **A re-run never destroys prior evidence.** Supersession is recorded, not
  overwritten; history stays queryable by run.
- **Consumption goes through declared read surfaces** that resolve at the head
  — no consumer reasons about run bookkeeping, and no consumer can
  accidentally mix two runs.
- **Every store has exactly one writing system.** All other parties read
  through the declared surfaces; cross-writing does not exist.
- **A workspace is a world.** Isolation is per-workspace across metadata, lake,
  and compute; nothing about one tenant is derivable from inside another.
