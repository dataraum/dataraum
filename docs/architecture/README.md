# Architecture — ideas and requirements

These documents state the **overarching ideas** of the system and the
**requirements** they serve — what a reader cannot get from any single file of
code.

The test for every sentence: **can the code already answer this?** Then it does
not belong here. Module wiring, chosen libraries, transports, table names —
the code states those precisely and is always current. What the code cannot
state is why the system has this shape and what must hold for it to fulfill
its purpose. That is all these documents carry.

Rules:

- Present tense, current truth, updated in place by the same PR that changes
  it. Git history is the only archive — no status fields, no supersede chains,
  no tickets, no dates.
- Ideas and requirements only. No decision log, no implementation inventory,
  no editor instructions ("do not add X" is agent guidance and belongs in
  CLAUDE.md or agent memory).
- A requirement is stated so a stranger can check the system against it.
  "Not yet defined: X" is a valid statement.
- One file per concern; the directory listing is the index.
