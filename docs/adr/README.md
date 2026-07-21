# Architecture Decision Records

Short, **git-tracked** records of settled architecture decisions — the *what* and the
*why*, in the repo where agents and humans grep, not buried in chat or memory.

## Why this exists

Memory (`.claude/agent-memory/`) is the agent's local, gitignored working scratchpad — it
reflects what was true when written and is allowed to go stale. Confluence holds
long-form design exploration. Neither is a durable, shared, reviewed record of *what we
decided and why it still holds*. ADRs fill that gap:

- **In git** → shared with teammates + CI, reviewed in PRs, versioned with the code.
- **In the repo tree** → greppable in-workspace, no MCP round-trip.
- **Short** → the decision + context + consequences, not the 5-page exploration.

When a memory note and an ADR disagree, the ADR wins (it's the reviewed, authoritative
record). When code and an ADR disagree, the code wins and the ADR needs a superseding entry.

## How to use

- One decision per file: `NNNN-short-slug.md`. Numbers are sequential, never reused.
- Copy `0000-template.md`. Keep it to a screen.
- Decisions are immutable once `Accepted`. To change one, write a **new** ADR that
  supersedes it and flip the old one's status to `Superseded by ADR-NNNN`.
- Link the long-form design (Confluence DD space) and the driving ticket (Jira DAT-*).
  Ticket and design-doc references belong in the header, not as load-bearing prose — the
  Context section must state the requirement so the record reads without them.
- Structure: **Context** states the requirement and the forces; the **Decision** is what
  was chosen, with the rejected options and why; **Consequences** are what follows. A
  decision is driven by a requirement — it is not itself a requirement.
- No hand-maintained index — the directory listing *is* the index (a previous hand list
  here drifted out of date). Keep titles descriptive.
- One record (0023) is an **internal process record** — how the repository
  is developed — and are marked as such; the rest document product architecture.
