# Architecture Decision Records

Short, **git-tracked** records of settled architecture decisions — the *what* and the
*why*, in the repo where agents and humans grep, not buried in chat or memory.

## Why this exists

Memory (`.claude/memory/`) is the agent's local, gitignored working scratchpad — it
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
- No hand-maintained index — the directory listing *is* the index (a previous hand list
  here drifted out of date). Keep titles descriptive.
