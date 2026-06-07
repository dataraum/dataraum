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
- Add a one-line pointer? No — the directory listing *is* the index. Keep titles descriptive.

## Index

- [0001 — Temporal orchestration: Python workflows + activities on one worker](./0001-temporal-orchestration-python.md)
- [0002 — Engine is a pure Temporal activity worker (no HTTP / MCP transport)](./0002-engine-no-http-transport.md)
- [0003 — Postgres schema ownership: engine `ws_<id>`, cockpit `cockpit_db`, never shared](./0003-postgres-schema-ownership.md)
- [0004 — Agent-tier boundary: agentic LLM in the cockpit, durable pipeline in the engine](./0004-agent-tier-boundary.md)
- [0005 — Knowledge lives where its consumer reads it (memory / ADRs / Confluence / Jira)](./0005-knowledge-management.md)
- [0006 — Team-lead operating model: parallel lanes, gates at the intent layer](./0006-team-lead-operating-model.md)
- [0007 — Frame frozen-artifact contract: concept overlay rows as the engine↔cockpit grounding input](./0007-frame-frozen-artifact-contract.md)
- [0008 — Promoted reads are enforced by the database (head-joined views + grants), not by reader convention](./0008-promoted-read-views.md)
- [0009 — Entropy measures disagreement between witnesses; no deterministic semantic overrides](./0009-entropy-as-disagreement.md)
