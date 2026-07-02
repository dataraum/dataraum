# ADR-0005 — Knowledge lives where its consumer reads it

- **Status:** Accepted
- **Date:** 2026-05-30
- **Ticket:** —
- **Design doc:** —

> **Internal process record** — how this repository is developed, not product
> architecture. Not part of the documented product decision set.

## Context

Project knowledge had drifted across overlapping homes: a host-side harness memory store
(`~/.claude/projects/…/memory/`, auto-loaded, gone stale and over its size limit), a
repo-relative memory store (`.claude/memory/`, current), `CLAUDE.md`, Confluence, and Jira.
The same facts existed in several places and contradicted each other (e.g. a memory note
advising `permissionMode: bypassPermissions` that the current `CLAUDE.md` forbids). The cost
shows up as agent babysitting: re-learning solved lessons, or acting on stale ones.

The guiding principle: **an agent is fast when the knowledge it needs is in context
automatically, or one cheap deterministic fetch away, and trustworthy.** Knowledge should
live where its consumer reads it, at the latency that consumer needs.

## Decision

Four homes, by knowledge *type* — not by convenience:

| Knowledge | Home | Properties |
|---|---|---|
| **Agent working memory** (evolving context, gotchas-in-flight, WIP state) | `.claude/memory/` — repo-relative, **gitignored**, rides the sandbox bind-mount | Local, private, allowed to go stale; **not** shared via git |
| **Shared team conventions / map / procedures** | `CLAUDE.md`, `.claude/skills/`, `.claude/agents/`, hooks | Git-tracked, reviewed, auto-applied |
| **Settled architecture decisions** | `docs/adr/` (this directory) | Git-tracked, short, immutable-once-accepted, link to Confluence/Jira |
| **Long-form design + live tickets** | Confluence (DD space) / Jira (DAT-*) | Authoritative for exploration & work state; linked *from* ADRs, not duplicated |

Mechanics:

- The **harness auto-load path must point at the repo-relative store** (symlink
  `~/.claude/projects/…/memory` → `<repo>/.claude/memory`), on the host and via
  `sandbox-bootstrap.sh`, so there is exactly one memory store everywhere.
- A recurring *procedural* lesson graduates from a memory note into a **skill or hook**
  (executed, not merely recalled). A recurring *decision* graduates into an **ADR**.
- **No separate knowledge repo** — it reintroduces a sync seam; co-locate in this monorepo.
- **No MCP server for facts already in the repo** (e.g. DB schema is in the Drizzle files;
  read them, don't proxy them).

## Consequences

- One memory store, auto-loaded identically on host and in the sandbox; the stale host mirror is retired.
- Conflicts resolve by authority: code > ADR > CLAUDE.md > memory note. A memory note that contradicts an ADR or CLAUDE.md is deleted, not preserved.
- Memory stays small and current (it is *working* memory, not an archive); completed history lives in `project_history.md`, decisions in `docs/adr/`, exploration in Confluence.
