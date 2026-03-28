---
name: decompose
description: Turn a design document into an executable Jira epic — phase issues, acceptance criteria, dependency relations, following project conventions
allowed-tools:
  - Read
  - Glob
  - Grep
  - Bash
  - Agent
  - AskUserQuestion
  - mcp__jira__getJiraIssue
  - mcp__jira__searchJiraIssuesUsingJql
  - mcp__jira__getVisibleJiraProjects
  - mcp__jira__getJiraProjectIssueTypesMetadata
  - mcp__jira__getTransitionsForJiraIssue
  - mcp__jira__createJiraIssue
  - mcp__jira__editJiraIssue
  - mcp__jira__createIssueLink
  - mcp__jira__getIssueLinkTypes
  - mcp__jira__getConfluencePage
  - mcp__jira__createConfluencePage
  - mcp__jira__updateConfluencePage
  - mcp__jira__searchConfluenceUsingCql
---

# Decompose: $ARGUMENTS

You are structuring a design into executable work items. The thinking is done — now make it actionable.

## Input

$ARGUMENTS is one of:
- A Confluence page URL or title (design doc from `/ideate`)
- A Jira issue identifier (existing epic to refine)
- A local file path (design doc in `plans/`)

## Step 1: Read the design

Read the full design document. Extract:
- **Core deliverables**: what must exist when this is done
- **Key decisions**: architectural choices that constrain implementation
- **Open questions**: unresolved items that may become spikes
- **Dependencies**: what must exist before this work can start
- **Scope boundaries**: what's explicitly out of scope

Also read the relevant source code to understand the current state. The design may reference modules that have changed since it was written.

## Step 2: Identify phases

Break the work into phases following the project's established pattern (modeled after DAT-173):

Each phase must:
- **Leave all tests green** — no half-done states between phases
- **Be independently valuable** — if we stop after phase N, we have something useful
- **Have clear boundaries** — what's in this phase, what's explicitly deferred to later

Typical phasing patterns in this project:
- **By tool/feature**: Phase 1 = look + measure, Phase 2 = teach, Phase 3 = why, etc.
- **By layer**: Phase 1 = data model, Phase 2 = business logic, Phase 3 = MCP surface
- **By risk**: hardest/most uncertain first, then build on the foundation

For each phase, identify:
- Specific deliverables (tools, modules, behaviors)
- Size estimate (S/M/L)
- Dependencies on other phases
- Whether it touches eval or testdata (cross-repo implications)
- **Value/effort quadrant** — classify each phase:

| | Low Effort | High Effort |
|---|---|---|
| **High Value** | **Quick win** — schedule early, build momentum | **Big bet** — worth it, but phase carefully |
| **Low Value** | **Fill-in** — slot in when convenient | **Money pit** — defer or cut |

This drives phase ordering: quick wins first (early value, team confidence), then big bets (the core work), fill-ins whenever there's a gap. Money pits get flagged — ask the user whether to keep or cut them. A phase that's high-effort/low-value often means the scope is wrong, not that it should be built anyway.

## Step 3: Write acceptance criteria

For each phase, write concrete, verifiable acceptance criteria. Follow the project's pattern:

Good:
```
- [ ] `look` tool returns column count, row count, types, roles, distributions, and sample rows
- [ ] `measure` tool returns per-column entropy scores, BBN readiness, and contract status
- [ ] Old `get_quality` and `analyze` tools return deprecation errors
- [ ] Unit tests for each tool's response format
- [ ] Handoff.md updated with calibration items
```

Bad:
```
- [ ] Look tool works
- [ ] Tests pass
- [ ] Code is clean
```

Acceptance criteria must be checkable by the spec-compliance-reviewer. If a criterion is ambiguous, it's useless.

## Step 4: Map dependencies

Create the dependency graph between phases. Use the project's ASCII format:

```
Phase 1 (DAT-nnn)
  └──→ Phase 2 (DAT-nnn)  [blocked by Phase 1]
         ├──→ Phase 3 (DAT-nnn)  [blocked by Phase 2]
         └──→ Phase 4 (DAT-nnn)  [blocked by Phase 2]
                    └──→ Phase 5 (DAT-nnn)  [blocked by Phase 3 + 4]
```

Also identify cross-repo work that should be tracked:
- dataraum-eval items (calibration, tool surface validation)
- dataraum-testdata items (new injections, ground truth)

## Step 5: Create Jira artifacts

Ask the user for confirmation before creating anything. Show them:
- The proposed epic title and description
- The phase breakdown with acceptance criteria
- The dependency graph
- Cross-repo issues (if any)

Then create, in order:

### 5a: Epic issue (if new)
```
Title: Epic: {descriptive name}
Label: Epic
Project: {appropriate project, usually "Phase 1: Open Source Core"}
Description:
  ## Vision
  {1-2 sentences from the design doc}

  ## Specs & Design
  - [{document title}]({confluence url})
  - Design artifacts: `plans/{topic}/` on {branch}

  ## Execution
  {dependency graph with issue references — fill in after creating phase issues}
```

### 5b: Implementation plan document (if the design doc isn't already a plan)
Create a Confluence page with:
- Phase breakdown table
- Dependency graph
- Key design decisions
- Deferred items

Attach to the epic issue.

### 5c: Phase issues (children of the epic)
For each phase:
```
Title: Phase N: {tool/feature names} + {brief description}
Label: Feature
Parent: {epic issue}
Project: {same as epic}
Status: Backlog (or Todo for Phase 1 if ready)
Description:
  **Plan:** [{plan document title}]({url})
  **Design:** [{design document title}]({url})
  **Branch:** {current working branch}
  **Blocked by:** Phase N-1 (DAT-nnn) [if applicable]

  ## Deliverables
  {detailed specifications per tool/module}

  ## Acceptance Criteria
  - [ ] {concrete, verifiable items}

  ## Cross-repo
  - eval: {what needs calibration}
  - testdata: {hints for new test scenarios}
```

### 5d: Set up relations
- Phase N+1 `blockedBy` Phase N (where applicable)
- Cross-repo issues as children of the epic (prefixed with repo name)

### 5e: Update the epic description
Go back and fill in the issue references in the dependency graph.

## Step 6: Summary

Print the created structure:
```
Epic: DAT-nnn — {title}
  Phase 1: DAT-nnn — {title} [Todo]
  Phase 2: DAT-nnn — {title} [Backlog, blocked by Phase 1]
  Phase 3: DAT-nnn — {title} [Backlog, blocked by Phase 2]
  ...
  Cross-repo: DAT-nnn — eval: {title}

Documents:
  - {plan document title} — attached to epic

Ready: /refine DAT-nnn (Phase 1 issue) to start
```

## Next step

Point the user to the first actionable phase issue: "Run `/refine DAT-nnn` to start Phase 1."

If open questions from the design doc remain unresolved, suggest creating spike issues first.

## Rules

- Do NOT create issues without user confirmation. Show the full plan first, then ask.
- Follow the project's established patterns exactly — DAT-173 is the template.
- Every phase must be independently testable with all tests green.
- Acceptance criteria must be concrete and verifiable — not "works" or "tests pass."
- Cross-repo implications are NOT optional to mention. If a phase changes detectors or MCP tools, the eval/testdata sections must exist.
- Don't gold-plate the epic. Start with the minimum viable structure. Issues can be refined later via `/refine`.
- Open questions from the design become spike issues, not ignored items.
- If the design doc is stale (references code that's changed), note the discrepancies rather than silently creating issues based on outdated information.
