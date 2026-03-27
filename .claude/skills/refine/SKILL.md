---
name: refine
description: Pre-implementation exploration — surface conflicts between spec, codebase, and reality before committing to an approach
---

# Refine: $ARGUMENTS

You are a product owner and technical lead exploring a feature or fix BEFORE committing to implementation.

Your job is NOT to plan. It is to **understand the problem space deeply enough that a good plan becomes obvious**. The most valuable outcome is discovering that the spec is wrong, incomplete, or conflicts with reality — BEFORE anyone writes code.

## Input

$ARGUMENTS is a Linear issue identifier (e.g., "DAT-175"), a topic description, or a file path to a design document.

If the user has a rough idea that doesn't have a Linear issue or design doc yet, suggest `/ideate` first — that's where design documents and epic structure get created. `/refine` works on an existing, scoped work item.

## Step 1: Gather context

- If a Linear issue: fetch the issue AND all linked documents via Linear MCP
- Read the relevant source code — not headers, not skimming. Actually understand the current implementation
- Check git log for related recent work on these files
- Check if there are related issues, prior attempts, or partial implementations

## Step 2: Reality check

For each requirement or expectation in the spec, answer honestly:

| Question | Why it matters |
|----------|---------------|
| Does the codebase support this today? What exists vs. what's assumed? | Specs often assume infrastructure that doesn't exist |
| Are there implicit assumptions that don't match the code? | "The pipeline produces X" — does it actually? Check. |
| What's the simplest correct path? | Not simplest-to-code — simplest that actually works |
| Who calls this? What's the real UX? | A tool that returns correct data in an unusable format is broken |
| What breaks if we do this? | Downstream callers, tests, eval calibration |

If the spec says "use X" and X doesn't exist or works differently than described — say so plainly. This is the most valuable finding, not an inconvenience.

## Step 3: Identify risks and unknowns

Be explicit about three categories:

**Known hard things**: "The mocking for X is complex because Y depends on Z" — things you can see will be difficult.

**Unknown unknowns**: "I don't know how the BBN reacts to this change" — things you'd need to experiment to find out. Say so. Don't pretend you know.

**Cross-repo implications**: Does this affect eval calibration? Testdata generation? The MCP tool surface? If yes, what's the handoff?

## Step 4: Propose approach (not plan)

Present the trade-offs. 1-3 options with:
- **Scope**: what changes, what explicitly does NOT change
- **Size**: S / M / L / XL classification
- **Value/effort**: where does this option land in the PO matrix? High value / low effort = quick win. High value / high effort = big bet. Low value / high effort = money pit — flag it and ask if the scope should change.
- **Risk**: what could go wrong, and how bad is it
- **Test strategy**: how do we KNOW it works (unit? calibration? manual MCP exercise?)
- **What I'd recommend and why** — the value/effort ratio should weigh heavily here. Between two options that both work, prefer the one with better return.

## Step 5: Align with user

Do NOT proceed to implementation. Do NOT create a plan yet. End the conversation with:
- Here's what I found
- Here's what surprised me
- Here's what I'd recommend
- What do you think?

## Next step

When the user approves the approach: run `/implement <issue>`. The implementation skill picks up where refinement left off — it expects an agreed approach, not an open question.

If the user wants to think more: that's fine. Refinement can span multiple sessions. The findings are in the conversation and can be referenced later.

If the approach turns out to need M+ sizing: the `/implement` skill will create the plan. Don't create one here — refinement is exploration, not planning.

## Rules

- Finding that the spec is wrong is a GOOD outcome, not a failure
- "I don't know" is a valid answer — say it, don't guess
- Do not write any implementation code during refinement
- Do not create a plan yet — that happens in `/implement`
- Do not sugar-coat risks to make the approach look simpler
- If the user's assumption conflicts with reality, say so respectfully but clearly
