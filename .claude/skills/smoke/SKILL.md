---
name: smoke
description: Quick MCP smoke test — call the tools you just built and see how they feel as a practitioner
allowed-tools:
  - mcp__dataraum__look
  - mcp__dataraum__measure
  - mcp__dataraum__query
  - mcp__dataraum__run_sql
  - mcp__dataraum__begin_session
  - mcp__dataraum__add_source
  - Read
  - Bash
  - AskUserQuestion
---

# Smoke: $ARGUMENTS

You just implemented or changed MCP tools. Now USE them. Not to verify correctness (that's eval's job) — to feel what the UX is like.

**IMPORTANT:** If you changed MCP server code in this session, the user must restart the session first. The MCP server runs as a subprocess — it loaded the old code. Remind them if they haven't restarted.

## Input

$ARGUMENTS is one of:
- A tool name to focus on (e.g., "look", "measure")
- A brief scenario to play through (e.g., "check data quality then query revenue")
- Empty — exercise all available tools

## What this is

A quick, informal test drive. Like kicking the tires after a change. You're not checking ground truth or running calibration. You're checking:

- Does the tool respond at all?
- Does the output make sense to a human?
- Is the response format useful or confusing?
- Are there obvious gaps (missing fields, unhelpful messages, errors)?
- Would you, as a practitioner, know what to do next based on this output?

## How to do it

### 1. Orient

Start with `begin_session` if available — that's how a real session starts.

Then `look` at the data. Read the output as if you've never seen this dataset before. Does it tell you enough to start working?

### 2. Exercise the changed tools

Call each tool that was modified. Don't overthink the inputs — use them naturally, as a practitioner would.

For each call, note:
- **Response**: did it work? What came back?
- **Clarity**: would a practitioner understand this without reading source code?
- **Usefulness**: does this output help you decide what to do next?
- **Surprises**: anything unexpected, missing, or confusing?

### 3. Try a mini workflow

String 2-3 tool calls together as a practitioner would:
- look → measure → "I see high entropy on column X" → query about that column
- Or: look → "what's the revenue?" → query → "does this make sense given the data quality?" → measure

This tests the *flow*, not just individual tools.

### 4. Try to break it (gently)

- Call a tool with edge-case inputs (empty string, unknown column, weird query)
- Ask a question the data can't answer — does it fail gracefully?
- Skip a step (e.g., query without looking first) — is the experience still coherent?

### 5. Share impressions

Tell the user what you found. Not a formal report — just honest impressions:

- "The look output is clear, I immediately understood the data shape"
- "measure returns scores but I don't know what 0.73 means — needs context"
- "query works but doesn't mention that the column it aggregated has quality issues"
- "begin_session errors with: [actual error message]"

Be specific. Quote actual output. Name actual fields. This is feedback, not a verdict.

## Next step

After smoke testing:
1. Fix any obvious issues found during the smoke test (restart session again if you change server code)
2. Commit the implementation
3. Update `.claude/handoff.md` with what needs eval attention (and testdata hints if applicable)
4. Tell the user: "Ready for acceptance. Run `/accept handoff` in the eval repo after updating the vendor submodule."

If smoke testing reveals deeper problems (not just UX polish but fundamental issues): go back to `/implement` or even `/refine`. Don't patch over structural problems.

## Rules

- This is NOT acceptance testing — don't assert against ground truth
- This is NOT a unit test — don't test internal behavior
- This IS a UX check — would a human find this useful?
- If a tool errors: note the error, try to understand why, move on
- If the MCP server isn't responding: remind user to restart the session
- Spend 5-10 minutes, not 30. Quick impressions are the point.
- Be honest. "This feels clunky" is useful feedback. "Looks great!" is not.
