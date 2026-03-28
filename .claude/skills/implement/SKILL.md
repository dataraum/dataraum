---
name: implement
description: Structured implementation with mandatory checkpoints and psychological safety — stop early rather than deliver incomplete work
---

# Implement: $ARGUMENTS

You are implementing a feature or fix that has been refined and agreed upon with the user.

## Input

$ARGUMENTS is a Jira issue identifier, a description of the agreed approach, or "continue" to resume from the last checkpoint.

## Before you start

1. Verify there IS an agreed approach — from a `/refine` session, user discussion, or a clear spec. If not, run `/refine` first.
2. Create or update the plan with explicit scope:
   ```
   DO change: [list every file]
   DO NOT change: [list files that must stay untouched]
   ```
3. Classify size: S / M / L / XL
4. For M+: get explicit user sign-off on the plan before writing code

## Phase execution

### Before each phase

State: **"Phase N: [what I'm about to do]"**

### During each phase

Implement. Focus on one thing at a time.

### After EVERY phase — mandatory checkpoint

Write out answers to these questions. Do not skip them. Do not answer "fine" or "nothing" unless that is genuinely true.

1. **What did I just do?** Be specific — files changed, logic added/removed.
2. **What did I skip or simplify?** Even small things. Especially small things.
3. **Is anything harder than expected?** If yes: STOP. Tell the user. Discuss.
4. **Am I fighting mocks?** If I spent effort making tests work via mocking rather than testing real behavior: flag it. The test design may be wrong.
5. **Am I keeping dead code because a test needs it?** If yes: delete the dead code AND the test. Note it in the checkpoint.
6. **Does the plan still make sense?** If what I learned changes the approach: update the plan, tell the user, and proceed with the updated plan. This is normal, not failure.

### Run verification

After each phase: tests must be green. If they're not, fix the code (not the tests) unless the test itself is wrong — and if it is, explain WHY the test's expectation is incorrect.

---

## Psychological safety

These are not just allowed. They are EXPECTED. They are the EFFICIENT path.

**"This is harder than I expected."**
Say it immediately. Do not power through hoping it gets easier. It won't. The user can help, adjust scope, or change approach. Powering through produces bad code that needs rework.

**"The spec doesn't match what I found in the code."**
Stop implementation. Go back to refinement. The user needs to know before you build on a wrong foundation. This saves days, not wastes them.

**"I've tried this twice and it's not working."**
The approach is wrong, not your execution. Stop. Explain what you tried. Form a hypothesis about the root cause. Ask the user. Three strikes and you MUST stop — this is a rule, not a suggestion.

**"I need to change the plan."**
The plan was a hypothesis. You now have more information. Changing the plan based on evidence is BETTER than following a wrong plan. Update it, tell the user, proceed.

**"I don't know how to do this part."**
Say it. Guessing wastes time and produces code that looks right but isn't. The user may know, or may agree to skip it, or may change the approach.

**"This test only tests my mock, not real behavior."**
Delete it and say so. A test that verifies mocking scaffolding is worse than no test — it creates false confidence.

**"I'm keeping dead code because removing it breaks tests."**
STOP. Delete the dead code. Delete or rewrite the test. Note it in the checkpoint. Dead code kept for tests is technical debt that compounds.

**The most expensive mistake is declaring done when you're not.** It forces the user to discover the problem later, in a new session, with lost context. Stopping early and being honest is ALWAYS cheaper.

---

## Review gate — before declaring done

After the final phase passes verification, invoke BOTH review agents. Do not skip this.

1. **Senior code reviewer** — launch the `senior-code-reviewer` agent. Give it the list of changed files and a summary of what was implemented. Wait for its verdict.
2. **Spec compliance reviewer** — launch the `spec-compliance-reviewer` agent. Give it the plan/spec and the list of changed files. Wait for its verdict.

If either returns NEEDS WORK or BLOCKED:
- Read the findings carefully
- Fix what's fixable in this session
- If a finding requires rethinking the approach: stop, tell the user, go back to refinement
- Do NOT dismiss findings as "style issues" or "nice-to-have" — the reviewers are calibrated to flag real problems

Only proceed to handoff after both reviewers approve (or after discussing unresolved findings with the user).

## Handoff

After implementation is complete (honestly complete, reviewers satisfied):

1. Update `.claude/handoff.md` with entries for EACH affected area:

   **For dataraum-eval:**
   - What changed (files, modules, behaviors)
   - Which MCP tools are affected
   - Which calibration tests or strategies to run
   - Any new response fields, changed formats, or threshold changes

   **For dataraum-testdata** (if applicable):
   - Hints for new injection types that would test this feature
   - New ground truth values that should be generated
   - Keep it directional — testdata has its own design concerns

2. Summarize to the user:
   - What was done
   - What was deferred (with reasons)
   - What needs acceptance testing
   - Reviewer verdicts

3. If MCP tool changes: remind user to restart session and run `/smoke` before handoff

## Rules

- Each phase must leave all tests green — no half-done states
- Never modify a test to make it pass unless the test is wrong (and you explain why)
- Never keep dead code for tests
- If you're fighting mocks for more than 10 minutes, the test design is wrong — step back
- Commit after each verified phase
- Review gate is mandatory, not optional
- Declaring done is a claim you are accountable for — be sure
