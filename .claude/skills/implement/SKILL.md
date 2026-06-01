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
2. Classify size: S / M / L / XL.
   - **S (obvious, ~1–3 files):** skip the plan ceremony. Make the change, leave it green, done. A formal DO-change/DO-NOT-change list for a one-liner is theater.
   - **M:** lightweight plan, single session.
   - **L/XL:** full plan with explicit scope (below) + user sign-off before code.
3. For M+, create or update the plan with explicit scope:
   ```
   DO change: [the files this work touches]
   DO NOT change: [large/unrelated areas that must stay untouched — NOT a cage]
   ```
4. For M+: get explicit user sign-off on the plan before writing code.

### Scope is a fence against *unrelated* sprawl — not a cage

`DO NOT change` exists to stop you from wandering into unrelated subsystems mid-task. It does **not** forbid:
- **Design-implied cleanup** — deleting dead code, removing a retired field, adapting its tests. That's in-scope, not an "adjacent-edit violation" (see CLAUDE.md sizing note + "Default to the clean cut").
- **An obvious correctness fix you spot in your blast radius** — a clear one-line bug in a file you're already editing. Fix it and note it in the checkpoint. Leaving a bug you can see "because it's not in the ticket" is the corporate failure mode, not discipline. A ticket is a pointer to work, not a permission boundary.

If a fix is large or genuinely unrelated, *that's* when you note it for a separate item rather than expanding scope.

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

## Psychological safety — and its counterweight

These are not just allowed. They are EXPECTED. They are the EFFICIENT path.

**But stopping is not free, and "ask the user" is not the safe default — it's a real cost too.** Every escalation spends the user's attention and your momentum. The bar for stopping is a *genuine* fork or a *real* blocker, not a routine decision you could resolve with a grep, a sensible default, or by following the agreed design. The senior-engineer move is to **solve the problem and report the outcome**, not to narrate every micro-step and wait for a nod. Default to action; reserve the stop conditions below for when they truly fire.

**Don't spin in dependency circles.** If you find yourself "blocked by" a prerequisite, the answer is usually to *do the prerequisite*, build a thin vertical slice, or attack the real problem directly — not to report "blocked" and stall. Time-box deliberation: if you've spent longer deciding how to approach something than it would take to just try the obvious path and learn, try it. An over-thought, entry-level answer arrived at slowly is worse than a direct attempt that produces real information.

The stop conditions below are about *honesty when something is genuinely wrong* — not a license to bail on tractable work:

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

**The most expensive mistake is declaring done when you're not.** It forces the user to discover the problem later, in a new session, with lost context. Stopping early and being honest is cheaper than a false "done."

**The second most expensive mistake is escalating what you could have solved.** Asking the user to make a call you had the context to make yourself, or stopping on a routine fork, burns the same trust from the other direction. Honesty about being stuck and ownership of what you can resolve are the *same* virtue — calibrate which one the moment calls for.

---

## Review gate — before declaring done

After the final phase passes verification, invoke BOTH review agents. Do not skip this.

1. **Senior code reviewer** — launch the `senior-code-reviewer` agent. Give it the list of changed files and a summary of what was implemented. Wait for its verdict.
2. **Spec compliance reviewer** — launch the `spec-compliance-reviewer` agent. Give it the plan/spec and the list of changed files. Wait for its verdict.

**Reviewer cd policy (no need to repeat in each prompt — it lives in the agent definitions):** both reviewers have a "no cd" rule baked in (`.claude/agents/{senior-code-reviewer,spec-compliance-reviewer}.md`). They use absolute paths for `Read`, run `git` from anywhere in the repo, and use `uv --directory <abs>` to scope `uv` to a subpackage. Don't redundantly re-instruct them in every prompt — just give them the review task. If you observe a reviewer running `cd` despite the rule, that's a bug in the agent definition — fix it there, not by patching the prompt.

If either returns NEEDS WORK or BLOCKED:
- Read the findings carefully
- Fix what's fixable in this session
- If a finding requires rethinking the approach: stop, tell the user, go back to refinement
- Do NOT dismiss findings as "style issues" or "nice-to-have" — the reviewers are calibrated to flag real problems

Only proceed to handoff after both reviewers approve (or after discussing unresolved findings with the user).

## Handoff

After implementation is complete (honestly complete, reviewers satisfied):

1. **If this work touched engine detectors, pipeline phases, or response shapes that eval calibrates against**, update `.claude/handoff.md` with entries for EACH affected area:

   **For dataraum-eval:**
   - What changed (files, modules, behaviors)
   - Which engine routes or pipeline phases are affected
   - Which calibration tests or strategies to run
   - Any new response fields, changed formats, or threshold changes

   **For dataraum-testdata** (if applicable):
   - Hints for new injection types that would test this feature
   - New ground truth values that should be generated
   - Keep it directional — testdata has its own design concerns

   **Skip the handoff entirely** for anything outside `packages/engine/src/dataraum/{analysis,entropy,pipeline,graphs}/` — that includes `packages/cockpit/` (UI/widgets), `packages/infra/` (docker-compose), and engine-side platform shell (Starlette kernel in `src/dataraum/server/`, server bootstrap, transport plumbing). Eval doesn't consume those surfaces. For parallel platform work, follow the "Parallel platform work" runbook in CLAUDE.md instead.

2. Summarize to the user:
   - What was done
   - What was deferred (with reasons)
   - What needs acceptance testing
   - Reviewer verdicts

3. If the engine REST surface or the cockpit changed: run `/smoke` to drive the cockpit in a browser before handoff. If you only touched engine Python, also rebuild the container (`docker compose -f packages/infra/docker-compose.yml up -d --build control-plane`).

## Rules

- Each phase must leave all tests green — no half-done states
- Never modify a test to make it pass unless the test is wrong (and you explain why)
- Never keep dead code for tests
- If you're fighting mocks for more than 10 minutes, the test design is wrong — step back
- Commit after each verified phase
- Review gate is mandatory, not optional
- Declaring done is a claim you are accountable for — be sure
