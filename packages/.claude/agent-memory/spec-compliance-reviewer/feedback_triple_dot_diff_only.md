---
name: feedback_triple_dot_diff_only
description: Always diff a feature branch against the merge-base (git diff main...branch), never two-dot — this repo has many concurrent agents landing on main.
metadata:
  type: feedback
---

Always use triple-dot (`git diff main...branch`, merge-base diff) when auditing
a feature/worktree branch in dataraum-context — never two-dot (`git diff main
branch`). Two-dot includes every commit main has gained since the branch's
base, which in this repo is often substantial (several concurrent agents land
on main daily). Two-dot output made it look like the branch had reverted
unrelated work (e.g. DAT-603 thinking-mode changes in `graphs/agent.py`) that
was actually just main moving ahead after the branch was cut — a false
scope-creep/deviation signal.

**Why:** discovered auditing DAT-277 (surrogate-key mint): `git diff main
worktree-dat-277-surrogate-keys -- packages/engine/src/dataraum/graphs/` showed
a large diff that looked like a reversion; `git diff main...worktree-...`
(same path) showed zero diff — the file was never touched by the branch.

**How to apply:** every stat/diff call in a spec-compliance review of this repo
should use `...`. When comparing a branch against an OLDER reference/parked
branch (e.g. to confirm a verbatim port), two-dot diff between the two
branches is fine for that specific file-content comparison, but never for
"what did this branch change relative to main" — that comparison must be
triple-dot, else base drift is misattributed to the branch.
