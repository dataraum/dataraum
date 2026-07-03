---
name: feedback_verbatim_port_check
description: To verify a plan's "port X verbatim from parked branch Y" claim, diff the file directly between the two branches — byte-identical confirms it in one command.
metadata:
  type: feedback
---

When a plan says "port module/test-file Z verbatim from `<parked-branch>`",
verify with `git diff <parked-branch> <feature-branch> -- <path>` on that exact
file. An empty diff is a clean, fast confirmation — no need to read both files
side by side. Used successfully on DAT-277 to confirm `composite.py` and
`test_composite.py` were ported byte-for-byte from
`refactor/dat-277-composite-key-rescue`.

**Why:** direct, cheap, unambiguous — cheaper than reading both files and
diffing mentally, and catches even a single-character drift.

**How to apply:** for any "ported from parked/reference branch" claim in a
plan, run this diff before trusting the port. If it's non-empty, read the
diff — a small adaptation (e.g. renamed import) is normal when the target
branch's surrounding code moved on; a substantive change (dropped guard,
different default) is a deviation to flag.

Related: [[feedback_triple_dot_diff_only]] for the separate question of what
the feature branch changed relative to ITS OWN base (main).
