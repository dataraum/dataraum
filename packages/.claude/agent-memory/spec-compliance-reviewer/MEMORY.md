# spec-compliance-reviewer — memory

- [Always triple-dot diff a branch against main](./feedback_triple_dot_diff_only.md) — two-dot picks up unrelated concurrent main landings as false scope-creep.
- [Verifying "ported verbatim from parked branch X"](./feedback_verbatim_port_check.md) — `git diff <parked> <feature> -- <path>`, empty = confirmed.
- DAT-277 (surrogate-key mint, `worktree-dat-277-surrogate-keys`) reviewed 2026-07-03: COMPLIANT, all 5 plan items implemented, discarded payload confirmed absent, five named consumers confirmed byte-identical to main. Minor gap: `_build_surrogate_intent`'s `run_id is None` fallback branch has no dedicated unit test (the other two fallbacks — unresolvable component, anchor-echo-only — do).
