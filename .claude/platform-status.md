# DAT-294 Platform Lanes — Status

At-a-glance view of all parallel platform lanes. Maintained by `/take`.
Row is added when a lane opens its PR; removed when the PR merges.

| Task | Worktree | Branch | PR | Contract | Status |
|---|---|---|---|---|---|
| DAT-354 | .worktrees/DAT-354 | feat/DAT-354-chat-tool-result-chips | (pending — lead opens) | none (cockpit-only: chat tool-result chips + canvas-rehydration mechanism) | pushed, awaiting PR |
| DAT-352 | .worktrees/DAT-352 | feat/DAT-352-measure-progress-rehydration | (pending — lead opens) | mirrors engine ProgressSnapshot (DAT-406) into temporal/types.ts | pushed, awaiting PR — SALVAGED: impl agent looped on result-emit (context exhaustion); work was committed + gates-green; verified independently (biome/tsc/345 unit tests + 3/3 review) then pushed |
| DAT-414 | .worktrees/DAT-414 | feat/DAT-414-versioned-typing-ddl | #213 | none (engine-internal: typed/quarantine materialization DDL; "Cross-repo: None expected") | lane smoke green (4 real-DuckLake tests), 62 affected tests green, gates clean, awaiting review — reviewer subagents not spawnable from background lane (no Agent tool); self-reviewed |
| DAT-433 | .claude/worktrees/agent-af06c3be03c932571 | feat/dat-433-agent-name-leaks | #225 | none (cockpit-only; engine evidence shape read-only-mirrored in fixtures) | gates green (check ✓, tsc ✓, 590 unit tests ✓), lane smoke = agent-name-leaks property suite green, PR open — self-reviewed (no Agent tool in background lane); lead runs independent 3-reviewer pass |
| DAT-436 | .claude/worktrees/agent-add416cb8ba92c479 | fix/dat-436-onboarding-flow | #231 | none (cockpit-only; engine untouched) | gates green (check ✓, tsc ✓, 622 unit tests ✓), chip root cause empirically pinned, PR open — self-reviewed (no Agent tool in background lane); lead runs independent review. Stays clear of DAT-437's chat-rail text filtering + tool-chip-summary.ts |
| DAT-437 | .claude/worktrees/agent-ab1f432a11317f4a4 | feat/dat-437-presentation-polish | #230 | none (cockpit-only: shared agent-refs marker, logical chip count, shared evidence-detail renderer) | gates green (check ✓, tsc ✓, 614 unit tests ✓), lane smoke = 10 targeted suites/123 tests + leak grep sweep, PR open — chat-rail edit scoped to the text-part filter (DAT-436 owns the chip spinner region); lead runs independent review |

_(previously merged: DAT-320 #110, DAT-324 #111, DAT-321 #113, DAT-323 #114, DAT-325 #115, DAT-358 #128, DAT-340 #129, DAT-339 substrate #132, bun toolchain #133, DAT-400 #191, DAT-406 #192)_

For slice-1 feature ticket state see [`dat339-pivot-status.md`](./dat339-pivot-status.md) and [`dat339-slice1-features-plan.md`](./dat339-slice1-features-plan.md).
