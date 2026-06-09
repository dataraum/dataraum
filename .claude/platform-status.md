# Platform Lanes — Status

At-a-glance view of all parallel platform lanes. Maintained by `/take`.
Row is added when a lane opens its PR; removed when the PR merges.

| Task | Worktree | Branch | PR | Contract | Status |
|---|---|---|---|---|---|
| DAT-471 | .claude/worktrees/agent-a62d92129f276f4f2 | feat/dat-471-frame-metrics | #263 | none (cockpit-only; consumes DAT-466 MetricSpecSchema/teach_metric + DAT-469 frame-a-family core, both merged on main) | gates green (tsc ✓, biome ✓, 894 vitest ✓, build ✓), inline 2-rubric review PASS. AC3 goal-direction FINDING: engine `processor.py` reads file-only `load_all()`, NOT overlay — framed-metric goal-direction needs a 1-line engine swap (out of cockpit scope; documented in PR). Conflicts-at-merge with DAT-470 #262 (frame cycles) on frame.ts/model-frame.tsx/prompts — additive, 2nd-to-merge rebases. PR open — awaiting lead merge. |

_(previously merged: DAT-320 #110, DAT-324 #111, DAT-321 #113, DAT-323 #114, DAT-325 #115, DAT-358 #128, DAT-340 #129, DAT-339 substrate #132, bun toolchain #133, DAT-400 #191, DAT-406 #192, DAT-414 #213, DAT-433 #225, DAT-437 #230, DAT-436 #231, Intent enforcement #232, DAT-449 #234, DAT-451 #236, DAT-454 CI gates #237, DAT-452 #238, DAT-434 #240; DAT-354/DAT-352 shipped via the DAT-339 lanes)_

For slice-1 feature ticket state see [`dat339-pivot-status.md`](./dat339-pivot-status.md) and [`dat339-slice1-features-plan.md`](./dat339-slice1-features-plan.md).
