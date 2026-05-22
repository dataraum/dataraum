# DAT-294 Platform Lanes — Status

At-a-glance view of all parallel platform lanes. Maintained by `/take`.
Row is added when a lane opens its PR; removed when the PR merges.

| Task | Worktree | Branch | PR | Contract | Status |
|---|---|---|---|---|---|
| DAT-340 | .worktrees/DAT-340 | feat/dat-340-retire-mcp-tests | [#129](https://github.com/dataraum/dataraum/pull/129) | none (test deletions + infra hook) | lane smoke green, awaiting review |
| DAT-339 pivot (Phase 0a + 0b + 0c) | (direct branch) | feat/dat-339-pivot-p0-substrate → feat/dat-339-pivot | none yet | schema-per-workspace (locked via /refine, see [dat339-pivot-status.md](./dat339-pivot-status.md)) | Phase 0a DONE. Phase 0b DONE (3b7fd1c8 + ceee4129). Phase 0c DONE — Starlette + 3 stubs + eager substrate-init (resolves 0b follow-up) + delete api/ + delete packages/api + fastapi→starlette dep swap + doc sweep. Pending commit + reviewer gate. Next: Phase 0d (cockpit cleanup). |

_(previously merged: DAT-320 #110, DAT-324 #111, DAT-321 #113, DAT-323 #114, DAT-325 #115, DAT-358 #128)_
