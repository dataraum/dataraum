# DAT-294 Platform Lanes — Status

At-a-glance view of all parallel platform lanes. Maintained by `/take`.
Row is added when a lane opens its PR; removed when the PR merges.

| Task | Worktree | Branch | PR | Contract | Status |
|---|---|---|---|---|---|
| DAT-340 | .worktrees/DAT-340 | feat/dat-340-retire-mcp-tests | [#129](https://github.com/dataraum/dataraum/pull/129) | none (test deletions + infra hook) | lane smoke green, awaiting review |
| DAT-339 pivot (Phase 0a + 0b) | (direct branch) | feat/dat-339-pivot-p0-substrate → feat/dat-339-pivot | none yet | schema-per-workspace (locked via /refine, see [dat339-pivot-status.md](./dat339-pivot-status.md)) | Phase 0a DONE — A1 (ffb9b345) + A2 (17b4fe82) + Commit B (23b0516e). Phase 0b DONE — drizzle two-config + pull + normalize + client split + 0a-compose-aftermath fix. Pending commit + reviewer gate. Next: Phase 0c (Starlette + delete FastAPI). |

_(previously merged: DAT-320 #110, DAT-324 #111, DAT-321 #113, DAT-323 #114, DAT-325 #115, DAT-358 #128)_
