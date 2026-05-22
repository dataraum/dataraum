# DAT-294 Platform Lanes — Status

At-a-glance view of all parallel platform lanes. Maintained by `/take`.
Row is added when a lane opens its PR; removed when the PR merges.

| Task | Worktree | Branch | PR | Contract | Status |
|---|---|---|---|---|---|
| DAT-340 | .worktrees/DAT-340 | feat/dat-340-retire-mcp-tests | [#129](https://github.com/dataraum/dataraum/pull/129) | none (test deletions + infra hook) | lane smoke green, awaiting review |
| DAT-339 pivot (0a + 0b + 0c + 0d + 0e+0f) | (direct branch) | feat/dat-339-pivot-p0-substrate → feat/dat-339-pivot | none yet | schema-per-workspace (locked via /refine, see [dat339-pivot-status.md](./dat339-pivot-status.md)) | Slice-1 substrate complete: 0a + 0b + 0c + 0d committed + pushed. 0e+0f DONE — tools/ scaffold + README, cockpit_db created at first-boot, dataraum_lake mounted RW into cockpit (Phase 2 add_source handoff), CI drizzle-pull drift check + cross-container lake-handoff smoke. Pending commit + reviewer gate. Next: Phase 1 (read surfaces — first TS Drizzle tools + engine /query Arrow verb). |

_(previously merged: DAT-320 #110, DAT-324 #111, DAT-321 #113, DAT-323 #114, DAT-325 #115, DAT-358 #128)_
