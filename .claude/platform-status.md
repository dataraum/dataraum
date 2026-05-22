# DAT-294 Platform Lanes — Status

At-a-glance view of all parallel platform lanes. Maintained by `/take`.
Row is added when a lane opens its PR; removed when the PR merges.

| Task | Worktree | Branch | PR | Contract | Status |
|---|---|---|---|---|---|
| DAT-340 | .worktrees/DAT-340 | feat/dat-340-retire-mcp-tests | [#129](https://github.com/dataraum/dataraum/pull/129) | none (test deletions + infra hook) | lane smoke green, awaiting review |
| DAT-339 pivot (0a + 0b + 0c + 0d) | (direct branch) | feat/dat-339-pivot-p0-substrate → feat/dat-339-pivot | none yet | schema-per-workspace (locked via /refine, see [dat339-pivot-status.md](./dat339-pivot-status.md)) | Phase 0a + 0b + 0c DONE (committed + pushed). Phase 0d DONE — delete cockpit src/api/ + openapi deps + codegen, collapse agentic chat loop scaffold, sources.tsx placeholder. Pending commit + reviewer gate. Next: Phase 0e+0f (tool registry scaffold + infra mount + CI swap). |

_(previously merged: DAT-320 #110, DAT-324 #111, DAT-321 #113, DAT-323 #114, DAT-325 #115, DAT-358 #128)_
