# DAT-294 Platform Lanes — Status

At-a-glance view of all parallel platform lanes. Maintained by `/take`.
Row is added when a lane opens its PR; removed when the PR merges.

| Task | Worktree | Branch | PR | Contract | Status |
|---|---|---|---|---|---|
| DAT-389 | .worktrees/DAT-389 | feat/DAT-389-engine-ingest-s3 | (pending — lead opens) | none (path stays connection_config['path']; worker/contracts.py unchanged) | pushed, 3/3 in-lane reviews pass, lane-smoke green (s3:// httpfs ingest → raw+typed, no sources mount) |

_(previously merged: DAT-320 #110, DAT-324 #111, DAT-321 #113, DAT-323 #114, DAT-325 #115, DAT-358 #128, DAT-340 #129, DAT-339 substrate #132, bun toolchain #133)_

For slice-1 feature ticket state see [`dat339-pivot-status.md`](./dat339-pivot-status.md) and [`dat339-slice1-features-plan.md`](./dat339-slice1-features-plan.md).
