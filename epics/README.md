# epics/ — live objectives only

One file per epic currently being worked or next up. The full model is docs/architecture/development-process.md.

- An epic file lands on `main` via a small definition PR — approving that PR is the
  human gate and authorizes the epic's live-eval budget.
- The file is **frozen during the run**. Wrong output → the human fixes the spec.
- **Delete the file in the PR that completes the epic** (or a one-line commit on
  abandonment). Git history is the archive; no `done/`, no backlog files.
- A file that hasn't moved in weeks is a signal, not an archive.

Format: `TEMPLATE.md`. The fenced ` ```yaml scorecard ` block is the machine
contract read by `scorecard/run.py`.
