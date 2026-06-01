export const meta = {
  name: 'team-build',
  description: 'Fan out approved approaches into autonomous build lanes: per-lane pipeline implement → runtime-spawned 3-reviewer stage → DETERMINISTIC JS gate → push branch',
  whenToUse: 'After the lead drains the team-refine queue and approves a subset of approaches. Reviewers run per-lane as a pipeline stage spawned by the workflow RUNTIME (lane agents have no Agent tool — proven 2026-05-31), and the push gate is a deterministic JS decision on their verdicts. The lead opens PRs from the MacBook and picks merge order.',
  phases: [
    { title: 'Preflight', detail: 'one fetch + per-lane 5 parallel-safety STOP conditions (from /take Step 1)' },
    { title: 'Implement', detail: 'worktree → implement (may ASK the lead mid-run) → local CI gates + lane smoke → commit (no push)' },
    { title: 'Review', detail: 'runtime spawns spec-compliance + senior-code + strict per lane, concurrently' },
    { title: 'Gate', detail: 'deterministic JS gate on verdicts → push branch (or park blocked / stopped-early)' },
  ],
}

// ── MID-RUN ASK PROTOCOL (course-correct without losing context) ────────────
// A background lane that hits an ambiguous fork must NOT guess — guessing wrong
// costs hours of work down the wrong branch, and escalate-and-die throws away
// the lane's accumulated context. Instead the lane PAUSES and ASKS: it writes a
// question to a file mailbox, BLOCKS until an answer appears, then RESUMES with
// full context intact. Proven viable by probe wf_c18f4719 (2026-05-31).
//
// THE ORCHESTRATOR (the session that launched this workflow) is the human-proxy:
// while team-build runs in the background, the launcher MUST watch the mailbox
// (a Monitor on .claude/.mailbox emitting on new *.q files), and for each
// question either answer it from project context (CLAUDE.md / memory / ADRs) or
// PushNotification the user for a genuine fork, then write the *.a file. This is
// the org-chart model: the lead fields the engineers' questions and shields the
// user from the routine ones. If no orchestrator is draining the mailbox, asks
// time out → the lane stops-early (it still never guesses). See ADR-0006.
//
// Mailbox is at <repo>/.claude/.mailbox/ (gitignored). Per lane: {id}.q holds the
// open question, {id}.a the answer; both are deleted after each exchange, so a
// lane can ask sequentially (it blocks between asks → never two open questions
// from one lane). team-build is one-run-at-a-time (it mutates worktrees + the
// status board), so per-lane filenames don't collide across runs.
const ASK_PROTOCOL = (laneId) =>
  `\n\nMID-RUN ASK (use this instead of guessing on an ambiguous fork; it is NOT failure — guessing wrong is):\n` +
  `When you hit a real design fork the approved approach does not settle, do NOT pick a branch and barrel on. ASK the lead and wait:\n` +
  `  1. MB="$(git rev-parse --show-toplevel)/.claude/.mailbox"; mkdir -p "$MB"; rm -f "$MB/${laneId}.a"\n` +
  `  2. Write your question (be specific; give the options and the context the lead needs to decide) to "$MB/${laneId}.q" via a heredoc.\n` +
  `  3. BLOCK for the answer: \`for i in $(seq 1 360); do [ -f "$MB/${laneId}.a" ] && break; sleep 5; done\` (waits up to 30 min).\n` +
  `  4. If "$MB/${laneId}.a" exists: read it, \`rm -f "$MB/${laneId}.q" "$MB/${laneId}.a"\`, record the {question, answer} pair in your \`asks\` result, and CONTINUE with that decision (it overrides your default; honor any extra instruction it carries).\n` +
  `  5. If it never arrived (TIMEOUT — no orchestrator was draining the mailbox): do NOT guess. Set stopped_early:true, blocker:"unanswered ask: <your question>", committed:false, and return.\n` +
  `Ask sparingly and batch where you can — each ask blocks your lane (and holds a concurrency slot). Reserve it for forks that would be expensive to undo; keep using stop-early for "I am fundamentally blocked".`

// ── Input ────────────────────────────────────────────────────────────────
// args = approved approaches from team-refine (status:'approach' only), each
// optionally annotated by the lead with `redirect` notes. Shape per item:
//   { id, title, recommendation, assumptions[], contract_dependencies[], size,
//     test_strategy, redirect?: "lead's course-correction" }
// Pass as a real JSON array, NOT a stringified list.
// Tolerate args arriving as an array, an object with .lanes, or a JSON string
// (the saved-workflow `name` launch path can deliver args JSON-encoded).
let _args = args
if (typeof _args === 'string') {
  try { _args = JSON.parse(_args) } catch { _args = [] }
}
const LANES = Array.isArray(_args) ? _args : (_args?.lanes ?? [])
if (!LANES.length) {
  log('team-build: no approved lanes in args — pass args: [<approved approach>, ...]')
  return { error: 'no lanes', results: [] }
}

// ── Architecture (why reviewers are a STAGE, not nested in the lane) ────────
// A workflow agent()'s entire toolset is Bash, Edit, Read, Skill, ToolSearch,
// Write, StructuredOutput — there is NO Agent tool, and an agentType override
// does NOT add one (probe wf_acbcecdc, 2026-05-31). So a lane agent CANNOT
// spawn reviewer subagents — the old "in-lane nested review" design was
// structurally impossible (silent no-op). Fix: the RUNTIME spawns the three
// reviewers as their own pipeline stage, per lane. Because pipeline() has no
// barrier between stages, lane X's review fires the instant X's implement
// finishes — concurrent with lane Y still building. That keeps review per-lane
// (no one-big-review bottleneck) without nesting. The push gate is then a
// DETERMINISTIC JS decision on the structured verdicts (not a prompt the lane
// can skip — the PR #161 failure mode).
//
// Other load-bearing constraints (from /take SKILL.md, verified there):
//  - Worktrees live INSIDE the repo at .worktrees/{id}/ (gitignored), under
//    $CLAUDE_PROJECT_DIR, so the reviewer-stage agents can Read the lane's code.
//    Use manual `git worktree add`, not isolation:'worktree'.
//  - Lane agents do NOT fire the end-of-turn hook → they must run the full CI
//    gate set (ruff/biome + types/tests) locally before they commit, or CI
//    format-check goes red. (feedback_workflow_lanes_run_full_ci_gates)
//  - The lane COMMITS on its branch (so reviewers can diff origin/main...HEAD)
//    but does NOT push. Push happens only in the Gate stage, only if the JS
//    gate passes. gh has no token in the sandbox → push the branch, never open
//    a PR; the lead opens PRs from the MacBook and picks merge order.

const REVIEWERS = [
  {
    key: 'spec-compliance',
    agentType: 'spec-compliance-reviewer',
    lens: 'Was everything in the approved approach + ACs built, nothing extra, and is every AC tested? Traceability + scope-creep.',
  },
  {
    key: 'senior-code',
    agentType: 'senior-code-reviewer',
    lens: 'Correct + idiomatic (async, free-threading, state machines, MCP/Temporal contracts)? Flag mock-only tests, dead-code-for-tests, always-pass asserts.',
  },
  {
    key: 'strict',
    agentType: 'strict-reviewer',
    lens: 'Does it honor the agreed design + clean-cut rule (no shims, no half-cuts, no dead code kept to pass tests)?',
  },
]

const PREFLIGHT = {
  type: 'object',
  required: ['id', 'can_open', 'blockers'],
  properties: {
    id: { type: 'string' },
    can_open: { type: 'boolean', description: 'true only if ALL five STOP conditions pass' },
    blockers: { type: 'array', items: { type: 'string' }, description: 'which of the 5 conditions failed (empty if can_open)' },
    slug: { type: 'string', description: 'kebab slug for the branch feat/{id}-{slug}' },
  },
}

const IMPLEMENT_RESULT = {
  type: 'object',
  required: ['id', 'branch', 'worktree_path', 'ci_gates', 'committed', 'stopped_early', 'summary'],
  properties: {
    id: { type: 'string' },
    branch: { type: 'string' },
    worktree_path: { type: 'string', description: 'ABSOLUTE path to the lane worktree, so the reviewer stage can Read it' },
    ci_gates: { type: 'string', description: 'local ruff/biome + types/tests result (green/red + detail)' },
    lane_smoke: { type: 'string', description: "this task's lane-smoke surface result" },
    committed: { type: 'boolean', description: 'true if work is committed on the branch (ready for review). false if stopped_early before any commit.' },
    stopped_early: { type: 'boolean', description: 'true if the lane parked a blocker / found a wrong assumption instead of completing' },
    blocker: { type: 'string', description: 'if stopped_early: what blocked it (incl. "unanswered ask: ..." on ask timeout)' },
    asks: {
      type: 'array',
      description: 'every mid-run ASK this lane resolved (empty if none) — the course-corrections the lead made while it ran',
      items: {
        type: 'object',
        required: ['question', 'answer'],
        properties: {
          question: { type: 'string' },
          answer: { type: 'string', description: "the lead's decision the lane then acted on" },
        },
      },
    },
    lanes_unblocked: { type: 'array', items: { type: 'string' }, description: 'task IDs that become ready once this merges' },
    summary: { type: 'string' },
  },
}

const REVIEW_VERDICT = {
  type: 'object',
  required: ['reviewer', 'verdict', 'blocking_findings'],
  properties: {
    reviewer: { type: 'string', enum: ['spec-compliance', 'senior-code', 'strict'] },
    verdict: { type: 'string', enum: ['pass', 'block'] },
    blocking_findings: { type: 'array', items: { type: 'string' }, description: 'empty when verdict=pass' },
  },
}

const FINALIZE = {
  type: 'object',
  required: ['pushed', 'detail'],
  properties: {
    pushed: { type: 'boolean' },
    detail: { type: 'string', description: 'push output (if pushed) or what was recorded on the status board' },
  },
}

// One fetch before fan-out (avoid lanes racing on origin/main).
phase('Preflight')
log('team-build: fetching origin/main once before fan-out')
await agent(
  `Run \`git fetch origin main\` in the repo root and report the resulting origin/main sha. Do nothing else.`,
  { label: 'fetch-main', phase: 'Preflight' }
)

// Each lane runs the full chain independently (pipeline, no barrier between
// lanes). Stages: preflight → implement → review(3, runtime-spawned) → gate+push.
const results = await pipeline(
  LANES,

  // Stage 1 — preflight (the 5 STOP conditions from /take Step 1, CALLED not reinvented)
  (lane) =>
    agent(
      `Pre-flight check for build lane ${lane.id} ("${lane.title}"), per /take Step 1 (read .claude/skills/take/SKILL.md). ` +
      `Verify ALL five parallel-safety conditions and report can_open + any blockers:\n` +
      `1. Parent is a real epic phase; ticket status is To Do / In Progress.\n` +
      `2. Every \`blocked by\` dependency is Done.\n` +
      `3. No existing worktree on a mismatched branch at .worktrees/${lane.id}/ (resume if branch matches feat/${lane.id}-{slug}).\n` +
      `4. No PR already open (\`gh pr list --search "${lane.id} in:title"\` — may fail in sandbox; if gh has no token, note it and rely on the status board).\n` +
      `5. No status-board claim (.claude/platform-status.md) on this task or a contract it touches; and if the approach names a contract (${(lane.contract_dependencies || []).join(', ') || 'none'}), it must be locked on main.\n` +
      `Return the structured preflight result with a kebab slug for the branch.`,
      { label: `preflight:${lane.id}`, phase: 'Preflight', schema: PREFLIGHT }
    ).then((pf) => {
      if (!pf.can_open) throw new Error(`preflight failed for ${lane.id}: ${pf.blockers.join('; ')}`)
      return { ...lane, slug: pf.slug }
    }),

  // Stage 2 — IMPLEMENT: worktree → implement → local CI gates + lane smoke →
  // COMMIT (no review here — the lane has no Agent tool — and no push).
  (lane) =>
    agent(
      `You are an engineer taking ONE task to a committed, CI-green branch, autonomously, as part of a parallel team. ` +
      `Task ${lane.id} — "${lane.title}".\n` +
      `Approved approach: ${lane.recommendation}\n` +
      `Assumptions you may rely on: ${(lane.assumptions || []).join(' | ')}\n` +
      (lane.redirect ? `Lead's course-correction (overrides the above where they conflict): ${lane.redirect}\n` : '') +
      `Test strategy: ${lane.test_strategy}\n\n` +
      `Execute /take Steps 2-3 + 6 (read .claude/skills/take/SKILL.md), autonomously. You will NOT review or push — ` +
      `the workflow runtime runs the reviewers and the push gate after you (you have no Agent tool, so you cannot spawn reviewers yourself).\n\n` +
      `1. WORKTREE — create it INSIDE the repo with Bash: \`git worktree add .worktrees/${lane.id} -b feat/${lane.id}-${lane.slug} origin/main\`. ` +
      `If it fails on an index/worktree lock (concurrent lanes), wait briefly and retry up to 3x. ` +
      `Then \`pwd\` to record the ABSOLUTE worktree path (\`<repo>/.worktrees/${lane.id}\`) and report it as worktree_path — the reviewer stage Reads the code there. ` +
      `Work with absolute paths under that dir and \`git -C <worktree_path> ...\`; do not rely on EnterWorktree.\n` +
      `2. IMPLEMENT — per /implement discipline. DO-NOT-CHANGE scope: every contract file, any directory owned by another phase, any cross-cutting infra not owned by this task. ` +
      `Honor stop-early/three-strikes: if you hit a real blocker or discover a wrong assumption, set stopped_early:true + blocker, committed:false, do NOT commit, and return — do not power through. ` +
      `BUT for an ambiguous DESIGN FORK (not a hard block) where the approved approach is silent, use the MID-RUN ASK protocol below to get the lead's decision instead of guessing or stopping.` +
      ASK_PROTOCOL(lane.id) + `\n` +
      `3. LOCAL CI GATES — run the FULL gate set (you do NOT fire the end-of-turn hook): \`uv run ruff format\` + engine gates for Python, \`biome --write\` + \`tsc --noEmit\` for cockpit. Report ci_gates. If a gate is red and you cannot fix it cleanly, treat it as stop-early.\n` +
      `4. LANE SMOKE — run this task's lane-smoke surface; report lane_smoke.\n` +
      `5. COMMIT on the branch (\`git -C <worktree_path> add -A && git -C <worktree_path> commit\`) so the reviewers can diff origin/main...HEAD. Set committed:true. Do NOT push.\n` +
      `Return the structured implement result.`,
      { label: `impl:${lane.id}`, phase: 'Implement', schema: IMPLEMENT_RESULT }
    ).then((impl) => ({ ...lane, ...impl })),

  // Stage 3 — REVIEW: the RUNTIME spawns the three reviewers (each its own
  // agentType) over this lane's worktree, concurrently. This is the fix for the
  // nested-Agent limitation: reviewers are siblings spawned by the runtime, not
  // children of the lane. A stopped-early lane skips review.
  (built) => {
    if (built.stopped_early || !built.committed) {
      return { ...built, reviews: [] }
    }
    return parallel(
      REVIEWERS.map((rv) => () =>
        agent(
          `Review build lane ${built.id} — "${built.title}". The committed work is on branch ${built.branch} ` +
          `in the worktree at ${built.worktree_path}. Read it there; inspect the diff with ` +
          `\`git -C ${built.worktree_path} diff origin/main...HEAD\` and the changed files.\n\n` +
          `Approved approach (the spec to check against): ${built.recommendation}\n` +
          `Assumptions it was allowed to rely on: ${(built.assumptions || []).join(' | ')}\n` +
          (built.redirect ? `Lead's course-correction: ${built.redirect}\n` : '') +
          `\nYour review lens: ${rv.lens}\n\n` +
          `Return verdict 'pass' or 'block' (block only on a real, must-fix problem) with blocking_findings.`,
          { label: `review:${rv.key}:${built.id}`, phase: 'Review', schema: REVIEW_VERDICT, agentType: rv.agentType }
        )
      )
    ).then((reviews) => ({ ...built, reviews: reviews.filter(Boolean) }))
  },

  // Stage 4 — GATE + PUSH. The gate decision is DETERMINISTIC JS on the
  // verdicts — not a prompt the lane can skip. Only a clean sweep pushes.
  async (reviewed) => {
    let push_gate
    if (reviewed.stopped_early || !reviewed.committed) {
      push_gate = 'stopped-early'
    } else {
      const allPass =
        reviewed.reviews.length === REVIEWERS.length &&
        reviewed.reviews.every((r) => r.verdict === 'pass')
      push_gate = allPass ? 'passed' : 'blocked-by-review'
    }

    const blocking = (reviewed.reviews || [])
      .filter((r) => r.verdict === 'block')
      .map((r) => `${r.reviewer}: ${r.blocking_findings.join('; ')}`)

    const instruction =
      push_gate === 'passed'
        ? `GATE PASSED (all three reviewers passed — decided by the orchestrator). PUSH the branch: ` +
          `\`git -C ${reviewed.worktree_path} push origin ${reviewed.branch}\` (SSH works in the sandbox; do NOT open a PR — gh has no token here). ` +
          `Then set this lane's row in .claude/platform-status.md to "pushed, awaiting PR". Return pushed:true with the push output.`
        : push_gate === 'blocked-by-review'
        ? `GATE BLOCKED by review — do NOT push. Record this lane's row in .claude/platform-status.md as "blocked by review" with: ${blocking.join(' || ')}. Return pushed:false.`
        : `Lane stopped early before review (${reviewed.blocker || 'see implement summary'}) — do NOT push. ` +
          `Record this lane's row in .claude/platform-status.md as "stopped early: ${reviewed.blocker || 'blocker'}". Return pushed:false.`

    const fin = await agent(
      `Finalize build lane ${reviewed.id} ("${reviewed.title}"). ${instruction}`,
      { label: `gate:${reviewed.id}`, phase: 'Gate', schema: FINALIZE }
    )

    return {
      id: reviewed.id,
      branch: reviewed.branch,
      push_gate,
      pushed: !!fin.pushed,
      stopped_early: !!reviewed.stopped_early,
      blocker: reviewed.blocker,
      ci_gates: reviewed.ci_gates,
      lane_smoke: reviewed.lane_smoke,
      asks: reviewed.asks || [],
      reviews: reviewed.reviews || [],
      lanes_unblocked: reviewed.lanes_unblocked || [],
      summary: reviewed.summary,
      finalize_detail: fin.detail,
    }
  }
)

const lanes = results.filter(Boolean)
const pushed = lanes.filter((l) => l.push_gate === 'passed')
const blocked = lanes.filter((l) => l.push_gate === 'blocked-by-review')
const stalled = lanes.filter((l) => l.push_gate === 'stopped-early')
const totalAsks = lanes.reduce((n, l) => n + (l.asks?.length || 0), 0)
log(`team-build: ${pushed.length} pushed (review-green), ${blocked.length} blocked by review, ${stalled.length} stopped early, ${totalAsks} mid-run asks resolved`)

// Lead opens PRs (from the MacBook) for the pushed lanes and picks merge order;
// drains `blocked` (review disputes) and `stalled` (lane blockers) from the queue.
return { lanes, pushed, blocked, stalled, asks_resolved: totalAsks }
