export const meta = {
  name: 'team-build',
  description: 'Fan out approved approaches into autonomous build lanes: each lane = worktree → implement → IN-LANE review (3 agents) → gate → push branch',
  whenToUse: 'After the lead drains the team-refine queue and approves a subset of approaches. Each lane reviews itself (reviewers run inside the lane, like /take) and pushes only on a green review. The lead opens PRs from the MacBook and picks merge order.',
  phases: [
    { title: 'Preflight', detail: "one fetch + per-lane 5 parallel-safety STOP conditions (from /take Step 1)" },
    { title: 'Lane', detail: 'worktree → implement → in-lane review (spec-compliance + senior-code + strict) → gate → push' },
  ],
}

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

// Load-bearing constraints (from /take SKILL.md, verified there):
//  - Worktrees MUST be created INSIDE the repo at .worktrees/{id}/ so the
//    in-lane reviewer subagents (which inherit the orchestrator
//    $CLAUDE_PROJECT_DIR, NOT the EnterWorktree-shifted one) can Read the
//    lane's code. A sibling worktree makes the review gate pass SILENTLY
//    without reading anything. Use manual `git worktree add`, not
//    isolation:'worktree' (its in-project placement is not guaranteed).
//  - Lane subagents do NOT fire the end-of-turn hook → they must run the full
//    CI gate set (ruff/biome + types/tests) locally before pushing, or CI
//    format-check goes red. (feedback_workflow_lanes_run_full_ci_gates)
//  - Reviewers run IN-LANE: the lane agent itself spawns
//    senior-code-reviewer + spec-compliance-reviewer + strict-reviewer (it has
//    the Agent tool), exactly as /implement does inside /take. Push is GATED
//    on their verdict — a blocked review does NOT push.
//  - gh has no token in the sandbox → the lane PUSHES the branch but does NOT
//    open a PR. PRs open from the MacBook; merge is the lead's call.

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

// The lane returns its own work AND its in-lane review verdict. push_gate is
// the machine gate: a lane only pushes when all three reviewers pass.
const LANE_RESULT = {
  type: 'object',
  required: ['id', 'branch', 'ci_gates', 'reviews', 'push_gate', 'pushed', 'summary'],
  properties: {
    id: { type: 'string' },
    branch: { type: 'string' },
    stopped_early: { type: 'boolean', description: 'true if the lane parked a blocker instead of completing (stop-early honored, three-strikes)' },
    blocker: { type: 'string', description: 'if stopped_early: what blocked it' },
    ci_gates: { type: 'string', description: 'local ruff/biome + types/tests result before review (green/red + detail)' },
    lane_smoke: { type: 'string', description: "this task's lane-smoke surface result" },
    reviews: {
      type: 'array',
      description: 'the three IN-LANE reviewer verdicts',
      items: {
        type: 'object',
        required: ['reviewer', 'verdict', 'blocking_findings'],
        properties: {
          reviewer: { type: 'string', enum: ['spec-compliance', 'senior-code', 'strict'] },
          verdict: { type: 'string', enum: ['pass', 'block'] },
          blocking_findings: { type: 'array', items: { type: 'string' } },
        },
      },
    },
    push_gate: {
      type: 'string',
      enum: ['passed', 'blocked-by-review', 'stopped-early'],
      description: "passed = all 3 reviewers passed and the branch was pushed; blocked-by-review = ≥1 reviewer blocked, NOT pushed; stopped-early = lane parked a blocker before review",
    },
    pushed: { type: 'boolean' },
    lanes_unblocked: { type: 'array', items: { type: 'string' }, description: 'task IDs that become ready once this merges' },
    summary: { type: 'string' },
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
// lanes): preflight → build+review+push. A lane failing preflight drops out.
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

  // Stage 2 — the LANE: worktree → implement → IN-LANE review → gate → push.
  // This is one agent running /take Steps 2-7 (minus PR) autonomously. The
  // reviewers run INSIDE this lane (the agent spawns them via its own Agent
  // tool), and push is gated on their verdict.
  (lane) =>
    agent(
      `You are an engineer taking ONE task to a pushed, self-reviewed green branch, autonomously, as part of a parallel team. ` +
      `Task ${lane.id} — "${lane.title}".\n` +
      `Approved approach: ${lane.recommendation}\n` +
      `Assumptions you may rely on: ${(lane.assumptions || []).join(' | ')}\n` +
      (lane.redirect ? `Lead's course-correction (overrides the above where they conflict): ${lane.redirect}\n` : '') +
      `Test strategy: ${lane.test_strategy}\n\n` +
      `Execute /take Steps 2-7 (read .claude/skills/take/SKILL.md), autonomously, MINUS the PR step:\n\n` +
      `1. WORKTREE — create it INSIDE the repo: \`git worktree add .worktrees/${lane.id} -b feat/${lane.id}-${lane.slug} origin/main\`, then EnterWorktree on its absolute path. ` +
      `Inside-repo placement is LOAD-BEARING: your reviewer subagents (next step) inherit the orchestrator $CLAUDE_PROJECT_DIR and can only Read paths under it — a sibling worktree makes the review gate pass silently without reading the code.\n` +
      `2. IMPLEMENT — per /implement discipline. DO-NOT-CHANGE scope: every contract file, any directory owned by another phase, any cross-cutting infra not owned by this task. ` +
      `Honor stop-early/three-strikes: if you hit a real blocker or discover a wrong assumption, set stopped_early:true + blocker, push_gate:'stopped-early', do NOT push, and return — do not power through.\n` +
      `3. LOCAL CI GATES — before review, run the FULL gate set (you do NOT fire the end-of-turn hook): \`uv run ruff format\` + engine gates for Python, \`biome --write\` + \`tsc --noEmit\` for cockpit. Report ci_gates.\n` +
      `4. IN-LANE REVIEW — spawn all THREE reviewers as subagents (Agent tool) over your worktree code, exactly as /implement does, and collect their verdicts into \`reviews\`:\n` +
      `   • spec-compliance-reviewer — was everything in the approved approach + ACs built, nothing extra, every AC tested? (traceability + scope-creep)\n` +
      `   • senior-code-reviewer — correct + idiomatic (async, free-threading, state machines, MCP/Temporal contracts)? flag mock-only tests, dead-code-for-tests, always-pass asserts.\n` +
      `   • strict-reviewer — honors the agreed design + clean-cut (no shims, no half-cuts, no dead code)?\n` +
      `5. GATE — if ANY reviewer returns 'block', set push_gate:'blocked-by-review', do NOT push, and return with the blocking_findings so the lead can decide. Only if all three pass: commit, then PUSH the branch (\`git push origin feat/${lane.id}-${lane.slug}\` — SSH works in the sandbox; do NOT open a PR, gh has no token here), set push_gate:'passed', pushed:true.\n` +
      `6. LANE SMOKE — run this task's lane-smoke surface; report lane_smoke. Refresh this lane's row in .claude/platform-status.md.\n` +
      `7. ExitWorktree(action="keep").\n\n` +
      `The lane closes at branch-push (or at a blocked gate), never at merge. Return the structured lane result.`,
      { label: `lane:${lane.id}`, phase: 'Lane', schema: LANE_RESULT }
    )
)

const lanes = results.filter(Boolean)
const pushed = lanes.filter((l) => l.push_gate === 'passed')
const blocked = lanes.filter((l) => l.push_gate === 'blocked-by-review')
const stalled = lanes.filter((l) => l.push_gate === 'stopped-early')
log(`team-build: ${pushed.length} pushed (review-green), ${blocked.length} blocked by in-lane review, ${stalled.length} stopped early`)

// Lead opens PRs (from the MacBook) for the pushed lanes and picks merge order;
// drains `blocked` (review disputes) and `stalled` (lane blockers) from the queue.
return { lanes, pushed, blocked, stalled }
