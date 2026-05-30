export const meta = {
  name: 'team-refine',
  description: 'Fan out /refine across an approved cut of tasks; park a structured approach summary (or escalation) per task for the lead to drain',
  whenToUse: 'After /decompose, once the lead has approved the cut into independent tasks. Produces the approach queue (gate 3 in ADR-0006). Pairs with team-build.',
  phases: [
    { title: 'Refine', detail: 'one read-only refine agent per task → approach summary or escalation' },
  ],
}

// ── Input ────────────────────────────────────────────────────────────────
// args = array of tasks to refine. Each item:
//   { id: "DAT-391", title: "...", contract?: "path@sha", notes?: "lead's framing" }
// Pass as a real JSON array in the Workflow call's `args`, NOT a stringified list.
const TASKS = Array.isArray(args) ? args : (args?.tasks ?? [])
if (!TASKS.length) {
  log('team-refine: no tasks in args — pass args: [{id, title, ...}, ...]')
  return { error: 'no tasks', approaches: [] }
}

// Structured approach summary = what the lead reads at the queue. Deliberately
// intent-level (recommendation + assumptions + contract deps + risk + size),
// NOT a diff. This is the cheap-to-review artifact ADR-0006 is built around.
const APPROACH = {
  type: 'object',
  required: ['id', 'status', 'recommendation', 'assumptions', 'contract_dependencies', 'risk', 'size', 'test_strategy'],
  properties: {
    id: { type: 'string' },
    status: {
      type: 'string',
      enum: ['approach', 'escalate'],
      description: "'approach' = a recommended way forward the lead can approve; 'escalate' = spec is wrong / money pit / contract unlocked — needs a lead decision before any build",
    },
    recommendation: { type: 'string', description: 'The proposed approach in 2-4 sentences — the headline the lead judges' },
    assumptions: {
      type: 'array',
      items: { type: 'string' },
      description: 'EVERY load-bearing assumption made about the codebase/spec/contracts. This is where confidently-wrong assumptions get caught — be exhaustive and explicit, not confident.',
    },
    contract_dependencies: {
      type: 'array',
      items: { type: 'string' },
      description: 'Contracts/shared artifacts this approach reads or depends on (path@sha if known). Empty if none. Flags cross-lane coupling the cut may have missed.',
    },
    risk: { type: 'string', description: 'The single biggest risk / unknown-unknown, including cross-repo blast radius (eval calibration, testdata, MCP/tool surface)' },
    size: { type: 'string', enum: ['S', 'M', 'L', 'XL'] },
    test_strategy: { type: 'string', description: 'How this lane will prove itself (lane-smoke surface + unit/integration)' },
    escalation_reason: { type: 'string', description: "Required when status='escalate': what the lead must decide and why the lane cannot safely proceed" },
  },
}

phase('Refine')
const approaches = await parallel(
  TASKS.map((t) => () =>
    agent(
      `You are an engineer refining ONE task before implementation, as part of a parallel team. ` +
      `Task: ${t.id} — ${t.title}.` +
      (t.contract ? ` Declared contract: ${t.contract}.` : '') +
      (t.notes ? ` Lead's framing: ${t.notes}.` : '') +
      `\n\nRun the /refine discipline: fetch the Jira ticket (mcp__jira__getJiraIssue) and any linked Confluence design, ` +
      `read the ACTUAL source in depth (not skimmed), trace callers, check git log for related work, and reality-check the ` +
      `spec against the codebase. Your prized outcome is discovering the spec is WRONG or an assumption is unfounded.\n\n` +
      `CRITICAL DIFFERENCE from interactive /refine: do NOT stop and ask "what do you think?" and do NOT write code. ` +
      `Instead PARK a structured result for the lead to review asynchronously:\n` +
      `- If you have a sound way forward → status:'approach' with an exhaustive, honest assumptions list ` +
      `(this list is the firewall against confidently-wrong work — surface every assumption, especially the ones you're tempted to treat as obvious).\n` +
      `- If the spec is wrong, the value/effort is a money-pit, or a named contract is not locked on main → status:'escalate' with escalation_reason.\n` +
      `Return only the structured approach summary.`,
      { label: `refine:${t.id}`, phase: 'Refine', schema: APPROACH }
    )
  )
)

const parked = approaches.filter(Boolean)
const ready = parked.filter((a) => a.status === 'approach')
const escalations = parked.filter((a) => a.status === 'escalate')
log(`team-refine: ${ready.length} approaches parked, ${escalations.length} escalations, of ${TASKS.length} tasks`)

// The lead drains `approaches` in conversation (approve / redirect / defer),
// then feeds the approved subset to team-build.
return { approaches: parked, ready, escalations, requested: TASKS.length }
