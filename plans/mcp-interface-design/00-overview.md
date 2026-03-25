# MCP Interface Design: Overview

Spec: [Practitioner API](https://linear.app/dataraum/document/the-mcp-server-api-practitioner-level-agent-tooling-91181242fb56) |
Epic: [DAT-173](https://linear.app/dataraum/issue/DAT-173/epic-mcp-practitioner-api) |
Architecture: [DAT-144](https://linear.app/dataraum/issue/DAT-144/epic-agent-extensible-architecture) |
Session Protocol: [Agent Session Protocol](https://linear.app/dataraum/document/agent-session-protocol-from-investigation-to-autonomous-operation-c57a4b9c529e) |
hypothesize design: [hypothesize + fix](https://linear.app/dataraum/document/hypothesize-fix-design-document-08990bae7bff)

## Approach

Design the full tool surface holistically via scenarios, then derive implementation phases.
Tools cannot be designed in isolation — response formats and agent journeys compose across tools.

## Design principles

1. **Focused responses, not religious separation.** Each tool returns what its verb needs.
   Don't repeat every quality dimension in every response (no 65k get_quality).
   But if a natural byproduct is useful, include it.
2. **Pipeline is invisible.** The agent never thinks about phases. `measure` triggers
   computation if needed. The pipeline is an implementation detail.
3. **Tools inform, agent decides.** No system-imposed pauses or blocks. The agent sees
   scores, evidence, predictions — and makes its own judgment calls.
4. **Intelligence in tool responses, not system prompts.** A near-vanilla LLM with these
   tools should produce good results. The tool responses carry all domain knowledge.
5. **Agents where reasoning is needed.** `query` and `why` are LLM-powered — they
   synthesize, interpret, and suggest. Other tools are deterministic or BBN-based.
6. **Just the facts for escape hatches.** `run_sql` returns rows. No assumptions, no
   warnings, no magic. If the agent wants intelligence, it uses `query`.
7. **Incremental availability.** `measure` returns partial results as phases complete.
   The agent stays productive during long pipeline runs.
8. **Session trace is always on.** Every tool call logs to the investigation session
   trace (DAT-184). This is the raw material for the investigation narrative (report)
   and the provenance chain (deliver). Build it now, consume it later.
9. **One tool to tell the system things.** `teach` is the single mechanism for
   providing knowledge — column properties, domain rules, acceptances, user decisions.
   No separate fix tool. Agents provide knowledge; detectors verify it against data.

## Tool surface (9 active, 6 deferred)

### Active

| Tool | Verb | Agent-powered? | Modifies state? |
|------|------|---------------|-----------------|
| look | "What am I looking at?" | No | No |
| measure | "How much entropy?" | No | No (caches) |
| why | "Why this score?" | **Yes** (LLM) | No |
| hypothesize | "What if X?" | No (BBN + reads) | No |
| query | "Answer my question" | **Yes** (LLM) | No |
| run_sql | "Execute this SQL" | No | No |
| teach | "Tell the system something" | No | Yes (overlay + audit) |
| begin_session | "Start investigation" | No | Yes (session record) |
| add_source | "Register data source" | No | Yes |

### Deferred (added later based on learnings from active tools)

**Priority 1** — first post-launch additions (enterprise trust story):

| Tool | Verb | Notes |
|------|------|-------|
| deliver | "Seal investigation" | Seals session with provenance chain. session_id from begin_session is the anchor. |
| refuse | "Stop with evidence" | Evidence-based refusal with alternative offer. |
| escalate | "Surface to human" | Pauses session for human input. |
| report | "Current state?" | Compiles investigation narrative from session trace (DAT-184). |

**Priority 2** — structured shortcuts for patterns that emerge from run_sql usage:

| Tool | Verb | Notes |
|------|------|-------|
| compare | "How do these relate?" | Implies LLM. Agent uses run_sql + why suggestions for now. Track which SQL patterns repeat. |
| validate | "Does the math work?" | Agent uses run_sql for deterministic checks. Track which validation patterns repeat. |

## Resolution model

Every tool works from column to dataset. No maximum resolution.

| Tool | Min | Default | Notes |
|------|-----|---------|-------|
| look | column | dataset | `look()` = full schema. `look(target="table")` = columns. `look(target="table.col")` = profile. `look(target="table", sample=N)` = rows. |
| measure | column | table | Pipeline runs broadly; target filters output. Returns partial results while running. |
| why | column+dimension | table+dimension | Agent-powered. Can operate at dataset+dimension level too. |
| hypothesize | column | column | Dispatches on input shape: concept, teach_type, sql, or dimension. See [hypothesize design](https://linear.app/dataraum/document/hypothesize-fix-design-document-08990bae7bff). |
| query | question-scoped | — | LLM agent: NL → SQL → execute → format with decisions_made. |
| run_sql | sql-scoped | — | Just rows. No enrichment. |
| teach | column | column | Typed overlay. Each type has a validation schema. |
| begin_session | session | — | Declares intent + contract. Returns feasibility. |
| add_source | one source | — | Source registration. |

## teach: the unified overlay tool

`teach` is the single mechanism for providing knowledge to the system. It absorbs what
was previously split between `fix` and `teach`. See
[Session Protocol](https://linear.app/dataraum/document/agent-session-protocol-from-investigation-to-autonomous-operation-c57a4b9c529e)
for the rationale behind eliminating `fix`.

### Teach types

| Type | Purpose | Examples |
|------|---------|---------|
| **concept_property** | Any property on a column or concept | `{unit: "cents"}`, `{maps_to: "invoice_total"}`, `{business_name: "Invoice Amount"}`, `{type: "DECIMAL(10,2)"}`, `{role: "timestamp"}`, `{patterns: ["DD.MM.YYYY"]}` |
| **assumption** | Domain rule, free text knowledge | "For MRR, only subscription_cycle invoices count as recurring" |
| **acceptance** | Known issue, documented as expected | "Tax rounding ≤0.03 cents is Stripe convention" |
| **decision** | User calculation preference | "Normalize yearly contracts to monthly (÷12)" |
| **validation** | SQL check → promoted to snippet library | SQL + description, reusable across sessions |
| **detector** | Custom SQL assertion | SQL returning float ∈ [0,1] + dimension mapping |
| **metric** | Metric graph definition | Computation definition with inputs and formula |
| **cycle** | Business cycle definition | Stages, entities, completion indicators |
| **filter** | Cross-column consistency rule | Rule definition |

`concept_property` is the workhorse — it covers what was previously 7 separate fix types
(document_unit, document_business_name, document_type_override, document_type_pattern,
document_timestamp_role, document_unit_source, map_to_concept). The `params` dict
is flexible; validation comes from the config schema registry.

### Goodhart firewall: extensible vs locked

The firewall is **path-based**, not score-provenance-based (see Session Protocol).
Agents cannot change HOW detectors score, only WHAT information is available:

- **Extensible** (agent-writable via teach): Ontology, assumptions, metrics, validations,
  cycles, custom detectors, filters. Agents provide knowledge; detectors verify it
  against data.
- **Locked** (immutable at runtime): Detector scoring logic, contracts, thresholds,
  pipeline structure. Structural enforcement — the agent can't even attempt to write
  to these paths.

The key protection: **detectors verify teachings against actual data.** Teaching
"amount_due is in EUR" when values are 7-digit integers doesn't help — the detector
sees the mismatch. False teachings produce low-confidence annotations, not magically
improved scores. See Session Protocol §"The Third Safety Layer: Data Verification."

### why and hypothesize suggest teach calls

`why`'s LLM and `hypothesize`'s context assembly both know the teach type vocabulary.
Resolution options specify teach calls:

```yaml
# why suggests simple teachings:
resolution_options:
  - teach_type: concept_property
    params: { unit: "cents" }
    description: "Declare unit."

# why suggests hypothesize for complex decisions:
resolution_options:
  - tool: hypothesize
    concept: invoice_total
    description: "Preview concept mapping — see related metrics and validations."

# hypothesize doesn't suggest — it returns context for the agent to decide.
```

### Schema library

Each teach type has a validation schema. `concept_property` params are validated against
the config schema registry (derived from vertical config structure under `config/`).
Other types have their own schemas (e.g., validation requires `sql` + `content`).

Implementation: extend the existing `_build_pydantic_model()` from fix schemas to cover
all teach types. The schema registry is the single source of truth for what can be taught.

## hypothesize: preview before teaching

`hypothesize` is pure computation — no LLM. Dispatches on input shape:

| Input | What it does |
|-------|-------------|
| `concept="invoice_total"` | Ontology lookup → BBN propagation → related metrics, columns, snippets |
| `teach_type="concept_property"` | Schema lookup → BBN node mapping → `what_if_analysis` |
| `sql="SELECT ..."` | Execute read-only → actual score. With `dimension`: also BBN propagation |
| `dimension="value.temporal"` | Direct BBN node → `what_if_analysis`. Cheapest path. |

Returns: affected_nodes, intent_deltas, readiness_before/after, confidence level,
detector_evidence. For concept dispatch: also concept_properties, related_metrics,
related_columns, snippets.

**When to use hypothesize vs just teach:**
- Simple property (unit, business_name): why gives enough context → teach directly
- Concept mapping: hypothesize previews cascading effects → validate → teach
- Data claim (SQL): hypothesize executes + BBN → decide if worth teaching
- Uncertainty: hypothesize shows `must_validate` → agent runs SQL → decides

See [hypothesize design](https://linear.app/dataraum/document/hypothesize-fix-design-document-08990bae7bff)
for full specification.

## Measurement points

A measurement point is a `(target, dimension)` pair — the atomic unit of entropy measurement.

`measure` returns a list of measurement points with scores.
`why` accepts measurement points (or a target+dimension prefix that expands to points).

```yaml
# measure returns:
points:
  - { target: "column:amount_due", dimension: "semantic.units.unit_declaration", score: 0.13 }
  - { target: "column:billing_reason", dimension: "semantic.business_meaning.naming_clarity", score: 0.22 }
  - { target: "table:stripe_invoices", dimension: "computational.reconciliation", score: 0.09 }
```

## Long-running pipeline: polling pattern

MCP tool calls have a 30-second hard timeout. Pipeline runs take ~300 seconds.
`measure` returns partial results and the agent polls:

```
measure()  → status: running, phases_completed: [staging, typing], partial scores
measure()  → status: running, phases_completed: [..., semantic], more scores
measure()  → status: complete, all scores
```

Each call blocks up to ~25s, returns whatever's available. The agent stays productive
between calls — `look`, `why` (on completed dimensions), `run_sql` all work.

## why: agent-powered analysis

`why` is not a database dump. It's an LLM agent that:
1. Reads raw evidence (detector scores, measurements, BBN state)
2. Synthesizes — cross-references evidence, explains the story
3. Generates resolution options as teach calls or hypothesize suggestions
4. Receives the teach type vocabulary so it only suggests valid actions
5. For complex decisions (concept mapping), suggests hypothesize first

## Semantic contracts

| Tool | Returns | Does NOT return |
|------|---------|-----------------|
| look | Schema, types, stats, semantic enrichment (roles, business names), relationships, samples | Entropy scores, readiness |
| measure | Measurement points (target+dimension+score), BBN readiness per column, contract status, pipeline progress | Evidence detail, resolution options |
| why | LLM-synthesized analysis: evidence narrative, resolution options as teach types or hypothesize suggestions | Full column profiles |
| hypothesize | Intent deltas, readiness change, confidence, detector evidence. For concepts: properties, related metrics, related columns, snippets | Does not suggest what to do — agent decides |
| query | Answer (summary+data+sql), decisions_made, open_questions, confidence, teachable_decisions | Entropy scores |
| run_sql | Columns, rows | Nothing else |
| teach | Status, type, scope, teaching_id, measurement_hint | Full score vector |
| begin_session | Session ID, cached scores, known issues, feasibility | Full schema |

## Session anchor

`begin_session` returns a `session_id`. This ID is the anchor for everything:
- All tool calls within the session are logged to the trace (DAT-184)
- When `deliver` is added later, it seals this session with a provenance chain
- When `report` is added, it compiles the narrative from this session's trace
- `teach` records are linked to the session

Nothing about the current design prevents adding session lifecycle tools later.
The session_id threading must be correct from day one.

## teach absorbs fix

Every `fix` action maps cleanly to a `teach`:

| Original fix | As teach |
|---|---|
| `fix(document_unit, unit="cents")` | `teach(type="concept_property", params={unit: "cents"})` |
| `fix(document_business_name, name="...")` | `teach(type="concept_property", params={business_name: "..."})` |
| `fix(document_type_override, type="DATE")` | `teach(type="concept_property", params={type: "DATE"})` |
| `fix(map_to_concept, concept="revenue")` | `teach(type="concept_property", params={maps_to: "revenue"})` |
| `fix(document_accepted_*, reason="...")` | `teach(type="acceptance", params={reason: "..."})` |
| `fix(user_decision, rule="...")` | `teach(type="decision", params={rule: "..."})` |

The fix bridge/interpreter infrastructure survives — teach reuses the routing to config
and metadata interpreters. The fix concept, `apply_fix` MCP tool, and `fixes.yaml`
schemas are retired. Teach type schemas replace them.

### Snippet library as compare/validate infrastructure

SQL snippets (`query/snippet_library.py`) already support 4 discovery strategies and
usage tracking. `teach(type="validation")` promotes SQL to the snippet library.
`hypothesize` surfaces relevant snippets in its response. Future agents discover
validation snippets and re-run them via `run_sql`.

Pattern: `why` suggests SQL → agent runs via `run_sql` → agent teaches as validation
→ snippet promoted → future investigations discover and reuse.

## Known constraints and risks

### query context scaling (highest risk)

`query` is an LLM agent that needs: all teachings, all semantic annotations,
BBN readiness for involved columns. At 31 rows with 3 teachings, this fits easily. At 50
tables, 200 columns, 30 teachings — it may not.

**Mitigation:** Pre-agent for relevance filtering. A fast model (haiku-class) parses the
question, identifies relevant tables/columns from schema, and filters context. Only
teachings and annotations for touched columns (+ dependency chain) go to the full query
agent. Build telemetry for "query agent context size" from day one.

### teach type vocabulary in why

`why`'s LLM needs the teach type schemas as context to suggest valid actions. 9 types
with schemas is manageable. The LLM also needs to know which types apply to the current
target and when to suggest hypothesize instead of direct teaching.

### run_sql shifts analytical burden to agent

Deferring compare/validate means the agent writes SQL for cross-table validation and
comparison, informed by why's suggestions and hypothesize's context. Works for capable
models. Track which SQL patterns agents write repeatedly — those are snippet/tool candidates.

## Scenarios

The design is driven by scenarios that span all tools. Each scenario step has:
- What the agent calls (exact signature)
- What the system does internally
- What the response contains
- What the agent reads and decides
- What it calls next and why

Every field in every response must be read in at least one scenario.

See:
- [01-scenario-investigation.md](01-scenario-investigation.md) — Investigation flow (look, measure, why, run_sql, teach)
- [02-scenario-business-question.md](02-scenario-business-question.md) — Business question → investigation bridge (begin_session, query, teach, run_sql)
- [03-scenario-concept-mapping.md](03-scenario-concept-mapping.md) — Concept mapping with hypothesize (why, hypothesize, run_sql, teach, measure)
