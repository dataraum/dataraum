// `answer` — the natural-language query tool (DAT-485, DD/33259521).
//
// The 4th-attempt query migration, landed as a thin TS consumer over the engine-
// owned snippet substrate. The tool is a nested @tanstack/ai chat() SUB-AGENT
// with its own internal tools (snippet_search over the validated KB, run_steps to
// validate SQL). It composes a question's answer as concept-named steps + a
// combining final_sql, reusing validated snippets; it VALIDATES the composed CTE
// statement and reads a bounded headline; the BROWSER executes the full result
// via that SAME composed statement (the grid handle). Gating is gone: a read-only
// data-quality band rides along as INFORMATION, never a filter.
//
// CTE-based execution + bound validation (DAT-485 review): the run_steps tool
// composes the steps into ONE standalone CTE statement, validates THAT, and
// CAPTURES it. The grid is composed from the captured (validated) form — NOT from
// any re-emitted model output — so the headline the model states and the grid the
// user streams are provably the same query (no validate-X-emit-Y drift, no
// temp-view-vs-grid divergence). The model's structured draft carries only the
// narrative + provenance.
//
// Reuse is CLASSIFY-don't-substitute (DAT-485 review): the cockpit addresses
// tables as lake.<layer>.<name> while stored snippets use bare names, so swapping
// in a stored snippet's SQL would not resolve. Instead each component keeps its
// executable (validated) SQL and is tagged exact_reuse / adapted / fresh — the
// measurable re-usage surface (components[]) that P2a/P2b build on. The match uses
// `canonicalizeForReuse` so a qualified reference matches a bare stored snippet.
//
// The outer tool wraps the sub-agent in `asAgentError`: a failed run becomes the
// `{ error }` envelope the orchestrator reads and retries, not a dead turn.

import { randomUUID } from "node:crypto";
import { chat, maxIterations, toolDefinition } from "@tanstack/ai";
import { createAnthropicChat } from "@tanstack/ai-anthropic";
import { z } from "zod";

import { config } from "../config";
import { findById } from "../db/metadata/snippet-library";
import { saveQuerySnippet } from "../db/metadata/snippet-writer";
import {
	composeStandalone,
	runSteps,
	validateStepNames,
} from "../duckdb/run-steps";
import { linkedAbortController } from "../lib/abort";
import { sqlEquivalent } from "../lib/sql-canonical";
import {
	MAX_OUTPUT_TOKENS,
	MODEL,
	QUERY_SUBAGENT_MAX_ITERATIONS,
} from "../llm";
import { getQueryInstructions } from "../prompts";
import { asAgentError, withAgentError } from "./agent-error";
import { listTables } from "./list-tables";
import { buildSchemaBlock } from "./query-context";
import { buildVocabularyBlock, snippetSearchTool } from "./snippet-search";

// The three readiness bands the engine emits, worst-first ranked.
const BAND = z.enum(["ready", "investigate", "blocked"]);
type Band = z.infer<typeof BAND>;
const BAND_SEVERITY: Record<Band, number> = {
	ready: 1,
	investigate: 2,
	blocked: 3,
};

// --- The reuse classification — the measurable re-usage surface.

const UsageType = z.enum(["exact_reuse", "adapted", "fresh"]);

/** One validated CTE component + how it relates to the snippet KB. */
const Component = z.object({
	// The concept name (the CTE name).
	name: z.string(),
	// The executable (validated) SQL the component contributes.
	sql: z.string(),
	// The snippet it reused/adapted, or null when fresh / a hallucinated id.
	snippet_id: z.string().nullable(),
	// exact_reuse = reproduced a validated snippet (modulo table qualifier);
	// adapted = started from one but changed it; fresh = newly composed.
	usage: UsageType,
});
export type Component = z.infer<typeof Component>;

/**
 * How much of the answer's SQL is grounded in validated snippets vs newly
 * generated — a cheap, honest reliability read aggregated from `components`.
 * `grounded_ratio` = (exact_reuse + adapted) / total. INFORMATIONAL, never a gate
 * (the contract/confidence signal the old design tried to enforce, reborn as info).
 */
const Reliability = z.object({
	grounded_ratio: z.number(),
	exact_reuse: z.number(),
	adapted: z.number(),
	fresh: z.number(),
});

// --- The model's structured draft (inner outputSchema). The model emits ONLY the
// narrative + provenance; the steps/final_sql it validated live in its run_steps
// call (captured server-side), so they can't drift from what the grid runs.

const QueryDraftSchema = z.object({
	answer: z
		.string()
		.describe(
			"The practitioner-facing reply with the headline number(s) from the " +
				"validated sample. No SQL, tool names, or internal identifiers.",
		),
	assumptions: z
		.array(z.string())
		.describe(
			"Plain-sentence decisions made to resolve ambiguity (may be empty).",
		),
	concepts_used: z
		.array(z.string())
		.describe("The business concepts the answer draws on (provenance)."),
	tables_touched: z
		.array(z.string())
		.describe(
			"The physical table names the SQL reads (the <name> in lake.<layer>.<name>).",
		),
});
export type QueryDraft = z.infer<typeof QueryDraftSchema>;

// --- The tool's answer shape (outer). grid + data_quality are derived; both are
// nullable for the degenerate cases (no runnable query / no analyzed table).

const Grid = z
	.object({
		// The single self-contained statement the browser grid streams in full — the
		// EXACT composed CTE statement run_steps validated. No params: the sub-agent
		// bakes literals from the question, so the grid query is fully literal.
		sql: z.string(),
	})
	.nullable();

const DataQuality = z
	.object({
		band: BAND,
		note: z.string().optional(),
	})
	.nullable();

export const AnswerSchema = z.object({
	answer: z.string(),
	// The grid handle — the browser streams the FULL result from this SQL
	// (DAT-490 uncapped). null when the sub-agent produced no validated query.
	grid: Grid,
	assumptions: z.array(z.string()),
	concepts_used: z.array(z.string()),
	tables_touched: z.array(z.string()),
	// READ from the readiness views for the touched tables — informational, NEVER
	// a gate. null when no touched table has been analyzed.
	data_quality: DataQuality,
	// The validated CTE components + their reuse classification — the measurable
	// re-usage surface (P2a saves fresh/adapted as snippets; P2b counts reuse).
	components: z.array(Component),
	// How grounded the answer's SQL is in validated snippets (from components).
	reliability: Reliability,
});
export type AnswerResult = z.infer<typeof AnswerSchema>;

// --- run_steps as the sub-agent's internal validator tool.

const RunStepsOk = z.object({
	ok: z.literal(true),
	columns: z.array(z.string()),
	rowCount: z.number(),
	sample: z.array(z.record(z.string(), z.unknown())),
	truncated: z.boolean(),
});

/** What a successful run_steps validation captured, for the grid + the surface. */
interface ValidatedRun {
	composedSql: string;
	components: Component[];
}

// --- Reuse classification (CLASSIFY-don't-substitute; informed by the engine's
// agent.py `_resolve_snippet_references`).

/**
 * Classify each step against the snippet it referenced — the reuse teeth, as a
 * MEASURE rather than a substitution (the cockpit's qualified addressing means a
 * stored bare-name snippet wouldn't resolve if swapped in, so we keep the model's
 * executable SQL and tag the relationship):
 * - no snippet_id → `fresh`;
 * - unknown id (findById null) → `fresh`, the hallucinated id cleared;
 * - known id, SQL matches the stored snippet (canonicalized + normalized) →
 *   `exact_reuse` (the model reproduced the validated snippet);
 * - known id, SQL differs → `adapted`, snippet_id tracks provenance.
 *
 * `canonicalizeForReuse` strips the `lake.<layer>.` qualifier before the match so
 * a qualified model reference matches a bare stored snippet. findById is a global
 * PK lookup (P0) — not workspace-scoped — so a snippet_id from snippet_search
 * (which IS workspace-scoped) resolves to the one row it names.
 */
export async function classifyComponents(
	steps: { name: string; sql: string; snippet_id?: string | null }[],
): Promise<Component[]> {
	const out: Component[] = [];
	for (const step of steps) {
		if (!step.snippet_id) {
			out.push({
				name: step.name,
				sql: step.sql,
				snippet_id: null,
				usage: "fresh",
			});
			continue;
		}
		const record = await findById(step.snippet_id);
		if (record === null) {
			// Hallucinated id — treat as fresh.
			out.push({
				name: step.name,
				sql: step.sql,
				snippet_id: null,
				usage: "fresh",
			});
			continue;
		}
		// AST-canonical comparison (polyglot round-trip; DAT-485): exact_reuse when
		// the model reproduced the validated snippet modulo cosmetic variance, else
		// adapted. Self-consistent — polyglot canonicalizes BOTH sides — so it does
		// NOT assume cross-language agreement with the engine's sqlglot (the stress-
		// test found they diverge; one canonicalizer in both is DAT-492). The model's
		// executable SQL is always KEPT (classify, don't substitute).
		const usage = (await sqlEquivalent(step.sql, record.sql))
			? "exact_reuse"
			: "adapted";
		out.push({
			name: step.name,
			sql: step.sql,
			snippet_id: step.snippet_id,
			usage,
		});
	}
	return out;
}

/**
 * The per-invocation run_steps tool: it composes the model's steps + final_sql
 * into ONE standalone CTE statement, validates THAT (the exact form the grid
 * runs), and CAPTURES it (the last successful validation) into `captured`. The
 * model sees only the validator status; the composed SQL + components stay
 * server-side, so the grid is provably the validated query — not a re-emission.
 */
function makeRunStepsTool(captured: { value: ValidatedRun | null }) {
	return toolDefinition({
		name: "run_steps",
		description:
			"Validate your decomposed query before answering. Pass your concept " +
			"`steps` (each {name, sql}, plus snippet_id when you reuse/adapt a snippet) " +
			"and the combining `final_sql`. They are folded into one CTE statement and " +
			"run on a read-only connection; you get back ok + the result columns + a " +
			"BOUNDED headline sample (not the full result — the full result streams to " +
			"the user's grid), or an error message to repair and retry. Always call " +
			"this before you answer; the LAST query you validate here is the one the " +
			"user's grid will run.",
		inputSchema: z.object({
			steps: z
				.array(
					z.object({
						name: z.string(),
						sql: z.string(),
						snippet_id: z
							.string()
							.nullable()
							.optional()
							.describe("The reused/adapted snippet's id; omit for fresh SQL."),
					}),
				)
				.describe("The concept steps; each becomes a CTE named `name`."),
			final_sql: z
				.string()
				.describe("The query combining the step CTEs into the final result."),
		}),
		outputSchema: withAgentError(RunStepsOk),
	}).server(async (input, ctx) => {
		const nameError = validateStepNames(input.steps);
		if (nameError) return { error: nameError };
		const components = await classifyComponents(input.steps);
		const composed = composeStandalone(
			components.map((c) => ({ name: c.name, sql: c.sql })),
			input.final_sql,
		);
		const result = await runSteps(composed, ctx?.abortSignal);
		if ("ok" in result) captured.value = { composedSql: composed, components };
		return result;
	});
}

// --- Data-quality band (informational; reuses the tested list_tables rollup).

/**
 * Read the worst readiness band across the touched tables (informational, never a
 * gate). Matches `tables_touched` against the inventory's physical OR display
 * names (the model may emit either), takes the worst band among the analyzed
 * matches, and notes which tables carry it. null when nothing matched or nothing
 * is analyzed yet.
 */
export async function readDataQuality(
	tablesTouched: string[],
): Promise<z.infer<typeof DataQuality>> {
	if (tablesTouched.length === 0) return null;

	const inventory = await listTables();
	const touched = new Set(tablesTouched.map((t) => t.toLowerCase()));
	const matched = inventory.filter(
		(t) =>
			touched.has(t.physical_name.toLowerCase()) ||
			touched.has(t.table_name.toLowerCase()),
	);

	const banded = matched.filter(
		(t): t is typeof t & { worst_band: Band } => t.worst_band !== null,
	);
	if (banded.length === 0) return null;

	const worst = banded.reduce<Band>(
		(acc, t) =>
			BAND_SEVERITY[t.worst_band] > BAND_SEVERITY[acc] ? t.worst_band : acc,
		"ready",
	);
	const worstTables = banded
		.filter((t) => t.worst_band === worst)
		.map((t) => t.table_name);

	return {
		band: worst,
		note: `Worst readiness across the queried tables is '${worst}' (${worstTables.join(", ")}).`,
	};
}

// --- Assembly (pure) + the sub-agent.

/**
 * Assemble the answer from the model draft + the captured validated run + the
 * data-quality band (pure). The grid is the CAPTURED composed statement (what was
 * validated), null when nothing validated. The components are the captured reuse
 * surface. Unit-tested.
 */
export function assembleAnswer(
	draft: QueryDraft,
	validated: ValidatedRun | null,
	dataQuality: z.infer<typeof DataQuality>,
): AnswerResult {
	const grid =
		validated && validated.composedSql.trim() !== ""
			? { sql: validated.composedSql }
			: null;
	const components = validated ? validated.components : [];
	const counts = { exact_reuse: 0, adapted: 0, fresh: 0 };
	for (const c of components) counts[c.usage] += 1;
	const total = components.length;
	return {
		answer: draft.answer,
		grid,
		assumptions: draft.assumptions,
		concepts_used: draft.concepts_used,
		tables_touched: draft.tables_touched,
		data_quality: dataQuality,
		components,
		reliability: {
			grounded_ratio:
				total > 0 ? (counts.exact_reuse + counts.adapted) / total : 0,
			...counts,
		},
	};
}

// --- Save-on-clean (DAT-486 P2a): the learning loop.

/**
 * The components save-on-clean persists: the freshly-composed ones (`fresh`,
 * `adapted`). `exact_reuse` is skipped — that step already reproduced a curated
 * snippet, so saving a `query:` copy of the same concept is redundant (it would
 * just dedup), and re-saving over a `graph:` row is exactly what we must avoid.
 * Pure — unit-tested.
 */
export function componentsToSave(components: Component[]): Component[] {
	return components.filter((c) => c.usage === "fresh" || c.usage === "adapted");
}

/**
 * Persist a clean run's freshly-composed concept steps as learned `query:`
 * snippets so the library grows from real questions and reuse compounds (P2a).
 * Keyed by concept (standardField = the CTE name), first-writer-wins; all of one
 * answer's saved steps share one `query:<runId>` provenance group.
 *
 * Best-effort: a learning side-effect must NEVER fail the answer (the product),
 * so every error — including `permission denied` before the engine re-bootstraps
 * with the sql_snippets grant (read_views.py) — is logged and swallowed. Skipped
 * when nothing fresh/adapted was composed. Snippets are workspace-scoped now
 * (DAT-506: the `workspace_id` column replaced the session FK), so there is no
 * session gate.
 */
export async function persistLearnedSnippets(
	validated: ValidatedRun | null,
): Promise<void> {
	if (!validated) return;
	const toSave = componentsToSave(validated.components);
	if (toSave.length === 0) return;
	try {
		// Read-path workspace scoping resolves from the env-designated workspace,
		// not the cockpit_db registry (DAT-505 boundary): in single-active-workspace
		// the two are identical. It is BOTH the snippet's `workspace_id` and the
		// `schema_mapping_id` key value. Per-request registry resolution of the
		// active workspace for reads is the DAT-357 switcher.
		const workspaceId = config.dataraumWorkspaceId;
		const source = `query:${randomUUID()}`;
		for (const c of toSave) {
			await saveQuerySnippet({
				schemaMappingId: workspaceId,
				standardField: c.name,
				workspaceId,
				sql: c.sql,
				description: `Learned from a query: ${c.name}`,
				source,
				llmModel: MODEL,
			});
		}
	} catch (err) {
		console.warn(`[cockpit] save-on-clean failed: ${err}`);
	}
}

/**
 * The query sub-agent: ONE nested chat() over [snippet_search, run_steps] with the
 * concrete `QueryDraftSchema`, then deterministic post-processing (the grid +
 * components come from the captured validated run; the data-quality band is read).
 * `signal` forwards the outer run's abort into the nested call and the run_steps
 * validator.
 */
export async function querySubAgent(
	question: string,
	signal?: AbortSignal,
): Promise<AnswerResult> {
	const [schemaBlock, vocabularyBlock] = await Promise.all([
		buildSchemaBlock(),
		buildVocabularyBlock(),
	]);

	const userMessage = `<question>\n${question}\n</question>\n\n${schemaBlock}\n\n${vocabularyBlock}`;

	// Per-invocation capture cell — the run_steps tool writes the last successful
	// validation here, so it's isolated across concurrent answer calls.
	const captured: { value: ValidatedRun | null } = { value: null };

	// Combined tools + outputSchema is native for claude-sonnet-4-6 — one call
	// runs the tool loop and returns the validated structured draft (no separate
	// finalize round-trip). The concrete schema gives a typed result.
	const draft = await chat({
		adapter: createAnthropicChat(MODEL, config.anthropicApiKey),
		abortController: linkedAbortController(signal),
		modelOptions: { max_tokens: MAX_OUTPUT_TOKENS },
		agentLoopStrategy: maxIterations(QUERY_SUBAGENT_MAX_ITERATIONS),
		systemPrompts: [getQueryInstructions()],
		messages: [{ role: "user", content: userMessage }],
		tools: [snippetSearchTool, makeRunStepsTool(captured)],
		outputSchema: QueryDraftSchema,
	});

	const dataQuality = await readDataQuality(draft.tables_touched);
	// Save-on-clean (P2a): grow the snippet library from this answer's fresh/
	// adapted steps. Fire-and-forget — the learning write runs AFTER the answer is
	// assembled and is never on the answer's critical path; persistLearnedSnippets
	// swallows its own errors, so it can neither block nor fail the answer.
	void persistLearnedSnippets(captured.value);
	return assembleAnswer(draft, captured.value, dataQuality);
}

export const answerTool = toolDefinition({
	name: "answer",
	description:
		"Answer a natural-language question about the workspace's imported data by " +
		"composing and validating grounded DuckDB SQL — reusing validated snippets " +
		"from the knowledge base where they fit. Returns the practitioner answer " +
		"with the headline figure, a grid handle whose full result streams in the " +
		"canvas, the assumptions made, the concepts and tables used, an " +
		"informational data-quality band for the tables touched (NOT a gate), and " +
		"the reused/adapted/fresh components. Read-only. Use for analytical " +
		"questions ('what is total revenue', 'monthly sales trend') once data has " +
		"been imported and typed.",
	inputSchema: z.object({
		question: z
			.string()
			.describe(
				"The natural-language question to answer over the workspace data.",
			),
	}),
	outputSchema: withAgentError(AnswerSchema),
}).server((input, ctx) =>
	asAgentError(() => querySubAgent(input.question, ctx?.abortSignal)),
);
