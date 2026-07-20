// The generic "frame-a-family" core (DAT-469).
//
// `frame` co-designs the user's whole model — concepts AND the executable
// knowledge over them (validations, later cycles + metrics, DAT-470/471). Each
// family follows the SAME two-path shape the concept path proved (DAT-382):
//   - induce: no edited set → one NATIVE structured-output Anthropic call
//     (`induceNative`) proposes the family's set, seeded with structural
//     few-shot from the nearest shipped vertical. All four families now go
//     through it (DAT-807): validations and metrics were the last holdouts —
//     their payloads carry open maps constrained decoding cannot express, which
//     is now solved by giving those two families a separate ARRAY-shaped
//     LLM-facing schema (`validation-induction.ts` / `metric-induction.ts`) and
//     converting to the payload shape at the induce boundary, rather than by
//     keeping a forced-tool call.
//   - declare: an edited set → those are written verbatim, no LLM (the
//     accept/edit round-trip of the ModelFrame widget).
// Either way each member is persisted as a `config_overlay` row through the
// shared `teach` seam, and the written rows (+ overlay ids) are returned for the
// widget to render.
//
// This module factors that shape so adding a family (DAT-470/471) is supplying
// `{ teach type, item schema, induce fn, payload mapper }` — the induce-call
// scaffolding (`induceNative`), the declare/write loop
// (`frameFamily`), and the library-as-seed helpers (`nearestSeedVertical` +
// `formatSeedExamples`) are shared. The concept path runs through `frameFamily` unchanged.

import { readdir } from "node:fs/promises";
import { join } from "node:path";
import { chat } from "@tanstack/ai";
import { createAnthropicChat } from "@tanstack/ai-anthropic";
import type { z } from "zod";

import { config } from "../config";
import { linkedAbortController } from "../lib/abort";
import { llmOtel } from "../lib/llm-otel";
import { MAX_OUTPUT_TOKENS, MODEL } from "../llm";
import { teach } from "./teach";
import type { TeachType } from "./teach.validation";

/**
 * One induction call that returns a schema-GUARANTEED typed object, via
 * Anthropic native structured output (`output_config.format`, DAT-807): the
 * adapter attaches the JSON Schema to a single streaming request and the model's
 * final-turn text is constrained to it, so there is no tool-argument boundary to
 * malform and no client-side parse to fail. `chat({ outputSchema })` resolves to
 * the validated value or throws.
 *
 * Budget: MAX_OUTPUT_TOKENS (not STRUCTURED_OUTPUT_MAX_TOKENS) is correct here —
 * that lower ceiling exists only for models OUTSIDE the adapter's combined set,
 * which fall back to a non-streaming forced-tool call the Anthropic SDK refuses
 * above 21,333 tokens. `MODEL` is inside the set (pinned by llm.contract.test.ts),
 * so this is one streaming request and the induction keeps the full budget a
 * large concept/cycle set needs. Same reasoning as the grounding agent's
 * combined call (worker/grounding-agent.ts).
 *
 * `signal` is the tool-context abort (DAT-449) — a stopped run aborts the
 * nested call instead of billing it to completion.
 */
export async function induceNative<R>(opts: {
	instructions: string;
	userMessage: string;
	outputSchema: z.ZodType<R>;
	signal?: AbortSignal;
}): Promise<R> {
	// A generic `z.ZodType<R>` widens chat()'s inferred return to `unknown`; the
	// value is parsed against `outputSchema` inside chat() before it resolves, so
	// narrow to R (same widening the forced-tool path hits on its tool input).
	return (await chat({
		adapter: createAnthropicChat(MODEL, config.anthropicApiKey),
		// No toolArgsGuardMiddleware: there are no tools, so there is no
		// tool-argument boundary to guard (see lib/tool-args-guard.ts header).
		middleware: llmOtel("frame_family"),
		abortController: linkedAbortController(opts.signal),
		modelOptions: {
			max_tokens: MAX_OUTPUT_TOKENS,
			// Disable thinking: one-shot structured extraction, not agentic
			// reasoning. Sonnet 5 defaults adaptive thinking ON, which would bill a
			// thinking trace before every emit (×4 per frame) with no quality gain.
			// The agentic loops (agent-turn, query) keep thinking.
			thinking: { type: "disabled" },
		},
		systemPrompts: [opts.instructions],
		messages: [{ role: "user", content: opts.userMessage }],
		outputSchema: opts.outputSchema,
	})) as R;
}

/** A written family member + the overlay row id it landed as. */
export type Written<T> = T & { overlay_id: string };

export interface FrameFamilyResult<T> {
	// The resolved set (induced or declared) — what later families induce over.
	items: T[];
	// The same set, each tagged with the overlay id its `teach` write produced.
	written: Written<T>[];
}

/**
 * Resolve a family's set then write each member as a `config_overlay` row.
 *
 * `edited` present (incl. an explicit empty array) → declare those verbatim, no
 * LLM (the accept/edit path; an empty array writes nothing — curating the family
 * away, where the family permits it: `frame()` rejects an empty CONCEPT set
 * downstream, since a model needs a vocabulary). `edited` absent → run `induce`.
 * Each member is shaped to its overlay payload by `toPayload` and written through
 * the shared `teach` seam, vertical-tagged by the payload (the engine's per-type
 * applier filters on it).
 */
export async function frameFamily<T>(opts: {
	teachType: TeachType;
	// Per-member schema — parses an edited set on the declare path.
	itemSchema: z.ZodType<T>;
	// Family-specific induction (its own prompt + output schema). Called only on
	// the induce path.
	induce: (signal?: AbortSignal) => Promise<T[]>;
	// Map a resolved member → its vertical-tagged `teach` payload.
	toPayload: (item: T) => Record<string, unknown>;
	edited?: T[];
	signal?: AbortSignal;
}): Promise<FrameFamilyResult<T>> {
	const items =
		opts.edited !== undefined
			? opts.edited.map((i) => opts.itemSchema.parse(i))
			: await opts.induce(opts.signal);

	const written: Written<T>[] = [];
	for (const item of items) {
		const { overlay_id } = await teach({
			type: opts.teachType,
			payload: opts.toPayload(item),
		});
		written.push({ ...item, overlay_id });
	}
	return { items, written };
}

/** Drop undefined-valued keys so an overlay payload mirrors the engine's
 * `model_dump(exclude_none=True)` — no null spray into the JSONB row. Shared by
 * every family's `toPayload`. */
export function stripUndefined(
	obj: Record<string, unknown>,
): Record<string, unknown> {
	const out: Record<string, unknown> = {};
	for (const [k, v] of Object.entries(obj)) {
		if (v !== undefined) out[k] = v;
	}
	return out;
}

/** The shipped (builtin) vertical directory names under `<config>/verticals/`,
 * excluding underscore-prefixed internal seeds (`_adhoc`). A thin dir scan — the
 * seed resolver reads each candidate's family specs to pick the richest. An
 * unreadable config tree (not mounted) yields []. */
export async function shippedVerticalNames(): Promise<string[]> {
	try {
		const entries = await readdir(
			join(config.dataraumConfigPath, "verticals"),
			{
				withFileTypes: true,
				encoding: "utf8",
			},
		);
		return entries
			.filter((e) => e.isDirectory() && !e.name.startsWith("_"))
			.map((e) => e.name);
	} catch {
		return [];
	}
}

/**
 * Resolve the "nearest shipped vertical" to draw structural few-shot from
 * (DAT-468). The framed vertical's OWN shipped specs when it ships any (refining
 * on top of a shipped vertical — those specs double as the shadow targets the
 * per-item `teach_*` tools detect); otherwise the richest OTHER shipped builtin,
 * as a structural reference (today: finance). NOT semantic-nearest — there is no
 * similarity signal at frame time, so "nearest" means the richest available
 * reference library. Empty `specs` when nothing ships this family.
 */
export async function nearestSeedVertical<T>(
	framedVertical: string,
	readSeed: (vertical: string) => Promise<T[]>,
	// The shipped-vertical enumerator is injectable so the fallback is unit-testable
	// without the config tree; production uses the default fs scan.
	listShipped: () => Promise<string[]> = shippedVerticalNames,
): Promise<{ vertical: string; specs: T[] }> {
	const own = await readSeed(framedVertical);
	if (own.length > 0) return { vertical: framedVertical, specs: own };

	let best: { vertical: string; specs: T[] } = { vertical: "", specs: [] };
	for (const name of await listShipped()) {
		if (name === framedVertical) continue;
		const specs = await readSeed(name);
		if (specs.length > best.specs.length) best = { vertical: name, specs };
	}
	return best;
}

/**
 * Format shipped specs into the induce prompt's user message as STRUCTURAL
 * few-shot — flagged explicitly as examples to learn the SHAPE from, never
 * content to copy (the framing DAT-468 calls out as what makes structural
 * induction reliable). Generic over the family (`validation`, later `cycle` /
 * `metric`). An empty seed yields a one-line note so the induce step proceeds
 * from the schema + concepts alone.
 */
export function formatSeedExamples(
	specs: readonly unknown[],
	opts: { vertical: string; family: string },
): string {
	const tag = `${opts.family}_examples`;
	if (specs.length === 0) {
		return (
			`<${tag}>\n` +
			`(No shipped ${opts.family} library to draw structural examples from — ` +
			`propose from the schema + concepts directly.)\n` +
			`</${tag}>`
		);
	}
	return (
		`<${tag} vertical="${opts.vertical}">\n` +
		`These are EXAMPLE ${opts.family} specs shipped for a related vertical, shown ` +
		`to illustrate the STRUCTURE — the field shape and the kind of rule — NOT ` +
		`content to copy. Do NOT reuse their ids, names, or parameters verbatim: ` +
		`induce ${opts.family}s that fit THIS source's concepts and schema, using ` +
		`these only as a structural template.\n\n` +
		`${JSON.stringify(specs, null, 2)}\n` +
		`</${tag}>`
	);
}
