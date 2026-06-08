// The generic "frame-a-family" core (DAT-469).
//
// `frame` co-designs the user's whole model — concepts AND the executable
// knowledge over them (validations, later cycles + metrics, DAT-470/471). Each
// family follows the SAME two-path shape the concept path proved (DAT-382):
//   - induce: no edited set → one forced structured-output Anthropic call
//     proposes the family's set (seeded with structural few-shot from the
//     nearest shipped vertical).
//   - declare: an edited set → those are written verbatim, no LLM (the
//     accept/edit round-trip of the ModelFrame widget).
// Either way each member is persisted as a `config_overlay` row through the
// shared `teach` seam, and the written rows (+ overlay ids) are returned for the
// widget to render.
//
// This module factors that shape so adding a family (DAT-470/471) is supplying
// `{ teach type, item schema, induce fn, payload mapper }` — the induce-call
// scaffolding (`induceStructured`), the declare/write loop (`frameFamily`), and
// the library-as-seed helpers (`nearestSeedVertical` + `formatSeedExamples`) are
// shared. The concept path runs through `frameFamily` unchanged.

import { readdir } from "node:fs/promises";
import { join } from "node:path";
import { chat } from "@tanstack/ai";
import { createAnthropicChat } from "@tanstack/ai-anthropic";
import type { z } from "zod";

import { config } from "../config";
import { linkedAbortController } from "../lib/abort";
import { MAX_OUTPUT_TOKENS, MODEL } from "../llm";
import { teach } from "./teach";
import type { TeachType } from "./teach.validation";

/**
 * One forced structured-output Anthropic call: a cached system block
 * (`instructions`), the per-turn context (`userMessage`), and a Zod
 * `outputSchema` the model fills directly. Shared by every family's induce step
 * (concepts, validations, …) so the adapter / abort / max_tokens wiring lives in
 * one place. `signal` is the tool-context abort (DAT-449) — a stopped run aborts
 * the nested call instead of billing it to completion.
 */
export async function induceStructured<R>(opts: {
	instructions: string;
	userMessage: string;
	outputSchema: z.ZodType<R>;
	signal?: AbortSignal;
}): Promise<R> {
	// `chat()`'s structured-output overload infers the result from a CONCRETE
	// schema; a generic `z.ZodType<R>` widens it to unknown, so narrow back to R
	// (validation still runs server-side against `outputSchema`).
	const result = await chat({
		adapter: createAnthropicChat(MODEL, config.anthropicApiKey),
		abortController: linkedAbortController(opts.signal),
		modelOptions: { max_tokens: MAX_OUTPUT_TOKENS },
		systemPrompts: [opts.instructions],
		messages: [{ role: "user", content: opts.userMessage }],
		outputSchema: opts.outputSchema,
	});
	return result as R;
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
 * LLM (the accept/edit path; an empty array curates the family away). `edited`
 * absent → run `induce`. Each member is shaped to its overlay payload by
 * `toPayload` and written through the shared `teach` seam, vertical-tagged by the
 * payload (the engine's per-type applier filters on it).
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
	sessionId?: string | null;
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
			session_id: opts.sessionId ?? null,
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
