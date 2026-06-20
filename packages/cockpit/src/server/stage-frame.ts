// Server fns for the probe staging hub's frame/vertical step (DAT-594).
//
// The staging hub assembles a heterogeneous import set (probed queries + files),
// then declares a business model over it — either FRAME a new vertical (induce
// concepts from the assembled set's schemas) or USE_VERTICAL to adopt a builtin.
// `frame` and `use_vertical` are agent-only tools, so the UI needs thin server-fn
// wrappers it can call DIRECTLY (no LLM round-trip) — the same direct-manipulation
// pattern as `importSources`.
//
// Schema assembly (`assembleStagingSchema`) sniffs each staged item — a query via
// `probeDescribe` (DESCRIBE + sample), a file via `sniffFileSchema` (the connect
// file sniff) — and unions them into ONE synthetic `ConnectSchema` to induce from.
//
// Server-only deps (duckdb, config, the tools) load INSIDE the handlers so this
// module's static graph stays config-free — the probe WIDGET imports these at
// module scope, and the canvas registry must not drag config (mirrors
// server/import-sources.ts and server/active-vertical.ts).

import { createServerFn } from "@tanstack/react-start";
import { z } from "zod";

import type { UseVerticalResult } from "#/tools/use-vertical";

// The staging set to seed induction from — the same query/file shapes the import
// uses, minus what frame doesn't need (file_uri carries the schema; a query's SQL
// + connection sniff its schema).
const StageFrameInput = z
	.object({
		queries: z
			.array(
				z.object({
					source_name: z.string(),
					credential_source: z.string(),
					backend: z.string(),
					// `.min(1)` closes the empty-SQL fragility: a blank query would reach
					// `probeDescribe` as `DESCRIBE SELECT * FROM ()` (a syntax error). The
					// modal caller always passes complete queries; this guards direct calls.
					sql: z.string().min(1),
				}),
			)
			.default([]),
		files: z.array(z.object({ file_uri: z.string() })).default([]),
		// The new vertical to declare the induced model under (the user can rename in
		// the modal). Omitted → `_adhoc` (the cold-start fallback), per frame's default.
		vertical_name: z.string().nullish(),
	})
	// Reject an empty staging set up front (mirrors ImportSourcesInput) — without
	// this, `assembleStagingSchema` throws inside the handler and surfaces as a 500
	// rather than a structured validation error.
	.refine((v) => v.queries.length + v.files.length > 0, {
		message: "The staging set is empty — stage a query or file before framing.",
	});

/** The frame outcome the staging modal needs — a SERIALIZABLE summary (counts +
 * the vertical it landed under), NOT the full `FrameResult` (whose validation
 * `parameters: Record<string, unknown>` trips the server-fn serialization check,
 * and which the modal doesn't render — the model-review widget is the agent path).
 * The modal only flips the Start gate on success and shows what was written. */
export interface FrameStagingResult {
	vertical: string;
	concept_count: number;
	validation_count: number;
	cycle_count: number;
	metric_count: number;
}

/**
 * Frame a NEW vertical from the assembled staging set: sniff each staged item's
 * schema, union them into one synthetic ConnectSchema, and run `frame` over it
 * (inducing concepts + the executable knowledge). Returns a serializable summary
 * for the modal. An acting call — frame writes overlay rows immediately (DAT-598
 * tracks a propose/commit split; here Start gates the IMPORT, not the frame).
 */
export const frameStagingSet = createServerFn({ method: "POST" })
	.inputValidator((input: z.infer<typeof StageFrameInput>) =>
		StageFrameInput.parse(input),
	)
	.handler(async ({ data }): Promise<FrameStagingResult> => {
		const { probeDescribe } = await import("#/duckdb/probe");
		const { sniffFileSchema } = await import("#/duckdb/connect");
		const { assembleStagingSchema } = await import("#/select/stage-schema");
		const { frame } = await import("#/tools/frame");

		// Sniff every staged item's schema (queries describe their projection; files
		// sniff their reader). Parallel — independent per item.
		const [queries, files] = await Promise.all([
			Promise.all(
				data.queries.map(async (q) => ({
					source_name: q.source_name,
					// probeDescribe samples at its DEFAULT_ROW_LIMIT (1000 rows) — sized for
					// induction context; a deliberate default, not an oversight.
					schema: await probeDescribe({
						source_name: q.credential_source,
						backend: q.backend,
						sql: q.sql,
					}),
				})),
			),
			Promise.all(data.files.map((f) => sniffFileSchema(f.file_uri))),
		]);

		const schema = assembleStagingSchema({ queries, files });
		const result = await frame({ schema, vertical_name: data.vertical_name });
		return {
			vertical: result.vertical,
			concept_count: result.concepts.length,
			validation_count: result.validations.length,
			cycle_count: result.cycles.length,
			metric_count: result.metrics.length,
		};
	});

/**
 * Adopt an existing (builtin or already-framed) vertical onto the workspace —
 * the UI wrapper over the `use_vertical` tool. No schema assembly: a builtin ships
 * its own concepts, so there's nothing to induce. (Named `adopt…` not `useVertical…`
 * so the React rules-of-hooks lint doesn't mistake the `use` prefix for a hook.)
 */
export const adoptVerticalForStaging = createServerFn({ method: "POST" })
	.inputValidator((input: { name: string }) =>
		z.object({ name: z.string() }).parse(input),
	)
	.handler(async ({ data }): Promise<UseVerticalResult> => {
		const { useVertical } = await import("#/tools/use-vertical");
		return useVertical(data.name);
	});

/** A vertical the staging modal can adopt — the subset of the tool's `Vertical`
 * shape the picker renders. */
export interface AdoptableVertical {
	name: string;
	kind: "builtin" | "framed";
	description: string | null;
	concept_count: number;
}

/**
 * The verticals the staging modal can adopt (builtins + already-framed) — the UI
 * wrapper over `list_verticals`. The modal offers these as the use_vertical path.
 */
export const listAdoptableVerticals = createServerFn({ method: "GET" }).handler(
	async (): Promise<AdoptableVertical[]> => {
		const { listVerticals } = await import("#/tools/list-verticals");
		const verticals = await listVerticals();
		return verticals.map((v) => ({
			name: v.name,
			kind: v.kind,
			description: v.description,
			concept_count: v.concept_count,
		}));
	},
);
