// Server fn: import the probe surface's STAGING SET (DAT-592, DAT-594) — a
// heterogeneous import set of probed SQL queries AND uploaded files, persisted in
// ONE pass and started as ONE batched addSourceWorkflow run over the whole union.
//
// This is the UI analog of the agent `select` tool: the probe panel is direct
// manipulation (the user edits SQL / uploads files and clicks, no LLM round-trip),
// so the import flows through a server fn the widget calls directly. It reuses the
// SAME persist seams — `persistRecipeSources` (one source per query) and
// `persistFileSources` (one content-keyed source per file, DAT-594) — and the SAME
// trigger (`triggerAddSource`) as `select`, so the run, its progress widget, and
// the grounding cascade are identical; only the producer (a staged set, vs select's
// per-tool grain) differs.
//
// One run for the batch: the staged files + queries → a set of sources → a single
// `triggerAddSource` over the union, so the engine's run-scoped reduce + the
// grounding-teach loop run ONCE over the whole set (not per item), which is the
// whole point of staging a set before importing. Mixed file+query in one run is
// supported with no engine change — the import activity dispatches per-row off
// `Source.source_type`.

import { createServerFn } from "@tanstack/react-start";
import { z } from "zod";
import { runWithConversation } from "#/lib/run-context";
// `persistRecipeSources` / `persistFileSources` + `triggerAddSource` pull
// config-bearing modules (duckdb/probe, config) at load. They run ONLY server-side
// inside the handler, so they're imported there (dynamically) — NOT statically — to
// keep THIS module's graph config-free. The probe WIDGET imports this server fn at
// module scope; a static pull would drag config into every test that eagerly loads
// the widget registry (the canvas registry is deliberately config-free).
// `runWithConversation` is just AsyncLocalStorage (config-free), so it stays a
// normal import. The persist-fn TYPES are erased at build, so importing them as
// `type` keeps the module graph config-free while the union helper stays typed.
import type { FileSourceSpec } from "#/select/file-source";
import type { RecipeSourceSpec } from "#/select/recipe-source";

// One staged query → one single-statement `db_recipe` source.
const QuerySpec = z.object({
	source_name: z.string(),
	credential_source: z.string(),
	backend: z.string(),
	sql: z.string(),
});

// One staged upload → one content-keyed `src_<digest>` file source (DAT-594).
const FileSpec = z.object({
	file_uri: z.string(),
});

const ImportSourcesInput = z
	.object({
		// The probed-query half of the set — each imports as its own db_recipe source.
		queries: z.array(QuerySpec).default([]),
		// The uploaded-file half of the set — each imports as its own content-keyed
		// source (DAT-594). Mixing files + queries in one set is supported.
		files: z.array(FileSpec).default([]),
		// The originating chat (route param). The agent `select` tool reads this from
		// the request ALS (set by /api/chat); a direct server fn has none, so the
		// widget passes it explicitly and we bind it around the trigger. Without it the
		// run is recorded with no conversation → the completion-watcher (which filters
		// by conversationId) never tracks it, so the inline progress never ticks and
		// the completion never narrates. (Null when the widget is off-route — degraded.)
		conversationId: z.string().nullish(),
	})
	.refine((v) => v.queries.length + v.files.length > 0, {
		message:
			"The import set is empty — stage a query or a file before importing.",
	});

/** The started run + the sources it imports. The widget renders the
 * `add-source-progress` widget INLINE from this (the canvas is message-derived, so
 * a direct action can't project a canvas member) — same carry as the `select`
 * tool result; the background completion-watcher narrates completion into the chat. */
export interface ImportSourcesResult {
	workflow_id: string;
	run_id: string;
	/** The minted/UPSERTed source ids the run ingests (queries ∪ files). */
	sources: string[];
	/** The source names imported — for the success message (query names + file
	 * content-keyed names). */
	source_names: string[];
}

/** The persist seams the union helper composes — injected so the union ordering +
 * id/name carry are unit-testable without a live Postgres or config (the server fn
 * wires the real dynamic imports). */
export interface ImportSetPersisters {
	persistRecipeSources: (
		specs: RecipeSourceSpec[],
	) => Promise<{ source_id: string; source_name: string }[]>;
	persistFileSources: (
		specs: FileSourceSpec[],
	) => Promise<{ source_id: string; source_name: string }[]>;
}

/** The persisted union of the import set: the source ids the run ingests + their
 * names for the success message. Queries first, then files (a query validation
 * failure rejects before any file is written). */
export interface ImportSet {
	sourceIds: string[];
	sourceNames: string[];
}

/**
 * Persist the heterogeneous import set (queries + files) and union the result.
 *
 * VALIDATE-ALL-UP-FRONT then persist: both producers validate the whole batch
 * BEFORE any write (bad name / reserved prefix / unsupported backend / empty SQL /
 * duplicate name / non-upload URI), so a rejected import leaves no half-state.
 * Queries are persisted first so a query validation failure rejects before any
 * file is written. Persistence ONLY — the caller triggers the batched import.
 */
export async function persistImportSet(
	data: { queries: RecipeSourceSpec[]; files: FileSourceSpec[] },
	persist: ImportSetPersisters,
): Promise<ImportSet> {
	const persistedQueries =
		data.queries.length > 0
			? await persist.persistRecipeSources(data.queries)
			: [];
	const persistedFiles =
		data.files.length > 0 ? await persist.persistFileSources(data.files) : [];

	return {
		sourceIds: [
			...persistedQueries.map((p) => p.source_id),
			...persistedFiles.map((p) => p.source_id),
		],
		sourceNames: [
			...persistedQueries.map((p) => p.source_name),
			...persistedFiles.map((p) => p.source_name),
		],
	};
}

/**
 * Persist the heterogeneous import set (queries + files) and start the batched
 * import over the union.
 *
 * A Temporal start failure (infra) propagates as a thrown error the mutation
 * surfaces; re-invoking recovers — every upsert is idempotent (digest / recipe_hash).
 */
export const importSources = createServerFn({ method: "POST" })
	.inputValidator((input: z.infer<typeof ImportSourcesInput>) =>
		ImportSourcesInput.parse(input),
	)
	.handler(async ({ data }): Promise<ImportSourcesResult> => {
		const { persistRecipeSources } = await import("#/select/recipe-source");
		const { persistFileSources } = await import("#/select/file-source");
		const { triggerAddSource } = await import("#/temporal/trigger-add-source");

		const { sourceIds, sourceNames } = await persistImportSet(
			{ queries: data.queries, files: data.files },
			{ persistRecipeSources, persistFileSources },
		);

		// Bind the conversation so the run is recorded against it (the watcher routes
		// progress + narration by conversationId). Off-route (no id) → bare trigger,
		// the null-conversation run the watcher simply doesn't narrate.
		const run = await (data.conversationId
			? runWithConversation(data.conversationId, () =>
					triggerAddSource({ sources: sourceIds }),
				)
			: triggerAddSource({ sources: sourceIds }));
		return {
			workflow_id: run.workflow_id,
			run_id: run.run_id,
			sources: sourceIds,
			source_names: sourceNames,
		};
	});
