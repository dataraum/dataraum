// Server fn: import the probe surface's import set (DAT-592) — persist each
// staged query as its own single-statement `db_recipe` source, then start ONE
// batched addSourceWorkflow run over the whole set.
//
// This is the UI analog of the agent `select` tool: the probe panel is direct
// manipulation (the user edits SQL and clicks, no LLM round-trip), so the import
// flows through a server fn the widget calls directly — exactly like the probe
// grid hits `/api/probe-sql`. It reuses the SAME persist seam (`persistRecipeSources`
// → `source-write`) and the SAME trigger (`triggerAddSource`) as `select`, so the
// run, its progress widget, and the grounding cascade are identical; only the
// producer (one source per query, vs select's bundled table-pick) differs.
//
// One run for the batch: N queries → N sources → a single `triggerAddSource` over
// the set, so the engine's run-scoped reduce + the grounding-teach loop run ONCE
// over the union (not N times), which is the whole point of staging a set before
// importing.

import { createServerFn } from "@tanstack/react-start";
import { z } from "zod";

// `persistRecipeSources` + `triggerAddSource` pull config-bearing modules
// (duckdb/probe, config) at load. They run ONLY server-side inside the handler, so
// they're imported there (dynamically) — NOT statically — to keep THIS module's
// graph config-free. The probe WIDGET imports this server fn at module scope; a
// static pull would drag config into every test that eagerly loads the widget
// registry (the canvas registry is deliberately config-free). `runWithConversation`
// is just AsyncLocalStorage (config-free), so it stays a normal import.
import { runWithConversation } from "#/lib/run-context";

const ImportSourcesInput = z.object({
	sources: z
		.array(
			z.object({
				source_name: z.string(),
				credential_source: z.string(),
				backend: z.string(),
				sql: z.string(),
			}),
		)
		.min(1),
	// The originating chat (route param). The agent `select` tool reads this from
	// the request ALS (set by /api/chat); a direct server fn has none, so the widget
	// passes it explicitly and we bind it around the trigger. Without it the run is
	// recorded with no conversation → the completion-watcher (which filters by
	// conversationId) never tracks it, so the inline progress never ticks and the
	// completion never narrates. (Null when the widget is off-route — degraded.)
	conversationId: z.string().nullish(),
});

/** The started run + the sources it imports. The widget renders the
 * `add-source-progress` widget INLINE from this (the canvas is message-derived, so
 * a direct action can't project a canvas member) — same carry as the `select`
 * tool result; the background completion-watcher narrates completion into the chat. */
export interface ImportSourcesResult {
	workflow_id: string;
	run_id: string;
	/** The minted/UPSERTed source ids the run ingests. */
	sources: string[];
	/** The source names imported — for the success message. */
	source_names: string[];
}

/**
 * Persist the import set as one source per query and start the batched import.
 *
 * Validation (bad name / reserved prefix / unsupported backend / empty SQL /
 * duplicate name / empty set) raises BEFORE any write — `persistRecipeSources`
 * checks the whole batch first, so a rejected import leaves no half-state. A
 * Temporal start failure (infra) propagates as a thrown error the mutation
 * surfaces; re-invoking recovers (the source upserts are idempotent).
 */
export const importSources = createServerFn({ method: "POST" })
	.inputValidator((input: z.infer<typeof ImportSourcesInput>) =>
		ImportSourcesInput.parse(input),
	)
	.handler(async ({ data }): Promise<ImportSourcesResult> => {
		const { persistRecipeSources } = await import("#/select/recipe-source");
		const { triggerAddSource } = await import("#/temporal/trigger-add-source");
		const persisted = await persistRecipeSources(data.sources);
		const sourceIds = persisted.map((p) => p.source_id);
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
			source_names: persisted.map((p) => p.source_name),
		};
	});
