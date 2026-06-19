// Server fn: is the active workspace FRAMED? (DAT-592 follow-up)
//
// Importing a source runs `semantic_per_column` grounding, which fails loud when
// the workspace's vertical has no concepts ("Run frame before add_source"). The
// probe import set is a deterministic button that bypasses the agent's frame step,
// so it must pre-check: don't let the user stage/import sources into an unframed
// workspace, only to have the run die deep in the pipeline.
//
// `framed` reuses the SAME concept count the engine's grounding guard keys off
// (`verticalConceptCount` — builtin ontology + active config_overlay concept rows),
// so the UI gate and the engine's fail-loud agree. A selected builtin vertical
// (shipped concepts) and a framed `_adhoc` both read framed=true; a cold-start
// `_adhoc` reads false.
//
// Server-only deps load INSIDE the handler so this module's static graph stays
// config-free — the probe WIDGET imports it at module scope, and the canvas
// registry must not drag config (mirrors server/import-sources.ts).

import { createServerFn } from "@tanstack/react-start";

export interface ActiveVerticalStatus {
	/** The active workspace's vertical (`_adhoc` when cold-start / unframed). */
	vertical: string;
	/** True when that vertical resolves to ≥1 concept — the import set's gate. */
	framed: boolean;
}

export const getActiveVerticalStatus = createServerFn({
	method: "GET",
}).handler(async (): Promise<ActiveVerticalStatus> => {
	const { resolveActiveWorkspaceRow } = await import("#/db/cockpit/registry");
	const { verticalConceptCount } = await import("#/tools/list-verticals");
	const ws = await resolveActiveWorkspaceRow();
	const count = await verticalConceptCount(ws.vertical);
	return { vertical: ws.vertical, framed: count > 0 };
});
