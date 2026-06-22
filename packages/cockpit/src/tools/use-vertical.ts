// adopt-vertical helper (DAT-523) — adopt an EXISTING vertical onto the workspace.
// The agent `use_vertical` TOOL was removed by DAT-597 (acquisition moved to the
// staging hub); this is now the shared adopt-vertical helper `server/stage-frame.ts`
// (adoptVerticalForStaging) calls directly.
//
// The acting counterpart to `list_verticals`: the agent calls this to ADOPT a
// builtin (e.g. finance) or an already-framed vertical onto the workspace,
// writing `workspaces.vertical` (cockpit_db) so the next workflow's `verticals[]`
// manifest resolves against it. This is the BUILTIN half of "the workspace
// acquires its vertical" — `frame` declares a NEW vertical's model (inducing
// concepts), `use_vertical` picks one that already exists (a builtin ships its
// own concepts on disk, so there is nothing to induce or re-declare).
//
// Post-DAT-506 the vertical is a WORKSPACE property, not a `select` input, so
// this explicit acting step is the ONLY surface that lands a builtin on the
// workspace — without it, adopting finance would still need a hand-seeded
// registry row (the gap the DAT-506 smoke worked around).
//
// Born-loud (DAT-479): an unknown name is rejected — only a vertical
// `list_verticals` would return (a builtin directory or a framed overlay) is
// adoptable, so a typo can't silently pin a non-resolving vertical that
// add_source then fails on with a misleading message.

import { z } from "zod";

import { setActiveWorkspaceVertical } from "#/db/cockpit/registry";
import { AgentActionableError } from "./agent-error";
import { listVerticals } from "./list-verticals";

export const UseVerticalResult = z.object({
	// The adopted vertical's name — now the workspace's vertical.
	vertical: z.string(),
	// Where it came from: a shipped builtin or one framed in this workspace.
	kind: z.enum(["builtin", "framed"]),
});
export type UseVerticalResult = z.infer<typeof UseVerticalResult>;

/**
 * Adopt an existing vertical onto the active workspace: validate the name
 * resolves to a real vertical (builtin or framed, per `list_verticals`), then
 * persist it to `workspaces.vertical` so the next run's manifest carries it.
 * `list` is injected for testability; the default is the real `listVerticals`.
 */
export async function useVertical(
	name: string,
	list: typeof listVerticals = listVerticals,
): Promise<UseVerticalResult> {
	const available = await list();
	const match = available.find((v) => v.name === name);
	if (!match) {
		throw new AgentActionableError(
			`Vertical '${name}' is not available to adopt. Call list_verticals to see ` +
				"the adoptable verticals (builtins + already-framed ones), or frame a new " +
				"one if none fits.",
		);
	}
	// Born-loud at the adopt boundary (DAT-479): a vertical with no concepts would
	// ground against nothing — add_source's semantic phase fails deep instead of
	// here. `concept_count` is an UPPER BOUND (overlay overrides double-count), so
	// `=== 0` can only be a true zero — safe to reject on.
	if (match.concept_count === 0) {
		throw new AgentActionableError(
			`Vertical '${name}' has no concepts to ground against — adopting it would ` +
				"fail the import. Frame concepts for it first, or pick a vertical that " +
				"ships them (e.g. a builtin like finance).",
		);
	}
	await setActiveWorkspaceVertical(name);
	return { vertical: name, kind: match.kind };
}
