// Boot seam (DAT-819): kick the idempotent workspace-registry seed when a
// WORKSPACE cockpit starts. The seed used to be lazy-only (first
// resolveActiveWorkspace inside a request handler) — but the membership gate
// now fronts every request, and the gate itself does not seed, so on a fresh
// database nothing would ever create the dev user the portal login needs:
// gated request → 401 → no handler → no seed → no user → login impossible.
// Boot-seeding breaks that cycle deterministically at container start; the
// lazy path stays as the retry fallback (the seed memo resets on failure).
//
// Fire-and-forget like the worker plugin: Postgres may not be up yet in host
// dev — a failed boot seed logs and the next request-path resolve retries.
// Portal mode has no boot workspace and seeds nothing.

import { definePlugin } from "nitro";
import { baseConfig } from "#/config.base";

export default definePlugin(async () => {
	if (baseConfig.portalMode) {
		return;
	}
	const { resolveActiveWorkspaceRow } = await import("#/db/cockpit/registry");
	resolveActiveWorkspaceRow().catch((err) => {
		console.error("[registry-seed] boot seed failed (will retry lazily):", err);
	});
});
