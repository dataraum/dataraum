// Server function for the `/` redirect route. Peeled out of the isomorphic route
// file so the cockpit_db registry read (and the server-only config it seeds from)
// never rides into the client bundle — the static import is an RPC stub there.
// See routes/(app)/.../$conversationId.functions.ts for the full rationale.

import { createServerFn } from "@tanstack/react-start";
import { resolveActiveWorkspace } from "#/db/cockpit/registry";

// `/` resolves the active workspace and sends the user straight to its
// cockpit. The id comes from the cockpit_db workspace registry (DAT-461),
// seeded from DATARAUM_WORKSPACE_ID; once multi-workspace lands (DAT-357) this
// becomes a real picker. Resolved server-side — the registry read (and the
// server-only config it seeds from) never reaches the client bundle.
export const getActiveWorkspaceId = createServerFn({ method: "GET" }).handler(
	() => resolveActiveWorkspace(),
);
