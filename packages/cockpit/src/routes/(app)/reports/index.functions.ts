// Server function for the reports-gallery route (DAT-624).
//
// Peeled out of the route file into `*.functions.ts` (the TanStack Start idiom):
// the route is ISOMORPHIC, so the cockpit_db helpers would otherwise ride into the
// CLIENT bundle. Here they live ONLY inside the `createServerFn` handler; the route
// imports this as an RPC stub and the helpers never reach the client.

import { createServerFn } from "@tanstack/react-start";
import { resolveActiveWorkspace } from "#/db/cockpit/registry";
import { listReports } from "#/db/cockpit/reports";

export const loadReports = createServerFn({ method: "GET" }).handler(
	async () => {
		const workspaceId = await resolveActiveWorkspace();
		return listReports(workspaceId);
	},
);
