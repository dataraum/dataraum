// Server functions for the (app) pathless layout route.
//
// Peeled out of the route file into `*.functions.ts` (the TanStack Start idiom):
// a route is ISOMORPHIC, so server-only helpers imported at its top level ride
// into the CLIENT bundle. Here the helper is imported ONLY inside the
// `createServerFn` handler; the route imports the fn as an RPC stub and the
// helper never reaches the client.

import { createServerFn } from "@tanstack/react-start";
import { resolveActiveWorkspace } from "#/db/cockpit/registry";

// Active workspace id from the cockpit_db registry (DAT-461). The shell needs it
// so the rail's workspace links resolve on global routes like /settings, which
// carry no wsId param. Resolved server-side — the registry read never reaches
// the client bundle.
export const getActiveWorkspaceId = createServerFn({ method: "GET" }).handler(
	() => resolveActiveWorkspace(),
);
