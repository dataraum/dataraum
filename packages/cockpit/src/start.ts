// TanStack Start instance (DAT-819) — global request middleware.
//
// Two members, order matters:
//   1. the framework CSRF middleware for server-fn RPCs (declaring our own
//      requestMiddleware REPLACES the built-in default, so it must be
//      re-added explicitly — start-server-core warns otherwise),
//   2. the workspace membership gate: on a per-workspace cockpit every
//      server request (SSR document, /api/* route, server-fn RPC) requires a
//      better-auth session WITH membership of the boot workspace. This is
//      the data security boundary — route-level guards are only UX on top.
//
// The gate's logic lives in src/auth/gate.server.ts and is imported INSIDE
// the .server() closure: this file is isomorphic (the client bundle carries
// the instance shell), and the dynamic import keeps the server-only auth/db
// modules out of the client graph (cockpit isomorphic-file convention).

import { createCsrfMiddleware, createMiddleware, createStart } from "@tanstack/react-start";

const csrfMiddleware = createCsrfMiddleware({
	filter: (ctx) => ctx.handlerType === "serverFn",
});

const membershipGate = createMiddleware().server(async ({ next, request }) => {
	const { baseConfig } = await import("#/config.base");
	// The portal role is ungated here: its two surfaces are the login screen
	// (public by definition) and the membership list (guards itself on the
	// session inside the server fn).
	if (!baseConfig.portalMode) {
		const { gateRequest } = await import("#/auth/gate.server");
		const rejection = await gateRequest(request);
		if (rejection) {
			throw rejection;
		}
	}
	return next();
});

export const startInstance = createStart(() => ({
	requestMiddleware: [csrfMiddleware, membershipGate],
}));
