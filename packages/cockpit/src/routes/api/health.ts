// Liveness probe (DAT-819) — the compose healthcheck target for both roles.
// Public by design (the membership gate allow-lists it): a healthcheck has no
// session, and `/` now 401s signed-out requests, so probing `/` would mark a
// perfectly healthy cockpit unhealthy and wedge `up --wait`. Liveness only —
// no DB touch, no substrate checks (the process serving HTTP IS the signal,
// matching what the old `/` probe actually proved).

import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/api/health")({
	server: {
		handlers: {
			GET: () => Response.json({ ok: true }),
		},
	},
});
