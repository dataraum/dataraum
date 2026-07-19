// better-auth HTTP handler mount (DAT-819) — the splat catches every
// /api/auth/* endpoint (sign-in/sign-up/sign-out/get-session/...). Served by
// BOTH roles of the image: the portal is where sign-in happens; a workspace
// cockpit still needs sign-out + session reads on its own origin. The gate
// middleware (src/auth/gate.server.ts) allow-lists this prefix — auth must be
// reachable signed-out by definition; better-auth applies its own origin
// checks (trustedOrigins covers the parent-domain family).

import { createFileRoute } from "@tanstack/react-router";
import { auth } from "#/auth/auth";

export const Route = createFileRoute("/api/auth/$")({
	server: {
		handlers: {
			GET: ({ request }) => auth.handler(request),
			POST: ({ request }) => auth.handler(request),
		},
	},
});
