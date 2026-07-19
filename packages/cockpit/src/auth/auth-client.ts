// better-auth React client (DAT-819) — the browser side of the auth surface.
// No baseURL: the client talks to /api/auth on ITS OWN origin (the portal for
// sign-in; a workspace cockpit for sign-out), which is where the handler is
// mounted in both roles. Keep this the ONLY client entry to better-auth —
// the provider-swap seam (Kinde long-term) stays one module wide.

import { createAuthClient } from "better-auth/react";

export const authClient = createAuthClient();
