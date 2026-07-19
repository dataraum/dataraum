// Server function for the operating-model route (DAT-591).
//
// Peeled out of the route file into `*.functions.ts` (the TanStack Start idiom):
// the route is ISOMORPHIC, so the metadata Drizzle read would otherwise ride into
// the CLIENT bundle. Here it lives ONLY inside the `createServerFn` handler; the
// route imports this as an RPC stub and the helper never reaches the client.

import { createServerFn } from "@tanstack/react-start";
import { loadOperatingModelGraph } from "#/tools/operating-model-load";

export const loadModel = createServerFn({ method: "GET" }).handler(() =>
	loadOperatingModelGraph(),
);
