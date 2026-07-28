// Server function for the operating-model route (DAT-591).
//
// Peeled out of the route file into `*.functions.ts` (the TanStack Start idiom):
// the route is ISOMORPHIC, so the metadata Drizzle read would otherwise ride into
// the CLIENT bundle. Here it lives ONLY inside the `createServerFn` handler; the
// route imports this as an RPC stub and the helper never reaches the client.

import { createServerFn } from "@tanstack/react-start";
import { loadConceptGraph } from "#/tools/concept-graph-load";
import { loadOperatingModelGraph } from "#/tools/operating-model-load";

export const loadModel = createServerFn({ method: "GET" }).handler(() =>
	loadOperatingModelGraph(),
);

// The concept vocabulary graph (DAT-737) — a SEPARATE server fn from
// `loadModel`: concepts are seeded at `frame`-time (config→DB, DAT-728),
// independent of whether the operating_model stage has run, so it has its
// own lifecycle and its own empty state (see `operating-model.tsx`).
export const loadConcepts = createServerFn({ method: "GET" }).handler(() =>
	loadConceptGraph(),
);
