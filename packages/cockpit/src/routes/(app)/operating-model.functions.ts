// Server function for the operating-model route (DAT-591).
//
// Peeled out of the route file into `*.functions.ts` (the TanStack Start idiom):
// the route is ISOMORPHIC, so the metadata Drizzle read would otherwise ride into
// the CLIENT bundle. Here it lives ONLY inside the `createServerFn` handler; the
// route imports this as an RPC stub and the helper never reaches the client.

import { createServerFn } from "@tanstack/react-start";
import { loadBusMatrix } from "#/tools/bus-matrix-load";
import { loadConceptGraph } from "#/tools/concept-graph-load";
import { loadCoverageMap } from "#/tools/coverage-map-load";
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

// The bus matrix (DAT-740) — which facts share which conformed dimensions, i.e.
// which cross-fact comparisons can be composed at all. A THIRD server fn for the
// same reason `loadConcepts` is a second: it is derived at catalog time by the
// dimension-hierarchies phase, so it is present (or absent) independently of the
// operating_model stage and carries its own empty state.
export const loadBus = createServerFn({ method: "GET" }).handler(() =>
	loadBusMatrix(),
);

// The coverage map (DAT-855 B2) — a FOURTH independent server fn, and a fourth
// independent lifecycle: it reads the `metrics`/`concepts` VOCABULARY (frame-time,
// present with or without a run — same as `loadConcepts`) fused with the promoted
// operating_model run's lifecycle/grounding/reconciliation state (present only once
// that run exists — same as `loadModel`). Neither `loadModel`, `loadConcepts`, nor
// `loadBus` reads that fusion, so folding coverage into any of them would make ITS
// failure blank a pane that has nothing to do with it — the exact fault-isolation
// concern this route's header documents for the first three.
export const loadCoverage = createServerFn({ method: "GET" }).handler(() =>
	loadCoverageMap(),
);
