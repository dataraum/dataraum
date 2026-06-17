import { QueryClient } from "@tanstack/react-query";
import { createRouter as createTanStackRouter } from "@tanstack/react-router";
import { setupRouterSsrQueryIntegration } from "@tanstack/react-router-ssr-query";

import { routeTree } from "./routeTree.gen";

export function getRouter() {
	const queryClient = new QueryClient({
		defaultOptions: {
			queries: {
				// Single-user dev: re-fetch on focus is mostly noise. Re-enable
				// when the cockpit grows live-collaborative widgets.
				refetchOnWindowFocus: false,
			},
		},
	});

	const router = createTanStackRouter({
		routeTree,
		context: { queryClient },
		scrollRestoration: true,
		defaultPreload: "intent",
		// 0 is DELIBERATE, not a leftover: TanStack Query owns data caching (via
		// setupRouterSsrQueryIntegration below). If the router ALSO cached preloaded
		// loader data, the two SWR layers would drift — so we disable the router's
		// preload cache and let Query's per-query staleTime be the single authority.
		// Intent-preload still warms Query's cache; it just doesn't add a second one.
		// (Official router+query integration guidance — router-core data-loading skill.)
		defaultPreloadStaleTime: 0,
	});

	// Wires QueryClientProvider around the router and lets route loaders
	// call queryClient.ensureQueryData(...) for SSR-friendly data fetching.
	setupRouterSsrQueryIntegration({ router, queryClient });

	return router;
}

declare module "@tanstack/react-router" {
	interface Register {
		router: ReturnType<typeof getRouter>;
	}
}

// Typed router history state. The cockpit carries a one-shot `seed` in router
// state — the landing "tell" entry and the Needs-you "Resolve in Stage" deep-link
// (DAT-553) both navigate with it, and the chat route reads it once on mount.
// Augmenting HistoryState (the same way react-router augments it for its own
// internal keys) types `navigate({ state: { seed } })` AND `useLocation().state.seed`
// end-to-end — replacing the `as never` write casts + the read-side cast that
// erased that safety (a rename of `seed` is now a compile error, not a silent
// runtime `undefined`).
declare module "@tanstack/history" {
	interface HistoryState {
		/** A message to send once into a freshly-opened chat on mount. */
		seed?: string;
	}
}
