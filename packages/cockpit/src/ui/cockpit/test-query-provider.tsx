// Test-only wrapper for trees that mount CockpitProvider — which reads
// useQueryClient for the Phase 2A.3 live-progress cache. A fresh QueryClient per
// mount (retries off so a failed seed fetch doesn't linger across assertions).
// NOT a .test file, so vitest won't run it as a suite.

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { type ReactNode, useState } from "react";

export function TestQueryProvider({ children }: { children: ReactNode }) {
	const [client] = useState(
		() => new QueryClient({ defaultOptions: { queries: { retry: false } } }),
	);
	return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}
