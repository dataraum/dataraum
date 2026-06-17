// Rail liveness badge (DAT-550) — a small processing dot on the Runs rail icon
// when the workspace has in-flight runs. Fed by a light cockpit_db count polled
// on an interval, so it reflects work the orchestration worker is doing with NO
// browser chat stream open (tab-independence). Polls the /api/running-runs route
// rather than importing the server module, so the cockpit_db client + config
// never enter the client bundle (same pattern as the progress widgets).

import { Indicator } from "@mantine/core";
import { useQuery } from "@tanstack/react-query";
import type { ReactNode } from "react";

// A PERSISTENT liveness poll — deliberately NOT the one-shot "refetchInterval
// returns false when done" progress pattern (cockpit React rule 3): the badge
// reflects an ever-changing "anything running?" signal, so it polls steadily.
const POLL_MS = 5000;

async function fetchRunningCount(): Promise<number> {
	const res = await fetch("/api/running-runs");
	if (!res.ok) return 0;
	const data = (await res.json()) as { count?: number };
	return data.count ?? 0;
}

export function RunLivenessBadge({ children }: { children: ReactNode }) {
	const { data } = useQuery({
		// TODO(DAT-357): when multi-workspace switching lands, scope this key (and
		// /api/running-runs) by workspace id — today the endpoint resolves the
		// single active workspace server-side, so the bare key is correct.
		queryKey: ["workspace-running-runs"],
		queryFn: fetchRunningCount,
		refetchInterval: POLL_MS,
		// The badge mounts in the always-rendered shell, so the query would
		// otherwise run during SSR — where a relative fetch has no host. Gate to
		// the client; the badge renders inactive on the server, then polls once
		// hydrated (the project's `typeof window` SSR-guard idiom).
		enabled: typeof window !== "undefined",
	});
	const running = (data ?? 0) > 0;
	return (
		<Indicator
			disabled={!running}
			processing
			color="blue"
			size={9}
			offset={3}
			data-testid="run-liveness"
			data-running={running ? "true" : "false"}
		>
			{children}
		</Indicator>
	);
}
