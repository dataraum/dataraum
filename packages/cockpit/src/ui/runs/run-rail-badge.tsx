// Rail badge on the Runs icon (DAT-550 liveness + DAT-553 "Needs you"). Two
// tab-independent cockpit_db polls feed a single Indicator with a PRIORITY:
//
//   1. Needs you (N) > 0  → a yellow NUMBERED badge — the actionable signal: the
//      grounding loop parked runs awaiting a human judgement (DAT-553). Takes
//      visual priority because it's a call to action, not just activity.
//   2. else running > 0   → a blue PROCESSING dot — work is in flight (DAT-550).
//   3. else               → nothing.
//
// Both counts poll the thin /api/* routes rather than importing the server module,
// so the cockpit_db client + config never enter the client bundle (same pattern as
// the progress widgets). The badge lives in the always-rendered shell.

import { Indicator } from "@mantine/core";
import { useQuery } from "@tanstack/react-query";
import type { ReactNode } from "react";

// A PERSISTENT liveness poll — deliberately NOT the one-shot "refetchInterval
// returns false when done" progress pattern (cockpit React rule 3): the badge
// reflects ever-changing "anything running / anything for me?" signals, so it
// polls steadily.
const POLL_MS = 5000;

async function fetchCount(path: string): Promise<number> {
	const res = await fetch(path);
	if (!res.ok) return 0;
	const data = (await res.json()) as { count?: number };
	return data.count ?? 0;
}

export function RunRailBadge({ children }: { children: ReactNode }) {
	// The badge mounts in the always-rendered shell, so the queries would otherwise
	// run during SSR — where a relative fetch has no host. Gate to the client; the
	// badge renders inactive on the server, then polls once hydrated (the project's
	// `typeof window` SSR-guard idiom).
	// TODO(DAT-357): when multi-workspace switching lands, scope these keys (and the
	// endpoints) by workspace id — today the endpoints resolve the single active
	// workspace server-side, so the bare keys are correct.
	const enabled = typeof window !== "undefined";
	const { data: runningData } = useQuery({
		queryKey: ["workspace-running-runs"],
		queryFn: () => fetchCount("/api/running-runs"),
		refetchInterval: POLL_MS,
		enabled,
	});
	const { data: awaitingData } = useQuery({
		queryKey: ["workspace-awaiting-input"],
		queryFn: () => fetchCount("/api/awaiting-input"),
		refetchInterval: POLL_MS,
		enabled,
	});
	const running = (runningData ?? 0) > 0;
	const needsYou = awaitingData ?? 0;

	// Needs-you (actionable) wins over the running dot. Both data-* attrs are always
	// set so the rail/tests can read either signal regardless of which one renders.
	if (needsYou > 0) {
		return (
			<Indicator
				label={needsYou}
				color="yellow"
				size={16}
				offset={3}
				data-testid="run-liveness"
				data-needs-you="true"
				data-running={running ? "true" : "false"}
			>
				{children}
			</Indicator>
		);
	}
	return (
		<Indicator
			disabled={!running}
			processing
			color="blue"
			size={9}
			offset={3}
			data-testid="run-liveness"
			data-needs-you="false"
			data-running={running ? "true" : "false"}
		>
			{children}
		</Indicator>
	);
}
