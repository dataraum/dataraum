import { createFileRoute } from "@tanstack/react-router";
import { createServerFn } from "@tanstack/react-start";
import { config } from "#/config";
import { resolveActiveWorkspace } from "#/db/cockpit/registry";
import { listRunsByWorkspace } from "#/db/cockpit/runs";
import { RunMonitor } from "#/ui/runs/run-monitor";

// The native run monitor (DAT-550) — replaces the external Temporal-UI iframe
// with a workspace-wide view of stage runs read from cockpit_db. The Temporal UI
// is kept as a deep-link for low-level debugging. Workspace resolved server-side
// (mirrors loadHistory), not from the route param. Bounded to the latest N.
const RUN_LIMIT = 100;

const loadRuns = createServerFn({ method: "GET" }).handler(async () => {
	const workspaceId = await resolveActiveWorkspace();
	return {
		runs: await listRunsByWorkspace(workspaceId, RUN_LIMIT),
		temporalUiUrl: config.temporalUiUrl,
		limit: RUN_LIMIT,
	};
});

export const Route = createFileRoute("/(app)/workspace/$wsId/workflows")({
	loader: () => loadRuns(),
	component: RunsSection,
});

function RunsSection() {
	const { runs, temporalUiUrl, limit } = Route.useLoaderData();
	return <RunMonitor runs={runs} limit={limit} temporalUiUrl={temporalUiUrl} />;
}
