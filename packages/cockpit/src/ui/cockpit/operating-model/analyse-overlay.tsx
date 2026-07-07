// The Model canvas's analyse surface (DAT-672, per-node re-cut DAT-702): run a
// metric/measure node in the SHARED result grid, with drill + chart on top.
//
// UX shape (decided in implementation, per the ticket's two options): the
// affordance lives in the NodeDetail panel — an "Analyse" button opening a
// large modal — rather than a second icon on the graph node. The 380px detail
// panel cannot hold a grid, and NodeDetail is already the node's "expand"
// surface, so the modal keeps one interaction path: click node → detail →
// analyse. Nothing here is canvas-local: the modal mounts the same
// DrillableGrid the answer surface inherits later (DAT-678).
//
// TWO TARGET KINDS (DAT-702):
//   - a MEASURE runs its persisted extract snippet directly (unchanged);
//   - a METRIC composes AD HOC on open — `/api/drill/node` rebuilds the node
//     from its persisted DAG parts, nothing pre-composed. The gate is the DAG
//     (`hasDag`), NOT the flattened snippet: a metric whose parts resolve is
//     analysable even when no flattened SQL was persisted, and a metric with
//     a hole gets the composer's NAMED refusal in the modal instead of a
//     silently missing button.
//
// Gating on measures = runnable SQL AND accepted (`grounded` — an unaccepted
// extract returns no rows, so executing it would render an empty grid as if
// it were data).

import { Alert, Button, Center, Group, Loader, Modal } from "@mantine/core";
import { useQuery } from "@tanstack/react-query";
import { Play } from "lucide-react";
import { useState } from "react";

import type { DrillAxesRequest } from "#/duckdb/drill";
import type { OMNode } from "#/tools/operating-model-graph";
import { DrillableGrid } from "#/ui/cockpit/widgets/drillable-grid";

/** The node's runnable analysis target, or null when there is nothing to run. */
export type AnalyseTarget =
	| { kind: "sql"; sql: string; axesRequest: DrillAxesRequest }
	| { kind: "metric-node"; metricKey: string; axesRequest: DrillAxesRequest };

export function analyseTarget(node: OMNode): AnalyseTarget | null {
	const d = node.data;
	// Node ids are namespaced (`metric:<graphId>` / `measure:<standardField>`) —
	// the suffix IS the resolver key.
	const key = node.id.slice(node.id.indexOf(":") + 1);
	if (d.kind === "metric" && d.hasDag) {
		return {
			kind: "metric-node",
			metricKey: key,
			axesRequest: { metricKey: key },
		};
	}
	if (d.kind === "measure" && d.sql && d.grounded) {
		return { kind: "sql", sql: d.sql, axesRequest: { standardField: key } };
	}
	return null;
}

/** `/api/drill/node` response, narrowed at the boundary (never trusted). */
type NodeComposeState =
	| { ok: true; sql: string }
	| { ok: false; reason: string };

function narrowNodeCompose(raw: unknown): NodeComposeState {
	if (typeof raw === "object" && raw !== null) {
		const r = raw as Record<string, unknown>;
		if (r.ok === true && typeof r.sql === "string") {
			return { ok: true, sql: r.sql };
		}
		if (r.ok === false && typeof r.reason === "string") {
			return { ok: false, reason: r.reason };
		}
	}
	return { ok: false, reason: "unexpected compose response" };
}

/** Compose-on-open for a metric node: fetch the ad-hoc composed SQL, then
 *  mount the shared grid on it. Loading/refusal states are part of the
 *  surface — a refusal names the missing part, never a dead end. */
function MetricNodeGrid({
	metricKey,
	axesRequest,
}: {
	metricKey: string;
	axesRequest: DrillAxesRequest;
}) {
	const compose = useQuery({
		queryKey: ["drill-node", metricKey],
		queryFn: async (): Promise<NodeComposeState> => {
			const res = await fetch("/api/drill/node", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({ metricKey }),
			});
			if (!res.ok) throw new Error(`compose failed (${res.status})`);
			return narrowNodeCompose(await res.json());
		},
	});

	if (compose.isPending) {
		return (
			<Center h={160} data-testid="node-analyse-composing">
				<Loader size="sm" />
			</Center>
		);
	}
	if (compose.isError) {
		return (
			<Alert color="red" title="Compose failed">
				{compose.error instanceof Error
					? compose.error.message
					: "unknown error"}
			</Alert>
		);
	}
	if (!compose.data.ok) {
		return (
			<Alert color="yellow" title="Cannot compose this node">
				{compose.data.reason}
			</Alert>
		);
	}
	return <DrillableGrid sql={compose.data.sql} axesRequest={axesRequest} />;
}

/** The "Analyse" button + modal for a metric/measure node; renders nothing for
 *  nodes without a runnable target (no affordance on ungrounded measures or
 *  DAG-less metrics — AC). */
export function AnalyseAction({ node }: { node: OMNode }) {
	const [opened, setOpened] = useState(false);
	const target = analyseTarget(node);
	if (!target) return null;
	// The Group keeps the button intrinsic-width inside NodeDetail's stretching
	// Stack; the Modal portals out, so it is the Group's only real child.
	return (
		<Group>
			<Button
				variant="light"
				size="compact-sm"
				leftSection={<Play size={13} />}
				onClick={() => setOpened(true)}
				data-testid="node-analyse-button"
			>
				Analyse
			</Button>
			<Modal
				opened={opened}
				onClose={() => setOpened(false)}
				title={node.label}
				size="90%"
				data-testid="node-analyse-modal"
			>
				{/* Mounted only while open (Mantine default) — the compose fetch, the
				    grid query, and the axes fetch fire on first open, not on node
				    selection. */}
				{target.kind === "sql" ? (
					<DrillableGrid sql={target.sql} axesRequest={target.axesRequest} />
				) : (
					<MetricNodeGrid
						metricKey={target.metricKey}
						axesRequest={target.axesRequest}
					/>
				)}
			</Modal>
		</Group>
	);
}
