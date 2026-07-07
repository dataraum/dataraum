// The Model canvas's analyse surface (DAT-672, per-node re-cut DAT-702/703):
// run a metric/measure node in the SHARED result grid, with drill + chart on
// top.
//
// UX shape (decided in implementation, per the ticket's two options): the
// affordance lives in the NodeDetail panel — an "Analyse" button opening a
// large modal — rather than a second icon on the graph node. The 380px detail
// panel cannot hold a grid, and NodeDetail is already the node's "expand"
// surface, so the modal keeps one interaction path: click node → detail →
// analyse. Nothing here is canvas-local: the modal mounts the same
// DrillableGrid the answer surface inherits later (DAT-678).
//
// BOTH node kinds compose AD HOC on open from their persisted clause parts
// (`/api/drill/node`, parts-at-source): a metric rebuilds its DAG subtree, a
// measure is the single-extract case of the same composition. Nothing is
// pre-composed; the persisted `sql` column stays a reference display in the
// detail panel, never the execution path. Gates: a metric needs a parsed DAG
// (`hasDag` — a hole gets the composer's NAMED refusal in the modal instead
// of a silently missing button); a measure must be accepted (`grounded` — an
// unaccepted extract would render an empty grid as if it were data).

import { Alert, Button, Center, Group, Loader, Modal } from "@mantine/core";
import { useQuery } from "@tanstack/react-query";
import { Play } from "lucide-react";
import { useState } from "react";

import type { DrillAxesRequest } from "#/duckdb/drill";
import type { OMNode } from "#/tools/operating-model-graph";
import { DrillableGrid } from "#/ui/cockpit/widgets/drillable-grid";

/** The node's compose target — the same ref shape the axes route resolves —
 *  or null when there is nothing to run. */
export function analyseTarget(node: OMNode): DrillAxesRequest | null {
	const d = node.data;
	// Node ids are namespaced (`metric:<graphId>` / `measure:<standardField>`) —
	// the suffix IS the resolver key.
	const key = node.id.slice(node.id.indexOf(":") + 1);
	if (d.kind === "metric" && d.hasDag) return { metricKey: key };
	if (d.kind === "measure" && d.grounded) return { standardField: key };
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

/** Compose-on-open: fetch the node's ad-hoc composed SQL from its parts, then
 *  mount the shared grid on it. Loading/refusal states are part of the
 *  surface — a refusal names the missing part, never a dead end. */
function NodeGrid({ nodeRef }: { nodeRef: DrillAxesRequest }) {
	const compose = useQuery({
		queryKey: ["drill-node", nodeRef],
		queryFn: async (): Promise<NodeComposeState> => {
			const res = await fetch("/api/drill/node", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify(nodeRef),
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
	return (
		<DrillableGrid
			sql={compose.data.sql}
			axesRequest={nodeRef}
			nodeRef={nodeRef}
		/>
	);
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
				<NodeGrid nodeRef={target} />
			</Modal>
		</Group>
	);
}
