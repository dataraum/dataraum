// The Model canvas's analyse surface (DAT-672): run a grounded metric/measure
// node's flattened SQL in the SHARED result grid, with drill + chart on top.
//
// UX shape (decided in implementation, per the ticket's two options): the
// affordance lives in the NodeDetail panel — an "Analyse" button opening a
// large modal — rather than a second icon on the graph node. The 380px detail
// panel cannot hold a grid, and NodeDetail is already the node's "expand"
// surface, so the modal keeps one interaction path: click node → detail →
// analyse. Nothing here is canvas-local: the modal mounts the same
// DrillableGrid the answer surface inherits later (DAT-678).
//
// Gating = runnable SQL: a metric needs its composed formula SQL; a measure
// additionally must be ACCEPTED (`grounded` — an unaccepted extract returns
// no rows, so executing it would render an empty grid as if it were data).

import { Button, Group, Modal } from "@mantine/core";
import { Play } from "lucide-react";
import { useState } from "react";

import type { DrillAxesRequest } from "#/duckdb/drill";
import type { OMNode } from "#/tools/operating-model-graph";
import { DrillableGrid } from "#/ui/cockpit/widgets/drillable-grid";

/** The node's runnable analysis target, or null when there is nothing to run. */
export function analyseTarget(
	node: OMNode,
): { sql: string; axesRequest: DrillAxesRequest } | null {
	const d = node.data;
	// Node ids are namespaced (`metric:<graphId>` / `measure:<standardField>`) —
	// the suffix IS the resolver key.
	const key = node.id.slice(node.id.indexOf(":") + 1);
	if (d.kind === "metric" && d.sql) {
		return { sql: d.sql, axesRequest: { metricKey: key } };
	}
	if (d.kind === "measure" && d.sql && d.grounded) {
		return { sql: d.sql, axesRequest: { standardField: key } };
	}
	return null;
}

/** The "Analyse" button + modal for a metric/measure node; renders nothing for
 *  nodes without runnable SQL (no affordance on ungrounded nodes — AC). */
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
				{/* Mounted only while open (Mantine default) — the grid query and the
				    axes fetch fire on first open, not on node selection. */}
				<DrillableGrid sql={target.sql} axesRequest={target.axesRequest} />
			</Modal>
		</Group>
	);
}
