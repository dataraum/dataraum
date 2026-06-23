// Dagre layered layout for the operating-model canvas (DAT-591). React Flow ships
// no layout algorithm (reactflow.dev/learn/layouting) — we run dagre over the
// VISIBLE graph (progressive disclosure keeps the node count in dagre's comfortable
// "hundreds" range; ELK-layered is the documented upgrade if a vertical outgrows it).
// Pure transform: (rf nodes, rf edges) → nodes with computed positions.

import dagre from "@dagrejs/dagre";
import type { Edge, Node } from "@xyflow/react";

const NODE_W = 210;
const NODE_H = 56;

/** Position nodes top-to-bottom by dagre rank; returns a new node array. */
export function layoutGraph(
	nodes: Node[],
	edges: Edge[],
	direction: "TB" | "LR" = "TB",
): Node[] {
	const g = new dagre.graphlib.Graph();
	g.setGraph({ rankdir: direction, nodesep: 36, ranksep: 90 });
	g.setDefaultEdgeLabel(() => ({}));

	for (const n of nodes) g.setNode(n.id, { width: NODE_W, height: NODE_H });
	for (const e of edges) g.setEdge(e.source, e.target);

	dagre.layout(g);

	return nodes.map((n) => {
		const p = g.node(n.id);
		// dagre centres nodes; React Flow positions by top-left corner.
		return { ...n, position: { x: p.x - NODE_W / 2, y: p.y - NODE_H / 2 } };
	});
}

export const OM_NODE_WIDTH = NODE_W;
export const OM_NODE_HEIGHT = NODE_H;
