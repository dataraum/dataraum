// Dagre layered layout for the operating-model canvas (DAT-591). React Flow ships
// no layout algorithm (reactflow.dev/learn/layouting) — we run dagre over the
// VISIBLE graph (progressive disclosure keeps the node count in dagre's comfortable
// "hundreds" range; ELK-layered is the documented upgrade if a vertical outgrows it).
// Pure transform: (rf nodes, rf edges) → nodes with computed positions.
//
// LAYOUT DIRECTION: LR (left→right), the lineage convention. The concept-spine is a
// shallow-but-wide fan-in — many artifacts (metrics/validations/cycles) → a few
// concept hubs → columns. TB would stack all ~40 sibling artifacts into ONE row
// (thousands of px wide → fitView zooms to an unreadable strip); LR turns that fan
// into a scrollable vertical column at each layer, reading like a dbt/lineage DAG.
// `network-simplex` gives the tightest layering; the tight nodesep packs siblings so
// the per-layer column stays compact.

import dagre from "@dagrejs/dagre";
import type { Edge, Node } from "@xyflow/react";

const NODE_W = 210;
const NODE_H = 56;

/** Position nodes left-to-right by dagre rank; returns a new node array. */
export function layoutGraph(
	nodes: Node[],
	edges: Edge[],
	direction: "TB" | "LR" = "LR",
): Node[] {
	const g = new dagre.graphlib.Graph();
	g.setGraph({
		rankdir: direction,
		// LR: nodesep = vertical gap between siblings in a layer; ranksep = horizontal
		// gap between layers. Tight sibling packing keeps the tall fan-in columns
		// legible; wide ranksep leaves room for edge routing between layers.
		nodesep: 18,
		ranksep: 160,
		ranker: "network-simplex",
	});
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
