// The operating-model canvas (DAT-591 Phase 1) — renders the workspace's
// concept-spine DAG with React Flow + dagre. Client-only (the route wraps this in
// <ClientOnly>): React Flow measures the DOM, so it must not render on the server.
//
// Progressive disclosure: columns are collapsed under their table by default
// (computeVisibleGraph re-points their edges to the table); clicking a table node
// toggles its columns. Clicking any other node opens a read-only detail panel
// (metric/validation SQL, driver dimensions, cycle completion). Pure render of
// engine-persisted values — no analysis happens here.

import "@xyflow/react/dist/style.css";

import {
	Badge,
	Box,
	Code,
	Group,
	ScrollArea,
	Stack,
	Text,
} from "@mantine/core";
import {
	Background,
	Controls,
	type Edge,
	MarkerType,
	MiniMap,
	type Node,
	type NodeMouseHandler,
	ReactFlow,
	useReactFlow,
} from "@xyflow/react";
import { X } from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";

import {
	computeVisibleGraph,
	type OMNode,
	type OperatingModelGraph,
} from "#/tools/operating-model-graph";
import { layoutGraph } from "./layout";
import { type OMRfData, omNodeTypes } from "./nodes";

const EDGE_COLOR = "var(--mantine-color-gray-5)";

export function OperatingModelCanvas({
	graph,
}: {
	graph: OperatingModelGraph;
}) {
	const [expanded, setExpanded] = useState<ReadonlySet<string>>(new Set());
	const [selected, setSelected] = useState<OMNode | null>(null);

	const visible = useMemo(
		() => computeVisibleGraph(graph, expanded),
		[graph, expanded],
	);

	const { nodes, edges } = useMemo(() => {
		const rfNodes: Node<OMRfData>[] = visible.nodes.map((om) => ({
			id: om.id,
			type: "om",
			position: { x: 0, y: 0 },
			data: { om, expanded: expanded.has(om.id) },
		}));
		const rfEdges: Edge[] = visible.edges.map((e) => ({
			id: e.id,
			source: e.source,
			target: e.target,
			markerEnd: { type: MarkerType.ArrowClosed, color: EDGE_COLOR },
			style: { stroke: EDGE_COLOR },
		}));
		return { nodes: layoutGraph(rfNodes, rfEdges), edges: rfEdges };
	}, [visible, expanded]);

	const onNodeClick = useCallback<NodeMouseHandler>((_event, node) => {
		const data = node.data as OMRfData;
		const om = data.om;
		if (om.kind === "table") {
			// Toggle this table's columns (progressive disclosure).
			setExpanded((prev) => {
				const next = new Set(prev);
				if (next.has(om.id)) next.delete(om.id);
				else next.add(om.id);
				return next;
			});
			return;
		}
		// Columns are leaf grounding nodes — nothing to detail; ignore the click.
		if (om.kind === "column") return;
		setSelected(om);
	}, []);

	return (
		<Box style={{ position: "relative", width: "100%", height: "100%" }}>
			<ReactFlow
				nodes={nodes}
				edges={edges}
				nodeTypes={omNodeTypes}
				onNodeClick={onNodeClick}
				onPaneClick={() => setSelected(null)}
				nodesDraggable={false}
				nodesConnectable={false}
				edgesFocusable={false}
				fitView
				minZoom={0.1}
				onlyRenderVisibleElements
				proOptions={{ hideAttribution: false }}
			>
				<Background />
				<Controls showInteractive={false} />
				<MiniMap pannable zoomable />
				{/* Re-fit when a table expand/collapse re-lays-out the graph (the
				    fitView prop only fires on mount). Must be a ReactFlow child to
				    reach useReactFlow(). */}
				<FitOnLayout layout={nodes} />
			</ReactFlow>
			{selected ? (
				<NodeDetail node={selected} onClose={() => setSelected(null)} />
			) : null}
		</Box>
	);
}

/** Refits the viewport whenever the laid-out node set changes (expand/collapse). */
function FitOnLayout({ layout }: { layout: Node[] }) {
	const { fitView } = useReactFlow();
	useEffect(() => {
		if (layout.length === 0) return;
		void fitView({ duration: 200 });
	}, [layout, fitView]);
	return null;
}

/** Read-only detail for the selected node — the "expand to read the SQL" surface. */
function NodeDetail({ node, onClose }: { node: OMNode; onClose: () => void }) {
	return (
		<Box
			style={{
				position: "absolute",
				top: 8,
				right: 8,
				bottom: 8,
				width: 360,
				zIndex: 5,
			}}
		>
			<Stack
				gap="sm"
				h="100%"
				p="md"
				style={{
					background: "var(--mantine-color-body)",
					border: "1px solid var(--mantine-color-default-border)",
					borderRadius: "var(--mantine-radius-md)",
					boxShadow: "var(--mantine-shadow-md)",
				}}
			>
				<Group justify="space-between" wrap="nowrap">
					<Group gap="xs" wrap="nowrap" style={{ minWidth: 0 }}>
						<Badge variant="light" tt="uppercase">
							{node.kind}
						</Badge>
						<Text fw={600} truncate title={node.label}>
							{node.label}
						</Text>
					</Group>
					<Box
						component="button"
						type="button"
						onClick={onClose}
						aria-label="Close detail"
						style={{
							background: "none",
							border: "none",
							cursor: "pointer",
							color: "var(--mantine-color-dimmed)",
						}}
					>
						<X size={16} />
					</Box>
				</Group>
				<ScrollArea style={{ flex: 1 }}>
					<NodeDetailBody node={node} />
				</ScrollArea>
			</Stack>
		</Box>
	);
}

function NodeDetailBody({ node }: { node: OMNode }) {
	const d = node.data;
	switch (d.kind) {
		case "metric":
			return (
				<Stack gap="xs">
					<Field label="State" value={d.state} />
					{d.stateReason ? (
						<Field label="Reason" value={d.stateReason} />
					) : null}
					<Field label="SQL steps" value={String(d.snippetCount)} />
					{d.sql ? (
						<Code block>{d.sql}</Code>
					) : (
						<Text size="sm" c="dimmed">
							No grounded SQL yet.
						</Text>
					)}
				</Stack>
			);
		case "validation":
			return (
				<Stack gap="xs">
					<Field
						label="Result"
						value={
							d.passed === true
								? "passed"
								: d.passed === false
									? "failed"
									: (d.status ?? d.state)
						}
					/>
					{d.severity ? <Field label="Severity" value={d.severity} /> : null}
					{d.sqlUsed ? (
						<Code block>{d.sqlUsed}</Code>
					) : (
						<Text size="sm" c="dimmed">
							No executed SQL.
						</Text>
					)}
				</Stack>
			);
		case "cycle":
			return (
				<Stack gap="xs">
					<Field label="State" value={d.state} />
					{d.completionRate !== null ? (
						<Field
							label="Completion"
							value={`${Math.round(d.completionRate * 100)}%`}
						/>
					) : null}
					{d.completedCycles !== null && d.totalRecords !== null ? (
						<Field
							label="Completed"
							value={`${d.completedCycles} / ${d.totalRecords}`}
						/>
					) : null}
				</Stack>
			);
		case "driver":
			return (
				<Stack gap="xs">
					<Field label="Target" value={d.targetType} />
					<Field label="Grain" value={d.grain} />
					<Text size="xs" c="dimmed" tt="uppercase" fw={600}>
						Top dimensions
					</Text>
					{d.topDimensions.length ? (
						<Group gap="xs">
							{d.topDimensions.map((dim) => (
								<Badge key={dim} variant="light" color="orange">
									{dim}
								</Badge>
							))}
						</Group>
					) : (
						<Text size="sm" c="dimmed">
							No significant dimensions.
						</Text>
					)}
				</Stack>
			);
		case "concept":
			return (
				<Text size="sm" c="dimmed">
					A vocabulary concept — the hub linking the metrics, cycles and
					validations that reference it to the columns it grounds to.
				</Text>
			);
		default:
			return (
				<Text size="sm" c="dimmed">
					{node.kind} node.
				</Text>
			);
	}
}

function Field({ label, value }: { label: string; value: string }) {
	return (
		<Group gap="xs" wrap="nowrap" align="flex-start">
			<Text
				size="xs"
				c="dimmed"
				tt="uppercase"
				fw={600}
				style={{ flexShrink: 0 }}
			>
				{label}
			</Text>
			<Text size="sm" style={{ wordBreak: "break-word" }}>
				{value}
			</Text>
		</Group>
	);
}
