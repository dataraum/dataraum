// The operating-model METRIC canvas (DAT-591) — renders the workspace's metric
// dependency graph with React Flow + dagre. Client-only (the route wraps this in
// <ClientOnly>): React Flow measures the DOM, so it must not render on the server.
//
// STRUCTURE the graph shows: metric → metric (composition) → measure → table, plus
// metric → constant. EXECUTION detail: clicking a metric/measure opens a read-only
// panel with its flattened SQL. Progressive disclosure: base fact/dim tables are
// collapsed under their enriched view (computeVisibleGraph re-points their edges);
// clicking an enriched-view node toggles them. Pure render of engine-persisted values.

import "@xyflow/react/dist/style.css";

import {
	Badge,
	Box,
	Center,
	Chip,
	Group,
	Paper,
	ScrollArea,
	Stack,
	Switch,
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
	filterGraph,
	OM_NODE_KINDS,
	type OMNode,
	type OMNodeKind,
	type OperatingModelGraph,
} from "#/tools/operating-model-graph";
import { SqlBlock } from "#/ui/cockpit/widgets/sql-block";
import { layoutGraph } from "./layout";
import { type OMRfData, omNodeTypes } from "./nodes";

const EDGE_COLOR = "var(--mantine-color-gray-5)";

// Land at a READABLE zoom, not fit-everything-tiny. Cap the fit zoom so nodes stay
// legible on entry; the minimap + pan carry navigation across the rest.
const FIT_VIEW_OPTIONS = { maxZoom: 0.85, padding: 0.15 } as const;

/** True for an enriched-view table node (the expandable ones — base tables are leaves). */
function isEnrichedView(om: OMNode): boolean {
	return om.data.kind === "table" && om.data.layer === "enriched";
}

export function OperatingModelCanvas({
	graph,
}: {
	graph: OperatingModelGraph;
}) {
	const [expanded, setExpanded] = useState<ReadonlySet<string>>(new Set());
	// Store the selected id, not the node — the node is DERIVED from the visible set
	// (React idiom rule 1), so the detail panel never goes stale and auto-closes when a
	// filter/collapse removes the node.
	const [selectedId, setSelectedId] = useState<string | null>(null);
	const [enabledKinds, setEnabledKinds] = useState<ReadonlySet<OMNodeKind>>(
		() => new Set(OM_NODE_KINDS),
	);
	// Hiding orphans is ON by default — it clears any ungrounded-measure floaters and
	// declared-but-uncomposed metrics from first paint.
	const [hideOrphans, setHideOrphans] = useState(true);

	// Pipeline: collapse base tables under their enriched view → filter by kind + orphan
	// → dagre layout. Orphan degree is measured post-collapse.
	const visible = useMemo(
		() => computeVisibleGraph(graph, expanded),
		[graph, expanded],
	);
	const filtered = useMemo(
		() => filterGraph(visible, { kinds: enabledKinds, hideOrphans }),
		[visible, enabledKinds, hideOrphans],
	);
	// Derived, not mirrored: null when the selection is filtered/collapsed out of view.
	const selected =
		selectedId != null
			? (filtered.nodes.find((n) => n.id === selectedId) ?? null)
			: null;

	const { nodes, edges } = useMemo(() => {
		const rfNodes: Node<OMRfData>[] = filtered.nodes.map((om) => ({
			id: om.id,
			type: "om",
			position: { x: 0, y: 0 },
			data: { om, expanded: expanded.has(om.id) },
		}));
		const rfEdges: Edge[] = filtered.edges.map((e) => ({
			id: e.id,
			source: e.source,
			target: e.target,
			markerEnd: { type: MarkerType.ArrowClosed, color: EDGE_COLOR },
			style: { stroke: EDGE_COLOR },
		}));
		return { nodes: layoutGraph(rfNodes, rfEdges), edges: rfEdges };
	}, [filtered, expanded]);

	const onNodeClick = useCallback<NodeMouseHandler>((_event, node) => {
		const om = (node.data as OMRfData).om;
		if (isEnrichedView(om)) {
			// Toggle this enriched view's base tables (progressive disclosure).
			setExpanded((prev) => {
				const next = new Set(prev);
				if (next.has(om.id)) next.delete(om.id);
				else next.add(om.id);
				return next;
			});
			return;
		}
		// Base tables are leaf grounding nodes — nothing to detail; ignore the click.
		if (om.kind === "table") return;
		setSelectedId(om.id);
	}, []);

	return (
		<Box style={{ position: "relative", width: "100%", height: "100%" }}>
			<ReactFlow
				nodes={nodes}
				edges={edges}
				nodeTypes={omNodeTypes}
				onNodeClick={onNodeClick}
				onPaneClick={() => setSelectedId(null)}
				nodesDraggable={false}
				nodesConnectable={false}
				edgesFocusable={false}
				fitView
				fitViewOptions={FIT_VIEW_OPTIONS}
				minZoom={0.1}
				onlyRenderVisibleElements
				proOptions={{ hideAttribution: false }}
			>
				<Background />
				<Controls showInteractive={false} />
				<MiniMap pannable zoomable />
				{/* Re-fit when an expand/collapse re-lays-out the graph (fitView only fires
				    on mount). Must be a ReactFlow child to reach useReactFlow(). */}
				<FitOnLayout layout={nodes} />
			</ReactFlow>
			<FilterBar
				enabledKinds={enabledKinds}
				onKindsChange={setEnabledKinds}
				hideOrphans={hideOrphans}
				onHideOrphansChange={setHideOrphans}
			/>
			{nodes.length === 0 ? (
				<Center
					style={{ position: "absolute", inset: 0, pointerEvents: "none" }}
				>
					<Text size="sm" c="dimmed">
						No nodes match the current filters.
					</Text>
				</Center>
			) : null}
			{selected ? (
				<NodeDetail node={selected} onClose={() => setSelectedId(null)} />
			) : null}
		</Box>
	);
}

/** Refits the viewport whenever the laid-out node set changes (expand/collapse). */
function FitOnLayout({ layout }: { layout: Node[] }) {
	const { fitView } = useReactFlow();
	useEffect(() => {
		if (layout.length === 0) return;
		void fitView({ duration: 200, ...FIT_VIEW_OPTIONS });
	}, [layout, fitView]);
	return null;
}

/** Overlay filter control: per-kind toggles + hide-unconnected. */
function FilterBar({
	enabledKinds,
	onKindsChange,
	hideOrphans,
	onHideOrphansChange,
}: {
	enabledKinds: ReadonlySet<OMNodeKind>;
	onKindsChange: (kinds: ReadonlySet<OMNodeKind>) => void;
	hideOrphans: boolean;
	onHideOrphansChange: (value: boolean) => void;
}) {
	return (
		<Paper
			shadow="sm"
			p="xs"
			radius="md"
			withBorder
			style={{
				position: "absolute",
				top: 8,
				right: 8,
				zIndex: 5,
				maxWidth: 320,
			}}
		>
			<Stack gap={8}>
				<Chip.Group
					multiple
					value={[...enabledKinds]}
					onChange={(vals) => onKindsChange(new Set(vals as OMNodeKind[]))}
				>
					<Group gap={4}>
						{OM_NODE_KINDS.map((k) => (
							<Chip key={k} value={k} size="xs" variant="light">
								{k}
							</Chip>
						))}
					</Group>
				</Chip.Group>
				<Switch
					size="xs"
					label="Hide unconnected"
					checked={hideOrphans}
					onChange={(e) => onHideOrphansChange(e.currentTarget.checked)}
				/>
			</Stack>
		</Paper>
	);
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
				width: 380,
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
					{d.unit ? <Field label="Unit" value={d.unit} /> : null}
					{d.stateReason ? (
						<Field label="Reason" value={d.stateReason} />
					) : null}
					{d.formula ? <Field label="Formula" value={d.formula} /> : null}
					{d.sql ? (
						<SqlBlock sql={d.sql} maxHeight={320} />
					) : (
						<Text size="sm" c="dimmed">
							No composed SQL — the metric did not ground.
						</Text>
					)}
				</Stack>
			);
		case "measure":
			return (
				<Stack gap="xs">
					{d.statement ? <Field label="Statement" value={d.statement} /> : null}
					{d.aggregation ? (
						<Field label="Aggregation" value={d.aggregation} />
					) : null}
					{!d.grounded ? (
						<Text size="sm" c="orange" fw={500}>
							Not accepted — the extract composed SQL below, but it returned no
							rows (no support), so no metric using it can execute.
						</Text>
					) : null}
					{d.sql ? (
						<SqlBlock sql={d.sql} maxHeight={320} />
					) : (
						<Text size="sm" c="dimmed">
							No SQL composed.
						</Text>
					)}
				</Stack>
			);
		case "constant":
			return (
				<Stack gap="xs">
					<Field label="Value" value={d.value ?? "—"} />
					<Text size="sm" c="dimmed">
						A declared parameter used by the metrics that reference it.
					</Text>
				</Stack>
			);
		case "table":
			return (
				<Stack gap="xs">
					<Field
						label="Kind"
						value={d.layer === "enriched" ? "enriched view" : "base table"}
					/>
					<Text size="sm" c="dimmed">
						{d.layer === "enriched"
							? "The enriched view a measure reads — click the node to reveal the base fact/dim tables it derives from."
							: "A base fact/dimension table an enriched view is built from."}
					</Text>
				</Stack>
			);
		default:
			return null;
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
