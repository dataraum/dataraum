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
	Button,
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
	OM_PRESET_KINDS,
	type OMNode,
	type OMNodeKind,
	type OMPreset,
	type OperatingModelGraph,
} from "#/tools/operating-model-graph";
import { SqlBlock } from "#/ui/cockpit/widgets/sql-block";
import { layoutGraph } from "./layout";
import { type OMRfData, omNodeTypes } from "./nodes";

const EDGE_COLOR = "var(--mantine-color-gray-5)";

// Land at a READABLE zoom, not fit-everything-tiny. The concept-spine fans ~40
// artifacts into a few hubs — fitting the whole width would zoom to an illegible
// strip (≈0.13). Cap the fit zoom so nodes stay readable on entry; the minimap +
// pan carry navigation across the rest. padding keeps the entry off the edge.
const FIT_VIEW_OPTIONS = { maxZoom: 0.85, padding: 0.15 } as const;

export function OperatingModelCanvas({
	graph,
}: {
	graph: OperatingModelGraph;
}) {
	const [expanded, setExpanded] = useState<ReadonlySet<string>>(new Set());
	const [selected, setSelected] = useState<OMNode | null>(null);
	// Filter state: which kinds show + whether to drop unconnected nodes. Hiding
	// orphans is ON by default — it clears the finance vertical's ~8 declared-but-
	// not-detected cycles (degree-0 floaters) on first paint.
	const [enabledKinds, setEnabledKinds] = useState<ReadonlySet<OMNodeKind>>(
		() => new Set(OM_NODE_KINDS),
	);
	const [hideOrphans, setHideOrphans] = useState(true);

	// Pipeline: collapse columns under tables (progressive disclosure) → filter by
	// kind + orphan → dagre layout. Orphan degree is measured post-collapse so a
	// table's collapsed column edges count toward keeping it.
	const visible = useMemo(
		() => computeVisibleGraph(graph, expanded),
		[graph, expanded],
	);
	const filtered = useMemo(
		() => filterGraph(visible, { kinds: enabledKinds, hideOrphans }),
		[visible, enabledKinds, hideOrphans],
	);

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
				fitViewOptions={FIT_VIEW_OPTIONS}
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
			<FilterBar
				enabledKinds={enabledKinds}
				onKindsChange={setEnabledKinds}
				hideOrphans={hideOrphans}
				onHideOrphansChange={setHideOrphans}
			/>
			{nodes.length === 0 ? (
				<Center
					style={{
						position: "absolute",
						inset: 0,
						pointerEvents: "none",
					}}
				>
					<Text size="sm" c="dimmed">
						No nodes match the current filters.
					</Text>
				</Center>
			) : null}
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
		void fitView({ duration: 200, ...FIT_VIEW_OPTIONS });
	}, [layout, fitView]);
	return null;
}

// The kinds exposed as individual toggles. `column` is omitted — its visibility is
// governed by table expansion (progressive disclosure), not a top-level toggle, so
// it always rides in the effective filter set.
const TOGGLE_KINDS: readonly OMNodeKind[] = [
	"metric",
	"validation",
	"cycle",
	"driver",
	"concept",
	"table",
];

const PRESETS: readonly { key: OMPreset; label: string }[] = [
	{ key: "full", label: "Full" },
	{ key: "metrics", label: "Metrics" },
	{ key: "validations", label: "Validations" },
	{ key: "cycles", label: "Cycles" },
];

const sameKinds = (
	a: ReadonlySet<OMNodeKind>,
	b: readonly OMNodeKind[],
): boolean => a.size === b.length && b.every((k) => a.has(k));

/** Overlay filter control: preset lenses + per-kind toggles + hide-unconnected. */
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
	const activePreset = PRESETS.find((p) =>
		sameKinds(enabledKinds, OM_PRESET_KINDS[p.key]),
	)?.key;
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
				maxWidth: 360,
			}}
		>
			<Stack gap={8}>
				<Group gap={6} wrap="nowrap">
					<Text size="xs" c="dimmed" fw={600} tt="uppercase">
						Lens
					</Text>
					<Button.Group>
						{PRESETS.map((p) => (
							<Button
								key={p.key}
								size="compact-xs"
								variant={activePreset === p.key ? "filled" : "default"}
								onClick={() => onKindsChange(new Set(OM_PRESET_KINDS[p.key]))}
							>
								{p.label}
							</Button>
						))}
					</Button.Group>
				</Group>
				<Chip.Group
					multiple
					value={[...enabledKinds].filter((k) => k !== "column")}
					onChange={(vals) =>
						onKindsChange(new Set([...(vals as OMNodeKind[]), "column"]))
					}
				>
					<Group gap={4}>
						{TOGGLE_KINDS.map((k) => (
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
						<SqlBlock sql={d.sql} maxHeight={300} />
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
						<SqlBlock sql={d.sqlUsed} maxHeight={300} />
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
