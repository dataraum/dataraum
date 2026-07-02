// Custom node renderers for the operating-model METRIC canvas (DAT-591). One
// component switches on the node kind (metric / measure / constant / table) — a pure
// display of engine-persisted values (React idiom 12: widgets color/format, never
// recompute). Click handling lives on the canvas (onNodeClick), so these stay
// render-only. A metric shows its output formula as a subtitle; a measure its
// aggregation; a constant its value; an enriched-view table a collapse caret.

import { Badge, Group, Paper, Stack, Text } from "@mantine/core";
import { Handle, type Node, type NodeProps, Position } from "@xyflow/react";
import {
	Database,
	Gauge,
	Hash,
	type LucideIcon,
	Network,
	Table2,
} from "lucide-react";
import { memo } from "react";

import type { OMNode, OMNodeKind } from "#/tools/operating-model-graph";
import { OM_NODE_WIDTH } from "./layout";

/** RF node payload — the engine node plus the expanded flag (display only). */
export interface OMRfData extends Record<string, unknown> {
	om: OMNode;
	expanded: boolean;
}
export type OMRfNode = Node<OMRfData, "om">;

const KIND_STYLE: Record<OMNodeKind, { color: string; Icon: LucideIcon }> = {
	metric: { color: "blue", Icon: Gauge },
	measure: { color: "cyan", Icon: Database },
	constant: { color: "yellow", Icon: Hash },
	table: { color: "gray", Icon: Table2 },
};

/** A short status chip per kind — the at-a-glance signal on the node face. */
function statusBadge(om: OMNode): React.ReactNode {
	switch (om.data.kind) {
		case "metric":
			return (
				<Badge size="xs" variant="light" color="blue">
					{om.data.state}
				</Badge>
			);
		case "measure":
			// Aggregation (sum / avg / …), cyan when grounded; ORANGE when the extract
			// was not accepted (no support) — a clear flag, not a silent grey.
			return (
				<Badge
					size="xs"
					variant="light"
					color={om.data.grounded ? "cyan" : "orange"}
				>
					{om.data.grounded ? (om.data.aggregation ?? "measure") : "no support"}
				</Badge>
			);
		case "constant":
			return om.data.value !== null ? (
				<Badge size="xs" variant="light" color="yellow">
					{om.data.value}
				</Badge>
			) : null;
		default:
			return null;
	}
}

/** The metric's output formula, shown as a subtitle under its name (its computation). */
function subtitle(om: OMNode): string | null {
	return om.data.kind === "metric" ? om.data.formula : null;
}

function OperatingModelNodeImpl({ data, selected }: NodeProps<OMRfNode>) {
	const { om, expanded } = data;
	const { color, Icon } = KIND_STYLE[om.kind] ?? KIND_STYLE.metric;
	// Only enriched-view tables collapse (they own base tables); base tables are leaves.
	const isEnrichedView =
		om.data.kind === "table" && om.data.layer === "enriched";
	const sub = subtitle(om);
	return (
		<>
			{/* Hidden handles: edges attach, but the dots don't clutter the DAG. */}
			<Handle type="target" position={Position.Top} style={{ opacity: 0 }} />
			<Paper
				withBorder
				p="xs"
				radius="md"
				shadow={selected ? "md" : "xs"}
				style={{
					width: OM_NODE_WIDTH,
					borderColor: selected
						? `var(--mantine-color-${color}-filled)`
						: undefined,
					borderWidth: selected ? 2 : 1,
				}}
			>
				<Group gap="xs" wrap="nowrap" align="flex-start">
					<Icon
						size={16}
						color={`var(--mantine-color-${color}-filled)`}
						style={{ flexShrink: 0, marginTop: 2 }}
					/>
					<Stack gap={0} style={{ minWidth: 0, flex: 1 }}>
						<Text size="xs" c="dimmed" tt="uppercase" fw={600}>
							{om.kind}
							{isEnrichedView ? (expanded ? " ▾" : " ▸") : ""}
						</Text>
						<Text size="sm" fw={500} truncate title={om.label}>
							{om.label}
						</Text>
						{sub ? (
							<Text size="xs" c="dimmed" truncate title={sub}>
								{sub}
							</Text>
						) : null}
					</Stack>
					{statusBadge(om)}
				</Group>
			</Paper>
			<Handle type="source" position={Position.Bottom} style={{ opacity: 0 }} />
		</>
	);
}

// memo (rule 6): React Flow re-renders the whole node set on every pan/zoom/viewport
// change; memoizing the per-node component keeps that to nodes whose data actually
// changed — the xyflow-recommended pattern for custom nodes.
export const OperatingModelNode = memo(OperatingModelNodeImpl);

/** Single node type — the component switches on `data.om.kind` internally. */
export const omNodeTypes = { om: OperatingModelNode } as const;

/** The Network rail icon, re-exported so the empty state can echo it. */
export { Network as ModelIcon };
