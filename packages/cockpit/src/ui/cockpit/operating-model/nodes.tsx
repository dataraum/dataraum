// Custom node renderers for the operating-model canvas (DAT-591). One component
// switches on the node kind (concept / metric / validation / cycle / table /
// column / driver) — a pure display of engine-persisted values (React idiom rule
// 12: widgets color/format, never recompute). Click handling lives on the canvas
// (onNodeClick), so these stay render-only.

import { Badge, Group, Paper, Stack, Text } from "@mantine/core";
import { Handle, type Node, type NodeProps, Position } from "@xyflow/react";
import {
	Columns3,
	Database,
	FunctionSquare,
	Gauge,
	Hash,
	Lightbulb,
	type LucideIcon,
	Network,
	RefreshCw,
	ShieldCheck,
	Table2,
	TrendingUp,
} from "lucide-react";
import { memo } from "react";

import type { OMNode, OMNodeKind } from "#/tools/operating-model-graph";
import { OM_NODE_WIDTH } from "./layout";

/** RF node payload — the engine node plus the table-expanded flag (display only). */
export interface OMRfData extends Record<string, unknown> {
	om: OMNode;
	expanded: boolean;
}
export type OMRfNode = Node<OMRfData, "om">;

const KIND_STYLE: Record<OMNodeKind, { color: string; Icon: LucideIcon }> = {
	concept: { color: "grape", Icon: Lightbulb },
	metric: { color: "blue", Icon: Gauge },
	// The metric's DAG guts: a formula computes, an extract pulls from the data, a
	// constant is a fixed value — each visually distinct so the structure reads.
	formula: { color: "violet", Icon: FunctionSquare },
	extract: { color: "cyan", Icon: Database },
	constant: { color: "yellow", Icon: Hash },
	validation: { color: "teal", Icon: ShieldCheck },
	cycle: { color: "indigo", Icon: RefreshCw },
	table: { color: "gray", Icon: Table2 },
	column: { color: "gray", Icon: Columns3 },
	driver: { color: "orange", Icon: TrendingUp },
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
		case "validation":
			return (
				<Badge
					size="xs"
					variant="light"
					color={
						om.data.passed === true
							? "teal"
							: om.data.passed === false
								? "red"
								: "gray"
					}
				>
					{om.data.passed === true
						? "pass"
						: om.data.passed === false
							? "fail"
							: (om.data.state ?? "—")}
				</Badge>
			);
		case "cycle":
			return om.data.completionRate !== null ? (
				<Badge size="xs" variant="light" color="indigo">
					{Math.round(om.data.completionRate * 100)}%
				</Badge>
			) : null;
		// Extract shows its aggregation (sum / avg / …); constant shows its value —
		// the at-a-glance signal that differentiates the step at a distance.
		case "extract":
			return om.data.aggregation ? (
				<Badge size="xs" variant="light" color="cyan">
					{om.data.aggregation}
				</Badge>
			) : null;
		case "constant":
			return om.data.value !== null ? (
				<Badge size="xs" variant="light" color="yellow">
					{om.data.value}
				</Badge>
			) : null;
		case "driver":
			return (
				<Badge size="xs" variant="light" color="orange">
					{om.data.grain}
				</Badge>
			);
		default:
			return null;
	}
}

function OperatingModelNodeImpl({ data, selected }: NodeProps<OMRfNode>) {
	const { om, expanded } = data;
	const { color, Icon } = KIND_STYLE[om.kind] ?? KIND_STYLE.concept;
	const isTable = om.kind === "table";
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
							{isTable ? (expanded ? " ▾" : " ▸") : ""}
						</Text>
						<Text size="sm" fw={500} truncate title={om.label}>
							{om.label}
						</Text>
					</Stack>
					{statusBadge(om)}
				</Group>
			</Paper>
			<Handle type="source" position={Position.Bottom} style={{ opacity: 0 }} />
		</>
	);
}

export const OperatingModelNode = memo(OperatingModelNodeImpl);

/** Single node type — the component switches on `data.om.kind` internally. */
export const omNodeTypes = { om: OperatingModelNode } as const;

/** The Network rail icon, re-exported so the empty state can echo it. */
export { Network as ModelIcon };
