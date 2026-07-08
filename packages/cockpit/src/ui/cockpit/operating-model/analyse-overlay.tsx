// The Model canvas's analyse surface (DAT-672, per-node re-cut DAT-702/703,
// UX re-cut DAT-712): run a metric/measure node in the SHARED result grid,
// with the live equation header, drill + chart on top.
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
//
// LAYERING (DAT-712, the lead's constraint): DrillableGrid stays generic;
// this file is the PARTS-CONTEXT layer. The open call ships the node's
// formula shape + the totals statement; this layer owns the equation header,
// the operand hue assignment (handed verbatim to the grid's columnAccents),
// the totals footer cells, and the scope sentence — all keyed on "the
// response carries a structured shape", so answer-agent results (DAT-678)
// join by shipping the same block, not by being canvas nodes.

import { Alert, Button, Center, Group, Loader, Modal } from "@mantine/core";
import { useQuery } from "@tanstack/react-query";
import { Play } from "lucide-react";
import { useMemo, useState } from "react";

import { useChartData } from "#/charts/use-chart-data";
import type { DrillAxesRequest, DrillStep } from "#/duckdb/drill";
import { grainLabel, parseGrainToken } from "#/duckdb/grain";
import type { OMNode } from "#/tools/operating-model-graph";
import { DrillableGrid } from "#/ui/cockpit/widgets/drillable-grid";
import {
	EquationHeader,
	type NodeShapeWire,
	operandAccents,
} from "#/ui/cockpit/widgets/equation-header";

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

/** `/api/drill/node` open response, narrowed at the boundary (never trusted).
 *  `node` + `totals` are the DAT-712 header block — optional by design (the
 *  grid must open without them). */
type NodeComposeState =
	| {
			ok: true;
			sql: string;
			node: NodeShapeWire | null;
			totalsSql: string | null;
	  }
	| { ok: false; reason: string };

function narrowShape(raw: unknown): NodeShapeWire | null {
	if (typeof raw !== "object" || raw === null) return null;
	const r = raw as Record<string, unknown>;
	if (typeof r.targetStepId !== "string") return null;
	const operands = Array.isArray(r.operands)
		? r.operands.flatMap((o): NodeShapeWire["operands"] => {
				if (typeof o !== "object" || o === null) return [];
				const op = o as Record<string, unknown>;
				if (typeof op.stepId !== "string") return [];
				const kind =
					op.kind === "formula" || op.kind === "constant"
						? op.kind
						: ("extract" as const);
				return [
					{
						stepId: op.stepId,
						kind,
						value: typeof op.value === "string" ? op.value : null,
					},
				];
			})
		: [];
	return {
		name: typeof r.name === "string" ? r.name : null,
		unit: typeof r.unit === "string" ? r.unit : null,
		targetStepId: r.targetStepId,
		expression: typeof r.expression === "string" ? r.expression : null,
		additive: r.additive === true,
		operands,
	};
}

function narrowNodeCompose(raw: unknown): NodeComposeState {
	if (typeof raw === "object" && raw !== null) {
		const r = raw as Record<string, unknown>;
		if (r.ok === true && typeof r.sql === "string") {
			const totals = r.totals as Record<string, unknown> | undefined;
			return {
				ok: true,
				sql: r.sql,
				node: narrowShape(r.node),
				totalsSql:
					typeof totals === "object" &&
					totals !== null &&
					typeof totals.sql === "string"
						? totals.sql
						: null,
			};
		}
		if (r.ok === false && typeof r.reason === "string") {
			return { ok: false, reason: r.reason };
		}
	}
	return { ok: false, reason: "unexpected compose response" };
}

/** The drill scope in words, for the equation header's scope line and the
 *  missing-operand sentence: "all data", "by Month of entry_id__date",
 *  "pinned to 2025-01-01". Sober and literal — never inferred period names. */
export function scopeSentence(steps: DrillStep[]): string {
	const pins = steps.filter((s) => s.kind === "pin");
	if (pins.length > 0) {
		return `pinned to ${pins
			.map((p) => `${String(p.value ?? "∅")}`)
			.join(", ")}`;
	}
	const slices = steps.filter((s) => s.kind === "slice");
	if (slices.length > 0) {
		return `by ${slices
			.map((s) => {
				const grain = s.grain ? parseGrainToken(s.grain) : null;
				return grain ? `${grainLabel(grain)} of ${s.column}` : s.column;
			})
			.join(", ")}`;
	}
	return "all data";
}

/** Compose-on-open: fetch the node's ad-hoc composed SQL (+ the DAT-712
 *  header block) from its parts, then mount the equation layer over the
 *  shared grid. Loading/refusal states are part of the surface — a refusal
 *  names the missing part, never a dead end. */
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

	// The header's bindings (React rule 1: plain state set by grid callbacks,
	// everything else derived during render).
	const [hoverRow, setHoverRow] = useState<Record<string, unknown> | null>(
		null,
	);
	const [lockedRow, setLockedRow] = useState<Record<string, unknown> | null>(
		null,
	);
	const [steps, setSteps] = useState<DrillStep[]>([]);

	const data = compose.data;
	const ok: Extract<NodeComposeState, { ok: true }> | null =
		data !== undefined && data.ok === true ? data : null;
	// The totals row: one bounded query, shared cache (chart-data key).
	const totalsQuery = useChartData(
		ok?.totalsSql ?? "",
		[],
		ok?.totalsSql != null,
	);
	const totalsRow = totalsQuery.data?.rows[0] ?? null;

	const shape = ok?.node ?? null;
	const accents = useMemo(
		() => (shape ? operandAccents(shape) : undefined),
		[shape],
	);

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
	if (!ok) {
		return (
			<Alert color="yellow" title="Cannot compose this node">
				{compose.data && !compose.data.ok ? compose.data.reason : ""}
			</Alert>
		);
	}
	return (
		<div>
			{shape?.expression && (
				<EquationHeader
					shape={shape}
					totals={totalsRow}
					hoverRow={hoverRow}
					lockedRow={lockedRow}
					scope={scopeSentence(steps)}
				/>
			)}
			<DrillableGrid
				sql={ok.sql}
				axesRequest={nodeRef}
				nodeRef={nodeRef}
				footerCells={totalsRow ?? undefined}
				columnAccents={accents}
				columnUnits={shape?.unit ? { value: shape.unit } : undefined}
				onRowHover={setHoverRow}
				onPinnedRow={setLockedRow}
				onStepsChange={setSteps}
			/>
		</div>
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
