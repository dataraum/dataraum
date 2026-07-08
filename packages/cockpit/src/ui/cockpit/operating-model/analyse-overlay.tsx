// The Model canvas's analyse surface (DAT-672, per-node re-cut DAT-702/703,
// UX re-cut DAT-712): run a metric/measure node in the SHARED result grid,
// with the live equation header, drill + chart on top.
//
// UX shape (iteration 2, lead direction 2026-07-08): clicking a RUNNABLE
// node (metric with a DAG, grounded measure) opens this modal DIRECTLY — the
// side panel remains only for what can't run (constants, tables, failed/
// ungrounded nodes, where the state reason and attempted SQL matter). The
// modal's TITLE row is the node's one identity line: kind, name, unit,
// statement/aggregation, and the live drill scope — the equation carries only
// the math, right of the slice controls. Nothing here is canvas-local: the
// modal mounts the same DrillableGrid the answer surface inherits later
// (DAT-678).
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

import {
	Alert,
	Badge,
	Center,
	Group,
	Loader,
	Modal,
	Text,
} from "@mantine/core";
import { useQuery } from "@tanstack/react-query";
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
	unitSymbol,
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
		// A grained pin names its bucket width — "2025-01-01 (Month)" is a
		// month, not a day, and the missing-operand sentence inherits this.
		return `pinned to ${pins
			.map((p) => {
				const grain = p.grain ? parseGrainToken(p.grain) : null;
				const label = String(p.value ?? "∅");
				return grain ? `${label} (${grainLabel(grain)})` : label;
			})
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

/** The analyse modal for a RUNNABLE metric/measure node — opened directly by
 *  the canvas node click (iteration 2). Mount it keyed on the node id and
 *  only while open, so drill/scope state resets per node. The TITLE row holds
 *  everything above the grid (iteration 3): the identity line (kind chip,
 *  name, unit, statement · aggregation, live scope) on the left and the live
 *  equation on the right, a fair gap before the close button. The slice
 *  controls live in the grid's own toolbar (`toolbarStart`). */
export function AnalyseModal({
	node,
	onClose,
}: {
	node: OMNode;
	onClose: () => void;
}) {
	const target = analyseTarget(node);
	const [steps, setSteps] = useState<DrillStep[]>([]);
	// The equation's bindings (React rule 1: plain state set by grid
	// callbacks, everything else derived during render).
	const [hoverRow, setHoverRow] = useState<Record<string, unknown> | null>(
		null,
	);
	const [lockedRow, setLockedRow] = useState<Record<string, unknown> | null>(
		null,
	);

	const compose = useQuery({
		queryKey: ["drill-node", target],
		enabled: target !== null,
		queryFn: async (): Promise<NodeComposeState> => {
			const res = await fetch("/api/drill/node", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify(target),
			});
			if (!res.ok) throw new Error(`compose failed (${res.status})`);
			return narrowNodeCompose(await res.json());
		},
	});
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

	if (!target) return null;
	const d = node.data;
	const scope = scopeSentence(steps);

	return (
		<Modal
			opened
			onClose={onClose}
			size="90%"
			data-testid="node-analyse-modal"
			// The title stretches so identity (left) and equation (right) share
			// the header row; pr keeps a fair gap to the close button. The
			// content is a FIXED-height flex column and the grid body its only
			// vertical scroller (`fillHeight`) — otherwise the 480px-capped grid
			// plus header overflows the modal and a second scrollbar appears on
			// the modal content.
			styles={{
				title: { flex: 1 },
				content: {
					display: "flex",
					flexDirection: "column",
					height: "calc(100dvh - 10dvh)",
				},
				body: {
					flex: 1,
					minHeight: 0,
					display: "flex",
					flexDirection: "column",
				},
			}}
			title={
				<Group justify="space-between" wrap="wrap" align="center" pr="xl">
					<Group gap="xs" wrap="wrap">
						<Badge variant="light" tt="uppercase">
							{node.kind}
						</Badge>
						<Text fw={600}>{node.label}</Text>
						{d.kind === "metric" && d.unit && (
							<Badge size="sm" variant="light" color="gray">
								{unitSymbol(d.unit)}
							</Badge>
						)}
						{d.kind === "measure" && (d.statement || d.aggregation) && (
							<Text size="xs" c="dimmed">
								{[d.statement, d.aggregation].filter(Boolean).join(" · ")}
							</Text>
						)}
						<Text size="xs" c="dimmed" data-testid="analyse-scope">
							{scope}
						</Text>
					</Group>
					{shape?.expression && (
						<EquationHeader
							shape={shape}
							totals={totalsRow}
							hoverRow={hoverRow}
							lockedRow={lockedRow}
							scope={scope}
						/>
					)}
				</Group>
			}
		>
			{compose.isPending ? (
				<Center h={160} data-testid="node-analyse-composing">
					<Loader size="sm" />
				</Center>
			) : compose.isError ? (
				<Alert color="red" title="Compose failed">
					{compose.error instanceof Error
						? compose.error.message
						: "unknown error"}
				</Alert>
			) : !ok ? (
				<Alert color="yellow" title="Cannot compose this node">
					{data && !data.ok ? data.reason : ""}
				</Alert>
			) : (
				<DrillableGrid
					sql={ok.sql}
					axesRequest={target}
					nodeRef={target}
					footerCells={totalsRow ?? undefined}
					columnAccents={accents}
					columnUnits={
						shape?.unit ? { value: unitSymbol(shape.unit) } : undefined
					}
					onRowHover={setHoverRow}
					onPinnedRow={setLockedRow}
					onStepsChange={setSteps}
					fillHeight
				/>
			)}
		</Modal>
	);
}
