// MeasureProgress widget (DAT-352) — live per-phase progress for an add_source
// (or replay) run the TRIGGER started.
//
// Polls the engine's `get_progress` @workflow.query (DAT-406) via the
// `/api/add-source-progress` route (→ `getAddSourceProgress`) on a TanStack Query `refetchInterval` (~1s),
// keyed on the precise (workflowId, runId) the TRIGGER returned. It STOPS polling
// once the snapshot reports `done` — either phase==="done" OR a terminal
// describe() status (so a FAILED run, which never sets "done", still halts).
//
// Renders the phase pipeline (import → processing_tables → semantic_per_column →
// detect → done) with the current step highlighted, the per-table fan-out tally
// during processing_tables, plus done / failed states. Receives ONLY {state}
// (canvas widgets have no sendMessage) — the run identity is on the state.

import {
	Alert,
	Badge,
	Group,
	Loader,
	Progress,
	ScrollArea,
	Stack,
	Text,
} from "@mantine/core";
import { useQuery } from "@tanstack/react-query";
import { Check, X } from "lucide-react";
import type { AddSourceProgress, TableStep } from "#/temporal/progress";
import { PROGRESS_DONE_PHASE } from "#/temporal/types";
import type { CanvasState } from "#/ui/cockpit/canvas-state";

// The phase pipeline in order, with friendly labels. Mirrors the engine's
// advance sequence (workflows.py): import → processing_tables →
// semantic_per_column → detect → done. A `phase` the engine reports that isn't
// here (forward-compat) renders no highlight rather than crashing.
const PHASES = [
	{ key: "import", label: "Import" },
	{ key: "processing_tables", label: "Type tables" },
	{ key: "semantic_per_column", label: "Semantic" },
	{ key: "detect", label: "Detect" },
	{ key: "done", label: "Done" },
] as const;

// How long to count a phase as "active" before it's reached — the index of the
// reported phase in PHASES; everything before it reads as completed.
function phaseIndex(phase: string): number {
	return PHASES.findIndex((p) => p.key === phase);
}

// The friendly label for a phase key (for the failure message); falls back to
// the raw key for a forward-compat phase not in PHASES.
function phaseLabel(phase: string): string {
	return PHASES.find((p) => p.key === phase)?.label ?? phase;
}

/** The leading status glyph for one fanned-out table row. */
function TableStatusIcon({ status }: { status: TableStep["status"] }) {
	if (status === "done") {
		return (
			<Check
				size={14}
				color="var(--mantine-color-green-6)"
				data-testid="table-status-done"
			/>
		);
	}
	if (status === "failed") {
		return (
			<X
				size={14}
				color="var(--mantine-color-red-6)"
				data-testid="table-status-failed"
			/>
		);
	}
	return <Loader size={12} data-testid="table-status-running" />;
}

const POLL_INTERVAL_MS = 1000;

/** Poll the progress API route for one run. Throws on a non-2xx so TanStack
 * Query surfaces it as the widget's error state. */
async function fetchProgress(
	workflowId: string,
	runId: string,
): Promise<AddSourceProgress> {
	const res = await fetch("/api/add-source-progress", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({ workflow_id: workflowId, run_id: runId }),
	});
	if (!res.ok) {
		const body = (await res.json().catch(() => ({}))) as { error?: string };
		throw new Error(body.error ?? `Progress query failed (${res.status}).`);
	}
	return (await res.json()) as AddSourceProgress;
}

export function MeasureProgressWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "add-source-progress" }>;
}) {
	const { workflowId, runId } = state;

	const { data, error, isLoading } = useQuery({
		queryKey: ["add-source-progress", workflowId, runId],
		queryFn: () => fetchProgress(workflowId, runId),
		// Poll until the run is done; then stop (refetchInterval returns false).
		refetchInterval: (query) =>
			query.state.data?.done ? false : POLL_INTERVAL_MS,
		refetchOnWindowFocus: false,
	});

	if (error) {
		return (
			<Stack gap="xs" data-testid="canvas-measure-progress">
				<Text size="sm" fw={600}>
					Add source — progress
				</Text>
				<Alert color="red" data-testid="canvas-measure-progress-error">
					Couldn't read workflow progress: {(error as Error).message}
				</Alert>
			</Stack>
		);
	}

	if (isLoading || !data) {
		return (
			<Stack
				gap="sm"
				align="center"
				justify="center"
				h="100%"
				data-testid="canvas-measure-progress-loading"
			>
				<Loader size="sm" />
				<Text c="dimmed" size="sm">
					Starting add source…
				</Text>
			</Stack>
		);
	}

	const activeIdx = phaseIndex(data.phase);
	const failed =
		data.done &&
		data.phase !== PROGRESS_DONE_PHASE &&
		data.status !== "COMPLETED";
	const succeeded =
		data.phase === PROGRESS_DONE_PHASE || data.status === "COMPLETED";

	// During the per-table fan-out, surface the tally as a determinate bar.
	const showTally = data.phase === "processing_tables" && data.tables_total > 0;
	const tallyPct = showTally
		? Math.round((data.tables_completed / data.tables_total) * 100)
		: 0;

	return (
		<Stack gap="sm" data-testid="canvas-measure-progress">
			<Group justify="space-between" wrap="nowrap">
				<Text size="sm" fw={600}>
					Add source — progress
				</Text>
				{!data.done && (
					<Loader size="xs" data-testid="measure-progress-spinner" />
				)}
			</Group>

			{/* Phase pipeline — current step highlighted, prior steps done. */}
			<Group gap="xs" wrap="wrap" data-testid="measure-progress-phases">
				{PHASES.map((p, i) => {
					const isActive = i === activeIdx && !data.done;
					const isPast = activeIdx > i || (succeeded && p.key === "done");
					const color = isActive ? "blue" : isPast ? "green" : "gray";
					const variant = isActive || isPast ? "filled" : "light";
					return (
						<Badge
							key={p.key}
							color={color}
							variant={variant}
							size="sm"
							data-testid={`measure-phase-${p.key}`}
							data-state={isActive ? "active" : isPast ? "done" : "pending"}
						>
							{p.label}
						</Badge>
					);
				})}
			</Group>

			{showTally && (
				<Stack gap={4} data-testid="measure-progress-tally">
					<Text size="xs" c="dimmed">
						Typing tables: {data.tables_completed} / {data.tables_total}
					</Text>
					<Progress value={tallyPct} size="sm" />
				</Stack>
			)}

			{/* The named steps behind the count — which tables are running / done /
			    failed. Scrolls past a handful so a wide source can't blow out the
			    canvas. */}
			{data.tables.length > 0 && (
				<ScrollArea.Autosize mah={180} data-testid="measure-progress-tables">
					<Stack gap={2}>
						{data.tables.map((t) => (
							<Group key={t.raw_table_id} gap={6} wrap="nowrap">
								<TableStatusIcon status={t.status} />
								<Text
									size="xs"
									c={
										t.status === "failed"
											? "red"
											: t.status === "running"
												? "dimmed"
												: undefined
									}
									truncate
									data-testid={`measure-table-${t.raw_table_id}`}
								>
									{t.name}
								</Text>
							</Group>
						))}
					</Stack>
				</ScrollArea.Autosize>
			)}

			{succeeded && (
				<Alert color="green" data-testid="measure-progress-done">
					Add source complete — readiness is ready to inspect.
				</Alert>
			)}

			{failed && (
				<Alert color="red" data-testid="measure-progress-failed">
					{failureMessage(data)}
				</Alert>
			)}
		</Stack>
	);
}

/** The human-readable failure line: the engine's root-cause message, scoped to
 * the failed table (by name) or the failed source-level phase. Falls back to the
 * describe() status when the snapshot carried no failure detail (e.g. a run
 * TERMINATED out-of-band, which never stamps `failure`). */
function failureMessage(data: AddSourceProgress): string {
	const f = data.failure;
	if (!f) {
		return `The add source run ended in ${data.status.toLowerCase()} at the ${data.phase} phase.`;
	}
	if (f.table_id) {
		const name =
			data.tables.find((t) => t.raw_table_id === f.table_id)?.name ??
			`table ${f.table_id.slice(0, 8)}`;
		return `Add source failed on ${name}: ${f.message}`;
	}
	return `Add source failed during ${phaseLabel(f.phase)}: ${f.message}`;
}
