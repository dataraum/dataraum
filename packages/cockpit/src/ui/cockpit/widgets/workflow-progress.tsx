// Shared workflow-progress core (DAT-352 add_source; DAT-435 begin_session).
//
// One polling + rendering surface for every workflow-progress widget: a grouped
// phase pipeline with the current step highlighted, a live caption naming the
// running stage, the per-table fan-out detail when the snapshot carries it, and
// terminal success/failure alerts. The per-workflow widgets (measure-progress,
// session-progress) supply ONLY a `WorkflowProgressDisplay` config — shared
// visual vocabulary is shared code (React idiom rule 13, the why-detail
// precedent).
//
// Polls the engine's `get_progress` @workflow.query via the
// `/api/workflow-progress` route (→ `getWorkflowProgress`) on a TanStack Query
// `refetchInterval` (~1s), keyed on the precise (workflowId, runId). It STOPS
// polling once the snapshot reports `done` — either phase==="done" OR a
// terminal describe() status (so a FAILED run, which never sets "done", still
// halts). This is the polling template of React idiom rule 3.

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
import { displayTableName, stripSrcDigests } from "#/lib/display-names";
import type { TableStep, WorkflowProgress } from "#/temporal/progress";
import { PROGRESS_DONE_PHASE } from "#/temporal/types";

/** One badge in the pipeline, covering 1–N raw engine phases. A single-phase
 * group renders exactly like the ungrouped add_source pipeline; a multi-phase
 * group (the begin_session value layer) collapses noisy stages into one badge
 * while the caption below still names the precise running stage. */
export interface PhaseGroup {
	// Stable badge identity — the testid suffix (`${testId}-phase-${key}`).
	key: string;
	label: string;
	// The raw snapshot phases this badge covers, in execution order.
	phases: readonly string[];
}

/** The per-workflow display config — everything the shared view can't derive
 * from the polled snapshot. */
export interface WorkflowProgressDisplay {
	title: string;
	// testid prefix: root `canvas-${testId}-progress`, badges
	// `${testId}-phase-${key}`, … — keeps each widget's tests targetable.
	testId: string;
	// The display pipeline in order, INCLUDING the terminal done group. A phase
	// the engine reports that maps to no group (forward-compat) renders no
	// highlight rather than crashing.
	groups: readonly PhaseGroup[];
	// Live caption per raw phase, for phases that carry no per-table signal.
	// The tally phase (if any) is intentionally absent — it has its own bar.
	captions: Record<string, string>;
	// The raw phase whose per-table fan-out tally owns the surface (add_source's
	// "processing_tables"); omit for sequential workflows with no fan-out.
	tallyPhase?: string;
	// Label over the tally bar, e.g. "Typing tables".
	tallyLabel?: string;
	// Sentence prefix for failure copy: `${failurePrefix} failed during …`.
	failurePrefix: string;
	// Caption under the loader before the first snapshot lands.
	startingLabel: string;
	// The success-alert line, derived from the terminal snapshot.
	doneMessage: (data: WorkflowProgress) => string;
}

/** The group a raw phase renders under, or undefined for a forward-compat
 * phase the config doesn't know. */
function groupIndex(display: WorkflowProgressDisplay, phase: string): number {
	return display.groups.findIndex((g) => g.phases.includes(phase));
}

/**
 * The human descriptor for a failed phase: the group label, plus — when the
 * group collapses several raw stages — the stage's caption (lowercased,
 * ellipsis stripped) so the failure still names the precise stage the badge
 * hides: "Slice analysis (profiling each slice)". A single-phase group is
 * already precise; an unmapped phase falls back to the raw key.
 */
function phaseDescriptor(
	display: WorkflowProgressDisplay,
	phase: string,
): string {
	const group = display.groups.find((g) => g.phases.includes(phase));
	if (!group) return phase;
	if (group.phases.length === 1) return group.label;
	const caption = display.captions[phase];
	if (!caption) return group.label;
	const detail = caption.replace(/…+$/u, "");
	// Sentence-case → lowercase for the parenthetical, but leave an
	// acronym-leading caption ("LLM …") alone — only a [A-Z][a-z] start is a
	// sentence capital.
	const decapped = /^[A-Z][a-z]/.test(detail)
		? detail.charAt(0).toLowerCase() + detail.slice(1)
		: detail;
	return `${group.label} (${decapped})`;
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
): Promise<WorkflowProgress> {
	const res = await fetch("/api/workflow-progress", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({ workflow_id: workflowId, run_id: runId }),
	});
	if (!res.ok) {
		const body = (await res.json().catch(() => ({}))) as { error?: string };
		throw new Error(body.error ?? `Progress query failed (${res.status}).`);
	}
	return (await res.json()) as WorkflowProgress;
}

export function WorkflowProgressView({
	display,
	workflowId,
	runId,
}: {
	display: WorkflowProgressDisplay;
	workflowId: string;
	runId: string;
}) {
	const { data, error, isLoading } = useQuery({
		// One key namespace for both widgets — they poll the same endpoint for
		// the same run, so the cached snapshot is interchangeable.
		queryKey: ["workflow-progress", workflowId, runId],
		queryFn: () => fetchProgress(workflowId, runId),
		// Poll until the run is done; then stop (refetchInterval returns false).
		refetchInterval: (query) =>
			query.state.data?.done ? false : POLL_INTERVAL_MS,
		refetchOnWindowFocus: false,
	});

	const p = display.testId;

	if (error) {
		return (
			<Stack gap="xs" data-testid={`canvas-${p}-progress`}>
				<Text size="sm" fw={600}>
					{display.title}
				</Text>
				<Alert color="red" data-testid={`canvas-${p}-progress-error`}>
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
				data-testid={`canvas-${p}-progress-loading`}
			>
				<Loader size="sm" />
				<Text c="dimmed" size="sm">
					{display.startingLabel}
				</Text>
			</Stack>
		);
	}

	const activeIdx = groupIndex(display, data.phase);
	const failed =
		data.done &&
		data.phase !== PROGRESS_DONE_PHASE &&
		data.status !== "COMPLETED";
	const succeeded =
		data.phase === PROGRESS_DONE_PHASE || data.status === "COMPLETED";

	// During a per-table fan-out, surface the tally as a determinate bar.
	const showTally =
		display.tallyPhase !== undefined &&
		data.phase === display.tallyPhase &&
		data.tables_total > 0;
	const tallyPct = showTally
		? Math.round((data.tables_completed / data.tables_total) * 100)
		: 0;

	return (
		<Stack gap="sm" data-testid={`canvas-${p}-progress`}>
			{/* No corner spinner: the per-phase pipeline + captions below already
			    show liveness; a detached top-right Loader read as "stuck". */}
			<Text size="sm" fw={600}>
				{display.title}
			</Text>

			{/* Phase pipeline — current group highlighted, prior groups done. */}
			<Group gap="xs" wrap="wrap" data-testid={`${p}-progress-phases`}>
				{display.groups.map((g, i) => {
					const isActive = i === activeIdx && !data.done;
					const isPast =
						activeIdx > i || (succeeded && g.phases.includes("done"));
					const color = isActive ? "blue" : isPast ? "green" : "gray";
					const variant = isActive || isPast ? "filled" : "light";
					return (
						<Badge
							key={g.key}
							color={color}
							variant={variant}
							size="sm"
							data-testid={`${p}-phase-${g.key}`}
							data-state={isActive ? "active" : isPast ? "done" : "pending"}
						>
							{g.label}
						</Badge>
					);
				})}
			</Group>

			{showTally && (
				<Stack gap={4} data-testid={`${p}-progress-tally`}>
					<Text size="xs" c="dimmed">
						{display.tallyLabel}: {data.tables_completed} / {data.tables_total}
					</Text>
					<Progress value={tallyPct} size="sm" />
				</Stack>
			)}

			{/* The no-tally phases have no per-table signal — show an indeterminate
			    caption so the surface isn't dead air while they run. */}
			{!data.done && !showTally && display.captions[data.phase] && (
				<Group gap="xs" wrap="nowrap" data-testid={`${p}-progress-caption`}>
					<Loader size="xs" />
					<Text size="xs" c="dimmed">
						{display.captions[data.phase]}
					</Text>
				</Group>
			)}

			{/* The named steps behind the count — which tables are running / done /
			    failed. Scrolls past a handful so a wide source can't blow out the
			    canvas. Renders only when the snapshot carries steps (add_source). */}
			{data.tables.length > 0 && (
				<ScrollArea.Autosize mah={180} data-testid={`${p}-progress-tables`}>
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
									data-testid={`${p}-table-${t.raw_table_id}`}
								>
									{displayTableName(t.name)}
								</Text>
							</Group>
						))}
					</Stack>
				</ScrollArea.Autosize>
			)}

			{succeeded && (
				<Alert color="green" data-testid={`${p}-progress-done`}>
					{display.doneMessage(data)}
				</Alert>
			)}

			{failed && (
				<Alert color="red" data-testid={`${p}-progress-failed`}>
					{failureMessage(display, data)}
				</Alert>
			)}
		</Stack>
	);
}

/** The human-readable failure line: the engine's root-cause message, scoped to
 * the failed table (by name) or the failed run-level phase. Falls back to the
 * describe() status when the snapshot carried no failure detail (e.g. a run
 * TERMINATED out-of-band, which never stamps `failure`). The engine-built
 * message can embed content-keyed `src_<digest>` names or the staged-upload s3
 * URI — `stripSrcDigests` neutralizes them, the same treatment the agent-facing
 * workflow_status projection applies (DAT-433). */
function failureMessage(
	display: WorkflowProgressDisplay,
	data: WorkflowProgress,
): string {
	const f = data.failure;
	if (!f) {
		return `The ${display.failurePrefix.toLowerCase()} run ended in ${data.status.toLowerCase()} at the ${data.phase} phase.`;
	}
	if (f.table_id) {
		// Same name-display rule as the live list — strip the `<source>__` prefix so
		// a FAILED run reads `trial_balance`, not `finance_data__trial_balance`.
		const name =
			data.tables.find((t) => t.raw_table_id === f.table_id)?.name ??
			`table ${f.table_id.slice(0, 8)}`;
		return `${display.failurePrefix} failed on ${displayTableName(name)}: ${stripSrcDigests(f.message)}`;
	}
	return `${display.failurePrefix} failed during ${phaseDescriptor(display, f.phase)}: ${stripSrcDigests(f.message)}`;
}
