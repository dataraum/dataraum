import {
	ActionIcon,
	Anchor,
	Badge,
	Button,
	Center,
	Code,
	Group,
	ScrollArea,
	Stack,
	Text,
	TextInput,
	Title,
	Tooltip,
} from "@mantine/core";
import {
	createFileRoute,
	type ErrorComponentProps,
	Link,
	notFound,
	useNavigate,
	useRouter,
} from "@tanstack/react-router";
import { useServerFn } from "@tanstack/react-start";
import {
	Check,
	Library,
	Pencil,
	RefreshCw,
	Trash2,
	TriangleAlert,
	X,
} from "lucide-react";
import { useMemo, useState } from "react";
import type { ReportRow } from "#/db/cockpit/reports";
import type { DrillStep } from "#/duckdb/drill";
import {
	decodeDrillSearch,
	encodeDrillSearch,
} from "#/ui/cockpit/report-drill-search";
import { ConfidenceStrip } from "#/ui/cockpit/widgets/answer-result";
import { DrillableGrid } from "#/ui/cockpit/widgets/drillable-grid";
import { ReportChart } from "#/ui/cockpit/widgets/report-chart";
import {
	deleteReportFn,
	loadReport,
	regenerateSummaryFn,
	renameReportFn,
} from "./$reportId.functions";

// Report detail (DAT-624 / DAT-625) — the frozen artifact rendered over LIVE data:
// the SQL is re-run on every open through the same result-grid stream, so numbers
// stay current. The title is the one editable field (inline); the SQL / confidence
// are immutable; the summary is frozen prose, refreshed only via regenerate. Delete
// is soft (the row stays; children keep their lineage).
//
// On open we re-fingerprint the live result (DAT-625) and compare it to the stored
// fingerprint: a mismatch means the frozen summary is talking about stale numbers, so
// it's badged "outdated". A null stored fingerprint (pre-DAT-625 report, or a failed
// mint-time fingerprint) is lazy-backfilled here — start tracking, show clean.
//
// The loader + action server fns live in the sibling `$reportId.functions.ts`
// (the cockpit route convention) so their cockpit_db + lake handlers are stripped
// from the client bundle.
//
// DRILL PERSISTENCE (DAT-676): the grid mounts DIRECTLY on `DrillableGrid` (not
// the canvas-registered `DrillableResultGridWidget`, which owns the run_sql/
// canvas path and stays untouched here) so this route can wire a typed `?drill=`
// search param — decoded once as `initialSteps` so a reload/shared link restores
// the drilled result, and re-encoded on every live step change so the URL always
// names what the grid is showing. A step that no longer composes (the DrillableGrid
// rehydrate path) degrades to the base result with its own visible notice.
//
// CHILD-REPORT MINT (DAT-627): while drilled, the grid's toolbar carries its own
// "Report" action — mints a NEW report whose `sql`/`sqlParams` are the composed
// drilled statement and whose `parentId` is THIS report's id; the source row is
// never touched. No confidence describes a slice nobody computed one for, so the
// child mints with `confidence: null` (the same honesty the answer surface now
// carries — see answer-result.tsx).

export const Route = createFileRoute("/(app)/reports/$reportId")({
	validateSearch: (search: Record<string, unknown>) => {
		const drill = decodeDrillSearch(search.drill);
		return { drill: drill.length > 0 ? drill : undefined };
	},
	loader: async ({ params }) => {
		const data = await loadReport({ data: params.reportId });
		if (!data) throw notFound();
		return data;
	},
	component: ReportDetail,
	// A render throw must degrade to a readable error, never a white screen
	// (the operating-model.tsx precedent). Reports' confidence/chartConfig
	// jsonb is validated at MINT (mint.ts's MintBodySchema) but not on every
	// read — a direct DB edit or a future writer could still leave a shape
	// ConfidenceStrip/ReportChart don't expect, and this is the ONE artifact
	// on the page, so a full-page fallback is the right grain here (contrast
	// the gallery, where one bad row must not blank every card — band-badge.tsx
	// / inventory-grouping.ts carry that surface's hardening instead, since the
	// gallery only ever dereferences `.band`).
	errorComponent: ReportDetailError,
});

function ReportDetailError({ error }: ErrorComponentProps) {
	return (
		<Center h="100%">
			<Stack gap="xs" align="center" maw={560}>
				<TriangleAlert size={32} color="var(--mantine-color-red-6)" />
				<Text fw={600}>Couldn't load this report</Text>
				<Text size="sm" c="dimmed" ta="center">
					The stored report data didn't match what this page expects. This is
					usually a corrupted or hand-edited record — try the reports gallery.
				</Text>
				<ScrollArea.Autosize mah={200} w="100%">
					<Code block>{error.message}</Code>
				</ScrollArea.Autosize>
			</Stack>
		</Center>
	);
}

function ReportDetail() {
	const { report, outdated, parentTitle } = Route.useLoaderData();
	// Scoped to THIS route's own `drill` search param (Route.useNavigate binds
	// `from` to the route automatically) — lifted here (not read inside
	// ReportDetailBody) so that component takes every router-derived value as
	// a PROP, same as report/outdated/parentTitle below: it's the seam that
	// makes the drill-search wiring testable without a live router context
	// ($reportId.test.tsx mounts ReportDetailBody directly).
	const navigateSearch = Route.useNavigate();
	const search = Route.useSearch();
	// REMOUNT PER REPORT (React rule 5): the lineage link (below) and a future
	// child mint both navigate between two DIFFERENT `$reportId` matches on the
	// SAME route — TanStack Router reuses the component instance across a
	// params-only change, so without this key the title-edit draft, the drill's
	// committed steps, and the mint's "Saved to Reports" state would all leak
	// from the report just left into the one just opened (the AnswerResultWidget
	// `key={state.sql}` precedent, answer-result.tsx).
	return (
		<ReportDetailBody
			key={report.id}
			report={report}
			outdated={outdated}
			parentTitle={parentTitle}
			search={search}
			navigateSearch={navigateSearch}
		/>
	);
}

/** Exported for `$reportId.test.tsx` — the closest testable seam for the
 *  search.drill → initialSteps → onStepsChange → navigateSearch round trip.
 *  `createFileRoute` components can't be rendered without a live matched
 *  route tree (no precedent for it anywhere in this codebase), so `search`/
 *  `navigateSearch` are explicit props here rather than internal
 *  `Route.useSearch()`/`Route.useNavigate()` calls — everything this
 *  component needs from the router arrives as a prop, so a test can mount it
 *  directly with a mocked DrillableGrid (the answer-result.test.tsx
 *  precedent) and fake values for both. */
export function ReportDetailBody({
	report,
	outdated,
	parentTitle,
	search,
	navigateSearch,
}: {
	report: ReportRow;
	outdated: boolean;
	parentTitle: string | null;
	search: { drill?: DrillStep[] };
	navigateSearch: ReturnType<typeof Route.useNavigate>;
}) {
	const router = useRouter();
	const navigate = useNavigate();
	const rename = useServerFn(renameReportFn);
	const remove = useServerFn(deleteReportFn);
	const regenerate = useServerFn(regenerateSummaryFn);

	const [editing, setEditing] = useState(false);
	// Seeded when the editor opens (below), NOT from useState(report.title): after a
	// rename, router.invalidate() refreshes `report` WITHOUT remounting, so a
	// once-initialized draft would show the stale pre-rename title on the next open.
	const [draft, setDraft] = useState("");
	const [busy, setBusy] = useState(false);
	const [regenerating, setRegenerating] = useState(false);
	const [regenFailed, setRegenFailed] = useState(false);

	// The committed drill (DAT-676/627): the statement the grid is CURRENTLY
	// showing, mirroring the answer surface's own `drilled` state
	// (answer-result.tsx) — the toolbar's child-mint action needs the EFFECTIVE
	// query, and its very presence is gated on "is anything drilled at all".
	const [drilled, setDrilled] = useState<{
		sql: string;
		params: (string | number | boolean | null)[];
	} | null>(null);
	const [minting, setMinting] = useState(false);
	const [mintedId, setMintedId] = useState<string | null>(null);
	const [mintFailed, setMintFailed] = useState(false);

	// `?drill=` decodes ONCE, at load, into DrillableGrid's rehydrate path
	// (initialSteps is read only on mount there — see drillable-grid.tsx); a
	// later live change flows the other way, through `onStepsChange` below.
	const initialSteps: DrillStep[] = search.drill ?? [];

	const axesRequest = useMemo(
		() =>
			report.sqlParams && report.sqlParams.length > 0
				? { resultSql: report.sql, resultParams: report.sqlParams }
				: { resultSql: report.sql },
		[report.sql, report.sqlParams],
	);

	// Refresh the stale summary: regenerate server-side, then re-load so the new prose
	// + cleared badge render. On failure keep the old summary + badge and flag inline.
	const refreshSummary = async () => {
		setRegenerating(true);
		setRegenFailed(false);
		try {
			await regenerate({ data: report.id });
			await router.invalidate();
		} catch (err) {
			console.error("[reports] regenerate summary failed:", err);
			setRegenFailed(true);
		} finally {
			setRegenerating(false);
		}
	};

	// Mutations fired by user events live in handlers, not effects (React conv. 4).
	const saveTitle = async () => {
		const title = draft.trim();
		if (!title || title === report.title) {
			setEditing(false);
			return;
		}
		setBusy(true);
		try {
			await rename({ data: { id: report.id, title } });
			setEditing(false);
			await router.invalidate();
		} finally {
			setBusy(false);
		}
	};

	const deleteReport = async () => {
		setBusy(true);
		try {
			await remove({ data: report.id });
			navigate({ to: "/reports" });
		} finally {
			setBusy(false);
		}
	};

	// Mint a CHILD report from the drilled grid (DAT-627). No narrative
	// describes a slice nobody generated one for — same honesty the answer
	// surface carries (confidence: null, and here summary: "" too, since
	// `reports.summary` is NOT NULL and an empty string is the same "nothing
	// computed" state as a null confidence, not a fabricated one).
	const onMintChild = async () => {
		if (!drilled) return;
		setMinting(true);
		setMintFailed(false);
		try {
			const res = await fetch("/api/reports/mint", {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({
					sql: drilled.sql,
					sqlParams: drilled.params.length > 0 ? drilled.params : null,
					summary: "",
					title: `${report.title} (drilled)`,
					conversationId: null,
					confidence: null,
					parentId: report.id,
				}),
			});
			if (!res.ok) throw new Error(`mint failed: ${res.status}`);
			const { id } = (await res.json()) as { id: string };
			setMintedId(id);
		} catch (err) {
			console.error("[reports] child mint failed:", err);
			setMintFailed(true);
		} finally {
			setMinting(false);
		}
	};

	const childMintAction = drilled ? (
		mintedId ? (
			<Button
				variant="light"
				color="green"
				size="compact-xs"
				leftSection={<Library size={13} />}
				data-testid="report-child-mint-saved"
				renderRoot={(props) => (
					<Link
						to="/reports/$reportId"
						params={{ reportId: mintedId }}
						{...props}
					/>
				)}
			>
				Saved to Reports
			</Button>
		) : (
			<Tooltip
				label="Save this drilled view as a new report, linked to this one"
				maw={280}
				multiline
			>
				<Button
					variant="subtle"
					color="gray"
					size="compact-xs"
					leftSection={<Library size={13} />}
					onClick={onMintChild}
					loading={minting}
					data-testid="report-child-mint"
				>
					Report
				</Button>
			</Tooltip>
		)
	) : undefined;

	return (
		<Stack p="md" gap="md" data-testid="report-detail">
			<Group justify="space-between" wrap="nowrap">
				{editing ? (
					<Group gap="xs" style={{ flex: 1 }}>
						<TextInput
							value={draft}
							onChange={(e) => setDraft(e.currentTarget.value)}
							onKeyDown={(e) => {
								if (e.key === "Enter") saveTitle();
								if (e.key === "Escape") {
									setDraft(report.title);
									setEditing(false);
								}
							}}
							style={{ flex: 1 }}
							data-autofocus
							data-testid="report-title-input"
						/>
						<ActionIcon
							variant="light"
							onClick={saveTitle}
							loading={busy}
							aria-label="Save title"
						>
							<Check size={16} />
						</ActionIcon>
						<ActionIcon
							variant="subtle"
							onClick={() => {
								setDraft(report.title);
								setEditing(false);
							}}
							aria-label="Cancel"
						>
							<X size={16} />
						</ActionIcon>
					</Group>
				) : (
					<Stack gap={2}>
						<Group gap="xs">
							<Title order={3}>{report.title}</Title>
							<ActionIcon
								variant="subtle"
								onClick={() => {
									setDraft(report.title);
									setEditing(true);
								}}
								aria-label="Rename report"
							>
								<Pencil size={16} />
							</ActionIcon>
						</Group>
						{/* Evolve lineage (DAT-627): only when the parent still resolves —
						    a soft-deleted or foreign parent id (getReportParentTitle's null)
						    omits the link rather than pointing at a dead page. */}
						{report.parentId && parentTitle && (
							<Text size="xs" c="dimmed" data-testid="report-parent-link">
								Evolved from{" "}
								<Anchor
									size="xs"
									renderRoot={(props) => (
										<Link
											to="/reports/$reportId"
											params={{ reportId: report.parentId as string }}
											{...props}
										/>
									)}
								>
									{parentTitle}
								</Anchor>
							</Text>
						)}
					</Stack>
				)}
				<Button
					color="red"
					variant="light"
					leftSection={<Trash2 size={14} />}
					onClick={deleteReport}
					loading={busy}
				>
					Delete
				</Button>
			</Group>

			{report.summary && (
				<Stack gap="xs">
					{outdated && (
						<Group gap="xs">
							<Badge
								color="yellow"
								variant="light"
								leftSection={<TriangleAlert size={12} />}
								tt="none"
								data-testid="report-outdated"
							>
								Outdated — data changed since this summary
							</Badge>
							<Button
								variant="light"
								color="yellow"
								size="compact-xs"
								leftSection={<RefreshCw size={13} />}
								onClick={refreshSummary}
								loading={regenerating}
								data-testid="report-regenerate"
							>
								Regenerate
							</Button>
						</Group>
					)}
					{regenFailed && (
						<Text size="xs" c="red" data-testid="report-regenerate-error">
							Couldn’t regenerate the summary — try again.
						</Text>
					)}
					<Text>{report.summary}</Text>
				</Stack>
			)}
			{report.confidence && <ConfidenceStrip confidence={report.confidence} />}
			{mintFailed && (
				<Text size="xs" c="red" data-testid="report-child-mint-error">
					Couldn’t save the drilled view as a report — try again.
				</Text>
			)}
			{/* Frozen chart (DAT-626) over live re-run data — above the table it
			    summarizes. Absent → table-only report (first-class). */}
			{report.chartConfig && (
				<ReportChart
					sql={report.sql}
					params={report.sqlParams ?? undefined}
					config={report.chartConfig}
				/>
			)}
			{/* Drillable (DAT-678), tier A: a report freezes a STATEMENT, not a
			    calculation, so there is nothing upstream to recompose from — but
			    the reader can still group the live result by any catalogued
			    dimension it returns. A saved/shared `?drill=` link restores
			    (DAT-676); the drill is otherwise view-local, the report itself
			    stays immutable. */}
			<DrillableGrid
				sql={report.sql}
				params={report.sqlParams ?? undefined}
				axesRequest={axesRequest}
				initialSteps={initialSteps}
				onStepsChange={(steps, effective) => {
					setDrilled(steps.length > 0 ? effective : null);
					// Retire a stale mint state (rule 1 precedent, answer-result.tsx):
					// the previous drill's "Saved to Reports" no longer describes
					// what the grid shows once the steps change again.
					setMintedId(null);
					setMintFailed(false);
					// No `...prev` spread: `validateSearch` returns ONLY `{ drill }`
					// on this route (nothing else is defined on it TODAY), so `prev`
					// never carries another key to preserve — spreading it was a
					// no-op, not a safety net. If a future search param joins this
					// route, navigating a drill change will STRIP it (this always
					// replaces the whole search object) unless this updater is
					// widened to merge it back in explicitly — worth a second look
					// then, not a silent behavior change now.
					navigateSearch({
						search: { drill: encodeDrillSearch(steps) },
						replace: true,
						resetScroll: false,
					});
				}}
				toolbarActions={childMintAction}
			/>
		</Stack>
	);
}
