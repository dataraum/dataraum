import {
	ActionIcon,
	Badge,
	Button,
	Group,
	Stack,
	Text,
	TextInput,
	Title,
} from "@mantine/core";
import {
	createFileRoute,
	notFound,
	useNavigate,
	useRouter,
} from "@tanstack/react-router";
import { createServerFn, useServerFn } from "@tanstack/react-start";
import { Check, Pencil, Trash2, TriangleAlert, X } from "lucide-react";
import { useState } from "react";
import {
	getReport,
	renameReport,
	setReportFingerprint,
	softDeleteReport,
} from "#/db/cockpit/reports";
import { computeReportFingerprint } from "#/duckdb/report-fingerprint-read";
import { ConfidenceStrip } from "#/ui/cockpit/widgets/answer-result";
import { ResultGridWidget } from "#/ui/cockpit/widgets/result-grid";

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
// The loader + action server fns are defined inline (the cockpit route convention)
// so the plugin strips their cockpit_db + lake handlers from the client bundle.

const loadReport = createServerFn({ method: "GET" })
	.inputValidator((reportId: string) => reportId)
	.handler(async ({ data: reportId }) => {
		const report = await getReport(reportId);
		if (!report) return null;
		let outdated = false;
		try {
			const { fingerprint } = await computeReportFingerprint(report.sql);
			if (report.summaryFingerprint === null) {
				// First time we can fingerprint this report — backfill, don't badge.
				await setReportFingerprint(report.id, fingerprint);
			} else {
				outdated = report.summaryFingerprint !== fingerprint;
			}
		} catch (err) {
			// Best-effort: if the live result can't be fingerprinted (a since-broken
			// SQL, a lake hiccup), don't badge — the grid surfaces the real error.
			console.error("[reports] drift check failed — not flagging:", err);
		}
		return { report, outdated };
	});

const renameReportFn = createServerFn({ method: "POST" })
	.inputValidator((data: { id: string; title: string }) => data)
	.handler(async ({ data }) => {
		await renameReport(data.id, data.title);
	});

const deleteReportFn = createServerFn({ method: "POST" })
	.inputValidator((reportId: string) => reportId)
	.handler(async ({ data: reportId }) => {
		await softDeleteReport(reportId);
	});

export const Route = createFileRoute(
	"/(app)/workspace/$wsId/reports/$reportId",
)({
	loader: async ({ params }) => {
		const data = await loadReport({ data: params.reportId });
		if (!data) throw notFound();
		return data;
	},
	component: ReportDetail,
});

function ReportDetail() {
	const { report, outdated } = Route.useLoaderData();
	const { wsId } = Route.useParams();
	const router = useRouter();
	const navigate = useNavigate();
	const rename = useServerFn(renameReportFn);
	const remove = useServerFn(deleteReportFn);

	const [editing, setEditing] = useState(false);
	// Seeded when the editor opens (below), NOT from useState(report.title): after a
	// rename, router.invalidate() refreshes `report` WITHOUT remounting, so a
	// once-initialized draft would show the stale pre-rename title on the next open.
	const [draft, setDraft] = useState("");
	const [busy, setBusy] = useState(false);

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
			navigate({ to: "/workspace/$wsId/reports", params: { wsId } });
		} finally {
			setBusy(false);
		}
	};

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
						</Group>
					)}
					<Text>{report.summary}</Text>
				</Stack>
			)}
			<ConfidenceStrip confidence={report.confidence} />
			<ResultGridWidget state={{ kind: "result-grid", sql: report.sql }} />
		</Stack>
	);
}
